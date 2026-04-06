"""
retention_engine.py  v3  (2026-03)
────────────────────────────────────────────────────────────────────────────────
Auto-Auth → Sync → Analyze

Alur kerja:
  1. Baca kredensial dari config/secrets/<channel_id>_secret.json
     (path didapat dari settings.json → channel.google_client_secret)
  2. Token OAuth2 disimpan / di-refresh di config/credentials/<channel_id>_token.json
     (path dari settings.json → channel.credentials_file)
  3. Fetch daftar video yang sudah live via YouTube API → simpan ke SQLite video_registry
  4. Fetch basic stats (views, likes) via videos.list (tidak butuh Analytics API)
  5. Analisis data untuk menghasilkan insights & topic hints

Tidak butuh YOUTUBE_API_KEY — cukup OAuth2 client secret yang sudah ada.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from engine.utils import get_logger
from engine.state_manager import (
    upsert_video,
    get_videos_for_channel,
    get_video_ids_for_channel,
    count_videos_for_channel,
)

logger = get_logger("retention_engine")

# ─── Konstanta ────────────────────────────────────────────────────────────────
SETTINGS_PATH = "config/settings.json"
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]
_YT_ANALYTICS_SCOPE = "https://www.googleapis.com/auth/yt-analytics.readonly"


# ─── Helper: baca settings.json ───────────────────────────────────────────────
def _load_settings() -> list[dict]:
    """Return daftar channel dari config/settings.json."""
    with open(SETTINGS_PATH, encoding="utf-8") as f:
        return json.load(f)["channels"]


def _get_channel_cfg(channel_id: str) -> dict:
    """
    Cari konfigurasi channel berdasarkan id.
    Raise ValueError jika tidak ditemukan.
    """
    for ch in _load_settings():
        if ch["id"] == channel_id:
            return ch
    raise ValueError(f"Channel '{channel_id}' tidak ditemukan di {SETTINGS_PATH}")


# ─── Helper: OAuth2 ───────────────────────────────────────────────────────────
def _build_credentials(channel_cfg: dict):
    """
    Bangun google.oauth2.credentials.Credentials dari:
      - channel_cfg["google_client_secret"]  → client secret file (sudah ada)
      - channel_cfg["credentials_file"]      → token cache (dibuat otomatis saat pertama login)

    Return: google.oauth2.credentials.Credentials
    """
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as exc:
        raise ImportError(
            "Paket Google Auth belum terinstall. Jalankan:\n"
            "  pip install google-auth google-auth-oauthlib google-auth-httplib2"
        ) from exc

    secret_path = channel_cfg.get("google_client_secret", "")
    token_path = channel_cfg.get("credentials_file", "")

    if not secret_path or not Path(secret_path).exists():
        raise FileNotFoundError(
            f"File secret tidak ditemukan: {secret_path!r}\n"
            f"Pastikan field 'google_client_secret' di settings.json benar."
        )

    creds: Optional[Credentials] = None

    # Coba load token yang sudah ada
    if token_path and Path(token_path).exists():
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except Exception as e:
            logger.warning(f"Token cache rusak atau invalid, akan re-auth: {e}")
            creds = None

    # Token expired → refresh
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            logger.info(f"[{channel_cfg['id']}] Token di-refresh otomatis.")
        except Exception as e:
            logger.warning(f"Refresh token gagal, akan re-auth: {e}")
            creds = None

    # Belum ada / tidak valid → flow
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(secret_path, SCOPES)
        creds = flow.run_local_server(port=0, open_browser=True)
        logger.info(f"[{channel_cfg['id']}] Login sukses, menyimpan token.")

    # Simpan token baru
    if token_path:
        os.makedirs(Path(token_path).parent, exist_ok=True)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return creds


# ─── Helper: bangun YouTube client ────────────────────────────────────────────
def _build_youtube_client(channel_cfg: dict):
    """Return googleapiclient.discovery resource untuk YouTube Data API v3."""
    try:
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise ImportError(
            "google-api-python-client belum terinstall. Jalankan:\n"
            "  pip install google-api-python-client"
        ) from exc

    creds = _build_credentials(channel_cfg)
    return build("youtube", "v3", credentials=creds)


def _build_analytics_client(channel_cfg: dict):
    """Return googleapiclient.discovery resource untuk YouTube Analytics API v2."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        return None

    creds = _build_credentials(channel_cfg)
    # Cek apakah scope analytics tersedia
    if hasattr(creds, "scopes") and creds.scopes and _YT_ANALYTICS_SCOPE not in creds.scopes:
        return None
    return build("youtubeAnalytics", "v2", credentials=creds)


# ─── Core: Sync video terbaru ke DB ───────────────────────────────────────────
def sync_recent_videos(channel_id: str, max_results: int = 25) -> int:
    """
    Fetch video ID yang sudah live di channel, simpan ke video_registry.

    Strategi:
      1. Gunakan youtube.search().list (type=video, mine=True) untuk
         mendapat daftar video milik channel.
      2. Ambil basic stats (views, likes) via youtube.videos().list.
      3. Simpan semua ke SQLite via state_manager.upsert_video().

    Return: jumlah video yang baru di-upsert.
    """
    channel_cfg = _get_channel_cfg(channel_id)
    channel_niche = channel_cfg.get("niche", "")
    channel_lang = channel_cfg.get("language", "")

    logger.info(f"[{channel_id}] Mulai sync {max_results} video terbaru…")

    try:
        yt = _build_youtube_client(channel_cfg)
    except Exception as e:
        logger.error(f"[{channel_id}] Gagal build YouTube client: {e}")
        return 0

    # ── Step 1: Fetch daftar video via search.list ──────────────────────────
    video_ids: list[str] = []
    video_meta: dict[str, dict] = {}  # video_id → metadata dasar

    try:
        request = yt.search().list(
            part="id,snippet",
            forMine=True,
            type="video",
            order="date",
            maxResults=min(max_results, 50),
        )
        response = request.execute()

        for item in response.get("items", []):
            vid_id = item["id"].get("videoId")
            if not vid_id:
                continue
            snippet = item.get("snippet", {})
            video_ids.append(vid_id)
            video_meta[vid_id] = {
                "title":        snippet.get("title", ""),
                "published_at": snippet.get("publishedAt", ""),
            }

    except Exception as e:
        logger.error(f"[{channel_id}] search.list gagal: {e}")
        return 0

    if not video_ids:
        logger.warning(f"[{channel_id}] Tidak ada video ditemukan.")
        return 0

    logger.info(f"[{channel_id}] Ditemukan {len(video_ids)} video dari search.list.")

    # ── Step 2: Fetch basic stats (views, likes) via videos.list ───────────
    stats_map: dict[str, dict] = {}
    try:
        # videos.list menerima max 50 ID sekaligus
        chunk_size = 50
        for i in range(0, len(video_ids), chunk_size):
            chunk = video_ids[i:i + chunk_size]
            resp = yt.videos().list(
                part="statistics",
                id=",".join(chunk),
            ).execute()

            for item in resp.get("items", []):
                vid_id = item["id"]
                stats = item.get("statistics", {})
                stats_map[vid_id] = {
                    "views": int(stats.get("viewCount", 0)),
                    "likes": int(stats.get("likeCount", 0)),
                }

    except Exception as e:
        logger.warning(f"[{channel_id}] videos.list gagal (basic stats diset 0): {e}")

    # ── Step 3: Upsert ke DB ────────────────────────────────────────────────
    saved_count = 0
    now_iso = datetime.now().isoformat()

    for vid_id in video_ids:
        meta = video_meta.get(vid_id, {})
        stats = stats_map.get(vid_id, {})

        upsert_video({
            "video_id":           vid_id,
            "channel_id":         channel_id,
            "title":              meta.get("title", ""),
            "topic":              meta.get("title", ""),
            "niche":              channel_niche,
            "language":           channel_lang,
            "published_at":       meta.get("published_at", ""),
            "views":              stats.get("views", 0),
            "likes":              stats.get("likes", 0),
            "basic_stats_fetched": True,
            "source":             "sync_recent",
            "synced_at":          now_iso,
        })
        saved_count += 1

    logger.info(f"[{channel_id}] {saved_count} video berhasil di-sync ke DB.")
    return saved_count


# ─── Core: Enrich analytics lebih dalam (opsional) ────────────────────────────
def enrich_analytics(channel_id: str, days_back: int = 28) -> int:
    """
    Fetch avg_view_duration & avg_view_percentage via YouTube Analytics API.
    Fungsi ini opsional — berjalan hanya jika scope analytics tersedia.

    Return: jumlah video yang di-update.
    """
    channel_cfg = _get_channel_cfg(channel_id)

    try:
        analytics = _build_analytics_client(channel_cfg)
        if analytics is None:
            logger.info(f"[{channel_id}] Analytics API tidak tersedia, skip enrich.")
            return 0
    except Exception as e:
        logger.warning(f"[{channel_id}] Tidak bisa bangun Analytics client: {e}")
        return 0

    video_ids = list(get_video_ids_for_channel(channel_id))
    if not video_ids:
        logger.warning(f"[{channel_id}] Tidak ada video di DB untuk dienrich.")
        return 0

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    updated = 0
    for vid_id in video_ids:
        try:
            resp = analytics.reports().query(
                ids=f"channel==MINE",
                startDate=start_date,
                endDate=end_date,
                metrics="estimatedMinutesWatched,averageViewDuration,averageViewPercentage",
                dimensions="video",
                filters=f"video=={vid_id}",
            ).execute()

            rows = resp.get("rows", [])
            if not rows:
                continue

            _, _, avg_dur_sec, avg_pct = rows[0]
            upsert_video({
                "video_id":              vid_id,
                "channel_id":            channel_id,
                "avg_view_duration_sec": float(avg_dur_sec),
                "avg_view_percentage":   float(avg_pct),
                "analytics_fetched":     True,
            })
            updated += 1

        except Exception as e:
            logger.debug(f"[{channel_id}] enrich {vid_id} gagal: {e}")
            continue

    logger.info(f"[{channel_id}] Analytics enriched {updated}/{len(video_ids)} video.")
    return updated


# ─── Core: Analisis + Insights ────────────────────────────────────────────────
def analyze_channel(channel: dict) -> dict:
    """
    Analisis data retensi untuk satu channel.
    Auto-sync jika DB masih kosong.

    Args:
        channel: dict dari settings.json (harus memiliki key 'id')

    Return: dict insights siap pakai.
    """
    channel_id = channel["id"]

    # Auto-bootstrap: sync jika DB masih kosong
    total = count_videos_for_channel(channel_id)
    if total == 0:
        logger.info(f"[{channel_id}] DB kosong → auto-sync dari YouTube…")
        sync_recent_videos(channel_id)

    videos = get_videos_for_channel(channel_id)

    if not videos:
        logger.warning(f"[{channel_id}] Tidak ada data video setelah sync. Analisis dilewati.")
        return _empty_insights(channel_id)

    return _compute_insights(channel_id, videos)


def _compute_insights(channel_id: str, videos: list[dict]) -> dict:
    """Hitung insights dari list video."""
    total = len(videos)
    total_views = sum(v.get("views", 0) for v in videos)
    total_likes = sum(v.get("likes", 0) for v in videos)
    avg_views = total_views / total if total else 0
    avg_likes = total_likes / total if total else 0

    # Analytics-based (jika tersedia)
    has_analytics = any(v.get("analytics_fetched") for v in videos)
    avg_duration = 0.0
    avg_pct = 0.0
    if has_analytics:
        dur_vals = [v.get("avg_view_duration_sec", 0) for v in videos if v.get("analytics_fetched")]
        pct_vals = [v.get("avg_view_percentage", 0) for v in videos if v.get("analytics_fetched")]
        avg_duration = sum(dur_vals) / len(dur_vals) if dur_vals else 0
        avg_pct = sum(pct_vals) / len(pct_vals) if pct_vals else 0

    # Top performer
    top = sorted(videos, key=lambda v: v.get("views", 0), reverse=True)[:3]

    mode = "analytics" if has_analytics else "basic"

    insights = {
        "channel_id":       channel_id,
        "mode":             mode,
        "total_videos":     total,
        "avg_views":        round(avg_views, 1),
        "avg_likes":        round(avg_likes, 1),
        "avg_duration_sec": round(avg_duration, 1),
        "avg_view_pct":     round(avg_pct, 1),
        "top_videos":       [{"video_id": v["video_id"], "title": v.get("title", ""),
                               "views": v.get("views", 0)} for v in top],
        "top_topics":       [v.get("topic", v.get("title", "")) for v in top],
        "computed_at":      datetime.now().isoformat(),
    }

    logger.info(
        f"[{channel_id}] Insights ({mode}): {total} videos, "
        f"avg_views={avg_views:.0f}, avg_likes={avg_likes:.0f}"
    )
    return insights


def _empty_insights(channel_id: str) -> dict:
    return {
        "channel_id":       channel_id,
        "mode":             "empty",
        "total_videos":     0,
        "avg_views":        0,
        "avg_likes":        0,
        "avg_duration_sec": 0,
        "avg_view_pct":     0,
        "top_videos":       [],
        "top_topics":       [],
        "computed_at":      datetime.now().isoformat(),
    }


# ─── Topic Hints & Prompt Addon ───────────────────────────────────────────────
def get_topic_hints(channel: dict, n: int = 5) -> list[str]:
    """
    Return list of topik yang terbukti perform bagus untuk channel ini.
    Berguna sebagai seed bagi AI/topic engine.
    """
    channel_id = channel["id"]
    videos = get_videos_for_channel(channel_id)
    if not videos:
        return []

    top = sorted(videos, key=lambda v: v.get("views", 0), reverse=True)[:n]
    return [v.get("topic") or v.get("title", "") for v in top if v.get("topic") or v.get("title")]


def build_prompt_addon(channel: dict) -> str:
    """
    Buat teks tambahan untuk system-prompt script generator
    berdasarkan data performa aktual dari DB.
    """
    channel_id = channel["id"]
    insights = analyze_channel(channel)

    if insights["mode"] == "empty":
        return ""

    top_topics = insights["top_topics"][:3]
    top_str = "\n".join(f"  - {t}" for t in top_topics) if top_topics else "  (belum ada data)"

    mode_note = (
        f"Avg retensi: {insights['avg_view_pct']:.1f}%, "
        f"avg durasi: {insights['avg_duration_sec']:.0f}s"
        if insights["mode"] == "analytics"
        else f"Avg views: {insights['avg_views']:.0f}, avg likes: {insights['avg_likes']:.0f}"
    )

    return (
        f"[Data Performa {channel_id}]\n"
        f"Total video di DB: {insights['total_videos']}\n"
        f"{mode_note}\n"
        f"Topik terlaris:\n{top_str}\n"
        f"Buat konten yang mirip secara tema dan gaya, tapi dengan angle baru."
    )


# ─── Utility: register satu video manual (backward-compat) ────────────────────
def register_video(channel_id: str, video_id: str, topic: str = "", **kwargs):
    """
    Daftarkan satu video secara manual ke video_registry.
    Berguna saat pipeline selesai upload video baru.
    """
    channel_cfg = _get_channel_cfg(channel_id)
    upsert_video({
        "video_id":   video_id,
        "channel_id": channel_id,
        "topic":      topic,
        "niche":      channel_cfg.get("niche", ""),
        "language":   channel_cfg.get("language", ""),
        "source":     "manual_register",
        **kwargs,
    })
    logger.debug(f"[{channel_id}] Registered video {video_id} (topic: {topic!r})")