"""
upload_engine.py - Upload video ke YouTube
Setiap channel pakai Google Cloud Project sendiri (quota terpisah)
Ada delay antar upload untuk menghindari burst
"""

import os
import json
import time
from datetime import datetime
from engine.utils import get_logger, load_settings
from engine import state_manager

# ── BARU: Import register_video ──────────────────────────────────────────────
from engine.retention_engine import register_video

logger = get_logger("upload_engine")

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# Track waktu upload terakhir (global, semua channel)
_last_upload_time: float = 0.0


def upload(video_path: str, thumbnail_path: str, metadata: dict, channel: dict, **kwargs) -> str:
    ch_id      = channel["id"]
    cred_file  = channel.get("credentials_file", "")
    token_file = cred_file.replace("_token.json", "_token_token.pickle")

    if not os.path.exists(token_file):
        raise FileNotFoundError(
            f"[{ch_id}] Token tidak ditemukan: {token_file}\n"
            f"Jalankan 'python setup_auth.py' untuk generate token."
        )

    # ── Jeda antar upload (global, semua channel) ──────────────────────────
    _wait_before_upload()

    logger.info(f"[{ch_id}] Uploading: {metadata['title']}")

    youtube   = _get_youtube_client(cred_file)
    video_id  = _upload_video(youtube, video_path, metadata, channel)
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    if thumbnail_path and os.path.exists(thumbnail_path):
        _set_thumbnail(youtube, video_id, thumbnail_path, ch_id)

    state_manager.increment_upload_count(ch_id)
    _record_upload_time()

    logger.info(f"[{ch_id}] ✅ Upload sukses: {video_url}")

    # ── BARU: Daftarkan video untuk tracking retention ───────────────────────
    try:
        script_data = kwargs.get("script_data", {})  # pastikan script_data dipass ke upload()
        register_video(channel, video_id, script_data)
    except Exception as e:
        logger.warning(f"[{ch_id}] register_video gagal (non-fatal): {e}")

    return video_url


def can_upload_today(channel: dict) -> bool:
    ch_id     = channel["id"]
    quota     = channel.get("upload_quota_per_day", 6)
    count     = state_manager.get_upload_count_today(ch_id)
    remaining = quota - count

    if remaining <= 0:
        logger.warning(f"[{ch_id}] Quota hari ini habis ({count}/{quota})")
        return False

    logger.info(f"[{ch_id}] Quota: {count}/{quota} dipakai, sisa {remaining}")
    return True


# ─── Private Helpers ──────────────────────────────────────────────────────────

def _wait_before_upload():
    """Jeda antar upload agar tidak burst. Baca dari settings."""
    global _last_upload_time
    settings = load_settings()
    delay    = settings.get("upload", {}).get("delay_between_uploads_sec", 45)

    if _last_upload_time > 0:
        elapsed = time.time() - _last_upload_time
        if elapsed < delay:
            wait = delay - elapsed
            logger.info(f"Jeda {wait:.0f}s sebelum upload berikutnya...")
            time.sleep(wait)


def _record_upload_time():
    global _last_upload_time
    _last_upload_time = time.time()


def _get_youtube_client(cred_file: str):
    """
    Load OAuth token untuk channel ini.
    Token file: cred_file ganti _token.json → _token_token.pickle
    """
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    import pickle

    creds      = None
    token_file = cred_file.replace("_token.json", "_token_token.pickle")

    if os.path.exists(token_file):
        with open(token_file, "rb") as f:
            creds = pickle.load(f)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            with open(token_file, "wb") as f:
                pickle.dump(creds, f)
        except Exception as e:
            logger.warning(f"Token refresh failed: {e}")
            creds = None

    if not creds:
        raise PermissionError(
            "Token tidak valid atau belum dibuat. "
            "Jalankan 'python setup_auth.py' untuk generate ulang."
        )

    return build("youtube", "v3", credentials=creds)


def _upload_video(youtube, video_path: str, metadata: dict, channel: dict) -> str:
    from googleapiclient.http import MediaFileUpload

    settings    = load_settings()
    upload_conf = settings.get("upload", {})
    ai_conf     = settings.get("ai_disclosure", {})
    language    = channel.get("language", "id")
    publish_at  = metadata.get("publish_at")
    privacy     = "private" if publish_at else upload_conf.get("privacy", "public")

    body = {
        "snippet": {
            "title":                metadata["title"],
            "description":          metadata["description"],
            "tags":                 metadata.get("tags", []),
            "categoryId":           metadata.get("category_id", upload_conf.get("category_id", "27")),
            "defaultLanguage":      language,
            "defaultAudioLanguage": language,
        },
        "status": {
            "privacyStatus":           privacy,
            "selfDeclaredMadeForKids": upload_conf.get("youtube_made_for_kids", False),
            "embeddable":              upload_conf.get("embeddable", True),
            "publicStatsViewable":     upload_conf.get("public_stats_viewable", True),
            "license":                 upload_conf.get("license", "youtube"),
            "containsSyntheticMedia":  (
                ai_conf.get("has_synthetic_audio", True) or
                ai_conf.get("has_altered_visual", True)
            ),
        },
        "recordingDetails": {
            "recordingDate": datetime.utcnow().strftime("%Y-%m-%dT00:00:00.000Z")
        }
    }

    if publish_at:
        body["status"]["publishAt"] = publish_at
        logger.info(f"Dijadwalkan publish: {publish_at}")

    media   = MediaFileUpload(video_path, mimetype="video/mp4", resumable=True, chunksize=1024*1024*5)
    request = youtube.videos().insert(
        part="snippet,status,recordingDetails", body=body, media_body=media
    )

    response  = None
    attempt   = 0
    max_retry = 3

    while response is None:
        try:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                logger.debug(f"Upload progress: {pct}%")
        except Exception as e:
            attempt += 1
            if attempt >= max_retry:
                raise RuntimeError(f"Upload gagal setelah {max_retry} percobaan: {e}")
            wait = 2 ** attempt
            logger.warning(f"Upload error (attempt {attempt}), retry in {wait}s: {e}")
            time.sleep(wait)

    return response["id"]


def _set_thumbnail(youtube, video_id: str, thumbnail_path: str, ch_id: str):
    from googleapiclient.http import MediaFileUpload
    try:
        media = MediaFileUpload(thumbnail_path, mimetype="image/png")
        youtube.thumbnails().set(videoId=video_id, media_body=media).execute()
        logger.info(f"[{ch_id}] Thumbnail set: {video_id}")
    except Exception as e:
        logger.warning(f"[{ch_id}] Set thumbnail gagal (non-fatal): {e}")