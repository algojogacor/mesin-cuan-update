"""
footage_engine.py — Mesin Cuan Viral Architect
Download footage dinamis dari multi-source.

Source priority: Pixabay → Pexels → Coverr
Cache lokal: keyword → paths (hemat API call)
Fallback pool: stok lokal per niche (tidak pernah kosong)

.env variables yang dibutuhkan:
  PIXABAY_API_KEY   — https://pixabay.com/api/docs/
  PEXELS_API_KEY    — https://www.pexels.com/api/
  COVERR_API_KEY    — ec7836795048a65862b3d83fb53ab15a
  COVERR_APP_ID     — 9A5D5E43C12102FD5B8E

CHANGELOG v3:
  - visual_cues dari script_engine sekarang dibaca dan di-parse jadi search query
  - _parse_visual_cue_to_query() membersihkan format cue → query bersih untuk API
  - Shorts: visual_cues → keywords AI → keyword bank (prioritas berurutan)
  - Long Form: visual_cues dibagi ke intro/segmen/outro sesuai urutan
  - Coverr ditambahkan sebagai source ke-3 dengan API key dari .env
  - Keyword bank per niche 15+ keywords (English) — hasil footage lebih relevan
  - Fallback pool lokal per niche — setup via setup_fallback_pool()
  - Orientation portrait untuk Shorts, landscape untuk Long Form

CHANGELOG v3.1 (fix):
  - _parse_visual_cue_to_query() diperbaiki untuk handle format baru dari script_engine:
      Format lama (expected): "dark corridor — at line about victims"
      Format baru (actual):   "at 'bodies bearing injuries' → show dark forest"
    Sekarang parser bisa handle kedua format sekaligus.
"""

import os
import re
import time
import json
import requests
import random
from pathlib import Path
from engine.utils import get_logger, require_env, load_settings, timestamp, channel_data_path

logger = get_logger("footage_engine")

# ── Request Config ─────────────────────────────────────────────────────────────
MAX_DOWNLOAD_RETRIES = 3    # Retry download jika gagal
DOWNLOAD_RETRY_DELAY = 5    # Jeda antar retry (detik)
SEARCH_TIMEOUT       = 20   # Timeout API search (detik)
DOWNLOAD_TIMEOUT     = 120  # Timeout download file (detik)
CLIP_CACHE_MAX_KEYS  = 500  # Maks entry keyword cache (FIFO cleanup)
USED_CLIPS_MAX       = 1000 # Maks ID clip yang disimpan sebagai "sudah dipakai"
SHORTS_MIN_CLIPS     = 10   # Shorts butuh lebih banyak opsi agar pacing visual tetap rapat

# ── Fallback Pool ──────────────────────────────────────────────────────────────
FALLBACK_POOL_MIN_COUNT = 15  # Minimal clips per niche di fallback pool lokal

# ── Keyword Bank Per Niche ─────────────────────────────────────────────────────
# Semua keyword dalam English untuk hasil terbaik di Pixabay/Pexels/Coverr.
# Di-shuffle setiap fetch agar tidak repetitif antar video.
NICHE_KEYWORD_BANK: dict = {
    "horror_facts": [
        "dark forest night fog",       "abandoned house exterior",
        "old cemetery night",          "dark corridor creepy",
        "shadow silhouette mystery",   "lightning storm night",
        "candle flame dark room",      "misty lake at night",
        "haunted mansion exterior",    "dark water reflection",
        "moonlight through dark trees","rain dark empty street",
        "underground tunnel dark",     "cracked wall darkness",
        "fog covered forest path",
    ],
    "drama": [
        "rain window dramatic close",  "silhouette sunset dramatic",
        "dark ocean waves crashing",   "empty road fog dramatic",
        "person walking alone night",  "dark cloudy sky timelapse",
    ],
    "crime": [
        "dark alley night urban",      "city lights rain reflection",
        "empty warehouse industrial",  "shadowy figure walking night",
        "surveillance camera view",    "dark parking lot night",
    ],
    "psychology": [
        "person thinking alone window","brain neurons firing abstract",
        "meditation calm nature",      "silhouette mountain top success",
        "clock time passing timelapse","person studying library calm",
        "abstract blue mind concept",  "sunrise over mountain",
        "focus work desk minimal",     "self reflection water ripple",
        "person walking city morning", "writing notes notebook",
        "light rays through window",   "calm breathing nature",
        "two people handshake trust",
    ],
    "motivation": [
        "sunrise mountain achievement","person running dawn road",
        "athlete training hard gym",   "confident walk city street",
        "goal target arrow bullseye",  "team collaboration office",
        "businessman skyline success", "road forward horizon sunrise",
        "stars sky night inspiration", "writing goals journal",
        "winner podium celebration",   "growth upward momentum",
        "aerial city dawn timelapse",  "motivational nature light",
        "crowd cheering celebration",
    ],
    "science": [
        "laboratory science equipment","dna helix blue abstract",
        "space galaxy stars nebula",   "microscope research closeup",
        "data visualization hologram", "futuristic technology blue",
    ],
    "finance": [
        "stock market trading screen", "businessman city skyline",
        "gold bars wealth concept",    "modern office finance",
        "economic growth upward",      "digital data finance",
    ],
    "lifestyle": [
        "healthy breakfast morning",   "yoga outdoor sunrise",
        "coffee morning cafe cozy",    "travel adventure nature",
        "friends laughing outdoor",    "city morning walk",
    ],
    "history": [
        "ancient ruins dramatic sky",  "old parchment map closeup",
        "historical stone architecture","vintage monochrome cityscape",
        "dramatic war memorial",       "old library books dust",
    ],
    "nature": [
        "waterfall forest lush green", "aerial ocean waves drone",
        "mountain golden hour sunrise","flower blooming timelapse",
        "wildlife animals nature",     "green forest rays of light",
    ],
    "default": [
        "cinematic landscape dramatic", "abstract motion blur light",
        "aerial city timelapse dusk",   "dark dramatic storm sky",
        "particle light effects dark",  "light rays dark background",
    ],
}


def _get_niche_keywords(niche: str, count: int = 5) -> list:
    """Ambil keyword dari bank per niche, di-shuffle untuk variasi."""
    niche_lower = niche.lower()
    bank = None
    for key in NICHE_KEYWORD_BANK:
        if key in niche_lower:
            bank = NICHE_KEYWORD_BANK[key]
            break
    if bank is None:
        bank = NICHE_KEYWORD_BANK["default"]
    shuffled = bank.copy()
    random.shuffle(shuffled)
    return shuffled[:count]


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def fetch(script_data: dict, channel: dict, profile: str = "shorts") -> any:
    """Entry point. Return list paths (shorts) atau dict {intro,segments,outro} (long_form)."""
    ch_id = channel["id"]
    niche = channel.get("niche", "default")
    if profile == "long_form":
        return _fetch_long_form(script_data, ch_id, niche)
    return _fetch_shorts(script_data, ch_id, niche, profile)


def _fetch_shorts(script_data: dict, ch_id: str, niche: str, profile: str) -> list:
    """
    Fetch footage untuk Shorts — list flat clips.

    Sumber keyword (prioritas berurutan):
    1. visual_cues dari script AI — paling spesifik & relevan per video
    2. keywords dari script AI — sudah English, generic tapi relevan
    3. Keyword bank per niche — fallback jika AI tidak hasilkan cukup keyword
    """
    keywords: list = []

    for query in (
        _extract_visual_beat_queries(script_data, "opening")
        + _extract_visual_beat_queries(script_data, "middle")
        + _extract_visual_beat_queries(script_data, "ending")
    ):
        if query and query not in keywords:
            keywords.append(query)

    # ── Sumber 1: visual_cues (PRIORITAS — paling spesifik per video) ─────────
    raw_visual_cues = script_data.get("visual_cues", [])
    if isinstance(raw_visual_cues, list):
        for cue in raw_visual_cues:
            if not isinstance(cue, str):
                continue
            parsed = _parse_visual_cue_to_query(cue)
            if parsed:
                keywords.append(parsed)
        if keywords:
            logger.debug(f"[{ch_id}] visual_cues → {len(keywords)} queries: {keywords}")

    # ── Sumber 2: keywords dari script AI ────────────────────────────────────
    ai_keywords = script_data.get("keywords", [])
    if isinstance(ai_keywords, str):
        ai_keywords = [ai_keywords]
    for kw in ai_keywords:
        if kw and kw not in keywords:
            keywords.append(kw)

    # ── Sumber 3: keyword bank jika masih kurang dari 3 ──────────────────────
    if len(keywords) < 3:
        bank_kws = _get_niche_keywords(niche, count=4)
        keywords += [k for k in bank_kws if k not in keywords]

    n_clips = max(load_settings().get("footage", {}).get("clips_per_video", 7), SHORTS_MIN_CLIPS)
    logger.info(
        f"[{ch_id}] Shorts fetch {n_clips} clips | niche={niche} | "
        f"visual_cues={len(raw_visual_cues)} | total_kw={len(keywords)} | "
        f"top3={keywords[:3]}"
    )
    return _download_clips(keywords, n_clips, ch_id, niche, profile)


def _fetch_long_form(script_data: dict, ch_id: str, niche: str) -> dict:
    """
    Fetch footage untuk Long Form — dict per segmen.
    """
    settings      = load_settings()
    clips_per_seg = settings.get("video_profiles", {}).get("long_form", {}).get("clips_per_segment", 3)
    result        = {"intro": [], "segments": [], "outro": []}
    segments      = script_data.get("segments", [])

    raw_visual_cues  = script_data.get("visual_cues", [])
    parsed_cue_queries: list = []
    if isinstance(raw_visual_cues, list):
        for cue in raw_visual_cues:
            q = _parse_visual_cue_to_query(cue)
            if q:
                parsed_cue_queries.append(q)

    intro_kw = (
        parsed_cue_queries[0] if parsed_cue_queries
        else script_data.get("visual_keywords", {}).get("intro")
        or script_data.get("title", "dark mystery cinematic")
    )
    logger.info(f"[{ch_id}] Intro footage: [{intro_kw}]")
    result["intro"] = _download_clips([intro_kw], clips_per_seg, ch_id, niche, "long_form")

    for i, seg in enumerate(segments):
        cue_query = (parsed_cue_queries[i + 1]
                     if i + 1 < len(parsed_cue_queries) else None)

        seg_kw = (cue_query
                  or seg.get("visual_keyword")
                  or seg.get("keywords_footage")
                  or seg.get("judul")
                  or seg.get("poin")
                  or "mystery cinematic")

        kw_list = [seg_kw] if isinstance(seg_kw, str) else seg_kw
        if len(kw_list) < 2:
            kw_list += _get_niche_keywords(niche, count=2)

        logger.info(f"[{ch_id}] Segmen {i+1}: [{kw_list[0]}]")
        result["segments"].append(
            _download_clips(kw_list, clips_per_seg, ch_id, niche, "long_form")
        )
        time.sleep(0.5)

    outro_kw = (
        parsed_cue_queries[-1] if len(parsed_cue_queries) > len(segments)
        else script_data.get("visual_keywords", {}).get("outro")
        or "cinematic end dark dramatic"
    )
    logger.info(f"[{ch_id}] Outro footage: [{outro_kw}]")
    result["outro"] = _download_clips([outro_kw], clips_per_seg - 1, ch_id, niche, "long_form")

    total = (len(result["intro"])
             + sum(len(s) for s in result["segments"])
             + len(result["outro"]))
    logger.info(f"[{ch_id}] Long-form total: {total} clips")
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  VISUAL CUE PARSER
# ══════════════════════════════════════════════════════════════════════════════

def _parse_visual_cue_to_query(cue: str) -> str:
    """
    Konversi satu visual_cue dari script_engine ke search query yang bersih.

    Mendukung DUA format output dari script_engine:

    FORMAT LAMA (expected):
      "dark hospital corridor, red glitch overlay — at line about victims"
      "abandoned hospital, flickering light — at line about experiments"
      "[ENGLISH ONLY] dark water reflection, moonlight — closing shot"
      → Ambil bagian SEBELUM " — "

    FORMAT BARU (actual dari log):
      "at 'bodies bearing injuries' → show dark forest night"
      "at 'tent was ripped open' → wide shot snowy mountain"
      "at 'state secret' → show government building dark"
      → Ambil bagian SETELAH " → "

    Jika kedua format tidak cocok, coba ekstrak kata bermakna yang ada.
    """
    if not isinstance(cue, str) or not cue.strip():
        return ""

    text = cue.strip()

    # ── Step 1: Hapus prefix [ENGLISH ONLY] atau [...] ────────────────────────
    text = re.sub(r'^\[.*?\]\s*', '', text)

    # ── Step 2: Deteksi dan handle FORMAT BARU: "at '...' → description" ──────
    # Contoh: "at 'bodies bearing injuries' → show dark forest night"
    # Yang kita mau: bagian setelah → yaitu "show dark forest night"
    if "→" in text:
        after_arrow = text.split("→", 1)[1].strip()
        if after_arrow:
            # Hapus kata pengantar yang tidak bermakna untuk stock search
            after_arrow = re.sub(r'^(show|display|cut to|reveal|use|play)\s+', '', after_arrow, flags=re.IGNORECASE)
            text = after_arrow
        else:
            # Arrow ada tapi tidak ada konten sesudahnya → return kosong,
            # biar caller fall back ke keyword bank
            return ""

    # ── Step 3: Handle FORMAT LAMA: ambil sebelum " — " ─────────────────────
    elif " — " in text:
        text = text.split(" — ")[0]

    # ── Step 4: Jika masih diawali "at '" (format baru tanpa arrow) → skip ───
    # Contoh: "at 'tent was ripped open" (tanpa →)
    # Tidak ada deskripsi visual yang bisa dipakai → return kosong
    elif re.match(r"^at\s+'", text, re.IGNORECASE):
        return ""

    # ── Step 5: Ambil 2 segmen koma pertama sebagai query ────────────────────
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) >= 2 and len(parts[0].split()) >= 2:
        text = f"{parts[0]} {parts[1]}"
    elif parts:
        text = parts[0]

    # ── Step 6: Hapus kata-kata teknis efek (tidak relevan untuk stock API) ───
    EFFECT_WORDS_TO_REMOVE = [
        "glitch", "overlay", "effect", "slow motion", "zoom in", "zoom out",
        "fade", "transition", "desaturated", "closeup", "close-up", "close up",
        "wide shot", "tracking shot", "sfx", "sound", "silence", "music",
        "audio", "voiceover", "cut to", "reveal", "show", "display",
    ]
    text_lower = text.lower()
    for word in EFFECT_WORDS_TO_REMOVE:
        text_lower = text_lower.replace(word, " ")

    # ── Step 7: Bersihkan spasi ganda ────────────────────────────────────────
    text_clean = " ".join(text_lower.split())

    # ── Step 8: Sanitasi keyword Indonesia ───────────────────────────────────
    text_clean = _sanitize_keyword(text_clean)
    if not text_clean:
        return ""

    # ── Step 9: Ambil max 5 kata ──────────────────────────────────────────────
    words = text_clean.split()
    final_query = " ".join(words[:5])

    return final_query if len(final_query) > 3 else ""


def _extract_visual_beat_queries(script_data: dict, section: str) -> list:
    """Ambil query dari schema visual_beats sambil menjaga urutan editorial."""
    visual_beats = script_data.get("visual_beats", {})
    if not isinstance(visual_beats, dict):
        return []

    beats = visual_beats.get(section, [])
    if isinstance(beats, str):
        beats = [beats]
    if not isinstance(beats, list):
        return []

    queries: list = []
    for beat in beats:
        if isinstance(beat, dict):
            text = beat.get("cue") or beat.get("text") or beat.get("visual")
        else:
            text = beat
        query = _parse_visual_cue_to_query(text)
        if query and query not in queries:
            queries.append(query)
    return queries


# ══════════════════════════════════════════════════════════════════════════════
#  FALLBACK POOL
# ══════════════════════════════════════════════════════════════════════════════

def setup_fallback_pool(ch_id: str, niche: str, min_count: int = FALLBACK_POOL_MIN_COUNT):
    """
    Isi fallback pool lokal dengan minimal min_count footage per niche.
    Panggil sekali saat setup channel baru, atau tambahkan ke scheduler mingguan.
    """
    fallback_dir = _get_fallback_dir(ch_id, niche)
    fallback_dir.mkdir(parents=True, exist_ok=True)
    existing = list(fallback_dir.glob("*.mp4"))

    if len(existing) >= min_count:
        logger.info(f"[{ch_id}] Fallback pool '{niche}': {len(existing)} files — OK")
        return

    needed   = min_count - len(existing)
    keywords = _get_niche_keywords(niche, count=6)
    logger.info(f"[{ch_id}] Mengisi fallback pool '{niche}': butuh {needed} clips")

    for kw in keywords:
        if needed <= 0:
            break
        for clip in _search_all_sources(kw, needed + 3, "long_form"):
            if needed <= 0:
                break
            if not clip.get("url"):
                continue
            out_path = str(fallback_dir / f"{timestamp()}.mp4")
            if _download_video(clip["url"], out_path):
                needed -= 1
                logger.info(f"[{ch_id}] Fallback +1: {os.path.basename(out_path)}")
                time.sleep(0.3)

    logger.info(f"[{ch_id}] Fallback pool '{niche}' selesai")


def _get_fallback_dir(ch_id: str, niche: str) -> Path:
    """Return Path ke fallback pool directory per niche."""
    base = Path(channel_data_path(ch_id, "footage")) / "fallback"
    niche_lower = niche.lower()
    for key in NICHE_KEYWORD_BANK:
        if key in niche_lower:
            return base / key
    return base / "default"


def _get_fallback_clips(ch_id: str, niche: str, count: int) -> list:
    """Ambil clips dari fallback pool lokal (random sample)."""
    fallback_dir = _get_fallback_dir(ch_id, niche)
    if not fallback_dir.exists():
        return []
    all_files = list(fallback_dir.glob("*.mp4"))
    if not all_files:
        return []
    random.shuffle(all_files)
    selected = all_files[:count]
    logger.info(f"[{ch_id}] Fallback pool: {len(selected)} clips dari {fallback_dir.name}/")
    return [str(f) for f in selected]


# ══════════════════════════════════════════════════════════════════════════════
#  DOWNLOADER CORE
# ══════════════════════════════════════════════════════════════════════════════

# Kata Indonesia yang tidak boleh masuk ke query API (hasil jadi tidak relevan)
_INDONESIAN_STOPWORDS = {
    "ruang", "eksperimen", "otak", "bercahaya", "efek", "distorsi",
    "neuron", "saat", "pada", "ketika", "dengan", "untuk", "dari",
    "dalam", "dan", "yang", "ini", "itu", "ke", "di", "saya", "aku",
    "koridor", "kosong", "berkedip", "bayangan", "orang", "gelap",
    "menyebabkan", "mencekam", "menakutkan", "horor", "misteri",
    "suasana", "kegelapan", "penonton", "sebuah", "adalah", "akan",
}


def _sanitize_keyword(keyword: str) -> str:
    """
    Hapus keyword yang mengandung kata Indonesia.
    Return string kosong jika terdeteksi — caller akan skip keyword ini.
    """
    if not keyword:
        return ""
    words       = keyword.lower().split()
    id_detected = [w for w in words if w in _INDONESIAN_STOPWORDS]
    if id_detected:
        logger.warning(f"⚠ Keyword Indonesia terdeteksi: '{keyword}' → skip")
        return ""
    return keyword


def _download_clips(keywords: list, n: int, ch_id: str,
                    niche: str = "default", profile: str = "shorts") -> list:
    """
    Download N clips dari multi-source berdasarkan keyword list.
    Urutan prioritas: keyword cache → Pixabay → Pexels → Coverr → fallback pool
    """
    used_clip_ids = _load_used_clips(ch_id)
    out_dir       = channel_data_path(ch_id, "footage")
    clip_cache    = _load_clip_cache(ch_id)
    downloaded    = []

    clean_keywords = []
    for kw in keywords:
        if not kw:
            continue
        kw = kw.replace('"', '').replace("'", "").strip()
        kw = _sanitize_keyword(kw)
        if kw:
            clean_keywords.append(kw)

    for keyword in clean_keywords:
        if len(downloaded) >= n:
            break

        cache_key    = keyword.lower().strip()
        cached_paths = [p for p in clip_cache.get(cache_key, []) if os.path.exists(p)]
        if cached_paths:
            need = n - len(downloaded)
            reuse = cached_paths[:need]
            logger.info(f"[{ch_id}] Cache '{keyword}': {len(reuse)} clips reused")
            downloaded.extend(reuse)
            continue

        search_results = _search_all_sources(keyword, n + 5, profile)
        random.shuffle(search_results)

        for clip in search_results:
            if len(downloaded) >= n:
                break
            clip_id = str(clip.get("id", ""))
            if not clip.get("url") or clip_id in used_clip_ids:
                continue

            out_path = f"{out_dir}/{timestamp()}.mp4"
            if _download_video(clip["url"], out_path):
                downloaded.append(out_path)
                used_clip_ids.add(clip_id)
                clip_cache.setdefault(cache_key, []).append(out_path)
                time.sleep(0.2)

    if len(downloaded) < n:
        fallback = _get_fallback_clips(ch_id, niche, n - len(downloaded))
        if fallback:
            downloaded.extend(fallback)
            logger.info(f"[{ch_id}] Fallback pool: +{len(fallback)} clips")

    _save_used_clips(ch_id, used_clip_ids)
    _save_clip_cache(ch_id, clip_cache)
    return downloaded


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-SOURCE SEARCH
# ══════════════════════════════════════════════════════════════════════════════

def _search_all_sources(keyword: str, limit: int, profile: str = "shorts") -> list:
    """
    Cari footage dari semua source secara berurutan:
    Pixabay (utama) → Pexels (fallback) → Coverr (fallback ke-2)
    """
    orientation = "portrait" if profile == "shorts" else "landscape"
    results     = []

    results += _search_pixabay(keyword, limit, orientation)

    if len(results) < 2:
        logger.info(f"Pixabay kurang ({len(results)}) → Pexels")
        results += _search_pexels(keyword, limit, orientation)

    if len(results) < 2:
        logger.info(f"Pexels juga kurang → Coverr")
        results += _search_coverr(keyword, limit)

    return results


def _search_pixabay(keyword: str, limit: int, orientation: str = "landscape") -> list:
    """Search Pixabay Videos API."""
    try:
        api_key    = require_env("PIXABAY_API_KEY")
        pix_orient = "vertical" if orientation == "portrait" else "horizontal"

        resp = requests.get(
            "https://pixabay.com/api/videos/",
            params={
                "key":         api_key,
                "q":           keyword,
                "per_page":    min(limit, 20),
                "video_type":  "all",
                "orientation": pix_orient,
                "lang":        "en",
            },
            timeout=SEARCH_TIMEOUT
        )
        resp.raise_for_status()
        hits   = resp.json().get("hits", [])
        result = []
        for video in hits:
            videos_dict = video.get("videos", {})
            url = (videos_dict.get("large", {}).get("url")
                   or videos_dict.get("medium", {}).get("url")
                   or videos_dict.get("small", {}).get("url"))
            if url:
                result.append({"id": f"pb_{video['id']}", "url": url})

        logger.debug(f"Pixabay '{keyword}' ({pix_orient}): {len(result)} clips")
        return result
    except Exception as e:
        logger.warning(f"Pixabay gagal '{keyword}': {e}")
        return []


def _search_pexels(keyword: str, limit: int, orientation: str = "landscape") -> list:
    """Search Pexels Videos API."""
    try:
        api_key = require_env("PEXELS_API_KEY")
        resp    = requests.get(
            "https://api.pexels.com/videos/search",
            headers={"Authorization": api_key},
            params={
                "query":       keyword,
                "per_page":    limit,
                "size":        "medium",
                "orientation": orientation,
            },
            timeout=SEARCH_TIMEOUT
        )
        resp.raise_for_status()
        videos = resp.json().get("videos", [])
        result = []
        for video in videos:
            files = video.get("video_files", [])
            mp4   = next(
                (f["link"] for f in files if f.get("width", 0) >= 1280),
                None
            )
            if mp4:
                result.append({"id": f"px_{video['id']}", "url": mp4})

        logger.debug(f"Pexels '{keyword}' ({orientation}): {len(result)} clips")
        return result
    except Exception as e:
        logger.warning(f"Pexels gagal '{keyword}': {e}")
        return []


def _search_coverr(keyword: str, limit: int) -> list:
    """Search Coverr.co Videos API."""
    try:
        api_key = os.environ.get("COVERR_API_KEY", "")
        app_id  = os.environ.get("COVERR_APP_ID", "")

        headers = {}
        params  = {"query": keyword, "count": limit}

        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        if app_id:
            params["app_id"] = app_id

        resp = requests.get(
            "https://api.coverr.co/videos",
            headers=headers,
            params=params,
            timeout=SEARCH_TIMEOUT
        )

        if resp.status_code != 200:
            logger.debug(f"Coverr '{keyword}': HTTP {resp.status_code}")
            return []

        hits   = resp.json().get("hits", [])
        result = []
        for video in hits:
            url = video.get("mp4") or video.get("url")
            if url:
                vid_id = video.get("id", hash(url))
                result.append({"id": f"cv_{vid_id}", "url": url})

        logger.debug(f"Coverr '{keyword}': {len(result)} clips")
        return result
    except Exception as e:
        logger.warning(f"Coverr gagal '{keyword}': {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════════

def _download_video(url: str, out_path: str) -> bool:
    """Download satu video file dengan retry. Return True jika berhasil."""
    for attempt in range(MAX_DOWNLOAD_RETRIES):
        try:
            resp = requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT)
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
            return True
        except Exception as e:
            if os.path.exists(out_path):
                os.remove(out_path)
            logger.warning(f"Download attempt {attempt+1}/{MAX_DOWNLOAD_RETRIES} gagal: {e}")
            time.sleep(DOWNLOAD_RETRY_DELAY)
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  PERSISTENCE — Cache & Used Clips
# ══════════════════════════════════════════════════════════════════════════════

def _clip_cache_path(ch_id: str) -> str:
    return f"data/{ch_id}/topics/clip_cache.json"


def _load_clip_cache(ch_id: str) -> dict:
    """Load keyword → paths cache dari disk."""
    path = _clip_cache_path(ch_id)
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_clip_cache(ch_id: str, cache: dict):
    """Simpan keyword cache ke disk. Trim jika melebihi CLIP_CACHE_MAX_KEYS."""
    path = _clip_cache_path(ch_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if len(cache) > CLIP_CACHE_MAX_KEYS:
        keys  = list(cache.keys())
        cache = {k: cache[k] for k in keys[-CLIP_CACHE_MAX_KEYS:]}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def _load_used_clips(ch_id: str) -> set:
    """Load set ID clip yang sudah pernah dipakai (hindari duplikat antar video)."""
    path = f"data/{ch_id}/topics/used_clips.json"
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def _save_used_clips(ch_id: str, used: set):
    """Simpan used clips. Trim ke USED_CLIPS_MAX entry terbaru."""
    path = f"data/{ch_id}/topics/used_clips.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(used)[-USED_CLIPS_MAX:], f)
