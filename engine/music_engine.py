"""
music_engine.py - Download background music dari Freesound API
Fallback chain: Local folder → Freesound → Silent track

SETUP LOCAL MUSIC (recommended):
  Taruh file .mp3 di folder assets/music/<mood>/
  Contoh:
    assets/music/dark ambient/track1.mp3
    assets/music/horror tension/track2.mp3
  Kalau ada file lokal → langsung pakai, tidak perlu internet.
  Mood yang dipakai: dark ambient, horror tension, suspense,
                     calm introspective, mysterious, soft ambient
"""

import os
import time
import random
import requests
from engine.utils import get_logger, channel_data_path

logger = get_logger("music_engine")

# Folder musik lokal (taruh MP3 di sini, dibagi per mood)
LOCAL_MUSIC_ROOT = "assets/music"

# Mapping mood → query Freesound (dari spesifik ke umum sebagai fallback)
MOOD_QUERIES = {
    "dark ambient":       ["dark ambient background",      "dark cinematic ambient",    "dark atmosphere"],
    "horror tension":     ["horror tension suspense",      "horror background music",   "suspense thriller"],
    "suspense":           ["suspense thriller background", "tension music background",  "dramatic suspense"],
    "dark mystery":       ["dark mystery cinematic",       "mystery ambient dark",      "dark background"],
    "horror climax":      ["horror intense dark",          "horror climax music",       "dark intense"],
    "dark intro":         ["dark cinematic intro",         "dark intro music",          "cinematic dark"],
    "calm introspective": ["calm ambient meditation",      "soft ambient calm",         "peaceful background"],
    "thoughtful ambient": ["thoughtful ambient soft",      "soft background ambient",   "calm ambient"],
    "mysterious":         ["mysterious ambient",           "mystery background music",  "ambient mysterious"],
    "dark psychological": ["dark psychological ambient",   "psychological thriller",    "dark ambient"],
    "intense focus":      ["intense focus concentration",  "focus background music",    "intense ambient"],
    "soft ambient":       ["soft ambient calm",            "gentle background music",   "soft background"],
}

# Cache in-memory: mood → path (supaya 1 run tidak download berkali-kali)
_music_cache: dict[str, str] = {}


def fetch(mood: str, duration_sec: int, channel_id: str) -> str:
    """
    Ambil 1 track musik sesuai mood dan durasi minimal.
    Urutan: in-memory cache → local folder → Freesound → silent track
    Return: path ke file .mp3/.mp3
    """
    # 1. In-memory cache (1 session = 1 download per mood)
    if mood in _music_cache and os.path.exists(_music_cache[mood]):
        logger.info(f"Music cache hit: {mood}")
        return _music_cache[mood]

    # 2. Local music folder (prioritas utama — tidak butuh internet)
    local_path = _get_local_music(mood)
    if local_path:
        logger.info(f"Music dari lokal: {local_path}")
        _music_cache[mood] = local_path
        return local_path

    # 3. Freesound API
    out_dir  = channel_data_path(channel_id, "music")
    safe_mood = mood.replace(" ", "_").replace("/", "_")
    out_path  = f"{out_dir}/{safe_mood}.mp3"  # ← FIX: tidak pakai timestamp() biar bisa cache

    # Kalau sudah ada di disk dari run sebelumnya, pakai langsung
    if os.path.exists(out_path) and os.path.getsize(out_path) > 10_000:
        logger.info(f"Music dari disk cache: {out_path}")
        _music_cache[mood] = out_path
        return out_path

    freesound_path = _fetch_from_freesound(mood, duration_sec, out_path)
    if freesound_path:
        _music_cache[mood] = freesound_path
        return freesound_path

    # 4. Silent track sebagai last resort
    logger.warning(f"Semua sumber musik gagal untuk '{mood}' — pakai silent track")
    silent_path = _create_silent_track(out_path, duration_sec)
    return silent_path


# ─── Local music ──────────────────────────────────────────────────────────────

def _get_local_music(mood: str) -> str | None:
    """
    Cari file musik di assets/music/<mood>/ atau assets/music/ (fallback).
    Pilih random dari file yang ada.
    """
    # Coba folder spesifik mood dulu
    mood_dir = os.path.join(LOCAL_MUSIC_ROOT, mood)
    if os.path.isdir(mood_dir):
        files = [f for f in os.listdir(mood_dir) if f.lower().endswith((".mp3", ".wav", ".ogg"))]
        if files:
            return os.path.join(mood_dir, random.choice(files))

    # Fallback: cari semua file di root assets/music/
    if os.path.isdir(LOCAL_MUSIC_ROOT):
        all_files = []
        for root, _, files in os.walk(LOCAL_MUSIC_ROOT):
            for f in files:
                if f.lower().endswith((".mp3", ".wav", ".ogg")):
                    all_files.append(os.path.join(root, f))
        if all_files:
            logger.info(f"Mood '{mood}' tidak ada di lokal, pakai random dari {LOCAL_MUSIC_ROOT}")
            return random.choice(all_files)

    return None


# ─── Freesound ────────────────────────────────────────────────────────────────

def _fetch_from_freesound(mood: str, duration_sec: int, out_path: str) -> str | None:
    api_key = os.environ.get("FREESOUND_API_KEY")
    if not api_key:
        logger.warning("FREESOUND_API_KEY tidak ada — skip Freesound")
        return None

    queries = MOOD_QUERIES.get(mood, [f"{mood} background", "ambient background"])

    for query in queries:
        # Coba dengan filter lisensi dulu, lalu tanpa filter kalau gagal
        for use_license_filter in [True, False]:
            track = _search_freesound(api_key, query, duration_sec, use_license_filter)
            if track:
                logger.info(f"Freesound found: '{track['name']}' (query: '{query}')")
                download_url = _get_preview_url(api_key, track["id"])
                if download_url:
                    success = _download_file(download_url, api_key, out_path)
                    if success:
                        logger.info(f"Music downloaded dari Freesound: {out_path}")
                        return out_path
            time.sleep(1)

    logger.warning(f"Freesound tidak menemukan track untuk mood '{mood}'")
    return None


def _search_freesound(api_key: str, query: str, min_duration: int,
                      use_license_filter: bool = True) -> dict | None:
    url = "https://freesound.org/apiv2/search/text/"

    # Filter: dengan atau tanpa filter lisensi
    if use_license_filter:
        filter_str = f'duration:[{min_duration} TO *] license:"Creative Commons 0"'
    else:
        # Tanpa filter lisensi — lebih banyak hasil, tapi mungkin CC-BY
        filter_str = f"duration:[{min_duration} TO *]"

    params = {
        "query":     query,
        "filter":    filter_str,
        "fields":    "id,name,duration,license,previews",
        "sort":      "rating_desc",
        "page_size": 10,
        "token":     api_key,
    }

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                logger.warning("Freesound rate limit, tunggu 30s...")
                time.sleep(30)
                continue
            if resp.status_code == 401:
                logger.error("Freesound API key tidak valid")
                return None
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                return results[0]
            return None  # Query valid tapi 0 hasil
        except requests.exceptions.Timeout:
            logger.warning(f"Freesound timeout (attempt {attempt+1}/3)")
            time.sleep(5)
        except Exception as e:
            logger.warning(f"Freesound search error (attempt {attempt+1}/3): {e}")
            time.sleep(5)

    return None


def _get_preview_url(api_key: str, sound_id: int) -> str | None:
    """Ambil URL preview HQ MP3 (tidak butuh OAuth, langsung download)."""
    url = f"https://freesound.org/apiv2/sounds/{sound_id}/"
    try:
        resp = requests.get(url, params={"token": api_key}, timeout=15)
        resp.raise_for_status()
        data     = resp.json()
        previews = data.get("previews", {})
        # Preview HQ tidak butuh OAuth — lebih reliable dari direct download
        return (
            previews.get("preview-hq-mp3") or
            previews.get("preview-lq-mp3") or
            data.get("download")
        )
    except Exception as e:
        logger.warning(f"Freesound get sound error: {e}")
        return None


def _download_file(url: str, api_key: str, out_path: str) -> bool:
    """Download file audio ke disk dengan retry 3x."""
    headers = {"Authorization": f"Token {api_key}"}

    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, stream=True, timeout=60)
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            # Validasi file tidak kosong
            if os.path.getsize(out_path) < 10_000:
                logger.warning("File musik terlalu kecil, kemungkinan corrupt")
                os.remove(out_path)
                return False
            return True
        except Exception as e:
            if os.path.exists(out_path):
                os.remove(out_path)
            if attempt < 2:
                logger.warning(f"Download music gagal (attempt {attempt+1}/3), retry: {e}")
                time.sleep(10)
            else:
                logger.warning(f"Download music gagal setelah 3x: {e}")

    return False


# ─── Silent track fallback ────────────────────────────────────────────────────

def _create_silent_track(out_path: str, duration_sec: int) -> str:
    """Buat silent audio sebagai last-resort fallback."""
    import subprocess
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo",
        "-t", str(duration_sec),
        "-q:a", "9", "-acodec", "libmp3lame",
        out_path
    ]
    subprocess.run(cmd, capture_output=True)
    logger.info(f"Silent track created: {out_path}")
    return out_path