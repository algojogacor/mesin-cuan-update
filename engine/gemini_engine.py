"""
gemini_engine.py - Analisis video via Gemini File API
SDK: google-genai (bukan google.generativeai yang sudah deprecated)

Install: pip install google-genai
"""

import os
import re
import time
import json
import subprocess
import concurrent.futures
from google import genai
from google.genai import types
from engine.utils import get_logger, require_env

logger = get_logger("gemini_engine")

VALID_MODES      = ["tiktok", "podcast", "cinematic"]
DEFAULT_MODEL    = "gemini-2.5-flash"
MAX_DURATION_SEC = 1800   # 30 menit — kalau lebih, otomatis split jadi chunks
CHUNK_SEC        = 1500   # tiap chunk 25 menit


# ─── Public Entry Point ────────────────────────────────────────────────────────

def analyze(video_paths: list, mode: str, options: dict = {}) -> list:
    api_key = require_env("GEMINI_API_KEY")
    client  = genai.Client(api_key=api_key)

    model_name    = options.get("model", DEFAULT_MODEL)
    min_per_video = options.get("min_clips_per_video", 2)
    max_total     = options.get("max_total_clips", 10)

    logger.info(f"[gemini] Mulai analisis {len(video_paths)} video | mode={mode} | model={model_name}")

    all_clips = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_upload_and_analyze, client, path, mode, model_name, min_per_video): path
            for path in video_paths
        }
        for future in concurrent.futures.as_completed(futures):
            path = futures[future]
            try:
                clips = future.result()
                logger.info(f"[gemini] ✅ {os.path.basename(path)}: {len(clips)} clips ditemukan")
                all_clips.extend(clips)
            except Exception as e:
                logger.error(f"[gemini] ❌ {os.path.basename(path)} gagal: {e}")

    if not all_clips:
        logger.warning("[gemini] Tidak ada clips yang berhasil dianalisis!")
        return []

    all_clips.sort(key=lambda x: x.get("score", 0), reverse=True)
    if max_total:
        all_clips = all_clips[:max_total]
    all_clips.sort(key=lambda x: (x["source"], x["start_sec"]))

    logger.info(f"[gemini] Total clips terpilih: {len(all_clips)}")
    return all_clips


# ─── Upload & Analyze ─────────────────────────────────────────────────────────

def _upload_and_analyze(client, video_path: str, mode: str, model_name: str, min_clips: int) -> list:
    filename     = os.path.basename(video_path)
    duration_sec = _get_video_duration(video_path)
    duration_str = _sec_to_ts(duration_sec)
    logger.info(f"[gemini] [{filename}] Durasi: {duration_str} ({duration_sec:.1f}s)")

    # Video terlalu panjang -> split jadi chunks
    if duration_sec > MAX_DURATION_SEC:
        logger.info(f"[gemini] [{filename}] Video terlalu panjang, split jadi chunks {CHUNK_SEC//60} menit...")
        return _analyze_in_chunks(client, video_path, mode, model_name, min_clips, duration_sec)

    logger.info(f"[gemini] [{filename}] ACTIVE — analisis via helper")
    clips = _upload_file_and_analyze(client, video_path, mode, model_name, min_clips, duration_sec, duration_str)
    logger.info(f"[gemini] [{filename}] Parsed: {len(clips)} clips valid")
    return clips


# ─── Prompt ───────────────────────────────────────────────────────────────────

def _load_prompt(mode: str, min_clips: int, duration_sec: float, duration_str: str) -> str:
    template_path = f"templates/prompts/edit_{mode}.txt"
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            content = f.read()
        content = content.replace("{{MIN_CLIPS}}", str(min_clips))
        content = content.replace("{{DURATION_STR}}", duration_str)
        content = content.replace("{{DURATION_SEC}}", str(int(duration_sec)))
        return content

    logger.warning(f"[gemini] Template tidak ditemukan: {template_path} — pakai default")
    return _default_prompt(mode, min_clips, duration_sec, duration_str)


def _default_prompt(mode: str, min_clips: int, duration_sec: float, duration_str: str) -> str:
    base = f"""Analisis video ini secara menyeluruh.
Durasi video: {duration_str} ({int(duration_sec)} detik total).

Temukan {min_clips} hingga 8 momen paling menarik dan engaging.

PENTING — Format timestamp:
- Gunakan format HH:MM:SS (jam:menit:detik)
- Contoh untuk detik ke-45: 00:00:45
- Contoh untuk menit ke-2 detik ke-30: 00:02:30
- Semua timestamp HARUS dalam rentang 00:00:00 sampai {duration_str}
- JANGAN memberikan timestamp yang melebihi durasi video!

Return HANYA JSON valid berikut. Jangan ada teks lain, jangan ada markdown backtick:
{{
  "clips": [
    {{
      "start": "HH:MM:SS",
      "end": "HH:MM:SS",
      "score": 9.5,
      "reason": "Kenapa momen ini menarik (1-2 kalimat)",
      "hook": "Caption pendek max 10 kata"
    }}
  ]
}}

Rules wajib:
- score: 1-10 (10 = paling engaging)
- Durasi tiap clip: minimum 5 detik, maksimum 60 detik
- Jangan overlap antar clip
- Urutkan dari score tertinggi ke terendah
"""
    mode_hints = {
        "tiktok"   : "Fokus: momen lucu, mengejutkan, reaksi kuat, twist, fakta wow, dramatis — yang layak viral di TikTok/Shorts.",
        "podcast"  : "Fokus: kutipan powerful yang bisa berdiri sendiri, insight kunci, statement kontroversial, puncak storytelling.",
        "cinematic": "Fokus: shot visual indah, aksi dinamis, adegan emosional, komposisi stunning, momen sinematik impactful."
    }
    return base + "\n" + mode_hints.get(mode, "")


# ─── Response Parser ──────────────────────────────────────────────────────────

def _parse_response(text: str, source_path: str, video_duration: float = 0) -> list:
    cleaned = re.sub(r"```(?:json)?|```", "", text).strip()

    data = None
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r'\{[\s\S]*\}', cleaned)
        if match:
            try:
                data = json.loads(match.group())
            except json.JSONDecodeError:
                pass

    if not data:
        logger.error(f"[gemini] Gagal parse JSON:\n{text[:500]}")
        return []

    raw_clips = data.get("clips", [])
    result    = []

    for i, clip in enumerate(raw_clips):
        try:
            start_sec = _ts_to_sec(clip["start"])
            end_sec   = _ts_to_sec(clip["end"])

            # Auto-fix: kalau timestamp dalam satuan menit (Gemini salah format)
            # Deteksi: kalau end_sec > durasi video tapi end_sec/60 <= durasi
            if video_duration > 0 and end_sec > video_duration:
                # Coba interpretasikan sebagai MM:SS bukan HH:MM:SS
                start_fixed = _reinterpret_as_mmss(clip["start"])
                end_fixed   = _reinterpret_as_mmss(clip["end"])
                if end_fixed <= video_duration and end_fixed > start_fixed:
                    logger.info(
                        f"[gemini] Clip {i+1} auto-fix timestamp: "
                        f"{clip['start']}→{clip['end']} (HH:MM:SS) "
                        f"→ {_sec_to_ts(start_fixed)}→{_sec_to_ts(end_fixed)} (MM:SS reinterpreted)"
                    )
                    start_sec = start_fixed
                    end_sec   = end_fixed
                else:
                    logger.warning(f"[gemini] Clip {i+1} skip: timestamp {clip['start']}→{clip['end']} melebihi durasi video {_sec_to_ts(video_duration)}")
                    continue

            duration = round(end_sec - start_sec, 2)

            if end_sec <= start_sec:
                logger.warning(f"[gemini] Clip {i+1} skip: end <= start")
                continue
            if duration < 3:
                logger.warning(f"[gemini] Clip {i+1} skip: durasi terlalu pendek ({duration}s)")
                continue

            result.append({
                "source"    : source_path,
                "start"     : _sec_to_ts(start_sec),
                "end"       : _sec_to_ts(end_sec),
                "start_sec" : start_sec,
                "end_sec"   : end_sec,
                "duration"  : duration,
                "score"     : float(clip.get("score", 5.0)),
                "reason"    : clip.get("reason", ""),
                "hook"      : clip.get("hook", ""),
            })
        except (KeyError, ValueError) as e:
            logger.warning(f"[gemini] Clip {i+1} error: {e} — {clip}")

    return result


def _reinterpret_as_mmss(ts: str) -> float:
    """
    Reinterpret timestamp yang ditulis sebagai HH:MM:SS tapi sebenarnya MM:SS:frame
    Contoh: '00:17:00' → Gemini maksudnya '17 detik' bukan '17 menit'
    Coba: ambil bagian MM dan SS dari HH:MM:SS → jadikan total detik
    """
    parts = ts.strip().split(":")
    if len(parts) == 3:
        # '00:17:00' → h=0, m=17, s=0 → mungkin maksudnya 17 detik
        # Interpretasi: total = m detik + s/100 (m sebagai detik, s sebagai sub)
        # Atau lebih simpel: ambil total detik dari MM saja
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        # Kalau h=0 dan nilai m kecil (<= 59), kemungkinan m = detik sebenarnya
        if h == 0:
            return float(m) + float(s) / 60.0  # m menit → m detik
        return float(h) * 60 + float(m) + float(s) / 60.0
    return _ts_to_sec(ts)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _ts_to_sec(ts: str) -> float:
    ts    = ts.strip()
    parts = ts.split(":")
    try:
        if len(parts) == 3:
            h, m, s = parts
            return float(h) * 3600 + float(m) * 60 + float(s)
        elif len(parts) == 2:
            m, s = parts
            return float(m) * 60 + float(s)
        else:
            return float(parts[0])
    except ValueError:
        raise ValueError(f"Format timestamp tidak valid: '{ts}'")


def _sec_to_ts(sec: float) -> str:
    sec = int(sec)
    h   = sec // 3600
    m   = (sec % 3600) // 60
    s   = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _get_video_duration(path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return float(result.stdout.strip())
    except ValueError:
        return 0.0


def _guess_mime(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    return {
        ".mp4" : "video/mp4",
        ".mov" : "video/quicktime",
        ".avi" : "video/x-msvideo",
        ".mkv" : "video/x-matroska",
        ".webm": "video/webm",
        ".m4v" : "video/mp4",
    }.get(ext, "video/mp4")


# backward compat
def sec_to_ts(sec: float) -> str:
    return _sec_to_ts(sec)