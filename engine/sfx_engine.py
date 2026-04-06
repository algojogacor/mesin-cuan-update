"""
engine/sfx_engine.py - Sound Effects Engine
Mixes cinematic SFX (whoosh, impact, ambient) ke dalam final audio mix.

Cara pakai:
    from engine import sfx_engine
    mixed = sfx_engine.mix_sfx_to_audio(audio_path, niche, tmp_dir)
"""

import os
import subprocess
import logging
import shutil
import random
from typing import Optional

logger = logging.getLogger("sfx_engine")

# ─── SFX asset paths ──────────────────────────────────────────────────────────
_ASSET_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets", "sfx")

_SFX_FILES = {
    "whoosh":      os.path.join(_ASSET_DIR, "whoosh.mp3"),
    "impact":      os.path.join(_ASSET_DIR, "impact.wav"),
    "dark_ambient": os.path.join(_ASSET_DIR, "dark_ambient.mp3"),
}

# ─── Niche → SFX mapping ─────────────────────────────────────────────────────
# Setiap niche mendapat set SFX yang mendukung mood konten.
_NICHE_SFX: dict[str, list[dict]] = {
    "horror_facts": [
        {"key": "impact",      "volume": 0.35, "at": "start"},   # opening punch
        {"key": "dark_ambient","volume": 0.12, "at": "loop"},     # subtle tension
    ],
    "drama": [
        {"key": "whoosh",      "volume": 0.25, "at": "start"},
        {"key": "dark_ambient","volume": 0.10, "at": "loop"},
    ],
    "psychology": [
        {"key": "whoosh",      "volume": 0.20, "at": "start"},
    ],
    "motivation": [
        {"key": "whoosh",      "volume": 0.28, "at": "start"},
        {"key": "impact",      "volume": 0.22, "at": "start_delay"},  # 0.3s offset
    ],
    "history": [
        {"key": "dark_ambient","volume": 0.08, "at": "loop"},
    ],
    "default": [
        {"key": "whoosh",      "volume": 0.15, "at": "start"},
    ],
}


def _sfx_available(key: str) -> bool:
    """Cek apakah file SFX tersedia dan ukurannya wajar (>5KB)."""
    path = _SFX_FILES.get(key, "")
    return os.path.exists(path) and os.path.getsize(path) > 5 * 1024


def _get_duration(path: str) -> float:
    """Get duration of media file via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1",
             path],
            capture_output=True, text=True, timeout=20
        )
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def mix_sfx_to_audio(audio_path: str, niche: str, tmp_dir: str) -> str:
    """
    Mix cinematic SFX ke narasi audio.

    Workflow:
    1. Ambil daftar SFX untuk niche ini.
    2. Untuk setiap SFX yang tersedia, overlay ke narasi dengan volume rendah.
    3. Return path output (sama folder tmp_dir). Jika gagal, return audio_path asli.

    Args:
        audio_path: Path narasi audio (MP3/WAV).
        niche:      Niche konten (horror_facts, drama, motivation, dll).
        tmp_dir:    Direktori temp untuk output.

    Returns:
        Path ke file audio yang sudah di-mix dengan SFX.
    """
    sfx_list = _NICHE_SFX.get(niche, _NICHE_SFX["default"])
    available = [s for s in sfx_list if _sfx_available(s["key"])]

    if not available:
        logger.info(f"SFX: tidak ada SFX tersedia untuk niche={niche}, skip")
        return audio_path

    out_path = os.path.join(tmp_dir, "sfx_mixed.mp3")
    main_dur = _get_duration(audio_path)
    if main_dur <= 0:
        logger.warning("SFX: durasi audio utama 0, skip SFX mix")
        return audio_path

    # Build amix filter step by step
    # Strategy: chain setiap SFX sebagai overlay via FFmpeg amix/adelay/atrim
    try:
        return _mix_sfx_ffmpeg(audio_path, available, main_dur, out_path)
    except Exception as e:
        logger.warning(f"SFX mix gagal ({e}), return audio asli")
        return audio_path


def _mix_sfx_ffmpeg(narasi: str, sfx_list: list[dict], main_dur: float, out_path: str) -> str:
    """
    Mix SFX ke narasi menggunakan FFmpeg amix.

    Setiap SFX di-apply:
    - 'start'       : mulai dari t=0
    - 'start_delay' : mulai dari t=0.3s
    - 'loop'        : loop sepanjang durasi narasi (volume lebih rendah)
    """
    inputs = ["-i", narasi]
    filter_parts = []
    n_inputs = 1  # index 0 = narasi

    for sfx in sfx_list:
        key     = sfx["key"]
        vol     = sfx.get("volume", 0.15)
        timing  = sfx.get("at", "start")
        sfx_file = _SFX_FILES[key]

        inputs += ["-i", sfx_file]
        sfx_idx = n_inputs
        n_inputs += 1

        label = f"sfx{sfx_idx}"

        if timing == "loop":
            # Loop + trim ke durasi narasi + volume adjust
            filter_parts.append(
                f"[{sfx_idx}]aloop=loop=-1:size=2e+09,atrim=0:{main_dur:.3f},"
                f"volume={vol:.3f}[{label}]"
            )
        elif timing == "start_delay":
            # Delay 300ms + volume
            filter_parts.append(
                f"[{sfx_idx}]adelay=300|300,volume={vol:.3f}[{label}]"
            )
        else:
            # start: dari t=0
            filter_parts.append(
                f"[{sfx_idx}]volume={vol:.3f}[{label}]"
            )

    # Mix semua stream: [0] narasi + semua SFX
    sfx_labels = "".join(f"[sfx{i+1}]" for i in range(len(sfx_list)))
    n_mix = 1 + len(sfx_list)

    filter_complex = (
        ";".join(filter_parts) +
        f";[0]{sfx_labels}amix=inputs={n_mix}:duration=first:dropout_transition=2,"
        f"alimiter=level_in=1:level_out=0.95:limit=0.9:attack=7:release=100,aresample=44100[mixout]"
    )

    cmd = (
        ["ffmpeg", "-y"] + inputs +
        ["-filter_complex", filter_complex,
         "-map", "[mixout]",
         "-c:a", "libmp3lame", "-q:a", "2",
         "-t", f"{main_dur:.3f}",
         out_path]
    )

    logger.info(f"SFX mix: {len(sfx_list)} track(s) → {os.path.basename(out_path)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        err = (result.stderr or "")[-400:]
        raise RuntimeError(f"FFmpeg SFX mix error: {err}")

    out_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
    if out_size < 10 * 1024:
        raise RuntimeError(f"SFX output terlalu kecil ({out_size} bytes)")

    logger.info(f"✓ SFX mix OK: {out_size // 1024} KB")
    return out_path
