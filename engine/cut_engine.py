"""
cut_engine.py - Potong clips dari video source berdasarkan timestamps

Input  : list of clip dicts dari gemini_engine.analyze()
Output : list of clip dicts + field "clip_path" (path ke file hasil potongan)

Behavior:
  - GPU encode (h264_nvenc) → otomatis fallback ke CPU (libx264) kalau gagal
  - Pakai -ss sebelum -i (fast seek) untuk performa, bukan -ss sesudah
  - Tiap clip disimpan sebagai file .mp4 terpisah di tmp_dir
"""

import os
import subprocess
from engine.utils import get_logger

logger = get_logger("cut_engine")

# Encode config (sama pattern seperti video_engine.py)
GPU_ENCODE = ["-c:v", "h264_nvenc", "-preset", "p4", "-cq", "24"]
CPU_ENCODE = ["-c:v", "libx264", "-crf", "23", "-preset", "fast"]
USE_GPU    = True  # Set False kalau tidak ada NVIDIA GPU


# ─── Public ───────────────────────────────────────────────────────────────────

def cut_clips(clips: list, tmp_dir: str) -> list:
    """
    Potong semua clips dari source video masing-masing.

    Args:
        clips   : list of clip dicts (harus ada: source, start_sec, duration)
        tmp_dir : folder untuk menyimpan hasil potongan sementara

    Returns:
        list of clip dicts dengan tambahan field "clip_path"
        (clip yang gagal dipotong tidak dimasukkan ke result)
    """
    os.makedirs(tmp_dir, exist_ok=True)
    result = []
    total  = len(clips)

    for i, clip in enumerate(clips):
        src      = clip["source"]
        start    = clip["start_sec"]
        duration = clip["duration"]
        out_path = os.path.join(tmp_dir, f"clip_{i:03d}.mp4")

        if not os.path.exists(src):
            logger.error(f"[cut] ❌ Source tidak ditemukan: {src}")
            continue

        logger.info(
            f"[cut] [{i+1}/{total}] {os.path.basename(src)} "
            f"[{clip['start']} → {clip['end']}] ({duration:.1f}s)"
        )

        try:
            _ffmpeg_cut(src, start, duration, out_path)
            result.append({**clip, "clip_path": out_path})
            logger.info(f"[cut] ✅ Saved: {os.path.basename(out_path)}")
        except Exception as e:
            logger.error(f"[cut] ❌ Gagal potong clip {i+1}: {e}")

    logger.info(f"[cut] Selesai: {len(result)}/{total} clips berhasil dipotong")
    return result


# ─── FFmpeg Cut ───────────────────────────────────────────────────────────────

def _ffmpeg_cut(src: str, start_sec: float, duration: float, out_path: str):
    """
    Potong satu clip dengan FFmpeg.
    -ss sebelum -i = fast seek (tidak perlu decode dari awal)
    GPU → CPU fallback otomatis.
    """
    global USE_GPU
    encode = GPU_ENCODE if USE_GPU else CPU_ENCODE

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start_sec),          # fast seek (sebelum -i)
        "-i", src,
        "-t", str(duration),
        *encode,
        "-c:a", "aac", "-b:a", "192k",
        "-avoid_negative_ts", "make_zero",
        "-movflags", "+faststart",
        out_path
    ]

    try:
        _run_ffmpeg(cmd)
    except subprocess.CalledProcessError as e:
        if USE_GPU:
            logger.warning(f"[cut] GPU encode gagal, fallback ke CPU...")
            cmd_cpu = [
                "ffmpeg", "-y",
                "-ss", str(start_sec),
                "-i", src,
                "-t", str(duration),
                *CPU_ENCODE,
                "-c:a", "aac", "-b:a", "192k",
                "-avoid_negative_ts", "make_zero",
                "-movflags", "+faststart",
                out_path
            ]
            _run_ffmpeg(cmd_cpu)
        else:
            raise


def _run_ffmpeg(cmd: list):
    """Jalankan FFmpeg command, raise CalledProcessError jika gagal."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        # Log stderr untuk debugging
        logger.debug(f"[cut] FFmpeg stderr:\n{result.stderr[-500:]}")
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stderr)


# ─── Utility ──────────────────────────────────────────────────────────────────

def get_video_duration(path: str) -> float:
    """Ambil durasi video dalam detik via FFprobe."""
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


def validate_clips_against_source(clips: list) -> list:
    """
    Validasi clips: pastikan timestamp tidak melebihi durasi source video.
    Return clips yang valid saja.
    """
    validated     = []
    duration_cache = {}

    for clip in clips:
        src = clip["source"]
        if src not in duration_cache:
            duration_cache[src] = get_video_duration(src)
        src_duration = duration_cache[src]

        if src_duration > 0 and clip["end_sec"] > src_duration:
            logger.warning(
                f"[cut] Clip trimmed: end {clip['end_sec']:.1f}s > video duration {src_duration:.1f}s"
            )
            clip = {
                **clip,
                "end_sec" : src_duration,
                "duration": round(src_duration - clip["start_sec"], 2)
            }
            if clip["duration"] < 1:
                logger.warning(f"[cut] Clip skip: durasi < 1s setelah trim")
                continue

        validated.append(clip)

    return validated