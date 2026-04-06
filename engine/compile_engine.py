"""
compile_engine.py - Gabungkan clips menjadi satu video final

Fitur:
  - Normalize semua clip ke target aspect ratio (crop & scale, tidak letterbox)
  - Concat dengan FFmpeg concat demuxer (paling reliable)
  - Support transisi: "cut" (langsung) | "fade" (fade-to-black antar clip)
  - Target aspect: 9:16 (Shorts/TikTok) | 16:9 (YouTube) | 1:1 (Instagram)
  - GPU → CPU fallback otomatis
"""

import os
import subprocess
import shutil
from engine.utils import get_logger

logger = get_logger("compile_engine")

GPU_ENCODE = ["-c:v", "h264_nvenc", "-preset", "p4", "-cq", "24"]
CPU_ENCODE = ["-c:v", "libx264", "-crf", "23", "-preset", "fast"]
USE_GPU    = True

# Preset dimensi
ASPECT_PRESETS = {
    "9:16" : (1080, 1920),
    "16:9" : (1920, 1080),
    "1:1"  : (1080, 1080),
}


# ─── Public ───────────────────────────────────────────────────────────────────

def compile_clips(clips: list, out_path: str, options: dict = {}) -> str:
    """
    Gabungkan clips menjadi satu video final.

    Args:
        clips    : list of clip dicts dari cut_engine (harus ada "clip_path")
        out_path : path output video final
        options  : {
            "aspect"    : "9:16" | "16:9" | "1:1"  (default: "9:16")
            "transition": "cut" | "fade"             (default: "cut")
            "fps"       : int                        (default: 30)
            "width"     : int (override)
            "height"    : int (override)
        }

    Returns:
        out_path jika berhasil
    """
    aspect     = options.get("aspect", "9:16")
    transition = options.get("transition", "cut")
    fps        = options.get("fps", 30)

    # Tentukan dimensi
    default_w, default_h = ASPECT_PRESETS.get(aspect, (1080, 1920))
    w = options.get("width", default_w)
    h = options.get("height", default_h)

    # Filter clips yang valid
    valid_clips = [
        c for c in clips
        if "clip_path" in c and os.path.exists(c["clip_path"])
    ]

    if not valid_clips:
        raise ValueError("[compile] Tidak ada clip valid untuk dikompilasi!")

    logger.info(
        f"[compile] Compile {len(valid_clips)} clips | "
        f"aspect={aspect} ({w}x{h}) | transition={transition}"
    )

    tmp_dir = os.path.dirname(out_path)
    os.makedirs(tmp_dir, exist_ok=True)

    # Step 1: Normalize tiap clip ke target dimensi
    normalized_paths = []
    for i, clip in enumerate(valid_clips):
        norm_path = os.path.join(tmp_dir, f"norm_{i:03d}.mp4")
        logger.info(f"[compile] Normalizing clip {i+1}/{len(valid_clips)}...")
        _normalize_clip(clip["clip_path"], norm_path, w, h, fps)
        normalized_paths.append(norm_path)

    # Step 2: Concat
    if len(normalized_paths) == 1:
        shutil.copy(normalized_paths[0], out_path)
    else:
        if transition == "fade":
            _concat_with_fade(normalized_paths, out_path)
        else:
            _concat_direct(normalized_paths, out_path)

    # Cleanup normalized tmp files
    for p in normalized_paths:
        try:
            os.remove(p)
        except OSError:
            pass

    logger.info(f"[compile] ✅ Output: {out_path}")
    return out_path


# ─── Normalize Clip ───────────────────────────────────────────────────────────

def _normalize_clip(src: str, out: str, w: int, h: int, fps: int):
    """
    Resize + crop clip ke target dimensi.
    Pakai scale-to-cover + center-crop (tidak ada letterbox/pillarbox).
    Audio di-normalize ke sample rate 44100Hz stereo.
    """
    # scale ke ukuran yang COVER target (bukan fit), lalu crop center
    vf = (
        f"scale={w}:{h}:force_original_aspect_ratio=increase,"
        f"crop={w}:{h},"
        f"fps={fps}"
    )

    encode = GPU_ENCODE if USE_GPU else CPU_ENCODE

    cmd = [
        "ffmpeg", "-y", "-i", src,
        "-vf", vf,
        *encode,
        "-c:a", "aac", "-b:a", "192k",
        "-ar", "44100", "-ac", "2",      # stereo 44100Hz — penting untuk concat
        "-movflags", "+faststart",
        out
    ]

    try:
        _run(cmd)
    except subprocess.CalledProcessError:
        if USE_GPU:
            logger.warning("[compile] GPU normalize gagal, fallback ke CPU...")
            cmd[cmd.index(encode[0]):cmd.index(encode[0]) + len(encode)] = CPU_ENCODE
            _run(cmd)
        else:
            raise


# ─── Concat Methods ───────────────────────────────────────────────────────────

def _concat_direct(paths: list, out_path: str):
    """
    Concat langsung dengan FFmpeg concat demuxer.
    Paling cepat karena tidak perlu re-encode (stream copy).
    Semua input harus sudah dinormalize ke codec/resolution yang sama.
    """
    list_file = out_path + "_concat.txt"
    try:
        with open(list_file, "w", encoding="utf-8") as f:
            for p in paths:
                f.write(f"file '{os.path.abspath(p)}'\n")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", list_file,
            "-c", "copy",               # stream copy — sudah dinormalize
            "-movflags", "+faststart",
            out_path
        ]
        _run(cmd)
    finally:
        if os.path.exists(list_file):
            os.remove(list_file)


def _concat_with_fade(paths: list, out_path: str):
    """
    Concat dengan fade-to-black antar clip menggunakan filter_complex.
    Lebih berat dari direct concat tapi hasil lebih smooth.
    Fade duration: 0.3 detik tiap transisi.
    """
    FADE_DUR = 0.3

    # Build filter_complex untuk fade antar setiap clip
    n        = len(paths)
    inputs   = " ".join(f"-i \"{p}\"" for p in paths)
    filters  = []
    streams  = []

    for i in range(n):
        # Fade out di akhir, fade in di awal (kecuali clip pertama & terakhir)
        fade_in  = f"[{i}:v]fade=t=in:st=0:d={FADE_DUR}[fi{i}]"
        fade_out = f"[fi{i}]fade=t=out:st=0:d={FADE_DUR}[fo{i}]"   # placeholder, akan dihitung ulang
        filters.append(fade_in)
        streams.append(f"[fo{i}]")

    # Kalau terlalu kompleks, fallback ke direct concat
    # (fade via filter_complex bisa crash untuk banyak clips)
    if n > 10:
        logger.warning("[compile] Terlalu banyak clips untuk fade, fallback ke cut transition")
        _concat_direct(paths, out_path)
        return

    # Build simple xfade chain
    filter_parts = []
    prev_v = "[0:v]"
    prev_a = "[0:a]"

    for i in range(1, n):
        curr_v = f"[{i}:v]"
        curr_a = f"[{i}:a]"
        out_v  = f"[v{i}]" if i < n - 1 else "[vout]"
        out_a  = f"[a{i}]" if i < n - 1 else "[aout]"

        filter_parts.append(
            f"{prev_v}{curr_v}xfade=transition=fade:duration={FADE_DUR}:offset=0{out_v}"
        )
        filter_parts.append(
            f"{prev_a}{curr_a}acrossfade=d={FADE_DUR}{out_a}"
        )

        prev_v = out_v
        prev_a = out_a

    filter_complex = ";".join(filter_parts)

    input_args = []
    for p in paths:
        input_args.extend(["-i", p])

    encode = GPU_ENCODE if USE_GPU else CPU_ENCODE

    cmd = [
        "ffmpeg", "-y",
        *input_args,
        "-filter_complex", filter_complex,
        "-map", "[vout]",
        "-map", "[aout]",
        *encode,
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        out_path
    ]

    try:
        _run(cmd)
    except subprocess.CalledProcessError as e:
        logger.warning(f"[compile] Fade concat gagal ({e}), fallback ke direct concat")
        _concat_direct(paths, out_path)


# ─── Audio Mixing ─────────────────────────────────────────────────────────────

def mix_background_music(video_path: str, music_path: str, out_path: str,
                          music_volume: float = 0.12, original_volume: float = 1.0):
    """
    Mix video dengan background music.
    Default: original audio penuh, musik di volume 12%.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-i", music_path,
        "-filter_complex",
        (
            f"[0:a]volume={original_volume}[orig];"
            f"[1:a]volume={music_volume},aloop=loop=-1:size=2e+09[music];"
            f"[orig][music]amix=inputs=2:duration=first:dropout_transition=3[aout]"
        ),
        "-map", "0:v",
        "-map", "[aout]",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k",
        "-shortest",
        out_path
    ]
    _run(cmd)
    logger.info(f"[compile] ✅ Music mixed: {out_path}")


def mix_music_replace(video_path: str, music_path: str, out_path: str,
                       music_volume: float = 0.9, original_volume: float = 0.15):
    """
    Mix untuk mode cinematic: musik dominan, audio original pelan.
    """
    mix_background_music(video_path, music_path, out_path, music_volume, original_volume)


# ─── FFmpeg Runner ────────────────────────────────────────────────────────────

def _run(cmd: list):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.debug(f"[compile] FFmpeg error:\n{result.stderr[-800:]}")
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stderr)