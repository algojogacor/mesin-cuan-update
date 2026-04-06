"""
video_engine.py — Mesin Cuan Viral Architect
Render video dari footage + audio + subtitle
Support: Shorts (1080x1920) | Long Form (1920x1080)

Hardware Target: RTX 3050 4GB VRAM | Intel i7-13H (12T) | RAM 16GB
Strategi:
  - Encode  : NVENC h264 (hemat VRAM, cepat, tidak blokir RAM)
  - Whisper : CPU int8 small (VRAM 4GB terlalu sempit untuk Whisper medium+)
  - Zoompan : CPU libavfilter (GPU filter FFmpeg tidak stabil di VRAM kecil)
  - Thread  : 8 thread optimal (i7-13H = 8P-core + 4E-core, sisakan 4 untuk OS)
  - RAM     : buffer intermediate max 2GB (aman di 16GB)

CHANGELOG v5 (Clean Code + Hardware-Tuned):
  - Semua hardcoded value → CONFIG BLOCK terpusat dengan komentar
  - Emoji dihapus
  - Rainbow color cycling dihapus → highlight konsisten per niche
  - Vignette terpusat via _vignette_filter()
  - Subtitle margin diperbaiki: Shorts 18%, Long Form 14%
  - Dead code dihapus, pipeline dioptimasi
"""

import os
import json
import subprocess
import shutil
import random
from engine.utils import get_logger, load_settings, timestamp, channel_data_path

logger = get_logger("video_engine")


# ══════════════════════════════════════════════════════════════════════════════
#  CENTRALIZED CONFIGURATION
#  Edit di sini — perubahan otomatis berlaku ke seluruh pipeline
# ══════════════════════════════════════════════════════════════════════════════

# ── Hardware: Encoding ────────────────────────────────────────────────────────
# NVENC: encode di GPU, bebaskan CPU untuk filter lain (zoompan, blur)
# -cq 26 lebih ringan dari 24, kualitas masih bagus untuk YouTube
# -preset p3 lebih cepat dari p4, cocok untuk VRAM terbatas (4GB)
GPU_ENCODE_PARAMS = [
    "-c:v", "h264_nvenc",
    "-preset", "p3",        # p3=faster (vs p4=slow) — optimal RTX 3050
    "-cq", "26",            # Quality factor: 0=best, 51=worst. 26 = bagus+cepat
    "-b:v", "0",            # Biarkan CQ yang kontrol bitrate (VBR mode)
    "-rc", "vbr",           # Variable bitrate — lebih efisien untuk YouTube
    "-threads", "8",        # Sisakan 4 thread untuk OS & Whisper background
]
CPU_ENCODE_PARAMS = [
    "-c:v", "libx264",
    "-crf", "23",           # CRF 23 = default libx264, kualitas bagus
    "-preset", "fast",      # fast = tradeoff kecepatan vs ukuran file
    "-threads", "8",        # Sama dengan GPU, konsisten
]
USE_GPU = True              # False = paksa CPU (untuk debug atau saat VRAM penuh)

# ── Hardware: Whisper Transcription ───────────────────────────────────────────
# small = ~244MB RAM, akurasi bagus untuk ID/EN — JANGAN pakai medium di 16GB
# karena medium ~1.5GB RAM + VRAM, konflik dengan NVENC yang butuh VRAM
WHISPER_MODEL_SIZE   = "small"  # "tiny"(cepat,kurang akurat) | "small"(optimal) | "medium"(berat)
WHISPER_DEVICE       = "cpu"    # Tetap CPU — simpan VRAM 4GB untuk NVENC
WHISPER_COMPUTE_TYPE = "int8"   # int8 = RAM ~244MB vs float16 ~488MB, akurasi mirip
WHISPER_MIN_SEG_DUR  = 1.5      # Segmen < 1.5 detik digabung (kurangi jumlah FFmpeg calls)

# ── Hardware: FFmpeg General ──────────────────────────────────────────────────
FFMPEG_THREAD_COUNT  = 8        # i7-13H punya 12 thread, sisakan 4 untuk sistem
FFMPEG_BUFFER_SIZE   = "32M"    # Buffer I/O — aman untuk 16GB RAM
FFMPEG_MAX_MUXING_QUEUE = 1024  # Cegah buffer overflow saat mux audio+video panjang

# ── Ken Burns Zoom ────────────────────────────────────────────────────────────
# Zoom dilakukan di CPU (libavfilter) — lebih stabil dari GPU filter di VRAM kecil
KEN_BURNS_MIN_DUR_SEC = 0.3     # Skip segmen lebih pendek dari ini (hemat waktu render)
KEN_BURNS_ZOOM_DIRS   = [       # (x_expr, y_expr, zoom_start, zoom_end)
    ("iw/2-(iw/zoom/2)", "ih/2-(ih/zoom/2)", "1.0",  "1.06"),  # Center → zoom in
    ("0",                "0",                 "1.06", "1.0"),   # Top-left → zoom out
    ("iw-(iw/zoom)",     "ih/2-(ih/zoom/2)", "1.0",  "1.05"),  # Right → zoom in
    ("iw/2-(iw/zoom/2)", "ih-(ih/zoom)",      "1.05", "1.0"),   # Bottom → zoom out
]

# ── Blurred Background ────────────────────────────────────────────────────────
BG_BLUR_RADIUS       = 25       # Radius boxblur — 25 cukup cinematic, tidak terlalu berat
BG_BLUR_POWER        = 8        # Iterasi blur — jangan ubah tanpa tes
FG_ZOOM_SCALE_FACTOR = 2        # Scale foreground 2x sebelum zoompan (jaga kualitas)

# ── Subtitle ──────────────────────────────────────────────────────────────────
SUB_FONT_NAME            = "Arial"  # Font — pastikan ada di sistem (fallback aman)
SUB_FONT_BOLD            = -1       # -1=Bold, 0=Regular (format ASS)
SUB_FONT_SIZE_SHORTS     = 72       # px — cukup besar untuk mobile 1080x1920
SUB_FONT_SIZE_LONGFORM   = 52       # px — proporsional untuk 1920x1080
SUB_OUTLINE_THICKNESS    = 3        # Tebal outline hitam — keterbacaan di semua background
SUB_SHADOW_DEPTH         = 1        # Kedalaman shadow
SUB_ACTIVE_SIZE_FACTOR   = 1.12     # Kata aktif 12% lebih besar dari pasif
SUB_BOUNCE_PEAK_PCT      = 0.40     # Puncak scale bounce di 40% durasi kata
SUB_BOUNCE_SCALE_PEAK    = 115      # Scale % saat bounce puncak (100=normal)
SUB_BOUNCE_SCALE_SETTLE  = 110      # Scale % setelah bounce settle
SUB_MARGIN_L             = 20       # Margin kiri subtitle (px)
SUB_MARGIN_R             = 20       # Margin kanan subtitle (px)
# margin_v = jarak dari BAWAH layar — lihat NICHE_SUB_CONFIG

# ── Subtitle Margin Per Profile ────────────────────────────────────────────────
# Shorts 18% = 346px dari bawah — aman dari UI subscribe/like YouTube Shorts
# LongForm 14% = 151px dari bawah — lebih rendah dari 12% sebelumnya
SUB_MARGIN_V_PCT_SHORTS   = 0.18    # 18% tinggi video dari bawah (Shorts)
SUB_MARGIN_V_PCT_LONGFORM = 0.14    # 14% tinggi video dari bawah (Long Form)

# ── Subtitle Karaoke Window ───────────────────────────────────────────────────
SUB_WORDS_BEFORE_ACTIVE = 1         # Jumlah kata pasif sebelum kata aktif
SUB_WORDS_AFTER_ACTIVE  = 1         # Jumlah kata pasif sesudah kata aktif
SUB_MIN_WORD_DUR_SEC    = 0.08      # Durasi minimum tampil per kata (detik)

# ── Audio Mixing ──────────────────────────────────────────────────────────────
AUDIO_DUCKING_THRESHOLD  = 0.02     # Threshold sidechain compress — kapan musik diturunkan
AUDIO_DUCKING_RATIO      = 6        # Rasio kompresi musik saat narasi aktif
AUDIO_DUCKING_ATTACK_MS  = 30       # Attack ducking (ms) — lebih kecil = lebih responsif
AUDIO_DUCKING_RELEASE_MS = 250      # Release ducking (ms) — lebih besar = lebih smooth
AUDIO_MUSIC_FADEIN_DUR   = 2        # Durasi fade-in musik (detik)
AUDIO_MUSIC_FADEOUT_DUR  = 4        # Durasi fade-out musik sebelum video habis (detik)
AUDIO_MUSIC_FADEOUT_LEAD = 5        # Mulai fade-out N detik sebelum akhir video
AUDIO_OUTPUT_BITRATE     = "192k"   # Bitrate output audio — standar YouTube
AUDIO_OUTPUT_SAMPLERATE  = 48000    # Sample rate — standar YouTube/TikTok
AUDIO_LOUDNORM_TARGET    = -14      # Target LUFS — standar YouTube (-14 LUFS)
AUDIO_LOUDNORM_TP        = -1.5     # True Peak max (dBTP)
AUDIO_LOUDNORM_LRA       = 11       # Loudness Range target

# ── Intro Splash ──────────────────────────────────────────────────────────────
SPLASH_DURATION_SEC      = 1.5      # Durasi splash screen (detik)
SPLASH_FADE_IN_SEC       = 0.3      # Durasi fade-in teks channel
SPLASH_FADE_OUT_SEC      = 0.3      # Durasi fade-out teks channel
SPLASH_BG_OPACITY        = 0.95     # Opacity black overlay saat splash (0–1)
SPLASH_FONT_SIZE_SHORTS  = 90       # Font size channel name di Shorts
SPLASH_FONT_SIZE_LONGFORM= 64       # Font size channel name di Long Form
SPLASH_SHADOW_COLOR      = "#E50914"# Warna shadow teks (merah Netflix-style)
SPLASH_SHADOW_OPACITY    = 0.9      # Opacity shadow

# ── Title Overlay (Finishing) ──────────────────────────────────────────────────
TITLE_FONT_SIZE_SHORTS    = 55      # px — judul di Shorts
TITLE_FONT_SIZE_LONGFORM  = 54      # px — judul di Long Form
TITLE_Y_POS_SHORTS        = 180     # px dari atas — posisi judul di Shorts
TITLE_Y_POS_LONGFORM      = 80      # px dari atas — posisi judul di Long Form
TITLE_SHOW_UNTIL_SEC      = 5.0     # Judul tampil sampai detik ke-N
TITLE_FADEIN_DUR_SEC      = 0.4     # Durasi fade-in judul
TITLE_FADEOUT_DUR_SEC     = 0.5     # Durasi fade-out judul
TITLE_BOX_OPACITY         = 0.65    # Opacity background box di belakang judul
TITLE_BOX_PADDING         = 18      # Padding box (px)
TITLE_SHADOW_OPACITY      = 0.85    # Opacity drop shadow judul
TITLE_MAX_CHARS           = 40      # Potong judul jika lebih dari N karakter

# ── Progress Bar (Finishing) ──────────────────────────────────────────────────
PROGRESS_BAR_H_SHORTS    = 16       # Tinggi progress bar Shorts (px)
PROGRESS_BAR_H_LONGFORM  = 10       # Tinggi progress bar Long Form (px)
PROGRESS_BAR_OPACITY     = 0.9      # Opacity progress bar

# ── Cinematic Letterbox ────────────────────────────────────────────────────────
LETTERBOX_HEIGHT_PCT     = 0.07     # Tinggi bar hitam atas/bawah (7% dari tinggi video)
LETTERBOX_OPACITY        = 0.92     # Opacity bar letterbox
LETTERBOX_NICHES         = ("horror_facts", "drama", "history")  # Niche yang dapat letterbox

# ── Font Path ─────────────────────────────────────────────────────────────────
FONT_PRIMARY_PATH        = os.path.abspath(os.path.join("assets", "fonts", "THEBOLDFONT-FREEVERSION.ttf"))
FONT_FALLBACK_PATHS      = [        # Fallback jika font utama tidak ada
    "C:/Windows/Fonts/arialbd.ttf",
    "C:/Windows/Fonts/arial.ttf",
]

# ══════════════════════════════════════════════════════════════════════════════
#  NICHE CONFIGURATION — visual identity per kategori konten
# ══════════════════════════════════════════════════════════════════════════════

# ── Highlight Color Aktif Per Niche ───────────────────────────────────────────
# Format ASS: &H00BBGGRR (Blue-Green-Red, bukan RGB)
# Satu warna per niche, konsisten sepanjang video — tidak cycling
NICHE_HIGHLIGHT_COLOR: dict = {
    "horror_facts": "&H004444FF",   # Merah   — mencekam, intens
    "drama":        "&H004444FF",   # Merah   — dramatis
    "crime":        "&H003333EE",   # Merah gelap — serius
    "psychology":   "&H00FFCC00",   # Biru elektrik — profesional
    "motivation":   "&H000099FF",   # Oranye  — energetik
    "science":      "&H00FFEE88",   # Cyan muda — futuristik
    "finance":      "&H0088FF88",   # Hijau   — wealth
    "lifestyle":    "&H00EE88FF",   # Pink    — lifestyle
    "history":      "&H0022BBFF",   # Emas    — klasik
    "nature":       "&H0088FF44",   # Hijau segar — natural
    "default":      "&H0000FFFF",   # Kuning neon — fallback
}

# ── Color Grading Per Niche ────────────────────────────────────────────────────
NICHE_COLOR_GRADE: dict = {
    "horror_facts": "curves=master='0/0 0.4/0.3 1/0.85',eq=contrast=1.15:brightness=-0.05:saturation=0.6",
    "psychology":   "curves=master='0/0 0.5/0.55 1/1',eq=contrast=1.1:saturation=0.85:gamma=0.95",
    "motivation":   "curves=r='0/0 0.5/0.6 1/1':g='0/0 0.5/0.52 1/1':b='0/0 0.5/0.4 1/1',eq=saturation=1.3:contrast=1.05",
    "drama":        "curves=master='0/0 0.5/0.45 1/0.9',eq=contrast=1.2:saturation=0.7:gamma=1.1",
    "history":      "curves=r='0/0 0.5/0.55 1/1':g='0/0 0.5/0.5 1/0.95':b='0/0 0.5/0.35 1/0.8',eq=saturation=0.65",
    "finance":      "curves=r='0/0 0.5/0.52 1/1':g='0/0 0.5/0.58 1/1':b='0/0 0.5/0.38 1/0.85',eq=contrast=1.08:saturation=1.1",
    "science":      "curves=b='0/0 0.5/0.65 1/1':g='0/0 0.5/0.55 1/1':r='0/0 0.5/0.45 1/0.92',eq=saturation=0.9:contrast=1.12",
    "lifestyle":    "curves=r='0/0 0.5/0.62 1/1':g='0/0 0.5/0.57 1/1':b='0/0 0.5/0.48 1/0.95',eq=saturation=1.25:brightness=0.03:contrast=1.0",
    "crime":        "curves=master='0/0 0.45/0.35 1/0.88',eq=contrast=1.18:saturation=0.55:gamma=1.05",
    "nature":       "curves=g='0/0 0.5/0.62 1/1':r='0/0 0.5/0.48 1/0.95':b='0/0 0.5/0.45 1/0.9',eq=saturation=1.35:brightness=0.02",
    "default":      "eq=contrast=1.05:brightness=0.02:saturation=1.0",
}

# ── Progress Bar Color Per Niche ───────────────────────────────────────────────
NICHE_PROGRESS_BAR_COLOR: dict = {
    "horror_facts": "#CC0000",   # Merah
    "drama":        "#880088",   # Ungu
    "psychology":   "#0066FF",   # Biru
    "motivation":   "#FF6600",   # Oranye
    "history":      "#996633",   # Cokelat emas
    "default":      "#E50914",   # Merah Netflix
}

# ── Title Shadow Color Per Niche ───────────────────────────────────────────────
NICHE_TITLE_SHADOW_COLOR: dict = {
    "horror_facts": "#CC0000",
    "drama":        "#CC0000",
    "default":      "#1A1AFF",   # Biru untuk non-horror
}

# ── Vignette Per Niche ─────────────────────────────────────────────────────────
# PI/3.5 ≈ 35% opacity (horror) | PI/5 ≈ 20% | PI/6 ≈ 15%
# Sebelumnya semua hardcoded PI/4 — sekarang per niche
NICHE_VIGNETTE_FILTER: dict = {
    "horror_facts": "vignette=PI/3.5:eval=frame",  # Gelap di pinggir — horror
    "drama":        "vignette=PI/3.5:eval=frame",  # Sama dengan horror
    "crime":        "vignette=PI/3.8:eval=frame",  # Sedikit lebih terang
    "history":      "vignette=PI/4:eval=frame",    # Standard
    "psychology":   "vignette=PI/6:eval=frame",    # Minimal — clean & profesional
    "motivation":   "vignette=PI/6:eval=frame",    # Minimal — energetik
    "science":      "vignette=PI/6:eval=frame",    # Minimal
    "finance":      "vignette=PI/6:eval=frame",    # Minimal
    "lifestyle":    "vignette=PI/7:eval=frame",    # Hampir tidak terlihat
    "nature":       "vignette=PI/7:eval=frame",    # Hampir tidak terlihat
    "default":      "vignette=PI/5:eval=frame",    # Fallback
}


# ══════════════════════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _encode_params() -> list:
    """Return parameter encoding GPU atau CPU sesuai USE_GPU flag."""
    return GPU_ENCODE_PARAMS if USE_GPU else CPU_ENCODE_PARAMS


def _vignette_filter(niche: str) -> str:
    """Return FFmpeg vignette filter string sesuai niche."""
    for key, val in NICHE_VIGNETTE_FILTER.items():
        if key in niche.lower():
            return val
    return NICHE_VIGNETTE_FILTER["default"]


def _resolve_font_path() -> str:
    """Cari font utama, fallback ke sistem font jika tidak ada."""
    if os.path.exists(FONT_PRIMARY_PATH):
        return FONT_PRIMARY_PATH
    for fallback in FONT_FALLBACK_PATHS:
        if os.path.exists(fallback):
            return fallback
    return FONT_FALLBACK_PATHS[-1]   # Last resort


def _ffmpeg_font_path(font_path: str) -> str:
    """Convert path ke format FFmpeg drawtext (escape Windows drive letter)."""
    return font_path.replace("\\", "/").replace(":", "\\:")


def _run_ffmpeg(cmd: list, label: str):
    """Jalankan FFmpeg command, raise RuntimeError jika gagal."""
    logger.debug(f"FFmpeg [{label}]: {' '.join(cmd[:8])}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg [{label}] failed:\n{result.stderr[-500:]}")


def _run_ffmpeg_with_gpu_fallback(cmd: list, label: str):
    """
    Jalankan FFmpeg dengan GPU. Jika NVENC error (VRAM penuh / tidak ada GPU),
    otomatis fallback ke CPU libx264 tanpa crash pipeline.
    """
    global USE_GPU
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        return

    stderr       = result.stderr
    nvenc_errors = ["nvenc", "nvcuda", "no capable devices", "h264_nvenc", "out of memory"]

    if USE_GPU and any(err.lower() in stderr.lower() for err in nvenc_errors):
        logger.warning(f"[{label}] NVENC gagal (VRAM atau driver), fallback CPU libx264...")
        # Ganti parameter GPU → CPU tanpa ubah argumen lain
        cpu_cmd, skip_next = [], False
        for i, arg in enumerate(cmd):
            if skip_next:
                skip_next = False
                continue
            if arg == "h264_nvenc":
                cpu_cmd.append("libx264")
            elif arg == "-preset" and i + 1 < len(cmd):
                cpu_cmd += ["-preset", "fast"]
                skip_next = True
            elif arg in ["-cq", "-rc", "-b:v"]:
                if arg == "-cq":
                    cpu_cmd += ["-crf", "23"]
                skip_next = True
            elif arg in ["-gpu", "-hwaccel"]:
                if i + 1 < len(cmd) and cmd[i + 1].isdigit():
                    skip_next = True
                continue
            else:
                cpu_cmd.append(arg)

        result2 = subprocess.run(cpu_cmd, capture_output=True, text=True)
        if result2.returncode == 0:
            return
        logger.error(f"CPU fallback [{label}] juga gagal: {result2.stderr[-300:]}")

    raise RuntimeError(f"FFmpeg [{label}] failed (code {result.returncode})")


def _get_duration(file_path: str) -> float:
    """Dapatkan durasi file audio/video dalam detik via ffprobe."""
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", file_path],
        capture_output=True, text=True
    )
    return float(json.loads(result.stdout)["format"]["duration"])


def _ass_timestamp(seconds: float) -> str:
    """Convert detik float ke format timestamp ASS (H:MM:SS.cc)."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    cs = int((seconds % 1) * 100)   # centiseconds
    return f"{h}:{m:02}:{s:02}.{cs:02}"


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def render(script_data: dict, audio_path: str, footage: any,
           channel: dict, profile: str = "shorts") -> str:
    """
    Entry point render. Dispatch ke _render_shorts atau _render_long_form
    berdasarkan profile. Return path file video output.
    """
    ch_id    = channel["id"]
    niche    = channel.get("niche", "default")
    out_dir  = channel_data_path(ch_id, "output")
    ts       = timestamp()
    out_path = f"{out_dir}/{ts}_{profile}.mp4"
    tmp_dir  = f"{channel_data_path(ch_id, 'footage')}/tmp_{ts}"
    os.makedirs(tmp_dir, exist_ok=True)

    logger.info(f"[{ch_id}] Mulai render [{profile}] niche={niche}")

    try:
        if profile == "long_form":
            _render_long_form(script_data, audio_path, footage, ch_id, niche, out_path, tmp_dir)
        else:
            _render_shorts(script_data, audio_path, footage, ch_id, niche, out_path, tmp_dir)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info(f"[{ch_id}] Render selesai → {out_path}")
    return out_path


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER PIPELINE — SHORTS
# ══════════════════════════════════════════════════════════════════════════════

def _render_shorts(script_data: dict, audio_path: str, footage_paths: list,
                   ch_id: str, niche: str, out_path: str, tmp_dir: str):
    from engine import music_engine, sfx_engine

    settings  = load_settings()
    profile   = settings.get("video_profiles", {}).get("shorts", {})
    width     = profile.get("width", 1080)
    height    = profile.get("height", 1920)
    fps       = profile.get("fps", 24)           # 24fps — cukup untuk Shorts, ringan di VRAM
    music_vol = profile.get("music_volume", 0.12)
    audio_dur = _get_duration(audio_path)

    # Step 1: Transcribe audio → word-level timestamps
    sentences = _transcribe_audio(audio_path)

    # Step 2: Build footage synced ke kalimat + Ken Burns
    synced_footage_path = _build_synced_footage(
        footage_paths, sentences, audio_dur,
        width, height, fps, tmp_dir, ch_id, niche
    )

    # Step 3: Color grade sesuai niche
    graded_footage_path = f"{tmp_dir}/graded.mp4"
    _apply_color_grade(synced_footage_path, graded_footage_path, niche)

    # Step 4: Loop footage sampai pas dengan durasi audio
    video_synced_path = f"{tmp_dir}/video_synced.mp4"
    _sync_video_to_audio(graded_footage_path, audio_path, video_synced_path)

    # Step 5: Mix audio — narasi + musik background dengan ducking
    music_mood        = "dark ambient" if niche in ("horror_facts", "drama") else "uplifting"
    music_path        = music_engine.fetch(music_mood, int(audio_dur) + 3, ch_id)
    mixed_audio_path  = f"{tmp_dir}/mixed_audio.mp3"
    _mix_audio_with_ducking(audio_path, music_path, mixed_audio_path, music_vol, audio_dur)

    # Step 6: Tambah SFX sesuai niche
    sfx_audio_path = sfx_engine.mix_sfx_to_audio(mixed_audio_path, niche, tmp_dir)

    # Step 7: Ganti audio video dengan hasil mix
    video_with_audio_path = f"{tmp_dir}/with_audio.mp4"
    _replace_video_audio(video_synced_path, sfx_audio_path, video_with_audio_path)

    # Step 8: Generate & burn subtitle ASS karaoke
    subtitle_path  = _generate_ass_subtitle(sentences, ch_id, width, height, niche)
    base, ext      = os.path.splitext(out_path)
    subtitled_path = f"{base}_sub{ext}"
    _burn_subtitles(video_with_audio_path, subtitle_path, subtitled_path)

    # Step 9: Tambah intro splash screen channel
    splashed_path = f"{base}_splash{ext}"
    _add_intro_splash(subtitled_path, splashed_path, ch_id, width, height)

    # Step 10: Finishing — progress bar + title overlay + letterbox
    finished_path = f"{base}_finish{ext}"
    _add_finishing_effects(
        splashed_path, finished_path,
        title=script_data.get("title", ""),
        width=width, height=height,
        duration=audio_dur, niche=niche
    )

    # Step 11: Loudnorm final — standarisasi volume ke -14 LUFS (YouTube standard)
    _apply_loudnorm(finished_path, out_path)

    # Cleanup intermediate files
    for temp_path in [subtitled_path, splashed_path, finished_path]:
        if os.path.exists(temp_path):
            os.remove(temp_path)


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER PIPELINE — LONG FORM
# ══════════════════════════════════════════════════════════════════════════════

def _render_long_form(script_data: dict, audio_path: str, footage: dict,
                      ch_id: str, niche: str, out_path: str, tmp_dir: str):
    from engine import music_engine, sfx_engine

    settings    = load_settings()
    profile_cfg = settings.get("video_profiles", {}).get("long_form", {})
    width       = profile_cfg.get("width", 1920)
    height      = profile_cfg.get("height", 1080)
    fps         = profile_cfg.get("fps", 30)       # 30fps untuk long form
    music_vol   = profile_cfg.get("music_volume", 0.18)
    audio_dur   = _get_duration(audio_path)

    # Gabungkan semua clips: intro + segments + outro
    all_clips = (footage.get("intro", [])
                 + [c for seg in footage.get("segments", []) for c in seg]
                 + footage.get("outro", []))

    # Step 1-2: Concat blurred bg + color grade
    raw_footage_path    = f"{tmp_dir}/raw_footage.mp4"
    graded_footage_path = f"{tmp_dir}/graded_footage.mp4"
    _concat_with_blurred_bg(all_clips, raw_footage_path, width, height, fps, tmp_dir, niche)
    _apply_color_grade(raw_footage_path, graded_footage_path, niche)

    # Step 3: Sync ke durasi audio
    video_synced_path = f"{tmp_dir}/video_synced.mp4"
    _sync_video_to_audio(graded_footage_path, audio_path, video_synced_path)

    # Step 4-6: Mix audio
    music_path        = music_engine.fetch(
        script_data.get("keywords_music_intro", "dark ambient"),
        int(audio_dur) + 5, ch_id
    )
    mixed_audio_path      = f"{tmp_dir}/mixed_audio.mp3"
    video_with_audio_path = f"{tmp_dir}/with_audio.mp4"
    _mix_audio_with_ducking(audio_path, music_path, mixed_audio_path, music_vol, audio_dur)
    sfx_audio_path = sfx_engine.mix_sfx_to_audio(mixed_audio_path, niche, tmp_dir)
    _replace_video_audio(video_synced_path, sfx_audio_path, video_with_audio_path)

    # Step 7-10: Subtitle + Splash + Finishing + Loudnorm
    sentences      = _transcribe_audio(audio_path)
    subtitle_path  = _generate_ass_subtitle(sentences, ch_id, width, height, niche)
    base, ext      = os.path.splitext(out_path)
    subtitled_path = f"{base}_sub{ext}"
    splashed_path  = f"{base}_splash{ext}"
    finished_path  = f"{base}_finish{ext}"

    _burn_subtitles(video_with_audio_path, subtitle_path, subtitled_path)
    _add_intro_splash(subtitled_path, splashed_path, ch_id, width, height)
    _add_finishing_effects(
        splashed_path, finished_path,
        title=script_data.get("title", ""),
        width=width, height=height,
        duration=audio_dur, niche=niche
    )
    _apply_loudnorm(finished_path, out_path)

    for temp_path in [subtitled_path, splashed_path, finished_path]:
        if os.path.exists(temp_path):
            os.remove(temp_path)


# ══════════════════════════════════════════════════════════════════════════════
#  TRANSCRIPTION
# ══════════════════════════════════════════════════════════════════════════════

def _transcribe_audio(audio_path: str) -> list[dict]:
    """
    Transkripsi audio dengan Faster-Whisper, return list segmen dengan
    word-level timestamps. Segmen pendek (<WHISPER_MIN_SEG_DUR) digabung
    ke segmen sebelumnya untuk mengurangi jumlah footage cuts.

    Return format:
      [{"start": 0.0, "end": 2.5, "text": "...", "words": [{"word","start","end"}]}]
    """
    from faster_whisper import WhisperModel

    logger.info(f"Transkripsi audio: model={WHISPER_MODEL_SIZE} device={WHISPER_DEVICE}")
    model    = WhisperModel(WHISPER_MODEL_SIZE, device=WHISPER_DEVICE,
                            compute_type=WHISPER_COMPUTE_TYPE)
    segments, _ = model.transcribe(audio_path, word_timestamps=True)

    # Build raw segment list
    raw_segments = []
    for seg in segments:
        words = [{"word": w.word.strip(), "start": w.start, "end": w.end}
                 for w in (seg.words or []) if w.word.strip()]
        raw_segments.append({
            "start": seg.start, "end": seg.end,
            "text":  seg.text.strip(), "words": words
        })

    # Merge segmen pendek ke segmen sebelumnya
    merged, buffer = [], None
    for seg in raw_segments:
        if buffer is None:
            buffer = {**seg, "words": list(seg["words"])}
        elif (seg["end"] - seg["start"]) < WHISPER_MIN_SEG_DUR:
            buffer["end"]    = seg["end"]
            buffer["text"]  += " " + seg["text"]
            buffer["words"] += seg["words"]
        else:
            merged.append(buffer)
            buffer = {**seg, "words": list(seg["words"])}
    if buffer:
        merged.append(buffer)

    logger.info(f"Transkripsi selesai: {len(merged)} segmen")
    return merged if merged else raw_segments


# ══════════════════════════════════════════════════════════════════════════════
#  FOOTAGE PROCESSING
# ══════════════════════════════════════════════════════════════════════════════

def _apply_blurred_bg_single(clip_path: str, out_path: str,
                              width: int, height: int,
                              fps: int, niche: str = "default"):
    """
    Render satu clip dengan blurred background:
    - BG: clip di-scale penuh + blur kuat (sinematik)
    - FG: clip di-scale fit + overlay di tengah
    - Vignette per niche di overlay

    Optimasi RTX 3050: filter berjalan di CPU, output di-encode NVENC.
    Ini lebih stabil daripada GPU filter yang butuh banyak VRAM.
    """
    vignette = _vignette_filter(niche)
    filter_complex = (
        f"[0:v]split[raw_bg][raw_fg];"
        # Background: scale penuh + crop center + blur kuat
        f"[raw_bg]fps={fps},"
        f"scale={width}:{height}:force_original_aspect_ratio=increase,"
        f"crop={width}:{height},"
        f"boxblur={BG_BLUR_RADIUS}:{BG_BLUR_POWER},"
        f"setsar=1[blurred_bg];"
        # Foreground: scale fit (tidak crop) + overlay tengah
        f"[raw_fg]fps={fps},"
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"setsar=1[sharp_fg];"
        # Overlay FG di atas BG + vignette
        f"[blurred_bg][sharp_fg]overlay=(W-w)/2:(H-h)/2,{vignette},setsar=1[out]"
    )
    _run_ffmpeg_with_gpu_fallback(
        ["ffmpeg", "-y", "-i", clip_path,
         "-filter_complex", filter_complex,
         "-map", "[out]",
         "-threads", str(FFMPEG_THREAD_COUNT)]
        + _encode_params() + [out_path],
        f"blurred_bg {os.path.basename(clip_path)}"
    )


def _concat_with_blurred_bg(clip_paths: list, out_path: str,
                             width: int, height: int,
                             fps: int, tmp_dir: str, niche: str = "default"):
    """Process setiap clip dengan blurred bg lalu concat jadi satu file."""
    if not clip_paths:
        raise ValueError("Tidak ada footage untuk di-render")

    processed_paths = []
    for i, clip_path in enumerate(clip_paths):
        proc_path = f"{tmp_dir}/proc_{i}.mp4"
        try:
            _apply_blurred_bg_single(clip_path, proc_path, width, height, fps, niche)
            processed_paths.append(proc_path)
        except Exception as e:
            logger.warning(f"Clip {i} gagal diproses: {e}, skip")

    if not processed_paths:
        raise RuntimeError("Semua clip gagal diproses")

    _concat_clips(processed_paths, out_path, tmp_dir)


def _build_synced_footage(footage_paths: list, sentences: list, audio_dur: float,
                          width: int, height: int, fps: int,
                          tmp_dir: str, ch_id: str, niche: str = "default") -> str:
    """
    Build footage yang berganti per kalimat (sentence boundary) dengan
    Ken Burns effect (slow zoom + pan) untuk visual yang lebih sinematik.

    Setiap kalimat mendapat clip berbeda (di-shuffle). Durasi clip = durasi kalimat.
    Ken Burns direction berputar dari KEN_BURNS_ZOOM_DIRS agar tidak monoton.

    Return: path file footage yang sudah disync
    """
    if not footage_paths:
        raise ValueError("Tidak ada footage")
    if not sentences:
        # Fallback: concat semua footage tanpa sync
        out = f"{tmp_dir}/synced_raw.mp4"
        _concat_with_blurred_bg(footage_paths, out, width, height, fps, tmp_dir, niche)
        return out

    shuffled_clips = footage_paths.copy()
    random.shuffle(shuffled_clips)

    vignette   = _vignette_filter(niche)
    clip_index = 0
    seg_paths  = []

    for seg_idx, sentence in enumerate(sentences):
        seg_duration = sentence["end"] - sentence["start"]
        if seg_duration < KEN_BURNS_MIN_DUR_SEC:
            continue

        clip_path = shuffled_clips[clip_index % len(shuffled_clips)]
        clip_index += 1
        seg_out    = f"{tmp_dir}/seg_{seg_idx}.mp4"

        # Pilih arah Ken Burns bergiliran
        kb         = KEN_BURNS_ZOOM_DIRS[seg_idx % len(KEN_BURNS_ZOOM_DIRS)]
        x_expr     = kb[0]
        y_expr     = kb[1]
        zoom_start = float(kb[2])
        zoom_end   = float(kb[3])
        n_frames   = max(int(seg_duration * fps), 1)

        # Zoom expression: interpolasi linear dari zoom_start ke zoom_end
        zoom_delta  = zoom_end - zoom_start
        zoom_max    = max(zoom_start, zoom_end)
        zoom_expr   = (
            f"'if(lte(on,1),{zoom_start},"
            f"min(zoom+({zoom_delta:.4f}/{n_frames}),{zoom_max}))'"
        )

        filter_complex = (
            f"[0:v]split[raw_bg][raw_fg];"
            # Background: blur penuh
            f"[raw_bg]fps={fps},"
            f"scale={width}:{height}:force_original_aspect_ratio=increase,"
            f"crop={width}:{height},"
            f"boxblur={BG_BLUR_RADIUS}:{BG_BLUR_POWER},setsar=1[blurred_bg];"
            # Foreground: scale 2x dulu untuk zoompan berkualitas, lalu zoompan
            f"[raw_fg]fps={fps},"
            f"scale={width*FG_ZOOM_SCALE_FACTOR}:{height*FG_ZOOM_SCALE_FACTOR}"
            f":force_original_aspect_ratio=decrease,"
            f"zoompan=z={zoom_expr}:x='{x_expr}':y='{y_expr}'"
            f":d={n_frames}:s={width}x{height}:fps={fps},"
            f"setsar=1[zoomed_fg];"
            # Overlay + vignette
            f"[blurred_bg][zoomed_fg]overlay=(W-w)/2:(H-h)/2,{vignette},setsar=1[out]"
        )

        try:
            _run_ffmpeg_with_gpu_fallback(
                ["ffmpeg", "-y", "-stream_loop", "-1", "-i", clip_path,
                 "-t", f"{seg_duration:.3f}",
                 "-filter_complex", filter_complex,
                 "-map", "[out]",
                 "-threads", str(FFMPEG_THREAD_COUNT)]
                + _encode_params() + [seg_out],
                f"ken_burns_seg_{seg_idx}"
            )
            seg_paths.append(seg_out)
        except Exception as e:
            logger.warning(f"Ken Burns seg {seg_idx} gagal ({e}), fallback tanpa zoom")
            try:
                fc_fallback = (
                    f"[0:v]split[r1][r2];"
                    f"[r1]fps={fps},scale={width}:{height}:force_original_aspect_ratio=increase,"
                    f"crop={width}:{height},boxblur={BG_BLUR_RADIUS}:{BG_BLUR_POWER},setsar=1[bg];"
                    f"[r2]fps={fps},scale={width}:{height}:force_original_aspect_ratio=decrease,"
                    f"setsar=1[fg];"
                    f"[bg][fg]overlay=(W-w)/2:(H-h)/2,{vignette},setsar=1[out]"
                )
                _run_ffmpeg_with_gpu_fallback(
                    ["ffmpeg", "-y", "-stream_loop", "-1", "-i", clip_path,
                     "-t", f"{seg_duration:.3f}",
                     "-filter_complex", fc_fallback,
                     "-map", "[out]",
                     "-threads", str(FFMPEG_THREAD_COUNT)]
                    + _encode_params() + [seg_out],
                    f"seg_{seg_idx}_fallback"
                )
                seg_paths.append(seg_out)
            except Exception as e2:
                logger.warning(f"Seg {seg_idx} fallback juga gagal: {e2}, skip")

    if not seg_paths:
        out = f"{tmp_dir}/synced_raw.mp4"
        _concat_with_blurred_bg(footage_paths, out, width, height, fps, tmp_dir, niche)
        return out

    synced_out = f"{tmp_dir}/synced_raw.mp4"
    _concat_clips(seg_paths, synced_out, tmp_dir)
    return synced_out


def _concat_clips(clip_paths: list, out_path: str, tmp_dir: str):
    """
    Concat multiple clip files jadi satu.
    Coba stream copy dulu (cepat, tidak re-encode).
    Fallback ke re-encode jika format tidak kompatibel.
    """
    list_file = f"{tmp_dir}/concat_list.txt"
    with open(list_file, "w", encoding="utf-8") as f:
        for p in clip_paths:
            f.write(f"file '{p.replace(chr(92), '/')}'\n")

    result = subprocess.run(
        ["ffmpeg", "-y", "-f", "concat", "-safe", "0",
         "-i", list_file, "-c", "copy", out_path],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        return

    # Fallback: re-encode concat (lebih lambat tapi pasti jalan)
    logger.warning("Stream copy concat gagal, re-encode concat...")
    inputs = []
    for p in clip_paths:
        inputs += ["-i", p]
    v_in = "".join(f"[{i}:v]" for i in range(len(clip_paths)))
    _run_ffmpeg_with_gpu_fallback(
        ["ffmpeg", "-y"] + inputs +
        ["-filter_complex", f"{v_in}concat=n={len(clip_paths)}:v=1:a=0[vout]",
         "-map", "[vout]",
         "-threads", str(FFMPEG_THREAD_COUNT)]
        + _encode_params() + [out_path],
        "concat_reencode"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  COLOR GRADE
# ══════════════════════════════════════════════════════════════════════════════

def _apply_color_grade(input_path: str, out_path: str, niche: str):
    """Apply FFmpeg curves + eq color grade sesuai niche."""
    grade_filter = NICHE_COLOR_GRADE.get(niche, NICHE_COLOR_GRADE["default"])
    try:
        _run_ffmpeg_with_gpu_fallback(
            ["ffmpeg", "-y", "-i", input_path,
             "-vf", grade_filter,
             "-threads", str(FFMPEG_THREAD_COUNT)]
            + _encode_params() + ["-c:a", "copy", out_path],
            f"color_grade_{niche}"
        )
    except Exception as e:
        logger.warning(f"Color grade gagal ({e}), skip grade")
        shutil.copy2(input_path, out_path)


# ══════════════════════════════════════════════════════════════════════════════
#  SUBTITLE GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def _generate_ass_subtitle(sentences: list, ch_id: str,
                           width: int, height: int, niche: str = "default") -> str:
    """
    Generate file ASS subtitle dengan 3-word karaoke window:
    - Tampilkan 3 kata sekaligus (1 sebelum + aktif + 1 sesudah)
    - Kata aktif: warna per niche + scale bounce animasi
    - Kata pasif: putih, ukuran normal
    - Posisi: bottom-center, margin aman dari UI YouTube

    Return: path file .ass
    """
    sub_dir  = channel_data_path(ch_id, "subtitles")
    sub_path = f"{sub_dir}/{timestamp()}.ass"
    os.makedirs(sub_dir, exist_ok=True)

    is_shorts    = (width == 1080 and height == 1920)
    font_size    = SUB_FONT_SIZE_SHORTS if is_shorts else SUB_FONT_SIZE_LONGFORM
    margin_v_pct = SUB_MARGIN_V_PCT_SHORTS if is_shorts else SUB_MARGIN_V_PCT_LONGFORM
    margin_v     = int(height * margin_v_pct)   # px dari bawah

    ass_content  = _build_ass_header(width, height, font_size, margin_v, niche)
    ass_content += _build_ass_events(sentences, niche)

    with open(sub_path, "w", encoding="utf-8") as f:
        f.write(ass_content)

    return sub_path


def _build_ass_header(width: int, height: int, font_size: int,
                      margin_v: int, niche: str = "default") -> str:
    """Build ASS script header dengan 2 style: Default (pasif) dan Active (kata aktif)."""
    active_font_size = int(font_size * SUB_ACTIVE_SIZE_FACTOR)
    highlight_color  = NICHE_HIGHLIGHT_COLOR.get(niche, NICHE_HIGHLIGHT_COLOR["default"])

    return (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        f"PlayResX: {width}\n"
        f"PlayResY: {height}\n"
        "WrapStyle: 1\n"
        "ScaledBorderAndShadow: yes\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        # Style Default: kata pasif — putih, outline hitam tebal
        f"Style: Default,{SUB_FONT_NAME},{font_size},"
        f"&H00FFFFFF,&H000000FF,&H00000000,&H00000000,"
        f"{SUB_FONT_BOLD},0,0,0,100,100,0,0,"
        f"1,{SUB_OUTLINE_THICKNESS},{SUB_SHADOW_DEPTH},"
        f"2,{SUB_MARGIN_L},{SUB_MARGIN_R},{margin_v},1\n"
        # Style Active: kata aktif — warna per niche, sedikit lebih besar
        f"Style: Active,{SUB_FONT_NAME},{active_font_size},"
        f"{highlight_color},&H000000FF,&H00000000,&H00000000,"
        f"{SUB_FONT_BOLD},0,0,0,100,100,0,0,"
        f"1,{SUB_OUTLINE_THICKNESS},{SUB_SHADOW_DEPTH},"
        f"2,{SUB_MARGIN_L},{SUB_MARGIN_R},{margin_v},1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
    )


def _build_ass_events(sentences: list, niche: str = "default") -> str:
    """
    Build ASS Dialogue events untuk semua kata dengan 3-word window.
    Setiap kata aktif ditampilkan bersama 1 kata sebelum dan sesudahnya.
    Kata aktif mendapat animasi bounce via \\t() ASS tags.
    """
    events          = ""
    highlight_color = NICHE_HIGHLIGHT_COLOR.get(niche, NICHE_HIGHLIGHT_COLOR["default"])

    for sentence in sentences:
        text       = sentence.get("text", "").strip()
        if not text:
            continue

        raw_words  = sentence.get("words") or []
        sent_start = sentence["start"]
        sent_end   = sentence["end"]

        # Build word list dengan timestamp
        if raw_words:
            word_list = [
                {"word": w["word"].strip().upper(), "start": w["start"], "end": w["end"]}
                for w in raw_words if w["word"].strip()
            ]
        else:
            # Fallback: bagi durasi rata per kata
            plain_words = [t.upper() for t in text.split() if t.strip()]
            n_words     = max(len(plain_words), 1)
            word_dur    = (sent_end - sent_start) / n_words
            word_list   = [
                {"word": w, "start": sent_start + i * word_dur,
                 "end": sent_start + (i + 1) * word_dur}
                for i, w in enumerate(plain_words)
            ]

        if not word_list:
            continue

        n_words = len(word_list)

        for word_idx, active_word in enumerate(word_list):
            word_start = active_word["start"]
            # Kata aktif tampil sampai kata berikutnya mulai (atau kalimat habis)
            word_end = (word_list[word_idx + 1]["start"]
                        if word_idx + 1 < n_words else sent_end)
            if word_end - word_start < SUB_MIN_WORD_DUR_SEC:
                word_end = word_start + SUB_MIN_WORD_DUR_SEC

            # 3-word window: [prev, ACTIVE, next]
            window_start  = max(word_idx - SUB_WORDS_BEFORE_ACTIVE, 0)
            window_end    = min(word_idx + SUB_WORDS_AFTER_ACTIVE + 1, n_words)
            window        = word_list[window_start:window_end]
            active_in_win = word_idx - window_start  # index aktif dalam window

            # Hitung timing bounce animasi
            word_dur_ms    = max(int((word_end - word_start) * 1000), 100)
            bounce_peak_ms = min(int(word_dur_ms * SUB_BOUNCE_PEAK_PCT), 150)
            bounce_settle_ms = min(bounce_peak_ms * 2, word_dur_ms)

            # Build teks untuk setiap kata dalam window
            parts = []
            for win_pos, word_data in enumerate(window):
                word_text = word_data["word"]

                if win_pos == active_in_win:
                    # Kata aktif: warna highlight + bounce animasi
                    tag = (
                        r"{\fscx100\fscy100"
                        r"\t(0," + str(bounce_peak_ms) +
                        r",\fscx" + str(SUB_BOUNCE_SCALE_PEAK) +
                        r"\fscy" + str(SUB_BOUNCE_SCALE_PEAK) + r")"
                        r"\t(" + str(bounce_peak_ms) + r"," + str(bounce_settle_ms) +
                        r",\fscx" + str(SUB_BOUNCE_SCALE_SETTLE) +
                        r"\fscy" + str(SUB_BOUNCE_SCALE_SETTLE) + r")"
                        r"\1c" + highlight_color + r"}"
                    )
                else:
                    # Kata pasif: putih, normal
                    tag = r"{\fscx100\fscy100\1c&H00FFFFFF}"

                parts.append(tag + word_text)

            # Satu Dialogue line per kata aktif, semua 3 kata dalam satu baris
            dialogue_text = r"{\an2}" + "  ".join(parts)
            events += (
                f"Dialogue: 0,{_ass_timestamp(word_start)},"
                f"{_ass_timestamp(word_end)},Default,,0,0,0,,{dialogue_text}\n"
            )

    return events


def _burn_subtitles(video_path: str, subtitle_path: str, out_path: str):
    """Burn subtitle ASS ke video menggunakan libass filter."""
    subtitle_path_escaped = subtitle_path.replace("\\", "/").replace(":", "\\:")
    _run_ffmpeg_with_gpu_fallback(
        ["ffmpeg", "-y", "-i", video_path,
         "-vf", f"ass='{subtitle_path_escaped}'",
         "-threads", str(FFMPEG_THREAD_COUNT)]
        + _encode_params() + ["-c:a", "copy", out_path],
        "burn_subtitles"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  INTRO SPLASH
# ══════════════════════════════════════════════════════════════════════════════

def _add_intro_splash(input_path: str, out_path: str,
                      ch_id: str, width: int, height: int):
    """
    Tambah splash screen channel di 1.5 detik pertama via overlay filter.
    Metode overlay (bukan concat) memastikan audio TIDAK geser/desync.

    Visual: black overlay + channel name center, fade-in/out smooth.
    """
    is_shorts  = (width < height)
    font_size  = SPLASH_FONT_SIZE_SHORTS if is_shorts else SPLASH_FONT_SIZE_LONGFORM
    font_path  = _resolve_font_path()
    fp_escaped = _ffmpeg_font_path(font_path)
    label      = ch_id.upper().replace("_", " ")

    fade_out_start = SPLASH_DURATION_SEC - SPLASH_FADE_OUT_SEC
    alpha_expr = (
        f"if(lt(t,{SPLASH_FADE_IN_SEC}),"
        f"t/{SPLASH_FADE_IN_SEC},"
        f"if(gt(t,{fade_out_start:.1f}),"
        f"({SPLASH_DURATION_SEC:.1f}-t)/{SPLASH_FADE_OUT_SEC},1))"
    )

    filter_complex = (
        f"[0:v]"
        # Black overlay — aktif selama splash duration
        f"drawbox=x=0:y=0:w={width}:h={height}"
        f":color=black@{SPLASH_BG_OPACITY}:t=fill"
        f":enable='between(t,0,{SPLASH_DURATION_SEC})',"
        # Channel name — centered, fade-in/out
        f"drawtext=fontfile='{fp_escaped}':text='{label}'"
        f":fontsize={font_size}:fontcolor=white"
        f":x=(w-text_w)/2:y=(h-text_h)/2"
        f":shadowx=4:shadowy=4"
        f":shadowcolor={SPLASH_SHADOW_COLOR}@{SPLASH_SHADOW_OPACITY}"
        f":alpha='{alpha_expr}'"
        f":enable='between(t,0,{SPLASH_DURATION_SEC})'[outv]"
    )

    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", input_path,
             "-filter_complex", filter_complex,
             "-map", "[outv]", "-map", "0:a", "-c:a", "copy",
             "-threads", str(FFMPEG_THREAD_COUNT)]
            + _encode_params() + [out_path],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr[-400:])
        logger.info(f"Intro splash OK ({SPLASH_DURATION_SEC}s) — audio zero desync")
    except Exception as e:
        logger.warning(f"Intro splash gagal ({e}), skip")
        shutil.copy2(input_path, out_path)


# ══════════════════════════════════════════════════════════════════════════════
#  FINISHING EFFECTS
# ══════════════════════════════════════════════════════════════════════════════

def _add_finishing_effects(input_path: str, out_path: str, title: str,
                           width: int, height: int,
                           duration: float, niche: str = "default"):
    """
    Tambah elemen visual finishing:
    1. Cinematic letterbox (horror/drama/history) — black bars atas+bawah
    2. Progress bar berjalan — indikator durasi video
    3. Title overlay — judul muncul di awal, fade in/out

    Semua parameter dari CONFIG BLOCK di atas.
    """
    is_shorts        = (width == 1080 and height == 1920)
    progress_bar_h   = PROGRESS_BAR_H_SHORTS if is_shorts else PROGRESS_BAR_H_LONGFORM
    progress_color   = NICHE_PROGRESS_BAR_COLOR.get(niche, NICHE_PROGRESS_BAR_COLOR["default"])
    title_font_size  = TITLE_FONT_SIZE_SHORTS if is_shorts else TITLE_FONT_SIZE_LONGFORM
    title_y_pos      = TITLE_Y_POS_SHORTS if is_shorts else TITLE_Y_POS_LONGFORM
    title_shadow_col = NICHE_TITLE_SHADOW_COLOR.get(niche, NICHE_TITLE_SHADOW_COLOR["default"])

    # ── Step 1: Cinematic letterbox ───────────────────────────────────────────
    working_path = input_path
    letterbox_tmp = input_path + "_lb.mp4"

    if niche in LETTERBOX_NICHES:
        bar_height_px = int(height * LETTERBOX_HEIGHT_PCT)
        lb_filter = (
            f"[0:v]pad={width}:{height}:0:0:black,"
            f"drawbox=x=0:y=0:w={width}:h={bar_height_px}"
            f":color=black@{LETTERBOX_OPACITY}:t=fill,"
            f"drawbox=x=0:y={height-bar_height_px}:w={width}:h={bar_height_px}"
            f":color=black@{LETTERBOX_OPACITY}:t=fill[lbout]"
        )
        try:
            _run_ffmpeg_with_gpu_fallback(
                ["ffmpeg", "-y", "-i", input_path,
                 "-filter_complex", lb_filter,
                 "-map", "[lbout]", "-map", "0:a",
                 "-threads", str(FFMPEG_THREAD_COUNT)]
                + _encode_params() + ["-c:a", "copy", letterbox_tmp],
                "letterbox"
            )
            working_path = letterbox_tmp
            logger.info(f"Letterbox OK ({bar_height_px}px) niche={niche}")
        except Exception as e:
            logger.warning(f"Letterbox gagal ({e}), skip")

    # ── Step 2: Progress bar ──────────────────────────────────────────────────
    filter_chain = (
        f"color=c={progress_color}@{PROGRESS_BAR_OPACITY}"
        f":s={width}x{progress_bar_h}:d={duration:.3f} [bar]; "
        f"[0:v][bar]overlay=x='-W+(t/{duration:.3f})*W':y=H-h [with_bar]"
    )

    # ── Step 3: Title overlay ─────────────────────────────────────────────────
    if title:
        clean_title = (title.upper()
                       .replace("'", "").replace(":", "")
                       .replace('"', "").strip())
        if len(clean_title) > TITLE_MAX_CHARS:
            clean_title = clean_title[:TITLE_MAX_CHARS - 3] + "..."

        font_path  = _resolve_font_path()
        fp_escaped = _ffmpeg_font_path(font_path)

        # Hitung timing fade-out title
        title_show_until  = min(TITLE_SHOW_UNTIL_SEC, duration - 0.2)
        title_fadeout_start = title_show_until - TITLE_FADEOUT_DUR_SEC

        alpha_expr = (
            f"if(lt(t,{TITLE_FADEIN_DUR_SEC}),"
            f"t/{TITLE_FADEIN_DUR_SEC},"
            f"if(gt(t,{title_fadeout_start:.2f}),"
            f"({title_fadeout_start+TITLE_FADEOUT_DUR_SEC:.2f}-t)/{TITLE_FADEOUT_DUR_SEC},1))"
        )

        filter_chain += (
            f"; [with_bar]drawtext"
            f"=fontfile='{fp_escaped}'"
            f":text='{clean_title}'"
            f":fontsize={title_font_size}"
            f":fontcolor=white"
            f":alpha='{alpha_expr}'"
            f":x=(w-text_w)/2:y={title_y_pos}"
            f":shadowx=5:shadowy=5"
            f":shadowcolor={title_shadow_col}@{TITLE_SHADOW_OPACITY}"
            f":box=1:boxcolor=black@{TITLE_BOX_OPACITY}"
            f":boxborderw={TITLE_BOX_PADDING}"
            f":enable='between(t,0,{title_show_until:.1f})'"
            f" [outv]"
        )
    else:
        filter_chain += "; [with_bar]null [outv]"

    try:
        _run_ffmpeg_with_gpu_fallback(
            ["ffmpeg", "-y", "-i", working_path,
             "-filter_complex", filter_chain,
             "-map", "[outv]", "-map", "0:a",
             "-threads", str(FFMPEG_THREAD_COUNT)]
            + _encode_params() + ["-c:a", "copy", out_path],
            "finishing_effects"
        )
    except Exception as e:
        logger.warning(f"Finishing effects gagal ({e}), skip")
        shutil.copy2(working_path, out_path)
    finally:
        if working_path != input_path and os.path.exists(working_path):
            os.remove(working_path)


# ══════════════════════════════════════════════════════════════════════════════
#  AUDIO PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def _mix_audio_with_ducking(narration_path: str, music_path: str,
                            out_path: str, music_volume: float, duration: float):
    """
    Mix narasi + musik dengan sidechain ducking:
    - Musik otomatis turun saat narasi terdeteksi
    - Fade-in/out musik di awal/akhir
    - Fallback ke simple mix jika sidechain gagal
    """
    music_fadeout_start = max(0, duration - AUDIO_MUSIC_FADEOUT_LEAD)

    try:
        if _get_duration(music_path) < 1:
            return _copy_narration_only(narration_path, out_path, duration)
    except Exception:
        return _copy_narration_only(narration_path, out_path, duration)

    ducking_filter = (
        f"[1:a]volume={music_volume},"
        f"afade=t=in:st=0:d={AUDIO_MUSIC_FADEIN_DUR},"
        f"afade=t=out:st={music_fadeout_start}:d={AUDIO_MUSIC_FADEOUT_DUR},"
        f"atrim=0:{duration},asetpts=PTS-STARTPTS[music_prep];"
        f"[0:a]asplit=2[narration_out][narration_sc];"
        f"[music_prep][narration_sc]sidechaincompress"
        f"=threshold={AUDIO_DUCKING_THRESHOLD}"
        f":ratio={AUDIO_DUCKING_RATIO}"
        f":attack={AUDIO_DUCKING_ATTACK_MS}"
        f":release={AUDIO_DUCKING_RELEASE_MS}"
        f":level_sc=0.9[music_ducked];"
        f"[narration_out][music_ducked]amix=inputs=2"
        f":duration=first:dropout_transition=2[aout]"
    )

    cmd = [
        "ffmpeg", "-y", "-i", narration_path, "-i", music_path,
        "-filter_complex", ducking_filter,
        "-map", "[aout]",
        "-c:a", "libmp3lame", "-q:a", "2",
        "-threads", str(FFMPEG_THREAD_COUNT),
        out_path
    ]
    try:
        _run_ffmpeg(cmd, "mix_audio_ducking")
    except Exception:
        logger.warning("Sidechain ducking gagal, fallback ke simple mix")
        _mix_audio_simple(narration_path, music_path, out_path, music_volume, duration)


def _mix_audio_simple(narration_path: str, music_path: str,
                      out_path: str, music_volume: float, duration: float):
    """Fallback mix tanpa sidechain — volume musik statis."""
    music_fadeout_start = max(0, duration - 3)
    cmd = [
        "ffmpeg", "-y", "-i", narration_path, "-i", music_path,
        "-filter_complex",
        f"[1:a]volume={music_volume},"
        f"afade=t=in:st=0:d={AUDIO_MUSIC_FADEIN_DUR},"
        f"afade=t=out:st={music_fadeout_start}:d=3,"
        f"atrim=0:{duration},asetpts=PTS-STARTPTS[music];"
        f"[0:a][music]amix=inputs=2:duration=first:dropout_transition=2[aout]",
        "-map", "[aout]",
        "-c:a", "libmp3lame", "-q:a", "2",
        "-threads", str(FFMPEG_THREAD_COUNT),
        out_path
    ]
    try:
        _run_ffmpeg(cmd, "mix_audio_simple")
    except Exception:
        _copy_narration_only(narration_path, out_path, duration)


def _copy_narration_only(narration_path: str, out_path: str, duration: float):
    """Fallback terakhir: hanya copy narasi tanpa musik."""
    _run_ffmpeg(
        ["ffmpeg", "-y", "-i", narration_path,
         "-t", str(duration), "-c:a", "libmp3lame", "-q:a", "2", out_path],
        "copy_narration"
    )


def _replace_video_audio(video_path: str, audio_path: str, out_path: str):
    """Ganti audio track video dengan audio baru tanpa re-encode video."""
    _run_ffmpeg(
        ["ffmpeg", "-y", "-i", video_path, "-i", audio_path,
         "-map", "0:v", "-map", "1:a",
         "-c:v", "copy", "-c:a", "aac",
         "-b:a", AUDIO_OUTPUT_BITRATE,
         "-ar", str(AUDIO_OUTPUT_SAMPLERATE),
         "-shortest", out_path],
        "replace_audio"
    )


def _sync_video_to_audio(video_path: str, audio_path: str, out_path: str):
    """Loop video (stream_loop) sampai pas dengan durasi audio."""
    audio_duration = _get_duration(audio_path)
    _run_ffmpeg_with_gpu_fallback(
        ["ffmpeg", "-y",
         "-stream_loop", "-1", "-i", video_path,
         "-i", audio_path,
         "-t", str(audio_duration),
         "-map", "0:v", "-map", "1:a",
         "-threads", str(FFMPEG_THREAD_COUNT)]
        + _encode_params() + ["-c:a", "aac", "-shortest", out_path],
        "sync_video_to_audio"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  LOUDNORM (EBU R128)
# ══════════════════════════════════════════════════════════════════════════════

def _apply_loudnorm(input_path: str, out_path: str):
    """
    Standarisasi volume ke target LUFS YouTube via 2-pass loudnorm.
    Pass 1: analisis level audio saat ini
    Pass 2: apply normalisasi dengan parameter terukur (lebih akurat dari 1-pass)

    Target: AUDIO_LOUDNORM_TARGET LUFS (-14), TP: AUDIO_LOUDNORM_TP (-1.5 dBTP)
    """
    import re as _re

    # Pass 1: Analisis
    analyze_cmd = [
        "ffmpeg", "-y", "-i", input_path,
        "-af", (f"loudnorm=I={AUDIO_LOUDNORM_TARGET}:"
                f"TP={AUDIO_LOUDNORM_TP}:"
                f"LRA={AUDIO_LOUDNORM_LRA}:print_format=json"),
        "-f", "null", "-"
    ]
    result   = subprocess.run(analyze_cmd, capture_output=True, text=True)
    measured = {}
    try:
        match = _re.search(r'\{[^}]+\}', result.stderr, _re.DOTALL)
        if match:
            measured = json.loads(match.group())
    except Exception:
        pass

    # Pass 2: Apply dengan parameter terukur (2-pass lebih akurat)
    if measured.get("input_i"):
        loudnorm_filter = (
            f"loudnorm=I={AUDIO_LOUDNORM_TARGET}"
            f":TP={AUDIO_LOUDNORM_TP}"
            f":LRA={AUDIO_LOUDNORM_LRA}"
            f":measured_I={measured.get('input_i', '-23.0')}"
            f":measured_TP={measured.get('input_tp', '-2.0')}"
            f":measured_LRA={measured.get('input_lra', '7.0')}"
            f":measured_thresh={measured.get('input_thresh', '-33.0')}"
            f":offset={measured.get('target_offset', '0.0')}"
            f":linear=true:print_format=summary"
        )
    else:
        # Fallback 1-pass jika analisis gagal
        loudnorm_filter = (
            f"loudnorm=I={AUDIO_LOUDNORM_TARGET}"
            f":TP={AUDIO_LOUDNORM_TP}"
            f":LRA={AUDIO_LOUDNORM_LRA}"
        )

    _run_ffmpeg(
        ["ffmpeg", "-y", "-i", input_path,
         "-af", loudnorm_filter,
         "-c:v", "copy",
         "-c:a", "aac",
         "-b:a", AUDIO_OUTPUT_BITRATE,
         "-ar", str(AUDIO_OUTPUT_SAMPLERATE),
         out_path],
        "loudnorm"
    )