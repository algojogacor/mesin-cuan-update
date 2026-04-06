"""
edit.py - Entry point mesin_edit

Jalankan:
  python edit.py --mode tiktok
  python edit.py --mode podcast
  python edit.py --mode cinematic --music assets/music/track.mp3

  # Custom input folder
  python edit.py --mode tiktok --input input/tiktok/batch_01/

  # Dengan subtitle
  python edit.py --mode tiktok --subtitle

  # Dry run (analisis saja, tidak render)
  python edit.py --mode tiktok --dry-run

Flags:
  --mode          tiktok | podcast | cinematic  (WAJIB)
  --input         Folder/file video (default: input/<mode>/)
  --music         Path file musik (wajib untuk mode cinematic)
  --aspect        9:16 | 16:9 | 1:1             (default: 9:16)
  --model         Gemini model                   (default: gemini-2.5-flash)
  --min-clips     Min clips per video            (default: 2)
  --max-clips     Max total clips                (default: 10)
  --transition    cut | fade                     (default: cut)
  --subtitle      Tambahkan subtitle             (default: OFF)
  --dry-run       Analisis saja, tidak render
"""

import os
import sys
import json
import re
import requests
import argparse
import shutil
import platform
import subprocess
import tempfile

from engine.utils import get_logger, timestamp, save_json

logger = get_logger("edit")

VALID_MODES    = ["tiktok", "podcast", "cinematic"]
SUPPORTED_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}
IS_WINDOWS     = platform.system() == "Windows"

# Word highlight colors (cycling per kalimat) — sama seperti video_engine.py
HIGHLIGHT_COLORS = [
    "&H0000FFFF",  # Kuning
    "&H00FFFFFF",  # Putih
    "&H0055FFFF",  # Kuning-oranye
    "&H00AAFFFF",  # Kuning muda
]


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="mesin_edit — AI-powered Video Editor",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--mode",       required=True, choices=VALID_MODES)
    parser.add_argument("--input",      default=None)
    parser.add_argument("--music",      default=None)
    parser.add_argument("--aspect",     default="9:16", choices=["9:16", "16:9", "1:1"])
    parser.add_argument("--model",      default="gemini-2.5-flash")
    parser.add_argument("--min-clips",  type=int, default=2)
    parser.add_argument("--max-clips",  type=int, default=10)
    parser.add_argument("--transition", default="cut", choices=["cut", "fade"])
    parser.add_argument("--subtitle",   action="store_true")
    parser.add_argument("--dry-run",    action="store_true")
    args = parser.parse_args()

    if args.input is None:
        args.input = os.path.join("input", args.mode)
        logger.info(f"--input tidak ditulis, pakai default: {args.input}")

    video_paths = _scan_input(args.input)
    if not video_paths:
        logger.error(f"Tidak ada video ditemukan di: {args.input}")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info(f"mesin_edit | mode={args.mode.upper()} | {len(video_paths)} video input")
    logger.info("=" * 60)
    for p in video_paths:
        logger.info(f"  📹 {os.path.basename(p)}")

    ts       = timestamp()
    out_base = os.path.join("output", args.mode, ts)
    tmp_dir  = os.path.join(out_base, "tmp")
    os.makedirs(out_base, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)

    gemini_options = {
        "model"              : args.model,
        "min_clips_per_video": args.min_clips,
        "max_total_clips"    : args.max_clips,
    }
    compile_options = {
        "aspect"    : args.aspect,
        "transition": args.transition,
    }

    try:
        if args.mode == "tiktok":
            _run_tiktok(video_paths, out_base, tmp_dir, args, gemini_options, compile_options)
        elif args.mode == "podcast":
            _run_podcast(video_paths, out_base, tmp_dir, args, gemini_options, compile_options)
        elif args.mode == "cinematic":
            _run_cinematic(video_paths, out_base, tmp_dir, args, gemini_options, compile_options)
    except KeyboardInterrupt:
        logger.warning("Dibatalkan oleh user.")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info(f"✅ Selesai! Output: {out_base}/")


# ─── Mode: TikTok Highlight ────────────────────────────────────────────────────

def _run_tiktok(video_paths, out_base, tmp_dir, args, gemini_opts, compile_opts):
    from engine import ai_engine as gemini_engine, cut_engine, compile_engine

    logger.info("📡 Step 1/4: Analisis video dengan Gemini...")
    clips = gemini_engine.analyze(video_paths, mode="tiktok", options=gemini_opts)

    if not clips:
        logger.error("Tidak ada clips ditemukan. Coba --min-clips 1 atau ganti model.")
        return

    logger.info(f"🎯 {len(clips)} clips dipilih")
    save_json({"clips": clips, "mode": "tiktok"}, os.path.join(out_base, "analysis.json"))
    _print_summary(clips)

    if args.dry_run:
        logger.info("[DRY RUN] Selesai. Tidak ada render.")
        return

    clips = cut_engine.validate_clips_against_source(clips)

    logger.info("✂️  Step 2/4: Memotong clips...")
    cut_clips = cut_engine.cut_clips(clips, tmp_dir)

    if not cut_clips:
        logger.error("Tidak ada clips yang berhasil dipotong!")
        return

    logger.info("🎬 Step 3/4: Mengompilasi video...")
    out_video = os.path.join(out_base, "highlight.mp4")
    compile_engine.compile_clips(cut_clips, out_video, compile_opts)

    if args.subtitle:
        logger.info("🎨 Step 4/4: Menambahkan subtitle...")
        _add_subtitle(out_video, tmp_dir)
    else:
        logger.info("🎬 Step 4/4: Subtitle dilewati (pakai --subtitle untuk mengaktifkan)")

    logger.info(f"✅ TikTok highlight selesai: {out_video}")


# ─── Mode: Podcast Clipper ────────────────────────────────────────────────────

def _run_podcast(video_paths, out_base, tmp_dir, args, gemini_opts, compile_opts):
    from engine import ai_engine as gemini_engine, cut_engine, compile_engine

    logger.info("📡 Step 1/4: Analisis podcast dengan AI...")
    clips = gemini_engine.analyze(video_paths, mode="podcast", options=gemini_opts)

    if not clips:
        logger.error("Tidak ada clips ditemukan!")
        return

    logger.info(f"🎯 {len(clips)} clips dipilih")
    save_json({"clips": clips, "mode": "podcast"}, os.path.join(out_base, "analysis.json"))
    _print_summary(clips)

    if args.dry_run:
        logger.info("[DRY RUN] Selesai.")
        return

    clips = cut_engine.validate_clips_against_source(clips)

    logger.info("✂️  Step 2/4: Memotong clips...")
    cut_clips = cut_engine.cut_clips(clips, tmp_dir)

    logger.info(f"🎬 Step 3/4: Render + enhance {len(cut_clips)} video...")
    for i, clip in enumerate(cut_clips):
        if "clip_path" not in clip:
            continue

        out_path = os.path.join(out_base, f"clip_{i+1:02d}.mp4")

        # Render dulu (normalize aspect ratio)
        compile_engine.compile_clips([clip], out_path, compile_opts)

        # Step 4: Enhance — subtitle ASS + hook text + loudnorm
        logger.info(f"✨ Step 4/4: Enhance clip {i+1}/{len(cut_clips)}...")
        _enhance_podcast_clip(
            video_path=out_path,
            hook_text=clip.get("hook", ""),
            tmp_dir=tmp_dir,
            clip_index=i + 1,
        )

        logger.info(f"  📹 Clip {i+1}: {os.path.basename(out_path)}")

    logger.info(f"✅ Podcast clipper selesai: {len(cut_clips)} video di {out_base}/")


# ─── Mode: Cinematic Beat-Sync ────────────────────────────────────────────────

def _run_cinematic(video_paths, out_base, tmp_dir, args, gemini_opts, compile_opts):
    from engine import ai_engine as gemini_engine, cut_engine, compile_engine, beat_sync_engine

    if not args.music:
        logger.error("Mode cinematic butuh --music <path_file_musik>")
        return
    if not os.path.exists(args.music):
        logger.error(f"File musik tidak ditemukan: {args.music}")
        return

    logger.info("📡 Step 1/5: Analisis video dengan Gemini...")
    clips = gemini_engine.analyze(video_paths, mode="cinematic", options=gemini_opts)

    if not clips:
        logger.error("Tidak ada clips ditemukan!")
        return

    logger.info(f"🎯 {len(clips)} clips dipilih")
    logger.info("🥁 Step 2/5: Deteksi beat dari musik...")
    beat_times = beat_sync_engine.extract_beat_timestamps(args.music)
    clips      = beat_sync_engine.apply_beat_durations(clips, beat_times)
    save_json({"clips": clips, "mode": "cinematic"}, os.path.join(out_base, "analysis.json"))
    _print_summary(clips)

    if args.dry_run:
        logger.info("[DRY RUN] Selesai.")
        return

    clips     = cut_engine.validate_clips_against_source(clips)

    logger.info("✂️  Step 3/5: Memotong clips (beat-synced)...")
    cut_clips = cut_engine.cut_clips(clips, tmp_dir)

    logger.info("🎬 Step 4/5: Mengompilasi video...")
    out_raw = os.path.join(tmp_dir, "cinematic_raw.mp4")
    compile_engine.compile_clips(cut_clips, out_raw, compile_opts)

    logger.info("🎵 Step 5/5: Mixing dengan musik...")
    out_final = os.path.join(out_base, "cinematic_final.mp4")
    compile_engine.mix_music_replace(out_raw, args.music, out_final)

    logger.info(f"✅ Cinematic selesai: {out_final}")


# ─── Podcast Enhancement Pipeline ────────────────────────────────────────────
#
# Chain: video → [ASS subtitle] → [hook text] → [loudnorm] → final
#
# Semua step in-place: file out_path di-replace dengan versi yang sudah di-enhance.

def _enhance_podcast_clip(video_path: str, hook_text: str,
                           tmp_dir: str, clip_index: int = 1):
    """
    Jalankan full enhancement pipeline untuk satu podcast clip:
      1. Transcribe → ASS subtitle word-by-word highlight → burn
      2. Hook text overlay di 3 detik pertama
      3. Loudnorm ke -14 LUFS (standar YouTube/TikTok)

    File video_path di-replace in-place dengan versi final.
    """
    base, ext = os.path.splitext(video_path)
    tag       = f"_{clip_index:02d}"

    # ── Step 1: ASS Subtitle ─────────────────────────────────────────────────
    sub_path = _generate_ass_subtitle(video_path, tmp_dir, tag)
    if sub_path:
        subtitled = f"{base}_sub{tag}{ext}"
        _burn_ass_subtitle(video_path, sub_path, subtitled)
        if os.path.exists(subtitled):
            os.replace(subtitled, video_path)
            logger.info(f"  [enhance] ✅ Subtitle burned")
        else:
            logger.warning(f"  [enhance] Subtitle burn gagal, lanjut tanpa subtitle")
    else:
        logger.warning(f"  [enhance] Transcribe gagal, skip subtitle")

    # ── Step 2: Hook Text ────────────────────────────────────────────────────
    if hook_text:
        hooked = f"{base}_hook{tag}{ext}"
        _burn_hook_text(video_path, hooked, hook_text)
        if os.path.exists(hooked):
            os.replace(hooked, video_path)
            logger.info(f"  [enhance] ✅ Hook text: \"{hook_text}\"")
        else:
            logger.warning(f"  [enhance] Hook text gagal, lanjut tanpa hook")
    else:
        logger.info(f"  [enhance] Hook text kosong, dilewati")

    # ── Step 3: Loudnorm ─────────────────────────────────────────────────────
    normed = f"{base}_norm{tag}{ext}"
    _loudnorm(video_path, normed)
    if os.path.exists(normed):
        os.replace(normed, video_path)
        logger.info(f"  [enhance] ✅ Loudnorm -14 LUFS")
    else:
        logger.warning(f"  [enhance] Loudnorm gagal, pakai audio original")


# ─── ASS Subtitle Generator ───────────────────────────────────────────────────

def _generate_ass_subtitle(video_path: str, tmp_dir: str, tag: str = "") -> str | None:
    """
    Transcribe video dengan Whisper → generate file .ass dengan word-by-word highlight.
    Return path file .ass, atau None kalau gagal.
    """
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        logger.warning("[subtitle] faster-whisper tidak terinstall — skip")
        return None

    try:
        # Pakai model small untuk kecepatan (podcast sudah punya teks jelas)
        logger.info("  [subtitle] Transcribing dengan Whisper small...")
        model       = WhisperModel("small", device="auto", compute_type="int8")
        segs, info  = model.transcribe(video_path, word_timestamps=False)
        raw         = [
            {"start": s.start, "end": s.end, "text": s.text.strip()}
            for s in segs if s.text.strip()
        ]
        logger.info(f"  [subtitle] {len(raw)} segments | lang={info.language}")
    except Exception as e:
        logger.warning(f"  [subtitle] Whisper error: {e}")
        return None

    if not raw:
        logger.warning("  [subtitle] Tidak ada teks terdeteksi")
        return None

    # Merge segment sangat pendek (<1s) ke segment berikutnya
    sentences = _merge_short_segments(raw)

    # Deteksi dimensi video untuk font size
    w, h      = _get_video_dimensions(video_path)
    is_shorts = (h > w)  # portrait = Shorts/TikTok

    ass_path = os.path.join(tmp_dir, f"sub{tag}.ass")
    _write_ass_file(sentences, ass_path, w, h, is_shorts)
    return ass_path


def _merge_short_segments(segments: list) -> list:
    """Gabungkan segment < 1 detik ke segment berikutnya."""
    merged = []
    buf    = None
    for s in segments:
        if buf is None:
            buf = s.copy()
        elif (s["end"] - s["start"]) < 1.0:
            buf["end"]  = s["end"]
            buf["text"] += " " + s["text"]
        else:
            merged.append(buf)
            buf = s.copy()
    if buf:
        merged.append(buf)
    return merged if merged else segments


def _write_ass_file(sentences: list, out_path: str, w: int, h: int, is_shorts: bool):
    """
    Tulis file .ass dengan:
    - Font besar (62px shorts / 52px landscape)
    - Safe zone 25% dari bawah (aman dari UI TikTok/Shorts)
    - Word-by-word highlight: kata aktif kuning besar, kata lain abu-abu kecil
    """
    font_size = 62 if is_shorts else 52
    margin_v  = int(h * 0.25)  # 25% dari bawah

    header = (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        f"PlayResX: {w}\n"
        f"PlayResY: {h}\n"
        "WrapStyle: 1\n"
        "ScaledBorderAndShadow: yes\n"
        "\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Default,Arial Black,{font_size},"
        "&H00FFFFFF,&H000000FF,&H00000000,&H80000000,"
        "-1,0,0,0,100,100,2,0,1,3,2,"
        f"2,30,30,{margin_v},1\n"
        "\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
    )

    events = ""
    for sent_idx, sent in enumerate(sentences):
        text = sent["text"].strip()
        if not text:
            continue

        words    = text.upper().split()
        start    = sent["start"]
        end      = sent["end"]
        dur      = max(end - start, 0.1)
        word_dur = dur / max(len(words), 1)
        chunk_sz = 4
        chunks   = [words[i:i + chunk_sz] for i in range(0, len(words), chunk_sz)]
        highlight = HIGHLIGHT_COLORS[sent_idx % len(HIGHLIGHT_COLORS)]

        word_global_idx = 0
        for chunk in chunks:
            for wi, _ in enumerate(chunk):
                w_start = start + word_global_idx * word_dur
                w_end   = min(w_start + word_dur, end)
                if w_end - w_start < 0.05:
                    w_end = w_start + 0.05

                parts = []
                for j, cw in enumerate(chunk):
                    if j == wi:
                        # Kata aktif: warna highlight, sedikit lebih besar
                        parts.append(
                            r"{\fscx110\fscy110\1c" + highlight +
                            r"\3c&H00000000&\bord4\shad2}" + cw
                        )
                    else:
                        # Kata lain: abu-abu, normal size
                        parts.append(
                            r"{\fscx100\fscy100\1c&H00999999&\3c&H00000000&\bord2\shad1}" + cw
                        )

                line    = "  ".join(parts)
                events += (
                    f"Dialogue: 0,{_ass_ts(w_start)},{_ass_ts(w_end)},"
                    f"Default,,0,0,0,,{line}\n"
                )
                word_global_idx += 1

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(header + events)


def _ass_ts(seconds: float) -> str:
    """Convert detik → format timestamp ASS (H:MM:SS.cc)"""
    h  = int(seconds // 3600)
    m  = int((seconds % 3600) // 60)
    s  = int(seconds % 60)
    cs = int((seconds % 1) * 100)
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


def _burn_ass_subtitle(video_path: str, out_path: str, ass_path: str):
    """Burn file .ass ke video via FFmpeg."""
    # Windows: escape path untuk FFmpeg subtitle filter
    ass_ffmpeg = ass_path.replace("\\", "/")
    if IS_WINDOWS and len(ass_ffmpeg) > 1 and ass_ffmpeg[1] == ":":
        ass_ffmpeg = ass_ffmpeg[0] + "\\:" + ass_ffmpeg[2:]

    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", f"ass='{ass_ffmpeg}'",
        "-c:v", "libx264", "-crf", "23", "-preset", "fast",
        "-c:a", "copy",
        out_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"  [subtitle] FFmpeg error: {result.stderr[-300:]}")


# ─── Hook Text ────────────────────────────────────────────────────────────────

def _burn_hook_text(video_path: str, out_path: str, hook_text: str):
    """
    Overlay teks hook di tengah-atas layar selama 3 detik pertama.
    Style: bold besar, background semi-transparan, shadow.
    """
    # Escape karakter khusus FFmpeg drawtext
    escaped = (hook_text.upper()
               .replace("\\", "\\\\")
               .replace("'",  "\\'")
               .replace(":",  "\\:")
               .replace("%",  "\\%"))

    # Cari font bold yang tersedia (Windows)
    font_candidates = [
        ("C\\:/Windows/Fonts/impact.ttf",   "C:/Windows/Fonts/impact.ttf"),
        ("C\\:/Windows/Fonts/arialbd.ttf",  "C:/Windows/Fonts/arialbd.ttf"),
        ("C\\:/Windows/Fonts/calibrib.ttf", "C:/Windows/Fonts/calibrib.ttf"),
    ]
    fontfile_param = ""
    for ffmpeg_path, real_path in font_candidates:
        if os.path.exists(real_path):
            fontfile_param = f"fontfile='{ffmpeg_path}'"
            break

    # Build drawtext filter
    params = [p for p in [
        fontfile_param,
        f"text='{escaped}'",
        "fontsize=72",
        "fontcolor=white",
        "x=(w-text_w)/2",
        "y=h*0.08",              # 8% dari atas
        "shadowx=3",
        "shadowy=3",
        "shadowcolor=black@0.9",
        "box=1",
        "boxcolor=black@0.6",
        "boxborderw=20",
        "enable='between(t,0,3)'"
    ] if p]

    drawtext_filter = ":".join(params)

    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", drawtext_filter,
        "-c:v", "libx264", "-crf", "23", "-preset", "fast",
        "-c:a", "copy",
        out_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"  [hook] FFmpeg error: {result.stderr[-200:]}")


# ─── Loudnorm ─────────────────────────────────────────────────────────────────

def _loudnorm(video_path: str, out_path: str, target_lufs: float = -14.0):
    """
    Normalize audio ke target LUFS (default -14 LUFS, standar YouTube/TikTok).
    Two-pass: analisis dulu → apply dengan nilai terukur (lebih akurat).
    """
    # Pass 1: analisis
    cmd_analyze = [
        "ffmpeg", "-y", "-i", video_path,
        "-af", f"loudnorm=I={target_lufs}:TP=-1.5:LRA=11:print_format=json",
        "-f", "null", "-"
    ]
    result  = subprocess.run(cmd_analyze, capture_output=True, text=True)
    measured = {}

    try:
        match = re.search(r'\{[^}]+\}', result.stderr, re.DOTALL)
        if match:
            measured = json.loads(match.group())
    except Exception:
        pass

    # Pass 2: apply
    if measured.get("input_i"):
        af = (
            f"loudnorm=I={target_lufs}:TP=-1.5:LRA=11:"
            f"measured_I={measured.get('input_i', '-23.0')}:"
            f"measured_TP={measured.get('input_tp', '-2.0')}:"
            f"measured_LRA={measured.get('input_lra', '7.0')}:"
            f"measured_thresh={measured.get('input_thresh', '-33.0')}:"
            f"offset={measured.get('target_offset', '0.0')}:linear=true"
        )
    else:
        # Fallback single-pass kalau parse gagal
        af = f"loudnorm=I={target_lufs}:TP=-1.5:LRA=11"

    cmd_apply = [
        "ffmpeg", "-y", "-i", video_path,
        "-af", af,
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k",
        out_path
    ]
    result2 = subprocess.run(cmd_apply, capture_output=True, text=True)
    if result2.returncode != 0:
        logger.warning(f"  [loudnorm] FFmpeg error: {result2.stderr[-200:]}")


# ─── Legacy Subtitle (untuk --subtitle flag di tiktok/cinematic) ──────────────

def _add_subtitle(video_path: str, tmp_dir: str, suffix: str = ""):
    """
    Subtitle sederhana via Whisper + SRT untuk tiktok/cinematic mode.
    (Podcast mode pakai _enhance_podcast_clip yang lebih lengkap)
    """
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        logger.warning("[subtitle] faster-whisper tidak terinstall — skip")
        return

    try:
        logger.info("[subtitle] Transcribing dengan Whisper base...")
        model        = WhisperModel("base", device="auto", compute_type="int8")
        segs, _      = model.transcribe(video_path, word_timestamps=False)
        srt_path     = os.path.join(tmp_dir, f"sub{suffix}.srt")
        seg_list     = list(segs)

        if not seg_list:
            logger.warning("[subtitle] Tidak ada teks terdeteksi, skip subtitle")
            return

        with open(srt_path, "w", encoding="utf-8") as f:
            for i, seg in enumerate(seg_list, 1):
                start = _sec_to_srt(seg.start)
                end   = _sec_to_srt(seg.end)
                f.write(f"{i}\n{start} --> {end}\n{seg.text.strip()}\n\n")

        logger.info(f"[subtitle] {len(seg_list)} segments → {srt_path}")

        srt_ffmpeg = srt_path
        if IS_WINDOWS:
            srt_ffmpeg = srt_path.replace("\\", "/")
            if len(srt_ffmpeg) > 1 and srt_ffmpeg[1] == ":":
                srt_ffmpeg = srt_ffmpeg[0] + "\\:" + srt_ffmpeg[2:]

        out_path = video_path.replace(".mp4", f"_sub{suffix}.mp4")
        cmd = [
            "ffmpeg", "-y", "-i", video_path,
            "-vf", (
                f"subtitles='{srt_ffmpeg}':force_style='"
                "FontName=Arial Black,FontSize=18,"
                "PrimaryColour=&H00FFFFFF,"
                "OutlineColour=&H00000000,"
                "Outline=3,Shadow=2,"
                "Alignment=2,MarginV=120'"
            ),
            "-c:a", "copy",
            "-c:v", "libx264", "-crf", "23", "-preset", "fast",
            out_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0 and os.path.exists(out_path):
            os.replace(out_path, video_path)
            logger.info("[subtitle] ✅ Subtitle berhasil ditambahkan")
        else:
            logger.warning(f"[subtitle] FFmpeg gagal: {result.stderr[-300:]}")

    except Exception as e:
        logger.warning(f"[subtitle] Error: {e} — skip subtitle")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_video_dimensions(video_path: str) -> tuple[int, int]:
    """Return (width, height) via ffprobe. Default ke (1080, 1920) kalau gagal."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error",
             "-select_streams", "v:0",
             "-show_entries", "stream=width,height",
             "-of", "csv=p=0", video_path],
            capture_output=True, text=True
        )
        w, h = result.stdout.strip().split(",")
        return int(w), int(h)
    except Exception:
        return 1080, 1920


def _scan_input(path: str) -> list:
    if os.path.isfile(path):
        ext = os.path.splitext(path)[1].lower()
        return [os.path.abspath(path)] if ext in SUPPORTED_EXTS else []

    if os.path.isdir(path):
        files = []
        for fname in sorted(os.listdir(path)):
            ext = os.path.splitext(fname)[1].lower()
            if ext in SUPPORTED_EXTS:
                files.append(os.path.abspath(os.path.join(path, fname)))
        return files

    return []


def _print_summary(clips: list):
    print(f"\n{'─' * 60}")
    print(f"  CLIPS SUMMARY ({len(clips)} clips terpilih)")
    print(f"{'─' * 60}")
    for i, c in enumerate(clips, 1):
        print(
            f"  [{i:02d}] ⭐{c['score']:.1f}  "
            f"{c['start']} → {c['end']}  ({c['duration']:.1f}s)  "
            f"| {os.path.basename(c['source'])}"
        )
        if c.get("hook"):
            print(f"       💬 {c['hook']}")
    print(f"{'─' * 60}\n")


def _sec_to_srt(sec: float) -> str:
    ms  = int((sec % 1) * 1000)
    sec = int(sec)
    h   = sec // 3600
    m   = (sec % 3600) // 60
    s   = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


if __name__ == "__main__":
    main()