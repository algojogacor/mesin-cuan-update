"""
ai_engine.py - Analisis video via NVIDIA API (OpenAI-compatible, gratis)

Backend:
  - Text (podcast) : NVIDIA API → deepseek-ai/deepseek-v3-0324
  - Vision (tiktok) : NVIDIA API → meta/llama-4-maverick-17b-128e-instruct
  - Fallback        : Ollama lokal (kalau NVIDIA_API_KEY tidak ada)

Setup .env:
  NVIDIA_API_KEY=nvapi-xxxxx
  OLLAMA_BASE_URL=http://localhost:11434  (opsional, untuk fallback)

Install:
  pip install openai
"""

import os
import re
import json
import base64
import shutil
import requests
import tempfile
import subprocess
import concurrent.futures
import platform
import time

from engine.utils import get_logger

logger = get_logger("ai_engine")

VALID_MODES = ["tiktok", "podcast", "cinematic"]

# ── NVIDIA API config ──────────────────────────────────────────────────────────
NVIDIA_BASE_URL   = "https://integrate.api.nvidia.com/v1"
NVIDIA_TEXT_MODEL  = "meta/llama-3.1-70b-instruct"
NVIDIA_VISION_MODEL = "meta/llama-4-maverick-17b-128e-instruct"

# ── Ollama fallback config ─────────────────────────────────────────────────────
OLLAMA_BASE_URL     = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_VISION_MODEL = "qwen3-vl:235b-cloud"

# ── Frame sampling ─────────────────────────────────────────────────────────────
FRAME_INTERVAL_SEC = 3
MAX_FRAMES_BATCH   = 8    # NVIDIA limit: max 10 images per request   # NVIDIA mungkin ada limit ukuran request


# ─── Public Entry Point ────────────────────────────────────────────────────────

def analyze(video_paths: list, mode: str, options: dict = {}) -> list:
    """
    Analisis satu atau banyak video, return list clips.

    Args:
        video_paths : list path video lokal
        mode        : "tiktok" | "podcast" | "cinematic"
        options     : {
            "min_clips_per_video": int  (default: 2)
            "max_total_clips"    : int  (default: 10)
        }
    """
    if mode not in VALID_MODES:
        raise ValueError(f"Mode tidak valid: {mode}. Pilih: {VALID_MODES}")

    # Deteksi backend
    nvidia_key = os.getenv("NVIDIA_API_KEY", "")
    if nvidia_key:
        logger.info(f"[ai] Backend: NVIDIA API ✅")
    else:
        logger.info(f"[ai] Backend: Ollama (NVIDIA_API_KEY tidak ditemukan)")

    min_per_video = options.get("min_clips_per_video", 2)
    max_total     = options.get("max_total_clips", 10)

    logger.info(f"[ai] Analisis {len(video_paths)} video | mode={mode}")

    all_clips = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(_analyze_one, path, mode, min_per_video, nvidia_key): path
            for path in video_paths
        }
        for future in concurrent.futures.as_completed(futures):
            path = futures[future]
            try:
                clips = future.result()
                logger.info(f"[ai] ✅ {os.path.basename(path)}: {len(clips)} clips")
                all_clips.extend(clips)
            except Exception as e:
                logger.error(f"[ai] ❌ {os.path.basename(path)} gagal: {e}")

    if not all_clips:
        logger.warning("[ai] Tidak ada clips yang berhasil dianalisis!")
        return []

    all_clips.sort(key=lambda x: x.get("score", 0), reverse=True)
    if max_total:
        all_clips = all_clips[:max_total]
    all_clips.sort(key=lambda x: (x["source"], x["start_sec"]))

    logger.info(f"[ai] Total clips terpilih: {len(all_clips)}")
    return all_clips


# ─── Router ───────────────────────────────────────────────────────────────────

def _analyze_one(video_path: str, mode: str, min_clips: int, nvidia_key: str) -> list:
    filename     = os.path.basename(video_path)
    duration_sec = _get_video_duration(video_path)
    duration_str = _sec_to_ts(duration_sec)
    logger.info(f"[ai] [{filename}] Durasi: {duration_str} ({duration_sec:.1f}s)")

    if mode == "podcast":
        return _analyze_podcast(video_path, min_clips, duration_sec, nvidia_key)
    else:
        return _analyze_visual(video_path, mode, min_clips, duration_sec, nvidia_key)


# ─── Mode Podcast: Whisper → Text LLM ────────────────────────────────────────

def _analyze_podcast(video_path: str, min_clips: int,
                      duration_sec: float, nvidia_key: str) -> list:
    filename     = os.path.basename(video_path)
    assemblyai_key = os.getenv("ASSEMBLYAI_API_KEY", "")

    # Primary: AssemblyAI (cloud, cepat, tidak pakai GPU)
    if assemblyai_key:
        logger.info(f"[ai] [{filename}] PODCAST → AssemblyAI transkrip (cloud)...")
        try:
            transcript = _assemblyai_transcribe(video_path, assemblyai_key)
            if transcript:
                logger.info(f"[ai] [{filename}] AssemblyAI selesai: {len(transcript)} segments")
            else:
                raise RuntimeError("AssemblyAI return kosong")
        except Exception as e:
            logger.warning(f"[ai] AssemblyAI gagal: {e} — fallback ke Whisper lokal...")
            transcript = _whisper_transcribe_with_guard(video_path)
    else:
        # Fallback: Whisper lokal
        logger.info(f"[ai] [{filename}] PODCAST → Whisper lokal (ASSEMBLYAI_API_KEY tidak ada)...")
        transcript = _whisper_transcribe_with_guard(video_path)

    if not transcript:
        raise RuntimeError("Semua metode transkrip gagal")

    logger.info(f"[ai] [{filename}] Transkrip: {len(transcript)} segments")

    transcript_text = _format_transcript(transcript)
    duration_str    = _sec_to_ts(duration_sec)
    prompt          = _build_podcast_prompt(transcript_text, min_clips, duration_str, duration_sec)

    if nvidia_key:
        logger.info(f"[ai] Kirim ke NVIDIA: {NVIDIA_TEXT_MODEL}")
        response_text = _nvidia_text(nvidia_key, NVIDIA_TEXT_MODEL, prompt)
    else:
        logger.info(f"[ai] Kirim ke Ollama: {get_ollama_model()}")
        response_text = _ollama_text(OLLAMA_TEXT_MODEL, prompt)

    return _parse_response(response_text, video_path, duration_sec)


# ─── Mode TikTok / Cinematic: Frame Sampling → Vision LLM ────────────────────

def _analyze_visual(video_path: str, mode: str, min_clips: int,
                     duration_sec: float, nvidia_key: str) -> list:
    filename = os.path.basename(video_path)
    logger.info(f"[ai] [{filename}] {mode.upper()} → extract frames tiap {FRAME_INTERVAL_SEC}s...")

    tmp_dir = tempfile.mkdtemp(prefix="frames_")
    try:
        frame_times = _extract_frames(video_path, tmp_dir, duration_sec)
        logger.info(f"[ai] [{filename}] {len(frame_times)} frames di-extract")

        if not frame_times:
            raise RuntimeError("Tidak ada frame yang berhasil di-extract")

        # Split jadi batches
        batches = [
            frame_times[i:i + MAX_FRAMES_BATCH]
            for i in range(0, len(frame_times), MAX_FRAMES_BATCH)
        ]

        logger.info(f"[ai] [{filename}] {len(batches)} batch → {NVIDIA_VISION_MODEL if nvidia_key else OLLAMA_VISION_MODEL}")

        all_clips = []
        for i, batch in enumerate(batches):
            batch_start = batch[0]["time_sec"]
            batch_end   = min(batch[-1]["time_sec"] + FRAME_INTERVAL_SEC, duration_sec)
            logger.info(
                f"[ai] Batch {i+1}/{len(batches)} "
                f"[{_sec_to_ts(batch_start)}-{_sec_to_ts(batch_end)}] "
                f"({len(batch)} frames)..."
            )
            try:
                clips = _analyze_frame_batch(
                    batch, mode, min_clips,
                    batch_end - batch_start,
                    video_path, nvidia_key
                )
                all_clips.extend(clips)
                logger.info(f"[ai] Batch {i+1}: {len(clips)} clips")
            except Exception as e:
                logger.error(f"[ai] Batch {i+1} gagal: {e}")

        return all_clips
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _analyze_frame_batch(frames: list, mode: str, min_clips: int,
                           batch_duration: float, source_path: str,
                           nvidia_key: str) -> list:
    # Load frames sebagai base64
    images_b64  = []
    time_labels = []
    for frame in frames:
        with open(frame["path"], "rb") as f:
            images_b64.append(base64.b64encode(f.read()).decode("utf-8"))
        time_labels.append(_sec_to_ts(frame["time_sec"]))

    prompt = _build_visual_prompt(mode, min_clips, time_labels, _sec_to_ts(batch_duration))

    if nvidia_key:
        response_text = _nvidia_vision(nvidia_key, NVIDIA_VISION_MODEL, prompt, images_b64)
    else:
        response_text = _ollama_vision(OLLAMA_VISION_MODEL, prompt, images_b64)

    return _parse_response(response_text, source_path, batch_duration)


# ─── NVIDIA API Calls ─────────────────────────────────────────────────────────

def _nvidia_text(api_key: str, model: str, prompt: str) -> str:
    """Text completion via NVIDIA API (OpenAI-compatible)."""
    try:
        from openai import OpenAI
    except ImportError:
        raise RuntimeError("openai tidak terinstall: pip install openai")

    client = OpenAI(base_url=NVIDIA_BASE_URL, api_key=api_key)

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=2048,
        temperature=0.1,
    )
    return response.choices[0].message.content or ""


def _nvidia_vision(api_key: str, model: str, prompt: str, images_b64: list) -> str:
    """Vision completion via NVIDIA API — kirim frames sebagai base64."""
    try:
        from openai import OpenAI
    except ImportError:
        raise RuntimeError("openai tidak terinstall: pip install openai")

    client = OpenAI(base_url=NVIDIA_BASE_URL, api_key=api_key)

    # Build content list: teks + semua gambar
    content = []
    for b64 in images_b64:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}"}
        })
    content.append({"type": "text", "text": prompt})

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": content}],
        max_tokens=2048,
        temperature=0.1,
    )
    return response.choices[0].message.content or ""


# ─── Ollama Fallback ──────────────────────────────────────────────────────────

def _ollama_text(model: str, prompt: str) -> str:
    import requests
    url     = f"{OLLAMA_BASE_URL}/api/generate"
    payload = {"model": model, "prompt": prompt, "stream": False,
               "options": {"temperature": 0.1}}
    try:
        resp = requests.post(url, json=payload, timeout=300)
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception as e:
        raise RuntimeError(f"Ollama text error: {e}")


def _ollama_vision(model: str, prompt: str, images_b64: list) -> str:
    import requests
    url     = f"{OLLAMA_BASE_URL}/api/generate"
    payload = {"model": model, "prompt": prompt, "images": images_b64,
               "stream": False, "options": {"temperature": 0.1}}
    try:
        resp = requests.post(url, json=payload, timeout=600)
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception as e:
        raise RuntimeError(f"Ollama vision error: {e}")


# ─── Whisper Transcribe ───────────────────────────────────────────────────────

def _ollama_stop():
    """Stop Ollama process sementara biar tidak rebutan GPU dengan Whisper."""
    try:
        if platform.system() == "Windows":
            subprocess.run(["taskkill", "/F", "/IM", "ollama.exe"],
                         capture_output=True)
        else:
            subprocess.run(["pkill", "-f", "ollama"], capture_output=True)
        time.sleep(2)  # tunggu process benar-benar mati
        logger.info("[ai] Ollama di-stop sementara (GPU bebas untuk Whisper)")
    except Exception as e:
        logger.warning(f"[ai] Gagal stop Ollama: {e}")


def _ollama_start():
    """Start Ollama kembali setelah Whisper selesai."""
    try:
        if platform.system() == "Windows":
            subprocess.Popen(
                ["ollama", "serve"],
                creationflags=subprocess.CREATE_NO_WINDOW,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        else:
            subprocess.Popen(
                ["ollama", "serve"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        time.sleep(3)  # tunggu Ollama ready
        logger.info("[ai] Ollama di-start kembali")
    except Exception as e:
        logger.warning(f"[ai] Gagal start Ollama: {e}")


def _assemblyai_transcribe(video_path: str, api_key: str) -> list:
    """
    Transkrip via AssemblyAI API (primary).
    - Upload audio ke AssemblyAI
    - Poll sampai selesai
    - Return list of {"start": float, "end": float, "text": str}
    """
    import tempfile

    base_url = "https://api.assemblyai.com"
    headers  = {"authorization": api_key}

    # Step 1: Extract audio dulu (lebih kecil dari video, upload lebih cepat)
    logger.info("[ai] Extracting audio untuk upload...")
    tmp_audio = tempfile.mktemp(suffix=".mp3")
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vn", "-ar", "16000", "-ac", "1", "-q:a", "5",
        tmp_audio
    ]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg extract audio gagal")

    try:
        # Step 2: Upload audio ke AssemblyAI
        logger.info("[ai] Uploading audio ke AssemblyAI...")
        with open(tmp_audio, "rb") as f:
            upload_resp = requests.post(
                f"{base_url}/v2/upload",
                headers=headers,
                data=f,
                timeout=120
            )
        upload_resp.raise_for_status()
        audio_url = upload_resp.json()["upload_url"]
        logger.info("[ai] Upload selesai, mulai transkrip...")

       # Step 3: Request transkrip (FIXED)
        transcript_resp = requests.post(
            f"{base_url}/v2/transcript",
            headers=headers,
            json={
                "audio_url": audio_url,
                "language_code": "id",
                "speaker_labels": True,
                "speech_models": ["universal-2"]  # <--- Turuti kemauan API (pakai list [])
            },
            timeout=30
        )
        
        # --- BLOK DEBUGGING (Tetap biarkan ini buat jaga-jaga) ---
        if transcript_resp.status_code != 200:
            logger.error(f"AssemblyAI nolak! Alasan spesifik: {transcript_resp.text}")
            transcript_resp.raise_for_status()
        # ------------------------------------------

        transcript_id = transcript_resp.json()["id"]
        transcript_resp.raise_for_status()
        transcript_id = transcript_resp.json()["id"]
        polling_url   = f"{base_url}/v2/transcript/{transcript_id}"

        # Step 4: Poll sampai selesai
        poll_count = 0
        while True:
            poll_resp = requests.get(polling_url, headers=headers, timeout=30).json()
            status    = poll_resp["status"]

            if status == "completed":
                break
            elif status == "error":
                raise RuntimeError(f"AssemblyAI error: {poll_resp.get('error')}")

            poll_count += 1
            if poll_count % 4 == 0:  # log tiap 20 detik
                logger.info(f"[ai] AssemblyAI processing... ({poll_count * 5}s)")
            import time
            time.sleep(5)

        # Step 5: Parse utterances → format segments
        utterances = poll_resp.get("utterances") or []
        if utterances:
            # Kalau ada speaker diarization
            segments = [
                {
                    "start": round(u["start"] / 1000, 2),
                    "end"  : round(u["end"]   / 1000, 2),
                    "text" : u["text"].strip()
                }
                for u in utterances
            ]
        else:
            # Fallback ke words grouping per ~10 detik
            words    = poll_resp.get("words", [])
            segments = _group_words_to_segments(words, interval_sec=10)

        logger.info(f"[ai] AssemblyAI selesai | {len(segments)} segments")
        return segments

    finally:
        try:
            os.remove(tmp_audio)
        except OSError:
            pass


def _group_words_to_segments(words: list, interval_sec: float = 10) -> list:
    """Kelompokkan words dari AssemblyAI menjadi segments per ~interval_sec detik."""
    if not words:
        return []

    segments  = []
    current   = []
    seg_start = words[0]["start"] / 1000

    for word in words:
        word_start = word["start"] / 1000
        if word_start - seg_start >= interval_sec and current:
            segments.append({
                "start": round(seg_start, 2),
                "end"  : round(current[-1]["end"] / 1000, 2),
                "text" : " ".join(w["text"] for w in current)
            })
            current   = [word]
            seg_start = word_start
        else:
            current.append(word)

    if current:
        segments.append({
            "start": round(seg_start, 2),
            "end"  : round(current[-1]["end"] / 1000, 2),
            "text" : " ".join(w["text"] for w in current)
        })

    return segments


def _whisper_transcribe_with_guard(video_path: str) -> list:
    """Whisper lokal dengan stop/start Ollama agar tidak konflik GPU."""
    _ollama_stop()
    try:
        return _whisper_transcribe(video_path)
    finally:
        _ollama_start()


def _whisper_transcribe(video_path: str) -> list:
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        raise RuntimeError("faster-whisper tidak terinstall: pip install faster-whisper")

    import tempfile

    # Extract audio dulu via FFmpeg
    tmp_audio = tempfile.mktemp(suffix=".mp3")
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vn", "-ar", "16000", "-ac", "1", "-q:a", "5",
        tmp_audio
    ]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg extract audio gagal: {result.stderr[-200:]}")

    logger.info("[ai] Loading Whisper model (medium, cuda, int8)...")
    try:
        model = WhisperModel("medium", device="cuda", compute_type="int8")
    except Exception:
        logger.warning("[ai] CUDA gagal, fallback ke CPU...")
        model = WhisperModel("medium", device="cpu", compute_type="int8")

    try:
        segs, info = model.transcribe(tmp_audio, word_timestamps=False)
        result = [{"start": round(s.start, 2), "end": round(s.end, 2), "text": s.text.strip()} for s in segs]
        logger.info(f"[ai] Whisper selesai | lang={info.language} | {len(result)} segments")
        return result
    finally:
        try:
            os.remove(tmp_audio)
        except OSError:
            pass


def _format_transcript(segments: list) -> str:
    return "\n".join(f"[{_sec_to_ts(s['start'])}] {s['text']}" for s in segments)


# ─── Prompts ──────────────────────────────────────────────────────────────────

def _build_podcast_prompt(transcript: str, min_clips: int,
                            duration_str: str, duration_sec: float) -> str:
    template_path = "templates/prompts/edit_podcast.txt"
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            content = f.read()
        content = content.replace("{{MIN_CLIPS}}", str(min_clips))
        content = content.replace("{{DURATION_STR}}", duration_str)
        content = content.replace("{{DURATION_SEC}}", str(int(duration_sec)))
        return content + f"\n\n--- TRANSKRIP ---\n{transcript}"

    return f"""Kamu adalah AI editor podcast profesional.
Analisis transkrip berikut dan temukan {min_clips} hingga 8 kutipan terbaik
yang bisa dijadikan konten Shorts mandiri (30-60 detik).
Durasi video: {duration_str}

Fokus: kutipan powerful, insight kunci, statement kontroversial, momen memorable.
Tiap clip harus bisa berdiri sendiri tanpa perlu konteks dari bagian lain.

Return HANYA JSON valid. Jangan ada teks lain, jangan markdown, jangan <think>:
{{
  "clips": [
    {{
      "start": "HH:MM:SS",
      "end": "HH:MM:SS",
      "score": 9.5,
      "reason": "Kenapa kutipan ini powerful",
      "hook": "Hook pendek max 10 kata"
    }}
  ]
}}

Rules: durasi clip 20-60 detik, tidak overlap, urutkan score tertinggi.

--- TRANSKRIP ---
{transcript}"""


def _build_visual_prompt(mode: str, min_clips: int,
                           time_labels: list, duration_str: str) -> str:
    frame_list = "\n".join(f"Frame {i+1}: [{ts}]" for i, ts in enumerate(time_labels))
    hint = {
        "tiktok"   : "Fokus: momen lucu, reaksi kuat, dramatis, mengejutkan — layak viral TikTok/Shorts.",
        "cinematic": "Fokus: shot visual indah, komposisi bagus, gerakan dinamis, momen sinematik."
    }.get(mode, "")

    return f"""Kamu adalah AI video editor profesional.
Aku berikan {len(time_labels)} frame dari sebuah video (1 frame tiap 3 detik).
{hint}

Temukan {min_clips} hingga 5 momen terbaik berdasarkan frame yang kamu lihat.
Gunakan timestamp frame sebagai acuan untuk start/end tiap clip.

Frame timestamps:
{frame_list}

Return HANYA JSON valid. Jangan ada teks lain, jangan markdown:
{{
  "clips": [
    {{
      "start": "HH:MM:SS",
      "end": "HH:MM:SS",
      "score": 9.5,
      "reason": "Kenapa momen ini menarik",
      "hook": "Caption pendek max 10 kata"
    }}
  ]
}}

Rules: durasi clip min 9 detik, tidak overlap, urutkan score tertinggi."""


# ─── Frame Extractor ──────────────────────────────────────────────────────────

def _extract_frames(video_path: str, out_dir: str, duration_sec: float) -> list:
    out_pattern = os.path.join(out_dir, "frame_%05d.jpg")
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", f"fps=1/{FRAME_INTERVAL_SEC},scale=640:-2",
        "-q:v", "3",
        out_pattern
    ]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        logger.error(f"[ai] FFmpeg extract gagal: {result.stderr[-300:]}")
        return []

    frames = []
    for fname in sorted(os.listdir(out_dir)):
        if not fname.endswith(".jpg"):
            continue
        idx      = int(fname.replace("frame_", "").replace(".jpg", "")) - 1
        time_sec = round(idx * FRAME_INTERVAL_SEC, 2)
        if time_sec <= duration_sec:
            frames.append({"time_sec": time_sec, "path": os.path.join(out_dir, fname)})
    return frames


# ─── Response Parser ──────────────────────────────────────────────────────────

def _parse_response(text: str, source_path: str, video_duration: float = 0) -> list:
    # Strip thinking tags (deepseek style)
    text    = re.sub(r"<think>[\s\S]*?</think>", "", text).strip()
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
        logger.error(f"[ai] Gagal parse JSON:\n{text[:500]}")
        return []

    result = []
    for i, clip in enumerate(data.get("clips", [])):
        try:
            start_sec = _ts_to_sec(clip["start"])
            end_sec   = _ts_to_sec(clip["end"])
            if video_duration > 0 and end_sec > video_duration:
                end_sec = video_duration
            if end_sec <= start_sec:
                logger.warning(f"[ai] Clip {i+1} skip: end <= start")
                continue
            duration = round(end_sec - start_sec, 2)
            if duration < 3:
                logger.warning(f"[ai] Clip {i+1} skip: durasi {duration}s terlalu pendek")
                continue
            result.append({
                "source"   : source_path,
                "start"    : _sec_to_ts(start_sec),
                "end"      : _sec_to_ts(end_sec),
                "start_sec": start_sec,
                "end_sec"  : end_sec,
                "duration" : duration,
                "score"    : float(clip.get("score", 5.0)),
                "reason"   : clip.get("reason", ""),
                "hook"     : clip.get("hook", ""),
            })
        except (KeyError, ValueError) as e:
            logger.warning(f"[ai] Clip {i+1} error: {e} — {clip}")
    return result


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
        raise ValueError(f"Timestamp tidak valid: '{ts}'")


def _sec_to_ts(sec: float) -> str:
    sec = int(sec)
    h, m, s = sec // 3600, (sec % 3600) // 60, sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _get_video_duration(path: str) -> float:
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
           "-of", "default=noprint_wrappers=1:nokey=1", path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return float(result.stdout.strip())
    except ValueError:
        return 0.0