"""
qc_vision_engine.py - AI Video Quality Control menggunakan Gemini Vision

Alur:
  1. Upload video ke Gemini File API
  2. Gemini "menonton" video dan analisa semua aspek
  3. Return keputusan: APPROVED atau NEEDS_FIX + daftar masalah + saran
  4. Kalau NEEDS_FIX → auto-fix 1x → langsung upload ke GDrive
  5. Hapus file dari Gemini setelah selesai

Aspek yang dicek:
  - Subtitle posisi & keterbacaan (safe zone UI YouTube Shorts)
  - Aspect ratio & black bars
  - Hook text (3 detik pertama)
  - Kesesuaian footage dengan narasi/topik
  - Kualitas & keterbacaan thumbnail
  - Kualitas audio (noise, volume)
  - Pacing & transisi

Model: gemini-1.5-flash (fallback: gemini-2.5-flash kalau deprecated)
"""

import os
import json
import time
import subprocess
import tempfile
from engine.utils import get_logger, require_env

logger = get_logger("qc_vision")

# Model yang dipakai (urutan fallback)
GEMINI_MODELS = [
    "gemini-1.5-flash",
    "gemini-2.0-flash",
    "gemini-2.5-flash",
]

# Batas waktu tunggu video diproses Gemini (detik)
PROCESSING_TIMEOUT = 120


# ─── Public: review ──────────────────────────────────────────────────────────

def review_video(video_path: str, thumbnail_path: str,
                 script_data: dict, channel: dict, profile: str = "shorts") -> dict:
    """
    Review video dengan Gemini Vision.

    Return: {
        "status":    "APPROVED" | "NEEDS_FIX",
        "score":     int (1-10),
        "issues":    [{"aspect": str, "problem": str, "suggestion": str}, ...],
        "summary":   str,
        "auto_fixable": [str, ...],   ← masalah yang bisa di-fix otomatis
        "model_used": str,
    }
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY tidak ada — skip QC vision, langsung upload")
        return _approved_result("GEMINI_API_KEY tidak dikonfigurasi")

    if not os.path.exists(video_path):
        logger.warning(f"Video tidak ditemukan: {video_path} — skip QC")
        return _approved_result("Video tidak ditemukan")

    logger.info(f"[{channel['id']}] Memulai QC vision dengan Gemini...")

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
    except ImportError:
        logger.warning("google-generativeai tidak terinstall — skip QC")
        return _approved_result("google-generativeai tidak terinstall")

    # Upload video ke Gemini File API
    video_file = _upload_to_gemini(video_path)
    if not video_file:
        return _approved_result("Gagal upload video ke Gemini")

    try:
        # Buat prompt QC yang komprehensif
        prompt = _build_qc_prompt(script_data, channel, profile)

        # Coba tiap model sampai berhasil
        result = None
        model_used = None

        for model_name in GEMINI_MODELS:
            try:
                logger.info(f"Mencoba model: {model_name}...")
                import google.generativeai as genai
                model    = genai.GenerativeModel(model_name=model_name)
                response = model.generate_content(
                    [video_file, prompt],
                    generation_config={"temperature": 0.2}
                )
                result     = response.text
                model_used = model_name
                logger.info(f"QC berhasil dengan {model_name}")
                break
            except Exception as e:
                logger.warning(f"Model {model_name} gagal: {e}")
                continue

        if not result:
            logger.warning("Semua model Gemini gagal — skip QC, langsung upload")
            return _approved_result("Semua model Gemini gagal")

        # Parse hasil QC
        qc_result = _parse_qc_response(result, model_used)

        # Log ringkasan
        logger.info(f"QC Result: {qc_result['status']} | Score: {qc_result['score']}/10")
        for issue in qc_result.get("issues", []):
            logger.info(f"  ⚠️  [{issue['aspect']}] {issue['problem']}")
            logger.info(f"      → {issue['suggestion']}")

        return qc_result

    finally:
        # Selalu hapus file dari Gemini setelah selesai
        _delete_gemini_file(video_file)


# ─── Auto-fix ────────────────────────────────────────────────────────────────

def auto_fix(video_path: str, thumbnail_path: str,
             qc_result: dict, channel: dict, profile: str = "shorts") -> dict:
    """
    Auto-fix masalah yang bisa diperbaiki secara otomatis.
    Hanya 1x revisi — tidak ada loop.

    Return: {
        "video_path": str,
        "thumbnail_path": str,
        "fixes_applied": [str, ...]
    }
    """
    fixes_applied = []
    current_video = video_path
    current_thumb = thumbnail_path

    auto_fixable = qc_result.get("auto_fixable", [])

    if not auto_fixable:
        logger.info("Tidak ada masalah yang bisa di-fix otomatis")
        return {
            "video_path":     current_video,
            "thumbnail_path": current_thumb,
            "fixes_applied":  [],
        }

    logger.info(f"Auto-fix: {len(auto_fixable)} masalah akan diperbaiki...")

    for fix_type in auto_fixable:
        try:
            if fix_type == "subtitle_position":
                fixed = _fix_subtitle_position(current_video, profile)
                if fixed:
                    current_video = fixed
                    fixes_applied.append("subtitle_position")

            elif fix_type == "loudnorm":
                fixed = _fix_audio_loudnorm(current_video)
                if fixed:
                    current_video = fixed
                    fixes_applied.append("loudnorm")

            elif fix_type == "thumbnail_contrast":
                fixed = _fix_thumbnail_contrast(current_thumb)
                if fixed:
                    current_thumb = fixed
                    fixes_applied.append("thumbnail_contrast")

        except Exception as e:
            logger.warning(f"Auto-fix '{fix_type}' gagal: {e}")

    if fixes_applied:
        logger.info(f"Auto-fix selesai: {fixes_applied}")
    else:
        logger.info("Tidak ada fix yang berhasil diterapkan")

    return {
        "video_path":     current_video,
        "thumbnail_path": current_thumb,
        "fixes_applied":  fixes_applied,
    }


# ─── Gemini File API ──────────────────────────────────────────────────────────

def _upload_to_gemini(video_path: str):
    """Upload video ke Gemini File API dan tunggu sampai selesai diproses."""
    try:
        import google.generativeai as genai

        file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
        logger.info(f"Uploading video ke Gemini ({file_size_mb:.1f}MB)...")

        video_file = genai.upload_file(path=video_path)
        logger.info(f"Upload selesai: {video_file.display_name}")

        # Tunggu Gemini selesai proses video
        deadline = time.time() + PROCESSING_TIMEOUT
        while video_file.state.name == "PROCESSING":
            if time.time() > deadline:
                logger.error("Timeout menunggu Gemini proses video")
                return None
            logger.info("Gemini sedang memproses video...")
            time.sleep(5)
            video_file = genai.get_file(video_file.name)

        if video_file.state.name == "FAILED":
            logger.error("Gemini gagal memproses video")
            return None

        logger.info("Video siap dianalisa Gemini")
        return video_file

    except Exception as e:
        logger.error(f"Gagal upload ke Gemini: {e}")
        return None


def _delete_gemini_file(video_file):
    """Hapus file dari Gemini setelah selesai (hemat storage quota)."""
    try:
        import google.generativeai as genai
        genai.delete_file(video_file.name)
        logger.info("File dihapus dari Gemini")
    except Exception as e:
        logger.warning(f"Gagal hapus file dari Gemini: {e}")


# ─── QC Prompt ───────────────────────────────────────────────────────────────

def _build_qc_prompt(script_data: dict, channel: dict, profile: str) -> str:
    title    = script_data.get("title", "")
    niche    = channel.get("niche", "")
    language = channel.get("language", "id")
    is_id    = language == "id"

    aspect_guide = """
ASPEK YANG HARUS DICEK:

1. SUBTITLE SAFE ZONE
   - Apakah subtitle berada di area yang aman? (tidak terpotong UI YouTube Shorts di bagian bawah ~20% layar)
   - Apakah teks subtitle cukup besar dan terbaca?
   - Apakah highlight kata aktif terlihat jelas?

2. ASPECT RATIO & BLACK BARS
   - Apakah ada black bars (area hitam) yang berlebihan?
   - Apakah footage mengisi layar dengan baik (blurred background technique)?

3. HOOK TEXT (3 DETIK PERTAMA)
   - Apakah ada hook text di bagian atas layar?
   - Apakah hook text terbaca jelas dan menarik?
   - Apakah posisinya tidak bentrok dengan elemen lain?

4. KESESUAIAN FOOTAGE DENGAN NARASI
   - Apakah visual yang ditampilkan relevan dengan topik yang dibahas?
   - Apakah ada momen di mana footage tidak nyambung dengan audio?
   - Sebutkan timestamp spesifik jika ada ketidaksesuaian.

5. KUALITAS AUDIO
   - Apakah volume narasi konsisten dan cukup keras?
   - Apakah musik background terlalu keras atau terlalu pelan?
   - Apakah ada noise atau distorsi yang mengganggu?

6. PACING & TRANSISI
   - Apakah kecepatan narasi nyaman diikuti?
   - Apakah pergantian footage terasa natural?
   - Apakah ada bagian yang terlalu cepat atau terlalu lambat?

7. THUMBNAIL (frame pertama video atau yang ditampilkan)
   - Apakah teks pada thumbnail terbaca jelas?
   - Apakah kontras warna cukup baik?
   - Apakah thumbnail menarik dan relevan?
"""

    output_format = """
BERIKAN RESPONSE DALAM FORMAT JSON BERIKUT (tidak ada teks lain di luar JSON):
{
  "status": "APPROVED" atau "NEEDS_FIX",
  "score": <angka 1-10>,
  "summary": "<ringkasan singkat penilaian keseluruhan>",
  "issues": [
    {
      "aspect": "<nama aspek>",
      "severity": "HIGH" atau "MEDIUM" atau "LOW",
      "problem": "<deskripsi masalah spesifik>",
      "timestamp": "<timestamp video jika relevan, misal '0:05'>",
      "suggestion": "<saran perbaikan konkret>"
    }
  ],
  "auto_fixable": ["subtitle_position", "loudnorm", "thumbnail_contrast"],
  "approved_aspects": ["<aspek yang sudah bagus>"]
}

CATATAN:
- "auto_fixable" hanya isi dengan item dari list ini yang memang bermasalah:
  ["subtitle_position", "loudnorm", "thumbnail_contrast"]
- Score 8-10 = APPROVED, Score 1-7 = NEEDS_FIX
- Jika tidak ada masalah, "issues" = [] dan "auto_fixable" = []
- Berikan saran yang spesifik dan actionable, bukan saran umum
"""

    return f"""Kamu adalah seorang profesional editor video YouTube Shorts dengan pengalaman 10 tahun.
Tugasmu adalah me-review video ini secara kritis dan memberikan penilaian objektif.

INFO VIDEO:
- Judul: {title}
- Niche: {niche}
- Bahasa: {"Indonesia" if is_id else "English"}
- Format: {profile}

{aspect_guide}

{output_format}"""


# ─── Parse response ───────────────────────────────────────────────────────────

def _parse_qc_response(raw: str, model_used: str) -> dict:
    """Parse response JSON dari Gemini."""
    import re

    # Clean markdown fencing
    clean = raw.replace("```json", "").replace("```", "").strip()

    # Extract JSON object
    if not clean.startswith("{"):
        match = re.search(r'\{.*\}', clean, re.DOTALL)
        if match:
            clean = match.group()
        else:
            logger.warning(f"Tidak ada JSON di response Gemini: {raw[:200]}")
            return _approved_result("Tidak bisa parse response Gemini")

    try:
        data = json.loads(clean)
        data["model_used"] = model_used

        # Validasi field wajib
        data.setdefault("status",           "APPROVED")
        data.setdefault("score",            8)
        data.setdefault("summary",          "")
        data.setdefault("issues",           [])
        data.setdefault("auto_fixable",     [])
        data.setdefault("approved_aspects", [])

        # Safety: kalau score >= 8 pastikan status APPROVED
        if data["score"] >= 8:
            data["status"] = "APPROVED"
        elif data["score"] < 8 and data["status"] == "APPROVED":
            data["status"] = "NEEDS_FIX"

        return data

    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error: {e}\nRaw: {raw[:300]}")
        return _approved_result("Parse error — video di-approve by default")


# ─── Auto-fix implementations ─────────────────────────────────────────────────

def _fix_subtitle_position(video_path: str, profile: str) -> str | None:
    """
    Naikkan posisi subtitle agar lebih jauh dari bawah layar.
    Burn ulang subtitle dengan margin_v lebih tinggi.
    """
    # Cek apakah ada file .ass subtitle yang bisa di-edit
    base = os.path.splitext(video_path)[0]
    ass_candidates = [
        f"{base}.ass",
        video_path.replace("_sub.mp4", ".ass"),
    ]

    ass_file = next((f for f in ass_candidates if os.path.exists(f)), None)

    if not ass_file:
        logger.info("File .ass subtitle tidak ditemukan untuk di-fix, skip")
        return None

    # Baca dan naikkan MarginV
    with open(ass_file, "r", encoding="utf-8") as f:
        content = f.read()

    import re
    h_match = re.search(r'PlayResY:\s*(\d+)', content)
    if not h_match:
        return None

    h          = int(h_match.group(1))
    new_margin = int(h * 0.30)  # naikkan ke 30% dari bawah

    # Replace MarginV di Style line
    content_fixed = re.sub(
        r'(Style: Default.*?)(\d+)(,1\n)',
        lambda m: f"{m.group(1)}{new_margin}{m.group(3)}",
        content
    )

    # Simpan .ass yang sudah difix
    with open(ass_file, "w", encoding="utf-8") as f:
        f.write(content_fixed)

    # Burn ulang subtitle
    out_path = video_path.replace(".mp4", "_fixed_sub.mp4")
    ass_escaped = ass_file.replace("\\", "/").replace(":", "\\:")
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", f"ass='{ass_escaped}'",
        "-c:v", "libx264", "-crf", "23", "-preset", "fast",
        "-c:a", "copy",
        out_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        logger.info(f"Subtitle position fixed: {out_path}")
        return out_path
    return None


def _fix_audio_loudnorm(video_path: str) -> str | None:
    """Normalize ulang volume audio ke -14 LUFS."""
    out_path = video_path.replace(".mp4", "_loudnorm.mp4")
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-af", "loudnorm=I=-14:TP=-1.5:LRA=11",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k",
        out_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        logger.info(f"Audio loudnorm fixed: {out_path}")
        return out_path
    return None


def _fix_thumbnail_contrast(thumbnail_path: str) -> str | None:
    """Tingkatkan kontras thumbnail."""
    if not thumbnail_path or not os.path.exists(thumbnail_path):
        return None

    try:
        from PIL import Image, ImageEnhance
        img       = Image.open(thumbnail_path)
        enhancer  = ImageEnhance.Contrast(img)
        img_fixed = enhancer.enhance(1.3)  # +30% kontras
        out_path  = thumbnail_path.replace(".png", "_fixed.png")
        img_fixed.save(out_path, "PNG")
        logger.info(f"Thumbnail contrast fixed: {out_path}")
        return out_path
    except Exception as e:
        logger.warning(f"Thumbnail fix gagal: {e}")
        return None


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _approved_result(reason: str = "") -> dict:
    """Return hasil APPROVED default (kalau QC tidak bisa jalan)."""
    return {
        "status":           "APPROVED",
        "score":            8,
        "summary":          reason or "Auto-approved",
        "issues":           [],
        "auto_fixable":     [],
        "approved_aspects": [],
        "model_used":       "none",
    }