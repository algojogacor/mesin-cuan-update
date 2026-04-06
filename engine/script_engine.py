"""
script_engine.py - Generate script narasi video dari topik
Primary  : DeepSeek V3.1 671B via Ollama (lokal/cloud, gratis, kualitas tinggi)
Fallback1: Groq — Llama 3.3 70B (gratis, cepat)
Fallback2: Gemini 1.5 Flash (gratis, quota harian)
Fallback3: Anthropic Claude Haiku (berbayar, last resort)
Support  : profile "shorts" dan "long_form" (Target 1300+ kata)

Pengaman JSON & Karakter:
  - Strip markdown fencing (```json ... ```)
  - Quote Normalization: Mengganti smart-quotes (“”) yang merusak struktur JSON.
  - Regex extraction: Mencari blok { ... } jika ada teks sampah.
  - Auto-repair: Menambah kurung tutup yang hilang jika output terpotong.
  - Validasi: Menjamin field 'keywords' dan 'tags' tersedia untuk QC.
"""

import json
import os
import re
import time
import requests
from engine.utils import get_logger, require_env, load_prompt, timestamp, save_json, channel_data_path

# ── BARU: Imports untuk fitur Hook & Retention ─────────────────────────────────
from engine.hook_engine import inject_hook
from engine.retention_engine import build_prompt_addon

logger = get_logger("script_engine")

MIN_WORDS = {"shorts": 80, "long_form": 1300}

# Delay antar provider switch (detik)
PROVIDER_SWITCH_DELAY = 60

# Retry per provider kalau JSON gagal di-parse
MAX_JSON_RETRY = 3

# Ollama config
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.environ.get("OLLAMA_MODEL",    "deepseek-v3.1:671b-cloud")


def generate(topic_data: dict, channel: dict, profile: str = "shorts") -> dict:
    niche    = channel["niche"]
    language = channel["language"]
    ch_id    = channel["id"]
    topic              = topic_data["topic"]
    is_viral_iteration = topic_data.get("is_viral_iteration", False)

    logger.info(f"[{ch_id}] [{profile}] Generating script for: {topic}")
    if is_viral_iteration:
        logger.info(f"[{ch_id}] 🔁 Mode Viral Iteration aktif — sisipkan instruksi Part 2")

    system_prompt = load_prompt(niche, language, profile=profile)

    # ── BARU: Tambahkan retention insights ke system prompt ──────────────────
    retention_addon = build_prompt_addon(channel)
    if retention_addon:
        system_prompt += retention_addon
        logger.info(f"[{ch_id}] Retention insights disertakan ke prompt")

    # ── VIRAL ITERATION: Modifikasi user_message agar AI tahu ini Part 2 ────
    if is_viral_iteration:
        if language == "id":
            continuation_hint = (
                "\n\nCATATAN PENTING: Ini adalah LANJUTAN dari video sebelumnya yang sudah viral. "
                "Mulai dengan referensi bahwa penonton sudah tahu cerita dasarnya dan langsung "
                "masuk ke detail yang lebih dalam, sudut pandang baru, atau fakta lanjutan. "
                "JANGAN mengulang semua penjelasan awal dari video sebelumnya. "
                "Buat pembuka yang mengakui Part 1 dan langsung meningkatkan intensitas."
            )
        else:
            continuation_hint = (
                "\n\nIMPORTANT NOTE: This is a SEQUEL to a previously viral video. "
                "Start by acknowledging that viewers already know the basics, then go deeper "
                "with new angles, hidden details, or follow-up facts. "
                "DO NOT repeat all the introductory explanations from Part 1. "
                "Open by referencing Part 1 and immediately escalate the intensity."
            )
        user_message = (
            f"Topik: {topic}{continuation_hint}"
            if language == "id"
            else f"Topic: {topic}{continuation_hint}"
        )
    else:
        user_message = f"Topik: {topic}" if language == "id" else f"Topic: {topic}"

    result                       = _call_ai(system_prompt, user_message, profile=profile)
    result["topic"]              = topic
    result["profile"]            = profile  # Penting untuk hook_engine agar tahu target field
    result["is_viral_iteration"] = is_viral_iteration  # Forward metadata ke output JSON

    # ── BARU: Inject hook di awal narasi ────────────────────────────────────
    result = inject_hook(result, channel)
    logger.info(f"[{ch_id}] Hook disuntikkan ke narasi")

    out_dir  = channel_data_path(ch_id, "scripts")
    out_path = f"{out_dir}/{timestamp()}_{profile}.json"
    save_json(result, out_path)
    logger.info(f"[{ch_id}] Script saved: {out_path}")

    return {**result, "script_path": out_path, "profile": profile}


def _call_ai(system_prompt: str, user_message: str, profile: str = "shorts") -> dict:
    providers = [
        ("DeepSeek/Ollama", lambda: _call_ollama(system_prompt, user_message, profile)),
        ("Groq",            lambda: _call_groq(system_prompt, user_message, profile)),
        ("Gemini",          lambda: _call_gemini(system_prompt, user_message, profile)),
        ("Anthropic",       lambda: _call_anthropic(system_prompt, user_message, profile)),
    ]

    last_error = None
    for name, fn in providers:
        logger.info(f"Trying {name}...")
        try:
            result = fn()
            logger.info(f"✅ {name} berhasil")
            return result
        except Exception as e:
            last_error = e
            logger.warning(f"❌ {name} gagal: {e}")
            if name != "Anthropic":  # tidak perlu delay setelah provider terakhir
                logger.info(f"Tunggu {PROVIDER_SWITCH_DELAY}s sebelum provider berikutnya...")
                time.sleep(PROVIDER_SWITCH_DELAY)

    raise RuntimeError(f"Semua provider gagal. Error terakhir: {last_error}")


# ─── DeepSeek via Ollama ──────────────────────────────────────────────────────

def _call_ollama(system_prompt: str, user_message: str, profile: str) -> dict:
    max_tokens = 4500 if profile == "long_form" else 1200

    system_with_json = (
        system_prompt +
        "\n\nCRITICAL: Respond with ONLY a raw JSON object. "
        "No markdown, no ```json fences, no backticks, no explanation. "
        "Start your response directly with { and end with }."
    )

    for attempt in range(1, MAX_JSON_RETRY + 1):
        user_msg = user_message + _get_length_hint(profile)

        # Makin banyak gagal, makin tegas instruksinya
        if attempt > 1:
            user_msg += (
                f"\n\n[ATTEMPT {attempt}] IMPORTANT: Output ONLY the JSON object. "
                "Do NOT use markdown code blocks. Start directly with { ."
            )

        payload = {
            "model":   OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": system_with_json},
                {"role": "user",   "content": user_msg},
            ],
            "stream":  False,
            "format":  "json",
            "options": {
                "temperature": max(0.3, 0.75 - (attempt - 1) * 0.2),  # makin rendah tiap retry
                "num_predict": max_tokens,
                "num_ctx": 8192  # Mencegah context limit terpotong untuk 1300 kata
            },
        }

        try:
            resp = requests.post(
                f"{OLLAMA_BASE_URL}/api/chat",
                json=payload,
                timeout=600,  # 10 Menit timeout untuk naskah panjang
            )
            resp.raise_for_status()
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                f"Ollama tidak bisa dihubungi di {OLLAMA_BASE_URL}. "
                "Pastikan Ollama sudah jalan."
            )
        except requests.exceptions.Timeout:
            raise RuntimeError("Ollama timeout — model tidak merespons dalam 600 detik")

        raw = resp.json().get("message", {}).get("content", "").strip()
        if not raw:
            logger.warning(f"Ollama attempt {attempt}: response kosong, retry...")
            continue

        try:
            return _parse_json_response(raw, profile)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Ollama attempt {attempt}/{MAX_JSON_RETRY}: JSON parse gagal — {e}")
            if attempt == MAX_JSON_RETRY:
                raise RuntimeError(f"Ollama gagal produce valid JSON setelah {MAX_JSON_RETRY}x: {e}")

    raise RuntimeError("Ollama: semua retry habis")


# ─── Groq ─────────────────────────────────────────────────────────────────────

def _call_groq(system_prompt: str, user_message: str, profile: str) -> dict:
    from groq import Groq
    client     = Groq(api_key=require_env("GROQ_API_KEY"))
    max_tokens = 3000 if profile == "long_form" else 1000

    for attempt in range(1, MAX_JSON_RETRY + 1):
        user_msg = user_message + _get_length_hint(profile)
        if attempt > 1:
            user_msg += f"\n\n[ATTEMPT {attempt}] Output ONLY valid JSON, no markdown."

        try:
            resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_msg},
                ],
                response_format={"type": "json_object"},
                max_tokens=max_tokens,
                temperature=max(0.3, 0.7 - (attempt - 1) * 0.2),
            )
            raw = resp.choices[0].message.content.strip()
            return _parse_json_response(raw, profile)

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Groq attempt {attempt}/{MAX_JSON_RETRY}: JSON parse gagal — {e}")
            if attempt == MAX_JSON_RETRY:
                raise

        except Exception as e:
            raise  # Error non-JSON (rate limit, dll) langsung raise


# ─── Gemini ───────────────────────────────────────────────────────────────────

def _call_gemini(system_prompt: str, user_message: str, profile: str) -> dict:
    from google import genai
    client = genai.Client(api_key=require_env("GEMINI_API_KEY"))

    for attempt in range(1, MAX_JSON_RETRY + 1):
        user_msg = user_message + _get_length_hint(profile)
        if attempt > 1:
            user_msg += f"\n\n[ATTEMPT {attempt}] Output ONLY valid JSON, no markdown."

        prompt = f"{system_prompt}\n\n{user_msg}"

        try:
            resp = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=prompt,
            )
            if not resp.text:
                raise ValueError("Gemini returned empty response")

            return _parse_json_response(resp.text.strip(), profile)

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Gemini attempt {attempt}/{MAX_JSON_RETRY}: JSON parse gagal — {e}")
            if attempt == MAX_JSON_RETRY:
                raise

        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "quota" in err_str.lower() or "rate" in err_str.lower():
                wait = 60 if attempt == 1 else 120
                logger.warning(f"Gemini rate limit, tunggu {wait}s...")
                time.sleep(wait)
            else:
                raise

    raise RuntimeError("Gemini gagal setelah semua retry")


# ─── Anthropic ────────────────────────────────────────────────────────────────

def _call_anthropic(system_prompt: str, user_message: str, profile: str) -> dict:
    import anthropic
    client     = anthropic.Anthropic(api_key=require_env("ANTHROPIC_API_KEY"))
    max_tokens = 3000 if profile == "long_form" else 1000

    for attempt in range(1, MAX_JSON_RETRY + 1):
        user_msg = user_message + _get_length_hint(profile)
        if attempt > 1:
            user_msg += f"\n\n[ATTEMPT {attempt}] Output ONLY valid JSON, no markdown."

        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": user_msg}],
            )
            raw = msg.content[0].text.strip()
            return _parse_json_response(raw, profile)

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Anthropic attempt {attempt}/{MAX_JSON_RETRY}: JSON parse gagal — {e}")
            if attempt == MAX_JSON_RETRY:
                raise



# ─── JSON Parser (berlapis) ───────────────────────────────────────────────────

def _clean_raw_json(raw: str) -> str:
    """
    Bersihkan raw response dari model sebelum di-parse.
    Berlapis dari yang paling umum ke paling agresif.
    """
    text = raw.strip()

    # Layer 1: strip markdown fencing (```json ... ``` atau ``` ... ```)
    text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*```$', '', text)
    text = text.strip()

    # Layer 2: Normalisasi Karakter (Krusial untuk Hindi & DeepSeek)
    # Mengganti 'Smart Quotes' miring yang sering dihasilkan AI menjadi kutipan standar
    text = text.replace('“', '"').replace('”', '"').replace('‘', "'").replace('’', "'")
    
    # Layer 3: Tangani Newline liar di tengah string JSON
    text = re.sub(r':\s*\n\s*"', ': "', text)

    # Layer 4: kalau masih ada backtick di awal/akhir
    text = text.strip('`').strip()

    # Layer 5: kalau ada teks sebelum { (misal "Here is the JSON:")
    if not text.startswith('{'):
        match = re.search(r'\{', text)
        if match:
            text = text[match.start():]

    # Layer 6: kalau ada teks sesudah } terakhir
    if not text.endswith('}'):
        match = re.search(r'\}(?=[^}]*$)', text)
        if match:
            text = text[:match.end()]

    # Layer 7: cari JSON object terlengkap dengan regex DOTALL
    if not (text.startswith('{') and text.endswith('}')):
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            text = match.group()

    return text.strip()


def _repair_json(raw: str, profile: str) -> dict:
    """
    Repair JSON yang tidak lengkap / terpotong.
    Strategi: tambahkan penutup yang hilang.
    """
    text = raw.strip()

    # Hitung buka/tutup kurung
    open_braces  = text.count('{')
    close_braces = text.count('}')

    if open_braces > close_braces:
        # Tambah kurung tutup yang kurang
        text += '}' * (open_braces - close_braces)
        logger.warning(f"JSON repair: tambah {open_braces - close_braces} kurung tutup")

    # Hapus trailing comma sebelum }
    text = re.sub(r',\s*}', '}', text)
    text = re.sub(r',\s*]', ']', text)

    return json.loads(text)


def _parse_json_response(raw: str, profile: str) -> dict:
    """
    Parse JSON response dari model dengan pengaman berlapis.
    Layer 1: clean + parse normal
    Layer 2: repair (kurung hilang, trailing comma)
    Layer 3: validasi field & panjang
    """
    if not raw:
        raise ValueError("Response kosong")

    # ── Layer 1: clean dan parse normal
    cleaned = _clean_raw_json(raw)
    data    = None

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as e1:
        logger.warning(f"Parse normal gagal ({e1}), coba repair...")

        # ── Layer 2: repair JSON
        try:
            data = _repair_json(cleaned, profile)
            logger.info("JSON repair berhasil")
        except json.JSONDecodeError as e2:
            logger.error(
                f"JSON repair juga gagal: {e2}\n"
                f"Raw (100 char pertama): {raw[:100]}\n"
                f"Cleaned              : {cleaned[:100]}"
            )
            raise ValueError(f"Tidak bisa parse JSON setelah clean + repair: {e2}")

    # ── Layer 3: validasi struktur & panjang
    return _validate_and_fix(data, profile)


def _validate_and_fix(data: dict, profile: str) -> dict:
    """
    Validasi field wajib dan panjang script.
    Kalau field hilang tapi data ada, coba auto-fix.
    """
    if profile == "long_form":
        required = ["title", "intro", "segments", "outro", "tags", "description", "chapters"]
        for field in required:
            if field not in data:
                # Auto-fix non-kritis
                if field == "chapters": data["chapters"] = "0:00 Intro"
                elif field == "description": data["description"] = data.get("title", "")
                elif field == "tags": data["tags"] = ["mystery", "documentary"]
                else: raise ValueError(f"Long-form missing required field: '{field}'")

        if not isinstance(data.get("segments"), list) or len(data.get("segments", [])) < 4:
            raise ValueError(
                f"Long-form harus punya minimal 4 segmen, "
                f"dapat {len(data.get('segments', []))}"
            )

        # Auto-fix keywords untuk QC Engine
        if not data.get("keywords"):
            data["keywords"] = data.get("tags", ["documentary"])[:5]

        data["script"] = _flatten_long_form_script(data)
        
        total_words = (
            len(data.get("intro", "").split()) +
            sum(len(s.get("narasi", "").split()) for s in data.get("segments", [])) +
            len(data.get("outro", "").split())
        )
        
        logger.info(f"Long-form: {total_words} kata, {len(data.get('segments', []))} segmen")
        
        if total_words < MIN_WORDS["long_form"]:
            logger.warning(
                f"Long-form script di bawah target: {total_words} kata "
                f"(target min {MIN_WORDS['long_form']})"
            )

    else:
        required = ["title", "script", "keywords", "tags", "description"]
        for field in required:
            if field not in data:
                # Auto-fix field kosong yang tidak kritis
                if field == "keywords":
                    data["keywords"] = data.get("tags", [])
                    logger.warning("Auto-fix: keywords diisi dari tags")
                elif field == "tags":
                    data["tags"] = []
                    logger.warning("Auto-fix: tags diisi []")
                elif field == "description":
                    data["description"] = data.get("title", "")
                    logger.warning("Auto-fix: description diisi dari title")
                else:
                    raise ValueError(f"Shorts missing required field: '{field}'")

        word_count = len(data.get("script", "").split())
        if word_count < MIN_WORDS["shorts"]:
            raise ValueError(
                f"Script terlalu pendek: {word_count} kata "
                f"(min {MIN_WORDS['shorts']})"
            )

        logger.info(f"Shorts script: {word_count} kata")

    return data


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_length_hint(profile: str) -> str:
    if profile == "shorts":
        return (
            "\n\nCRITICAL: The 'script' field MUST be highly detailed. "
            "Write AT LEAST 4-5 long sentences, MORE than 80-120 words. "
            "Expand on the facts with vivid details!"
        )
    else:
        return (
            "\n\nCRITICAL LENGTH REQUIREMENT:\n"
            "- Total narration MUST EXCEED 1300 WORDS for a 10-12 minute video.\n"
            "- Intro MUST be > 150 words.\n"
            "- Each 'narasi' segment MUST be 250-300 words each.\n"
            "- Outro MUST be > 100 words.\n"
            "- Ensure NO unescaped quotes inside the JSON string values."
        )


def _flatten_long_form_script(data: dict) -> str:
    parts = [data.get("intro", "")]
    for seg in data.get("segments", []):
        if isinstance(seg, dict) and seg.get("narasi"):
            parts.append(seg["narasi"])
    parts.append(data.get("outro", ""))    
    return "\n\n".join(p.strip() for p in parts if p.strip())