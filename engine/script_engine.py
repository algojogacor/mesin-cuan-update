"""
script_engine.py - Generate script narasi video dari topik

Primary  : Dual Parallel Generation (Qwen + Ollama) dengan Cross-Provider Scoring
           Generator A: Qwen   → Scored by: Ollama
           Generator B: Ollama → Scored by: Qwen
           Winner (score tertinggi) yang lanjut ke TTS.
Fallback : Sequential waterfall jika parallel gagal/score < threshold
           Groq → Gemini → Anthropic (Groq TIDAK pernah jadi primary)
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
import random
import concurrent.futures
from copy import deepcopy
import requests
from engine.utils import get_logger, require_env, load_prompt, timestamp, save_json, channel_data_path

# ── BARU: Imports untuk fitur Hook & Retention ─────────────────────────────────
from engine.hook_engine import inject_hook
from engine.retention_engine import build_prompt_addon
from engine.memory_engine import build_script_memory_addon

logger = get_logger("script_engine")

MIN_WORDS = {"shorts": 80, "long_form": 1300}

# Delay antar provider switch (detik)
PROVIDER_SWITCH_DELAY = 60

# Retry per provider kalau JSON gagal di-parse
MAX_JSON_RETRY = 3

# Ollama config
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.environ.get("OLLAMA_MODEL",    "deepseek-v3.1:671b-cloud")
QWEN_API_BASE   = os.environ.get("QWEN_API_BASE",   "http://34.57.12.120:9000/v1")
QWEN_MODEL      = os.environ.get("QWEN_MODEL",      "qwen3-235b-a22b")
QWEN_MODEL_CANDIDATES = [
    m.strip() for m in os.environ.get(
        "QWEN_MODEL_CANDIDATES",
        "qwen3-235b-a22b,qwen3-plus,qwen2.5-72b-instruct,qwen3-30b-a3b,qwen3-turbo"
    ).split(",")
    if m.strip()
]

SHORTS_REVIEW_MIN_SCORE = float(os.environ.get("SHORTS_REVIEW_MIN_SCORE", "8.4"))
SHORTS_REVIEW_MAX_PASSES = 2
SCRIPT_MAX_TOKENS_SHORTS = int(os.environ.get("SCRIPT_MAX_TOKENS_SHORTS", "2400"))
SCRIPT_MAX_TOKENS_LONGFORM = int(os.environ.get("SCRIPT_MAX_TOKENS_LONGFORM", "7000"))
SCRIPT_REVIEW_MAX_TOKENS = int(os.environ.get("SCRIPT_REVIEW_MAX_TOKENS", "3200"))

# ── Script Quality Scorer (parallel gate) ────────────────────────────────────
# Threshold: jika max(score_A, score_B) >= ini → langsung lanjut
# Jika di bawah threshold → fallback ke sequential waterfall
SCRIPT_QUALITY_THRESHOLD = float(os.environ.get("SCRIPT_QUALITY_THRESHOLD", "7.8"))

# Timeout untuk parallel generation (detik)
PARALLEL_TIMEOUT = 600


def _post_json_no_proxy(url: str, headers: dict | None = None, payload: dict | None = None,
                        timeout: int = 300) -> requests.Response:
    session = requests.Session()
    session.trust_env = False
    try:
        return session.post(url, headers=headers, json=payload, timeout=timeout)
    finally:
        session.close()


def _qwen_models_to_try() -> list[str]:
    models: list[str] = []
    preferred = QWEN_MODEL.strip() if QWEN_MODEL else ""
    if preferred:
        models.append(preferred)
    for model in QWEN_MODEL_CANDIDATES:
        if model not in models:
            models.append(model)
    return models


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

    if niche == "horror_facts":
        memory_addon = build_script_memory_addon(channel, topic_data, profile=profile)
        if memory_addon:
            system_prompt += memory_addon
            logger.info(f"[{ch_id}] Creative memory horror disertakan ke prompt")

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

    # ── Dual Parallel Generation + Cross-Provider Scoring ──────────────────
    result = _generate_and_pick_best(system_prompt, user_message, profile, ch_id)
    if result is None:
        # Fallback: sequential waterfall (existing behavior, tidak ada perubahan)
        logger.info(f"[{ch_id}] Parallel scorer tidak menghasilkan winner → fallback ke sequential")
        result = _call_ai(system_prompt, user_message, profile=profile)
    result["topic"]              = topic
    result["profile"]            = profile  # Penting untuk hook_engine agar tahu target field
    result["is_viral_iteration"] = is_viral_iteration  # Forward metadata ke output JSON
    result["topic_source"]       = topic_data.get("topic_source", "unknown")

    # ── BARU: Inject hook di awal narasi ────────────────────────────────────
    result = inject_hook(result, channel)
    logger.info(f"[{ch_id}] Hook disuntikkan ke narasi")

    if profile == "shorts" and result.get("hook_meta", {}).get("rewritten"):
        result["script"] = _compose_shorts_script(result)
        logger.info(f"[{ch_id}] Hook horror direwrite dan script dirakit ulang")

    if profile == "shorts":
        result = review_and_iterate(result, channel, profile=profile)

    out_dir  = channel_data_path(ch_id, "scripts")
    out_path = f"{out_dir}/{timestamp()}_{profile}.json"
    save_json(result, out_path)
    logger.info(f"[{ch_id}] Script saved: {out_path}")

    return {**result, "script_path": out_path, "profile": profile}


def review_hook_only(script_data: dict, channel: dict, profile: str = "shorts") -> dict:
    """
    Apply only the horror hook review/upgrade pass without running the full
    script review loop, TTS, footage, or render steps.
    """
    current = deepcopy(script_data)
    current["profile"] = current.get("profile", profile)

    if current.get("profile") == "shorts":
        current = _normalize_shorts_schema(current)

    current = inject_hook(current, channel)
    if current.get("profile") == "shorts" and current.get("hook_meta", {}).get("rewritten"):
        current["script"] = _compose_shorts_script(current)
    return current


def _call_ai(system_prompt: str, user_message: str, profile: str = "shorts") -> dict:
    primary_providers = [("DeepSeek/Ollama", lambda: _call_ollama(system_prompt, user_message, profile))]
    if os.getenv("QWEN_API_KEY"):
        primary_providers.append(("Qwen", lambda: _call_qwen(system_prompt, user_message, profile)))
    random.shuffle(primary_providers)

    providers = primary_providers + [
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
            if name not in ("Anthropic", "DeepSeek/Ollama", "Qwen"):
                logger.info(f"Tunggu {PROVIDER_SWITCH_DELAY}s sebelum provider berikutnya...")
                time.sleep(PROVIDER_SWITCH_DELAY)

    raise RuntimeError(f"Semua provider gagal. Error terakhir: {last_error}")


# ─── Dual Parallel Generation + Cross-Provider Scoring ───────────────────────

def _generate_and_pick_best(
    system_prompt: str,
    user_message: str,
    profile: str,
    ch_id: str,
) -> dict | None:
    """
    Generate script dengan dua model secara paralel, lalu cross-score hasilnya.

    Strategi anti-sycophancy:
      Generator Qwen   → di-score oleh Ollama
      Generator Ollama → di-score oleh Qwen

    Return:
      dict  — script terbaik (score >= SCRIPT_QUALITY_THRESHOLD)
      None  — signal agar caller fallback ke _call_ai() sequential
    """
    from engine.script_quality_scorer import score_script

    qwen_available = bool(os.getenv("QWEN_API_KEY"))
    if not qwen_available:
        logger.info(f"[{ch_id}] QWEN_API_KEY tidak ada — skip parallel, gunakan sequential")
        return None

    logger.info(f"[{ch_id}] ⚡ Parallel generation: Qwen + Ollama ...")

    # ── Step 1: Generate paralel ─────────────────────────────────────────────
    generator_results: dict[str, dict | Exception] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            "qwen":   executor.submit(_call_qwen,  system_prompt, user_message, profile),
            "ollama": executor.submit(_call_ollama, system_prompt, user_message, profile),
        }
        for generator, future in futures.items():
            try:
                generator_results[generator] = future.result(timeout=PARALLEL_TIMEOUT)
                logger.info(f"[{ch_id}] [parallel] ✅ {generator} selesai generate")
            except concurrent.futures.TimeoutError:
                logger.warning(f"[{ch_id}] [parallel] ⚠️  {generator} timeout")
            except Exception as exc:
                logger.warning(f"[{ch_id}] [parallel] ❌ {generator} gagal: {exc}")

    successful = {k: v for k, v in generator_results.items() if isinstance(v, dict)}
    if not successful:
        logger.warning(f"[{ch_id}] Kedua generator gagal — fallback ke sequential")
        return None

    # ── Step 2: Cross-score (anti-sycophancy) ────────────────────────────────
    # Qwen output  → dinilai Ollama
    # Ollama output → dinilai Qwen
    CROSS_SCORER = {"qwen": "ollama", "ollama": "qwen"}

    scored: list[tuple[float, str, dict, dict]] = []
    for generator, script_data in successful.items():
        scorer_provider = CROSS_SCORER[generator]
        try:
            quality = score_script(
                script_data,
                profile=profile,
                scorer_provider=scorer_provider,
            )
            overall = quality.get("overall", 0.0)
            scored.append((overall, generator, script_data, quality))
            logger.info(
                f"[{ch_id}] [scorer] {generator} → dinilai {scorer_provider}: "
                f"{overall:.1f} ({quality.get('verdict', '?')}) | "
                f"hook={quality.get('hook_strength')} curiosity={quality.get('curiosity_gap')}"
            )
        except Exception as exc:
            logger.warning(
                f"[{ch_id}] [scorer] Scoring {generator} gagal: {exc} — skor fallback 5.0"
            )
            scored.append((5.0, generator, script_data, {"overall": 5.0, "verdict": "SCORE_FAILED"}))

    if not scored:
        logger.warning(f"[{ch_id}] Semua scoring gagal — fallback ke sequential")
        return None

    # ── Step 3: Pilih winner ─────────────────────────────────────────────────
    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_generator, best_script, best_quality = scored[0]

    # Attach quality metadata untuk debug/audit (tersimpan di script JSON)
    best_script["quality_score"] = best_quality

    if best_score >= SCRIPT_QUALITY_THRESHOLD:
        logger.info(
            f"[{ch_id}] ✅ Parallel winner: {best_generator} "
            f"(score={best_score:.1f} >= threshold={SCRIPT_QUALITY_THRESHOLD})"
        )
        return best_script

    logger.info(
        f"[{ch_id}] ⚠️  Best parallel score {best_score:.1f} < threshold {SCRIPT_QUALITY_THRESHOLD} "
        f"— fallback ke sequential waterfall"
    )
    return None


# ─── DeepSeek via Ollama ──────────────────────────────────────────────────────

def _call_ollama(system_prompt: str, user_message: str, profile: str) -> dict:
    max_tokens = SCRIPT_MAX_TOKENS_LONGFORM if profile == "long_form" else SCRIPT_MAX_TOKENS_SHORTS
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
                # Generator: high creativity, anti-repetition
                "temperature":    max(0.5, 0.90 - (attempt - 1) * 0.10),  # 0.90 -> 0.80 -> 0.70
                "top_p":          0.95,
                "top_k":          50,
                "repeat_penalty": 1.15,
                "num_predict":    max_tokens,
                "num_ctx":        16384,
                "seed":           -1,   # -1 = random, variasi antar video
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

def _call_qwen(system_prompt: str, user_message: str, profile: str) -> dict:
    max_tokens = SCRIPT_MAX_TOKENS_LONGFORM if profile == "long_form" else SCRIPT_MAX_TOKENS_SHORTS
    api_key = require_env("QWEN_API_KEY")
    max_attempts = 2 if profile == "shorts" else MAX_JSON_RETRY

    system_with_json = (
        system_prompt +
        "\n\nCRITICAL: Respond with ONLY a raw JSON object. "
        "No markdown, no ```json fences, no explanation. "
        "Start directly with { and end with }."
    )

    last_error = None
    for model_name in _qwen_models_to_try():
        for attempt in range(1, max_attempts + 1):
            user_msg = user_message + _get_length_hint(profile)
            if attempt > 1:
                user_msg += (
                    f"\n\n[ATTEMPT {attempt}] IMPORTANT: Output ONLY valid JSON. "
                    "No markdown, no prose, no code fences."
                )

            try:
                resp = _post_json_no_proxy(
                    f"{QWEN_API_BASE.rstrip('/')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    payload={
                        "model": model_name,
                        "messages": [
                            {"role": "system", "content": system_with_json},
                            {"role": "user", "content": user_msg},
                        ],
                        # Generator: high creativity (Qwen: no top_k/seed support)
                        "temperature":       max(0.5, 0.90 - (attempt - 1) * 0.10),
                        "top_p":             0.95,
                        "frequency_penalty": 0.35,  # ~repeat_penalty 1.15 di Ollama
                        "max_tokens":        max_tokens,
                    },
                    timeout=300,
                )
                resp.raise_for_status()
            except requests.HTTPError as exc:
                last_error = exc
                status_code = exc.response.status_code if exc.response is not None else None
                logger.warning(f"Qwen model={model_name} HTTP error: {exc}")
                if status_code in (400, 404):
                    break
                if attempt == max_attempts:
                    break
                continue
            except Exception as exc:
                last_error = exc
                logger.warning(f"Qwen model={model_name} request gagal: {exc}")
                if attempt == max_attempts:
                    break
                continue

            raw = (
                resp.json()
                .get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )
            if not raw:
                logger.warning(f"Qwen model={model_name} attempt {attempt}: response kosong, retry...")
                continue

            try:
                return _parse_json_response(raw, profile)
            except (json.JSONDecodeError, ValueError) as e:
                last_error = e
                logger.warning(f"Qwen model={model_name} attempt {attempt}/{max_attempts}: JSON parse gagal â€” {e}")
                if attempt == max_attempts:
                    break

    raise RuntimeError(f"Qwen: semua model/attempt gagal. Error terakhir: {last_error}")


def _call_groq(system_prompt: str, user_message: str, profile: str) -> dict:
    from groq import Groq
    client     = Groq(api_key=require_env("GROQ_API_KEY"))
    max_tokens = SCRIPT_MAX_TOKENS_LONGFORM if profile == "long_form" else SCRIPT_MAX_TOKENS_SHORTS

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
    max_tokens = SCRIPT_MAX_TOKENS_LONGFORM if profile == "long_form" else SCRIPT_MAX_TOKENS_SHORTS

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
        data = _normalize_shorts_schema(data)
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
            "\n\nCRITICAL SHORTS JSON CONTRACT:\n"
            "- You may output either a ready-to-use 'script' OR structured beats that can be assembled.\n"
            "- Preferred structured fields: 'hook_line', 'anchor_line', 'body_beats', 'final_reveal', 'cta_line'.\n"
            "- If you include 'body_beats', make it an array of 2-4 short beat objects or strings.\n"
            "- Include 'visual_beats' with keys opening, middle, ending when possible.\n"
            "- Include 'cta_plan' with start='none', middle='visual_only', end='strong_verbal'.\n"
            "- Include 'creative_direction' with thumbnail_text, thumbnail_style, opening_visual_priority, packaging_angle when possible.\n"
            "- Total spoken narration still MUST exceed 80 words once assembled."
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


def _normalize_shorts_schema(data: dict) -> dict:
    """
    Backward-compatible normalizer for the new beat-based shorts schema.
    The prompt may return a monolithic script, structured beats, or both.
    """
    if not isinstance(data, dict):
        return data

    if not data.get("script"):
        data["script"] = _compose_shorts_script(data)

    if not data.get("visual_cues"):
        visual_cues = _flatten_visual_beats(data.get("visual_beats"))
        if visual_cues:
            data["visual_cues"] = visual_cues

    data.setdefault("cta_plan", {
        "start": "none",
        "middle": "visual_only",
        "end": "strong_verbal",
    })

    if not data.get("cta_line"):
        data["cta_line"] = _extract_last_sentence(data.get("script", ""))

    if not data.get("cta_visual_text"):
        data["cta_visual_text"] = _build_cta_visual_text(data)

    if not isinstance(data.get("hook_score"), (int, float)):
        data["hook_score"] = _infer_hook_score(data)

    if not data.get("hook_reason"):
        data["hook_reason"] = _infer_hook_reason(data)

    if not data.get("music_mood"):
        data["music_mood"] = "dark intro"

    data.setdefault("music_direction", "")
    if not isinstance(data.get("music_keywords"), list):
        data["music_keywords"] = []
    if not isinstance(data.get("music_arc"), list):
        data["music_arc"] = []

    if not data.get("keywords"):
        data["keywords"] = data.get("tags", [])[:5]

    _normalize_creative_direction(data)
    _normalize_cta_language(data)

    return data


def _compose_shorts_script(data: dict) -> str:
    parts: list[str] = []

    for key in ("hook_line", "anchor_line"):
        value = data.get(key, "")
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())

    body_beats = data.get("body_beats", [])
    if isinstance(body_beats, list):
        for beat in body_beats:
            if isinstance(beat, dict):
                text = beat.get("text") or beat.get("line") or beat.get("naration") or beat.get("narasi")
            else:
                text = beat
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())

    for key in ("final_reveal", "cta_line"):
        value = data.get(key, "")
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())

    return " ".join(parts).strip()


def _flatten_visual_beats(visual_beats) -> list[str]:
    if not isinstance(visual_beats, dict):
        return []

    ordered_sections = ("opening", "middle", "ending")
    flattened: list[str] = []
    for section in ordered_sections:
        beats = visual_beats.get(section, [])
        if isinstance(beats, str):
            beats = [beats]
        if not isinstance(beats, list):
            continue
        for beat in beats:
            if isinstance(beat, dict):
                text = beat.get("cue") or beat.get("text") or beat.get("visual")
            else:
                text = beat
            if isinstance(text, str) and text.strip():
                flattened.append(text.strip())
    return flattened


def _extract_last_sentence(text: str) -> str:
    if not text:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", " ".join(text.split()))
    return sentences[-1].strip() if sentences else text.strip()


def _build_cta_visual_text(data: dict) -> str:
    hook_type = (data.get("hook_type") or "").strip().lower()
    title = " ".join(str(data.get("title", "")).upper().split())
    final_reveal = " ".join(str(data.get("final_reveal", "")).upper().split())

    if hook_type == "forbidden_record":
        return "SIMPAN SEBELUM HILANG"
    if hook_type == "conspiracy_reveal":
        return "RAHASIANYA BESOK"
    if hook_type == "body_horror":
        return "KALAU BERANI LANJUT"
    if hook_type == "historical_shock":
        return "BAGIAN GELAPNYA BESOK"
    if hook_type == "stat_attack":
        return "FAKTANYA MAKIN GILA"

    keyword_map = [
        ("GEREJA", "ARSIPNYA BESOK"),
        ("RITUAL", "JANGAN TONTON SENDIRI"),
        ("KASET", "PUTAR BESOK MALAM"),
        ("RUMAH SAKIT", "SIMPAN DULU"),
        ("SETAN", "KALAU BERANI LANJUT"),
    ]
    combined = f"{title} {final_reveal}"
    for needle, cta in keyword_map:
        if needle in combined:
            return cta
    return "KALAU BERANI LANJUT"


def _infer_hook_score(data: dict) -> float:
    hook_meta = data.get("hook_meta", {})
    if isinstance(hook_meta, dict) and isinstance(hook_meta.get("score"), (int, float)):
        return float(hook_meta["score"])

    opening = str(data.get("hook_line") or data.get("hook") or "").strip()
    words = len(opening.split())
    score = 7.0
    if 6 <= words <= 18:
        score += 0.7
    if any(token in opening.lower() for token in ("gereja", "rahasia", "ritual", "arsip", "kaset", "setan", "vatican", "forbidden", "secret")):
        score += 0.6
    return round(min(score, 9.3), 1)


def _infer_hook_reason(data: dict) -> str:
    hook_meta = data.get("hook_meta", {})
    if isinstance(hook_meta, dict) and hook_meta.get("reason"):
        return str(hook_meta.get("reason")).strip()

    hook_type = (data.get("hook_type") or "").strip().lower()
    reasons = {
        "forbidden_record": "Hook langsung menjual file terlarang atau bukti tersembunyi sejak detik pertama.",
        "conspiracy_reveal": "Hook langsung membuka rasa cover-up dan membuat penonton ingin tahu apa yang disembunyikan.",
        "historical_shock": "Hook menggabungkan konteks lama dengan ancaman yang terasa masih hidup.",
        "body_horror": "Hook memicu rasa jijik dan takut dengan anomali fisik yang sulit dilupakan.",
        "stat_attack": "Hook terasa dingin dan cepat karena menyerang dengan fakta atau klaim yang keras.",
    }
    return reasons.get(hook_type, "Hook membuka dengan ancaman atau misteri yang cukup tajam untuk menahan swipe.")


def _normalize_creative_direction(data: dict) -> None:
    creative = data.get("creative_direction")
    if not isinstance(creative, dict):
        creative = {}

    if not creative.get("thumbnail_text"):
        creative["thumbnail_text"] = _build_thumbnail_text(data)

    if not creative.get("thumbnail_style"):
        hook_type = (data.get("hook_type") or "").strip().lower()
        style_map = {
            "forbidden_record": "forbidden evidence",
            "conspiracy_reveal": "archival conspiracy",
            "historical_shock": "historical dread",
            "body_horror": "body horror shock",
            "stat_attack": "cold evidence shock",
        }
        creative["thumbnail_style"] = style_map.get(hook_type, "dark evidence shock")

    if not creative.get("opening_visual_priority"):
        opening = data.get("visual_beats", {}).get("opening", []) if isinstance(data.get("visual_beats"), dict) else []
        if isinstance(opening, list) and opening:
            creative["opening_visual_priority"] = str(opening[0]).strip()
        else:
            creative["opening_visual_priority"] = str(data.get("hook_line", "")).strip()[:80]

    if not creative.get("packaging_angle"):
        creative["packaging_angle"] = _build_packaging_angle(data)

    data["creative_direction"] = creative


def _build_thumbnail_text(data: dict) -> str:
    text = " ".join(
        str(item) for item in [
            data.get("hook_type", ""),
            data.get("title", ""),
            data.get("hook_line", ""),
            data.get("final_reveal", ""),
        ] if item
    ).upper()
    mappings = [
        ("FORBIDDEN", "TERLARANG"),
        ("SECRET", "RAHASIA"),
        ("RECORD", "ARSIP"),
        ("FILE", "BERKAS"),
        ("EXORC", "EXORCISM"),
        ("CHURCH", "GEREJA"),
        ("RITUAL", "RITUAL"),
        ("VATICAN", "VATICAN"),
        ("CURSE", "KUTUKAN"),
        ("VOICE", "SUARA"),
        ("TAPE", "KASET"),
        ("HOSPITAL", "RUMAH SAKIT"),
        ("DEMON", "SETAN"),
    ]
    for needle, replacement in mappings:
        if needle in text:
            return replacement
    return "MENCEKAM"


def _build_packaging_angle(data: dict) -> str:
    hook_type = (data.get("hook_type") or "").strip().lower()
    angle_map = {
        "forbidden_record": "forbidden file",
        "conspiracy_reveal": "hidden cover-up",
        "historical_shock": "dark historical proof",
        "body_horror": "disturbing physical anomaly",
        "stat_attack": "cold shocking proof",
    }
    return angle_map.get(hook_type, "dark hidden evidence")


def _normalize_cta_language(data: dict) -> None:
    cta_line = data.get("cta_line")
    if isinstance(cta_line, str) and cta_line.strip():
        normalized = re.sub(r"^\s*follow\b", "Subscribe", cta_line, flags=re.IGNORECASE)
        normalized = re.sub(r"^\s*ikuti\b", "Subscribe", normalized, flags=re.IGNORECASE)
        data["cta_line"] = normalized.strip()

    cta_visual_text = data.get("cta_visual_text")
    if isinstance(cta_visual_text, str) and cta_visual_text.strip():
        normalized_visual = re.sub(r"^\s*follow\b", "SUBSCRIBE", cta_visual_text, flags=re.IGNORECASE)
        normalized_visual = re.sub(r"^\s*ikuti\b", "SUBSCRIBE", normalized_visual, flags=re.IGNORECASE)
        if "LEAK" in normalized_visual.upper():
            normalized_visual = "SUBSCRIBE SEKARANG"
        data["cta_visual_text"] = normalized_visual.strip()


def review_and_iterate(script_data: dict, channel: dict, profile: str = "shorts") -> dict:
    """
    Retention-first review loop for Shorts.

    Flow:
    1. AI reviewer scores the draft.
    2. If score is below threshold, reviewer must directly rewrite the script JSON.
    3. Revised draft is scored one final time.
    4. If still below threshold, keep the revised version anyway to avoid infinite loops.
    """
    if profile != "shorts":
        return script_data

    current = deepcopy(script_data)
    threshold = SHORTS_REVIEW_MIN_SCORE

    first_review = _review_script_payload(current, channel, threshold, mode="rewrite_if_below")
    initial_score = float(first_review.get("score", 0))
    rewritten = False

    if initial_score < threshold and isinstance(first_review.get("updated_script"), dict):
        current = _merge_reviewed_script(current, first_review["updated_script"], profile)
        current = inject_hook(current, channel)
        rewritten = True

    final_score = initial_score
    final_review = first_review
    if rewritten and SHORTS_REVIEW_MAX_PASSES >= 2:
        final_review = _review_script_payload(current, channel, threshold, mode="score_only")
        final_score = float(final_review.get("score", initial_score))

    current["review_meta"] = {
        "threshold": threshold,
        "initial_score": initial_score,
        "final_score": final_score,
        "initial_status": first_review.get("status", "unknown"),
        "final_status": final_review.get("status", first_review.get("status", "unknown")),
        "rewritten": rewritten,
        "approved": final_score >= threshold or rewritten,
        "provider": final_review.get("provider") or first_review.get("provider"),
        "notes": final_review.get("reason") or first_review.get("reason", ""),
    }
    current["retention_score"] = final_score
    logger.info(
        f"[{channel['id']}] Review retention: {initial_score:.1f} -> {final_score:.1f} "
        f"| rewritten={rewritten}"
    )
    return current


def _review_script_payload(script_data: dict, channel: dict, threshold: float, mode: str) -> dict:
    prompt = _build_review_prompt(script_data, channel, threshold, mode)
    providers = ["DeepSeek/Ollama", "Groq"]
    if os.getenv("QWEN_API_KEY"):
        providers.append("Qwen")
    random.shuffle(providers)

    last_error = None
    for provider in providers:
        try:
            raw = _call_review_provider(provider, prompt)
            data = _clean_raw_json(raw)
            review = json.loads(data)
            review["provider"] = provider
            return review
        except Exception as exc:
            last_error = exc
            logger.warning(f"Review provider {provider} gagal: {exc}")

    raise RuntimeError(f"Semua reviewer gagal. Error terakhir: {last_error}")


def _call_review_provider(provider: str, prompt: str) -> str:
    if provider == "DeepSeek/Ollama":
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json={
                "model": OLLAMA_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a ruthless short-form editor. Output JSON only."},
                    {"role": "user", "content": prompt},
                ],
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.35, "num_predict": SCRIPT_REVIEW_MAX_TOKENS, "num_ctx": 8192},
            },
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json().get("message", {}).get("content", "").strip()

    if provider == "Qwen":
        last_error = None
        for model_name in _qwen_models_to_try():
            try:
                resp = _post_json_no_proxy(
                    f"{QWEN_API_BASE.rstrip('/')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {require_env('QWEN_API_KEY')}",
                        "Content-Type": "application/json",
                    },
                    payload={
                        "model": model_name,
                        "messages": [
                            {"role": "system", "content": "You are a ruthless short-form editor. Output JSON only."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.35,
                        "max_tokens": SCRIPT_REVIEW_MAX_TOKENS,
                    },
                    timeout=300,
                )
                resp.raise_for_status()
                return (
                    resp.json()
                    .get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                    .strip()
                )
            except Exception as exc:
                last_error = exc
                logger.warning(f"Review Qwen model={model_name} gagal: {exc}")
        raise RuntimeError(f"Review Qwen gagal di semua model: {last_error}")

    if provider == "Groq":
        from groq import Groq

        client = Groq(api_key=require_env("GROQ_API_KEY"))
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a ruthless short-form editor. Output JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.35,
            max_tokens=SCRIPT_REVIEW_MAX_TOKENS,
        )
        return resp.choices[0].message.content.strip()

    raise ValueError(f"Provider review tidak dikenal: {provider}")


def _build_review_prompt(script_data: dict, channel: dict, threshold: float, mode: str) -> str:
    language = "Bahasa Indonesia" if channel.get("language") == "id" else "English"
    niche = channel.get("niche", "horror_facts")
    mode_rules = (
        "If the score is below threshold, you MUST rewrite the script directly and return a full updated_script object."
        if mode == "rewrite_if_below"
        else "Score only. Do NOT rewrite the script. Set updated_script to null."
    )

    return f"""You are a retention-first YouTube Shorts editor.
Evaluate this script for retention, not factual accuracy.

Editorial stance:
- Creepypasta, urban legend, and dark plausible storytelling are allowed.
- Judge only on hook strength, escalation, pacing, tension, ending residue, CTA fit, and visual sharpness.
- Do not punish the script for being speculative if it sounds cinematic and sticky.
- Avoid generic filler and avoid anti-climactic explanations.
- Ending visual beats must not contain UI instructions like subscribe button overlays.
- Prefer music moods 'dark intro' or 'horror tension' for aggressive horror openings.
- Make thumbnail_text sharp, short, and dark. Avoid lazy generic words if a more specific hook-word exists.
- Pattern interrupt must actually bend the story away from the viewer's expected direction, not just add another shocking fact.
- hook_score and hook_reason should match the real strength of the opening.

Language: {language}
Niche: {niche}
Threshold: {threshold}
Mode: {mode}
{mode_rules}

Return JSON only:
{{
  "score": 0-10,
  "status": "approved|rewrite|forced_pass",
  "reason": "short reason",
  "updated_script": {{
    "title": "...",
    "hook_type": "...",
    "hook_line": "...",
    "hook_score": 8,
    "hook_reason": "...",
    "anchor_line": "...",
    "body_beats": [{{"purpose":"...","text":"..."}}],
    "final_reveal": "...",
    "cta_line": "...",
    "script": "...",
    "cta_plan": {{"start":"none","middle":"visual_only","end":"strong_verbal"}},
    "cta_visual_text": "...",
    "music_mood": "...",
    "music_direction": "...",
    "music_keywords": ["..."],
    "music_arc": ["..."],
    "creative_direction": {{
      "thumbnail_text": "...",
      "thumbnail_style": "...",
      "opening_visual_priority": "...",
      "packaging_angle": "..."
    }},
    "keywords": ["..."],
    "tags": ["..."],
    "description": "...",
    "visual_beats": {{"opening":["..."],"middle":["..."],"ending":["..."]}},
    "visual_cues": ["..."]
  }}
}}

SCRIPT JSON:
{json.dumps(script_data, ensure_ascii=False, indent=2)}
"""


def _merge_reviewed_script(original: dict, updated: dict, profile: str) -> dict:
    merged = deepcopy(original)
    for key, value in updated.items():
        merged[key] = value

    merged["topic"] = original.get("topic", merged.get("topic"))
    merged["profile"] = original.get("profile", profile)
    merged["is_viral_iteration"] = original.get("is_viral_iteration", False)

    merged = _normalize_shorts_schema(merged)
    return _validate_and_fix(merged, profile)
