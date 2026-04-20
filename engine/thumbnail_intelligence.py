"""
thumbnail_intelligence.py — Creative Style Library untuk Thumbnail Anti-Repetisi.

Konsep:
  Setiap thumbnail dihasilkan dari "style template" — kombinasi text_pattern,
  color_variant, dan hook_angle. LLM menggunakan pattern sebagai constraint
  untuk menghasilkan teks yang segar tapi tetap berpola kuat.

  Anti-repetisi:
    - Lacak 15 style_id terakhir yang dipakai
    - Prioritaskan style yang paling lama tidak digunakan
    - Trigger generate style baru jika library < MIN_ACTIVE_STYLES

  CTR feedback loop: TIDAK diimplementasikan sekarang.
  Tambahkan setelah 50+ video terkumpul.

Usage:
    from engine.thumbnail_intelligence import pick_and_generate_text
    text = pick_and_generate_text(channel, title, script_data)
    # → "TERKUBUR RAPAT" atau "3 FAKTA YANG DISEMBUNYIKAN"
"""

from __future__ import annotations

import json
import os
import random
import re
import requests
from datetime import datetime
from engine.utils import get_logger, channel_data_path

logger = get_logger("thumbnail_intelligence")

# ── Constants ─────────────────────────────────────────────────────────────────
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

# Berapa style_id terakhir yang tidak boleh diulang
ANTI_REPEAT_WINDOW = 15

# Minimum jumlah style aktif di library sebelum trigger generasi baru
MIN_ACTIVE_STYLES = 5

# Generator timeout
GEN_TIMEOUT = 45


# ── Predefined Seed Library ───────────────────────────────────────────────────
# Seed ini dipakai saat pertama kali library dibuat per channel.
# LLM akan menambah variasi baru di atas seed ini.

SEED_STYLES: dict[str, dict[str, list]] = {
    "horror_facts": {
        "id": [
            {
                "style_id": "horror_id_s001",
                "text_pattern": "KATA_TUNGGAL_CAPSLOCK",
                "description": "Satu kata shocking, all caps, tanpa tanda baca berlebihan",
                "examples": ["TERPENDAM", "DILARANG", "DISENSOR", "TERKUBUR", "TERSEGEL"],
                "color_variant": 1,
                "hook_angle": "forbidden_record",
            },
            {
                "style_id": "horror_id_s002",
                "text_pattern": "ANGKA_PLUS_KLAIM",
                "description": "Angka kecil + klaim mengejutkan, contoh: '3 FAKTA YANG DISEMBUNYIKAN'",
                "examples": ["3 FAKTA DISEMBUNYIKAN", "7 KASUS TERLARANG", "1 REKAMAN HILANG"],
                "color_variant": 3,
                "hook_angle": "stat_attack",
            },
            {
                "style_id": "horror_id_s003",
                "text_pattern": "PERNYATAAN_PARADOKS",
                "description": "Kalimat pendek yang terasa kontradiktif atau mengejutkan",
                "examples": ["BUKAN FIKSI", "INI NYATA", "MEREKA TAHU", "SUDAH TERLAMBAT"],
                "color_variant": 1,
                "hook_angle": "conspiracy_reveal",
            },
            {
                "style_id": "horror_id_s004",
                "text_pattern": "PERINGATAN_LANGSUNG",
                "description": "Peringatan yang mengajak pembaca berhenti sebelum melanjutkan",
                "examples": ["JANGAN TONTON SENDIRI", "HATI-HATI", "SIMPAN DULU", "AWAS"],
                "color_variant": 3,
                "hook_angle": "body_horror",
            },
            {
                "style_id": "horror_id_s005",
                "text_pattern": "LABEL_DOKUMEN",
                "description": "Terasa seperti label file rahasia atau arsip",
                "examples": ["ARSIP RAHASIA", "REKAMAN GELAP", "DIKLASIFIKASIKAN", "FILE TERLARANG"],
                "color_variant": 2,
                "hook_angle": "forbidden_record",
            },
        ],
        "en": [
            {
                "style_id": "horror_en_s001",
                "text_pattern": "SINGLE_WORD_CAPSLOCK",
                "description": "One shocking word, all caps, no excessive punctuation",
                "examples": ["BURIED", "BANNED", "SEALED", "ERASED", "FORBIDDEN"],
                "color_variant": 1,
                "hook_angle": "forbidden_record",
            },
            {
                "style_id": "horror_en_s002",
                "text_pattern": "NUMBER_PLUS_CLAIM",
                "description": "Small number + shocking claim, e.g. '3 FACTS THEY HIDE'",
                "examples": ["3 HIDDEN FACTS", "7 BANNED CASES", "1 LOST RECORDING"],
                "color_variant": 3,
                "hook_angle": "stat_attack",
            },
            {
                "style_id": "horror_en_s003",
                "text_pattern": "PARADOX_STATEMENT",
                "description": "Short sentence that feels contradictory or shocking",
                "examples": ["NOT FICTION", "THIS IS REAL", "THEY KNOW", "TOO LATE"],
                "color_variant": 1,
                "hook_angle": "conspiracy_reveal",
            },
            {
                "style_id": "horror_en_s004",
                "text_pattern": "DIRECT_WARNING",
                "description": "Warning that makes the viewer hesitate before watching",
                "examples": ["DON'T WATCH ALONE", "BE CAREFUL", "SAVE THIS FIRST", "WARNING"],
                "color_variant": 3,
                "hook_angle": "body_horror",
            },
            {
                "style_id": "horror_en_s005",
                "text_pattern": "DOCUMENT_LABEL",
                "description": "Feels like a classified file or declassified archive label",
                "examples": ["CLASSIFIED", "DARK ARCHIVE", "DECLASSIFIED", "FORBIDDEN FILE"],
                "color_variant": 2,
                "hook_angle": "forbidden_record",
            },
        ],
    },
    "psychology": {
        "id": [
            {
                "style_id": "psych_id_s001",
                "text_pattern": "PERTANYAAN_PROVOKATIF",
                "description": "Pertanyaan singkat yang membuat orang mempertanyakan diri sendiri",
                "examples": ["KAMU BISA DIMANIPULASI?", "OTAKMU BOHONG?", "KAMU SADAR?"],
                "color_variant": 2,
                "hook_angle": "conspiracy_reveal",
            },
            {
                "style_id": "psych_id_s002",
                "text_pattern": "KATA_TUNGGAL_CAPSLOCK",
                "description": "Satu kata psikologis yang powerful",
                "examples": ["MANIPULASI", "ILUSI", "TERPENJARA", "TERPROGRAM"],
                "color_variant": 1,
                "hook_angle": "stat_attack",
            },
            {
                "style_id": "psych_id_s003",
                "text_pattern": "KLAIM_SAINS",
                "description": "Klaim terasa seperti fakta ilmiah yang mengejutkan",
                "examples": ["OTAK BISA MEMBOHONGI", "80% TIDAK SADAR", "TERBUKTI SECARA ILMIAH"],
                "color_variant": 2,
                "hook_angle": "historical_shock",
            },
        ],
        "en": [
            {
                "style_id": "psych_en_s001",
                "text_pattern": "PROVOCATIVE_QUESTION",
                "description": "Short question making people question themselves",
                "examples": ["CAN YOU BE MANIPULATED?", "IS YOUR BRAIN LYING?", "ARE YOU AWARE?"],
                "color_variant": 2,
                "hook_angle": "conspiracy_reveal",
            },
            {
                "style_id": "psych_en_s002",
                "text_pattern": "SINGLE_WORD_CAPSLOCK",
                "description": "One powerful psychological word",
                "examples": ["MANIPULATION", "ILLUSION", "TRAPPED", "PROGRAMMED"],
                "color_variant": 1,
                "hook_angle": "stat_attack",
            },
            {
                "style_id": "psych_en_s003",
                "text_pattern": "SCIENCE_CLAIM",
                "description": "Claim that feels like a shocking scientific fact",
                "examples": ["YOUR BRAIN LIES", "80% ARE UNAWARE", "SCIENTIFICALLY PROVEN"],
                "color_variant": 2,
                "hook_angle": "historical_shock",
            },
        ],
    },
}


# ─── Public Entry Point ───────────────────────────────────────────────────────

def pick_and_generate_text(channel: dict, title: str, script_data: dict) -> str:
    """
    Pilih style dari library channel, generate teks thumbnail menggunakan LLM.

    Return: str teks untuk thumbnail (misal "TERKUBUR RAPAT" atau "3 FAKTA DISEMBUNYIKAN")
    Fallback ke None jika gagal — caller harus handle dengan _get_contextual_text().
    """
    ch_id    = channel["id"]
    niche    = channel.get("niche", "horror_facts")
    language = channel.get("language", "id")

    try:
        library = _load_or_init_library(ch_id, niche, language)
        style   = _pick_style(library)

        if style is None:
            logger.info(f"[{ch_id}] Tidak ada style tersedia — pakai contextual fallback")
            return None

        text = _generate_text_from_style(style, title, niche, language, script_data)

        if text:
            _mark_style_used(library, style["style_id"], ch_id)
            logger.info(f"[{ch_id}] 🎨 Thumbnail text: '{text}' (style: {style['style_id']})")
            return text

    except Exception as exc:
        logger.warning(f"[{ch_id}] thumbnail_intelligence error: {exc} — pakai contextual fallback")

    return None


# ─── Library Management ───────────────────────────────────────────────────────

def _library_path(ch_id: str) -> str:
    out_dir = channel_data_path(ch_id, "thumbnail_library")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, "style_library.json")


def _load_or_init_library(ch_id: str, niche: str, language: str) -> dict:
    """Load library dari disk. Jika belum ada, inisialisasi dari seed."""
    path = _library_path(ch_id)

    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                library = json.load(f)
            # Trigger gen styles baru jika library kurang dari minimum
            available = _available_styles(library)
            if len(available) < MIN_ACTIVE_STYLES:
                logger.info(f"[{ch_id}] Library kurang dari {MIN_ACTIVE_STYLES} style aktif — generate baru")
                library = _expand_library(library, niche, language, ch_id)
                _save_library(library, path)
            return library
        except Exception as exc:
            logger.warning(f"[{ch_id}] Library corrupt ({exc}) — reinit dari seed")

    # Buat fresh dari seed
    seeds = SEED_STYLES.get(niche, {}).get(language, [])
    library = {
        "version": 1,
        "channel_id": ch_id,
        "niche": niche,
        "language": language,
        "styles": seeds,
        "usage_log": [],          # List style_id yang terakhir dipakai (max ANTI_REPEAT_WINDOW)
        "updated_at": datetime.now().isoformat(),
    }
    _save_library(library, path)
    logger.info(f"[{ch_id}] Library baru dibuat: {len(seeds)} seed styles")
    return library


def _save_library(library: dict, path: str) -> None:
    library["updated_at"] = datetime.now().isoformat()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(library, f, ensure_ascii=False, indent=2)


def _available_styles(library: dict) -> list[dict]:
    """Return styles yang tidak ada dalam usage_log (anti-repetisi)."""
    recent_ids = set(library.get("usage_log", [])[-ANTI_REPEAT_WINDOW:])
    return [
        style for style in library.get("styles", [])
        if style.get("style_id") not in recent_ids
    ]


def _pick_style(library: dict) -> dict | None:
    """Pilih style yang paling lama tidak dipakai dari available pool."""
    available = _available_styles(library)
    if not available:
        # Semua style sudah dipakai → reset window dan pakai yang paling stale
        all_styles = library.get("styles", [])
        if not all_styles:
            return None
        usage_log = library.get("usage_log", [])
        # Sort: style yang tidak ada di usage_log (atau paling awal) diprioritaskan
        def staleness_key(s):
            sid = s.get("style_id", "")
            try:
                return usage_log[::-1].index(sid)  # Posisi dari belakang = lebih stale
            except ValueError:
                return len(usage_log)  # Tidak ada di log = paling stale
        all_styles.sort(key=staleness_key)
        return all_styles[0]
    return available[0]


def _mark_style_used(library: dict, style_id: str, ch_id: str) -> None:
    """Catat penggunaan style dan persist ke disk."""
    usage_log = library.get("usage_log", [])
    usage_log.append(style_id)
    # Kurangi ukuran log agar tidak membengkak tanpa batas
    library["usage_log"] = usage_log[-50:]
    path = _library_path(ch_id)
    _save_library(library, path)


def _expand_library(library: dict, niche: str, language: str, ch_id: str) -> dict:
    """Generate 3 style baru via LLM dan tambahkan ke library."""
    existing_styles = library.get("styles", [])
    existing_patterns = [s.get("text_pattern", "") for s in existing_styles]
    existing_ids = {s.get("style_id", "") for s in existing_styles}

    new_styles = _generate_new_styles(niche, language, existing_patterns, ch_id)
    added = 0
    for style in new_styles:
        style_id = style.get("style_id", "")
        if style_id and style_id not in existing_ids:
            library["styles"].append(style)
            existing_ids.add(style_id)
            added += 1

    logger.info(f"[{ch_id}] Library expanded: +{added} style baru")
    return library


# ─── LLM: Generate Text dari Style ───────────────────────────────────────────

def _generate_text_from_style(
    style: dict,
    title: str,
    niche: str,
    language: str,
    script_data: dict,
) -> str | None:
    """
    Gunakan LLM untuk membuat teks thumbnail berdasarkan style pattern + konteks judul.
    Provider: Qwen primary → Ollama fallback. Groq tidak dipakai di sini.
    """
    pattern     = style.get("text_pattern", "")
    description = style.get("description", "")
    examples    = style.get("examples", [])
    hook_line   = str(script_data.get("hook_line") or script_data.get("hook") or "").strip()

    lang_label = "Bahasa Indonesia" if language == "id" else "English"
    examples_str = ", ".join(f'"{e}"' for e in examples[:4])

    prompt = f"""You are a viral YouTube thumbnail copywriter.
Create ONE short text for a thumbnail using the exact style pattern below.

Content context:
- Title: {title}
- Hook: {hook_line or "N/A"}
- Niche: {niche}
- Language: {lang_label}

Style to follow:
- Pattern: {pattern}
- Pattern description: {description}
- Examples of this pattern: {examples_str}

Rules:
- MAX 4 words (ideally 1-3 words)
- ALL CAPS
- Must feel urgent, dark, or shocking
- Must match the pattern description exactly
- Do NOT repeat the examples verbatim — create a FRESH variation
- Return ONLY the text, nothing else. No quotes, no explanation."""

    # Provider order: Qwen primary → Ollama fallback (no Groq)
    providers = ["qwen", "ollama"]
    if not os.getenv("QWEN_API_KEY"):
        providers = ["ollama"]

    for provider in providers:
        try:
            if provider == "qwen":
                result = _qwen_generate(prompt)
            else:
                result = _ollama_generate(prompt)

            if result:
                # Clean dan validasi
                text = result.strip().strip('"\'').upper()
                text = re.sub(r"\s+", " ", text).strip()
                words = text.split()
                if 1 <= len(words) <= 6:
                    return text
                # Ambil max 4 kata jika model terlalu verbose
                return " ".join(words[:4])

        except Exception as exc:
            logger.debug(f"[thumb_intel] {provider} text gen gagal: {exc}")

    return None


def _generate_new_styles(
    niche: str,
    language: str,
    existing_patterns: list[str],
    ch_id: str,
) -> list[dict]:
    """Generate 3 style template baru via LLM."""
    lang_label = "Bahasa Indonesia" if language == "id" else "English"
    existing_str = ", ".join(existing_patterns[:8]) if existing_patterns else "none"

    timestamp_suffix = datetime.now().strftime("%m%d%H%M")

    prompt = f"""You are a viral content strategist creating thumbnail text style templates.
Generate 3 NEW thumbnail text style patterns for {niche} content in {lang_label}.

Existing patterns (DO NOT duplicate): {existing_str}

Each style must be a unique formula — a constraint that guides text creation.
Return ONLY valid JSON array, no markdown:
[
  {{
    "style_id": "{niche[:5]}_{language}_{timestamp_suffix}_a",
    "text_pattern": "PATTERN_NAME_CAPSLOCK",
    "description": "One sentence describing what makes this pattern work",
    "examples": ["EXAMPLE1", "EXAMPLE2", "EXAMPLE3"],
    "color_variant": 1,
    "hook_angle": "forbidden_record"
  }}
]

hook_angle options: forbidden_record, conspiracy_reveal, body_horror, historical_shock, stat_attack
color_variant options: 1 (red/yellow), 2 (cyan/navy), 3 (red/black)
Rules:
- Each pattern must be meaningfully different from existing ones
- Examples must be 1-4 words max, all caps
- Patterns must work for viral short-form content
- Return exactly 3 style objects"""

    providers = ["qwen", "ollama"]
    if not os.getenv("QWEN_API_KEY"):
        providers = ["ollama"]

    for provider in providers:
        try:
            if provider == "qwen":
                raw = _qwen_generate(prompt)
            else:
                raw = _ollama_generate(prompt)

            if not raw:
                continue

            # Parse JSON array
            cleaned = raw.strip()
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s*```$", "", cleaned).strip()

            # Find array
            if not cleaned.startswith("["):
                match = re.search(r"\[.*\]", cleaned, re.DOTALL)
                if match:
                    cleaned = match.group()

            styles = json.loads(cleaned)
            if isinstance(styles, list) and styles:
                logger.info(f"[{ch_id}] Generated {len(styles)} new styles via {provider}")
                return styles[:3]

        except Exception as exc:
            logger.debug(f"[thumb_intel] Style gen {provider} gagal: {exc}")

    logger.warning(f"[{ch_id}] Gagal generate style baru — library tidak di-expand")
    return []


# ─── Provider Calls (lightweight, no Groq) ───────────────────────────────────

def _qwen_generate(prompt: str) -> str:
    api_key = os.getenv("QWEN_API_KEY", "")
    if not api_key:
        raise RuntimeError("QWEN_API_KEY tidak tersedia")

    for model_name in _qwen_models_to_try():
        session = requests.Session()
        session.trust_env = False
        try:
            resp = session.post(
                f"{QWEN_API_BASE.rstrip('/')}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model_name,
                    "messages": [{"role": "user", "content": prompt}],
                    # Thumbnail generator: creative but controlled (Qwen: no top_k/seed)
                    "temperature":       0.75,
                    "top_p":             0.95,
                    "frequency_penalty": 0.25,  # ~repeat_penalty 1.1 di Ollama
                    "max_tokens":        200,
                },
                timeout=GEN_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (400, 404):
                continue
            raise
        finally:
            session.close()

    raise RuntimeError("Semua Qwen model gagal")


def _ollama_generate(prompt: str) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {
            # Thumbnail generator: creative but controlled
            "temperature":    0.75,
            "top_p":          0.95,
            "top_k":          45,
            "repeat_penalty": 1.10,
            "num_predict":    200,
            "num_ctx":        2048,
            "seed":           -1,
        },
    }
    resp = requests.post(
        f"{OLLAMA_BASE_URL}/api/chat",
        json=payload,
        timeout=GEN_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("message", {}).get("content", "").strip()


def _qwen_models_to_try() -> list[str]:
    models: list[str] = []
    preferred = QWEN_MODEL.strip() if QWEN_MODEL else ""
    if preferred:
        models.append(preferred)
    for model in QWEN_MODEL_CANDIDATES:
        if model not in models:
            models.append(model)
    return models
