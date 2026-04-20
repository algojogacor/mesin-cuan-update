"""
hook_engine.py - Manage hook metadata for short-form scripts.

The generator prompt now owns the primary opening line. This module only:
1. Preserves a clean hook field for downstream tools.
2. Fills intro visual keyword hints when helpful.
3. Avoids injecting generic extra lines that dilute the opening.
"""

import json
import os
import random
import re
import requests
from engine.utils import get_logger, require_env

logger = get_logger("hook_engine")

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "deepseek-v3.1:671b-cloud")

HOOK_TEMPLATES = {
    "horror_facts": {
        "id": [
            {"text": "Ini bukan cerita film. Ini catatan yang benar-benar ada.", "visual": "declassified archive document"},
            {"text": "Kasus ini jauh lebih buruk dari versi yang pernah kamu dengar.", "visual": "black and white evidence board"},
            {"text": "Yang disembunyikan dari kasus ini justru bagian paling mengerikan.", "visual": "sealed medical file"},
        ],
        "en": [
            {"text": "This was never just a movie story. The records were real.", "visual": "declassified archive document"},
            {"text": "The worst part of this case is the part people stopped talking about.", "visual": "sealed medical file"},
            {"text": "The truth behind this case is uglier than the version you know.", "visual": "black and white evidence board"},
        ],
    },
    "psychology": {
        "id": [
            {"text": "Bagian paling berbahaya dari otakmu sering bekerja tanpa izinmu.", "visual": "brain scan close up"},
            {"text": "Yang kamu anggap keputusan bebas bisa jadi cuma pola yang dipicu.", "visual": "neural network abstract"},
        ],
        "en": [
            {"text": "The most dangerous part of your mind usually works without permission.", "visual": "brain scan close up"},
            {"text": "What feels like a free choice may be a pattern that got triggered.", "visual": "neural network abstract"},
        ],
    },
}


def inject_hook(script_data: dict, channel: dict) -> dict:
    """
    Populate hook metadata without bloating the actual narration.

    Shorts rely on a tight cold open, so we avoid prepending generic copy unless
    the script is missing a usable opening altogether.
    """
    niche = channel["niche"]
    language = channel["language"]
    ch_id = channel["id"]
    profile = script_data.get("profile", "shorts")

    target_text = script_data.get("script") or script_data.get("intro") or ""
    if not target_text.strip():
        logger.warning(f"[{ch_id}] hook_engine: teks narasi kosong, skip")
        return script_data

    native_hook = (
        script_data.get("hook_line")
        or script_data.get("hook")
        or _extract_opening_sentence(target_text)
    ).strip()

    generated_hook = ""
    hook_data = None
    if not native_hook or _needs_hook_help(native_hook, profile):
        hook_data = _generate_hook_ai(target_text, niche, language, ch_id) or _pick_template_hook(
            niche, language
        )
        generated_hook = (hook_data or {}).get("text", "").strip()

    final_hook = native_hook or generated_hook
    if final_hook:
        script_data["hook"] = final_hook

    if profile != "shorts" and generated_hook and generated_hook != native_hook:
        if "intro" in script_data and script_data["intro"].strip():
            script_data["intro"] = f"{generated_hook}\n\n{script_data['intro']}"
            script_data["hook"] = generated_hook

    visual_hint = ""
    if hook_data and hook_data.get("visual"):
        visual_hint = hook_data["visual"]
    elif script_data.get("visual_beats", {}).get("opening"):
        visual_hint = script_data["visual_beats"]["opening"][0]

    if visual_hint:
        script_data.setdefault("visual_keywords", {})
        script_data["visual_keywords"].setdefault("intro", visual_hint)

    logger.info(f"[{ch_id}] Hook ready: {script_data.get('hook', '')[:60]}...")
    return script_data


def _extract_opening_sentence(text: str) -> str:
    if not text:
        return ""
    clean = " ".join(text.strip().split())
    parts = re.split(r"(?<=[.!?])\s+", clean, maxsplit=1)
    return (parts[0] if parts else clean).strip()


def _needs_hook_help(opening: str, profile: str) -> bool:
    if not opening:
        return True
    if profile != "shorts":
        return False
    words = opening.split()
    if len(words) > 22:
        return True
    bland_patterns = (
        "tahukah kamu",
        "pernahkah kamu",
        "did you know",
        "have you ever",
    )
    return any(pat in opening.lower() for pat in bland_patterns)


def _generate_hook_ai(narration: str, niche: str, language: str, channel_id: str) -> dict | None:
    lang_label = "Bahasa Indonesia" if language == "id" else "English"
    niche_label = "horror and real dark facts" if niche == "horror_facts" else niche
    preview = " ".join(narration.split()[:180])

    prompt = f"""You are a viral short-form editor.
Write one opening line that creates an immediate curiosity gap without sounding generic.
Also provide one short English visual hint for the first shot.

Language: {lang_label}
Niche: {niche_label}
Context: {preview}

Return JSON only:
{{
  "text": "opening line",
  "visual": "visual hint in english"
}}"""

    try:
        from groq import Groq

        client = Groq(api_key=require_env("GROQ_API_KEY"))
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.6,
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as exc:
        logger.debug(f"[{channel_id}] Groq hook gagal: {exc}")

    try:
        payload = {
            "model": OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": "You are a viral copywriting assistant. Output JSON only."},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "format": "json",
        }
        resp = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=30)
        resp.raise_for_status()
        return json.loads(resp.json().get("message", {}).get("content", ""))
    except Exception as exc:
        logger.debug(f"[{channel_id}] Ollama hook gagal: {exc}")

    return None


def _pick_template_hook(niche: str, language: str) -> dict:
    templates = HOOK_TEMPLATES.get(niche, {}).get(language, [])
    return random.choice(templates) if templates else {"text": "", "visual": ""}
