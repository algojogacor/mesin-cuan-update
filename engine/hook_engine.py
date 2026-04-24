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
from engine.utils import get_logger, require_env, get_ollama_model
from engine.memory_engine import get_recent_creative_memory

logger = get_logger("hook_engine")

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
QWEN_API_BASE = os.environ.get("QWEN_API_BASE", "http://34.57.12.120:9000/v1")
QWEN_MODEL = os.environ.get("QWEN_MODEL", "qwen3-235b-a22b")
QWEN_MODEL_CANDIDATES = [
    m.strip() for m in os.environ.get(
        "QWEN_MODEL_CANDIDATES",
        "qwen3-235b-a22b,qwen3-plus,qwen2.5-72b-instruct,qwen3-30b-a3b,qwen3-turbo"
    ).split(",")
    if m.strip()
]
HOOK_MIN_SCORE = float(os.environ.get("HOOK_MIN_SCORE", "8.7"))

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

INDONESIAN_VISUAL_TOKENS = {
    "dengan", "dan", "yang", "kaset", "gereja", "suara", "darah", "dinding",
    "berdarah", "salib", "dokter", "rumah", "siluet", "berbisik", "ritual",
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

    if profile == "shorts" and niche == "horror_facts":
        script_data = _evaluate_and_upgrade_horror_hook(script_data, channel)
        script_data["hook"] = (
            script_data.get("hook_line")
            or script_data.get("hook")
            or final_hook
        )

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
            temperature=0.9,
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as exc:
        logger.debug(f"[{channel_id}] Groq hook gagal: {exc}")

    try:
        payload = {
            "model": get_ollama_model(),
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


def _evaluate_and_upgrade_horror_hook(script_data: dict, channel: dict) -> dict:
    review = _review_horror_hook(script_data, channel)
    if not review:
        return script_data

    score = _to_score(review.get("score"))
    opening_score = _to_score(review.get("hook_score_0_3"), fallback=score)
    anchor_score = _to_score(review.get("anchor_score_4_10"), fallback=score)
    anti_generic_score = _to_score(review.get("anti_generic_score"), fallback=score)
    penalties = _clean_string_list(review.get("penalties"))
    updated = review.get("updated_hook") or {}
    reviewer_status = str(review.get("status", "unknown")).strip().lower()
    rewritten = bool(updated) and (score < HOOK_MIN_SCORE or reviewer_status == "rewrite")
    normalized_status = "rewrite" if rewritten else (reviewer_status or "approved")

    if rewritten:
        if updated.get("hook_line"):
            script_data["hook_line"] = updated["hook_line"].strip()
        if updated.get("anchor_line"):
            script_data["anchor_line"] = updated["anchor_line"].strip()
        if updated.get("hook_type"):
            script_data["hook_type"] = updated["hook_type"].strip()
        if updated.get("visual"):
            visual_hint = _sanitize_visual_hint(updated["visual"])
            if visual_hint:
                script_data.setdefault("visual_keywords", {})
                script_data["visual_keywords"]["intro"] = visual_hint
                visual_beats = script_data.setdefault("visual_beats", {})
                if isinstance(visual_beats, dict):
                    opening = visual_beats.get("opening", [])
                    if not isinstance(opening, list):
                        opening = [opening] if opening else []
                    if visual_hint not in opening:
                        visual_beats["opening"] = [visual_hint, *opening][:2]

    variant_meta = _generate_horror_hook_variants(script_data, channel, review)
    selected_variant = ""
    variant_changed = False
    if variant_meta:
        script_data["hook_variants"] = variant_meta.get("variants", [])
        winner = variant_meta.get("winner_variant") or {}
        selected_variant = str(variant_meta.get("winner_label", "")).strip().lower()
        if winner.get("hook_line") and winner.get("hook_line", "").strip() != str(script_data.get("hook_line", "")).strip():
            script_data["hook_line"] = winner["hook_line"].strip()
            variant_changed = True
        if winner.get("anchor_line") and winner.get("anchor_line", "").strip() != str(script_data.get("anchor_line", "")).strip():
            script_data["anchor_line"] = winner["anchor_line"].strip()
            variant_changed = True
        if winner.get("hook_type"):
            script_data["hook_type"] = winner["hook_type"].strip()
        if winner.get("visual"):
            visual_hint = _sanitize_visual_hint(winner["visual"])
            if visual_hint:
                script_data.setdefault("visual_keywords", {})
                script_data["visual_keywords"]["intro"] = visual_hint
                visual_beats = script_data.setdefault("visual_beats", {})
                if isinstance(visual_beats, dict):
                    opening = visual_beats.get("opening", [])
                    if not isinstance(opening, list):
                        opening = [opening] if opening else []
                    if visual_hint not in opening:
                        visual_beats["opening"] = [visual_hint, *opening][:2]

    script_data["hook_meta"] = {
        "score": score,
        "hook_score_0_3": opening_score,
        "anchor_score_4_10": anchor_score,
        "anti_generic_score": anti_generic_score,
        "status": normalized_status,
        "reason": review.get("reason", ""),
        "penalties": penalties,
        "suggested_fix": str(review.get("suggested_fix", "")).strip(),
        "rewritten": rewritten or variant_changed,
        "threshold": HOOK_MIN_SCORE,
        "provider": review.get("provider", ""),
        "variant_selected": selected_variant,
        "variant_reason": (variant_meta or {}).get("winner_reason", ""),
    }
    return script_data


def _review_horror_hook(script_data: dict, channel: dict) -> dict | None:
    prompt = _build_horror_hook_review_prompt(script_data, channel)
    providers = []
    if os.getenv("QWEN_API_KEY"):
        providers.append("qwen")
    if os.getenv("GROQ_API_KEY"):
        providers.append("groq")
    providers.append("ollama")

    last_error = None
    for provider in providers:
        try:
            raw = _call_hook_provider(provider, prompt)
            data = _parse_hook_json(raw)
            data["provider"] = provider
            return data
        except Exception as exc:
            last_error = exc
            logger.debug(f"[{channel['id']}] hook review provider {provider} gagal: {exc}")

    logger.debug(f"[{channel['id']}] hook review semua provider gagal: {last_error}")
    return None


def _build_horror_hook_review_prompt(script_data: dict, channel: dict) -> str:
    memory = get_recent_creative_memory(channel)
    return f"""You are a ruthless horror shorts hook editor.
Judge only the opening performance.

Goals:
- hook_line must dominate the first 0-3 seconds
- anchor_line must lock retention for 4-10 seconds
- avoid passive exposition, generic setup, and slow historical context
- avoid repeating the same hook flavor used recently
- prefer forbidden file, hidden evidence, body anomaly, ritual panic, or conspiracy dread
- pattern interrupt energy should already be implied by the opening, not feel like setup-only exposition

Scoring rubric:
- hook_score_0_3: raw scroll-stopping power of the first line
- anchor_score_4_10: how strongly the second line pulls viewers deeper
- anti_generic_score: how fresh and non-repetitive the opening feels against recent videos
- score: overall opening strength after penalties

Penalty triggers:
- vague opener
- too much setup
- recycled hook flavor
- hook and anchor feeling like the same sentence twice
- visual cue not footage-friendly or not in English

If the hook is weak, rewrite hook_line and anchor_line directly.
Return JSON only:
{{
  "score": 0-10,
  "hook_score_0_3": 0-10,
  "anchor_score_4_10": 0-10,
  "anti_generic_score": 0-10,
  "status": "approved|rewrite",
  "reason": "short reason",
  "penalties": ["short penalty"],
  "suggested_fix": "one-line fix direction",
  "updated_hook": {{
    "hook_type": "forbidden_record",
    "hook_line": "...",
    "anchor_line": "...",
    "visual": "english opening visual cue"
  }}
}}

Recent hooks to avoid:
{json.dumps(memory.get("recent_hooks", [])[:6], ensure_ascii=False)}

Recent titles to avoid mirroring:
{json.dumps(memory.get("recent_titles", [])[:6], ensure_ascii=False)}

Script JSON:
{json.dumps(script_data, ensure_ascii=False, indent=2)}
"""


def _call_hook_provider(provider: str, prompt: str) -> str:
    if provider == "groq":
        from groq import Groq

        client = Groq(api_key=require_env("GROQ_API_KEY"))
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a horror hook editor. Output JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.25,
            max_tokens=500,
        )
        return resp.choices[0].message.content.strip()

    if provider == "qwen":
        last_error = None
        for model_name in _qwen_models_to_try():
            session = requests.Session()
            session.trust_env = False
            try:
                resp = session.post(
                    f"{QWEN_API_BASE.rstrip('/')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {require_env('QWEN_API_KEY')}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model_name,
                        "messages": [
                            {"role": "system", "content": "You are a horror hook editor. Output JSON only."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.25,
                        "max_tokens": 1200,   # 500 terlalu kecil — JSON terpotong di char 857/1426
                    },
                    timeout=45,
                )
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"]
            except Exception as exc:
                last_error = exc
            finally:
                session.close()
        raise RuntimeError(f"Hook Qwen gagal di semua model: {last_error}")

    payload = {
        "model": get_ollama_model(),
        "messages": [
            {"role": "system", "content": "You are a horror hook editor. Output JSON only."},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "format": "json",
        "options": {
            # Hook Reviewer/Scorer: deterministik, seed fixed
            "temperature":    0.25,
            "top_p":          0.90,
            "top_k":          25,
            "repeat_penalty": 1.0,
            "num_predict":    500,
            "num_ctx":        4096,
            "seed":           42,
        },
    }
    resp = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=45)
    resp.raise_for_status()
    return resp.json().get("message", {}).get("content", "")


def _parse_hook_json(raw: str) -> dict:
    cleaned = (raw or "").strip()
    if not cleaned:
        raise ValueError("hook response kosong")
    if cleaned.startswith("```"):
        cleaned = cleaned.replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except Exception:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            return json.loads(cleaned[start:end + 1])
        raise


def _generate_horror_hook_variants(script_data: dict, channel: dict, review: dict | None = None) -> dict | None:
    prompt = _build_horror_hook_variant_prompt(script_data, channel, review=review)
    providers = []
    if os.getenv("QWEN_API_KEY"):
        providers.append("qwen")
    if os.getenv("GROQ_API_KEY"):
        providers.append("groq")
    providers.append("ollama")

    last_error = None
    for provider in providers:
        try:
            raw = _call_hook_provider(provider, prompt)
            data = _parse_hook_json(raw)
            variants = data.get("variants", [])
            if isinstance(variants, list) and len(variants) >= 2:
                return {
                    "variants": variants[:2],
                    "winner_label": str(data.get("winner", "")).strip().lower(),
                    "winner_reason": str(data.get("winner_reason", "")).strip(),
                    "winner_variant": _pick_winner_variant(variants, str(data.get("winner", "")).strip().lower()),
                }
        except Exception as exc:
            last_error = exc
            logger.debug(f"[{channel['id']}] hook variant provider {provider} gagal: {exc}")

    logger.debug(f"[{channel['id']}] hook variant semua provider gagal: {last_error}")
    return None


def _build_horror_hook_variant_prompt(script_data: dict, channel: dict, review: dict | None = None) -> str:
    memory = get_recent_creative_memory(channel)
    review = review or {}
    return f"""You are a horror shorts hook director.
Create two distinct hook variants for the same topic.

Rules:
- Variant A and Variant B must feel materially different in phrasing and hook flavor.
- Both must fit the first 0-3 seconds and the follow-up 4-10 seconds.
- Prefer forbidden record, body anomaly, ritual panic, conspiracy dread, or hidden evidence energy.
- Avoid repeating recent hooks.
- Use the review findings to fix the current opening's weaknesses.
- Pick the stronger winner.

Return JSON only:
{{
  "variants": [
    {{
      "label": "a",
      "hook_type": "forbidden_record",
      "hook_line": "...",
      "anchor_line": "...",
      "visual": "english opening visual cue",
      "reason": "why this variant works"
    }},
    {{
      "label": "b",
      "hook_type": "forbidden_record",
      "hook_line": "...",
      "anchor_line": "...",
      "visual": "english opening visual cue",
      "reason": "why this variant works"
    }}
  ],
  "winner": "a",
  "winner_reason": "why this one is stronger"
}}

Recent hooks to avoid:
{json.dumps(memory.get("recent_hooks", [])[:6], ensure_ascii=False)}

Review findings to solve:
{json.dumps({
    "score": review.get("score"),
    "hook_score_0_3": review.get("hook_score_0_3"),
    "anchor_score_4_10": review.get("anchor_score_4_10"),
    "anti_generic_score": review.get("anti_generic_score"),
    "penalties": review.get("penalties"),
    "suggested_fix": review.get("suggested_fix"),
}, ensure_ascii=False, indent=2)}

Script JSON:
{json.dumps(script_data, ensure_ascii=False, indent=2)}
"""


def _pick_winner_variant(variants: list, winner_label: str) -> dict:
    for variant in variants:
        if str(variant.get("label", "")).strip().lower() == winner_label:
            return variant
    return variants[0] if variants else {}


def _sanitize_visual_hint(value: str) -> str:
    text = " ".join(str(value).split()).strip()
    if not text:
        return ""
    tokens = {
        token.lower()
        for token in re.split(r"[^a-zA-Z]+", text)
        if token.strip()
    }
    if tokens & INDONESIAN_VISUAL_TOKENS:
        return ""
    return text


def _clean_string_list(values) -> list[str]:
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return []
    cleaned: list[str] = []
    for value in values:
        text = " ".join(str(value).split()).strip()
        if text:
            cleaned.append(text)
    return cleaned[:6]


def _to_score(value, fallback: float = 0.0) -> float:
    try:
        return round(float(value), 1)
    except Exception:
        return round(float(fallback), 1)


def _qwen_models_to_try() -> list[str]:
    models: list[str] = []
    preferred = QWEN_MODEL.strip() if QWEN_MODEL else ""
    if preferred:
        models.append(preferred)
    for model in QWEN_MODEL_CANDIDATES:
        if model not in models:
            models.append(model)
    return models
