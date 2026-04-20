"""
script_quality_scorer.py — Pre-render LLM quality gate.

Evaluates a generated script across 6 dimensions BEFORE it enters TTS/render.
Called with an explicit scorer_provider to enable cross-provider scoring
(anti-sycophancy: the model that generated the script does NOT score it).

Usage:
    from engine.script_quality_scorer import score_script
    result = score_script(script_data, profile="shorts", scorer_provider="ollama")
    # → {"overall": 8.2, "hook_strength": 9.0, ..., "verdict": "PASS"}

Scoring Dimensions (0-10 each):
  hook_strength       – Does the opening immediately create a curiosity gap?
  information_density – Are there real facts/insights, not filler?
  pacing_score        – Does the narrative flow naturally without dead weight?
  curiosity_gap       – Is there a mystery/question keeping viewers watching?
  cta_effectiveness   – Is the CTA natural, not too sales-y?
  anti_generic_score  – Does it feel fresh — not a generic AI template?

Provider logic (Groq is NEVER primary scorer):
  scorer_provider="qwen"   → Qwen API  → Ollama fallback → Groq last resort
  scorer_provider="ollama" → Ollama    → Qwen fallback   → Groq last resort
"""

from __future__ import annotations

import json
import os
import re

import requests

from engine.utils import get_logger

logger = get_logger("script_quality_scorer")

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

QUALITY_THRESHOLD = float(os.environ.get("SCRIPT_QUALITY_THRESHOLD", "7.8"))

# Max tokens for scorer (evaluation is lighter than generation)
SCORER_MAX_TOKENS = 512

# Dimension weights for overall calculation
DIMENSION_WEIGHTS = {
    "hook_strength":       0.25,  # First impression is crucial for Shorts
    "curiosity_gap":       0.20,  # Retention driver
    "information_density": 0.20,  # Value delivery
    "pacing_score":        0.15,  # Flow quality
    "anti_generic_score":  0.15,  # Freshness
    "cta_effectiveness":   0.05,  # Minor weight — least predictive of viral
}


# ─── Public Entry Point ───────────────────────────────────────────────────────

def score_script(
    script_data: dict,
    profile: str = "shorts",
    scorer_provider: str = "ollama",
) -> dict:
    """
    Score a script dict with an explicit provider (cross-provider scoring).

    Args:
        script_data:      Output dict from script_engine (must contain 'script' or 'intro')
        profile:          "shorts" | "long_form"
        scorer_provider:  "qwen" | "ollama" — which model evaluates this script
                          Should be the OPPOSITE of the generator.

    Returns:
        {
            "hook_strength": float,
            "information_density": float,
            "pacing_score": float,
            "curiosity_gap": float,
            "cta_effectiveness": float,
            "anti_generic_score": float,
            "overall": float,
            "verdict": "PASS" | "FAIL",
            "strongest_aspect": str,
            "weakest_aspect": str,
            "critique": str,
            "scorer_provider": str,
        }
    """
    narration = _extract_narration(script_data, profile)
    if not narration.strip():
        logger.warning("[scorer] Narasi kosong, tidak bisa di-score")
        return _fallback_score(scorer_provider, reason="empty_narration")

    prompt = _build_scoring_prompt(narration, profile, script_data)

    # Provider waterfall (order depends on scorer_provider argument)
    provider_order = _build_provider_order(scorer_provider)

    raw = None
    used_provider = None

    for provider in provider_order:
        try:
            raw = _call_provider(provider, prompt)
            if raw:
                used_provider = provider
                break
        except Exception as exc:
            logger.debug(f"[scorer] Provider {provider} gagal: {exc}")

    if not raw:
        logger.warning("[scorer] Semua provider scorer gagal — pakai fallback score")
        return _fallback_score(scorer_provider, reason="all_providers_failed")

    return _parse_score_response(raw, used_provider)


# ─── Prompt Builder ───────────────────────────────────────────────────────────

def _build_scoring_prompt(narration: str, profile: str, script_data: dict) -> str:
    """
    Build a lightweight evaluation prompt.
    We pass the narration text + title, NOT the full script JSON,
    to keep token cost minimal.
    """
    title    = str(script_data.get("title", "")).strip()
    hook_line = str(
        script_data.get("hook_line") or
        script_data.get("hook") or ""
    ).strip()

    # Only send first 400 words for shorts (cost-efficient)
    words = narration.split()
    if profile == "shorts":
        preview = " ".join(words[:400])
    else:
        # For long_form, send intro + first segment only (~300 words)
        preview = " ".join(words[:300])

    opening_info = f"Opening hook line: {hook_line}\n" if hook_line else ""

    return f"""You are an expert YouTube Shorts content critic.
Evaluate the following short-form video script excerpt based on 6 dimensions.
Score each dimension from 0 to 10 with one decimal place.

Script title: {title}
{opening_info}
Script narration excerpt:
---
{preview}
---

Scoring rubric:
- hook_strength (0-10): Does the opening immediately create a curiosity gap without setup? 10 = instant scroll-stop, 0 = generic opener
- information_density (0-10): Are there real facts, specific details, or genuine insights? 10 = packed with substance, 0 = all filler
- pacing_score (0-10): Does the narrative flow naturally without dead weight or repetition? 10 = tight and propulsive, 0 = slow and padded
- curiosity_gap (0-10): Does the script create unresolved questions that force the viewer to keep watching? 10 = powerful mystery engine, 0 = no tension
- cta_effectiveness (0-10): Does the closing call-to-action feel organic and compelling? 10 = natural and powerful, 0 = forced or absent
- anti_generic_score (0-10): Does this feel like uniquely crafted content vs generic AI output? 10 = highly original voice, 0 = could have been written by any AI

Return ONLY valid JSON, no markdown, no extra text:
{{
  "hook_strength": 0.0,
  "information_density": 0.0,
  "pacing_score": 0.0,
  "curiosity_gap": 0.0,
  "cta_effectiveness": 0.0,
  "anti_generic_score": 0.0,
  "strongest_aspect": "name of best dimension",
  "weakest_aspect": "name of worst dimension",
  "critique": "one sentence max — the single most important improvement"
}}"""


# ─── Provider Calls ───────────────────────────────────────────────────────────

def _build_provider_order(scorer_provider: str) -> list[str]:
    """
    Build scorer waterfall order based on requested provider.
    Groq is always last resort — never primary scorer.
    """
    if scorer_provider == "qwen" and os.getenv("QWEN_API_KEY"):
        return ["qwen", "ollama", "groq"]
    elif scorer_provider == "ollama":
        if os.getenv("QWEN_API_KEY"):
            return ["ollama", "qwen", "groq"]
        return ["ollama", "groq"]
    # Fallback: whatever is available
    order = ["ollama"]
    if os.getenv("QWEN_API_KEY"):
        order.append("qwen")
    order.append("groq")
    return order


def _call_provider(provider: str, prompt: str) -> str:
    if provider == "qwen":
        return _call_qwen(prompt)
    elif provider == "ollama":
        return _call_ollama(prompt)
    elif provider == "groq":
        return _call_groq(prompt)
    raise ValueError(f"Unknown provider: {provider}")


def _call_qwen(prompt: str) -> str:
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
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a strict content quality evaluator. Output JSON only.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": SCORER_MAX_TOKENS,
                },
                timeout=60,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()
            if content:
                logger.debug(f"[scorer] Qwen ({model_name}) responded")
                return content
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (400, 404):
                continue  # Try next model
            raise
        except Exception:
            raise
        finally:
            session.close()

    raise RuntimeError("Semua Qwen model gagal untuk scoring")


def _call_ollama(prompt: str) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You are a strict content quality evaluator. Output JSON only.",
            },
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.1,
            "num_predict": SCORER_MAX_TOKENS,
            "num_ctx": 4096,
        },
    }
    resp = requests.post(
        f"{OLLAMA_BASE_URL}/api/chat",
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json().get("message", {}).get("content", "")


def _call_groq(prompt: str) -> str:
    """Groq — absolute last resort only."""
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        raise RuntimeError("GROQ_API_KEY tidak tersedia")
    try:
        from groq import Groq
        client = Groq(api_key=groq_key)
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": "You are a strict content quality evaluator. Output JSON only.",
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=SCORER_MAX_TOKENS,
        )
        return resp.choices[0].message.content.strip()
    except ImportError:
        raise RuntimeError("groq package tidak terinstall")


# ─── Response Parser ──────────────────────────────────────────────────────────

def _parse_score_response(raw: str, scorer_provider: str) -> dict:
    """Parse and validate scorer JSON response."""
    cleaned = raw.strip()
    # Strip markdown fencing if present
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    # Find JSON object
    if not cleaned.startswith("{"):
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if match:
            cleaned = match.group()
        else:
            logger.warning(f"[scorer] Tidak ada JSON di response: {raw[:200]}")
            return _fallback_score(scorer_provider, reason="parse_failed")

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning(f"[scorer] JSON parse gagal: {cleaned[:200]}")
        return _fallback_score(scorer_provider, reason="json_decode_failed")

    # Extract and validate dimensions
    dimensions = {}
    for dim in DIMENSION_WEIGHTS:
        val = data.get(dim)
        try:
            dimensions[dim] = round(max(0.0, min(10.0, float(val))), 1)
        except (TypeError, ValueError):
            dimensions[dim] = 5.0  # Neutral fallback if model skips a dimension

    # Weighted overall
    overall = round(
        sum(dimensions[dim] * weight for dim, weight in DIMENSION_WEIGHTS.items()),
        2,
    )

    verdict = "PASS" if overall >= QUALITY_THRESHOLD else "FAIL"

    result = {
        **dimensions,
        "overall": overall,
        "verdict": verdict,
        "threshold": QUALITY_THRESHOLD,
        "strongest_aspect": str(data.get("strongest_aspect", "")).strip(),
        "weakest_aspect":   str(data.get("weakest_aspect", "")).strip(),
        "critique":         str(data.get("critique", "")).strip(),
        "scorer_provider":  scorer_provider,
    }

    logger.info(
        f"[scorer] Score by {scorer_provider}: overall={overall:.1f} | "
        f"hook={dimensions['hook_strength']} | "
        f"curiosity={dimensions['curiosity_gap']} | "
        f"verdict={verdict}"
    )
    return result


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _extract_narration(script_data: dict, profile: str) -> str:
    """Extract the actual narration text from script_data regardless of schema."""
    if profile == "long_form":
        parts = [script_data.get("intro", "")]
        for seg in script_data.get("segments", []):
            if isinstance(seg, dict):
                parts.append(seg.get("narasi", ""))
        parts.append(script_data.get("outro", ""))
        return "\n\n".join(p.strip() for p in parts if p and p.strip())

    # Shorts: try assembled script first, then individual beats
    script = script_data.get("script", "")
    if script and len(script.split()) >= 30:
        return script

    # Assemble from beats
    parts = []
    for key in ("hook_line", "anchor_line"):
        val = script_data.get(key, "")
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())

    body = script_data.get("body_beats", [])
    if isinstance(body, list):
        for beat in body:
            text = beat.get("text") or beat.get("narasi") or beat if isinstance(beat, str) else ""
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())

    for key in ("final_reveal", "cta_line"):
        val = script_data.get(key, "")
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())

    return " ".join(parts)


def _fallback_score(scorer_provider: str, reason: str = "unknown") -> dict:
    """
    Return a neutral mid-range score when scoring fails.
    Neutral score (6.0) is intentionally below threshold so the system
    falls back to the sequential waterfall rather than auto-passing.
    """
    neutral = 6.0
    logger.warning(f"[scorer] Menggunakan fallback score (6.0) — reason: {reason}")
    return {
        "hook_strength":       neutral,
        "information_density": neutral,
        "pacing_score":        neutral,
        "curiosity_gap":       neutral,
        "cta_effectiveness":   neutral,
        "anti_generic_score":  neutral,
        "overall":             neutral,
        "verdict":             "FAIL",
        "threshold":           QUALITY_THRESHOLD,
        "strongest_aspect":    "",
        "weakest_aspect":      "",
        "critique":            f"Score tidak tersedia ({reason})",
        "scorer_provider":     scorer_provider,
    }


def _qwen_models_to_try() -> list[str]:
    models: list[str] = []
    preferred = QWEN_MODEL.strip() if QWEN_MODEL else ""
    if preferred:
        models.append(preferred)
    for model in QWEN_MODEL_CANDIDATES:
        if model not in models:
            models.append(model)
    return models
