"""
hook_engine.py - Auto-hook generator untuk 3 detik pertama video
Tujuan: mencegah penonton skip di awal dengan kalimat pembuka yang memancing.
Prioritas: Groq (Llama 3.3) -> Ollama (DeepSeek) -> Static Templates
"""

import os
import json
import random
import re
import requests
from engine.utils import get_logger, require_env

logger = get_logger("hook_engine")

# Konfigurasi Ollama (diambil dari environment yang sama dengan script_engine)
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.environ.get("OLLAMA_MODEL",    "deepseek-v3.1:671b-cloud")

# ─── Template hook statis dengan Visual Keywords ──────────────────────────────

HOOK_TEMPLATES = {
    "horror_facts": {
        "id": [
            {"text": "Kamu mungkin tidak akan bisa tidur setelah mendengar ini...", "visual": "creepy bedroom dark"},
            {"text": "Sebagian besar orang tidak tahu fakta ini, dan mungkin lebih baik begitu.", "visual": "ancient mystery folder"},
            {"text": "Ini bukan fiksi. Ini benar-benar terjadi.", "visual": "security camera footage ghost"},
            {"text": "Ada alasan kenapa para ilmuwan tidak mau membicarakan hal ini secara terbuka.", "visual": "dark laboratory experiment"},
        ],
        "en": [
            {"text": "You might not be able to sleep after hearing this...", "visual": "horror nightmare silhouette"},
            {"text": "Most people don't know this fact, and maybe it's better that way.", "visual": "mysterious old document"},
            {"text": "This is not fiction. This actually happened.", "visual": "vhs glitch horror"},
            {"text": "There's a reason scientists refuse to talk about this publicly.", "visual": "classified file top secret"},
        ],
    },
    "psychology": {
        "id": [
            {"text": "Otak kamu sedang melakukan ini tanpa kamu sadari.", "visual": "brain neural network"},
            {"text": "Psikolog menyebut ini sebagai salah satu rahasia terbesar perilaku manusia.", "visual": "crowd observation psychological"},
            {"text": "Kamu sudah pernah merasakan ini, tapi tidak tahu namanya.", "visual": "confused person mirror"},
        ],
        "en": [
            {"text": "Your brain is doing this right now without you realizing it.", "visual": "human brain scan animation"},
            {"text": "Psychologists call this one of the biggest secrets of human behavior.", "visual": "social experiment observation"},
            {"text": "You've felt this before, but you never knew what it was called.", "visual": "deja vu effect glitch"},
        ],
    },
}

# ─── Public API ───────────────────────────────────────────────────────────────

def inject_hook(script_data: dict, channel: dict) -> dict:
    """
    Sisipkan hook di awal narasi script_data dan tambahkan saran visual.
    """
    niche    = channel["niche"]
    language = channel["language"]
    ch_id    = channel["id"]
    
    target_text = script_data.get("script", "")
    if not target_text:
        target_text = script_data.get("intro", "")

    if not target_text:
        logger.warning(f"[{ch_id}] hook_engine: teks narasi kosong, skip inject hook")
        return script_data

    # 1. Generate hook via AI (Groq atau Ollama)
    hook_data = _generate_hook_ai(target_text, niche, language, ch_id)

    # 2. Fallback ke template statis jika AI gagal
    if not hook_data:
        hook_data = _pick_template_hook(niche, language)
        logger.info(f"[{ch_id}] Menggunakan template hook offline")

    # 3. Sisipkan hook ke field yang sesuai
    hook_text = hook_data.get("text", "")
    if "script" in script_data:
        script_data["script"] = f"{hook_text}\n\n{script_data['script']}"
    if "intro" in script_data:
        script_data["intro"] = f"{hook_text}\n\n{script_data['intro']}"

    script_data["hook"] = hook_text
    
    # Tambahkan visual keyword intro ke metadata script
    if hook_data.get("visual"):
        if "visual_keywords" not in script_data:
            script_data["visual_keywords"] = {}
        script_data["visual_keywords"]["intro"] = hook_data["visual"]

    # 4. Sisipkan pattern interrupt di tengah (khusus Shorts)
    if "script" in script_data:
        script_data["script"] = _inject_pattern_interrupt(script_data["script"], language)

    logger.info(f"[{ch_id}] ✅ Hook disuntikkan: {hook_text[:60]}...")
    return script_data


# ─── AI Hook Generator ────────────────────────────────────────────────────────

def _generate_hook_ai(narration: str, niche: str, language: str, channel_id: str) -> dict:
    """
    Generate hook dan saran visual. Prioritas: Groq -> Ollama.
    """
    lang_label = "Bahasa Indonesia" if language == "id" else "English"
    niche_label = "horror dan fakta gelap" if niche == "horror_facts" else "psikologi"
    preview = " ".join(narration.split()[:200])

    prompt = f"""Kamu adalah copywriter video viral.
Buat 1 kalimat hook pembuka video yang SANGAT memancing rasa ingin tahu.
Gunakan teknik Curiosity Gap. JANGAN mulai dengan "Tahukah kamu".
Berikan juga 2-3 kata kunci visual (English) untuk footage yang cocok.

Bahasa: {lang_label}
Niche: {niche_label}
Konteks Cerita: {preview}

Output HARUS format JSON:
{{
  "text": "kalimat hook di sini",
  "visual": "visual keywords in english"
}}"""

    # 1. Coba Groq
    try:
        from groq import Groq
        client = Groq(api_key=require_env("GROQ_API_KEY"))
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.8,
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.debug(f"[{channel_id}] Groq hook gagal: {e}")

    # 2. Coba Ollama / DeepSeek
    try:
        payload = {
            "model": OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": "You are a viral copywriting assistant. Output JSON only."},
                {"role": "user", "content": prompt}
            ],
            "stream": False,
            "format": "json"
        }
        resp = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=30)
        resp.raise_for_status()
        return json.loads(resp.json().get("message", {}).get("content", ""))
    except Exception as e:
        logger.debug(f"[{channel_id}] Ollama hook gagal: {e}")

    return None


def _pick_template_hook(niche: str, language: str) -> dict:
    templates = HOOK_TEMPLATES.get(niche, {}).get(language, [])
    return random.choice(templates) if templates else {"text": "", "visual": ""}


def _inject_pattern_interrupt(narration: str, language: str) -> str:
    # (Logika pattern interrupt tetap sama seperti sebelumnya)
    words = narration.split()
    if len(words) < 60: return narration
    insert_at = int(len(words) * 0.45)
    
    interrupts = ["But that's only half of it.", "Wait, there's something even more shocking."] if language == "en" else \
                 ["Tapi ini baru setengahnya.", "Tunggu, ada yang lebih mengejutkan."]
    
    before = " ".join(words[:insert_at])
    after = " ".join(words[insert_at:])
    return f"{before}\n\n{random.choice(interrupts)}\n\n{after}"    