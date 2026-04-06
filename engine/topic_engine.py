"""
topic_engine.py - Generate topik konten berdasarkan niche & language

VIRAL LOOP ENGINE v1:
  - _get_viral_iteration(): Deteksi video >1K views dari state_manager.
    Jika ada, buat Part 2 / Deep Dive dari judul tersebut, maks 1x repetisi.
  - generate(): Prioritas: ViralIteration → Trending → Seed → AI
"""

import os
import json
import random
from datetime import datetime
from engine.utils import get_logger, require_env, channel_data_path, timestamp, save_json
from engine.trending_engine import get_trending_topics
from engine.retention_engine import get_topic_hints

logger = get_logger("topic_engine")

# ─── Seed topics per niche ─────────────────────────────────────────────────────

SEED_TOPICS = {
    "horror_facts": {
        "id": [
            "Fakta mengerikan tentang tidur paralisis",
            "Apa yang terjadi pada tubuh manusia setelah mati",
            "Ritual gelap yang masih dilakukan di dunia",
            "Kasus pembunuhan berantai paling misterius",
            "Tempat paling angker yang telah dibuktikan secara ilmiah",
            "Fakta tersembunyi tentang rumah sakit jiwa kuno",
            "Penyakit langka yang membuat manusia tidak bisa tidur",
            "Suara-suara misterius yang direkam di laut dalam",
            "Eksperimen gelap yang pernah dilakukan ilmuwan",
            "Fakta menakutkan tentang gua terdalam di dunia",
        ],
        "en": [
            "Terrifying facts about sleep paralysis",
            "What happens to your body after you die",
            "Dark rituals still practiced around the world",
            "Most mysterious unsolved serial killer cases",
            "Scientifically proven haunted places on Earth",
            "Hidden facts about old mental asylums",
            "The rare disease that prevents humans from sleeping",
            "Mysterious sounds recorded in the deep ocean",
            "Dark experiments conducted by scientists in history",
            "Terrifying facts about the world's deepest caves",
        ]
    },
    "psychology": {
        "id": [
            "Trik psikologi untuk membaca pikiran orang lain",
            "Mengapa otak manusia mudah tertipu ilusi optik",
            "Efek psikologi yang terjadi saat kamu jatuh cinta",
            "Cara manipulator mengontrol korbannya tanpa sadar",
            "Mengapa kita lebih takut kehilangan daripada mendapat",
            "Fakta psikologi tentang orang yang sering bermimpi buruk",
            "Tanda-tanda seseorang sedang berbohong padamu",
            "Efek Dunning-Kruger: kenapa orang bodoh merasa pintar",
            "Psikologi warna: bagaimana warna mengontrol emosimu",
            "Mengapa otak kita menciptakan kenangan palsu",
        ],
        "en": [
            "Psychology tricks to read anyone's mind",
            "Why the human brain is easily fooled by optical illusions",
            "Psychological effects that happen when you fall in love",
            "How manipulators control their victims without them knowing",
            "Why we fear losing more than we enjoy gaining",
            "Psychology facts about people who have frequent nightmares",
            "Signs that someone is lying to you",
            "The Dunning-Kruger Effect: why incompetent people feel smart",
            "Color psychology: how colors secretly control your emotions",
            "Why our brains create false memories",
        ]
    }
}

# ─── Viral Iteration Config ────────────────────────────────────────────────────

# Threshold views untuk dianggap viral dan layak di-iterate
VIRAL_VIEW_THRESHOLD = 1000

# Suffix Part 2 / Deep Dive per bahasa
_ITERATION_SUFFIX = {
    "id": [
        "Part 2 (Yang Lebih Mengejutkan)",
        "Versi Yang Lebih Gelap",
        "Fakta Lanjutan Yang Jarang Diketahui",
        "Deep Dive: Sisi Tersembunyi",
    ],
    "en": [
        "Part 2 (Even More Shocking)",
        "The Darker Version",
        "More Hidden Facts You Didn't Know",
        "Deep Dive: The Hidden Side",
    ],
}

# Key yang disimpan di used_topics untuk track sudah diiterasi
_ITERATED_MARKER = "__iterated__"


def _get_viral_iteration(channel: dict, used_topics: list) -> str | None:
    """
    Cari video dengan views > VIRAL_VIEW_THRESHOLD di state_manager channel.
    Jika ada dan belum pernah diiterasi (maks 1x), kembalikan judul barunya.
    Mengembalikan None jika tidak ada kandidat.
    """
    ch_id    = channel["id"]
    language = channel.get("language", "id")

    # Baca state dari state_manager (file: data/{ch_id}/state.json)
    state_path = f"data/{ch_id}/state.json"
    if not os.path.exists(state_path):
        return None

    try:
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:
        logger.warning(f"[viral_iter] Gagal baca state: {e}")
        return None

    videos = state.get("videos", [])
    if not videos:
        return None

    # Filter video yang memenuhi threshold & belum pernah diiterasi
    already_iterated = {
        t.replace(_ITERATED_MARKER, "").strip()
        for t in used_topics
        if _ITERATED_MARKER in t
    }

    candidates = []
    for v in videos:
        views = v.get("views", 0)
        title = v.get("title", "").strip()
        if not title:
            continue
        if views >= VIRAL_VIEW_THRESHOLD and title not in already_iterated:
            candidates.append((views, title))

    if not candidates:
        return None

    # Pilih yang paling viral dulu
    candidates.sort(key=lambda x: x[0], reverse=True)
    _, best_title = candidates[0]

    suffix_list = _ITERATION_SUFFIX.get(language, _ITERATION_SUFFIX["id"])
    suffix      = random.choice(suffix_list)

    iteration_topic = f"{best_title} — {suffix}"
    logger.info(
        f"[{ch_id}] 🔁 Viral Iteration: '{best_title}' "
        f"({candidates[0][0]:,} views) → '{iteration_topic}'"
    )
    return iteration_topic


def generate(channel: dict, profile: str = "shorts") -> dict:
    niche    = channel["niche"]
    language = channel["language"]
    ch_id    = channel["id"]

    used_path   = f"data/{ch_id}/topics/used_topics.json"
    used_topics = []
    if os.path.exists(used_path):
        with open(used_path, "r", encoding="utf-8") as f:
            used_topics = json.load(f)

    is_viral_iteration = False

    # ── PRIORITAS 0: Viral Iteration ─────────────────────────────────────────
    viral_topic = _get_viral_iteration(channel, used_topics)
    if viral_topic:
        topic              = viral_topic
        is_viral_iteration = True
        # Tandai iterasi di used_topics agar tidak diulang
        used_topics.append(f"{_ITERATED_MARKER}{viral_topic}")
        logger.info(f"[{ch_id}] ✅ Pakai Viral Iteration topic")
    else:
        all_topics = SEED_TOPICS.get(niche, {}).get(language, [])
        fresh      = [t for t in all_topics if t not in used_topics]

        # ── PRIORITAS 1: Trending Topics ─────────────────────────────────────
        trending = get_trending_topics(niche, language, channel_id=ch_id, limit=10)
        fresh_trending = [t for t in trending if t not in used_topics]

        if fresh_trending:
            topic = random.choice(fresh_trending)
            logger.info(f"[{ch_id}] 📈 Trending topic: {topic}")
        elif fresh:
            # ── PRIORITAS 2: Seed Topics ──────────────────────────────────────
            topic = random.choice(fresh)
        else:
            # ── PRIORITAS 3: AI Brainstorm ────────────────────────────────────
            logger.info(f"[{ch_id}] Seed habis, generate via AI")
            hints = get_topic_hints(channel)
            topic = _generate_via_ai(niche, language, hints=hints)

        used_topics.append(topic)

    os.makedirs(os.path.dirname(used_path), exist_ok=True)
    with open(used_path, "w", encoding="utf-8") as f:
        json.dump(used_topics, f, ensure_ascii=False, indent=2)

    logger.info(f"[{ch_id}] Topic: {topic}")
    return {
        "topic": topic,
        "niche": niche,
        "language": language,
        "is_viral_iteration": is_viral_iteration,   # ← flag untuk script_engine
    }


def _generate_via_ai(niche: str, language: str, hints = None) -> str:
    lang_label  = "Bahasa Indonesia" if language == "id" else "English"
    niche_label = "horror dan fakta gelap" if niche == "horror_facts" else "psikologi dan perilaku manusia"

    if isinstance(hints, list):
        hints = {"best_topics": hints}
    hints = hints or {}

    hints_text = ""
    if hints:
        best   = hints.get("best_topics", [])[:3]
        avoid  = hints.get("avoid_topics", [])[:2]
        if best:
            hints_text += f"\nTopik yang proven viral di channel ini: {', '.join(best)}."
        if avoid:
            hints_text += f"\nHindari topik seperti ini (retention rendah): {', '.join(avoid)}."

    prompt = (
        f"Berikan 10 ide topik video short-form viral tentang {niche_label} "
        f"dalam {lang_label}. Format: JSON array of strings. Tidak ada teks lain."
        f"{hints_text}"
    )

    # Coba Groq dulu
    try:
        from groq import Groq
        client  = Groq(api_key=require_env("GROQ_API_KEY"))
        resp    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
        )
        topics = json.loads(resp.choices[0].message.content.strip())
        return random.choice(topics)
    except Exception as e:
        logger.warning(f"Groq topic failed: {e} → trying Gemini...")

    # Fallback Gemini
    try:
        import google.generativeai as genai
        genai.configure(api_key=require_env("GEMINI_API_KEY"))
        model  = genai.GenerativeModel("gemini-1.5-flash")
        resp   = model.generate_content(prompt)
        topics = json.loads(resp.text.strip().replace("```json", "").replace("```", ""))
        return random.choice(topics)
    except Exception as e:
        logger.warning(f"Gemini topic failed: {e} → trying Anthropic...")

    # Fallback Anthropic
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=require_env("ANTHROPIC_API_KEY"))
        msg    = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        topics = json.loads(msg.content[0].text)
        return random.choice(topics)
    except Exception as e:
        logger.error(f"Semua AI topic gagal: {e}")
        fallbacks = {
            ("horror_facts", "id"): "Fakta mengerikan yang jarang diketahui orang",
            ("horror_facts", "en"): "Terrifying facts most people don't know",
            ("psychology",   "id"): "Trik psikologi yang mengejutkan",
            ("psychology",   "en"): "Surprising psychology tricks",
        }
        return fallbacks.get((niche, language), "Fakta mengejutkan hari ini")