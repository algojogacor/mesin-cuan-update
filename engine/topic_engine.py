"""
topic_engine.py - Generate topik konten berdasarkan niche & language.

Upgrade:
  - Tetap memakai YouTube/Google Trends dari trending_engine.
  - Tidak lagi random pick mentah dari trending/seed.
  - Memakai selector yang mempertimbangkan novelty, retention hints,
    recent history, dan source quality.
  - Menyimpan debug pemilihan topik agar gampang diaudit.
  - [NEW] Manual override via topic_overrides.json (prioritas tertinggi).
  - [NEW] Series episode otomatis dari series_catalog (prioritas kedua).
"""

import json
import os
import random
import re
from datetime import datetime

import requests

from engine.retention_engine import get_topic_hints
from engine.trending_engine import get_trending_topics
from engine.utils import channel_data_path, get_logger, require_env, save_json, timestamp, get_ollama_model

logger = get_logger("topic_engine")


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
        ],
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
        ],
    },
}

VIRAL_VIEW_THRESHOLD = int(os.environ.get("VIRAL_VIEW_THRESHOLD", "1000"))
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
_ITERATED_MARKER = "__iterated__"
TOPIC_RECENT_WINDOW = 20
TOPIC_SELECTOR_MAX_CANDIDATES = 14
QWEN_API_BASE = os.environ.get("QWEN_API_BASE", "http://34.57.12.120:9000/v1")
QWEN_MODEL = os.environ.get("QWEN_MODEL", "qwen3-235b-a22b")
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")


def _get_viral_iteration(channel: dict, used_topics: list) -> str | None:
    ch_id = channel["id"]
    language = channel.get("language", "id")
    state_path = f"data/{ch_id}/state.json"
    if not os.path.exists(state_path):
        return None

    try:
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception as exc:
        logger.warning(f"[viral_iter] Gagal baca state: {exc}")
        return None

    videos = state.get("videos", [])
    if not videos:
        return None

    already_iterated = {
        t.replace(_ITERATED_MARKER, "").strip()
        for t in used_topics
        if isinstance(t, str) and _ITERATED_MARKER in t
    }

    candidates = []
    for video in videos:
        views = video.get("views", 0)
        title = video.get("title", "").strip()
        if not title:
            continue
        if views >= VIRAL_VIEW_THRESHOLD and title not in already_iterated:
            candidates.append((views, title))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0], reverse=True)
    _, best_title = candidates[0]
    suffix = random.choice(_ITERATION_SUFFIX.get(language, _ITERATION_SUFFIX["id"]))
    iteration_topic = f"{best_title} - {suffix}"
    logger.info(
        f"[{ch_id}] Viral Iteration: '{best_title}' "
        f"({candidates[0][0]:,} views) -> '{iteration_topic}'"
    )
    return iteration_topic


def generate(channel: dict, profile: str = "shorts") -> dict:
    niche = channel["niche"]
    language = channel["language"]
    ch_id = channel["id"]

    used_path = f"data/{ch_id}/topics/used_topics.json"
    used_topics = []
    if os.path.exists(used_path):
        with open(used_path, "r", encoding="utf-8") as f:
            used_topics = json.load(f)

    is_viral_iteration = False
    topic_source = "unknown"
    recent_topics = _recent_non_iterated_topics(used_topics, TOPIC_RECENT_WINDOW)
    hint_topics = get_topic_hints(channel)

    # Extra metadata dari override/series untuk diteruskan ke script_engine
    series_meta: dict = {}
    candidate_debug = []

    # ── PRIORITAS 1: Manual override dari topic_overrides.json ──────────────
    override = _pop_next_override(ch_id)
    if override:
        topic = override["topic"]
        topic_source = "manual_override"
        is_viral_iteration = bool(override.get("part_number") == 2)
        series_meta = {
            "series_name": override.get("series_name"),
            "series_item": override.get("series_item"),
            "part_number": override.get("part_number"),
            "original_title": override.get("original_title"),
        }
        used_topics.append(topic)
        logger.info(f"[{ch_id}] 🎯 Manual Override: {topic}")

    # ── PRIORITAS 2: Viral iteration dari video viral (sudah ada sebelumnya) ─
    elif (viral_topic := _get_viral_iteration(channel, used_topics)):
        topic = viral_topic
        topic_source = "viral_iteration"
        is_viral_iteration = True
        used_topics.append(f"{_ITERATED_MARKER}{viral_topic}")
        logger.info(f"[{ch_id}] Pakai Viral Iteration topic")

    # ── PRIORITAS 3: AI / Trending / Seed (flow lama) ──────────────────────
    else:
        seed_topics = SEED_TOPICS.get(niche, {}).get(language, [])
        fresh_seed = [t for t in seed_topics if t not in used_topics]
        trending = get_trending_topics(niche, language, channel_id=ch_id, limit=10)
        fresh_trending = [t for t in trending if t not in used_topics]
        ai_topics = _generate_candidates_via_ai(
            niche,
            language,
            hints={"best_topics": hint_topics},
            recent_topics=recent_topics,
        )

        candidate_pool = _build_candidate_pool(
            fresh_trending,
            fresh_seed,
            ai_topics,
            hint_topics,
            recent_topics,
            niche,
            language,
        )
        picked = _pick_best_candidate(channel, candidate_pool, recent_topics, hint_topics)

        if picked:
            topic = picked["topic"]
            topic_source = picked["source"]
            candidate_debug = candidate_pool[:5]
            logger.info(f"[{ch_id}] Topic selector picked ({topic_source}): {topic}")
        else:
            topic = _generate_via_ai(niche, language, hints={"best_topics": hint_topics})
            topic_source = "ai_fallback"

        used_topics.append(topic)

    os.makedirs(os.path.dirname(used_path), exist_ok=True)
    with open(used_path, "w", encoding="utf-8") as f:
        json.dump(used_topics, f, ensure_ascii=False, indent=2)

    _save_topic_debug(
        ch_id,
        {
            "generated_at": datetime.now().isoformat(),
            "topic": topic,
            "topic_source": topic_source,
            "profile": profile,
            "is_viral_iteration": is_viral_iteration,
            "series_meta": series_meta,
            "recent_topics": recent_topics[:10],
            "hint_topics": hint_topics[:5],
            "top_candidates": candidate_debug,
        },
    )

    logger.info(f"[{ch_id}] Topic: {topic}")
    return {
        "topic": topic,
        "niche": niche,
        "language": language,
        "is_viral_iteration": is_viral_iteration,
        "topic_source": topic_source,
        "series_meta": series_meta,
    }


def _generate_via_ai(niche: str, language: str, hints=None) -> str:
    topics = _generate_candidates_via_ai(niche, language, hints=hints)
    if topics:
        return random.choice(topics)
    return _topic_fallback(niche, language)


def _generate_candidates_via_ai(niche: str, language: str, hints=None,
                                recent_topics: list[str] | None = None) -> list[str]:
    lang_label = "Bahasa Indonesia" if language == "id" else "English"
    niche_label = "horror dan fakta gelap" if niche == "horror_facts" else "psikologi dan perilaku manusia"

    if isinstance(hints, list):
        hints = {"best_topics": hints}
    hints = hints or {}

    hints_text = ""
    best = hints.get("best_topics", [])[:3]
    avoid = hints.get("avoid_topics", [])[:2]
    if best:
        hints_text += f"\nTopik yang proven viral di channel ini: {', '.join(best)}."
    if avoid:
        hints_text += f"\nHindari topik seperti ini (retention rendah): {', '.join(avoid)}."
    if recent_topics:
        hints_text += f"\nJangan mengulang topik yang terlalu mirip dengan: {', '.join(recent_topics[:8])}."

    prompt = (
        f"Berikan 10 ide topik video short-form viral tentang {niche_label} "
        f"dalam {lang_label}. Format: JSON array of strings. Tidak ada teks lain."
        f"{hints_text}"
    )

    # ── PRIORITAS 1: Qwen ─────────────────────────────────────────────────────
    if os.getenv("QWEN_API_KEY"):
        _sess = requests.Session()
        _sess.trust_env = False
        try:
            qwen_timeout = int(os.environ.get("QWEN_TIMEOUT", "600"))
            resp = _sess.post(
                f"{QWEN_API_BASE.rstrip('/')}/chat/completions",
                headers={
                    "Authorization": f"Bearer {os.getenv('QWEN_API_KEY', '')}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": QWEN_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.85,
                    "top_p": 0.95,
                    "frequency_penalty": 0.20,
                    "max_tokens": 400,
                },
                timeout=qwen_timeout,
            )
            resp.raise_for_status()
            raw = resp.json()["choices"][0]["message"]["content"].strip()
            cleaned = raw.replace("```json", "").replace("```", "").strip()
            return _clean_topic_list(json.loads(cleaned))
        except Exception as exc:
            logger.warning(f"Qwen topic failed: {exc} -> trying Ollama...")
        finally:
            _sess.close()

    # ── PRIORITAS 2: Ollama (DeepSeek) ────────────────────────────────────────
    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json={
                "model": get_ollama_model(),
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "format": "json",
                "options": {
                    "temperature": 0.85,
                    "top_p": 0.95,
                    "top_k": 40,
                    "repeat_penalty": 1.1,
                    "num_predict": 400,
                    "num_ctx": 4096,
                },
            },
            timeout=120,
        )
        resp.raise_for_status()
        raw = resp.json().get("message", {}).get("content", "").strip()
        cleaned = raw.replace("```json", "").replace("```", "").strip()
        return _clean_topic_list(json.loads(cleaned))
    except Exception as exc:
        logger.warning(f"Ollama topic failed: {exc} -> trying Groq...")

    # ── FALLBACK: Groq ────────────────────────────────────────────────────────
    try:
        from groq import Groq

        client = Groq(api_key=require_env("GROQ_API_KEY"))
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=1.1,
            top_p=0.98,
            frequency_penalty=0.50,
            max_tokens=300,
        )
        return _clean_topic_list(json.loads(resp.choices[0].message.content.strip()))
    except Exception as exc:
        logger.warning(f"Groq topic failed: {exc} -> trying Gemini...")

    # ── FALLBACK: Gemini ──────────────────────────────────────────────────────
    try:
        import google.generativeai as genai

        genai.configure(api_key=require_env("GEMINI_API_KEY"))
        model = genai.GenerativeModel("gemini-1.5-flash")
        resp = model.generate_content(prompt)
        return _clean_topic_list(json.loads(resp.text.strip().replace("```json", "").replace("```", "")))
    except Exception as exc:
        logger.warning(f"Gemini topic failed: {exc} -> trying Anthropic...")

    # ── FALLBACK: Anthropic ───────────────────────────────────────────────────
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=require_env("ANTHROPIC_API_KEY"))
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return _clean_topic_list(json.loads(msg.content[0].text))
    except Exception as exc:
        logger.error(f"Semua AI topic gagal: {exc}")
        return []


def _topic_fallback(niche: str, language: str) -> str:
    fallbacks = {
        ("horror_facts", "id"): "Fakta mengerikan yang jarang diketahui orang",
        ("horror_facts", "en"): "Terrifying facts most people don't know",
        ("psychology", "id"): "Trik psikologi yang mengejutkan",
        ("psychology", "en"): "Surprising psychology tricks",
    }
    return fallbacks.get((niche, language), "Fakta mengejutkan hari ini")


def _clean_topic_list(topics) -> list[str]:
    if not isinstance(topics, list):
        return []
    cleaned = []
    seen = set()
    for topic in topics:
        if not isinstance(topic, str):
            continue
        text = " ".join(topic.split()).strip()
        if len(text) < 12:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(text)
    return cleaned


def _recent_non_iterated_topics(used_topics: list, limit: int) -> list[str]:
    recent = []
    for topic in reversed(used_topics):
        if not isinstance(topic, str):
            continue
        if topic.startswith(_ITERATED_MARKER):
            continue
        clean = topic.strip()
        if clean and clean not in recent:
            recent.append(clean)
        if len(recent) >= limit:
            break
    return recent


def _build_candidate_pool(fresh_trending: list[str], fresh_seed: list[str], ai_topics: list[str],
                          hint_topics: list[str], recent_topics: list[str],
                          niche: str, language: str) -> list[dict]:
    candidates = []
    seen = set()

    def add_topics(items: list[str], source: str, limit: int):
        for topic in items[:limit]:
            text = " ".join(str(topic).split()).strip()
            key = text.lower()
            if not text or key in seen:
                continue
            if _is_too_similar_to_recent(text, recent_topics):
                continue
            seen.add(key)
            candidates.append({"topic": text, "source": source})

    add_topics(fresh_trending, "trending", 8)
    add_topics(fresh_seed, "seed", 6)
    add_topics(ai_topics, "ai", 6)

    for candidate in candidates:
        candidate["score"] = _score_topic_candidate(
            candidate["topic"],
            candidate["source"],
            hint_topics,
            recent_topics,
            niche,
            language,
        )

    candidates.sort(key=lambda item: item.get("score", 0), reverse=True)
    return candidates[:TOPIC_SELECTOR_MAX_CANDIDATES]


def _score_topic_candidate(topic: str, source: str, hint_topics: list[str],
                           recent_topics: list[str], niche: str, language: str) -> int:
    text = topic.lower()
    score = {"trending": 5, "ai": 3, "seed": 1, "viral_iteration": 7}.get(source, 0)

    if 24 <= len(topic) <= 72:
        score += 3
    elif 18 <= len(topic) <= 88:
        score += 1

    power_words = (
        ["rahasia", "terlarang", "gelap", "mengerikan", "misteri", "kutukan", "rekaman", "hilang"]
        if language == "id"
        else ["secret", "forbidden", "dark", "terrifying", "mystery", "curse", "recorded", "vanished"]
    )
    score += sum(2 for word in power_words if word in text)

    if niche == "horror_facts":
        niche_tokens = ["gereja", "ritual", "arsip", "rumah sakit", "setan", "vatican", "exorcism"]
    else:
        niche_tokens = ["otak", "manipulasi", "bohong", "memori", "kepribadian", "brain", "mind", "lie"]
    score += sum(1 for token in niche_tokens if token in text)

    topic_tokens = _topic_tokens(topic)
    if any(topic_tokens & _topic_tokens(hint) for hint in hint_topics[:5]):
        score += 3

    for recent in recent_topics[:8]:
        overlap = _topic_overlap_ratio(topic, recent)
        if overlap >= 0.70:
            score -= 6
        elif overlap >= 0.50:
            score -= 3

    return score


def _topic_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-zA-Z0-9]+", text.lower())
        if len(token) >= 4 and token not in {"yang", "tentang", "lebih", "fakta", "facts", "dark"}
    }


def _topic_overlap_ratio(a: str, b: str) -> float:
    tokens_a = _topic_tokens(a)
    tokens_b = _topic_tokens(b)
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / max(min(len(tokens_a), len(tokens_b)), 1)


def _is_too_similar_to_recent(topic: str, recent_topics: list[str]) -> bool:
    return any(_topic_overlap_ratio(topic, old) >= 0.80 for old in recent_topics[:10])


def _pick_best_candidate(channel: dict, candidate_pool: list[dict], recent_topics: list[str],
                         hint_topics: list[str]) -> dict | None:
    if not candidate_pool:
        return None
    if len(candidate_pool) == 1:
        return candidate_pool[0]

    ai_index = _pick_candidate_with_ai(channel, candidate_pool, recent_topics, hint_topics)
    if ai_index is not None:
        picked = candidate_pool[ai_index]
        picked["reason"] = "ai_selector"
        return picked

    candidate_pool[0]["reason"] = "heuristic_selector"
    return candidate_pool[0]


def _pick_candidate_with_ai(channel: dict, candidate_pool: list[dict],
                            recent_topics: list[str], hint_topics: list[str]) -> int | None:
    prompt = _build_topic_selector_prompt(channel, candidate_pool, recent_topics, hint_topics)
    for provider in _topic_selector_provider_order():
        try:
            raw = _call_topic_selector(provider, prompt)
            data = _parse_selector_json(raw)
            index = int(data.get("index", -1))
            if 0 <= index < len(candidate_pool):
                logger.info(f"[{channel['id']}] Topic selector AI ({provider}) memilih index {index}")
                return index
        except Exception as exc:
            logger.debug(f"[{channel['id']}] Topic selector {provider} gagal: {exc}")
    return None


def _build_topic_selector_prompt(channel: dict, candidate_pool: list[dict],
                                 recent_topics: list[str], hint_topics: list[str]) -> str:
    return json.dumps(
        {
            "task": "pick_best_short_form_video_topic",
            "channel": {
                "id": channel.get("id", ""),
                "language": channel.get("language", "id"),
                "niche": channel.get("niche", "horror_facts"),
            },
            "rules": [
                "Choose the single best topic for retention and novelty.",
                "Prefer a topic that feels clickable, dark, and not repetitive.",
                "Avoid candidates too similar to recent uploads.",
                "Prefer angles that can create a strong 0-3 second hook.",
                "Return JSON only.",
            ],
            "recent_topics_to_avoid": recent_topics[:10],
            "top_performing_topics": hint_topics[:5],
            "candidates": [
                {
                    "index": idx,
                    "topic": item["topic"],
                    "source": item["source"],
                    "heuristic_score": item.get("score", 0),
                }
                for idx, item in enumerate(candidate_pool)
            ],
            "response_schema": {"index": 0, "reason": "short reason"},
        },
        ensure_ascii=False,
    )


def _topic_selector_provider_order() -> list[str]:
    providers = ["ollama"]
    if os.getenv("QWEN_API_KEY"):
        providers.insert(0, "qwen")
    return providers


def _call_topic_selector(provider: str, prompt: str) -> str:
    if provider == "qwen":
        session = requests.Session()
        session.trust_env = False
        try:
            resp = session.post(
                f"{QWEN_API_BASE.rstrip('/')}/chat/completions",
                headers={
                    "Authorization": f"Bearer {os.getenv('QWEN_API_KEY', '')}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": QWEN_MODEL,
                    "messages": [
                        {"role": "system", "content": "You are a viral topic strategist. Output JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    # Selector: deterministik (Qwen: no seed/top_k support)
                    "temperature":       0.20,
                    "top_p":             0.90,
                    "frequency_penalty": 0.0,
                    "max_tokens":        300,
                },
                timeout=45,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        finally:
            session.close()

    resp = requests.post(
        f"{OLLAMA_BASE_URL}/api/chat",
        json={
            "model": get_ollama_model(),
            "messages": [
                {"role": "system", "content": "You are a viral topic strategist. Output JSON only."},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "format": "json",
            "options": {
                # Selector: deterministik, konsisten
                "temperature":    0.20,
                "top_p":          0.90,
                "top_k":          20,
                "repeat_penalty": 1.0,
                "num_predict":    300,
                "num_ctx":        4096,
                "seed":           42,
            },
        },
        timeout=45,
    )
    resp.raise_for_status()
    return resp.json().get("message", {}).get("content", "")


def _parse_selector_json(raw: str) -> dict:
    cleaned = (raw or "").strip()
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


def _save_topic_debug(channel_id: str, payload: dict) -> None:
    # channel_data_path mengembalikan FOLDER, bukan file path.
    # Bangun path file secara eksplisit agar tidak terjadi PermissionError.
    debug_dir = os.path.join("data", channel_id, "topics")
    os.makedirs(debug_dir, exist_ok=True)
    debug_file = os.path.join(debug_dir, f"topic_selector_{timestamp()}.json")
    try:
        save_json(payload, debug_file)
    except Exception as exc:
        logger.debug(f"[{channel_id}] Gagal simpan topic debug: {exc}")


def _pop_next_override(ch_id: str) -> dict | None:
    """
    Ambil override pertama yang belum dipakai dari topic_overrides.json.
    Mark sebagai 'used: true' setelah diambil.
    Return None jika tidak ada override pending.
    """
    overrides_dir = f"data/{ch_id}"
    os.makedirs(overrides_dir, exist_ok=True)
    overrides_path = os.path.join(overrides_dir, "topic_overrides.json")
    
    if not os.path.exists(overrides_path):
        return None

    try:
        with open(overrides_path, "r", encoding="utf-8") as f:
            overrides = json.load(f)
        if not isinstance(overrides, list):
            return None
    except Exception as exc:
        logger.warning(f"[{ch_id}] Gagal baca topic_overrides.json: {exc}")
        return None

    # Cari entry pertama yang belum dipakai
    picked = None
    for entry in overrides:
        if not entry.get("used", False):
            picked = entry
            break

    if not picked:
        return None

    # Mark sebagai used
    picked["used"] = True
    picked["used_at"] = datetime.now().isoformat()

    try:
        with open(overrides_path, "w", encoding="utf-8") as f:
            json.dump(overrides, f, ensure_ascii=False, indent=2)
        logger.info(f"[{ch_id}] Override dikonsumsi: '{picked['topic'][:60]}'")
    except Exception as exc:
        logger.warning(f"[{ch_id}] Gagal update topic_overrides.json: {exc}")

    return picked
