"""
memory_engine.py - Creative memory for anti-generic generation.

Fungsi utama:
  - Mengambil memori kreatif dari script/topic terbaru per channel
  - Menyusun addon prompt agar topic/script tidak terasa repetitif
  - Menyediakan konteks packaging untuk thumbnail / CTA / music choices
"""

from __future__ import annotations

import glob
import json
import os
from collections import Counter

from engine.utils import channel_data_path, get_logger

logger = get_logger("memory_engine")

MEMORY_SCRIPT_LIMIT = 12
MEMORY_TOPIC_DEBUG_LIMIT = 8
COMMON_STOPWORDS = {
    "yang", "untuk", "dengan", "tentang", "dalam", "lebih", "fakta",
    "this", "that", "with", "from", "into", "about", "more", "dark",
    "secret", "rahasia", "gelap", "facts",
}


def get_recent_creative_memory(channel: dict, limit: int = MEMORY_SCRIPT_LIMIT) -> dict:
    ch_id = channel["id"]
    scripts = _load_recent_script_payloads(ch_id, limit=limit)
    topic_debug = _load_recent_topic_debug(ch_id, limit=min(limit, MEMORY_TOPIC_DEBUG_LIMIT))

    memory = {
        "recent_titles": [],
        "recent_topics": [],
        "recent_hooks": [],
        "recent_ctas": [],
        "recent_thumbnail_texts": [],
        "recent_music_moods": [],
        "dominant_words": [],
        "recent_topic_sources": [],
    }

    word_counter: Counter[str] = Counter()
    for payload in scripts:
        title = _clean_text(payload.get("title", ""))
        topic = _clean_text(payload.get("topic", ""))
        hook = _clean_text(payload.get("hook_line") or payload.get("hook", ""))
        cta = _clean_text(payload.get("cta_line", ""))
        thumb = _clean_text(
            payload.get("creative_direction", {}).get("thumbnail_text", "")
            if isinstance(payload.get("creative_direction"), dict)
            else ""
        )
        mood = _clean_text(payload.get("music_mood", ""))

        if title:
            memory["recent_titles"].append(title)
            word_counter.update(_meaningful_tokens(title))
        if topic:
            memory["recent_topics"].append(topic)
            word_counter.update(_meaningful_tokens(topic))
        if hook:
            memory["recent_hooks"].append(hook)
            word_counter.update(_meaningful_tokens(hook))
        if cta:
            memory["recent_ctas"].append(cta)
        if thumb:
            memory["recent_thumbnail_texts"].append(thumb)
        if mood:
            memory["recent_music_moods"].append(mood)

    for item in topic_debug:
        source = _clean_text(item.get("topic_source", ""))
        if source:
            memory["recent_topic_sources"].append(source)

    memory["dominant_words"] = [word for word, _ in word_counter.most_common(10)]
    return memory


def build_script_memory_addon(channel: dict, topic_data: dict, profile: str = "shorts") -> str:
    memory = get_recent_creative_memory(channel)
    parts: list[str] = []

    recent_titles = memory["recent_titles"][:6]
    recent_hooks = memory["recent_hooks"][:5]
    recent_ctas = memory["recent_ctas"][:4]
    dominant_words = memory["dominant_words"][:6]
    recent_sources = memory["recent_topic_sources"][:5]

    if recent_titles:
        parts.append("Judul terbaru yang harus dihindari polanya:")
        parts.extend(f"- {title}" for title in recent_titles)

    if recent_hooks:
        parts.append("Hook terbaru yang jangan diulang rasa/kalimatnya:")
        parts.extend(f"- {hook}" for hook in recent_hooks)

    if recent_ctas:
        parts.append("CTA terbaru yang jangan diulang mentah-mentah:")
        parts.extend(f"- {cta}" for cta in recent_ctas)

    if dominant_words:
        parts.append(
            "Kata yang terlalu dominan belakangan ini, jadi variasikan atau hindari mengulang terlalu sering: "
            + ", ".join(dominant_words)
        )

    if recent_sources:
        parts.append(
            "Sumber topik yang baru dipakai: " + ", ".join(recent_sources)
            + ". Jika topik baru bukan viral iteration, usahakan angle terasa baru."
        )

    topic_source = topic_data.get("topic_source", "")
    if topic_source:
        parts.append(f"Topic source saat ini: {topic_source}.")

    if profile == "shorts":
        parts.append(
            "Target variasi shorts: hook harus terasa baru, CTA jangan memakai formula yang sama, "
            "dan angle visual pembuka harus berbeda dari 5 video terakhir."
        )

    return "\n[Creative Memory]\n" + "\n".join(parts) + "\n" if parts else ""


def build_packaging_memory(channel: dict) -> dict:
    memory = get_recent_creative_memory(channel)
    return {
        "recent_thumbnail_texts": memory["recent_thumbnail_texts"][:8],
        "recent_titles": memory["recent_titles"][:8],
        "recent_ctas": memory["recent_ctas"][:5],
        "dominant_words": memory["dominant_words"][:8],
    }


def _load_recent_script_payloads(channel_id: str, limit: int) -> list[dict]:
    scripts_dir = channel_data_path(channel_id, "scripts")
    paths = sorted(glob.glob(os.path.join(scripts_dir, "*.json")), reverse=True)
    payloads: list[dict] = []
    for path in paths:
        if path.endswith("_reviewed.json"):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                payloads.append(json.load(f))
        except Exception as exc:
            logger.debug(f"[{channel_id}] Gagal baca script memory {path}: {exc}")
            continue
        if len(payloads) >= limit:
            break
    return payloads


def _load_recent_topic_debug(channel_id: str, limit: int) -> list[dict]:
    topics_dir = channel_data_path(channel_id, "topics")
    pattern = os.path.join(topics_dir, "topic_selector_*.json")
    paths = sorted(glob.glob(pattern), reverse=True)
    payloads: list[dict] = []
    for path in paths[:limit]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                payloads.append(json.load(f))
        except Exception:
            continue
    return payloads


def _meaningful_tokens(text: str) -> list[str]:
    tokens = []
    for token in text.lower().replace("-", " ").split():
        token = "".join(ch for ch in token if ch.isalnum())
        if len(token) < 4 or token in COMMON_STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def _clean_text(value: str) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()
