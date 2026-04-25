"""
metadata_engine.py - Generate metadata YouTube
Jadwal publish_at:
  - Campaign mode : di-set oleh main.py via publish_at_override, bukan di sini
  - Legacy mode   : dihitung otomatis (prime time WIB, persistent ke file)
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone
from engine.utils import get_logger, require_env, load_settings

logger = get_logger("metadata_engine")

EXTRA_TAGS = {
    ("horror_facts", "id", "shorts"): [
        "#shorts", "#faktahoror", "#faktagelap", "#faktaunik",
        "#horror", "#misteri", "#faktamencekam", "#faktaaneh"
    ],
    ("horror_facts", "id", "long_form"): [
        "#faktahoror", "#faktagelap", "#horror", "#misteri",
        "#faktamencekam", "#faktaunik", "#horrorindonesia", "#faktaseram"
    ],
    ("horror_facts", "en", "shorts"): [
        "#shorts", "#horrorfacts", "#darkfacts", "#scaryfacts",
        "#mystery", "#creepy", "#frightening", "#horrorstories"
    ],
    ("horror_facts", "en", "long_form"): [
        "#horrorfacts", "#darkfacts", "#scaryfacts", "#mystery",
        "#creepy", "#horrorstories", "#darkhistory", "#truecrime"
    ],
    ("psychology", "id", "shorts"): [
        "#shorts", "#psikologi", "#faktapsikologi", "#mindtricks",
        "#faktaunik", "#otak", "#ilmuotak", "#perilakumanusia"
    ],
    ("psychology", "id", "long_form"): [
        "#psikologi", "#faktapsikologi", "#mindtricks", "#ilmuotak",
        "#perilakumanusia", "#psikologiindonesia", "#faktamenarik", "#selfimprovement"
    ],
    ("psychology", "en", "shorts"): [
        "#shorts", "#psychology", "#psychologyfacts", "#mindtricks",
        "#humanbehavior", "#brainscience", "#mentalhealth", "#lifehacks"
    ],
    ("psychology", "en", "long_form"): [
        "#psychology", "#psychologyfacts", "#mindtricks", "#humanbehavior",
        "#brainscience", "#mentalhealth", "#darkpsychology", "#selfimprovement"
    ],
}

CHANNEL_TITLE_HASHTAGS = {
    "ch_id_horror": ["#faktahoror", "#shorts"],
    "ch_horror_id": ["#faktahoror", "#shorts"],
    "ch_id_psych": ["#faktapsikologi", "#shorts"],
    "ch_en_horror": ["#horrorfacts", "#shorts"],
    "ch_en_psych": ["#psychologyfacts", "#shorts"],
}

# ─── Psychological Title Triggers ─────────────────────────────────────────────
# CTR psychology: curiosity gap, urgency, social proof, fear

_PSYCH_PREFIXES: dict[tuple, list[str]] = {
    ("horror_facts", "id"): [
        "TERUNGKAP!", "MENCEKAM!", "⚠️ JANGAN TONTON MALAM!",
        "FAKTA GELAP:", "MISTERI:", "BUKTI:",
    ],
    ("horror_facts", "en"): [
        "REVEALED:", "DARK TRUTH:", "⚠️ WARNING:",
        "TERRIFYING:", "HIDDEN:", "EXPOSED:",
    ],
    ("psychology", "id"): [
        "TERBUKTI!", "FAKTANYA:", "RAHASIA:",
        "MENGEJUTKAN!", "PSIKOLOGI:", "ILMUWAN MEMBUKTIKAN:",
    ],
    ("psychology", "en"): [
        "PROVEN:", "SCIENCE SAYS:", "SHOCKING:",
        "WHY YOUR BRAIN:", "PSYCHOLOGISTS SAY:", "TRUTH:",
    ],
    ("motivation", "id"): [
        "BANGKIT!", "STOP MENUNDA:", "FAKTA:",
        "JANGAN NYERAH:", "MOTIVASI:", "UBAH HIDUPMU:",
    ],
    ("motivation", "en"): [
        "CHANGE YOUR LIFE:", "STOP WASTING TIME:", "FACT:",
        "TRUTH ABOUT SUCCESS:", "MINDSET:", "WAKE UP:",
    ],
    ("drama", "id"): [
        "MENGEJUTKAN!", "VIRAL!", "KONTROVERSI:",
        "SYOK!", "SKANDAL:", "TANPA FILTER:",
    ],
    ("drama", "en"): [
        "SHOCKING!", "VIRAL!", "CONTROVERSY:",
        "UNFILTERED:", "EXPOSED!", "DRAMA:",
    ],
    ("history", "id"): [
        "TERSEMBUNYI!", "SEJARAH KELAM:", "FAKTA:",
        "BUKU SEJARAH BOHONG:", "TERUNGKAP!", "DIBALIK:",
    ],
    ("history", "en"): [
        "HIDDEN HISTORY:", "DARK PAST:", "SHOCKING FACT:",
        "THEY LIED:", "REVEALED:", "UNTOLD:",
    ],
}


def _amplify_title(title: str, niche: str, language: str) -> str:
    """Inject psychological trigger prefix jika judul belum mengandung trigger."""
    import random
    triggers = _PSYCH_PREFIXES.get((niche, language), [])
    if not triggers:
        return title
    # Jangan tambahkan prefix jika judul sudah mulai dengan karakter caps panjang / emoji
    first_word = title.split()[0] if title.split() else ""
    if len(first_word) >= 5 and first_word.isupper():
        return title  # already has trigger
    prefix = random.choice(triggers)
    new_title = f"{prefix} {title}"
    return new_title[:100]  # YouTube max title length


def _dedupe_keep_order(items: list[str]) -> list[str]:
    return list(dict.fromkeys(item for item in items if item))


def _normalize_hashtag(tag: str) -> str:
    text = str(tag or "").strip()
    if not text:
        return ""
    text = text.lstrip("#")
    text = re.sub(r"[^0-9A-Za-z_]+", "", text)
    if not text:
        return ""
    return f"#{text[:40].lower()}"


def _extract_hashtags(text: str) -> list[str]:
    return [match.lower() for match in re.findall(r"#\w+", text or "")]


def _build_hashtag_pool(script_tags: list[str], extra_tags: list[str], profile: str) -> list[str]:
    normalized = [
        _normalize_hashtag(tag)
        for tag in _dedupe_keep_order(list(script_tags or []) + list(extra_tags or []))
    ]
    hashtags = _dedupe_keep_order(normalized)
    if profile == "shorts" and "#shorts" not in hashtags:
        hashtags.insert(0, "#shorts")
    return hashtags


def _select_title_hashtags(
    channel_id: str,
    hashtags: list[str],
    profile: str,
    niche: str,
    language: str,
) -> list[str]:
    channel_specific = [
        _normalize_hashtag(tag)
        for tag in CHANNEL_TITLE_HASHTAGS.get(channel_id, [])
    ]
    channel_specific = _dedupe_keep_order(channel_specific)
    if len(channel_specific) >= 2:
        return channel_specific[:2]

    non_shorts = [tag for tag in hashtags if tag != "#shorts"]
    selected = non_shorts[:1]
    if profile == "shorts" and "#shorts" in hashtags:
        selected.append("#shorts")
    else:
        selected.extend(non_shorts[1:2])

    fallback_pairs = {
        ("horror_facts", "id"): ["#faktahoror", "#shorts"],
        ("horror_facts", "en"): ["#horrorfacts", "#shorts"],
        ("psychology", "id"): ["#faktapsikologi", "#shorts"],
        ("psychology", "en"): ["#psychologyfacts", "#shorts"],
    }
    if len(selected) < 2:
        selected.extend(fallback_pairs.get((niche, language), []))

    return _dedupe_keep_order([_normalize_hashtag(tag) for tag in selected])[:2]


def _append_hashtags_to_title(title: str, hashtags: list[str], max_len: int = 100) -> str:
    title = (title or "").strip()
    if not title or not hashtags:
        return title[:max_len]

    existing = set(_extract_hashtags(title))
    suffix_tags = [tag for tag in hashtags if tag.lower() not in existing]
    if not suffix_tags:
        return title[:max_len]

    suffix = " " + " ".join(suffix_tags)
    if len(title) + len(suffix) <= max_len:
        return f"{title}{suffix}"

    allowed_title_len = max_len - len(suffix)
    if allowed_title_len <= 0:
        return " ".join(suffix_tags)[:max_len]

    trimmed = title[:allowed_title_len].rstrip()
    if allowed_title_len >= 4 and len(trimmed) < len(title):
        trimmed = trimmed[:max(allowed_title_len - 3, 1)].rstrip() + "..."
    return f"{trimmed}{suffix}"[:max_len]


def _append_hashtags_to_description(description: str, hashtags: list[str], limit: int = 5) -> str:
    description = (description or "").strip()
    if not hashtags:
        return description

    existing = set(_extract_hashtags(description))
    suffix_tags = [tag for tag in hashtags if tag.lower() not in existing][:limit]
    if not suffix_tags:
        return description

    hashtag_line = " ".join(suffix_tags)
    if not description:
        return hashtag_line
    return f"{description}\n\n{hashtag_line}"

PRIME_TIME_WIB = {
    "id": 17,
    "en": 8,
}

# File persistent untuk simpan jadwal antar run (legacy mode)
SCHEDULE_STATE_FILE = "data/schedule_state.json"


# ─── State helpers ────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if os.path.exists(SCHEDULE_STATE_FILE):
        try:
            with open(SCHEDULE_STATE_FILE, "r") as f:
                raw = json.load(f)
            return {k: datetime.fromisoformat(v) for k, v in raw.items()}
        except Exception:
            pass
    return {}


def _save_state(state: dict):
    os.makedirs(os.path.dirname(SCHEDULE_STATE_FILE), exist_ok=True)
    with open(SCHEDULE_STATE_FILE, "w") as f:
        json.dump({k: v.isoformat() for k, v in state.items()}, f, indent=2)


# ─── Public: generate ────────────────────────────────────────────────────────

def generate(script_data: dict, channel: dict, profile: str = "shorts") -> dict:
    """
    Generate metadata untuk 1 video.

    publish_at di sini diisi dengan prime time otomatis (legacy/fallback).
    Kalau dipanggil dari campaign mode (main.py), publish_at akan di-override
    oleh main.py setelah fungsi ini return — jadi tidak perlu diurus di sini.
    """
    ch_id       = channel["id"]
    niche       = channel["niche"]
    language    = channel["language"]
    settings    = load_settings()
    upload_conf = settings.get("upload", {})

    # Hashtag pool dipakai untuk title, description, dan tag API.
    script_tags  = script_data.get("tags", [])
    extra_tags   = EXTRA_TAGS.get((niche, language, profile), [])
    hashtag_pool = _build_hashtag_pool(script_tags, extra_tags, profile)
    title_tags   = _select_title_hashtags(ch_id, hashtag_pool, profile, niche, language)
    desc_tags    = hashtag_pool[:5]

    # Judul
    title = script_data.get("title", "").strip()
    if len(title) > 100:
        title = title[:97] + "..."

    # ── Psychological CTR triggers ───────────────────────────────────────────
    title = _amplify_title(title, niche, language)
    title = _append_hashtags_to_title(title, title_tags)
    logger.info(f"[{ch_id}] Title (amplified): {title}")

    # Deskripsi
    base_desc     = script_data.get("description", "")
    cta_id        = "\n\n🔔 Subscribe & aktifkan notifikasi untuk konten baru setiap hari!"
    cta_en        = "\n\n🔔 Subscribe and turn on notifications for daily content!"
    cta           = cta_id if language == "id" else cta_en
    chapters_text = ""
    if profile == "long_form" and script_data.get("chapters"):
        chapters_text = "\n\n" + "\n".join(script_data["chapters"])
    description = f"{base_desc}{chapters_text}{cta}"
    description = _append_hashtags_to_description(description, desc_tags)

    # Tags
    all_tags    = list(dict.fromkeys(script_tags + extra_tags))
    all_tags    = [t.lstrip("#") for t in all_tags]
    final_tags  = []
    total_chars = 0
    for tag in all_tags[:30]:
        if total_chars + len(tag) + 1 <= 500:
            final_tags.append(tag)
            total_chars += len(tag) + 1
        else:
            break

    title_b    = _generate_alt_title(title, language)
    publish_at = _next_publish_time(ch_id, language, profile)

    metadata = {
        "title":               title,
        "title_b":             title_b,
        "description":         description,
        "tags":                final_tags,
        "category_id":         upload_conf.get("category_id", "27"),
        "privacy":             "private",
        "publish_at":          publish_at,
        "made_for_kids":       upload_conf.get("youtube_made_for_kids", False),
        "language":            language,
        "contains_ai_content": True,
        "profile":             profile,
        "status":              "ready",
    }

    logger.info(f"[{ch_id}] [{profile}] Metadata: {title} | publish: {publish_at}")
    return metadata


# ─── Publish time (legacy / fallback) ────────────────────────────────────────

def _next_publish_time(channel_id: str, language: str, profile: str) -> str:
    """
    Hitung slot publish berikutnya secara otomatis.
    Dipakai di legacy mode (--legacy) dan sebagai placeholder di campaign mode
    (akan di-override oleh main.py dengan slot dari campaign_engine).
    Persistent ke file — tidak reset antar run.
    """
    now_wib    = datetime.now(timezone.utc) + timedelta(hours=7)
    prime_hour = PRIME_TIME_WIB.get(language, 17)
    state      = _load_state()
    last       = state.get(channel_id)

    for day_offset in range(30):
        base_day  = now_wib + timedelta(days=day_offset)
        candidate = base_day.replace(
            hour=prime_hour, minute=0, second=0, microsecond=0
        )

        if candidate <= now_wib + timedelta(hours=1):
            continue

        if last is not None:
            last_wib   = last + timedelta(hours=7)
            diff_hours = (candidate - last_wib).total_seconds() / 3600
            if diff_hours < 20:
                continue

        publish_utc       = candidate - timedelta(hours=7)
        state[channel_id] = publish_utc
        _save_state(state)
        return publish_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Fallback
    fallback          = now_wib + timedelta(days=31)
    fallback          = fallback.replace(hour=prime_hour, minute=0, second=0, microsecond=0)
    publish_utc       = fallback - timedelta(hours=7)
    state[channel_id] = publish_utc
    _save_state(state)
    return publish_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ─── Alt title ────────────────────────────────────────────────────────────────

def _generate_alt_title(title: str, language: str) -> str:
    try:
        from groq import Groq
        client = Groq(api_key=require_env("GROQ_API_KEY"))
        prompt = (
            f"Buat 1 alternatif judul YouTube yang lebih menarik dari: '{title}'. "
            f"Maksimal 80 karakter. Jawab hanya judulnya saja."
            if language == "id" else
            f"Create 1 alternative YouTube title more compelling than: '{title}'. "
            f"Max 80 characters. Reply with title only."
        )
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
        )
        return resp.choices[0].message.content.strip().strip('"').strip("'")[:100]
    except Exception as e:
        logger.warning(f"Groq alt title failed: {e} → trying Anthropic...")

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=require_env("ANTHROPIC_API_KEY"))
        prompt = (
            f"Buat 1 alternatif judul YouTube yang lebih menarik dari: '{title}'. "
            f"Maksimal 80 karakter. Jawab hanya judulnya saja."
            if language == "id" else
            f"Create 1 alternative YouTube title more compelling than: '{title}'. "
            f"Max 80 characters. Reply with title only."
        )
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text.strip().strip('"').strip("'")[:100]
    except Exception as e:
        logger.warning(f"Alt title failed: {e}")
        return title
