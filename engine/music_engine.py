"""
music_engine.py - Download background music dari Freesound API
Fallback chain: Local folder → Freesound → Silent track

SETUP LOCAL MUSIC (recommended):
  Taruh file .mp3 di folder assets/music/<mood>/
  Contoh:
    assets/music/dark ambient/track1.mp3
    assets/music/horror tension/track2.mp3
  Kalau ada file lokal → langsung pakai, tidak perlu internet.
  Mood yang dipakai: dark ambient, horror tension, suspense,
                     calm introspective, mysterious, soft ambient
"""

import os
import json
import time
import random
import re
import shutil
import requests
from engine.utils import get_logger, channel_data_path

logger = get_logger("music_engine")

# Folder musik lokal (taruh MP3 di sini, dibagi per mood)
LOCAL_MUSIC_ROOT = "assets/music"
SOURCES_MANIFEST_PATH = os.path.join(LOCAL_MUSIC_ROOT, "SOURCES.json")


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


AUTO_MUSIC_ENRICH_LOCAL = _env_flag("AUTO_MUSIC_ENRICH_LOCAL", True)
AUTO_MUSIC_PROMOTE_FETCHED = _env_flag("AUTO_MUSIC_PROMOTE_FETCHED", True)
LOCAL_MUSIC_MIN_TRACKS = int(os.environ.get("LOCAL_MUSIC_MIN_TRACKS", "10"))
MUSIC_AI_SELECTION = _env_flag("MUSIC_AI_SELECTION", True)
MUSIC_SELECTION_MAX_CANDIDATES = int(os.environ.get("MUSIC_SELECTION_MAX_CANDIDATES", "10"))
QWEN_API_BASE = os.environ.get("QWEN_API_BASE", "http://34.57.12.120:9000/v1")
QWEN_MODEL = os.environ.get("QWEN_MODEL", "qwen3-235b-a22b")
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "deepseek-v3.1:671b-cloud")

MIXKIT_MOOD_PAGES = {
    "dark ambient": "https://mixkit.co/free-stock-music/ambient/",
    "dark intro": "https://mixkit.co/free-stock-music/mood/eerie/",
    "horror tension": "https://mixkit.co/free-stock-music/mood/tension/",
    "horror climax": "https://mixkit.co/free-stock-music/mood/suspenseful/",
    "dark mystery": "https://mixkit.co/free-stock-music/mood/cautious/",
}

# Mapping mood → query Freesound (dari spesifik ke umum sebagai fallback)
MOOD_QUERIES = {
    "dark ambient":       ["dark ambient background",      "dark cinematic ambient",    "dark atmosphere"],
    "dark intro":         ["dark cinematic intro",         "ominous intro ambient",     "dark suspense intro"],
    "horror tension":     ["horror tension suspense",      "horror background music",   "suspense thriller"],
    "suspense":           ["suspense thriller background", "tension music background",  "dramatic suspense"],
    "dark mystery":       ["dark mystery cinematic",       "mystery ambient dark",      "dark background"],
    "horror climax":      ["horror intense dark",          "horror climax music",       "dark intense"],
    "calm introspective": ["calm ambient meditation",      "soft ambient calm",         "peaceful background"],
    "thoughtful ambient": ["thoughtful ambient soft",      "soft background ambient",   "calm ambient"],
    "mysterious":         ["mysterious ambient",           "mystery background music",  "ambient mysterious"],
    "dark psychological": ["dark psychological ambient",   "psychological thriller",    "dark ambient"],
    "intense focus":      ["intense focus concentration",  "focus background music",    "intense ambient"],
    "soft ambient":       ["soft ambient calm",            "gentle background music",   "soft background"],
    "uplifting":          ["uplifting cinematic background","motivational ambient",      "hopeful background"],
}

# Cache in-memory: mood → path (supaya 1 run tidak download berkali-kali)
_music_cache: dict[str, str] = {}

ARC_DEFAULTS = {
    "shorts": [
        {"label": "opening", "start_ratio": 0.0, "end_ratio": 0.30},
        {"label": "middle", "start_ratio": 0.30, "end_ratio": 0.78},
        {"label": "ending", "start_ratio": 0.78, "end_ratio": 1.0},
    ],
    "long_form": [
        {"label": "opening", "start_ratio": 0.0, "end_ratio": 0.22},
        {"label": "middle", "start_ratio": 0.22, "end_ratio": 0.82},
        {"label": "ending", "start_ratio": 0.82, "end_ratio": 1.0},
    ],
}


def fetch(mood: str, duration_sec: int, channel_id: str,
          script_data: dict | None = None, profile: str = "shorts") -> str:
    """
    Ambil 1 track musik sesuai mood dan durasi minimal.
    Urutan: in-memory cache → local folder → Freesound → silent track
    Return: path ke file .mp3/.mp3
    """
    # 1. In-memory cache (1 session = 1 download per mood)
    if mood in _music_cache and os.path.exists(_music_cache[mood]):
        logger.info(f"Music cache hit: {mood}")
        return _music_cache[mood]

    # 2. Local music folder (prioritas utama — tidak butuh internet)
    _ensure_local_music_pool(mood)
    local_path = _get_local_music(mood, script_data=script_data, profile=profile)
    if local_path:
        logger.info(f"Music dari lokal: {local_path}")
        _music_cache[mood] = local_path
        return local_path

    # 3. Freesound API
    out_dir  = channel_data_path(channel_id, "music")
    safe_mood = mood.replace(" ", "_").replace("/", "_")
    out_path  = f"{out_dir}/{safe_mood}.mp3"  # ← FIX: tidak pakai timestamp() biar bisa cache

    # Kalau sudah ada di disk dari run sebelumnya, pakai langsung
    if os.path.exists(out_path) and os.path.getsize(out_path) > 10_000:
        logger.info(f"Music dari disk cache: {out_path}")
        _promote_track_to_local_library(mood, out_path, source_url="channel_cache", source_page="channel_cache")
        _music_cache[mood] = out_path
        return out_path

    freesound_path = _fetch_from_freesound(mood, duration_sec, out_path)
    if freesound_path:
        _promote_track_to_local_library(
            mood,
            freesound_path,
            source_url=f"freesound:{mood}",
            source_page="https://freesound.org/"
        )
        _music_cache[mood] = freesound_path
        return freesound_path

    # 4. Silent track sebagai last resort
    logger.warning(f"Semua sumber musik gagal untuk '{mood}' — pakai silent track")
    silent_path = _create_silent_track(out_path, duration_sec)
    return silent_path


def build_music_plan(base_mood: str, duration_sec: float, channel_id: str,
                     script_data: dict | None = None, profile: str = "shorts") -> list[dict]:
    """
    Bangun rencana musik multi-segmen agar video tidak terasa memakai satu loop datar.
    Setiap segmen punya mood, judul track, dan rentang waktu sendiri.
    """
    script_data = script_data or {}
    segments = _resolve_music_segments(base_mood, script_data, profile, duration_sec)
    used_paths: set[str] = set()
    plan: list[dict] = []

    for segment in segments:
        mood = segment["mood"]
        start = max(0.0, float(segment["start"]))
        end = min(duration_sec, float(segment["end"]))
        if end - start < 1.2:
            continue

        chosen = _select_track_entry(
            mood,
            script_data=script_data,
            profile=profile,
            exclude_paths=used_paths,
        )
        if not chosen:
            fallback_path = fetch(mood, max(int(end - start) + 2, 4), channel_id, script_data, profile)
            chosen = {
                "path": fallback_path,
                "title": _fallback_track_title(fallback_path, mood),
                "tags": [mood],
                "mood": mood,
            }

        path = chosen.get("path")
        if not path:
            continue
        used_paths.add(os.path.normpath(path))
        plan.append({
            "label": segment["label"],
            "mood": mood,
            "start": start,
            "end": end,
            "path": path,
            "title": chosen.get("title") or _fallback_track_title(path, mood),
            "tags": chosen.get("tags", []),
        })

    if not plan:
        single_path = fetch(base_mood, int(duration_sec) + 3, channel_id, script_data, profile)
        return [{
            "label": "full",
            "mood": base_mood,
            "start": 0.0,
            "end": duration_sec,
            "path": single_path,
            "title": _fallback_track_title(single_path, base_mood),
            "tags": [base_mood],
        }]

    return plan


# ─── Local music ──────────────────────────────────────────────────────────────

def _get_local_music(mood: str, script_data: dict | None = None, profile: str = "shorts") -> str | None:
    """
    Cari file musik di assets/music/<mood>/ atau assets/music/ (fallback).
    Pilih random dari file yang ada.
    """
    # Coba folder spesifik mood dulu
    candidates = _get_local_music_candidates(mood)
    if candidates:
        chosen = _choose_best_local_track(mood, candidates, script_data or {}, profile=profile)
        return chosen.get("path") if chosen else random.choice(candidates).get("path")

    # Fallback: cari semua file di root assets/music/
    if os.path.isdir(LOCAL_MUSIC_ROOT):
        all_files = []
        for root, _, files in os.walk(LOCAL_MUSIC_ROOT):
            for f in files:
                if f.lower().endswith((".mp3", ".wav", ".ogg")):
                    all_files.append(os.path.join(root, f))
        if all_files:
            logger.info(f"Mood '{mood}' tidak ada di lokal, pakai random dari {LOCAL_MUSIC_ROOT}")
            return random.choice(all_files)

    return None


def _list_local_music_files(mood: str) -> list[str]:
    mood_dir = os.path.join(LOCAL_MUSIC_ROOT, mood)
    if not os.path.isdir(mood_dir):
        return []
    return [
        os.path.join(mood_dir, f)
        for f in os.listdir(mood_dir)
        if f.lower().endswith((".mp3", ".wav", ".ogg"))
    ]


def _get_local_music_candidates(mood: str, exclude_paths: set[str] | None = None) -> list[dict]:
    files = _list_local_music_files(mood)
    if not files:
        return []

    manifest = _load_sources_manifest()
    manifest_entries = manifest.get(mood, [])
    lookup: dict[str, dict] = {}
    for entry in manifest_entries:
        key = os.path.normpath(entry.get("file", "")).replace("\\", "/")
        if key:
            lookup[key] = entry

    candidates: list[dict] = []
    for path in files:
        if exclude_paths and os.path.normpath(path) in exclude_paths:
            continue
        norm = os.path.normpath(path).replace("\\", "/")
        entry = lookup.get(norm, {})
        title = _normalize_track_title(entry.get("title"), mood, path)
        tags = _merge_track_tags(entry.get("tags", []), title)
        candidates.append({
            "path": path,
            "title": title,
            "tags": tags,
            "artist": entry.get("artist", ""),
            "mood": mood,
            "source_page": entry.get("source_page", ""),
            "source_url": entry.get("source_url", ""),
            "duration": entry.get("duration", 0),
        })
    return candidates


def _select_track_entry(mood: str, script_data: dict, profile: str,
                        exclude_paths: set[str] | None = None) -> dict | None:
    _ensure_local_music_pool(mood)
    candidates = _get_local_music_candidates(mood, exclude_paths=exclude_paths)
    if not candidates and exclude_paths:
        candidates = _get_local_music_candidates(mood)
    if not candidates:
        return None
    return _choose_best_local_track(mood, candidates, script_data, profile)


def _resolve_music_segments(base_mood: str, script_data: dict, profile: str, duration_sec: float) -> list[dict]:
    arc_beats = script_data.get("music_arc", [])
    if not isinstance(arc_beats, list):
        arc_beats = []

    template = ARC_DEFAULTS.get(profile, ARC_DEFAULTS["shorts"])
    segments: list[dict] = []
    for idx, slot in enumerate(template):
        beat_text = ""
        if idx < len(arc_beats) and isinstance(arc_beats[idx], str):
            beat_text = arc_beats[idx].strip()
        mood = _infer_arc_mood(slot["label"], beat_text, base_mood, script_data)
        segments.append({
            "label": slot["label"],
            "mood": mood,
            "start": duration_sec * slot["start_ratio"],
            "end": duration_sec * slot["end_ratio"],
            "beat": beat_text,
        })
    return segments


def _infer_arc_mood(label: str, beat_text: str, base_mood: str, script_data: dict) -> str:
    haystack = " ".join([
        label,
        beat_text or "",
        script_data.get("music_direction", "") or "",
        " ".join(script_data.get("music_keywords", []) if isinstance(script_data.get("music_keywords"), list) else []),
        base_mood,
    ]).lower()

    if label == "opening":
        if any(token in haystack for token in ("cold", "eerie", "whisper", "ritual", "intro", "archive", "forbidden")):
            return "dark intro"
        if "mystery" in haystack:
            return "dark mystery"
        return base_mood if base_mood in {"dark intro", "dark mystery"} else "dark intro"

    if label == "middle":
        if any(token in haystack for token in ("tension", "rising", "chase", "pulse", "panic", "escalation")):
            return "horror tension"
        if "mystery" in haystack or "unseen" in haystack:
            return "dark mystery"
        return base_mood

    if any(token in haystack for token in ("climax", "attack", "final", "reveal", "explosive", "impact")):
        return "horror climax"
    if any(token in haystack for token in ("unresolved", "linger", "curse", "aftertaste", "dark ending")):
        return "dark mystery"
    return "horror climax" if base_mood != "dark mystery" else "dark mystery"


def _ensure_local_music_pool(mood: str) -> None:
    """
    Jika stok lokal untuk mood ini tipis, coba isi otomatis dari manifest SOURCES.json.
    Ini membuat local library bertumbuh mengikuti kebutuhan run harian.
    """
    if not AUTO_MUSIC_ENRICH_LOCAL:
        return

    _ensure_manifest_capacity(mood, LOCAL_MUSIC_MIN_TRACKS)

    existing = _list_local_music_files(mood)
    if len(existing) >= LOCAL_MUSIC_MIN_TRACKS:
        return

    needed = max(LOCAL_MUSIC_MIN_TRACKS - len(existing), 1)
    hydrated = _hydrate_local_music_from_manifest(mood, needed)
    if hydrated:
        logger.info(f"Auto-hydrate music '{mood}': +{hydrated} track lokal")

    existing = _list_local_music_files(mood)
    if len(existing) < LOCAL_MUSIC_MIN_TRACKS:
        seeded = _seed_from_existing_library(mood, LOCAL_MUSIC_MIN_TRACKS - len(existing))
        if seeded:
            logger.info(f"Auto-seed music '{mood}': +{seeded} track dari local library")


def _ensure_manifest_capacity(mood: str, target_count: int) -> None:
    manifest = _load_sources_manifest()
    existing_entries = manifest.get(mood, [])
    if isinstance(existing_entries, list) and len(existing_entries) >= target_count:
        return

    page_url = MIXKIT_MOOD_PAGES.get(mood)
    if not page_url:
        return

    scraped = _scrape_mixkit_tracks(page_url)
    if not scraped:
        return

    updated = False
    entries = manifest.setdefault(mood, [])
    known_urls = {entry.get("source_url") for entry in entries}
    next_index = len(entries) + 1

    for track in scraped:
        if len(entries) >= target_count:
            break
        if track["source_url"] in known_urls:
            continue
        ext = os.path.splitext(track["source_url"])[1] or ".mp3"
        file_path = os.path.join(LOCAL_MUSIC_ROOT, mood, f"track_{next_index}{ext}").replace("\\", "/")
        next_index += 1
        entries.append({
            "file": file_path,
            "source_url": track["source_url"],
            "source_page": page_url,
            "bytes": 0,
            "title": track["title"],
            "tags": track["tags"],
            "artist": track.get("artist", ""),
            "duration": track.get("duration", 0),
        })
        known_urls.add(track["source_url"])
        updated = True

    if updated:
        _write_sources_manifest(manifest)
        logger.info(f"Manifest musik '{mood}' diperluas jadi {len(entries)} entry")


def _scrape_mixkit_tracks(page_url: str) -> list[dict]:
    try:
        session = _download_session()
        resp = session.get(page_url, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except Exception as exc:
        logger.warning(f"Scrape Mixkit gagal '{page_url}': {exc}")
        return []
    finally:
        try:
            session.close()
        except Exception:
            pass

    blocks = re.findall(
        r'"@type":"MusicRecording".*?"name":"(.*?)".*?"genre":"(.*?)".*?"byArtist":"(.*?)".*?"duration":"(.*?)".*?"url":"(https://assets\.mixkit\.co/music/\d+/\d+\.mp3)"',
        html,
        flags=re.DOTALL,
    )
    tracks: list[dict] = []
    for title, genre, artist, duration_iso, source_url in blocks:
        tags = {genre.lower().replace(" music", "").strip(), artist.lower().strip()}
        for token in re.split(r"[^a-zA-Z0-9]+", title.lower()):
            if len(token) >= 4:
                tags.add(token)
        tracks.append({
            "title": title.strip(),
            "artist": artist.strip(),
            "duration": _parse_iso_duration(duration_iso),
            "source_url": source_url.strip(),
            "tags": sorted(t for t in tags if t),
        })
    return tracks


def _choose_best_local_track(mood: str, candidates: list[dict], script_data: dict, profile: str) -> dict | None:
    if not candidates:
        return None

    limited = candidates[:MUSIC_SELECTION_MAX_CANDIDATES]
    if MUSIC_AI_SELECTION:
        ai_choice = _choose_track_with_ai(mood, limited, script_data, profile)
        if ai_choice:
            logger.info(f"Music AI choice '{mood}': {ai_choice.get('title')} -> {ai_choice.get('path')}")
            return ai_choice

    scored = sorted(
        limited,
        key=lambda c: _heuristic_track_score(c, script_data, mood),
        reverse=True,
    )
    top_bucket = scored[: min(3, len(scored))]
    return random.choice(top_bucket) if top_bucket else random.choice(limited)


def _heuristic_track_score(candidate: dict, script_data: dict, mood: str) -> int:
    haystack_parts = [
        script_data.get("title", ""),
        script_data.get("hook_line", ""),
        script_data.get("anchor_line", ""),
        script_data.get("final_reveal", ""),
        script_data.get("music_direction", ""),
        " ".join(script_data.get("music_keywords", []) if isinstance(script_data.get("music_keywords"), list) else []),
        mood,
    ]
    haystack = " ".join(haystack_parts).lower()

    score = 0
    title = candidate.get("title", "").lower()
    for token in (candidate.get("tags") or []):
        token_str = str(token).lower()
        if token_str and token_str in haystack:
            score += 3
    for token in title.split():
        if len(token) >= 4 and token in haystack:
            score += 2
    if mood in haystack:
        score += 1
    return score


def _choose_track_with_ai(mood: str, candidates: list[dict], script_data: dict, profile: str) -> dict | None:
    if len(candidates) <= 1:
        return candidates[0] if candidates else None

    prompt = _build_music_choice_prompt(mood, candidates, script_data, profile)
    for provider in _music_ai_provider_order():
        try:
            raw = _call_music_ai_provider(provider, prompt)
            data = _parse_music_ai_response(raw)
            index = int(data.get("index", -1))
            if 0 <= index < len(candidates):
                return candidates[index]
        except Exception as exc:
            logger.warning(f"Music AI provider {provider} gagal: {exc}")
    return None


def _build_music_choice_prompt(mood: str, candidates: list[dict], script_data: dict, profile: str) -> str:
    compact_candidates = []
    for idx, candidate in enumerate(candidates):
        compact_candidates.append({
            "index": idx,
            "title": candidate.get("title", ""),
            "artist": candidate.get("artist", ""),
            "duration": candidate.get("duration", 0),
            "tags": candidate.get("tags", []),
            "source_page": candidate.get("source_page", ""),
        })

    return json.dumps({
        "task": "choose_best_music_track_for_video",
        "rules": [
            "Pick the single best track for the video mood and pacing.",
            "Prefer tracks whose title/tags fit the hook, escalation, and ending tone.",
            "Return JSON only with index and short reason.",
            "Do not invent indexes outside the list.",
        ],
        "video_profile": profile,
        "music_mood": mood,
        "video_context": {
            "title": script_data.get("title", ""),
            "hook_line": script_data.get("hook_line", ""),
            "anchor_line": script_data.get("anchor_line", ""),
            "final_reveal": script_data.get("final_reveal", ""),
            "cta_line": script_data.get("cta_line", ""),
            "music_direction": script_data.get("music_direction", ""),
            "music_keywords": script_data.get("music_keywords", []),
            "music_arc": script_data.get("music_arc", []),
        },
        "candidates": compact_candidates,
        "response_schema": {"index": 0, "reason": "short reason"}
    }, ensure_ascii=False)


def _music_ai_provider_order() -> list[str]:
    providers = ["ollama"]
    if os.getenv("QWEN_API_KEY"):
        providers.insert(0, "qwen")
    return providers


def _call_music_ai_provider(provider: str, prompt: str) -> str:
    if provider == "qwen":
        api_key = os.getenv("QWEN_API_KEY", "")
        if not api_key:
            raise RuntimeError("QWEN_API_KEY tidak tersedia")
        qwen_models = [QWEN_MODEL] + [
            m for m in os.environ.get(
                "QWEN_MODEL_CANDIDATES",
                "qwen3-235b-a22b,qwen3-30b-a3b,qwen3-turbo"
            ).split(",")
            if m.strip() and m.strip() != QWEN_MODEL
        ]
        last_err = None
        for model_name in qwen_models:
            session = _download_session()
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
                            {"role": "system", "content": "You are a music supervisor for short-form video. Output JSON only."},
                            {"role": "user", "content": prompt},
                        ],
                        # Selector: deterministik (Qwen: no seed/top_k)
                        "temperature":       0.20,
                        "top_p":             0.90,
                        "frequency_penalty": 0.0,
                        "max_tokens":        200,
                    },
                    timeout=45,
                )
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"]
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                last_err = exc
                if status in (400, 404):
                    continue  # Coba model berikutnya
                raise
            except Exception as exc:
                last_err = exc
                raise
            finally:
                session.close()
        raise RuntimeError(f"Music Qwen gagal semua model: {last_err}")

    resp = requests.post(
        f"{OLLAMA_BASE_URL}/api/chat",
        json={
            "model": OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": "You are a music supervisor for short-form video. Output JSON only."},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "format": "json",
            "options": {
                # Selector: deterministik
                "temperature":    0.20,
                "top_p":          0.90,
                "top_k":          20,
                "repeat_penalty": 1.0,
                "num_predict":    200,
                "num_ctx":        4096,
                "seed":           42,
            },
        },
        timeout=45,
    )
    resp.raise_for_status()
    return resp.json().get("message", {}).get("content", "")


def _fallback_track_title(path: str, mood: str) -> str:
    stem = os.path.splitext(os.path.basename(path))[0].replace("_", " ").strip()
    if stem:
        return stem.title()
    return f"{mood.title()} Track"


def _normalize_track_title(title: str | None, mood: str, path: str) -> str:
    raw = (title or "").strip()
    if raw and not re.fullmatch(r"Track\s+\d+", raw, flags=re.IGNORECASE):
        return raw

    stem = os.path.splitext(os.path.basename(path))[0]
    number_match = re.search(r"(\d+)$", stem)
    number = number_match.group(1) if number_match else ""
    mood_map = {
        "dark ambient": "Dark Ambient Pulse",
        "dark intro": "Dark Intro Omen",
        "dark mystery": "Dark Mystery Veil",
        "horror tension": "Horror Tension Pulse",
        "horror climax": "Horror Climax Surge",
    }
    prefix = mood_map.get(mood, mood.title())
    return f"{prefix} {number}".strip()


def _build_seed_track_title(target_mood: str, donor_title: str, index: int) -> str:
    donor_title = (donor_title or "").strip()
    if " - " in donor_title:
        donor_title = donor_title.split(" - ")[-1].strip()
    donor_title = re.sub(
        r"^(Dark|Horror)\s+(Ambient|Intro|Mystery|Tension|Climax)\s+"
        r"(Pulse|Omen|Veil|Surge)\s+\d+\s*$",
        "",
        donor_title,
        flags=re.IGNORECASE,
    ).strip()
    if not donor_title:
        donor_title = f"Variant {index}"
    return f"{target_mood.title()} Cut {index} - {donor_title}"


def _parse_music_ai_response(raw: str) -> dict:
    cleaned = (raw or "").strip()
    if not cleaned:
        raise ValueError("respons AI musik kosong")
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            return json.loads(cleaned[start:end + 1])
        raise


def _merge_track_tags(tags: list, title: str) -> list[str]:
    merged = {str(tag).strip().lower() for tag in (tags or []) if str(tag).strip()}
    for token in re.split(r"[^a-zA-Z0-9]+", title.lower()):
        if len(token) >= 4:
            merged.add(token)
    return sorted(merged)


def _download_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    return session


def _hydrate_local_music_from_manifest(mood: str, needed: int) -> int:
    manifest = _load_sources_manifest()
    entries = manifest.get(mood, [])
    if not isinstance(entries, list) or not entries:
        return 0

    mood_dir = os.path.join(LOCAL_MUSIC_ROOT, mood)
    os.makedirs(mood_dir, exist_ok=True)

    added = 0
    for entry in entries:
        if added >= needed:
            break
        source_url = entry.get("source_url")
        target = entry.get("file")
        if not source_url or not target:
            continue
        target_path = os.path.normpath(target)
        if os.path.exists(target_path) and os.path.getsize(target_path) > 10_000:
            continue
        try:
            if _download_public_file(source_url, target_path):
                added += 1
        except Exception as exc:
            logger.warning(f"Hydrate music gagal '{source_url}': {exc}")
    return added


def _download_public_file(url: str, out_path: str) -> bool:
    session = requests.Session()
    session.trust_env = False
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    try:
        resp = session.get(url, stream=True, timeout=60)
        resp.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        return os.path.getsize(out_path) > 10_000
    finally:
        session.close()


def _seed_from_existing_library(target_mood: str, needed: int) -> int:
    if needed <= 0:
        return 0

    donors: list[dict] = []
    for donor_mood in MIXKIT_MOOD_PAGES.keys():
        if donor_mood == target_mood:
            continue
        for candidate in _get_local_music_candidates(donor_mood):
            donors.append({**candidate, "donor_mood": donor_mood})

    if not donors:
        return 0

    random.shuffle(donors)
    target_dir = os.path.join(LOCAL_MUSIC_ROOT, target_mood)
    os.makedirs(target_dir, exist_ok=True)
    current_files = _list_local_music_files(target_mood)
    next_index = len(current_files) + 1
    added = 0

    for donor in donors:
        if added >= needed:
            break
        source_path = donor.get("path")
        if not source_path or not os.path.exists(source_path):
            continue
        ext = os.path.splitext(source_path)[1] or ".mp3"
        dest_path = os.path.join(target_dir, f"seed_{next_index}{ext}")
        next_index += 1
        if os.path.exists(dest_path):
            continue
        try:
            shutil.copy2(source_path, dest_path)
            seeded_title = _build_seed_track_title(target_mood, donor.get("title", ""), added + 1)
            seeded_tags = _merge_track_tags(
                list(donor.get("tags", [])) + target_mood.lower().split(),
                seeded_title,
            )
            _register_source_entry(
                target_mood,
                dest_path,
                source_url=f"local_seed:{donor.get('donor_mood', 'library')}",
                source_page=f"local_seed:{donor.get('donor_mood', 'library')}",
                title=seeded_title,
                tags=seeded_tags,
                artist=donor.get("artist", ""),
                duration=donor.get("duration", 0),
            )
            added += 1
        except Exception as exc:
            logger.warning(f"Seed music gagal '{source_path}' -> '{target_mood}': {exc}")

    return added


def _promote_track_to_local_library(mood: str, source_path: str, source_url: str, source_page: str) -> None:
    """
    Simpan hasil fetch baru ke local mood library agar run berikutnya tidak tergantung network.
    """
    if not AUTO_MUSIC_PROMOTE_FETCHED:
        return
    if not os.path.exists(source_path) or os.path.getsize(source_path) < 10_000:
        return

    mood_dir = os.path.join(LOCAL_MUSIC_ROOT, mood)
    os.makedirs(mood_dir, exist_ok=True)

    existing = _list_local_music_files(mood)
    next_index = len(existing) + 1
    ext = os.path.splitext(source_path)[1] or ".mp3"
    dest_path = os.path.join(mood_dir, f"auto_{next_index}{ext}")

    if os.path.abspath(source_path) == os.path.abspath(dest_path):
        return
    if os.path.exists(dest_path):
        return

    try:
        shutil.copy2(source_path, dest_path)
        _register_source_entry(mood, dest_path, source_url, source_page)
        logger.info(f"Music dipromosikan ke local library: {dest_path}")
    except Exception as exc:
        logger.warning(f"Promote local music gagal: {exc}")


def _load_sources_manifest() -> dict:
    if not os.path.exists(SOURCES_MANIFEST_PATH):
        return {}
    try:
        with open(SOURCES_MANIFEST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        changed = False
        for mood, entries in data.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                file_path = entry.get("file", "")
                if "title" not in entry:
                    entry["title"] = _fallback_track_title(file_path, mood)
                    changed = True
                normalized_title = _normalize_track_title(entry.get("title"), mood, file_path)
                if normalized_title != entry.get("title"):
                    entry["title"] = normalized_title
                    changed = True
                if "tags" not in entry:
                    entry["tags"] = _infer_track_tags(mood, entry.get("source_page", ""))
                    changed = True
                merged_tags = _merge_track_tags(entry.get("tags", []), entry.get("title", ""))
                if merged_tags != entry.get("tags", []):
                    entry["tags"] = merged_tags
                    changed = True
        if changed:
            _write_sources_manifest(data)
        return data
    except Exception as exc:
        logger.warning(f"Gagal baca SOURCES.json: {exc}")
        return {}


def _write_sources_manifest(manifest: dict) -> None:
    try:
        with open(SOURCES_MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        logger.warning(f"Gagal update SOURCES.json: {exc}")


def _register_source_entry(mood: str, file_path: str, source_url: str, source_page: str,
                           title: str | None = None, tags: list[str] | None = None,
                           artist: str = "", duration: int = 0) -> None:
    manifest = _load_sources_manifest()
    entries = manifest.setdefault(mood, [])
    normalized = os.path.normpath(file_path).replace("\\", "/")
    for entry in entries:
        if os.path.normpath(entry.get("file", "")).replace("\\", "/") == normalized:
            entry["title"] = _normalize_track_title(title or entry.get("title"), mood, file_path)
            entry["tags"] = _merge_track_tags(tags or entry.get("tags") or _infer_track_tags(mood, source_page), entry["title"])
            if artist:
                entry["artist"] = artist
            if duration:
                entry["duration"] = duration
            _write_sources_manifest(manifest)
            return

    normalized_title = _normalize_track_title(title or "", mood, file_path)
    entries.append({
        "file": normalized,
        "source_url": source_url,
        "source_page": source_page,
        "bytes": os.path.getsize(file_path),
        "title": normalized_title,
        "tags": _merge_track_tags(tags or _infer_track_tags(mood, source_page), normalized_title),
        "artist": artist,
        "duration": duration,
    })

    _write_sources_manifest(manifest)


def _infer_track_tags(mood: str, source_page: str) -> list[str]:
    tags = set(filter(None, mood.lower().split()))
    page = (source_page or "").lower()
    for token in ("eerie", "ambient", "tension", "suspense", "mystery", "dark", "horror", "intro", "climax", "cautious"):
        if token in page:
            tags.add(token)
    return sorted(tags)


def _parse_iso_duration(value: str) -> int:
    match = re.match(r"PT(?:(\d+)M)?(?:(\d+)S)?", value or "")
    if not match:
        return 0
    minutes = int(match.group(1) or 0)
    seconds = int(match.group(2) or 0)
    return minutes * 60 + seconds


# ─── Freesound ────────────────────────────────────────────────────────────────

def _fetch_from_freesound(mood: str, duration_sec: int, out_path: str) -> str | None:
    api_key = os.environ.get("FREESOUND_API_KEY")
    if not api_key:
        logger.warning("FREESOUND_API_KEY tidak ada — skip Freesound")
        return None

    queries = MOOD_QUERIES.get(mood, [f"{mood} background", "ambient background"])

    for query in queries:
        # Coba dengan filter lisensi dulu, lalu tanpa filter kalau gagal
        for use_license_filter in [True, False]:
            track = _search_freesound(api_key, query, duration_sec, use_license_filter)
            if track:
                logger.info(f"Freesound found: '{track['name']}' (query: '{query}')")
                download_url = _get_preview_url(api_key, track["id"])
                if download_url:
                    success = _download_file(download_url, api_key, out_path)
                    if success:
                        logger.info(f"Music downloaded dari Freesound: {out_path}")
                        return out_path
            time.sleep(1)

    logger.warning(f"Freesound tidak menemukan track untuk mood '{mood}'")
    return None


def _search_freesound(api_key: str, query: str, min_duration: int,
                      use_license_filter: bool = True) -> dict | None:
    url = "https://freesound.org/apiv2/search/text/"

    # Filter: dengan atau tanpa filter lisensi
    if use_license_filter:
        filter_str = f'duration:[{min_duration} TO *] license:"Creative Commons 0"'
    else:
        # Tanpa filter lisensi — lebih banyak hasil, tapi mungkin CC-BY
        filter_str = f"duration:[{min_duration} TO *]"

    params = {
        "query":     query,
        "filter":    filter_str,
        "fields":    "id,name,duration,license,previews",
        "sort":      "rating_desc",
        "page_size": 10,
        "token":     api_key,
    }

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                logger.warning("Freesound rate limit, tunggu 30s...")
                time.sleep(30)
                continue
            if resp.status_code == 401:
                logger.error("Freesound API key tidak valid")
                return None
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                return results[0]
            return None  # Query valid tapi 0 hasil
        except requests.exceptions.Timeout:
            logger.warning(f"Freesound timeout (attempt {attempt+1}/3)")
            time.sleep(5)
        except Exception as e:
            logger.warning(f"Freesound search error (attempt {attempt+1}/3): {e}")
            time.sleep(5)

    return None


def _get_preview_url(api_key: str, sound_id: int) -> str | None:
    """Ambil URL preview HQ MP3 (tidak butuh OAuth, langsung download)."""
    url = f"https://freesound.org/apiv2/sounds/{sound_id}/"
    try:
        resp = requests.get(url, params={"token": api_key}, timeout=15)
        resp.raise_for_status()
        data     = resp.json()
        previews = data.get("previews", {})
        # Preview HQ tidak butuh OAuth — lebih reliable dari direct download
        return (
            previews.get("preview-hq-mp3") or
            previews.get("preview-lq-mp3") or
            data.get("download")
        )
    except Exception as e:
        logger.warning(f"Freesound get sound error: {e}")
        return None


def _download_file(url: str, api_key: str, out_path: str) -> bool:
    """Download file audio ke disk dengan retry 3x."""
    headers = {"Authorization": f"Token {api_key}"}

    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, stream=True, timeout=60)
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            # Validasi file tidak kosong
            if os.path.getsize(out_path) < 10_000:
                logger.warning("File musik terlalu kecil, kemungkinan corrupt")
                os.remove(out_path)
                return False
            return True
        except Exception as e:
            if os.path.exists(out_path):
                os.remove(out_path)
            if attempt < 2:
                logger.warning(f"Download music gagal (attempt {attempt+1}/3), retry: {e}")
                time.sleep(10)
            else:
                logger.warning(f"Download music gagal setelah 3x: {e}")

    return False


# ─── Silent track fallback ────────────────────────────────────────────────────

def _create_silent_track(out_path: str, duration_sec: int) -> str:
    """Buat silent audio sebagai last-resort fallback."""
    import subprocess
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo",
        "-t", str(duration_sec),
        "-q:a", "9", "-acodec", "libmp3lame",
        out_path
    ]
    subprocess.run(cmd, capture_output=True)
    logger.info(f"Silent track created: {out_path}")
    return out_path
