"""
series_engine.py - Manajemen seri konten dan auto-queue Part 2 dari video viral.

Fitur:
  1. Baca series_catalog.json → track progress di series_state.json
  2. Antri episode berikutnya dari series aktif ke topic_overrides.json
  3. Deteksi video viral dari DB → antri Part 2 ke topic_overrides.json
  4. Semua operasi idempoten — aman dijalankan berulang kali

Catalog dibuat manual via strategi.bat atau langsung edit JSON.
State dikelola otomatis oleh engine ini.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional

from engine.utils import get_logger, save_json
from engine.state_manager import get_videos_for_channel

logger = get_logger("series_engine")

VIRAL_THRESHOLD_DEFAULT = 1000  # views minimum untuk trigger Part 2
_PART2_SUFFIXES_ID = [
    "Part 2 — Yang Lebih Gelap",
    "Part 2 — Sisi yang Disembunyikan",
    "Part 2 — Fakta Lanjutan yang Lebih Mengejutkan",
    "Part 2 — Kedalaman yang Tidak Pernah Diceritakan",
]
_PART2_SUFFIXES_EN = [
    "Part 2 — The Darker Side",
    "Part 2 — What They Hid",
    "Part 2 — Even More Shocking Facts",
    "Part 2 — The Depth Never Told",
]
_ITERATED_MARKER = "__iterated__"


# ─── Public API ───────────────────────────────────────────────────────────────

def check_and_queue_parts(channel: dict) -> None:
    """
    Entry point utama. Dipanggil di awal batch render untuk setiap channel.

    Urutan:
      1. Antri episode berikutnya dari series aktif (hingga 3 item sekaligus)
      2. Jika tidak ada series aktif, AI invent series baru secara otomatis.
      3. Antri Part 2 dari video viral yang belum punya lanjutan
    """
    ch_id = channel["id"]
    language = channel.get("language", "id")

    _queue_next_series_episodes(ch_id, language, channel)
    _queue_viral_part2(channel)


def get_series_status(ch_id: str) -> list[dict]:
    """
    Return status semua series untuk tampilan di strategi.bat.
    """
    catalog = _load_catalog(ch_id)
    state = _load_state(ch_id)
    result = []

    for series in catalog.get("series", []):
        sid = series["id"]
        items = series.get("items", [])
        s_state = state.get(sid, {})
        done = s_state.get("items_done", [])
        skipped = s_state.get("items_skipped", [])
        remaining = [i for i in items if i not in done and i not in skipped]
        next_up = s_state.get("next_up") or (remaining[0] if remaining else None)

        result.append({
            "id": sid,
            "name": series.get("name", sid),
            "active": series.get("active", True),
            "total": len(items),
            "done": len(done),
            "skipped": len(skipped),
            "remaining": len(remaining),
            "next_up": next_up,
            "completed": len(remaining) == 0,
        })

    return result


def skip_current_episode(ch_id: str, series_id: str) -> bool:
    """
    Skip episode next_up sekarang → maju ke item berikutnya.
    Return True jika berhasil.
    """
    catalog = _load_catalog(ch_id)
    state = _load_state(ch_id)

    series = next((s for s in catalog.get("series", []) if s["id"] == series_id), None)
    if not series:
        logger.warning(f"[series] Series '{series_id}' tidak ditemukan di catalog")
        return False

    s_state = state.setdefault(series_id, {})
    done = s_state.get("items_done", [])
    skipped = s_state.setdefault("items_skipped", [])
    items = series.get("items", [])
    remaining = [i for i in items if i not in done and i not in skipped]

    if not remaining:
        logger.info(f"[series] '{series_id}' sudah tidak ada item tersisa")
        return False

    current = s_state.get("next_up") or remaining[0]
    if current not in skipped:
        skipped.append(current)
        logger.info(f"[series] '{series_id}' skip: {current}")

    # Advance next_up
    remaining_after = [i for i in items if i not in done and i not in skipped]
    s_state["next_up"] = remaining_after[0] if remaining_after else None

    _save_state(ch_id, state)
    return True


def toggle_series_active(ch_id: str, series_id: str) -> bool:
    """Toggle active status sebuah series. Return new active value."""
    catalog = _load_catalog(ch_id)
    for series in catalog.get("series", []):
        if series["id"] == series_id:
            series["active"] = not series.get("active", True)
            _save_catalog(ch_id, catalog)
            new_val = series["active"]
            logger.info(f"[series] '{series_id}' active → {new_val}")
            return new_val
    return False


def add_series(ch_id: str, series_data: dict) -> None:
    """Tambah series baru ke catalog."""
    catalog = _load_catalog(ch_id)
    # Hindari duplikat ID
    existing_ids = {s["id"] for s in catalog.get("series", [])}
    if series_data["id"] in existing_ids:
        logger.warning(f"[series] Series '{series_data['id']}' sudah ada, skip")
        return
    catalog.setdefault("series", []).append(series_data)
    _save_catalog(ch_id, catalog)
    logger.info(f"[series] Series baru ditambahkan: {series_data['id']}")


def mark_episode_done(ch_id: str, series_id: str, item: str, views: int = 0) -> None:
    """
    Dipanggil setelah render/upload sukses untuk advance state series.
    """
    state = _load_state(ch_id)
    s_state = state.setdefault(series_id, {})
    done = s_state.setdefault("items_done", [])

    if item not in done:
        done.append(item)

    s_state["last_item"] = item
    s_state["last_views"] = views
    s_state["last_updated"] = datetime.now().isoformat()

    # Recalculate next_up
    catalog = _load_catalog(ch_id)
    series = next((s for s in catalog.get("series", []) if s["id"] == series_id), None)
    if series:
        items = series.get("items", [])
        skipped = s_state.get("items_skipped", [])
        remaining = [i for i in items if i not in done and i not in skipped]
        s_state["next_up"] = remaining[0] if remaining else None

    _save_state(ch_id, state)
    logger.info(f"[series] '{series_id}' mark done: {item} | next: {s_state.get('next_up')}")


def queue_part2_for_video(ch_id: str, video_title: str, language: str = "id") -> bool:
    """
    Tambahkan Part 2 dari video tertentu ke topic_overrides.json secara manual.
    Dipanggil dari strategi.bat menu [9].
    Return True jika berhasil ditambahkan.
    """
    import random
    suffixes = _PART2_SUFFIXES_ID if language == "id" else _PART2_SUFFIXES_EN
    suffix = random.choice(suffixes)
    topic = f"{video_title} — {suffix}"
    return _add_to_overrides(ch_id, {
        "topic": topic,
        "series_name": None,
        "series_item": None,
        "part_number": 2,
        "original_title": video_title,
        "notes": "Part 2 manual dari strategi.bat",
        "used": False,
        "created_at": datetime.now().isoformat(),
    })


def get_viral_candidates(channel: dict, threshold: Optional[int] = None) -> list[dict]:
    """
    Return daftar video viral yang belum punya Part 2 di overrides.
    Dipakai strategi.bat menu [8].
    """
    ch_id = channel["id"]
    threshold = threshold or VIRAL_THRESHOLD_DEFAULT
    videos = get_videos_for_channel(ch_id)
    overrides = _load_overrides(ch_id)

    # Kumpulkan original_title yang sudah punya Part 2 di overrides
    already_queued = {
        o.get("original_title", "").strip().lower()
        for o in overrides
        if o.get("part_number") == 2
    }

    # Video di series_state yang sudah pernah diiterasi
    state = _load_state(ch_id)
    all_done = set()
    for s_state in state.values():
        all_done.update(s_state.get("items_done", []))

    candidates = []
    for video in videos:
        views = video.get("views", 0)
        title = (video.get("title") or video.get("topic") or "").strip()
        if not title or views < threshold:
            continue
        if title.lower() in already_queued:
            continue
        candidates.append({
            "title": title,
            "views": views,
            "video_id": video.get("video_id", ""),
            "published_at": video.get("published_at", ""),
        })

    return sorted(candidates, key=lambda v: v["views"], reverse=True)


# ─── Internal: series queue ───────────────────────────────────────────────────

def _queue_next_series_episodes(ch_id: str, language: str, channel: dict) -> None:
    """Untuk setiap series aktif, antri episode berikutnya (maks 1) ke overrides."""
    catalog = _load_catalog(ch_id)
    state = _load_state(ch_id)
    overrides = _load_overrides(ch_id)

    # Kumpulkan topics yang sudah ada di overrides (belum used)
    pending_topics = {o["topic"].lower() for o in overrides if not o.get("used")}

    # Pastikan kita punya 3 active series dengan sisa episode
    active_series = []
    for series in catalog.get("series", []):
        if not series.get("active", True):
            continue
        
        sid = series["id"]
        items = series.get("items", [])
        s_state = state.setdefault(sid, {})
        done = s_state.get("items_done", [])
        skipped = s_state.get("items_skipped", [])
        remaining = [i for i in items if i not in done and i not in skipped]

        if not remaining:
            logger.info(f"[series] '{sid}' semua episode selesai")
            s_state["completed"] = True
            series["active"] = False
            _save_catalog(ch_id, catalog)
            continue
            
        active_series.append((series, remaining, s_state))
        
    changed = False

    # Auto-invent if active series < 3
    if len(active_series) < 3:
        needed = 3 - len(active_series)
        logger.info(f"[series] Butuh {needed} series aktif lagi. Auto-inventing...")
        avoid_topics = [s[0].get("name", s[0]["id"]) for s in active_series]
        
        for _ in range(needed):
            success = _auto_invent_new_series(ch_id, language, channel, avoid_topics)
            if success:
                # Reload by calling recursively so it evaluates catalog again
                _queue_next_series_episodes(ch_id, language, channel)
                return  # Return immediately, recursive call handles the rest
        
    # Queue exactly 1 episode for each active series (Horizontal Binge)
    for series, remaining, s_state in active_series:
        sid = series["id"]
        items = series.get("items", [])
        
        for next_item in remaining[:1]:
            # Generate topic dari series item
            topic = _build_series_topic(series, next_item, language)

            # Cek apakah sudah ada di overrides pending
            if topic.lower() in pending_topics:
                continue

            part_num = items.index(next_item) + 1
            
            # Tambah ke overrides
            added = _add_to_overrides(ch_id, {
                "topic": topic,
                "series_name": sid,
                "series_item": next_item,
                "part_number": part_num,
                "notes": f"Auto dari series: {series.get('name', sid)} (Part {part_num})",
                "used": False,
                "created_at": datetime.now().isoformat(),
            })
            if added:
                logger.info(f"[series] '{sid}' antri episode: {next_item} → '{topic}' (Part {part_num})")
                changed = True
                pending_topics.add(topic.lower())
                
        # Advance next_up to the one right after the queued ones, just for local state display
        if remaining:
            s_state["next_up"] = remaining[0]

    if changed:
        _save_state(ch_id, state)

def _auto_invent_new_series(ch_id: str, language: str, channel: dict, avoid_topics: list[str] = None) -> bool:
    """
    Jika tidak ada series aktif dengan sisa episode, AI invent 3-part series baru 
    menggunakan Perplexity Web Research dan Groq.
    """
    logger.info(f"[{ch_id}] 🤖 Auto-inventing new series karena catalog habis...")
    from engine.research_engine import research_topic
    from engine.utils import require_env
    
    # 1. Research
    query = "Berita horor, kejadian misteri nyata, atau thread konspirasi terbaru yang sedang viral dan mendalam" if language == "id" else "Recent viral horror news, mysterious real events, or unsolved deep internet mysteries"
    research = research_topic(query, channel)
    context = research.get("text", "")[:1200] if research else "Gunakan data pengetahuan umum."
    
    niche = channel.get("niche", "horror_facts")
    niche_ctx = "horor dan fakta gelap nyata" if niche == "horror_facts" else "psikologi gelap atau mind-bending"
    
    avoid_str = ""
    if avoid_topics:
        avoid_str = f"\nPENTING: JANGAN membahas topik yang mirip atau berhubungan dengan: {', '.join(avoid_topics)}."

    # 2. Prompt LLM
    prompt = f"""
Buat 1 IDE SERI KONTEN (terdiri dari tepat 3 Episode berurutan) tentang sebuah {niche_ctx} yang menarik.
Gunakan referensi riset terbaru ini jika relevan: 
{context}
{avoid_str}

Format WAJIB JSON tunggal murni (tanpa tag markdown ```json ):
{{
  "id": "misteri_contoh_slug_tanpa_spasi",
  "name": "Judul Topik Series (singkat)",
  "description": "Deskripsi singkat series",
  "items": [
     "Judul spesifik Part 1 (Pembuka/Misteri)",
     "Judul spesifik Part 2 (Eskalasi/Investigasi)",
     "Judul spesifik Part 3 (Klimaks/Kesimpulan)"
  ]
}}
"""
    try:
        import requests
        OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
        OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "deepseek-v3.1:671b-cloud")
        
        payload = {
            "model": OLLAMA_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.75}
        }
        
        resp = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=300)
        resp.raise_for_status()
        
        content = resp.json().get("message", {}).get("content", "").strip()
        from engine.script_engine import _clean_raw_json
        
        cleaned = _clean_raw_json(content)
        series_data = json.loads(cleaned)
        
        series_data["active"] = True
        series_data["viral_threshold_for_part2"] = 1000
        
        if len(series_data.get("items", [])) > 0:
            add_series(ch_id, series_data)
            logger.info(f"[{ch_id}] ✅ Berhasil auto-invent series via Ollama: {series_data['id']}")
            return True
            
    except Exception as exc:
        logger.warning(f"[{ch_id}] Gagal auto-invent series via Ollama, mencoba opsi terakhir (Groq): {exc}")
        try:
            from groq import Groq
            client = Groq(api_key=require_env("GROQ_API_KEY"))
            resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.75,
            )
            content = resp.choices[0].message.content.strip()
            series_data = json.loads(content)
            series_data["active"] = True
            series_data["viral_threshold_for_part2"] = 1000
            
            if len(series_data.get("items", [])) > 0:
                add_series(ch_id, series_data)
                logger.info(f"[{ch_id}] ✅ Berhasil auto-invent series via Groq: {series_data['id']}")
                return True
        except Exception as exc_groq:
            logger.warning(f"[{ch_id}] Opsi terakhir (Groq) gagal: {exc_groq}")
    
    return False


def _queue_viral_part2(channel: dict) -> None:
    """
    Deteksi video viral menggunakan "Efficiency Ratio" (bukan sekadar views).

    Sinyal viral yang dipertimbangkan:
      1. AVP (avg_view_percentage) > CHANNEL_AVG * 1.15  → Retention Signal
      2. Engagement Ratio (likes/views) > CHANNEL_AVG * 1.5 → Engagement Signal
      3. Views > VIRAL_THRESHOLD_DEFAULT                 → Volume Signal

    Sebuah video butuh memenuhi MINIMAL 2 dari 3 sinyal untuk trigger Part 2.
    """
    import random
    ch_id = channel["id"]
    language = channel.get("language", "id")
    videos = get_videos_for_channel(ch_id)
    overrides = _load_overrides(ch_id)

    already_queued_part2 = {
        o.get("original_title", "").strip().lower()
        for o in overrides
        if o.get("part_number") == 2
    }

    suffixes = _PART2_SUFFIXES_ID if language == "id" else _PART2_SUFFIXES_EN

    # ── Hitung benchmark channel ────────────────────────────────────────────
    benchmark = _get_channel_benchmark(videos)
    logger.debug(f"[series] [{ch_id}] Benchmark: views_avg={benchmark['views_avg']:.0f}, "
                 f"avp_avg={benchmark['avp_avg']:.1f}%, engagement_avg={benchmark['engagement_avg']:.4f}")

    for video in videos:
        title = (video.get("title") or video.get("topic") or "").strip()
        if not title:
            continue
        if title.lower() in already_queued_part2:
            continue

        views = video.get("views", 0) or 0
        likes = video.get("likes", 0) or 0
        avp   = video.get("avg_view_percentage", 0) or 0

        # ── Hitung sinyal ────────────────────────────────────────────────
        signals_met = 0
        signal_log  = []

        # Sinyal 1: Volume (views > threshold)
        if views >= VIRAL_THRESHOLD_DEFAULT:
            signals_met += 1
            signal_log.append(f"views={views:,}")

        # Sinyal 2: Retention (AVP >= 1.15x rata-rata channel, minimal AVP > 0)
        if avp > 0 and benchmark["avp_avg"] > 0:
            if avp >= benchmark["avp_avg"] * 1.15:
                signals_met += 1
                signal_log.append(f"avp={avp:.1f}%>avg{benchmark['avp_avg']:.1f}%")

        # Sinyal 3: Engagement ratio (likes/views vs rata-rata channel)
        engagement = (likes / views) if views > 0 else 0
        if benchmark["engagement_avg"] > 0 and engagement >= benchmark["engagement_avg"] * 1.5:
            signals_met += 1
            signal_log.append(f"engagement={engagement:.4f}>avg{benchmark['engagement_avg']:.4f}")

        # ── Hanya trigger Part 2 jika >= 2 sinyal terpenuhi ─────────────
        if signals_met < 2:
            continue

        suffix = random.choice(suffixes)
        topic  = f"{title} — {suffix}"

        added = _add_to_overrides(ch_id, {
            "topic":          topic,
            "series_name":    None,
            "series_item":    None,
            "part_number":    2,
            "original_title": title,
            "viral_signals":  signal_log,
            "notes":          f"Auto Part 2 — {signals_met}/3 sinyal terpenuhi: {', '.join(signal_log)}",
            "used":           False,
            "created_at":     datetime.now().isoformat(),
        })
        if added:
            logger.info(f"[series] Auto Part 2: '{title}' | signals: {signal_log}")


def _get_channel_benchmark(videos: list[dict]) -> dict:
    """
    Hitung rata-rata performa channel dari semua video yang ada di DB.
    Return dict dengan views_avg, avp_avg, engagement_avg.
    """
    if not videos:
        return {"views_avg": 0.0, "avp_avg": 0.0, "engagement_avg": 0.0}

    views_list       = [v.get("views", 0) or 0 for v in videos]
    avp_list         = [v.get("avg_view_percentage", 0) or 0 for v in videos if (v.get("avg_view_percentage") or 0) > 0]
    engagement_list  = [(v.get("likes", 0) or 0) / max(v.get("views", 1), 1) for v in videos]

    return {
        "views_avg":       sum(views_list) / len(views_list) if views_list else 0.0,
        "avp_avg":         sum(avp_list) / len(avp_list) if avp_list else 0.0,
        "engagement_avg":  sum(engagement_list) / len(engagement_list) if engagement_list else 0.0,
    }


# ─── Internal: catalog/state/overrides IO ─────────────────────────────────────

def _data_dir(ch_id: str) -> str:
    """Return path to data/{ch_id}/ directory, create if not exists."""
    path = f"data/{ch_id}"
    os.makedirs(path, exist_ok=True)
    return path


def _catalog_path(ch_id: str) -> str:
    return os.path.join(_data_dir(ch_id), "series_catalog.json")


def _state_path(ch_id: str) -> str:
    return os.path.join(_data_dir(ch_id), "series_state.json")


def _overrides_path(ch_id: str) -> str:
    return os.path.join(_data_dir(ch_id), "topic_overrides.json")


def _load_catalog(ch_id: str) -> dict:
    path = _catalog_path(ch_id)
    if not os.path.exists(path):
        return {"series": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning(f"[series] Gagal baca catalog: {exc}")
        return {"series": []}


def _save_catalog(ch_id: str, catalog: dict) -> None:
    path = _catalog_path(ch_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    save_json(catalog, path)


def _load_state(ch_id: str) -> dict:
    path = _state_path(ch_id)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning(f"[series] Gagal baca state: {exc}")
        return {}


def _save_state(ch_id: str, state: dict) -> None:
    path = _state_path(ch_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    save_json(state, path)


def _load_overrides(ch_id: str) -> list:
    path = _overrides_path(ch_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning(f"[series] Gagal baca overrides: {exc}")
        return []


def _save_overrides(ch_id: str, overrides: list) -> None:
    path = _overrides_path(ch_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    save_json(overrides, path)


def _add_to_overrides(ch_id: str, entry: dict) -> bool:
    """Tambah entry ke overrides jika belum ada topik yang sama (pending)."""
    overrides = _load_overrides(ch_id)
    topic_lower = entry["topic"].lower()
    for o in overrides:
        if o["topic"].lower() == topic_lower and not o.get("used"):
            return False  # sudah ada
    overrides.append(entry)
    _save_overrides(ch_id, overrides)
    return True


def _build_series_topic(series: dict, item: str, language: str) -> str:
    """
    Buat topik dari series item.
    AI akan mengembangkan ini menjadi script penuh nanti.
    """
    series_name = series.get("name", "")

    if language == "id":
        templates = {
            "misteri_provinsi": f"Misteri Horor Tersembunyi di {item}",
            "kota_horror": f"Tempat Paling Angker di {item}",
            "eksperimen_gelap": f"Eksperimen Gelap: {item}",
            "ritual_nusantara": f"Ritual Terlarang: {item}",
        }
    else:
        templates = {
            "misteri_provinsi": f"Hidden Horror Mysteries of {item}",
            "kota_horror": f"Most Haunted Places in {item}",
            "eksperimen_gelap": f"Dark Experiment: {item}",
            "ritual_nusantara": f"Forbidden Ritual: {item}",
        }

    sid = series.get("id", "")
    if sid in templates:
        return templates[sid]

    # Generic fallback
    if language == "id":
        return f"Fakta Gelap tentang {item} — {series_name}"
    return f"Dark Facts about {item} — {series_name}"
