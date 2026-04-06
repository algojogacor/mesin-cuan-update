"""
campaign_engine.py - Baca campaigns.json, buat rencana render batch,
                     dan proteksi double booking via booked_slots.json

Alur:
  1. Baca campaigns.json → dapat daftar tanggal + slot jam
  2. Tiap slot dicek ke booked_slots.json
     - Sudah ada → SKIP (tidak render lagi)
     - Belum ada → masuk antrian render
  3. Setelah render sukses → catat slot ke booked_slots.json
"""

import json
import os
from datetime import date, datetime, timedelta, timezone
from engine.utils import get_logger, load_settings

logger         = get_logger("campaign_engine")
CAMPAIGN_FILE  = "config/campaigns.json"
BOOKED_FILE    = "data/booked_slots.json"

WEEKDAY_MAP = {
    "monday": 0, "tuesday": 1, "wednesday": 2,
    "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
}


# ─── Booked slots (proteksi double booking) ───────────────────────────────────

def _load_booked() -> dict:
    """
    Load semua slot yang sudah pernah di-render.
    Format: {"ch_id_horror": ["2026-03-27T10:00:00.000Z", ...], ...}
    """
    if os.path.exists(BOOKED_FILE):
        try:
            with open(BOOKED_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_booked(booked: dict):
    os.makedirs(os.path.dirname(BOOKED_FILE), exist_ok=True)
    with open(BOOKED_FILE, "w") as f:
        json.dump(booked, f, indent=2)


def is_slot_booked(channel_id: str, slot_utc: str) -> bool:
    """Cek apakah slot tertentu sudah pernah di-render."""
    booked = _load_booked()
    return slot_utc in booked.get(channel_id, [])


def book_slot(channel_id: str, slot_utc: str):
    """Tandai slot sebagai sudah di-render. Dipanggil setelah render sukses."""
    booked = _load_booked()
    if channel_id not in booked:
        booked[channel_id] = []
    if slot_utc not in booked[channel_id]:
        booked[channel_id].append(slot_utc)
        # Bersihkan slot lama (lebih dari 90 hari lalu) biar file tidak membengkak
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        booked[channel_id] = [
            s for s in booked[channel_id]
            if datetime.fromisoformat(s.replace("Z", "+00:00")) > cutoff
        ]
        _save_booked(booked)
        logger.debug(f"Slot booked: [{channel_id}] {slot_utc}")


def unbook_slot(channel_id: str, slot_utc: str):
    """
    Hapus booking slot (misal kalau render gagal dan mau dicoba ulang).
    Jalankan manual: python -c "from engine.campaign_engine import unbook_slot; unbook_slot('ch_id_horror', '2026-03-27T10:00:00.000Z')"
    """
    booked = _load_booked()
    if channel_id in booked and slot_utc in booked[channel_id]:
        booked[channel_id].remove(slot_utc)
        _save_booked(booked)
        logger.info(f"Slot un-booked: [{channel_id}] {slot_utc}")


# ─── Campaign loader ──────────────────────────────────────────────────────────

def load_campaigns() -> list:
    if not os.path.exists(CAMPAIGN_FILE):
        logger.warning(f"campaigns.json tidak ditemukan: {CAMPAIGN_FILE}")
        return []
    with open(CAMPAIGN_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [c for c in data.get("campaigns", []) if c.get("active", False)]


# ─── Build render queue ───────────────────────────────────────────────────────

def build_render_queue() -> list[dict]:
    """
    Buat antrian render lengkap dari semua campaign aktif.
    Setiap item = 1 video yang perlu dirender.
    Slot yang sudah di-render sebelumnya di-SKIP otomatis.

    Return: list of {
        "channel_id": str,
        "profile": "shorts" | "long_form",
        "publish_at_utc": str,   ← ISO UTC string untuk metadata
        "publish_date": date,    ← tanggal WIB
        "publish_hour_wib": int
    }
    """
    campaigns  = load_campaigns()
    settings   = load_settings()
    ch_configs = {
        ch["id"]: ch for ch in settings.get("channels", [])
        if ch.get("active", True)
    }

    queue      = []
    seen_slots = set()  # deduplikasi dalam satu run

    for camp in campaigns:
        dates = _get_campaign_dates(camp)

        for target_date in dates:
            ch_plans = _get_channel_plans(camp, ch_configs)

            for ch_id, plan in ch_plans.items():
                slots = _build_slots(target_date, plan)

                for slot_utc, hour_wib, profile in slots:
                    key = (ch_id, slot_utc)

                    # Skip kalau sudah di-render sebelumnya
                    if is_slot_booked(ch_id, slot_utc):
                        logger.info(f"SKIP (sudah ada): [{ch_id}] {slot_utc}")
                        continue

                    # Skip duplikat dalam run ini
                    if key in seen_slots:
                        continue
                    seen_slots.add(key)

                    queue.append({
                        "channel_id":      ch_id,
                        "profile":         profile,
                        "publish_at_utc":  slot_utc,
                        "publish_date":    target_date,
                        "publish_hour_wib": hour_wib,
                        "campaign_id":     camp["id"],
                    })

    # Sort berdasarkan tanggal publish
    queue.sort(key=lambda x: x["publish_at_utc"])

    if queue:
        logger.info(f"Render queue: {len(queue)} video")
        for item in queue:
            logger.info(
                f"  [{item['channel_id']}] {item['profile']:9} "
                f"→ {item['publish_date']} {item['publish_hour_wib']}:00 WIB"
            )
    else:
        logger.info("Render queue kosong — semua slot sudah di-render atau tidak ada campaign aktif")

    return queue


def get_todays_plan(target_date: date = None) -> list[dict]:
    """
    Compatibility function — return rencana untuk hari ini saja.
    Dipakai oleh scheduler.py.
    """
    today = target_date or date.today()
    queue = build_render_queue()
    return [item for item in queue if item["publish_date"] == today]


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _get_campaign_dates(camp: dict) -> list[date]:
    """Dapatkan semua tanggal yang termasuk dalam campaign ini."""
    schedule = camp.get("schedule", {})
    stype    = schedule.get("type", "")
    today    = date.today()

    try:
        if stype == "range":
            start  = date.fromisoformat(schedule["start_date"])
            end    = date.fromisoformat(schedule["end_date"])
            start  = max(start, today)  # jangan render untuk masa lalu
            return [start + timedelta(days=i) for i in range((end - start).days + 1)]

        elif stype == "specific_dates":
            dates = [date.fromisoformat(d) for d in schedule.get("dates", [])]
            return [d for d in dates if d >= today]

        elif stype == "weekdays":
            start    = date.fromisoformat(schedule["start_date"])
            end      = date.fromisoformat(schedule["end_date"])
            start    = max(start, today)
            weekdays = [WEEKDAY_MAP[w.lower()] for w in schedule.get("weekdays", [])]
            return [
                start + timedelta(days=i)
                for i in range((end - start).days + 1)
                if (start + timedelta(days=i)).weekday() in weekdays
            ]

        else:
            logger.warning(f"Schedule type tidak dikenal: '{stype}'")
            return []

    except (KeyError, ValueError) as e:
        logger.error(f"Error parsing campaign '{camp.get('id')}': {e}")
        return []


def _get_channel_plans(camp: dict, ch_configs: dict) -> dict:
    """Return dict {ch_id: {shorts_per_day, long_form_per_day, publish_hours_wib}}"""
    if "all_channels" in camp:
        return {ch_id: camp["all_channels"] for ch_id in ch_configs}
    elif "per_channel" in camp:
        return {
            ch_id: cfg
            for ch_id, cfg in camp["per_channel"].items()
            if ch_id in ch_configs
        }
    return {}


def _build_slots(target_date: date, plan: dict) -> list[tuple]:
    """
    Buat list slot untuk 1 hari.
    Return: [(slot_utc, hour_wib, profile), ...]
    """
    shorts    = plan.get("shorts_per_day", 0)
    long_form = plan.get("long_form_per_day", 0)
    hours     = plan.get("publish_hours_wib", [17])

    # Buat list (profile, hour) pairs
    video_list = (
        [("shorts", None)] * shorts +
        [("long_form", None)] * long_form
    )

    # Assign jam ke tiap video
    while len(hours) < len(video_list):
        hours = hours + [hours[-1] + 1]

    result = []
    for i, (profile, _) in enumerate(video_list):
        hour_wib = hours[i] % 24
        dt_wib   = datetime(
            target_date.year, target_date.month, target_date.day,
            hour_wib, 0, 0,
            tzinfo=timezone(timedelta(hours=7))
        )
        dt_utc   = dt_wib.astimezone(timezone.utc)
        slot_utc = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        result.append((slot_utc, hour_wib, profile))

    return result


# ─── Preview ──────────────────────────────────────────────────────────────────

def preview_campaign(campaign_id: str = None, days: int = 30) -> None:
    """Print preview render queue beserta status booked/free."""
    campaigns = load_campaigns()
    if campaign_id:
        campaigns = [c for c in campaigns if c["id"] == campaign_id]

    if not campaigns:
        print("Tidak ada campaign aktif.")
        return

    settings   = load_settings()
    ch_configs = {ch["id"]: ch for ch in settings.get("channels", []) if ch.get("active", True)}

    print(f"\n{'='*60}")
    print(f"  Preview Campaign (maks {days} hari ke depan)")
    print(f"{'='*60}")

    today    = date.today()
    cutoff   = today + timedelta(days=days)
    total_free   = 0
    total_booked = 0

    for camp in campaigns:
        print(f"\n📋 [{camp['id']}] {camp['name']}")
        dates = [d for d in _get_campaign_dates(camp) if d <= cutoff]

        for target_date in dates:
            ch_plans = _get_channel_plans(camp, ch_configs)
            day_lines = []

            for ch_id, plan in ch_plans.items():
                slots = _build_slots(target_date, plan)
                for slot_utc, hour_wib, profile in slots:
                    booked = is_slot_booked(ch_id, slot_utc)
                    status = "✅ sudah render" if booked else "⬜ belum render"
                    if booked:
                        total_booked += 1
                    else:
                        total_free += 1
                    day_lines.append(
                        f"    {ch_id:15} {profile:9} {hour_wib:02d}:00 WIB  {status}"
                    )

            if day_lines:
                print(f"\n  📅 {target_date.strftime('%A, %d %b %Y')}:")
                for line in day_lines:
                    print(line)

    print(f"\n{'='*60}")
    print(f"  Ringkasan: {total_free} belum render, {total_booked} sudah render")
    if total_free > 0:
        print(f"  → Jalankan 'python main.py' untuk render {total_free} video")
    else:
        print(f"  → Semua slot sudah di-render!")
    print(f"{'='*60}\n")