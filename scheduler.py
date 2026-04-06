"""
scheduler.py - Jadwalkan pipeline berjalan otomatis berdasarkan campaigns.json
Jalankan: python scheduler.py

Mode:
  1. Campaign mode  — pipeline jalan sesuai jadwal di campaigns.json
  2. Manual mode    — pipeline jalan di jam tetap (SCHEDULE_TIMES), seperti versi lama

Untuk preview jadwal campaign:
  python scheduler.py --preview
  python scheduler.py --preview --days 30
"""

import sys
import signal
import argparse
from datetime import date
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from engine.retention_engine import analyze_channel
from engine.utils import get_logger
from engine.campaign_engine import get_todays_plan, preview_campaign
from main import run_all_channels, run_channel

load_dotenv()       
logger = get_logger("scheduler")

# ─── Jam cek campaign (tiap jam berapa scheduler bangun untuk cek) ────────────
# Scheduler akan cek campaigns.json setiap jam ini.
# Kalau ada campaign aktif hari ini, pipeline langsung jalan.
# Rekomendasi: jam 06:00 WIB (sebelum semua prime time)
CAMPAIGN_CHECK_HOURS = [6, 12]  # WIB

# ─── Manual fallback (dipakai kalau tidak ada campaign aktif) ─────────────────
# Set MANUAL_FALLBACK = True kalau mau pipeline tetap jalan walau tidak ada campaign
MANUAL_FALLBACK = False
SCHEDULE_TIMES  = [
    {"hour": 8,  "minute": 0},
    {"hour": 20, "minute": 0},
]
# ─────────────────────────────────────────────────────────────────────────────


def job_campaign():
    """
    Job utama: cek campaign hari ini dan jalankan pipeline sesuai rencana.
    """
    today = date.today()
    plan  = get_todays_plan(today)

    if not plan:
        logger.info("Tidak ada campaign aktif hari ini — skip pipeline.")
        if MANUAL_FALLBACK:
            logger.info("MANUAL_FALLBACK aktif → jalankan pipeline default...")
            _run_default_pipeline()
        return

    logger.info(f"⏰ Campaign aktif! Menjalankan pipeline untuk {len(plan)} channel...")

    from engine.utils import load_settings
    settings     = load_settings()
    ch_map       = {ch["id"]: ch for ch in settings.get("channels", [])}

    for p in plan:
        ch_id    = p["channel_id"]
        channel  = ch_map.get(ch_id)
        if not channel:
            logger.warning(f"Channel '{ch_id}' tidak ditemukan di settings.json, skip")
            continue

        logger.info(
            f"[{ch_id}] Render: {p['shorts']} shorts + {p['long_form']} long_form "
            f"| publish slots: {p['publish_slots_wib']} WIB"
        )

        # Override daily_plan dari settings dengan rencana campaign hari ini
        from engine import state_manager
        state_manager.init_db()

        _run_channel_with_plan(channel, p)

def run_daily_analytics():
    from engine.utils import load_settings
    settings = load_settings()
    for channel in settings["channels"]:
        if channel.get("active"):
            logger.info(f"[{channel['id']}] Fetching retention analytics...")
            analyze_channel(channel)
 

def _run_channel_with_plan(channel: dict, plan: dict):
    """
    Jalankan pipeline untuk 1 channel dengan jumlah video dari campaign plan.
    """
    from main import run_once
    ch_id         = channel["id"]
    n_shorts      = plan["shorts"]
    n_long_form   = plan["long_form"]
    consecutive_fail = 0

    # Render shorts
    for i in range(n_shorts):
        success = run_once(channel, profile="shorts")
        if success:
            consecutive_fail = 0
            logger.info(f"[{ch_id}] Shorts {i+1}/{n_shorts} ✅")
        else:
            consecutive_fail += 1
            logger.error(f"[{ch_id}] Shorts {i+1}/{n_shorts} ❌")
            if consecutive_fail >= 3:
                logger.error(f"[{ch_id}] 3 gagal berturut-turut, hentikan channel ini")
                return

    # Render long form
    for i in range(n_long_form):
        success = run_once(channel, profile="long_form")
        if success:
            logger.info(f"[{ch_id}] Long form {i+1}/{n_long_form} ✅")
        else:
            logger.error(f"[{ch_id}] Long form {i+1}/{n_long_form} ❌")

    from engine import cleanup_engine
    cleanup_engine.run()


def _run_default_pipeline():
    """Jalankan pipeline default (semua channel, pakai settings.json daily_plan)."""
    try:
        run_all_channels()
    except Exception as e:
        logger.error(f"Default pipeline error: {e}")


def job_manual():
    """Job untuk mode manual (tanpa campaign)."""
    logger.info("⏰ Manual scheduler memicu pipeline...")
    _run_default_pipeline()


def handle_exit(sig, frame):
    logger.info("🛑 Scheduler dihentikan.")
    sys.exit(0)


def cmd_preview(days: int = 14):
    """Tampilkan preview jadwal campaign N hari ke depan."""
    preview_campaign(days=days)


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scheduler pipeline mesin_cuan")
    parser.add_argument("--preview",      action="store_true", help="Preview jadwal campaign")
    parser.add_argument("--days",         type=int, default=14, help="Jumlah hari untuk preview (default 14)")
    parser.add_argument("--run-now",      action="store_true", help="Jalankan pipeline sekarang (tanpa tunggu jadwal)")
    parser.add_argument("--manual-only",  action="store_true", help="Pakai manual schedule saja (abaikan campaign)")
    args = parser.parse_args()

    # Mode preview
    if args.preview:
        cmd_preview(args.days)
        sys.exit(0)

    # Mode run-now
    if args.run_now:
        logger.info("🚀 Run now mode — jalankan pipeline langsung...")
        job_campaign()
        sys.exit(0)

    signal.signal(signal.SIGINT,  handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    scheduler = BlockingScheduler(timezone="Asia/Jakarta")

    if args.manual_only:
        # Mode manual: pakai SCHEDULE_TIMES saja
        logger.info("Mode: Manual schedule")
        for t in SCHEDULE_TIMES:
            scheduler.add_job(
                job_manual,
                trigger=CronTrigger(hour=t["hour"], minute=t["minute"]),
                id=f"manual_{t['hour']:02d}{t['minute']:02d}",
                name=f"Manual jam {t['hour']:02d}:{t['minute']:02d}",
                max_instances=1,
                misfire_grace_time=300,
            )
        logger.info("📅 Manual scheduler aktif. Jadwal:")
        for t in SCHEDULE_TIMES:
            logger.info(f"   → Setiap hari jam {t['hour']:02d}:{t['minute']:02d} WIB")

    else:
        # Mode campaign: cek campaign di jam-jam tertentu
        logger.info("Mode: Campaign schedule")
        for hour in CAMPAIGN_CHECK_HOURS:
            scheduler.add_job(
                job_campaign,
                trigger=CronTrigger(hour=hour, minute=0),
                id=f"campaign_{hour:02d}00",
                name=f"Campaign check jam {hour:02d}:00",
                max_instances=1,
                misfire_grace_time=300,
            )

        logger.info("📅 Campaign scheduler aktif. Cek campaign setiap:")
        for hour in CAMPAIGN_CHECK_HOURS:
            logger.info(f"   → jam {hour:02d}:00 WIB")
        logger.info("\nTips:")
        logger.info("   python scheduler.py --preview       → lihat jadwal 14 hari ke depan")
        logger.info("   python scheduler.py --run-now       → jalankan pipeline sekarang")
        logger.info("   python scheduler.py --manual-only   → pakai jadwal manual saja")

    logger.info("\n   Edit config/campaigns.json untuk ubah jadwal kampanye")
    logger.info("   Tekan Ctrl+C untuk berhenti.\n")

    try:
        scheduler.start()
    except Exception as e:
        logger.error(f"Scheduler error: {e}")