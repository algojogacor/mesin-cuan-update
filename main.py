"""
main.py - Pipeline utama Auto Content Machine (v5)
Render video → QC Vision (Gemini) → auto-fix → upload ke GDrive queue → Koyeb upload ke YouTube

Cara pakai:
  python main.py                                    → render sesuai campaigns.json
  python main.py --preview                          → lihat status slot campaign
  python main.py --channel ch_id_horror             → render 1 channel saja
  python main.py --dry-run                          → test tanpa upload ke GDrive
  python main.py --skip-qc                          → skip QC vision (lebih cepat)
  python main.py --legacy                           → pakai daily_plan di settings.json
  python main.py --analytics                        → jalankan update data retention harian
  python main.py --channel ch_x --topic "Topik"     → inject topik manual untuk testing
  python main.py --channel ch_x --topic "T" --debug → verbose debug logging

v5: prepare_fresh_run() — auto-cleanup audio/footage/temp sebelum batch render
"""

import argparse
import glob
import os
import shutil
import traceback
from dotenv import load_dotenv
from engine.utils import get_logger, load_settings, channel_data_path, load_json, save_json, timestamp
from engine import (
    topic_engine, script_engine, qc_engine,
    tts_engine, footage_engine, video_engine,
    thumbnail_engine, metadata_engine, gdrive_engine,
    state_manager, notif_engine, cleanup_engine
)

# ── Import untuk analytics harian ────────────────────────────────────────────
from engine.retention_engine import analyze_channel

load_dotenv()
logger = get_logger("main")


# ─── Auto-Cleanup (Disk Management) ──────────────────────────────────────────

_CLEAN_SUBDIRS = ("audio", "footage", "temp")


def prepare_fresh_run(channels: list, dry_run: bool = False) -> None:
    """
    UPGRADE: Auto-Cleanup System (Disk Management).
    Hapus isi folder audio/, footage/, temp/ di setiap channel aktif
    SEBELUM batch render dimulai — agar SSD tidak membengkak.

    Aman: hanya menghapus konten di dalam subfolder, bukan folder-nya sendiri.
    Di-skip jika dry_run=True (preview mode, tidak ada render nyata).
    """
    if dry_run:
        logger.info("[prepare_fresh_run] DRY RUN — skip cleanup")
        return

    logger.info("🧹 [prepare_fresh_run] Membersihkan sisa run sebelumnya...")
    total_deleted = 0

    for ch in channels:
        ch_id = ch.get("id", "unknown")
        for sub in _CLEAN_SUBDIRS:
            try:
                folder = channel_data_path(ch_id, sub)
            except Exception:
                continue

            if not os.path.isdir(folder):
                continue

            deleted = 0
            for item in glob.glob(os.path.join(folder, "*")):
                try:
                    if os.path.isfile(item) or os.path.islink(item):
                        os.remove(item)
                        deleted += 1
                    elif os.path.isdir(item):
                        shutil.rmtree(item)
                        deleted += 1
                except OSError as exc:
                    logger.warning(f"[prepare_fresh_run] Gagal hapus {item}: {exc}")

            if deleted:
                logger.info(f"  [{ch_id}] {sub}/ → {deleted} item dihapus")
            total_deleted += deleted

    logger.info(f"🧹 [prepare_fresh_run] Selesai — total {total_deleted} item dibersihkan")


def run_once(channel: dict, profile: str = "shorts",
             dry_run: bool = False,
             publish_at_override: str = None,
             skip_qc: bool = False,
             topic_override: str = None) -> bool:
    ch_id  = channel["id"]
    run_id = None

    try:
        logger.info(f"\n{'='*50}")
        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 1/8 — Topic")
        if topic_override:
            logger.info(f"[{ch_id}] 🎯 TOPIC OVERRIDE: {topic_override}")
            topic_data = {
                "topic": topic_override,
                "niche": channel.get("niche", "horror_facts"),
                "language": channel.get("language", "id"),
                "is_viral_iteration": False,
            }
        else:
            topic_data = topic_engine.generate(channel, profile=profile)

        run_id = state_manager.create_run(ch_id, topic_data["topic"], profile=profile)

        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 2/8 — Script")
        script_data = script_engine.generate(topic_data, channel, profile=profile)

        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 3/8 — QC Script")
        qc_engine.check(script_data, channel, profile=profile)

        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 4/8 — TTS")
        audio_path = tts_engine.generate(script_data, channel)

        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 5/8 — Footage")
        footage_paths = footage_engine.fetch(script_data, channel, profile=profile)

        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 6/8 — Render")
        video_path = video_engine.render(script_data, audio_path, footage_paths, channel, profile=profile)

        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 7/8 — Thumbnail & Metadata")
        thumbnail_path = thumbnail_engine.generate(script_data, video_path, channel, profile=profile)
        metadata       = metadata_engine.generate(script_data, channel, profile=profile)

        if publish_at_override:
            metadata["publish_at"] = publish_at_override
            logger.info(f"[{ch_id}] publish_at → {publish_at_override}")

        # ── QC Vision (Gemini) ───────────────────────────────────────────────
        if not skip_qc:
            logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 7.5/8 — QC Vision")
            video_path, thumbnail_path = _run_qc_vision(
                video_path, thumbnail_path, script_data, channel, profile
            )
        else:
            logger.info(f"[{ch_id}] QC Vision di-skip (--skip-qc)")

        # ── Upload ke GDrive ─────────────────────────────────────────────────
        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 8/8 — Upload ke GDrive")
        if dry_run:
            logger.info(f"[{ch_id}] DRY RUN — skip upload GDrive")
            state_manager.update_run(run_id, status="dry_run")
            return True

        folder_ts = gdrive_engine.upload_to_queue(video_path, thumbnail_path, metadata, channel)
        state_manager.update_run(run_id, status="queued_gdrive")
        logger.info(f"[{ch_id}] ✅ [{profile.upper()}] masuk GDrive queue → {folder_ts}\n")
        return True

    except Exception as e:
        logger.debug(traceback.format_exc())
        if run_id:
            state_manager.mark_failed(run_id, str(e))
        logger.error(f"[{ch_id}] [{profile.upper()}] FAILED: {e}")
        return False


def _run_qc_vision(video_path: str, thumbnail_path: str,
                   script_data: dict, channel: dict, profile: str):
    """
    Jalankan QC Vision dan auto-fix kalau perlu.
    Return: (video_path, thumbnail_path) — mungkin sudah diganti versi yang di-fix.
    """
    try:
        from engine.qc_vision_engine import review_video, auto_fix

        qc_result = review_video(video_path, thumbnail_path, script_data, channel, profile)

        status = qc_result.get("status", "APPROVED")
        score  = qc_result.get("score", 8)
        logger.info(f"[{channel['id']}] QC Score: {score}/10 | Status: {status}")

        if status == "APPROVED":
            logger.info(f"[{channel['id']}] ✅ QC APPROVED — video langsung diupload")
            return video_path, thumbnail_path

        # NEEDS_FIX: auto-fix 1x lalu langsung upload
        logger.info(f"[{channel['id']}] ⚠️  QC NEEDS_FIX — auto-fix sekarang...")
        issues = qc_result.get("issues", [])
        for issue in issues:
            logger.info(f"  [{issue.get('severity','?')}] {issue.get('aspect')}: {issue.get('problem')}")
            logger.info(f"   → {issue.get('suggestion')}")

        fix_result     = auto_fix(video_path, thumbnail_path, qc_result, channel, profile)
        fixed_video    = fix_result.get("video_path", video_path)
        fixed_thumb    = fix_result.get("thumbnail_path", thumbnail_path)
        fixes_applied  = fix_result.get("fixes_applied", [])

        if fixes_applied:
            logger.info(f"[{channel['id']}] ✅ Auto-fix diterapkan: {fixes_applied}")
        else:
            logger.info(f"[{channel['id']}] Tidak ada fix yang bisa diterapkan otomatis — upload versi original")

        return fixed_video, fixed_thumb

    except Exception as e:
        logger.warning(f"QC Vision error: {e} — skip QC, upload versi original")
        return video_path, thumbnail_path

# ── BARU: Fungsi Analytics ───────────────────────────────────────────────────
def run_daily_analytics():
    settings = load_settings()
    for channel in settings.get("channels", []):
        if channel.get("active"):
            logger.info(f"[{channel['id']}] Fetching retention analytics...")
            analyze_channel(channel)


def _infer_channel_id_from_script_path(script_path: str) -> str | None:
    parts = os.path.normpath(script_path).split(os.sep)
    if "data" in parts:
        idx = parts.index("data")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def review_script_file(script_path: str, channel_id: str | None = None) -> str:
    settings = load_settings()
    ch_map = {ch["id"]: ch for ch in settings.get("channels", [])}
    inferred_channel_id = channel_id or _infer_channel_id_from_script_path(script_path)
    channel = ch_map.get(inferred_channel_id or "")

    if not channel:
        raise ValueError(
            "Channel untuk review script tidak ditemukan. "
            "Gunakan --channel atau simpan file di data/<channel_id>/scripts/..."
        )

    script_data = load_json(script_path)
    profile = script_data.get("profile", "shorts")
    logger.info(f"[{channel['id']}] Review script file: {script_path}")
    reviewed = script_engine.review_and_iterate(script_data, channel, profile=profile)

    base, ext = os.path.splitext(script_path)
    out_path = f"{base}_reviewed{ext}"
    out_path = _save_review_payload(reviewed, out_path, channel["id"], "reviewed")

    review_meta = reviewed.get("review_meta", {})
    logger.info(
        f"[{channel['id']}] Review done | initial={review_meta.get('initial_score')} "
        f"| final={review_meta.get('final_score')} | rewritten={review_meta.get('rewritten')}"
    )
    if out_path:
        logger.info(f"[{channel['id']}] Reviewed script saved: {out_path}")
    else:
        logger.warning(f"[{channel['id']}] Reviewed script tidak bisa disimpan di environment ini")
    return out_path


def review_hook_file(script_path: str, channel_id: str | None = None) -> str:
    settings = load_settings()
    ch_map = {ch["id"]: ch for ch in settings.get("channels", [])}
    inferred_channel_id = channel_id or _infer_channel_id_from_script_path(script_path)
    channel = ch_map.get(inferred_channel_id or "")

    if not channel:
        raise ValueError(
            "Channel untuk review hook tidak ditemukan. "
            "Gunakan --channel atau simpan file di data/<channel_id>/scripts/..."
        )

    script_data = load_json(script_path)
    profile = script_data.get("profile", "shorts")
    logger.info(f"[{channel['id']}] Review hook file: {script_path}")
    reviewed = script_engine.review_hook_only(script_data, channel, profile=profile)

    base, ext = os.path.splitext(script_path)
    out_path = f"{base}_hooked{ext}"
    out_path = _save_review_payload(reviewed, out_path, channel["id"], "hooked")

    hook_meta = reviewed.get("hook_meta", {})
    logger.info(
        f"[{channel['id']}] Hook review done | total={hook_meta.get('score')} "
        f"| 0-3s={hook_meta.get('hook_score_0_3')} "
        f"| 4-10s={hook_meta.get('anchor_score_4_10')} "
        f"| rewritten={hook_meta.get('rewritten')}"
    )
    if out_path:
        logger.info(f"[{channel['id']}] Hook-reviewed script saved: {out_path}")
    else:
        logger.warning(f"[{channel['id']}] Hook-reviewed script tidak bisa disimpan di environment ini")
    return out_path


def _save_review_payload(data: dict, preferred_path: str, channel_id: str, suffix: str) -> str:
    try:
        save_json(data, preferred_path)
        return preferred_path
    except PermissionError:
        try:
            fallback_dir = channel_data_path(channel_id, "scripts")
            fallback_name = f"{timestamp()}_{suffix}.json"
            fallback_path = os.path.join(fallback_dir, fallback_name)
            save_json(data, fallback_path)
            logger.warning(
                f"[{channel_id}] Path review default terkunci, simpan fallback ke {fallback_path}"
            )
            return fallback_path
        except PermissionError:
            logger.warning(
                f"[{channel_id}] Tidak bisa menulis file review di environment ini. "
                "Hasil audit tetap terlihat di log."
            )
            return ""

# ─── Campaign batch render ────────────────────────────────────────────────────

def run_campaign(target_channel_id=None, dry_run=False, skip_qc=False):
    """
    Render batch berdasarkan campaigns.json.
    Proteksi double booking: slot yang sudah dirender di-skip otomatis.
    """
    from engine.campaign_engine import build_render_queue, book_slot

    state_manager.init_db()
    queue = build_render_queue()

    if target_channel_id:
        queue = [item for item in queue if item["channel_id"] == target_channel_id]

    if not queue:
        logger.info("✅ Tidak ada video yang perlu dirender saat ini.")
        logger.info("   Tips: python main.py --preview  → lihat status slot")
        logger.info("         Edit config/campaigns.json untuk tambah/aktifkan campaign")
        return

    settings  = load_settings()
    ch_map    = {ch["id"]: ch for ch in settings.get("channels", [])}

    # ── UPGRADE: Auto-Cleanup sebelum batch render dimulai ──────────────────
    active_channels = [
        ch for ch in settings.get("channels", [])
        if target_channel_id is None or ch["id"] == target_channel_id
    ]
    prepare_fresh_run(active_channels, dry_run=dry_run)
    # ────────────────────────────────────────────────────────────────────────

    logger.info(f"\n{'#'*60}")
    logger.info(f"# Batch render: {len(queue)} video")
    if skip_qc:
        logger.info("# QC Vision: DINONAKTIFKAN (--skip-qc)")
    logger.info(f"{'#'*60}")

    ok_count         = 0
    fail_count       = 0
    consecutive_fail = 0

    for i, item in enumerate(queue, 1):
        ch_id   = item["channel_id"]
        profile = item["profile"]
        slot    = item["publish_at_utc"]
        channel = ch_map.get(ch_id)

        if not channel:
            logger.warning(f"Channel '{ch_id}' tidak ada di settings.json, skip")
            continue

        logger.info(
            f"\n[{i}/{len(queue)}] [{ch_id}] {profile.upper()} "
            f"→ {item['publish_date']} jam {item['publish_hour_wib']}:00 WIB"
        )

        success = run_once(
            channel,
            profile=profile,
            dry_run=dry_run,
            publish_at_override=slot,
            skip_qc=skip_qc,
        )

        if success:
            ok_count         += 1
            consecutive_fail  = 0
            if not dry_run:
                book_slot(ch_id, slot)
        else:
            fail_count       += 1
            consecutive_fail += 1
            if consecutive_fail >= 3:
                logger.error("3 video gagal berturut-turut, hentikan batch render.")
                break

    logger.info(f"\n{'='*50}")
    logger.info(f"Batch selesai: {ok_count} ✅ berhasil | {fail_count} ❌ gagal")
    if ok_count > 0:
        logger.info("Video sudah masuk GDrive queue → Koyeb akan upload sesuai jadwal")
    logger.info(f"{'='*50}\n")

    logger.info("🧹 Menjalankan cleanup file lama...")
    cleanup_engine.run(dry_run=dry_run)


# ─── Legacy mode ─────────────────────────────────────────────────────────────

def run_channel(channel: dict, dry_run: bool = False, skip_qc: bool = False):
    ch_id    = channel["id"]
    settings = load_settings()
    plan     = settings.get("daily_plan", {"shorts": 1, "long_form": 0})

    n_shorts    = plan.get("shorts", 1)
    n_long_form = plan.get("long_form", 0)

    logger.info(f"\n{'#'*60}")
    logger.info(f"# Channel: {channel['name']} ({ch_id})")
    logger.info(f"# Plan: {n_shorts} shorts + {n_long_form} long-form")
    logger.info(f"{'#'*60}")

    shorts_ok   = 0
    shorts_fail = 0

    consecutive_fail = 0
    for i in range(n_shorts):
        success = run_once(channel, profile="shorts", dry_run=dry_run, skip_qc=skip_qc)
        if success:
            shorts_ok += 1
            consecutive_fail = 0
        else:
            shorts_fail += 1
            consecutive_fail += 1
            if consecutive_fail >= 3:
                logger.error(f"[{ch_id}] 3 Shorts gagal berturut-turut, skip channel.")
                break

    long_ok = 0
    if n_long_form > 0:
        success = run_once(channel, profile="long_form", dry_run=dry_run, skip_qc=skip_qc)
        long_ok = 1 if success else 0

    logger.info(f"[{ch_id}] Selesai: {shorts_ok} shorts ✅ | {shorts_fail} shorts ❌ | {long_ok} long-form ✅")


def run_all_channels(target_channel_id=None, dry_run=False, skip_qc=False):
    settings = load_settings()
    channels = [
        ch for ch in settings["channels"]
        if ch.get("active", True)
        and (target_channel_id is None or ch["id"] == target_channel_id)
    ]

    if not channels:
        logger.warning("Tidak ada channel aktif.")
        return

    logger.info(f"Menjalankan pipeline untuk {len(channels)} channel aktif")
    state_manager.init_db()

    # ── UPGRADE: Auto-Cleanup sebelum batch render dimulai ──────────────────
    prepare_fresh_run(channels, dry_run=dry_run)
    # ────────────────────────────────────────────────────────────────────────

    for channel in channels:
        run_channel(channel, dry_run=dry_run, skip_qc=skip_qc)

    logger.info("🧹 Menjalankan cleanup file lama...")
    cleanup_engine.run(dry_run=dry_run)

    logger.info("\n✅ Semua pipeline selesai! Video masuk GDrive queue.")
    logger.info("   Koyeb akan upload ke YouTube sesuai jadwal.")


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Auto Content Machine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Contoh testing:\n"
            "  python main.py --channel ch_horror_id --topic \"Eksperimen Tidur Rusia\" --dry-run --debug\n"
            "  python main.py --channel ch_id_horror --topic \"Kaset ritual terlarang\" --script-only\n"
            "  python main.py --review-hook data\\ch_id_horror\\scripts\\file.json --channel ch_id_horror"
        )
    )
    parser.add_argument("--channel",  help="Jalankan 1 channel saja (by id)")
    parser.add_argument("--dry-run",  action="store_true", help="Test tanpa upload ke GDrive")
    parser.add_argument("--preview",  action="store_true", help="Lihat status slot campaign")
    parser.add_argument("--skip-qc",  action="store_true", help="Skip QC Vision (lebih cepat)")
    parser.add_argument("--legacy",   action="store_true", help="Pakai daily_plan di settings.json")
    parser.add_argument("--analytics",action="store_true", help="Jalankan analytics harian untuk retention")
    parser.add_argument("--topic",    help="Inject topik manual (bypass topic_engine, untuk testing)")
    parser.add_argument("--review-script", help="Nilai dan iterasi file script JSON yang sudah ada")
    parser.add_argument("--review-hook", help="Audit dan upgrade hook saja dari file script JSON yang sudah ada")
    parser.add_argument("--script-only", action="store_true", help="Generate script saja tanpa TTS/render/upload")
    parser.add_argument("--debug",    action="store_true", help="Enable verbose debug logging")
    parser.add_argument("--profile",  choices=["shorts", "long_form", "all"],
                        default="all", help="Render profile tertentu (legacy mode)")
    args = parser.parse_args()

    # ── Debug mode: set root logger ke DEBUG ──────────────────────────────────
    if args.debug:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("🔍 DEBUG MODE AKTIF — verbose logging enabled")

    if args.dry_run:
        logger.info("⚠️  DRY RUN MODE — tidak akan upload ke GDrive")

    if args.topic and not args.channel:
        parser.error("--topic membutuhkan --channel juga. Contoh: --channel ch_horror_id --topic \"Topik\"")

    if args.analytics:
        logger.info("Mode: Daily Analytics")
        run_daily_analytics()

    elif args.preview:
        from engine.campaign_engine import preview_campaign
        preview_campaign()

    elif args.review_script:
        logger.info(f"Mode: Review Script â†’ {args.review_script}")
        review_script_file(args.review_script, channel_id=args.channel)

    elif args.review_hook:
        logger.info(f"Mode: Review Hook -> {args.review_hook}")
        review_hook_file(args.review_hook, channel_id=args.channel)

    elif args.topic:
        # ── Mode: Single test run dengan topik manual ─────────────────────────
        logger.info(f"Mode: Manual Topic Test → channel={args.channel} topic='{args.topic}'")
        settings = load_settings()
        ch_map   = {ch["id"]: ch for ch in settings.get("channels", [])}
        channel  = ch_map.get(args.channel)
        if not channel:
            logger.error(f"Channel '{args.channel}' tidak ditemukan di settings.json")
            raise SystemExit(1)
        state_manager.init_db()
        profile = args.profile if args.profile != "all" else "shorts"
        if args.script_only:
            topic_data = {
                "topic": args.topic,
                "niche": channel.get("niche", "horror_facts"),
                "language": channel.get("language", "id"),
                "is_viral_iteration": False,
                "topic_source": "manual_topic_test",
            }
            script_data = script_engine.generate(topic_data, channel, profile=profile)
            success = bool(script_data.get("script_path"))
            logger.info(f"[{channel['id']}] Script-only selesai -> {script_data.get('script_path')}")
        else:
            success = run_once(
                channel,
                profile=profile,
                dry_run=args.dry_run,
                skip_qc=args.skip_qc,
                topic_override=args.topic,
            )
        logger.info("✅ Test selesai" if success else "❌ Test gagal")

    elif args.legacy:
        logger.info("Mode: Legacy (daily_plan dari settings.json)")
        run_all_channels(
            target_channel_id=args.channel,
            dry_run=args.dry_run,
            skip_qc=args.skip_qc,
        )

    else:
        logger.info("Mode: Campaign (dari campaigns.json)")
        run_campaign(
            target_channel_id=args.channel,
            dry_run=args.dry_run,
            skip_qc=args.skip_qc,
        )
