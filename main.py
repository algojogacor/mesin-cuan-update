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
import concurrent.futures
import glob
import os
import shutil
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from engine.utils import get_logger, load_settings, channel_data_path, load_json, save_json, timestamp
from engine import (
    topic_engine, script_engine, qc_engine,
    tts_engine, footage_engine, video_engine,
    thumbnail_engine, metadata_engine, gdrive_engine,
    state_manager, notif_engine, cleanup_engine,
    series_engine
)
from engine.pipeline_estimator import SmartETAEstimator, format_eta, PIPELINE_STAGES

# ── Import untuk analytics harian ────────────────────────────────────────────
from engine.retention_engine import analyze_channel

load_dotenv()
logger = get_logger("main")


# ─── Auto-Cleanup (Disk Management) ──────────────────────────────────────────

_CLEAN_SUBDIRS = ("audio", "footage", "temp")
PIPELINE_STAGE_WORKERS = {
    "script": 1,
    "prep": 1,
    "render": 1,
    "post": 1,
}


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


def _create_pipeline_job(batch_index: int, batch_total: int, channel: dict, profile: str,
                         publish_at_override: str = None, topic_override: str = None,
                         slot_item: dict | None = None) -> dict:
    return {
        "job_id": f"{channel['id']}:{profile}:{timestamp()}:{batch_index}",
        "batch_index": batch_index,
        "batch_total": batch_total,
        "channel": channel,
        "profile": profile,
        "publish_at_override": publish_at_override,
        "topic_override": topic_override,
        "slot_item": slot_item or {},
        "status": "pending",
        "current_stage": None,
        "stage_started_at": None,
        "completed_stages": [],
        "stage_durations": {},
        "topic_data": None,
        "script_data": None,
        "audio_path": None,
        "footage_paths": None,
        "transcript_sentences": None,
        "video_path": None,
        "thumbnail_path": None,
        "metadata": None,
        "run_id": None,
        "created_at": time.time(),
    }


def _job_label(job: dict) -> str:
    ch_id = job["channel"]["id"]
    profile = job["profile"].upper()
    return f"[{job['batch_index']}/{job['batch_total']}] [{ch_id}] [{profile}]"


def _next_stage(job: dict) -> str | None:
    if job.get("current_stage"):
        return job["current_stage"]
    completed = set(job.get("completed_stages", []))
    for stage in PIPELINE_STAGES:
        if stage not in completed:
            return stage
    return None


def _log_eta_snapshot(estimator: SmartETAEstimator, jobs: list[dict], full: bool = False) -> None:
    snapshot = estimator.build_snapshot(jobs, workers=PIPELINE_STAGE_WORKERS)
    total = snapshot["total"]
    completed_jobs = len([job for job in jobs if job.get("status") == "completed"])
    failed_jobs = len([job for job in jobs if job.get("status") == "failed"])

    logger.info(
        "[ETA][BATCH] progress=%s/%s selesai | gagal=%s | cepat=%s | kemungkinan=%s | lama=%s",
        completed_jobs,
        len(jobs),
        failed_jobs,
        format_eta(total["fast"]),
        format_eta(total["likely"]),
        format_eta(total["slow"]),
    )

    if full:
        jobs_to_log = [
            job for job in jobs
            if job.get("status") not in ("completed", "failed")
        ]
    else:
        jobs_to_log = [
            job for job in jobs
            if job.get("current_stage") and job.get("status") == "running"
        ]
        if not jobs_to_log:
            next_job = next(
                (
                    job for job in sorted(jobs, key=lambda item: item["batch_index"])
                    if job.get("status") not in ("completed", "failed")
                ),
                None,
            )
            jobs_to_log = [next_job] if next_job else []

    for job in jobs_to_log:
        if not job:
            continue
        eta = snapshot["per_video"].get(job["job_id"], {})
        logger.info(
            "[ETA][VIDEO] %s stage=%s | cepat=%s | kemungkinan=%s | lama=%s",
            _job_label(job),
            _next_stage(job) or "-",
            format_eta(eta.get("fast")),
            format_eta(eta.get("likely")),
            format_eta(eta.get("slow")),
        )


def _mark_stage_started(job: dict, stage: str) -> None:
    job["status"] = "running"
    job["current_stage"] = stage
    job["stage_started_at"] = time.time()
    logger.info(f"{_job_label(job)} ▶ {stage.upper()} mulai")


def _mark_stage_finished(job: dict, stage: str, estimator: SmartETAEstimator | None,
                         success: bool = True) -> float:
    ended_at = time.time()
    started_at = float(job.get("stage_started_at") or ended_at)
    duration = max(0.001, ended_at - started_at)
    job["stage_started_at"] = None
    job["current_stage"] = None
    job["stage_durations"][stage] = duration

    if success and stage not in job["completed_stages"]:
        job["completed_stages"].append(stage)
        if estimator is not None:
            estimator.record_stage_duration(job["profile"], stage, duration)

    if success:
        logger.info(f"{_job_label(job)} ✓ {stage.upper()} selesai dalam {format_eta(duration)}")
    else:
        logger.warning(f"{_job_label(job)} ✗ {stage.upper()} berhenti setelah {format_eta(duration)}")
    return duration


def _apply_stage_result(job: dict, result: dict | None) -> None:
    if not result:
        return
    for key, value in result.items():
        job[key] = value


def _is_ready_for_stage(job: dict, stage: str) -> bool:
    if job.get("status") in ("completed", "failed"):
        return False
    if job.get("current_stage") is not None:
        return False
    completed = set(job.get("completed_stages", []))
    if stage in completed:
        return False

    stage_index = PIPELINE_STAGES.index(stage)
    required = PIPELINE_STAGES[:stage_index]
    return all(prev in completed for prev in required)


def _run_stage_script(job: dict) -> dict:
    channel = job["channel"]
    profile = job["profile"]
    ch_id = channel["id"]
    topic_override = job.get("topic_override")

    logger.info(f"{_job_label(job)} STEP 1/4 — Topic + Script")
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
    job["run_id"] = run_id

    script_data = script_engine.generate(topic_data, channel, profile=profile)
    qc_engine.check(script_data, channel, profile=profile)
    state_manager.update_run(
        run_id,
        status="script_ready",
        script_path=script_data.get("script_path", ""),
    )
    return {
        "topic_data": topic_data,
        "script_data": script_data,
    }


def _run_stage_prep(job: dict) -> dict:
    channel = job["channel"]
    profile = job["profile"]
    ch_id = channel["id"]
    script_data = job["script_data"]

    logger.info(f"{_job_label(job)} STEP 2/4 — Prep (TTS + Footage + Whisper)")
    audio_path = tts_engine.generate(script_data, channel)

    def _fetch_footage():
        return footage_engine.fetch(script_data, channel, profile=profile)

    def _prepare_transcript():
        try:
            return video_engine.prepare_transcript(audio_path, profile=profile)
        except Exception as exc:
            logger.error(f"[{ch_id}] Whisper prep gagal: {exc}")
            return []

    with ThreadPoolExecutor(max_workers=2) as executor:
        fut_footage = executor.submit(_fetch_footage)
        fut_transcript = executor.submit(_prepare_transcript)
        footage_paths = fut_footage.result()
        transcript_sentences = fut_transcript.result()

    if job.get("run_id"):
        state_manager.update_run(
            job["run_id"],
            status="prep_ready",
            audio_path=audio_path,
        )

    return {
        "audio_path": audio_path,
        "footage_paths": footage_paths,
        "transcript_sentences": transcript_sentences,
    }


def _run_stage_render(job: dict) -> dict:
    logger.info(f"{_job_label(job)} STEP 3/4 — Render")
    video_path = video_engine.render(
        job["script_data"],
        job["audio_path"],
        job["footage_paths"],
        job["channel"],
        profile=job["profile"],
        transcript_sentences=job.get("transcript_sentences"),
    )

    if job.get("run_id"):
        state_manager.update_run(
            job["run_id"],
            status="rendered",
            video_path=video_path,
        )

    return {"video_path": video_path}


def _run_stage_post(job: dict, dry_run: bool = False, skip_qc: bool = False) -> dict:
    ch_id = job["channel"]["id"]
    profile = job["profile"]
    script_data = job["script_data"]
    video_path = job["video_path"]
    channel = job["channel"]

    logger.info(f"{_job_label(job)} STEP 4/4 — Post (Thumbnail + Metadata + QC + Upload)")

    def _run_thumbnail():
        try:
            path = thumbnail_engine.generate(script_data, video_path, channel, profile=profile)
            logger.info(f"[{ch_id}] [POST] Thumbnail selesai: {os.path.basename(path)}")
            return path
        except Exception as exc:
            logger.error(f"[{ch_id}] Thumbnail gagal ({exc}), lanjut tanpa thumbnail")
            return None

    def _run_metadata():
        try:
            meta = metadata_engine.generate(script_data, channel, profile=profile)
            logger.info(f"[{ch_id}] [POST] Metadata selesai")
            return meta
        except Exception as exc:
            logger.error(f"[{ch_id}] Metadata gagal ({exc}), gunakan metadata minimal")
            return {"title": script_data.get("title", ""), "description": "", "tags": []}

    with ThreadPoolExecutor(max_workers=2) as executor:
        fut_thumb = executor.submit(_run_thumbnail)
        fut_meta = executor.submit(_run_metadata)
        thumbnail_path = fut_thumb.result()
        metadata = fut_meta.result()

    publish_at_override = job.get("publish_at_override")
    if publish_at_override:
        metadata["publish_at"] = publish_at_override
        logger.info(f"[{ch_id}] publish_at → {publish_at_override}")

    final_video_path = video_path
    final_thumbnail_path = thumbnail_path
    if not skip_qc:
        final_video_path, final_thumbnail_path = _run_qc_vision(
            video_path, thumbnail_path, script_data, channel, profile
        )
    else:
        logger.info(f"[{ch_id}] QC Vision di-skip (--skip-qc)")

    if dry_run:
        logger.info(f"[{ch_id}] DRY RUN — skip upload GDrive")
        if job.get("run_id"):
            state_manager.update_run(
                job["run_id"],
                status="dry_run",
                thumbnail_path=final_thumbnail_path or "",
                video_path=final_video_path or "",
            )
        return {
            "thumbnail_path": final_thumbnail_path,
            "metadata": metadata,
            "video_path": final_video_path,
            "upload_folder": None,
        }

    folder_ts = gdrive_engine.upload_to_queue(final_video_path, final_thumbnail_path, metadata, channel)
    if job.get("run_id"):
        state_manager.update_run(
            job["run_id"],
            status="queued_gdrive",
            thumbnail_path=final_thumbnail_path or "",
            video_path=final_video_path or "",
        )

    logger.info(f"[{ch_id}] ✅ [{profile.upper()}] masuk GDrive queue → {folder_ts}")
    return {
        "thumbnail_path": final_thumbnail_path,
        "metadata": metadata,
        "video_path": final_video_path,
        "upload_folder": folder_ts,
    }


def _run_job_sequential(job: dict, estimator: SmartETAEstimator | None,
                        dry_run: bool = False, skip_qc: bool = False) -> bool:
    try:
        stage_handlers = {
            "script": lambda item: _run_stage_script(item),
            "prep": lambda item: _run_stage_prep(item),
            "render": lambda item: _run_stage_render(item),
            "post": lambda item: _run_stage_post(item, dry_run=dry_run, skip_qc=skip_qc),
        }

        for stage in PIPELINE_STAGES:
            _mark_stage_started(job, stage)
            result = stage_handlers[stage](job)
            _apply_stage_result(job, result)
            _mark_stage_finished(job, stage, estimator=estimator, success=True)

        job["status"] = "completed"
        return True

    except Exception as exc:
        logger.debug(traceback.format_exc())
        if job.get("current_stage"):
            _mark_stage_finished(job, job["current_stage"], estimator=None, success=False)
        job["status"] = "failed"
        if job.get("run_id"):
            state_manager.mark_failed(job["run_id"], str(exc))
        logger.error(f"{_job_label(job)} FAILED: {exc}")
        return False


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

        # ══ OPTIMASI 3: Thumbnail generation paralel dengan Metadata generation ══
        # Thumbnail dan metadata tidak saling bergantung — keduanya hanya butuh
        # script_data, channel, dan video_path yang sudah tersedia setelah render.
        # Thumbnail: CPU-only (PIL + FFmpeg frame extract) — tidak pakai GPU render.
        # Metadata: pure I/O / string processing — sangat ringan.
        # Jalankan paralel untuk mempersingkat step 7 sebelum QC dan upload.
        logger.info(f"[{ch_id}] [{profile.upper()}] ▶ STEP 7/8 — Thumbnail & Metadata [PARALEL-3]")

        def _run_thumbnail():
            try:
                path = thumbnail_engine.generate(script_data, video_path, channel, profile=profile)
                logger.info(f"[{ch_id}] [PARALEL-3] Thumbnail selesai: {os.path.basename(path)}")
                return path
            except Exception as exc:
                logger.error(f"[{ch_id}] Thumbnail gagal ({exc}), lanjut tanpa thumbnail")
                return None  # Non-fatal: upload tetap jalan, thumbnail dikosongkan

        def _run_metadata():
            try:
                meta = metadata_engine.generate(script_data, channel, profile=profile)
                logger.info(f"[{ch_id}] [PARALEL-3] Metadata selesai")
                return meta
            except Exception as exc:
                logger.error(f"[{ch_id}] Metadata gagal ({exc}), gunakan metadata minimal")
                return {"title": script_data.get("title", ""), "description": "", "tags": []}

        with ThreadPoolExecutor(max_workers=2) as executor:
            fut_thumb = executor.submit(_run_thumbnail)
            fut_meta  = executor.submit(_run_metadata)
            thumbnail_path = fut_thumb.result()   # Tunggu thumbnail selesai
            metadata       = fut_meta.result()    # Tunggu metadata selesai
        logger.info(f"[{ch_id}] [PARALEL-3] Thumbnail + Metadata selesai")

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


def run_once(channel: dict, profile: str = "shorts",
             dry_run: bool = False,
             publish_at_override: str = None,
             skip_qc: bool = False,
             topic_override: str = None) -> bool:
    """Override run_once lama dengan versi berbasis stage helper."""
    logger.info(f"\n{'='*50}")
    job = _create_pipeline_job(
        batch_index=1,
        batch_total=1,
        channel=channel,
        profile=profile,
        publish_at_override=publish_at_override,
        topic_override=topic_override,
    )
    return _run_job_sequential(job, estimator=SmartETAEstimator(), dry_run=dry_run, skip_qc=skip_qc)


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

    settings  = load_settings()
    ch_map    = {ch["id"]: ch for ch in settings.get("channels", [])}

    # ── Auto-Cleanup sebelum batch render dimulai ──────────────────────────
    active_channels = [
        ch for ch in settings.get("channels", [])
        if target_channel_id is None or ch["id"] == target_channel_id
    ]
    prepare_fresh_run(active_channels, dry_run=dry_run)

    # ── Series Engine: antri episode series & Part 2 viral sebelum render ──
    if not dry_run:
        for ch in active_channels:
            try:
                series_engine.check_and_queue_parts(ch)
            except Exception as exc:
                logger.warning(f"[{ch['id']}] series_engine error (skip): {exc}")

    if not queue:
        logger.info("✅ Tidak ada video yang perlu dirender saat ini.")
        logger.info("   Tips: python main.py --preview  → lihat status slot")
        logger.info("         Edit config/campaigns.json untuk tambah/aktifkan campaign")
        return

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

def _submit_ready_pipeline_stages(jobs: list[dict], executors: dict[str, ThreadPoolExecutor],
                                  active_futures: dict, dry_run: bool, skip_qc: bool) -> int:
    submitted = 0
    stage_runners = {
        "script": lambda job: _run_stage_script(job),
        "prep": lambda job: _run_stage_prep(job),
        "render": lambda job: _run_stage_render(job),
        "post": lambda job: _run_stage_post(job, dry_run=dry_run, skip_qc=skip_qc),
    }

    for stage in reversed(PIPELINE_STAGES):
        active_count = sum(1 for meta in active_futures.values() if meta["stage"] == stage)
        capacity = max(0, PIPELINE_STAGE_WORKERS.get(stage, 1) - active_count)
        if capacity <= 0:
            continue

        for job in sorted(jobs, key=lambda item: item["batch_index"]):
            if capacity <= 0:
                break
            if not _is_ready_for_stage(job, stage):
                continue

            _mark_stage_started(job, stage)
            future = executors[stage].submit(stage_runners[stage], job)
            active_futures[future] = {"stage": stage, "job": job}
            capacity -= 1
            submitted += 1

    return submitted


def run_campaign(target_channel_id=None, dry_run=False, skip_qc=False):
    """
    Override run_campaign lama dengan pipeline antar-video:
    script -> prep -> render -> post.
    """
    from engine.campaign_engine import build_render_queue, book_slot

    state_manager.init_db()
    settings = load_settings()
    ch_map = {ch["id"]: ch for ch in settings.get("channels", [])}

    active_channels = [
        ch for ch in settings.get("channels", [])
        if target_channel_id is None or ch["id"] == target_channel_id
    ]

    prepare_fresh_run(active_channels, dry_run=dry_run)

    if not dry_run:
        for ch in active_channels:
            try:
                series_engine.check_and_queue_parts(ch)
            except Exception as exc:
                logger.warning(f"[{ch['id']}] series_engine error (skip): {exc}")

    queue = build_render_queue()
    if target_channel_id:
        queue = [item for item in queue if item["channel_id"] == target_channel_id]

    if not queue:
        logger.info("✅ Tidak ada video yang perlu dirender saat ini.")
        logger.info("   Tips: python main.py --preview  -> lihat status slot")
        logger.info("         Edit config/campaigns.json untuk tambah/aktifkan campaign")
        return

    jobs: list[dict] = []
    total_slots = len(queue)
    for i, item in enumerate(queue, 1):
        ch_id = item["channel_id"]
        channel = ch_map.get(ch_id)
        if not channel:
            logger.warning(f"Channel '{ch_id}' tidak ada di settings.json, skip")
            continue

        job = _create_pipeline_job(
            batch_index=i,
            batch_total=total_slots,
            channel=channel,
            profile=item["profile"],
            publish_at_override=item["publish_at_utc"],
            slot_item=item,
        )
        jobs.append(job)
        logger.info(
            "%s queued -> %s jam %s:00 WIB",
            _job_label(job),
            item["publish_date"],
            item["publish_hour_wib"],
        )

    if not jobs:
        logger.info("✅ Tidak ada job valid yang bisa dijalankan.")
        return

    logger.info(f"\n{'#'*60}")
    logger.info(f"# Smart pipeline batch: {len(jobs)} video")
    logger.info("# Lane: script=1 | prep=1 | render=1 | post=1")
    if skip_qc:
        logger.info("# QC Vision: DINONAKTIFKAN (--skip-qc)")
    logger.info(f"{'#'*60}")

    estimator = SmartETAEstimator()
    executors = {
        stage: ThreadPoolExecutor(max_workers=PIPELINE_STAGE_WORKERS[stage])
        for stage in PIPELINE_STAGES
    }
    active_futures: dict = {}
    last_eta_log = 0.0

    try:
        _log_eta_snapshot(estimator, jobs, full=True)
        last_eta_log = time.time()

        while True:
            _submit_ready_pipeline_stages(jobs, executors, active_futures, dry_run=dry_run, skip_qc=skip_qc)

            unfinished = [job for job in jobs if job.get("status") not in ("completed", "failed")]
            if not unfinished and not active_futures:
                break

            if not active_futures:
                logger.warning("Pipeline berhenti karena tidak ada stage aktif dan tidak ada job yang bisa dijadwalkan.")
                break

            done, _ = concurrent.futures.wait(
                list(active_futures.keys()),
                timeout=1.0,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            if not done:
                now = time.time()
                if now - last_eta_log >= 60:
                    _log_eta_snapshot(estimator, jobs)
                    last_eta_log = now
                continue

            for future in done:
                meta = active_futures.pop(future)
                stage = meta["stage"]
                job = meta["job"]

                try:
                    result = future.result()
                    _apply_stage_result(job, result)
                    _mark_stage_finished(job, stage, estimator=estimator, success=True)

                    if stage == "post":
                        job["status"] = "completed"
                        if not dry_run:
                            slot = job.get("publish_at_override")
                            if slot:
                                book_slot(job["channel"]["id"], slot)
                    else:
                        job["status"] = "pending"

                except Exception as exc:
                    logger.debug(traceback.format_exc())
                    if job.get("current_stage"):
                        _mark_stage_finished(job, stage, estimator=None, success=False)
                    job["status"] = "failed"
                    if job.get("run_id"):
                        state_manager.mark_failed(job["run_id"], str(exc))
                    logger.error(f"{_job_label(job)} FAILED di stage {stage}: {exc}")

                _log_eta_snapshot(estimator, jobs)
                last_eta_log = time.time()

    finally:
        for executor in executors.values():
            executor.shutdown(wait=True)

    ok_count = len([job for job in jobs if job.get("status") == "completed"])
    fail_count = len([job for job in jobs if job.get("status") == "failed"])

    logger.info(f"\n{'='*50}")
    logger.info(f"Batch selesai: {ok_count} ✅ berhasil | {fail_count} ❌ gagal")
    if ok_count > 0:
        logger.info("Video sudah masuk GDrive queue -> Koyeb akan upload sesuai jadwal")
    logger.info(f"{'='*50}\n")

    logger.info("🧹 Menjalankan cleanup file lama...")
    cleanup_engine.run(dry_run=dry_run)


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
        logger.info(f"Mode: Review Script → {args.review_script}")
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
