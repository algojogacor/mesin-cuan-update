"""
cleanup_engine.py - Hapus file temporary yang lebih dari 1 hari
Folder yang dibersihkan per channel: audio, footage, subtitles, scripts
Folder yang TIDAK dihapus: output (video final + thumbnail)
"""

import os
import time
import shutil
from engine.utils import get_logger, load_settings

logger = get_logger("cleanup_engine")

# Folder yang dibersihkan (bukan output — itu video final)
CLEANUP_FOLDERS = ["audio", "footage", "subtitles", "scripts", "music"]

# Hapus file lebih dari berapa jam
MAX_AGE_HOURS = 24


def run(dry_run: bool = False):
    """Jalankan cleanup untuk semua channel aktif."""
    settings = load_settings()
    channels = [ch for ch in settings["channels"] if ch.get("active", True)]

    total_deleted = 0
    total_freed   = 0

    for channel in channels:
        deleted, freed = _cleanup_channel(channel["id"], dry_run=dry_run)
        total_deleted += deleted
        total_freed   += freed

    freed_mb = total_freed / (1024 * 1024)
    action   = "Akan dihapus" if dry_run else "Dihapus"
    logger.info(f"Cleanup selesai — {action} {total_deleted} file ({freed_mb:.1f} MB)")


def _cleanup_channel(ch_id: str, dry_run: bool = False) -> tuple:
    deleted = 0
    freed   = 0
    now     = time.time()
    cutoff  = now - (MAX_AGE_HOURS * 3600)

    for folder in CLEANUP_FOLDERS:
        path = f"data/{ch_id}/{folder}"
        if not os.path.exists(path):
            continue

        for item in os.listdir(path):
            item_path = os.path.join(path, item)

            # Hapus folder tmp_ (sisa render yang crash)
            if os.path.isdir(item_path) and item.startswith("tmp_"):
                size = _get_folder_size(item_path)
                if not dry_run:
                    shutil.rmtree(item_path, ignore_errors=True)
                    logger.debug(f"[{ch_id}] Removed tmp dir: {item_path}")
                deleted += 1
                freed   += size
                continue

            # Hapus file lebih dari MAX_AGE_HOURS
            if os.path.isfile(item_path):
                mtime = os.path.getmtime(item_path)
                if mtime < cutoff:
                    size = os.path.getsize(item_path)
                    if not dry_run:
                        os.remove(item_path)
                        logger.debug(f"[{ch_id}] Removed: {item_path}")
                    deleted += 1
                    freed   += size

    if deleted > 0:
        freed_mb = freed / (1024 * 1024)
        action   = "Akan dihapus" if dry_run else "Dibersihkan"
        logger.info(f"[{ch_id}] {action}: {deleted} file ({freed_mb:.1f} MB)")

    return deleted, freed


def _get_folder_size(path: str) -> int:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total += os.path.getsize(fp)
            except Exception:
                pass
    return total