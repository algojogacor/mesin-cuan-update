"""
notif_engine.py - Kirim notifikasi ke Telegram
"""

import requests
from engine.utils import get_logger

logger = get_logger("notif_engine")


def _send(text: str):
    """Kirim pesan ke Telegram. Gagal silent (non-fatal)."""
    try:
        import os
        token   = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            logger.debug("Telegram tidak dikonfigurasi, skip notif")
            return

        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        logger.warning(f"Notif Telegram gagal (non-fatal): {e}")


def upload_success(channel: dict, title: str, url: str):
    ch_name = channel.get("name", channel["id"])
    _send(
        f"✅ <b>Upload Sukses</b>\n"
        f"📺 Channel: {ch_name}\n"
        f"🎬 Judul: {title}\n"
        f"🔗 {url}"
    )


def upload_failed(channel: dict, error: str):
    ch_name = channel.get("name", channel["id"])
    _send(
        f"❌ <b>Pipeline GAGAL</b>\n"
        f"📺 Channel: {ch_name}\n"
        f"💥 Error: {error[:300]}"
    )


def daily_summary(results: list[dict]):
    """
    Kirim ringkasan harian.
    results = [{"channel": str, "uploaded": int, "failed": int}, ...]
    """
    total_uploaded = sum(r["uploaded"] for r in results)
    total_failed   = sum(r["failed"] for r in results)

    lines = ["📊 <b>Ringkasan Harian</b>\n"]
    for r in results:
        status = "✅" if r["failed"] == 0 else "⚠️"
        lines.append(f"{status} {r['channel']}: {r['uploaded']} upload, {r['failed']} gagal")

    lines.append(f"\n<b>Total: {total_uploaded} video diupload, {total_failed} gagal</b>")
    _send("\n".join(lines))


def pipeline_start(total_channels: int):
    _send(f"🚀 <b>Pipeline dimulai</b> — {total_channels} channel aktif")
