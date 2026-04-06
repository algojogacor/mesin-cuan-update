"""
qc_engine.py - Quality control sebelum lanjut ke TTS & render
"""

import os
from engine.utils import get_logger

logger = get_logger("qc_engine")

# Kata-kata yang bisa sebabkan demonetisasi atau strike
BLACKLIST = [
    "membunuh diri sendiri",
]


def check(script_data: dict, channel: dict, profile: str = "shorts") -> bool:
    """
    Cek kualitas script. Return True kalau lolos, raise Exception kalau gagal.
    """
    ch_id  = channel["id"]
    script = script_data.get("script", "")
    title  = script_data.get("title", "")

    logger.info(f"[{ch_id}] Running QC check...")

    # 1. Cek panjang script
    word_count = len(script.split())
    if word_count < 50:
        raise ValueError(f"QC FAILED: Script terlalu pendek ({word_count} kata, min 50)")

    # 2. Cek judul tidak kosong
    if not title or len(title) < 5:
        raise ValueError(f"QC FAILED: Judul terlalu pendek atau kosong")

    if len(title) > 100:
        raise ValueError(f"QC FAILED: Judul terlalu panjang ({len(title)} char, max 100)")

    # 3. Cek blacklist kata
    combined = (script + " " + title).lower()
    for word in BLACKLIST:
        if word.lower() in combined:
            raise ValueError(f"QC FAILED: Mengandung kata terlarang: '{word}'")

    # 4. Cek keywords & tags tidak kosong
    if not script_data.get("keywords"):
        raise ValueError("QC FAILED: Keywords kosong")

    logger.info(f"[{ch_id}] QC PASSED ✅ ({word_count} kata, judul: {len(title)} char)")
    return True
