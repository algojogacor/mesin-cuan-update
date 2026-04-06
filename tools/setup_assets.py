#!/usr/bin/env python3
"""
tools/setup_assets.py - Asset Hunter Script
Downloads royalty-free SFX dari URL publik valid ke assets/sfx/

Usage:
    python tools/setup_assets.py
    python tools/setup_assets.py --force   # re-download even if exists
"""

import os
import sys
import argparse
import logging
import urllib.request
import urllib.error

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("setup_assets")

# ─── Target folder ────────────────────────────────────────────────────────────
SFX_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets", "sfx")

# ─── SFX Catalog (royalty-free, public CDN URLs) ─────────────────────────────
# Sources: Freesound CDN (CC0 / CC-BY), Pixabay Audio, SoundBible
SFX_CATALOG: list[dict] = [
    {
        "filename": "whoosh.mp3",
        "desc": "Whoosh transition sound – fast air swoosh",
        "urls": [
            # Pixabay royalty-free (no attribution required)
            "https://cdn.pixabay.com/audio/2023/05/09/audio_17e11b764e.mp3",
            # Fallback: generic whoosh from OpenGameArt / USFX (CC0)
            "https://opengameart.org/sites/default/files/audio_preview/Swoosh.ogg.mp3",
        ],
    },
    {
        "filename": "impact.wav",
        "desc": "Heavy impact / hit SFX – punchy low thud",
        "urls": [
            # Pixabay royalty-free
            "https://cdn.pixabay.com/audio/2022/10/30/audio_98a31e16a8.mp3",
            # Fallback WAV from Freesound CDN (CC0)
            "https://freesound.org/data/previews/561/561660_5674468-lq.mp3",
        ],
    },
    {
        "filename": "dark_ambient.mp3",
        "desc": "Dark ambient loop – cinematic background tension",
        "urls": [
            # Pixabay royalty-free dark ambient
            "https://cdn.pixabay.com/audio/2024/02/15/audio_f78ac88928.mp3",
            # Fallback: Free music from Bensound (Royalty Free)
            "https://www.bensound.com/bensound-music/bensound-mystery.mp3",
        ],
    },
]


def _download_file(url: str, out_path: str, timeout: int = 30) -> bool:
    """Download satu file dari URL ke out_path. Return True jika berhasil."""
    try:
        logger.info(f"  ↓ {os.path.basename(out_path)} ← {url[:80]}...")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0 Safari/537.36"
            )
        }
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        if len(data) < 1024:
            logger.warning(f"  ⚠ File terlalu kecil ({len(data)} bytes), skip.")
            return False
        with open(out_path, "wb") as f:
            f.write(data)
        logger.info(f"  ✓ Saved {len(data) / 1024:.1f} KB → {out_path}")
        return True
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
        logger.warning(f"  ✗ Gagal ({type(e).__name__}): {e}")
        return False


def download_all_sfx(force: bool = False) -> dict[str, bool]:
    """
    Download semua SFX di SFX_CATALOG ke SFX_DIR.
    Jika file sudah ada dan force=False, skip.
    Return dict filename → success.
    """
    os.makedirs(SFX_DIR, exist_ok=True)
    logger.info(f"📁 SFX target dir: {SFX_DIR}")

    results: dict[str, bool] = {}

    for asset in SFX_CATALOG:
        fname   = asset["filename"]
        out     = os.path.join(SFX_DIR, fname)
        desc    = asset["desc"]
        success = False

        if os.path.exists(out) and not force:
            size_kb = os.path.getsize(out) / 1024
            logger.info(f"✔ Skip (sudah ada, {size_kb:.1f} KB): {fname}")
            results[fname] = True
            continue

        logger.info(f"\n🔊 Downloading: {fname} — {desc}")
        for url in asset["urls"]:
            if _download_file(url, out):
                success = True
                break
            # Cleanup partial file if exists
            if os.path.exists(out):
                os.remove(out)

        if not success:
            logger.error(f"❌ GAGAL download {fname} dari semua URL.")
            # Create placeholder silent file so engine won't crash
            _create_silent_placeholder(out, fname)
            logger.warning(f"🔇 Placeholder silent dibuat: {out}")

        results[fname] = success

    return results


def _create_silent_placeholder(path: str, fname: str):
    """Buat file placeholder minimal agar engine tidak crash."""
    # Minimal MP3 header (silent ~0.1s) — 113 bytes ID3 + silent frame
    # Ini placeholder valid agar ffmpeg tidak error saat load.
    # Source: minimal mp3 silent frame spec.
    if fname.endswith(".wav"):
        # Minimal WAV header (44 bytes, 1 ch, 16-bit, 44100Hz, 0 samples)
        wav_header = (
            b"RIFF$\x00\x00\x00WAVEfmt \x10\x00\x00\x00"
            b"\x01\x00\x01\x00\x44\xac\x00\x00\x88X\x01\x00"
            b"\x02\x00\x10\x00data\x00\x00\x00\x00"
        )
        with open(path, "wb") as f:
            f.write(wav_header)
    else:
        # Minimal silent MP3 (ID3v2 + 1 silent frame)
        silent_mp3 = (
            b"\xff\xfb\x90\x00" +  # MPEG1 Layer3 frame header (silent)
            b"\x00" * 413 +        # Frame data (silent)
            b"\xff\xfb\x90\x00" +
            b"\x00" * 413
        )
        with open(path, "wb") as f:
            f.write(silent_mp3)


def verify_sfx():
    """Verifikasi semua SFX ada di folder. Print status table."""
    print("\n" + "=" * 55)
    print(f"{'FILE':<22} {'SIZE':>10}  {'STATUS'}")
    print("-" * 55)
    all_ok = True
    for asset in SFX_CATALOG:
        fname = asset["filename"]
        path  = os.path.join(SFX_DIR, fname)
        if os.path.exists(path):
            size  = os.path.getsize(path) / 1024
            mark  = "✓" if size > 5 else "⚠ (placeholder)"
            print(f"{fname:<22} {size:>8.1f} KB  {mark}")
            if size <= 5:
                all_ok = False
        else:
            print(f"{fname:<22} {'—':>10}  ✗ MISSING")
            all_ok = False
    print("=" * 55)
    if all_ok:
        print("🎉 Semua SFX siap digunakan!")
    else:
        print("⚠  Ada SFX yang belum terdownload. Jalankan ulang atau cek URL.")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mesin Cuan – SFX Asset Downloader")
    parser.add_argument("--force", action="store_true", help="Re-download meskipun file sudah ada")
    parser.add_argument("--verify", action="store_true", help="Hanya cek status, tidak download")
    args = parser.parse_args()

    if args.verify:
        verify_sfx()
        sys.exit(0)

    logger.info("🚀 Mesin Cuan Asset Hunter — Starting SFX download...")
    results = download_all_sfx(force=args.force)

    n_ok   = sum(1 for v in results.values() if v)
    n_fail = len(results) - n_ok

    print()
    verify_sfx()
    logger.info(f"Done. ✓ {n_ok} berhasil | ✗ {n_fail} gagal (placeholder dibuat)")

    if n_fail > 0:
        sys.exit(1)
