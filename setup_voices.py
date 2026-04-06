"""
setup_voices.py - Helper untuk menyiapkan voice samples untuk F5-TTS

Cara pakai:
  python setup_voices.py

Script ini akan:
  1. Membuat folder assets/voices/ kalau belum ada
  2. Memberi panduan rekaman suara yang optimal
  3. Memvalidasi file WAV yang sudah ada
  4. Trim silence dan normalize audio WAV yang sudah ada

Requirement:
  - FFmpeg (untuk trim + normalize)
  - Mikrofon (untuk rekam)
"""

import os
import subprocess
import sys

VOICES_DIR = "assets/voices"
CHANNELS = [
    {
        "id":       "ch_id_horror",
        "filename": "ch_id_horror.wav",
        "lang":     "ID",
        "gender":   "Pria",
        "style":    "Dramatis, misterius, tegang",
        "sample_text": (
            "Malam itu, rumah tua di ujung gang berdiri sendiri dalam kegelapan. "
            "Tidak ada yang berani mendekatinya sejak kejadian itu. "
            "Mereka bilang, kalau kamu mendengar suara langkah di lantai dua, jangan pernah naik. "
            "Karena apa yang menunggumu di atas sana bukan manusia."
        ),
    },
    {
        "id":       "ch_id_psych",
        "filename": "ch_id_psych.wav",
        "lang":     "ID",
        "gender":   "Wanita atau Pria",
        "style":    "Tenang, edukatif, percaya diri",
        "sample_text": (
            "Tahukah kamu bahwa 95 persen dari keputusan yang kamu buat setiap hari "
            "sebenarnya dikendalikan oleh alam bawah sadarmu? "
            "Para ilmuwan menyebutnya sebagai proses otomatis otak. "
            "Ini bukan kelemahan, ini justru cara otak menghemat energi untuk hal yang benar-benar penting."
        ),
    },
    {
        "id":       "ch_en_horror",
        "filename": "ch_en_horror.wav",
        "lang":     "EN",
        "gender":   "Male",
        "style":    "Dark, mysterious, tense",
        "sample_text": (
            "The old house at the end of the road had been abandoned for thirty years. "
            "Nobody dared go near it after what happened. "
            "They say if you ever hear footsteps on the second floor, don't go up. "
            "Because what's waiting for you up there is not human."
        ),
    },
    {
        "id":       "ch_en_psych",
        "filename": "ch_en_psych.wav",
        "lang":     "EN",
        "gender":   "Male or Female",
        "style":    "Calm, educational, confident",
        "sample_text": (
            "Did you know that 95 percent of the decisions you make every day "
            "are actually controlled by your subconscious mind? "
            "Scientists call this the brain's automatic processing system. "
            "This isn't a weakness — it's actually how your brain conserves energy "
            "for the things that truly matter."
        ),
    },
]


def create_folders():
    os.makedirs(VOICES_DIR, exist_ok=True)
    print(f"✅ Folder dibuat: {VOICES_DIR}/")


def print_recording_guide():
    print("\n" + "="*60)
    print("PANDUAN REKAM SUARA UNTUK F5-TTS VOICE CLONING")
    print("="*60)
    print("""
Tips rekaman yang optimal:
  ✅ Durasi: 10-30 detik (lebih panjang = lebih akurat)
  ✅ Lingkungan: Tenang, tidak ada noise/AC/kipas
  ✅ Mikrofon: Headset atau mic eksternal lebih baik
  ✅ Jarak: 15-20cm dari mikrofon
  ✅ Format: WAV (bukan MP3!) — bisa convert nanti
  ✅ Baca dengan gaya yang KONSISTEN dengan channel

  ❌ Hindari: noise background, ruangan bergema
  ❌ Hindari: suara terlalu pelan atau terlalu keras
  ❌ Hindari: jeda terlalu panjang di awal/akhir

Cara rekam di Windows:
  1. Buka Voice Recorder (aplikasi bawaan Windows)
  2. Rekam → Save
  3. File tersimpan di Documents/Sound Recordings/
  4. Copy ke assets/voices/<nama_file>.wav

Atau pakai Audacity (gratis):
  1. Download audacity.org
  2. Record → Export as WAV
""")


def validate_wav(filepath: str) -> bool:
    """Validasi file WAV menggunakan ffprobe."""
    if not os.path.exists(filepath):
        return False

    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_streams", "-show_format", filepath],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        return False

    import json
    try:
        data     = json.loads(result.stdout)
        duration = float(data["format"].get("duration", 0))
        size_kb  = os.path.getsize(filepath) / 1024

        print(f"  ✅ Valid WAV")
        print(f"     Durasi  : {duration:.1f} detik")
        print(f"     Size    : {size_kb:.0f} KB")

        if duration < 6:
            print(f"  ⚠️  Durasi terlalu pendek (min 6 detik, recommended 15+ detik)")
        elif duration > 60:
            print(f"  ⚠️  Durasi terlalu panjang (max 60 detik)")
        else:
            print(f"  ✅ Durasi OK")

        return True
    except Exception:
        return False


def process_wav(filepath: str, output_path: str):
    """
    Process WAV: trim silence + normalize volume.
    Ini membantu F5-TTS mendapat referensi yang lebih bersih.
    """
    print(f"  Processing: {os.path.basename(filepath)}...")

    cmd = [
        "ffmpeg", "-y",
        "-i", filepath,
        # Trim silence di awal dan akhir
        "-af", (
            "silenceremove=start_periods=1:start_silence=0.1:start_threshold=-50dB,"
            "silenceremove=stop_periods=-1:stop_silence=0.3:stop_threshold=-50dB,"
            "loudnorm=I=-16:TP=-1.5:LRA=11"
        ),
        "-ar", "22050",    # Sample rate yang F5-TTS suka
        "-ac", "1",        # Mono
        output_path
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  ✅ Processed → {output_path}")
        return True
    else:
        print(f"  ❌ Processing gagal: {result.stderr[-200:]}")
        return False


def check_all_voices():
    """Cek status semua voice sample."""
    print("\n" + "="*60)
    print("STATUS VOICE SAMPLES")
    print("="*60)

    all_ready = True
    for ch in CHANNELS:
        filepath = os.path.join(VOICES_DIR, ch["filename"])
        print(f"\n[{ch['id']}] — {ch['lang']} {ch['gender']} ({ch['style']})")
        print(f"  File: {filepath}")

        if os.path.exists(filepath):
            validate_wav(filepath)
        else:
            print(f"  ❌ BELUM ADA")
            print(f"\n  Teks untuk dibaca (salin dan rekam):")
            print(f"  {'─'*40}")
            for line in ch["sample_text"].strip().split(". "):
                if line.strip():
                    print(f"  \"{line.strip()}.\"")
            print(f"  {'─'*40}")
            all_ready = False

    return all_ready


def process_all_existing():
    """Process semua WAV yang sudah ada (trim + normalize)."""
    print("\n" + "="*60)
    print("PROCESSING VOICE SAMPLES")
    print("="*60)

    for ch in CHANNELS:
        raw_path  = os.path.join(VOICES_DIR, ch["filename"])
        proc_path = os.path.join(VOICES_DIR, ch["filename"].replace(".wav", "_processed.wav"))

        if os.path.exists(raw_path):
            print(f"\n[{ch['id']}]")
            if process_wav(raw_path, proc_path):
                # Replace raw dengan processed
                import shutil
                shutil.move(proc_path, raw_path)
                print(f"  ✅ {ch['filename']} sudah di-process")
        else:
            print(f"\n[{ch['id']}] ⚠️  Skip — file belum ada")


if __name__ == "__main__":
    print("="*60)
    print("Voice Sample Setup untuk F5-TTS")
    print("="*60)

    # Buat folder
    create_folders()

    # Print panduan rekam
    print_recording_guide()

    # Cek status
    all_ready = check_all_voices()

    if not all_ready:
        print("\n" + "="*60)
        print("⚠️  Beberapa voice sample belum ada.")
        print("   Ikuti panduan di atas untuk merekam.")
        print("   Setelah rekam, jalankan script ini lagi.")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("✅ Semua voice sample sudah ada!")
        print("   Memproses (trim silence + normalize)...")
        process_all_existing()
        print("\n✅ Semua voice sample siap dipakai.")
        print("   Jalankan: python test_tts.py")
        print("="*60)