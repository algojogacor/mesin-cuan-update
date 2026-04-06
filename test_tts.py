"""
test_tts.py - Test F5-TTS voice cloning + Edge TTS
"""

import os
import time
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ".")

OUTPUT_DIR = "test_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

VOICE_SAMPLE_ID = "assets/voices/ch_id_horror.wav"
VOICE_SAMPLE_EN = "assets/voices/ch_en_horror.wav"

TEXT_ID = """
Pernahkah kamu merasa diawasi di tempat yang sepi?
Para peneliti menemukan bahwa otak manusia memiliki mekanisme khusus yang disebut hyperactive agency detection.
Ini adalah sistem peringatan dini evolusioner yang membuat kita melihat kehadiran entitas bahkan ketika tidak ada.
Di masa prasejarah, lebih baik salah melihat predator yang tidak ada daripada tidak melihat yang benar-benar ada.
Tapi di era modern, inilah yang menyebabkan kita merasa ada sesuatu di sudut ruangan yang gelap.
"""

TEXT_EN = """
Have you ever felt like something was watching you in an empty room?
Researchers have discovered that the human brain has a special mechanism called hyperactive agency detection.
This is an evolutionary early-warning system that makes us perceive the presence of entities even when there are none.
In prehistoric times, it was better to falsely detect a predator than to miss one that was real.
But in the modern era, this is exactly what causes us to feel like something is lurking in the dark corner.
"""


def check_setup():
    print("Checking setup...")
    checks = [
        ("f5_tts",        "F5-TTS"),
        ("faster_whisper", "Faster-Whisper"),
        ("edge_tts",      "Edge TTS"),
        ("soundfile",     "SoundFile"),
        ("torch",         "PyTorch"),
    ]
    for module, name in checks:
        try:
            __import__(module)
            print(f"  ✅ {name}")
        except ImportError:
            print(f"  ❌ {name}")

    try:
        import torch
        if torch.cuda.is_available():
            print(f"  ✅ CUDA ({torch.cuda.get_device_name(0)})")
        else:
            print(f"  ⚠️  CUDA tidak tersedia — F5-TTS jalan di CPU (lebih lambat)")
    except Exception:
        pass

    import subprocess
    result = subprocess.run(["ffmpeg", "-version"], capture_output=True)
    print(f"  {'✅' if result.returncode == 0 else '❌'} FFmpeg")

    print("\nChecking voice samples...")
    for ch_id, path in [("ch_id_horror", VOICE_SAMPLE_ID), ("ch_en_horror", VOICE_SAMPLE_EN)]:
        if os.path.exists(path):
            size_kb = os.path.getsize(path) / 1024
            print(f"  ✅ {ch_id}: {path} ({size_kb:.0f}KB)")
        else:
            print(f"  ⚠️  {ch_id}: {path} — BELUM ADA")


def test_f5tts(text: str, voice_sample: str, language: str, label: str):
    print(f"\n{'='*50}")
    print(f"Testing F5-TTS: {label}")

    if not os.path.exists(voice_sample):
        print(f"❌ SKIP — voice sample tidak ditemukan: {voice_sample}")
        return None

    out_path = f"{OUTPUT_DIR}/f5tts_{label}.mp3"

    try:
        from f5_tts.api import F5TTS
        import soundfile as sf
        import subprocess
        import tempfile
        import re

        print("Loading F5-TTS v1 model...")
        t0    = time.time()
        model = F5TTS()
        print(f"Model loaded dalam {time.time()-t0:.1f}s")

        processed = text.strip().replace("\n\n", "... ").replace("\n", " ")
        processed = re.sub(r'([.!?])\s+(?=[A-Z])', r'\1 ... ', processed)

        print("Generating audio...")
        t1 = time.time()

        wav, sr, _ = model.infer(
            ref_file = voice_sample,
            ref_text = "",
            gen_text = processed,
            seed     = -1,
            speed    = 1.0,
        )

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            tmp_wav = tmp.name
        sf.write(tmp_wav, wav, sr)

        subprocess.run([
            "ffmpeg", "-y", "-i", tmp_wav,
            "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
            "-codec:a", "libmp3lame", "-qscale:a", "2",
            out_path
        ], capture_output=True)
        os.remove(tmp_wav)

        elapsed = time.time() - t1
        size_kb = os.path.getsize(out_path) / 1024

        print(f"✅ F5-TTS berhasil!")
        print(f"   Output  : {out_path}")
        print(f"   Durasi generate: {elapsed:.1f}s")
        print(f"   File size: {size_kb:.0f}KB")
        return out_path

    except Exception as e:
        print(f"❌ F5-TTS gagal: {e}")
        return None


def test_edge_tts(text: str, language: str, label: str):
    print(f"\n{'='*50}")
    print(f"Testing Edge TTS: {label}")

    out_path = f"{OUTPUT_DIR}/edge_tts_{label}.mp3"

    try:
        import asyncio
        import edge_tts
        import re

        voice_map = {"id": "id-ID-ArdiNeural", "en": "en-US-ChristopherNeural"}
        voice     = voice_map.get(language, "en-US-ChristopherNeural")

        paced = text.strip().replace("\n\n", ". ... ").replace("\n", " ")
        paced = re.sub(r'([.!?])\s+(?=[A-Z])', r'\1 ... ', paced)

        t0 = time.time()

        async def _gen():
            comm = edge_tts.Communicate(paced, voice)
            await comm.save(out_path)

        asyncio.run(_gen())
        elapsed = time.time() - t0
        size_kb = os.path.getsize(out_path) / 1024

        print(f"✅ Edge TTS berhasil!")
        print(f"   Output  : {out_path}")
        print(f"   Voice   : {voice}")
        print(f"   Durasi generate: {elapsed:.1f}s")
        print(f"   File size: {size_kb:.0f}KB")
        return out_path

    except Exception as e:
        print(f"❌ Edge TTS gagal: {e}")
        return None


if __name__ == "__main__":
    print("=" * 60)
    print("F5-TTS Voice Clone Test")
    print("=" * 60)

    check_setup()

    test_edge_tts(TEXT_ID, "id", "id")
    test_edge_tts(TEXT_EN, "en", "en")

    test_f5tts(TEXT_ID, VOICE_SAMPLE_ID, "id", "id")
    test_f5tts(TEXT_EN, VOICE_SAMPLE_EN, "en", "en")

    print(f"\n{'='*60}")
    print(f"Test selesai! Cek folder: {OUTPUT_DIR}/")
    print(f"Dengarkan edge_tts_*.mp3 vs f5tts_*.mp3")
    print("=" * 60)