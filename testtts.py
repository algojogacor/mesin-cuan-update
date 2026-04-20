"""
test_chatterbox.py — Standalone test untuk Chatterbox TTS
Jalankan SEBELUM implementasi ke tts_engine.py

Cara pakai:
  python test_chatterbox.py

Output:
  test_output_default.wav   → TTS tanpa voice cloning
  test_output_cloned.wav    → TTS dengan voice cloning (jika voices/test_sample.wav ada)

Hardware target: RTX 3050 4GB VRAM, 16GB RAM
"""

import os
import time
import torch

# ── Teks narasi untuk test ────────────────────────────────────────────────────
TEST_TEXT = (
    "Welcome to Horror Facts channel. "
    "Today we will discuss mysterious events "
    "that have never been revealed before."
)

# ── Path voice sample untuk cloning ──────────────────────────────────────────
VOICE_SAMPLE_PATH = "voices/test_sample.wav"

# ── Output paths ──────────────────────────────────────────────────────────────
OUTPUT_DEFAULT = "test_output_default.wav"
OUTPUT_CLONED  = "test_output_cloned.wav"


def print_separator(title: str) -> None:
    print(f"\n{'='*55}")
    print(f"  {title}")
    print(f"{'='*55}")


def check_device() -> str:
    """Deteksi device terbaik yang tersedia."""
    if torch.cuda.is_available():
        vram_gb = torch.cuda.get_device_properties(0).total_memory / 1e9
        print(f"  [GPU] CUDA tersedia — {torch.cuda.get_device_name(0)}")
        print(f"  [GPU] VRAM total: {vram_gb:.1f} GB")
        return "cuda"
    else:
        print("  [CPU] CUDA tidak tersedia — menggunakan CPU")
        return "cpu"


def load_model(device: str):
    """Load Chatterbox model."""
    print(f"\n  Loading Chatterbox model ke {device.upper()}...")
    print("  (Proses ini bisa 1-3 menit pertama kali karena download model)\n")

    from chatterbox.tts import ChatterboxTTS

    start = time.time()
    model = ChatterboxTTS.from_pretrained(device=device)

    if device == "cuda":
        print(f"  CUDA aktif — model siap digunakan")

    elapsed = time.time() - start
    print(f"  Model loaded dalam {elapsed:.1f} detik")
    return model

def generate_audio(model, text: str, output_path: str,
                   audio_prompt_path: str = None) -> dict:
    """
    Generate audio dari teks.
    Jika audio_prompt_path disediakan → mode voice cloning.
    Return dict berisi info hasil generate.
    """
    import torchaudio

    start = time.time()

    if audio_prompt_path:
        wav = model.generate(text, audio_prompt_path=audio_prompt_path)
    else:
        wav = model.generate(text)

    elapsed = time.time() - start

    # Simpan output
    torchaudio.save(output_path, wav, model.sr)

    # Info file
    file_size_kb = os.path.getsize(output_path) / 1024
    abs_path     = os.path.abspath(output_path)

    return {
        "duration_sec": elapsed,
        "file_size_kb": file_size_kb,
        "path": abs_path,
    }


def print_result(label: str, info: dict) -> None:
    print(f"\n  Hasil {label}:")
    print(f"    Durasi generate : {info['duration_sec']:.1f} detik")
    print(f"    Ukuran file     : {info['file_size_kb']:.1f} KB")
    print(f"    Path output     : {info['path']}")


def main():
    print_separator("CHATTERBOX TTS — STANDALONE TEST")
    print(f"\n  Teks uji:\n  \"{TEST_TEXT}\"\n")

    # ── Cek device ────────────────────────────────────────────────────────────
    print_separator("1. Cek Hardware")
    device = check_device()

    # ── Load model ────────────────────────────────────────────────────────────
    print_separator("2. Load Model")
    try:
        model = load_model(device)
    except ImportError:
        print("\n  [ERROR] Chatterbox belum terinstall!")
        print("  Jalankan dulu:\n")
        print("    pip install chatterbox-tts\n")
        return
    except Exception as e:
        print(f"\n  [ERROR] Gagal load model: {e}")
        return

    # ── Mode A: Default voice ─────────────────────────────────────────────────
    print_separator("3. Mode A — Default Voice")
    print("  Generate tanpa voice sample...")
    try:
        info = generate_audio(model, TEST_TEXT, OUTPUT_DEFAULT)
        print_result("Mode A (default)", info)
        print(f"\n  Output tersimpan: {OUTPUT_DEFAULT}")
    except Exception as e:
        print(f"\n  [ERROR] Mode A gagal: {e}")

    # ── Mode B: Voice cloning ─────────────────────────────────────────────────
    print_separator("4. Mode B — Voice Cloning")
    if not os.path.exists(VOICE_SAMPLE_PATH):
        print(f"\n  [SKIP] File voice sample tidak ditemukan:")
        print(f"         {os.path.abspath(VOICE_SAMPLE_PATH)}")
        print(f"\n  Untuk test voice cloning, simpan rekaman suaramu di:")
        print(f"    voices/test_sample.wav  (durasi ~20-30 detik, format WAV)")
        print(f"\n  Lalu jalankan ulang script ini.")
    else:
        print(f"  Voice sample ditemukan: {VOICE_SAMPLE_PATH}")
        print(f"  Generate dengan voice cloning...")
        try:
            info = generate_audio(model, TEST_TEXT, OUTPUT_CLONED,
                                  audio_prompt_path=VOICE_SAMPLE_PATH)
            print_result("Mode B (cloned)", info)
            print(f"\n  Output tersimpan: {OUTPUT_CLONED}")
        except Exception as e:
            print(f"\n  [ERROR] Mode B gagal: {e}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print_separator("SELESAI")
    print("\n  File output yang dihasilkan:")
    for fname in [OUTPUT_DEFAULT, OUTPUT_CLONED]:
        if os.path.exists(fname):
            size_kb = os.path.getsize(fname) / 1024
            print(f"    {fname} ({size_kb:.1f} KB)")
        else:
            print(f"    {fname} — tidak ada (di-skip atau gagal)")

    print("\n  Dengarkan hasilnya, lalu lapor ke Claude apakah kualitasnya oke.")
    print("  Kalau bagus → lanjut implementasi ke engine/tts_engine.py\n")


if __name__ == "__main__":
    main()