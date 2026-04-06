"""
check_requirements.py - Cek semua dependency mesin_cuan + mesin_edit
Jalankan: python check_requirements.py
"""

import importlib
import subprocess
import sys

CHECKS = [
    # (import_name, pip_name, keterangan)
    ("dotenv",              "python-dotenv",              "Core"),
    ("requests",            "requests",                   "Core"),
    ("groq",                "groq",                       "AI - LLM"),
    ("google.genai",        "google-genai",               "AI - Gemini (mesin_edit)"),
    ("anthropic",           "anthropic",                  "AI - Claude fallback"),
    ("f5_tts",              "f5-tts",                     "TTS - F5 (EN voice cloning)"),
    ("edge_tts",            "edge-tts",                   "TTS - Edge (ID)"),
    ("soundfile",           "soundfile",                  "TTS - WAV convert"),
    ("faster_whisper",      "faster-whisper",             "ASR - Whisper subtitle"),
    ("PIL",                 "Pillow",                     "Image - thumbnail"),
    ("librosa",             "librosa",                    "Audio - beat detection (cinematic)"),
    ("googleapiclient",     "google-api-python-client",   "Google API - YouTube/Drive"),
    ("torch",               "torch",                      "PyTorch - GPU inference"),
    ("tqdm",                "tqdm",                       "Utils - progress bar"),
]

BINARY_CHECKS = [
    ("ffmpeg",  "ffmpeg -version"),
    ("ffprobe", "ffprobe -version"),
]

print("=" * 60)
print("  DEPENDENCY CHECK — mesin_cuan + mesin_edit")
print("=" * 60)

missing = []

for import_name, pip_name, keterangan in CHECKS:
    try:
        importlib.import_module(import_name)
        print(f"  ✅ {pip_name:<35} {keterangan}")
    except ImportError:
        print(f"  ❌ {pip_name:<35} {keterangan}  ← MISSING")
        missing.append(pip_name)

print()
print("─ Binary ─────────────────────────────────────────────")
for name, cmd in BINARY_CHECKS:
    result = subprocess.run(cmd.split(), capture_output=True)
    if result.returncode == 0:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}  ← MISSING (install FFmpeg dan tambahkan ke PATH)")

# Cek GPU
print()
print("─ GPU ────────────────────────────────────────────────")
try:
    import torch
    if torch.cuda.is_available():
        gpu = torch.cuda.get_device_name(0)
        mem = torch.cuda.get_device_properties(0).total_memory // 1024**3
        print(f"  ✅ CUDA GPU: {gpu} ({mem}GB)")
    else:
        print(f"  ⚠️  CUDA tidak tersedia — akan pakai CPU (lebih lambat)")
except ImportError:
    print(f"  ❌ PyTorch belum terinstall")

print()
if missing:
    print("─ Install yang missing ───────────────────────────────")
    print(f"  pip install {' '.join(missing)}")
else:
    print("  🎉 Semua dependency lengkap!")
print("=" * 60)