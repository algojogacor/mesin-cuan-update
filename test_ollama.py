"""
test_ollama.py - Test koneksi dan response Ollama + DeepSeek

Cara pakai:
  python test_ollama.py

Yang ditest:
  1. Koneksi ke Ollama server
  2. List model yang tersedia
  3. Test generate teks sederhana
  4. Test generate JSON (simulasi script_engine)
  5. Speed test
"""

import requests
import json
import time
import os

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.environ.get("OLLAMA_MODEL",    "deepseek-v3.1:671b-cloud")


def separator(title: str = ""):
    print(f"\n{'='*55}")
    if title:
        print(f"  {title}")
        print(f"{'='*55}")


# ─── Test 1: Koneksi ──────────────────────────────────────────────────────────

def test_connection():
    separator("TEST 1 — Koneksi ke Ollama")
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/", timeout=5)
        if resp.status_code == 200:
            print(f"  ✅ Ollama berjalan di {OLLAMA_BASE_URL}")
            return True
        else:
            print(f"  ❌ Ollama merespons tapi status: {resp.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"  ❌ Tidak bisa terhubung ke {OLLAMA_BASE_URL}")
        print(f"     Pastikan Ollama sudah jalan!")
        print(f"     Cara start: buka Ollama app atau jalankan 'ollama serve'")
        return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


# ─── Test 2: List model ───────────────────────────────────────────────────────

def test_list_models():
    separator("TEST 2 — Model yang Tersedia")
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=10)
        resp.raise_for_status()
        models = resp.json().get("models", [])

        if not models:
            print("  ⚠️  Tidak ada model yang ter-install")
            return False

        print(f"  {len(models)} model ditemukan:")
        target_found = False
        for m in models:
            name    = m.get("name", "unknown")
            size_gb = m.get("size", 0) / (1024**3)
            marker  = " ← TARGET" if name == OLLAMA_MODEL else ""
            print(f"    • {name} ({size_gb:.1f}GB){marker}")
            if name == OLLAMA_MODEL:
                target_found = True

        if not target_found:
            print(f"\n  ⚠️  Model target '{OLLAMA_MODEL}' tidak ditemukan di list")
            print(f"     Coba: ollama pull {OLLAMA_MODEL}")
        else:
            print(f"\n  ✅ Model target '{OLLAMA_MODEL}' tersedia")

        return target_found

    except Exception as e:
        print(f"  ❌ Gagal list model: {e}")
        return False


# ─── Test 3: Generate teks sederhana ─────────────────────────────────────────

def test_simple_generate():
    separator("TEST 3 — Generate Teks Sederhana")
    print(f"  Model  : {OLLAMA_MODEL}")
    print(f"  Prompt : 'Siapa kamu? Jawab dalam 1 kalimat.'")

    payload = {
        "model":  OLLAMA_MODEL,
        "messages": [
            {"role": "user", "content": "Siapa kamu? Jawab dalam 1 kalimat singkat."}
        ],
        "stream":  False,
        "options": {"temperature": 0.5, "num_predict": 100},
    }

    try:
        t0   = time.time()
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        elapsed = time.time() - t0

        content = resp.json().get("message", {}).get("content", "").strip()
        if content:
            print(f"\n  ✅ Response ({elapsed:.1f}s):")
            print(f"     \"{content[:200]}\"")
            return True
        else:
            print(f"  ❌ Response kosong")
            return False

    except requests.exceptions.Timeout:
        print(f"  ❌ Timeout — model tidak merespons dalam 120 detik")
        return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


# ─── Test 4: Generate JSON (simulasi script_engine) ───────────────────────────

def test_json_generate():
    separator("TEST 4 — Generate JSON (Simulasi Script Engine)")

    system_prompt = """Kamu adalah penulis skrip konten YouTube Shorts tentang psikologi.
Tugasmu: buat skrip narasi singkat berdasarkan topik yang diberikan.
Output HARUS berupa JSON valid dengan struktur:
{
  "title": "judul video yang menarik",
  "script": "narasi lengkap minimal 80 kata",
  "keywords": ["keyword1", "keyword2", "keyword3"],
  "tags": ["tag1", "tag2"],
  "description": "deskripsi video singkat"
}
Jangan tambahkan teks apapun di luar JSON."""

    user_message = "Topik: Mengapa manusia sering menunda pekerjaan (prokrastinasi)"

    payload = {
        "model":  OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_message},
        ],
        "stream":  False,
        "format":  "json",
        "options": {"temperature": 0.7, "num_predict": 1000},
    }

    try:
        print(f"  Generating script JSON...")
        t0   = time.time()
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        elapsed = time.time() - t0

        raw     = resp.json().get("message", {}).get("content", "").strip()
        if not raw:
            print(f"  ❌ Response kosong")
            return False

        # Parse JSON — strip markdown fencing kalau ada (```json ... ```)
        try:
            clean = raw.replace("```json", "").replace("```", "").strip()
            if not clean.startswith("{"):
                import re
                match = re.search(r'\{.*\}', clean, re.DOTALL)
                clean = match.group() if match else clean
            data       = json.loads(clean)
            title      = data.get("title", "")
            script     = data.get("script", "")
            word_count = len(script.split())

            print(f"\n  ✅ JSON valid! ({elapsed:.1f}s)")
            print(f"     Title      : {title}")
            print(f"     Script     : {script[:120]}...")
            print(f"     Word count : {word_count} kata")
            print(f"     Keywords   : {data.get('keywords', [])}")

            if word_count >= 80:
                print(f"  ✅ Panjang script OK (≥80 kata)")
            else:
                print(f"  ⚠️  Script terlalu pendek ({word_count} kata, min 80)")

            return True

        except json.JSONDecodeError as e:
            print(f"  ❌ Response bukan JSON valid: {e}")
            print(f"     Raw response: {raw[:300]}")
            return False

    except requests.exceptions.Timeout:
        print(f"  ❌ Timeout — coba model yang lebih kecil")
        return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


# ─── Test 5: Speed benchmark ─────────────────────────────────────────────────

def test_speed():
    separator("TEST 5 — Speed Benchmark")
    print("  Generate 50 token untuk ukur kecepatan...")

    payload = {
        "model":  OLLAMA_MODEL,
        "messages": [{"role": "user", "content": "Tulis 3 fakta menarik tentang otak manusia."}],
        "stream":  False,
        "options": {"temperature": 0.5, "num_predict": 50},
    }

    try:
        t0   = time.time()
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        elapsed = time.time() - t0

        data       = resp.json()
        content    = data.get("message", {}).get("content", "")
        eval_count = data.get("eval_count", 0)
        tok_per_s  = eval_count / elapsed if elapsed > 0 else 0

        print(f"  ✅ Selesai dalam {elapsed:.1f} detik")
        print(f"     Tokens    : {eval_count}")
        print(f"     Speed     : {tok_per_s:.1f} token/detik")

        if tok_per_s > 20:
            print(f"  ✅ Kecepatan bagus (>20 tok/s)")
        elif tok_per_s > 5:
            print(f"  ⚠️  Kecepatan sedang ({tok_per_s:.1f} tok/s) — masih OK untuk pipeline")
        else:
            print(f"  ⚠️  Kecepatan lambat ({tok_per_s:.1f} tok/s) — pertimbangkan model lebih kecil")

        # Estimasi waktu generate 1 script shorts (~300 token)
        est_shorts = (300 / tok_per_s) if tok_per_s > 0 else 999
        print(f"\n  Estimasi waktu generate 1 script shorts: ~{est_shorts:.0f} detik")

    except Exception as e:
        print(f"  ❌ Error: {e}")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  Ollama + DeepSeek Connection Test")
    print(f"  URL   : {OLLAMA_BASE_URL}")
    print(f"  Model : {OLLAMA_MODEL}")
    print("=" * 55)

    # Test 1: koneksi dulu, kalau gagal stop
    if not test_connection():
        print("\n❌ Stop — Ollama tidak bisa dihubungi.")
        print("   Pastikan Ollama app sudah dibuka atau jalankan 'ollama serve'")
        exit(1)

    # Test 2: cek model
    test_list_models()

    # Test 3: generate sederhana
    if not test_simple_generate():
        print("\n⚠️  Generate sederhana gagal, skip test berikutnya.")
        exit(1)

    # Test 4: generate JSON
    test_json_generate()

    # Test 5: speed
    test_speed()

    separator()
    print("  Test selesai!")
    print("  Kalau semua ✅ → script_engine.py siap dipakai")
    print("  Kalau ada ❌  → cek pesan error di atas")
    print("=" * 55)