"""
test_qwen_api.py
Tes koneksi ke Qwen API proxy via REST (invoke method langsung, tanpa SDK).
Jalankan: python test_qwen_api.py
"""

import json
import time
import requests

# ── Config — sesuaikan kalau perlu ───────────────────────────────────────────
QWEN_API_BASE = "http://34.57.12.120:9000/v1"
QWEN_API_KEY  = "sk-proxy-local"
QWEN_MODEL    = "qwen3-235b-a22b-instruct-2507"

HEADERS = {
    "Authorization": f"Bearer {QWEN_API_KEY}",
    "Content-Type":  "application/json",
}

TIMEOUT = 120  # detik

# ── Helper ────────────────────────────────────────────────────────────────────
def section(title: str):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print('='*55)

def ok(msg):  print(f"  ✅  {msg}")
def fail(msg): print(f"  ❌  {msg}")
def info(msg): print(f"  ℹ️   {msg}")

# ── Test 1: Reachability ──────────────────────────────────────────────────────
section("TEST 1 — Reachability (GET /v1/models)")
try:
    r = requests.get(
        f"{QWEN_API_BASE}/models",
        headers=HEADERS,
        timeout=10,
    )
    if r.status_code == 200:
        ok(f"Server reachable — HTTP {r.status_code}")
        models = r.json().get("data", [])
        if models:
            ok(f"Models tersedia: {[m.get('id') for m in models[:5]]}")
        else:
            info("Endpoint /models tidak return daftar model (mungkin proxy)")
    else:
        fail(f"HTTP {r.status_code} — {r.text[:200]}")
except requests.exceptions.ConnectionError as e:
    fail(f"Tidak bisa konek ke {QWEN_API_BASE} → {e}")
except requests.exceptions.Timeout:
    fail("Timeout saat cek reachability")

# ── Test 2: Basic Chat Completion ─────────────────────────────────────────────
section("TEST 2 — Basic Chat Completion")
payload = {
    "model": QWEN_MODEL,
    "messages": [
        {"role": "system", "content": "Kamu adalah asisten singkat. Jawab dalam 1 kalimat."},
        {"role": "user",   "content": "Siapa kamu dan model apa yang kamu pakai?"},
    ],
    "temperature": 0.7,
    "max_tokens":  150,
}

try:
    t0  = time.time()
    r   = requests.post(
        f"{QWEN_API_BASE}/chat/completions",
        headers=HEADERS,
        json=payload,
        timeout=TIMEOUT,
    )
    elapsed = time.time() - t0

    if r.status_code == 200:
        data    = r.json()
        content = data["choices"][0]["message"]["content"].strip()
        usage   = data.get("usage", {})
        ok(f"Response diterima dalam {elapsed:.1f}s")
        ok(f"Model di response: {data.get('model', '?')}")
        info(f"Jawaban: {content}")
        info(f"Tokens — prompt: {usage.get('prompt_tokens','?')} | completion: {usage.get('completion_tokens','?')} | total: {usage.get('total_tokens','?')}")
    else:
        fail(f"HTTP {r.status_code}")
        fail(f"Body: {r.text[:500]}")
except requests.exceptions.ConnectionError as e:
    fail(f"Connection error: {e}")
except requests.exceptions.Timeout:
    fail(f"Timeout setelah {TIMEOUT}s")
except (KeyError, IndexError) as e:
    fail(f"Parse response gagal: {e}")
    info(f"Raw response: {r.text[:500]}")

# ── Test 3: JSON Output (krusial untuk script_engine) ─────────────────────────
section("TEST 3 — JSON Output Mode (simulasi script_engine)")
json_payload = {
    "model": QWEN_MODEL,
    "messages": [
        {
            "role": "system",
            "content": (
                "Kamu adalah script writer. "
                "CRITICAL: Respond with ONLY a raw JSON object. "
                "No markdown, no ```json fences, no explanation. "
                "Start directly with { and end with }."
            ),
        },
        {
            "role": "user",
            "content": (
                "Topik: Rumah tua di pinggir hutan yang menyimpan rahasia gelap.\n\n"
                "Buat JSON dengan field: title, hook_line, script (maks 80 kata), tags (array 3 item)."
            ),
        },
    ],
    "temperature":       0.90,
    "top_p":             0.95,
    "frequency_penalty": 0.35,
    "max_tokens":        500,
}

try:
    t0  = time.time()
    r   = requests.post(
        f"{QWEN_API_BASE}/chat/completions",
        headers=HEADERS,
        json=json_payload,
        timeout=TIMEOUT,
    )
    elapsed = time.time() - t0

    if r.status_code == 200:
        raw = r.json()["choices"][0]["message"]["content"].strip()
        ok(f"Response diterima dalam {elapsed:.1f}s")
        info(f"Raw output (50 char pertama): {raw[:50]}")

        # Cek apakah ada <think> blocks (bahaya untuk script_engine)
        if "<think>" in raw:
            fail("BAHAYA: Ada <think>...</think> blocks di output! Model thinking mode aktif.")
            info("Solusi: Ganti ke model non-thinking atau tambah /no_think di prompt")
        else:
            ok("Tidak ada <think> blocks — aman untuk _parse_json_response()")

        # Coba parse JSON
        try:
            # Strip markdown fence kalau ada
            cleaned = raw
            if cleaned.startswith("```"):
                import re
                cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r'\s*```$', '', cleaned).strip()

            parsed = json.loads(cleaned)
            ok("JSON valid dan berhasil di-parse!")
            info(f"  title      : {parsed.get('title', '—')}")
            info(f"  hook_line  : {parsed.get('hook_line', '—')}")
            info(f"  script     : {str(parsed.get('script', '—'))[:80]}...")
            info(f"  tags       : {parsed.get('tags', '—')}")
        except json.JSONDecodeError as e:
            fail(f"JSON parse gagal: {e}")
            info(f"Raw (full): {raw[:300]}")
    else:
        fail(f"HTTP {r.status_code}")
        fail(f"Body: {r.text[:500]}")

except requests.exceptions.ConnectionError as e:
    fail(f"Connection error: {e}")
except requests.exceptions.Timeout:
    fail(f"Timeout setelah {TIMEOUT}s")

# ── Test 4: Bahasa Indonesia Tone ─────────────────────────────────────────────
section("TEST 4 — Bahasa Indonesia + Horror Tone")
id_payload = {
    "model": QWEN_MODEL,
    "messages": [
        {
            "role": "system",
            "content": "Kamu adalah narator horror Indonesia yang mengerikan. Gunakan bahasa yang mencekam.",
        },
        {
            "role": "user",
            "content": "Tulis opening hook 2 kalimat untuk video horror tentang ritual Jawa kuno yang terlarang.",
        },
    ],
    "temperature": 0.95,
    "max_tokens":  100,
}

try:
    t0  = time.time()
    r   = requests.post(
        f"{QWEN_API_BASE}/chat/completions",
        headers=HEADERS,
        json=id_payload,
        timeout=TIMEOUT,
    )
    elapsed = time.time() - t0

    if r.status_code == 200:
        content = r.json()["choices"][0]["message"]["content"].strip()
        ok(f"Response dalam {elapsed:.1f}s")
        info(f"Output:\n\n{content}\n")
        if "<think>" in content:
            fail("Ada <think> blocks — perlu ditangani")
        else:
            ok("Output bersih, tidak ada thinking blocks")
    else:
        fail(f"HTTP {r.status_code} — {r.text[:300]}")

except Exception as e:
    fail(f"Error: {e}")

# ── Summary ───────────────────────────────────────────────────────────────────
section("SELESAI")
print("  Cek hasil di atas:")
print("  ✅ = PASS   ❌ = FAIL   ℹ️  = INFO")
print()