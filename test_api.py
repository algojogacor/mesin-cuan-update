import os
from dotenv import load_dotenv

# Load API Keys dari file .env
load_dotenv()

print("🔍 MEMULAI TES API...\n")

# ==========================================
# 1. TEST GROQ API
# ==========================================
print("▶ Mengetes GROQ...")
try:
    from groq import Groq
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        print("❌ GROQ_API_KEY tidak ditemukan di file .env!")
    else:
        client = Groq(api_key=groq_key)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": "Balas pesan ini dengan kalimat: 'Halo, Groq siap bekerja!'"}]
        )
        print(f"✅ SUKSES Groq menjawab: {response.choices[0].message.content}")
except Exception as e:
    print(f"❌ GAGAL Groq Error: {e}")

print("-" * 40)

# ==========================================
# 2. TEST GEMINI API (Versi Baru)
# ==========================================
print("▶ Mengetes GEMINI...")
try:
    from google import genai
    import os
    
    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        print("❌ GEMINI_API_KEY tidak ditemukan di file .env!")
    else:
        # SDK Baru tidak butuh genai.configure
        client = genai.Client(api_key=gemini_key)
        
        # Kita panggil gemini-2.0-flash pakai format terbaru
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents="Balas pesan ini dengan kalimat: 'Halo, Gemini siap bekerja!'"
        )
        print(f"✅ SUKSES Gemini menjawab: {response.text.strip()}")
except Exception as e:
    print(f"❌ GAGAL Gemini Error: {e}")

print("\n🏁 TES SELESAI.")