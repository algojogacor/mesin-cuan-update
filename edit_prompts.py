import os
import shutil
import re

files = [
    "D:/mesin_cuan/templates/prompts/horror_id.txt",
    "D:/mesin_cuan/templates/prompts/horror_en.txt",
    "D:/mesin_cuan/templates/prompts/horror_id_long.txt",
    "D:/mesin_cuan/templates/prompts/horror_en_long.txt",
    "D:/mesin_cuan/templates/prompts/psychology_id.txt",
    "D:/mesin_cuan/templates/prompts/psychology_en.txt",
    "D:/mesin_cuan/templates/prompts/psychology_id_long.txt",
    "D:/mesin_cuan/templates/prompts/psychology_en_long.txt"
]

for file in files:
    if not os.path.exists(file):
        print(f"Skipping {file}, does not exist.")
        continue
        
    shutil.copy(file, file + ".bak")
    print(f"Backed up {file} to {file}.bak")
    
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
        
    is_id = "_id" in file
    
    # 1. HOOK FORMULA
    if is_id:
        hook_addition = """
Panduan 5 Emosi Primer (Pilih Satu):
1. Takut kehilangan (Fear of loss)
2. Rasa malu (Shame/Embarrassment)
3. Ancaman identitas (Identity threat)
4. Shock faktual (Factual shock)
5. Curiosity gap yang menyakitkan (Painful curiosity gap)

Tes Tweet Viral: Jika hook ini ditulis sebagai Tweet, apakah orang akan berhenti men-scroll?

CONTOH HOOK EKSTREM KONTRAS:
  "Kamu pikir [Asumsi Normal]? Salah. [Fakta Gelap Terbalik]."
"""
        content = content.replace("CONTOH HOOK YANG BENAR", hook_addition + "\nCONTOH HOOK YANG BENAR")
    else:
        hook_addition = """
5 Primary Emotions Guide (Choose One):
1. Fear of loss
2. Shame/Embarrassment
3. Identity threat
4. Factual shock
5. Painful curiosity gap

Viral Tweet Test: If this hook were a Tweet, would people stop scrolling?

EXTREME CONTRAST HOOK EXAMPLE:
  "You think [Normal Assumption]? Wrong. [Dark Twisted Fact]."
"""
        content = content.replace("EXAMPLE OF CORRECT HOOK", hook_addition + "\nEXAMPLE OF CORRECT HOOK")
        
    # 2. BADAN NARASI
    if is_id:
        badan_addition = """
  • RETENSI ANCHOR (Detik 8-12 / Kata 25-35): Berikan fakta mengejutkan pertama dengan cepat untuk menahan penonton yang hampir pergi.
  • DILARANG: menggunakan transisi generik seperti "Selain itu", "Kemudian", "Dan juga". Ganti dengan transisi yang membangun urgensi/ketegangan (contoh: "Tapi itu belum seberapa...", "Yang lebih parah...", "Puncaknya...")."""
        content = re.sub(r'(\[BADAN[^\]]*\]:)', r'\1' + badan_addition, content)
    else:
        badan_addition = """
  • RETENTION ANCHOR (Second 8-12 / Words 25-35): Deliver the first shocking fact quickly to retain viewers who are about to leave.
  • BANNED: generic transitions like "Additionally", "Next", "Also". Replace with urgency/tension building transitions (e.g., "But that's not even the worst part...", "What's more terrifying...", "The climax...")."""
        content = re.sub(r'(\[BODY[^\]]*\]:)', r'\1' + badan_addition, content)

    # 3. PENUTUP & CTA
    if is_id:
        cta_addition = """    - "Cek link di bio untuk fakta yang lebih gila."
    - "Apakah kamu sadar? Komen di bawah."
    - "Tag temanmu yang harus tahu ini sebelum terlambat." """
        # Find the end of the CTA list which is right before the visual instructions section
        content = re.sub(r'((\n\s+- "[^"]*"\.?)+)', r'\1\n' + cta_addition, content, count=1)
    else:
        cta_addition = """    - "Check the link in bio for crazier facts."
    - "Did you realize this? Comment below."
    - "Tag a friend who needs to know this before it's too late." """
        content = re.sub(r'((\n\s+- "[^"]*"\.?)+)', r'\1\n' + cta_addition, content, count=1)

    # 4. VISUAL CUES formatting rules
    if is_id:
        vis_rule = """
Wajib menghasilkan 3–4 instruksi visual/B-roll yang spesifik dan actionable untuk editor video.
Setiap instruksi harus mendeskripsikan: WAJIB menggunakan keyword sinematik spesifik dalam Bahasa Inggris. BUKAN efek editing generik.
Contoh BENAR: "cinematic wide shot, abandoned asylum, eerie fog, 4k resolution"
Contoh SALAH: "rumah sakit kosong + zoom in + efek glitch"
"""
        content = re.sub(r'Wajib menghasilkan.*?Contoh format: [^\n]+', vis_rule.strip(), content, flags=re.DOTALL)
    else:
        vis_rule = """
Must produce 3–4 specific, actionable visual/B-roll instructions for the video editor.
Each instruction MUST use specific cinematic English keywords, NOT generic editing effects.
Correct Example: "cinematic wide shot, abandoned asylum, eerie fog, 4k resolution"
Wrong Example: "empty hospital + zoom in + glitch effect"
"""
        content = re.sub(r'Must produce.*?Example format: [^\n]+', vis_rule.strip(), content, flags=re.DOTALL)

    # 5. CONTOH LENGKAP (Second example block)
    if is_id:
        second_example = """
════════════════════════════════
CONTOH LENGKAP 2 (Topik Baru + Retensi Anchor + Visual Cues Benar)
════════════════════════════════

SCRIPT:
"Otakmu bisa dimanipulasi dengan mudah. Eksperimen MKUltra membuktikan CIA pernah mencuci otak ribuan orang tanpa mereka sadari. Dan tidak ada yang tahu apakah kamu salah satunya — sampai saat itu datang. Di tahun 1950-an, mereka memberikan LSD ke warga sipil tanpa izin, mencoba mengontrol pikiran sepenuhnya. Tapi itu belum seberapa... dokumen rahasia yang bocor menunjukkan proyek ini mungkin tidak pernah benar-benar dihentikan, hanya berganti nama. Pikiranmu mungkin sedang diretas saat kamu menonton video ini. Tag temanmu yang harus tahu ini sebelum terlambat."

VISUAL_CUES untuk contoh di atas:
- "cinematic close up, human eye dilating, dark lighting, surreal atmosphere"
- "declassified MKUltra documents, red redaction markers, top secret folder, spy thriller style"
- "shadowy figure behind glass, low key lighting, interrogation room, silhouette"
- "pitch black screen, subtle static noise, minimalist typography"
"""
        content = content.replace("FORMAT OUTPUT", "CONTOH LENGKAP 2 (Topik Baru + Retensi Anchor + Visual Cues Benar)\n" + second_example.strip() + "\n\n════════════════════════════════\nFORMAT OUTPUT")
    else:
        second_example = """
════════════════════════════════
FULL CORRECT EXAMPLE 2 (New Topic + Retention Anchor + Correct Visual Cues)
════════════════════════════════

SCRIPT:
"Your brain can be easily manipulated. The MKUltra experiments proved the CIA successfully brainwashed thousands without their knowledge. And nobody knows if you are one of them — until the trigger moment arrives. In the 1950s, they dosed civilians with LSD without consent to achieve total mind control. But that's not even the worst part... leaked classified documents suggest this project may have never truly ended, only changed names. Your thoughts might be hacked right as you watch this video. Tag a friend who needs to know this before it's too late."

VISUAL_CUES for the example above:
- "cinematic close up, human eye dilating, dark lighting, surreal atmosphere"
- "declassified MKUltra documents, red redaction markers, top secret folder, spy thriller style"
- "shadowy figure behind glass, low key lighting, interrogation room, silhouette"
- "pitch black screen, subtle static noise, minimalist typography"
"""
        content = content.replace("OUTPUT FORMAT", "FULL CORRECT EXAMPLE 2 (New Topic + Retention Anchor + Correct Visual Cues)\n" + second_example.strip() + "\n\n════════════════════════════════\nOUTPUT FORMAT")

    with open(file, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print(f"Successfully processed {file}")
