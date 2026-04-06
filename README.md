<div align="center">

```
██████████████████████████████████████████████████████████
█                                                        █
█   ███╗   ███╗███████╗███████╗██╗███╗   ██╗            █
█   ████╗ ████║██╔════╝██╔════╝██║████╗  ██║            █
█   ██╔████╔██║█████╗  ███████╗██║██╔██╗ ██║            █
█   ██║╚██╔╝██║██╔══╝  ╚════██║██║██║╚██╗██║            █
█   ██║ ╚═╝ ██║███████╗███████║██║██║ ╚████║            █
█                                                        █
█          C U A N   V I R A L   A R C H I T E C T      █
█                                                       █
█                                                        █
██████████████████████████████████████████████████████████
```

### *Automate the Fame. Master the Algorithm.*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![FFmpeg](https://img.shields.io/badge/FFmpeg-7.x-007808?style=flat-square&logo=ffmpeg&logoColor=white)](https://ffmpeg.org)
[![YouTube API](https://img.shields.io/badge/YouTube_API-v3-FF0000?style=flat-square&logo=youtube)](https://developers.google.com/youtube)
[![Gemini](https://img.shields.io/badge/Gemini_AI-2.5_Pro-4285F4?style=flat-square&logo=google)](https://ai.google.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)](LICENSE)

</div>

---

## 🌐 Language / Bahasa

> **[🇮🇩 Bahasa Indonesia](#-bahasa-indonesia)** &nbsp;|&nbsp; **[🇬🇧 English](#-english)**

---

<br/>

# 🇮🇩 Bahasa Indonesia

## Visi & Misi

> *"Ini bukan bot. Ini arsitek konten berbasis AI yang bekerja tanpa henti — menemukan tren, menulis naskah, merender video sinematik, dan mengupload ke YouTube, semuanya otomatis."*

**Mesin Cuan Viral Architect v5** adalah sistem produksi konten YouTube bertenaga AI yang beroperasi penuh 24/7. Ia bukan sekadar script; ia adalah **pabrik video otonom** yang memahami algoritma, mendeteksi tren sebelum viral, dan mengeksekusi setiap frame dengan presisi sinematik.

Bayangkan memiliki tim produksi lengkap — riset tren, penulis naskah, voice-over artist, video editor, dan manajer upload — yang bekerja tanpa istirahat, tanpa gaji, tanpa keluhan. Itulah Mesin Cuan.

---

## ⚙️ Core Engines

### 📡 Viral Loop Engine
Mesin riset tren real-time yang memadukan **YouTube Data API v3**, **Google Trends (via Cloudflare)**, dan **YouTube Search Suggestions**. Hasilnya disaring oleh AI Ollama untuk memilih topik dengan potensi viral tertinggi — diperbarui setiap 6 jam tanpa intervensi manual.

### 🌟 Neon Visuals v5
Renderer video sinematik berbasis FFmpeg dengan efek estetik premium:
- **Teks glowing neon** dengan animasi masuk yang dinamis
- **Gradient panel glassmorphism** sebagai latar narasi
- **Cinematic letterbox** + vignette + color grading otomatis
- Output: **Shorts 9:16 (60 detik)** & **Long Form 16:9 (8–12 menit)**

### 🔊 Smart SFX Mixer
Lapisan suara cerdas yang menyesuaikan sound effect berdasarkan niche konten:
- 🩸 **Horror** → heartbeat, thunderclap, whisper ambience
- 🧠 **Psychology** → mind-tone, focus hum, deep bass
- 💪 **Motivation** → crowd cheer, stadium echo, power hit
- 📜 **History** → parchment ambience, dramatic orchestral

### 📊 OAuth2 Analytics
Koneksi langsung ke **YouTube Analytics API v2** menggunakan OAuth2 Authorization Code Flow. Dashboard retensi per channel memberikan insight mendalam tentang performa video, drop-off poin, dan pola engagement untuk optimasi konten berikutnya.

---

## 🛠️ Tech Stack

| Komponen | Teknologi | Fungsi |
|---|---|---|
| **Bahasa** | Python 3.11+ | Orkestrasi pipeline |
| **Rendering** | FFmpeg 7.x | Render video & audio |
| **AI Script** | Ollama · Gemini · Groq · Claude | Penulisan naskah otomatis (fallback chain) |
| **AI Vision QC** | Google Gemini Vision | Quality control video |
| **Text-to-Speech** | Google Cloud TTS · Edge TTS · Coqui | 50+ suara multilingual |
| **Footage** | Pexels API · Pixabay API | B-roll footage + clip cache |
| **SFX** | Freesound API | Sound effect otomatis |
| **Upload** | YouTube Data API v3 (OAuth2) | Upload & scheduling |
| **Analytics** | YouTube Analytics API v2 (OAuth2) | Retensi & insight |
| **Trending** | YouTube API · Cloudflare Browser Rendering | Deteksi tren real-time |
| **Storage** | Google Drive API v3 | Antrian upload |
| **Notifikasi** | Telegram Bot API | Alert real-time |
| **Worker** | Koyeb | Upload otomatis dari cloud |

---

## 🚀 Instalasi & Setup

### Langkah 1 — Clone & Environment

```bash
git clone https://github.com/algojogacor/mesin-cuan.git
cd mesin-cuan

python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Linux / Mac

pip install -r requirements.txt
```

### Langkah 2 — Konfigurasi `.env`

Buat file `.env` di root folder:

```env
# ── AI ───────────────────────────────────────────
GEMINI_API_KEY=your_key
GROQ_API_KEY=your_key
ANTHROPIC_API_KEY=your_key

# ── Footage ───────────────────────────────────────
PEXELS_API_KEY=your_key
PIXABAY_API_KEY=your_key
FREESOUND_API_KEY=your_key          # Opsional

# ── YouTube & Google ──────────────────────────────
YT_API_KEY=your_key
GOOGLE_CLIENT_ID=your_id
GOOGLE_CLIENT_SECRET=your_secret
GOOGLE_DRIVE_FOLDER_ID=your_folder

# ── Notifikasi ────────────────────────────────────
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id

# ── Cloudflare Trends ─────────────────────────────
CF_ACCOUNT_ID=your_id
CF_API_TOKEN=your_token
```

### Langkah 3 — Setup Assets & Auth

```bash
# Download SFX pack + buat folder assets
python tools/setup_assets.py

# OAuth2 YouTube (buka browser, login sekali)
python setup_auth.py

# Verifikasi semua koneksi
python check_requirements.py
```

---

## 💻 Cara Penggunaan

```bash
# Jalankan semua channel sesuai campaign
python main.py

# Jalankan channel tertentu saja
python main.py --channel ch_id_horror

# Jalankan semua channel sekaligus
python main.py --all

# Preview jadwal tanpa render
python main.py --preview

# Test pipeline tanpa upload ke GDrive
python main.py --dry-run

# Skip QC Vision (lebih cepat, untuk testing)
python main.py --skip-qc

# Update dashboard analytics retensi
python main.py --analytics
```

---

<br/>

# 🇬🇧 English

## The Vision

> *"This isn't a bot. It's an AI Video Architect that works around the clock — discovering trends, writing scripts, rendering cinematic footage, and uploading to YouTube, fully automated."*

**Mesin Cuan Viral Architect v5** is a 24/7 AI-powered YouTube content production system. It's not merely a script; it's an **autonomous video factory** that understands the algorithm, detects trends before they peak, and executes every frame with cinematic precision.

Imagine having a complete production team — trend researcher, scriptwriter, voice actor, video editor, and upload manager — working without rest, without salary, without complaints. That's Mesin Cuan.

---

## ⚙️ Core Engines

### 📡 Viral Loop Engine
A real-time trend intelligence system combining **YouTube Data API v3**, **Google Trends (via Cloudflare)**, and **YouTube Search Suggestions**. Results are filtered by Ollama AI to select topics with the highest viral potential — refreshed every 6 hours, zero manual effort required.

### 🌟 Neon Visuals v5
A cinematic video renderer built on FFmpeg with premium aesthetic effects:
- **Glowing neon text** with dynamic entrance animations
- **Glassmorphism gradient panels** as narration backgrounds
- **Cinematic letterbox** + vignette + automatic color grading
- Output: **Shorts 9:16 (60s)** & **Long Form 16:9 (8–12 min)**

### 🔊 Smart SFX Mixer
An intelligent audio layer that maps sound effects to content niche:
- 🩸 **Horror** → heartbeat, thunderclap, whisper ambience
- 🧠 **Psychology** → mind-tone, focus hum, deep bass
- 💪 **Motivation** → crowd cheer, stadium echo, power hit
- 📜 **History** → parchment ambience, dramatic orchestral

### 📊 OAuth2 Analytics
Direct integration with **YouTube Analytics API v2** via OAuth2 Authorization Code Flow. A per-channel retention dashboard delivers deep insight into video performance, audience drop-off points, and engagement patterns for continuous content optimization.

---

## 🛠️ Tech Stack

| Layer | Technology | Role |
|---|---|---|
| **Language** | Python 3.11+ | Pipeline orchestration |
| **Rendering** | FFmpeg 7.x | Video & audio composition |
| **AI Script** | Ollama · Gemini · Groq · Claude | Automated scriptwriting (fallback chain) |
| **AI Vision QC** | Google Gemini Vision | Automated quality control |
| **Text-to-Speech** | Google Cloud TTS · Edge TTS · Coqui | 50+ multilingual voices |
| **Footage** | Pexels API · Pixabay API | B-roll footage + clip cache |
| **SFX** | Freesound API | Automated sound effects |
| **Upload** | YouTube Data API v3 (OAuth2) | Scheduled uploads |
| **Analytics** | YouTube Analytics API v2 (OAuth2) | Retention & insights |
| **Trending** | YouTube API · Cloudflare Rendering | Real-time trend detection |
| **Storage** | Google Drive API v3 | Upload queue |
| **Notifications** | Telegram Bot API | Real-time alerts |
| **Worker** | Koyeb | Cloud-based upload automation |

---

## 🚀 Installation & Setup

### Step 1 — Clone & Environment

```bash
git clone https://github.com/algojogacor/mesin-cuan.git
cd mesin-cuan

python -m venv venv
source venv/bin/activate          # Linux / Mac
# venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

### Step 2 — Configure `.env`

Create a `.env` file in the root directory:

```env
# ── AI ───────────────────────────────────────────
GEMINI_API_KEY=your_key
GROQ_API_KEY=your_key
ANTHROPIC_API_KEY=your_key

# ── Footage ───────────────────────────────────────
PEXELS_API_KEY=your_key
PIXABAY_API_KEY=your_key
FREESOUND_API_KEY=your_key          # Optional

# ── YouTube & Google ──────────────────────────────
YT_API_KEY=your_key
GOOGLE_CLIENT_ID=your_id
GOOGLE_CLIENT_SECRET=your_secret
GOOGLE_DRIVE_FOLDER_ID=your_folder

# ── Notifications ─────────────────────────────────
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id

# ── Cloudflare Trends ─────────────────────────────
CF_ACCOUNT_ID=your_id
CF_API_TOKEN=your_token
```

### Step 3 — Asset Bootstrap & Auth

```bash
# Download SFX packs and initialize asset folders
python tools/setup_assets.py

# One-time OAuth2 YouTube authorization (opens browser)
python setup_auth.py

# Verify all connections and dependencies
python check_requirements.py
```

---

## 💻 Usage

```bash
# Run all channels according to campaign schedule
python main.py

# Run a specific channel only
python main.py --channel ch_id_horror

# Run all channels simultaneously
python main.py --all

# Preview campaign schedule without rendering
python main.py --preview

# Full pipeline test without uploading to GDrive
python main.py --dry-run

# Skip QC Vision for faster iteration
python main.py --skip-qc

# Refresh per-channel retention analytics
python main.py --analytics
```

---

<div align="center">

---

## 📜 License & Permission / Lisensi & Izin

> **Mesin Cuan Viral Architect v5** dilindungi oleh **Custom Proprietary License**.  
> Kode ini dibuat publik untuk tujuan edukatif dan portofolio — **bukan open-source**.

### 🇮🇩 Ketentuan Penggunaan (Indonesia)

| Aktivitas | Status |
|---|---|
| 📖 Mempelajari kode untuk keperluan pribadi | ✅ Diizinkan |
| 🔬 Referensi akademik non-komersial | ✅ Diizinkan |
| 🍴 Fork untuk eksperimen pribadi (tidak dipublikasikan) | ✅ Diizinkan |
| 💰 Penggunaan komersial (layanan berbayar, SaaS, dll.) | ❌ **Dilarang tanpa izin** |
| 📢 Distribusi publik / deploy sebagai produk | ❌ **Dilarang tanpa izin** |
| ✏️ Modifikasi dan distribusi ulang | ❌ **Dilarang tanpa izin** |

**Untuk meminta izin forking, modifikasi, distribusi, atau penggunaan komersial:**

> 📩 Hubungi via Instagram: **[@aryarizky04](https://www.instagram.com/aryarizky04/)**  
> Semua permintaan ditinjau secara individual. Otorisasi harus diterima **secara tertulis** sebelum aktivitas dimulai.

---

### 🇬🇧 Terms of Use (English)

| Activity | Status |
|---|---|
| 📖 Studying the code for personal education | ✅ Permitted |
| 🔬 Non-commercial academic reference | ✅ Permitted |
| 🍴 Private fork for personal experimentation (not public) | ✅ Permitted |
| 💰 Commercial use (paid service, SaaS, revenue generation) | ❌ **Prohibited without permission** |
| 📢 Public distribution / deploy as a product | ❌ **Prohibited without permission** |
| ✏️ Modification and redistribution | ❌ **Prohibited without permission** |

**To request permission for forking, modification, distribution, or commercial use:**

> 📩 Contact via Instagram: **[@aryarizky04](https://www.instagram.com/aryarizky04/)**  
> All requests are reviewed on a case-by-case basis. Written authorization must be received before any restricted activity begins.

---

*Mesin Cuan Viral Architect v5 — Custom Proprietary License*

**Built for creators who refuse to be limited by time.**

```
[ 🤖 AI-POWERED ]  [ 🎬 CINEMATIC ]  [ 📡 VIRAL-READY ]  [ 🔄 24/7 AUTO ]
```

</div>