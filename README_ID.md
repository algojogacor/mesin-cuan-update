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

![Demo](docs/demo.gif)

> 📖 [🇬🇧 Read in English](README.md)

---

## Visi & Misi

> *"Ini bukan bot. Ini arsitek konten berbasis AI yang bekerja tanpa henti — menemukan tren, menulis naskah, merender video sinematik, dan mengupload ke YouTube, semuanya otomatis."*

**Mesin Cuan Viral Architect** adalah sistem produksi konten YouTube bertenaga AI yang beroperasi penuh 24/7. Ia bukan sekadar script; ia adalah **pabrik video otonom** yang memahami algoritma, mendeteksi tren sebelum viral, dan mengeksekusi setiap frame dengan presisi sinematik.

Bayangkan memiliki tim produksi lengkap — riset tren, penulis naskah, voice-over artist, video editor, dan manajer upload — yang bekerja tanpa istirahat, tanpa gaji, tanpa keluhan. Itulah Mesin Cuan.

---

## 🤔 Kenapa Mesin Cuan?

| | Editing Manual | Hiring Editor | AI Tools Lain | **Mesin Cuan** |
|---|---|---|---|---|
| **Biaya** | Waktu kamu (8j/video) | Rp 7-30jt/bulan | Rp 300rb-1.5jt/bln | **Gratis (self-hosted)** |
| **Kecepatan** | 1 video/hari | 2-3 video/minggu | 1-2 video/hari | **12+ video/hari** |
| **Kualitas** | Tergantung skill | Bervariasi | Terasa AI generik | **Dual AI cross-scored** |
| **Privasi Data** | ✅ | ❌ Dibagi ke editor | ❌ Upload ke cloud | **✅ 100% lokal** |
| **Operasi 24/7** | ❌ | ❌ | ❌ | **✅ Otonom** |

Inti alurnya: **Provider-A (Mistral) + Provider-B (DeepSeek) generate naskah secara paralel**, lalu **saling menilai** untuk mengeliminasi AI slop sebelum masuk produksi.

---

## 🏗️ Arsitektur

```
 Campaign    →  Topic Engine  →  Research Engine
 Schedule        (tren)            (konteks web)
                                        ↓
    ┌───────────────────────────────────┴───────────────────────────────┐
    │                  DUAL PARALLEL AI GENERATION                       │
    │  ┌──────────┐     cross-score     ┌──────────┐                    │
    │  │ PROVIDER-A│ ←───────────────→ │PROVIDER-B│                    │
    │  │  Mistral  │                    │ DeepSeek │                    │
    │  └────┬─────┘                    └────┬─────┘                    │
    │       └──────────┬───────────────────┘                          │
    │                  ↓                                              │
    │         Script Quality Scorer                                   │
    │    (hook · pacing · curiosity · retention)                       │
    └──────────────────────┬──────────────────────────────────────────┘
                           ↓
    Hook → TTS → Footage → FFmpeg → Vision QC → Thumbnail → GDrive → ☁️ Uploader
    Engine   (F5)  (Pexels)  Render    (NVIDIA)   Intelligence  Queue      (Koyeb)
```

> **Alur auto-upload:** Mesin Cuan render video di lokal → push ke antrian Google Drive → [`mesin-cuan-uploader`](https://github.com/algojogacor/mesin-cuan-uploader) (di-host di Koyeb/Railway) ambil dari GDrive dan upload ke YouTube dengan jadwal terjeda. Arsitektur dua-langkah ini mencegah YouTube mendeteksi upload massal dari satu IP.

---

## ⚙️ Arsitektur 12 Engine

### 📡 Viral Loop Engine
Riset tren real-time menggabungkan **YouTube Data API v3**, **Google Trends**, dan **YouTube Search Suggestions** — diperbarui setiap 6 jam.

### 🧠 Dual Parallel AI Scripting
**Provider-A (Mistral) + Provider-B (DeepSeek) generate naskah secara paralel**, lalu saling menilai dengan cross-provider scoring (anti-sycophancy). Skor tertinggi yang lanjut ke produksi. Fallback chain: Provider-A → Provider-B → Groq.

### 🎯 Script Quality Scorer
Multi-dimensional scoring: **hook strength**, **curiosity gap**, **pacing**, **emotional impact**, **retention prediction** — naskah di bawah threshold otomatis ditulis ulang.

### 🔗 Hook Engine
Auto-inject hook pembuka yang mempertahankan retensi penonton di 3 detik pertama — berbasis data retention analytics per channel.

### 📚 Memory Engine
Melacak topik yang sudah diproduksi — mencegah pengulangan konten dan memastikan variasi antar video.

### 🔍 Research Engine
Web research otomatis sebelum penulisan naskah — memastikan fakta akurat dan konteks terkini.

### 📺 Series Engine
Auto-generate konten berseri (Part 1, 2, 3...) dengan cliffhanger logic dan call-back ke episode sebelumnya.

### 🌟 Neon Visuals
Renderer video sinematik FFmpeg: **teks glowing neon**, **glassmorphism panels**, **cinematic letterbox** + vignette + color grading.

### 🔊 Smart SFX Mixer
Sound effect otomatis berdasarkan niche: Horror → heartbeat/thunder, Psychology → mind-tone/focus hum, Motivation → crowd cheer/stadium echo.

### 🖼️ Thumbnail Intelligence
AI-driven thumbnail text & style selection berdasarkan niche dan topik — optimasi CTR.

### 📊 OAuth2 Analytics
Integrasi **YouTube Analytics API v2** — dashboard retensi per channel, analisis drop-off, optimasi konten.

### ⚡ Pipeline Estimator
Prediksi ETA untuk batch render multi-channel — estimasi akurat sebelum eksekusi.

---

## 🛠️ Tech Stack

| Komponen | Teknologi | Fungsi |
|---|---|---|
| **Bahasa** | Python 3.11+ | Orkestrasi pipeline |
| **AI Primary** | Provider-A Mistral + Provider-B DeepSeek | Dual parallel script generation + cross-scoring |
| **AI Quality** | Script Quality Scorer | Multi-dimensional validation |
| **AI Vision QC** | NVIDIA API / Ollama Vision | Quality control video |
| **AI Fallback** | Groq | Last-resort script generation |
| **Text-to-Speech** | F5-TTS · Edge TTS · Coqui | 50+ suara multilingual |
| **Rendering** | FFmpeg 7.x | Render video & audio |
| **Footage** | Pexels API · Pixabay API | B-roll footage + clip cache |
| **SFX** | Freesound API | Sound effect otomatis |
| **Upload** | YouTube Data API v3 (OAuth2) | Upload & scheduling |
| **Analytics** | YouTube Data API v3 | Retensi & insight |
| **Trending** | YouTube API · Google Trends | Deteksi tren real-time |
| **Research** | Perplexity API / Ollama | Konteks akurat sebelum naskah |
| **Storage** | Google Drive API v3 | Antrian upload |
| **Notifikasi** | Telegram Bot API | Alert real-time |
| **Scheduler** | APScheduler | Campaign automation |

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

Salin `.env.example` ke `.env` dan isi API key kamu:

```bash
cp .env.example .env
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

# Preview jadwal tanpa render
python main.py --preview

# Test pipeline tanpa upload ke GDrive
python main.py --dry-run

# Skip QC Vision (lebih cepat, untuk testing)
python main.py --skip-qc

# Update dashboard analytics retensi
python main.py --analytics

# Generate naskah saja (tanpa render)
python main.py --script-only

# Review kualitas hook tanpa render penuh
python main.py --review-hook

# Pilih profil: shorts atau long_form
python main.py --profile long_form

# Aktifkan debug logging
python main.py --debug
```

---

## 🔑 API Key Reference

### 🔴 WAJIB — Pipeline tidak berfungsi tanpa ini

| Key | Provider | Fungsi | Cara Dapat |
|---|---|---|---|
| `YOUTUBE_API_KEY` | YouTube Data API v3 | Trending detection & topic discovery | [Google Cloud Console](https://console.cloud.google.com/apis/credentials) |
| `PEXELS_API_KEY` | Pexels | B-roll footage & foto | [pexels.com/api](https://www.pexels.com/api/) |
| `GOOGLE_CLIENT_ID` | Google OAuth2 | Upload YouTube + Google Drive | [Google Cloud Console](https://console.cloud.google.com/apis/credentials) |
| `GOOGLE_CLIENT_SECRET` | Google OAuth2 | Pasangan GOOGLE_CLIENT_ID | [Google Cloud Console](https://console.cloud.google.com/apis/credentials) |
| `GOOGLE_DRIVE_FOLDER_ID` | Google Drive | Folder antrian upload | Klik kanan folder GDrive → Share → Copy link |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API | Notifikasi real-time | [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID` | Telegram | Tujuan notifikasi | [@userinfobot](https://t.me/userinfobot) |

### 🟠 AI Provider — Minimal 1, rekomendasi isi semua untuk fallback

| Key | Provider | Fungsi | Cara Dapat |
|---|---|---|---|
| `GEMINI_API_KEY` | Google Gemini | **Rekomendasi** — script generation & QC Vision | [aistudio.google.com/apikey](https://aistudio.google.com/apikey) |
| `PROVIDER_A_API_KEY` | NVIDIA NIM / Mistral Large 3 | Primary script generation | [integrate.api.nvidia.com](https://integrate.api.nvidia.com) |
| `PROVIDER_B_API_KEY` | DeepSeek V4 Pro | Scoring dan evaluasi | [platform.deepseek.com](https://platform.deepseek.com) |
| `GROQ_API_KEY` | Groq | Fallback LLM cepat | [console.groq.com/keys](https://console.groq.com/keys) |

> ⚠️ Tanpa AI Provider, script generation tidak bisa jalan. Isi minimal 1.

### 🟡 OPSIONAL — Tool tetap jalan, fitur tertentu skip

| Key | Provider | Fungsi | Dampak Jika Kosong |
|---|---|---|---|
| `PIXABAY_API_KEY` | Pixabay | Footage fallback | Fallback footage tidak tersedia |
| `PERPLEXITY_API_KEY` | Perplexity | Web research — konteks akurat | Research pakai Ollama lokal |
| `ANTHROPIC_API_KEY` | Anthropic | Last-resort AI fallback | Hanya 3 tier fallback |
| `NVIDIA_API_KEY` | NVIDIA NIM | Vision QC alternatif (gratis) | QC pakai Gemini saja |
| `ASSEMBLYAI_API_KEY` | AssemblyAI | Transkrip podcast | Tanpa fitur transkrip |
| `ELEVENLABS_API_KEY` | ElevenLabs | TTS suara premium | Fallback: Edge TTS gratis |
| `FREESOUND_API_KEY` | Freesound | SFX otomatis berbasis niche | Tanpa sound effect |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare | Google Trends scraping | Trending detection terbatas |
| `CLOUDFLARE_API_TOKEN` | Cloudflare | Pasangan ACCOUNT_ID | — |
| `COVERR_API_KEY` | Coverr | Stock video gratis | Alternatif footage |
| `COVERR_APP_ID` | Coverr | Pasangan COVERR_API_KEY | — |

> 💡 **Pro tip:** Jalankan `python main.py --setup` untuk setup wizard interaktif — tidak perlu edit `.env` manual!

### ⚡ Quick Start (Mode Minimal)

Cuma butuh **Ollama + FFmpeg** — tanpa API key:

```bash
# 1. Install Ollama dan pull model
ollama pull llama3.3:latest

# 2. Install FFmpeg
# Ubuntu: sudo apt install ffmpeg
# macOS: brew install ffmpeg
# Windows: https://ffmpeg.org/download.html

# 3. Setup API key via wizard (atau skip — pakai Ollama saja)
python main.py --setup

# 4. Jalankan dengan Ollama saja (skip footage, upload, analytics)
python main.py --channel ch_id_horror --skip-qc
```

---

<div align="center">

---

## 📜 Lisensi

**MIT License** — bebas pakai, modifikasi, dan distribusi untuk keperluan apapun, termasuk komersial.

Lihat [LICENSE](LICENSE) untuk teks lengkap.

---

*Dibuat untuk kreator yang menolak dibatasi oleh waktu.*

```
[ 🤖 AI-POWERED ]  [ 🎬 CINEMATIC ]  [ 📡 VIRAL-READY ]  [ 🔄 24/7 AUTO ]
```

</div>
