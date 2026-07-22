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

> 📖 [🇮🇩 Baca dalam Bahasa Indonesia](README_ID.md)

---

## The Vision

> *"This isn't a bot. It's an AI Video Architect that works around the clock — discovering trends, writing scripts, rendering cinematic footage, and uploading to YouTube, fully automated."*

**Mesin Cuan Viral Architect** is a 24/7 AI-powered YouTube content production system. It's not merely a script; it's an **autonomous video factory** that understands the algorithm, detects trends before they peak, and executes every frame with cinematic precision.

Imagine having a complete production team — trend researcher, scriptwriter, voice actor, video editor, and upload manager — working without rest, without salary, without complaints. That's Mesin Cuan.

---

## 🤔 Why Mesin Cuan?

| | Manual Editing | Hiring Editors | Other AI Tools | **Mesin Cuan** |
|---|---|---|---|---|
| **Cost** | Your time (8h/video) | $500-2000/month | $20-100/month | **Free (self-hosted)** |
| **Speed** | 1 video/day | 2-3 videos/week | 1-2 videos/day | **12+ videos/day** |
| **Quality** | Skill-dependent | Variable | Generic AI feel | **Dual AI cross-scored** |
| **Data Privacy** | ✅ | ❌ Shared with editor | ❌ Uploaded to cloud | **✅ 100% local** |
| **24/7 Operation** | ❌ | ❌ | ❌ | **✅ Autonomous** |

The core flow: **Provider-A (Mistral) + Provider-B (DeepSeek) generate scripts in parallel**, then **cross-score each other** to eliminate AI slop before it reaches production.

---

## 🏗️ Architecture

```
 Campaign    →  Topic Engine  →  Research Engine
 Schedule        (trends)          (web context)
                                        ↓
    ┌───────────────────────────────────┴───────────────────────────────┐
    │                    DUAL PARALLEL AI GENERATION                     │
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

> **Auto-upload flow:** Mesin Cuan renders videos locally → pushes to Google Drive queue → [`mesin-cuan-uploader`](https://github.com/algojogacor/mesin-cuan-uploader) (deployed on Koyeb/Railway) pulls from GDrive and uploads to YouTube with paced scheduling. This two-step architecture prevents YouTube from flagging bulk uploads from a single IP.

---

## ⚙️ 12-Engine Architecture

### 📡 Viral Loop Engine
Real-time trend intelligence combining **YouTube Data API v3**, **Google Trends**, and **YouTube Search Suggestions** — refreshed every 6 hours.

### 🧠 Dual Parallel AI Scripting
**Provider-A (Mistral) + Provider-B (DeepSeek) generate scripts in parallel**, then cross-score each other (anti-sycophancy). Highest-scoring script proceeds to production. Fallback chain: Provider-A → Provider-B → Groq.

### 🎯 Script Quality Scorer
Multi-dimensional scoring: **hook strength**, **curiosity gap**, **pacing**, **emotional impact**, **retention prediction** — scripts below threshold are auto-rewritten.

### 🔗 Hook Engine
Auto-injects opening hooks that maintain viewer retention in the first 3 seconds — driven by per-channel retention analytics.

### 📚 Memory Engine
Tracks previously produced topics — prevents content repetition and ensures variety across videos.

### 🔍 Research Engine
Automated web research before script generation — ensures factual accuracy and up-to-date context.

### 📺 Series Engine
Auto-generates multi-part content (Part 1, 2, 3...) with cliffhanger logic and cross-episode callbacks.

### 🌟 Neon Visuals
Cinematic FFmpeg renderer: **glowing neon text**, **glassmorphism panels**, **cinematic letterbox** + vignette + color grading.

### 🔊 Smart SFX Mixer
Niche-aware sound effects: Horror → heartbeat/thunder, Psychology → mind-tone/focus hum, Motivation → crowd cheer/stadium echo.

### 🖼️ Thumbnail Intelligence
AI-driven thumbnail text & style selection based on niche and topic — CTR optimization.

### 📊 OAuth2 Analytics
**YouTube Analytics API v2** integration — per-channel retention dashboard, drop-off analysis, content optimization.

### ⚡ Pipeline Estimator
ETA prediction for multi-channel batch renders — accurate estimation before execution.

---

## 🛠️ Tech Stack

| Layer | Technology | Role |
|---|---|---|
| **Language** | Python 3.11+ | Pipeline orchestration |
| **AI Primary** | Provider-A Mistral + Provider-B DeepSeek | Dual parallel script generation + cross-scoring |
| **AI Quality** | Script Quality Scorer | Multi-dimensional validation |
| **AI Vision QC** | NVIDIA API / Ollama Vision | Automated quality control |
| **AI Fallback** | Groq | Last-resort script generation |
| **Text-to-Speech** | F5-TTS · Edge TTS · Coqui | 50+ multilingual voices |
| **Rendering** | FFmpeg 7.x | Video & audio composition |
| **Footage** | Pexels API · Pixabay API | B-roll footage + clip cache |
| **SFX** | Freesound API | Automated sound effects |
| **Upload** | YouTube Data API v3 (OAuth2) | Scheduled uploads |
| **Analytics** | YouTube Data API v3 | Retention & insights |
| **Trending** | YouTube API · Google Trends | Real-time trend detection |
| **Research** | Perplexity API / Ollama | Accurate context before scripting |
| **Storage** | Google Drive API v3 | Upload queue |
| **Notifications** | Telegram Bot API | Real-time alerts |
| **Scheduler** | APScheduler | Campaign automation |

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

Copy `.env.example` to `.env` and fill in your API keys:

```bash
cp .env.example .env
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

# 🔑 Interactive API Key Setup Wizard (no manual .env editing)
python main.py --setup

# Run a specific channel only
python main.py --channel ch_id_horror

# Preview campaign schedule without rendering
python main.py --preview

# Full pipeline test without uploading to GDrive
python main.py --dry-run

# Skip QC Vision for faster iteration
python main.py --skip-qc

# Refresh per-channel retention analytics
python main.py --analytics

# Generate script only (no render)
python main.py --script-only

# Review hook quality without full render
python main.py --review-hook

# Specify profile: shorts or long_form
python main.py --profile long_form

# Enable debug logging
python main.py --debug
```

---

## 🔑 API Key Reference

### 🔴 Required — Pipeline won't work without these

| Key | Provider | Purpose | Get It Here |
|---|---|---|---|
| `YOUTUBE_API_KEY` | YouTube Data API v3 | Trending detection & topic discovery | [Google Cloud Console](https://console.cloud.google.com/apis/credentials) |
| `PEXELS_API_KEY` | Pexels | B-roll footage & images | [pexels.com/api](https://www.pexels.com/api/) |
| `GOOGLE_CLIENT_ID` | Google OAuth2 | YouTube upload + Google Drive | [Google Cloud Console](https://console.cloud.google.com/apis/credentials) |
| `GOOGLE_CLIENT_SECRET` | Google OAuth2 | Pair for GOOGLE_CLIENT_ID | [Google Cloud Console](https://console.cloud.google.com/apis/credentials) |
| `GOOGLE_DRIVE_FOLDER_ID` | Google Drive | Upload queue folder | Right-click folder → Share → Copy link |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API | Real-time notifications | [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID` | Telegram | Notification target | [@userinfobot](https://t.me/userinfobot) |

### 🟠 AI Provider — At least 1 required, recommend all for fallback

| Key | Provider | Purpose | Get It Here |
|---|---|---|---|
| `GEMINI_API_KEY` | Google Gemini | **Recommended** — script generation & QC Vision | [aistudio.google.com/apikey](https://aistudio.google.com/apikey) |
| `PROVIDER_A_API_KEY` | NVIDIA NIM / Mistral Large 3 | Primary script generation | [integrate.api.nvidia.com](https://integrate.api.nvidia.com) |
| `PROVIDER_B_API_KEY` | DeepSeek V4 Pro | Scoring and evaluation | [platform.deepseek.com](https://platform.deepseek.com) |
| `GROQ_API_KEY` | Groq | Fast fallback LLM | [console.groq.com/keys](https://console.groq.com/keys) |

> ⚠️ Without an AI Provider, script generation won't run. Fill in at least 1.

### 🟡 Optional — Tool still works, specific features gracefully skip

| Key | Provider | Purpose | Impact if Missing |
|---|---|---|---|
| `PIXABAY_API_KEY` | Pixabay | Footage fallback | No fallback footage |
| `PERPLEXITY_API_KEY` | Perplexity | Web research — accurate context | Uses local Ollama instead |
| `ANTHROPIC_API_KEY` | Anthropic | Last-resort AI fallback | Only 3-tier fallback chain |
| `NVIDIA_API_KEY` | NVIDIA NIM | Vision QC alternative (free) | QC via Gemini only |
| `ASSEMBLYAI_API_KEY` | AssemblyAI | Podcast transcription | No transcript feature |
| `ELEVENLABS_API_KEY` | ElevenLabs | Premium TTS voices | Fallback: Edge TTS (free) |
| `FREESOUND_API_KEY` | Freesound | Niche-based auto SFX | No sound effects |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare | Google Trends scraping | Limited trending |
| `CLOUDFLARE_API_TOKEN` | Cloudflare | Pair for ACCOUNT_ID | — |
| `COVERR_API_KEY` | Coverr | Free stock video | Alternative footage source |
| `COVERR_APP_ID` | Coverr | Pair for COVERR_API_KEY | — |

> 💡 **Pro tip:** Run `python main.py --setup` for an interactive setup wizard — no manual `.env` editing needed!

## ⚡ Quick Start (Minimal Mode)

Only need **Ollama + FFmpeg** — no API keys required:

```bash
# 1. Install Ollama and pull a model
ollama pull llama3.3:latest

# 2. Install FFmpeg
# Ubuntu: sudo apt install ffmpeg
# macOS: brew install ffmpeg
# Windows: https://ffmpeg.org/download.html

# 3. Run the API key setup wizard (or skip — Ollama-only mode)
python main.py --setup

# 4. Run with Ollama only (skip footage, upload, analytics)
python main.py --channel ch_id_horror --skip-qc
```

---

<div align="center">

---

## 📜 License

**MIT License** — free to use, modify, and distribute for any purpose, including commercial.

See [LICENSE](LICENSE) for full text.

---

*Built for creators who refuse to be limited by time.*

```
[ 🤖 AI-POWERED ]  [ 🎬 CINEMATIC ]  [ 📡 VIRAL-READY ]  [ 🔄 24/7 AUTO ]
```

</div>
