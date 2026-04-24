"""
trending_engine.py - Discover trending topics dari multiple sources.
  1. fetch_trending_niche() — YouTube Data API v3 (Shorts viral 24 jam)
  2. Google Trends via Cloudflare Browser Rendering
  3. YouTube Search Suggestions
  4. Ollama AI Brainstorming
Hasil disimpan di data/{channel_id}/topics/trending_cache.json
"""

import glob
import os
import json
import time
import re
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus
from engine.utils import get_logger, require_env, save_json, get_ollama_model

logger = get_logger("trending_engine")

# YouTube Data API
YT_API_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YT_VIRAL_MAX_RESULTS = 5

# Cache kadaluarsa setelah berapa jam
CACHE_TTL_HOURS = 6

# Cloudflare endpoint
CF_CRAWL_URL = "https://api.cloudflare.com/client/v4/accounts/{account_id}/browser-rendering/crawl"

# Ollama config (mengambil dari env yang sama dengan script_engine)
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

# ─── Keyword mapping per niche & language ─────────────────────────────────────
NICHE_KEYWORDS = {
    "horror_facts": {
        "id": ["fakta horor", "misteri tersembunyi", "kejadian aneh", "tempat angker", "ritual gelap"],
        "en": ["horror facts", "dark mystery", "haunted places", "creepy true stories", "scary facts"],
    },
    "psychology": {
        "id": ["psikologi", "trik pikiran", "manipulasi", "perilaku manusia", "fakta otak"],
        "en": ["psychology facts", "mind tricks", "human behavior", "dark psychology", "brain facts"],
    },
}

# YouTube search suggestion endpoint
YT_SUGGEST_URL = "https://suggestqueries.google.com/complete/search"


def _format_as_topic(yt_title: str, niche: str, language: str) -> str | None:
    """
    Konversi judul video YouTube viral menjadi format topik narasi yang clean.
    - Hapus hashtag, emoji berlebihan, dan kapitalisasi ALL CAPS.
    - Pertahankan makna inti.
    - Return None jika judul terlalu pendek / tidak relevan.
    """
    # Hapus hashtag
    title = re.sub(r"#\S+", "", yt_title).strip()
    # Hapus karakter berulang berlebihan
    title = re.sub(r"[!?]{3,}", "!", title)
    # Normalkan spasi
    title = re.sub(r"\s+", " ", title).strip()
    # Judul terlalu pendek → skip
    if len(title) < 12:
        return None
    # Konversi ALL CAPS ke Title Case agar tidak terlalu frontal
    if title == title.upper() and len(title) > 20:
        title = title.title()
    return title


def _load_yt_api_key() -> str | None:
    """
    Baca YouTube API key dari:
    1. Env var YOUTUBE_API_KEY
    2. config/secrets/*.json (field 'api_key' atau 'youtube_api_key')
    """
    key = os.environ.get("YOUTUBE_API_KEY", "").strip()
    if key:
        return key

    patterns = [
        "config/secrets/*.json",
        "config/*.json",
        "secrets/*.json",
    ]
    for pattern in patterns:
        for path in glob.glob(pattern):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for field in ("api_key", "youtube_api_key", "YOUTUBE_API_KEY"):
                    val = data.get(field, "").strip()
                    if val:
                        logger.info(f"[trending] YouTube API key dari {path}")
                        return val
            except Exception:
                continue
    return None


def fetch_trending_niche(niche: str, language: str) -> list[str]:
    """
    Query YouTube Data API v3 untuk 5 Shorts paling viral dalam niche
    horror_facts ATAU psychology dalam 24 jam terakhir (global).
    Return: list judul video sebagai pola inspirasi topik.
    """
    api_key = _load_yt_api_key()
    if not api_key:
        logger.warning("[trending] Tidak ada YouTube API key → skip fetch_trending_niche")
        return []

    # Niche hanya horror_facts atau psychology
    if niche == "horror_facts":
        queries = (
            ["horror facts shorts", "dark facts shorts", "scary facts shorts"]
            if language == "en"
            else ["fakta horor shorts", "fakta gelap shorts", "misteri tersembunyi shorts"]
        )
    elif niche == "psychology":
        queries = (
            ["dark psychology shorts", "psychology facts shorts", "mind tricks shorts"]
            if language == "en"
            else ["psikologi gelap shorts", "fakta psikologi shorts", "trik pikiran shorts"]
        )
    else:
        logger.warning(f"[trending] Niche '{niche}' tidak dikenal → skip")
        return []

    # 24 jam lalu dalam format RFC 3339
    published_after = (
        datetime.now(timezone.utc) - timedelta(hours=24)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    hl = "id" if language == "id" else "en"

    titles: list[str] = []
    seen: set[str] = set()

    for query in queries:
        if len(titles) >= YT_VIRAL_MAX_RESULTS:
            break
        params = {
            "key":            api_key,
            "part":           "snippet",
            "q":              query,
            "type":           "video",
            "videoDuration":  "short",           # Shorts (<= 60 detik)
            "order":          "viewCount",       # Paling banyak ditonton
            "publishedAfter": published_after,   # 24 jam terakhir
            "maxResults":     YT_VIRAL_MAX_RESULTS,
            "hl":             hl,
            "relevanceLanguage": hl,
        }
        try:
            resp = requests.get(YT_API_SEARCH_URL, params=params, timeout=15)
            resp.raise_for_status()
            items = resp.json().get("items", [])
            for item in items:
                title = item.get("snippet", {}).get("title", "").strip()
                if title and title.lower() not in seen:
                    seen.add(title.lower())
                    formatted = _format_as_topic(title, niche, language)
                    if formatted:
                        titles.append(formatted)
                        logger.info(f"[trending] YT viral: {formatted}")
            time.sleep(0.2)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                logger.warning("[trending] YouTube API quota habis / key tidak valid")
                return []
            logger.warning(f"[trending] YouTube API error untuk '{query}': {e}")
        except Exception as e:
            logger.warning(f"[trending] YouTube fetch error: {e}")

    logger.info(f"[trending] fetch_trending_niche → {len(titles)} judul viral")
    return titles[:YT_VIRAL_MAX_RESULTS]


def get_trending_topics(niche: str, language: str, channel_id: str = None, limit: int = 10) -> list[str]:
    """
    Ambil daftar topik trending.
    Prioritas: Cache -> Cloudflare -> YT Suggest -> Ollama (AI Brainstorm).
    """
    cache_id = channel_id or f"{niche}_{language}"
    cache_path = _cache_path(cache_id)

    # 1. Cek cache
    cached = _load_cache(cache_path)
    if cached:
        logger.info(f"[trending] Cache ditemukan ({cache_id}) -> {len(cached)} topik")
        return cached[:limit]

    topics = []

    # 2. YouTube Data API — Viral Shorts 24 jam (prioritas tertinggi)
    try:
        yt_viral = fetch_trending_niche(niche, language)
        if yt_viral:
            topics.extend(yt_viral)
            logger.info(f"[trending] YouTube viral Shorts -> {len(yt_viral)} topik")
    except Exception as e:
        logger.warning(f"[trending] YouTube viral fetch gagal: {e}")

    # 3. Cloudflare Browser Rendering
    try:
        cf_topics = _crawl_google_trends(niche, language)
        if cf_topics:
            existing = set(t.lower() for t in topics)
            new = [t for t in cf_topics if t.lower() not in existing]
            topics.extend(new)
            logger.info(f"[trending] Cloudflare berhasil -> {len(new)} topik baru")
    except Exception as e:
        logger.warning(f"[trending] Cloudflare gagal: {e}")

    # 4. YouTube Suggestions (Fallback)
    try:
        yt_topics = _youtube_suggestions(niche, language)
        if yt_topics:
            existing = set(t.lower() for t in topics)
            new = [t for t in yt_topics if t.lower() not in existing]
            topics.extend(new)
            logger.info(f"[trending] YouTube suggestions -> {len(new)} topik baru")
    except Exception as e:
        logger.warning(f"[trending] YouTube suggestions gagal: {e}")

    # 4. Ollama AI Brainstorming (Sebagai pelengkap atau jika semua sumber gagal)
    if len(topics) < limit:
        try:
            logger.info(f"[trending] Mencoba brainstorming via Ollama ({get_ollama_model()})...")
            ai_topics = _generate_via_ollama(niche, language, existing_topics=topics)
            if ai_topics:
                topics.extend(ai_topics)
                logger.info(f"[trending] Ollama berhasil -> {len(ai_topics)} topik tambahan")
        except Exception as e:
            logger.warning(f"[trending] Ollama gagal: {e}")

    if not topics:
        logger.warning(f"[trending] Semua sumber gagal, mengembalikan list kosong")
        return []

    # Simpan ke cache
    _save_cache(cache_path, topics)
    logger.info(f"[trending] Total {len(topics)} topik disimpan ke cache")
    return topics[:limit]


def _generate_via_ollama(niche: str, language: str, existing_topics: list) -> list[str]:
    """
    Gunakan AI lokal untuk memikirkan topik yang berpotensi viral berdasarkan niche.
    """
    niche_desc = "horror mysteries and dark facts" if niche == "horror_facts" else "psychology and human behavior"
    lang_name = "Indonesian" if language == "id" else "English"
    
    prompt = (
        f"You are a viral content strategist for YouTube. "
        f"Give me 10 fresh, highly engaging, and viral video topic ideas for the niche: {niche_desc}. "
        f"The topics must be in {lang_name}. "
        f"Format the output as a simple list, one topic per line. No numbers, no extra text. "
        f"Focus on topics that create curiosity or fear. Each topic must contain a 'curiosity gap' or a 'shock factor. Use power words like: 'Secret', 'Forbidden', 'Never told', 'Warning'."
    )

    if existing_topics:
        prompt += f"\nAvoid these topics: {', '.join(existing_topics[:10])}"

    payload = {
        "model": get_ollama_model(),
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 1.0, "top_p": 0.98, "top_k": 80}
    }

    try:
        resp = requests.post(f"{OLLAMA_BASE_URL}/api/generate", json=payload, timeout=60)
        resp.raise_for_status()
        raw_response = resp.json().get("response", "").strip()
        
        # Bersihkan baris dan ambil yang valid
        lines = [line.strip() for line in raw_response.split('\n') if len(line.strip()) > 10]
        
        valid_topics = []
        for line in lines:
            # Bersihkan angka di awal jika ada (misal "1. Topic")
            clean_line = re.sub(r'^\d+[\.\)\-\s]+', '', line).strip()
            formatted = _format_as_topic(clean_line, niche, language)
            if formatted:
                valid_topics.append(formatted)
                
        return valid_topics
    except Exception as e:
        logger.error(f"[trending] Ollama API Error: {e}")
        return []


def _crawl_google_trends(niche: str, language: str) -> list[str]:
    account_id = require_env("CLOUDFLARE_ACCOUNT_ID").strip()
    api_token  = require_env("CLOUDFLARE_API_TOKEN").strip()

    keywords = NICHE_KEYWORDS.get(niche, {}).get(language, [])
    if not keywords:
        return []

    query = keywords[0]
    geo   = "ID" if language == "id" else "US"
    target_url = f"https://trends.google.com/trends/explore?q={requests.utils.quote(query)}&geo={geo}"

    payload = {"url": target_url}

    cf_url = CF_CRAWL_URL.format(account_id=account_id)
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(cf_url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        topics = []
        if data.get("success") and data.get("result"):
            result_data = data["result"]
            content = str(result_data)

            # Ekstraksi teks yang terlihat seperti judul/search term (minimal 2 kata)
            raw_list = re.findall(r'["\']([^"\']{10,80})["\']', content)
            
            for text in raw_list:
                formatted = _format_as_topic(text, niche, language)
                if formatted:
                    topics.append(formatted)

        return list(dict.fromkeys(topics))[:15]
    except Exception as e:
        logger.error(f"[trending] Cloudflare Crawl Error: {e}")
        return []


def _format_as_topic(raw_text: str, niche: str, language: str) -> str:
    """
    Membersihkan dan memformat teks mentah menjadi judul topik video.
    Menggunakan filter ketat untuk membuang token/ID sampah internet dan redundansi.
    """
    text = raw_text.strip()
    
    # 1. Harus punya spasi (Frasa asli pasti lebih dari 1 kata)
    if " " not in text:
        return ""
    
    # 2. Batas panjang karakter (Minimal 2 kata bermakna)
    if len(text) < 10 or len(text) > 80: 
        return ""
    
    # 3. Filter Token/ID/Aset Produksi (Noise)
    # Filter ID/Token acak
    if re.search(r'[0-9]{3,}', text) and not re.search(r'[aeiou]{2,}', text.lower()):
        return "" 
    
    # Blacklist kata kunci sistem & Noise Produksi
    blacklisted = [
        "trending", "explore", "interest", "related", "queries", 
        "google", "terms", "search", "topic", "feedback", "policy",
        "cookie", "privacy", "help", "about", "sign in", "user",
        "hindi", "telugu", "tamil", "urdu", "malayalam", "bengali",
        "token", "session", "cache", "auth",
        # Noise Produksi (Sangat penting dibuang)
        "music", "background", "no copyright", "ncs", "audionautix", 
        "soundridemusic", "intro", "outro", "gameplay", "bedrock mining",
        "download", "free", "mp3", "video", "shorts", "tiktok"
    ]
    
    low_text = text.lower()
    if any(b in low_text for b in blacklisted): 
        return ""

    # 4. Penanganan Redundansi Awalan (Fix: "Dark facts about horror facts")
    # Jika niche horror_facts
    if niche == "horror_facts":
        prefix = "Fakta gelap tentang" if language == "id" else "Dark facts about"
        # Jika teks sudah mengandung "horror" atau "fakta" atau "dark mystery", jangan dipaksa pakai prefix panjang
        identity_keywords = ["horror", "fakta", "facts", "dark", "mystery", "misteri"]
        
        if any(ik in low_text for ik in identity_keywords):
            # Cukup bersihkan dan jadikan Proper Case jika belum
            return text[0].upper() + text[1:]
        
        if not low_text.startswith(prefix.lower()):
            return f"{prefix} {text}"
            
    # Jika niche psychology
    elif niche == "psychology":
        prefix = "Psikologi di balik" if language == "id" else "Psychology behind"
        identity_keywords = ["psychology", "psikologi", "mind", "otak", "perilaku"]
        
        if any(ik in low_text for ik in identity_keywords):
            return text[0].upper() + text[1:]
            
        if not low_text.startswith(prefix.lower()):
            return f"{prefix} {text}"

    return text


def _youtube_suggestions(niche: str, language: str) -> list[str]:
    keywords = NICHE_KEYWORDS.get(niche, {}).get(language, [])
    all_suggestions = []

    for kw in keywords[:2]:
        params = {
            "client": "youtube",
            "ds": "yt",
            "q": kw,
            "hl": "id" if language == "id" else "en",
        }
        try:
            resp = requests.get(YT_SUGGEST_URL, params=params, timeout=10)
            matches = re.findall(r'["\']([^"\']{8,60})["\']', resp.text)
            for m in matches:
                if m.lower() == kw.lower(): continue
                formatted = _format_as_topic(m, niche, language)
                if formatted:
                    all_suggestions.append(formatted)
            time.sleep(0.3)
        except:
            continue

    return list(dict.fromkeys(all_suggestions))


def _cache_path(cid: str) -> str:
    path = f"data/{cid}/topics/trending_cache.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def _load_cache(path: str) -> list | None:
    if not os.path.exists(path): return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        saved_at = datetime.fromisoformat(data.get("saved_at", "2000-01-01"))
        if datetime.now() - saved_at > timedelta(hours=CACHE_TTL_HOURS):
            return None
        return data.get("topics", [])
    except:
        return None


def _save_cache(path: str, topics: list):
    save_json({
        "saved_at": datetime.now().isoformat(),
        "topics": topics
    }, path)