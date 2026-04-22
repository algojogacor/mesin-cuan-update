"""
research_engine.py - Web research context untuk memperkaya script AI.

Strategi:
  1. Perplexity API (primary) — sonar model, real-time web search
  2. Ollama tool call web_search (fallback) — jika model support tool calling
  3. Graceful skip — jika keduanya tidak tersedia, return kosong TANPA error

Output diinjeksikan ke system_prompt script_engine sebagai addon konteks.
"""

from __future__ import annotations

import os
import json
import requests

from engine.utils import get_logger

logger = get_logger("research_engine")

PERPLEXITY_API_BASE = "https://api.perplexity.ai"
PERPLEXITY_MODEL = os.environ.get("PERPLEXITY_MODEL", "perplexity/sonar")

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "deepseek-v3.1:671b-cloud")

RESEARCH_TIMEOUT = 30  # detik — cepat, jangan block pipeline terlalu lama
MAX_CONTEXT_CHARS = 1800  # Batasi panjang context agar tidak membanjiri prompt


def research_topic(topic: str, channel: dict | None = None) -> dict:
    """
    Riset konteks untuk topik video.

    Args:
        topic: Topik yang akan dibuatkan video
        channel: Dict channel (untuk context bahasa/niche)

    Return:
        {
            "context": str,   ← teks konteks untuk inject ke prompt
            "sources": list,  ← daftar URL sumber (jika ada)
            "used": bool,     ← True jika riset berhasil
            "provider": str,  ← "perplexity" | "ollama" | "none"
        }
    """
    language = (channel or {}).get("language", "id")
    niche = (channel or {}).get("niche", "horror_facts")

    query = _build_search_query(topic, language, niche)

    # ── 1. Coba Perplexity (Primary) ──────────────────────────────────────────
    result = _try_perplexity(query, language)
    if result["used"]:
        logger.info(f"[research] ✅ Perplexity berhasil untuk: {topic[:50]}")
        return result

    # ── 2. Coba Ollama Native Web Search API (Secondary) ──────────────────────
    result = _try_ollama_native_search(query, language)
    if result["used"]:
        logger.info(f"[research] ✅ Ollama Native Search berhasil untuk: {topic[:50]}")
        return result

    # ── 3. Coba Ollama web_search tool (DDGS - Fallback) ─────────────────────
    result = _try_ollama_web_search(query, language)
    if result["used"]:
        logger.info(f"[research] ✅ Ollama DDGS Search berhasil untuk: {topic[:50]}")
        return result

    # ── 3. Graceful skip ──────────────────────────────────────────────────────
    logger.info(f"[research] ℹ️  Tidak ada web research tersedia, skip (pipeline tetap jalan)")
    return {"context": "", "sources": [], "used": False, "provider": "none"}


def build_research_addon(research_result: dict, language: str = "id") -> str:
    """
    Konversi hasil riset menjadi addon teks untuk system_prompt.
    Return string kosong jika riset tidak berhasil.
    """
    if not research_result.get("used") or not research_result.get("context"):
        return ""

    context = research_result["context"][:MAX_CONTEXT_CHARS]
    provider = research_result.get("provider", "web")

    if language == "id":
        header = f"\n[Konteks Riset dari {provider.title()}]\n"
        footer = "\nGunakan informasi ini sebagai bahan, bukan sebagai narasi verbatim. Tetap prioritaskan angle horor dan gaya Shorts.\n"
    else:
        header = f"\n[Research Context from {provider.title()}]\n"
        footer = "\nUse this as background material only, not verbatim narration. Keep the horror angle and Shorts style.\n"

    return header + context + footer


# ─── Internal: Perplexity ────────────────────────────────────────────────────

def _try_perplexity(query: str, language: str) -> dict:
    api_key = os.environ.get("PERPLEXITY_API_KEY", "")
    if not api_key:
        return {"context": "", "sources": [], "used": False, "provider": "none"}

    try:
        session = requests.Session()
        session.trust_env = False
        resp = session.post(
            f"{PERPLEXITY_API_BASE}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": PERPLEXITY_MODEL,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Kamu adalah peneliti konten horor Indonesia. "
                            "Berikan ringkasan faktual singkat tentang topik yang diminta. "
                            "Fokus pada fakta, lokasi, sejarah, atau legenda yang dapat digunakan "
                            "sebagai bahan konten video horor. Maksimal 300 kata."
                            if language == "id"
                            else
                            "You are a horror content researcher. "
                            "Provide a concise factual summary about the requested topic. "
                            "Focus on facts, locations, history, or legends usable as horror video content. "
                            "Maximum 300 words."
                        ),
                    },
                    {"role": "user", "content": query},
                ],
                "max_tokens": 8192,
                "temperature": 0.2,
            },
            timeout=RESEARCH_TIMEOUT,
        )
        session.close()
        resp.raise_for_status()

        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        citations = data.get("citations", [])

        if not content:
            return {"context": "", "sources": [], "used": False, "provider": "none"}

        return {
            "context": content,
            "sources": citations[:5],
            "used": True,
            "provider": "perplexity",
        }

    except Exception as exc:
        logger.debug(f"[research] Perplexity gagal: {exc}")
        return {"context": "", "sources": [], "used": False, "provider": "none"}

# ─── Internal: Ollama Native Web Search API ──────────────────────────────────
def _try_ollama_native_search(query: str, language: str) -> dict:
    api_key = os.environ.get("OLLAMA_API_KEY", "")
    if not api_key:
        return {"context": "", "sources": [], "used": False, "provider": "none"}
    
    try:
        # Panggil Ollama Web Search API
        resp = requests.post(
            "https://ollama.com/api/web_search",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"query": query, "max_results": 5},
            timeout=RESEARCH_TIMEOUT
        )
        resp.raise_for_status()
        
        results = resp.json().get("results", [])
        if not results:
            return {"context": "", "sources": [], "used": False, "provider": "none"}
        
        sources = [r.get("url") for r in results if r.get("url")]
        search_context = "\n".join([f"[{r.get('title')}] {r.get('content')}" for r in results])
        
        # Sekarang suruh Ollama lokal meringkasnya
        summary_prompt = (
            f"Ringkas informasi faktual berikut ini tentang: {query}.\n\n{search_context}\n\n"
            "Fokus pada fakta sejarah, lokasi, atau legenda untuk bahan konten video horor. Jangan membuat intro/outro, hanya ringkasan."
            if language == "id" else
            f"Summarize this factual information about: {query}.\n\n{search_context}\n\n"
            "Focus on history, locations, or legends for a horror video. No intro/outro, just the summary."
        )
        
        payload = {
            "model": OLLAMA_MODEL,
            "messages": [{"role": "user", "content": summary_prompt}],
            "stream": False,
            "options": {"temperature": 0.2}
            # num_predict dihapus agar max tokens limit tidak dibatasi
        }
        resp2 = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=RESEARCH_TIMEOUT)
        resp2.raise_for_status()
        
        content = resp2.json().get("message", {}).get("content", "").strip()
        if content and len(content) > 50:
            return {
                "context": content,
                "sources": sources,
                "used": True,
                "provider": "ollama_native_search",
            }
    except Exception as exc:
        logger.warning(f"[research] Ollama Native Web Search gagal: {exc}")
    
    return {"context": "", "sources": [], "used": False, "provider": "none"}


# ─── Internal: Ollama web_search tool ────────────────────────────────────────

def _try_ollama_web_search(query: str, language: str) -> dict:
    """
    Coba panggil Ollama dengan tool web_search via DuckDuckGo.
    Jika model tidak support tools -> gagal graceful.
    """
    try:
        messages = [
            {
                "role": "user",
                "content": (
                    f"Cari informasi faktual tentang: {query}. "
                    "Ringkas dalam 200-250 kata, fokus pada aspek yang bisa digunakan untuk konten horor."
                    if language == "id"
                    else
                    f"Search for factual information about: {query}. "
                    "Summarize in 200-250 words, focus on aspects usable for horror content."
                ),
            }
        ]
        
        payload = {
            "model": OLLAMA_MODEL,
            "messages": messages,
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "web_search",
                        "description": "Search the web for current information",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The search query",
                                }
                            },
                            "required": ["query"],
                        },
                    },
                }
            ],
            "stream": False,
            "options": {"temperature": 0.2}, # Hapus batas num_predict
        }

        resp = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=RESEARCH_TIMEOUT)
        resp.raise_for_status()

        data = resp.json()
        message = data.get("message", {})
        tool_calls = message.get("tool_calls", [])

        # 1. Jika model memilih menggunakan tool web_search
        if tool_calls:
            search_query = tool_calls[0].get("function", {}).get("arguments", {}).get("query", query)
            logger.info(f"[research] 🦆 Ollama mengeksekusi DDGS web_search: '{search_query}'")
            
            try:
                from ddgs import DDGS
                with DDGS() as ddgs:
                    raw_results = list(ddgs.text(search_query, max_results=3))
                    
                sources = [r.get("href") for r in raw_results if r.get("href")]
                search_context = json.dumps(raw_results, ensure_ascii=False)
                
                # Append riwayat chat
                messages.append(message)
                messages.append({
                    "role": "tool",
                    "content": search_context
                })
                
                # Panggil lagi Ollama untuk meramu jawaban dari hasil search
                payload["messages"] = messages
                payload.pop("tools", None)  # Hapus tools argumen krn kita mau final text
                
                resp2 = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=RESEARCH_TIMEOUT)
                resp2.raise_for_status()
                
                content = resp2.json().get("message", {}).get("content", "").strip()
                if content and len(content) > 50:
                    return {
                        "context": content,
                        "sources": sources,
                        "used": True,
                        "provider": "ollama_ddg",
                    }
            except Exception as tool_exc:
                logger.warning(f"[research] DDGS Search tool gagal dieksekusi: {tool_exc}")

        # 2. Jika model menolak tool / gagal / langsung jawab
        content = message.get("content", "").strip()
        if content and len(content) > 50:
            return {
                "context": content,
                "sources": [],
                "used": True,
                "provider": "ollama",
            }

        return {"context": "", "sources": [], "used": False, "provider": "none"}

    except Exception as exc:
        logger.debug(f"[research] Ollama web_search pipeline gagal: {exc}")
        return {"context": "", "sources": [], "used": False, "provider": "none"}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _build_search_query(topic: str, language: str, niche: str) -> str:
    """Buat query pencarian yang relevan untuk niche."""
    if language == "id":
        if niche == "horror_facts":
            return f"{topic} sejarah misteri fakta horor Indonesia"
        return f"{topic} fakta psikologi penjelasan ilmiah"
    else:
        if niche == "horror_facts":
            return f"{topic} history mystery horror facts"
        return f"{topic} psychology facts scientific explanation"
