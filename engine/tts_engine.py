"""
tts_engine.py — Mesin Cuan Viral Architect
Pipeline Text-to-Speech multi-engine dengan preprocessing ekspresif.

Engine priority:
  Primary  : Edge TTS   — Microsoft Cloud, gratis, 100+ suara
  Secondary: Gemini TTS — Google Cloud, ultra cepat, butuh API key
  Fallback : F5-TTS v1  — Lokal, voice cloning, butuh RTX

Hardware Target: RTX 3050 4GB | i7-13H | RAM 16GB
Strategi:
  - Edge TTS  : berjalan di cloud → tidak pakai RAM/GPU lokal
  - Gemini TTS: berjalan di cloud → sama
  - F5-TTS    : berjalan lokal di CPU (bukan GPU) — VRAM 4GB
                disimpan untuk NVENC video_engine.py
  - FFmpeg audio processing: 8 thread (sama dengan video_engine)
  - Loudnorm hanya di tahap AKHIR (bukan per-segmen) → 1 pass, hemat waktu

CHANGELOG v6 (Clean Code + Hardware-Tuned):
  - Semua hardcoded value → CONFIG BLOCK terpusat dengan komentar
  - Hardware config disesuaikan RTX 3050 + i7-13H + 16GB RAM
  - KAPITAL kata kunci horror/psycho otomatis sebelum TTS
  - Em-dash sebelum konjungsi dramatis (horror/drama)
  - Rate horror long_form = -5% (lebih lambat, lebih mencekam)
  - Dynamic tagging [LOUD][QUIET][SLOW] per segmen
  - Dead code dihapus, async pipeline dioptimasi

CHANGELOG v6.1 (Voice Fallback):
  - NICHE_VOICE_MAP diubah dari dict of str → dict of list (ordered fallback)
  - _get_voice() ditambah parameter fallback_index
  - _run_edge_tts_async() otomatis retry dengan voice berikutnya jika gagal
  - Hapus en-US-DavisNeural & en-US-AndrewNeural (tidak ada di daftar resmi edge-tts)
"""

import os
import re
import asyncio
import tempfile
import subprocess
import requests
import base64
import wave
import shutil
from typing import List, Tuple
from engine.utils import get_logger, timestamp, channel_data_path, require_env

logger = get_logger("tts_engine")


# ══════════════════════════════════════════════════════════════════════════════
#  CENTRALIZED CONFIGURATION
#  Edit di sini — perubahan berlaku ke seluruh pipeline TTS
# ══════════════════════════════════════════════════════════════════════════════

# ── Engine Selector ───────────────────────────────────────────────────────────
# Ganti via env var TTS_MODE atau langsung di sini
# "edge"   → Microsoft Edge TTS (gratis, cloud)
# "gemini" → Google Gemini TTS (butuh GEMINI_API_KEY)
# "f5"     → F5-TTS lokal (butuh voice sample + GPU/CPU lokal)
TTS_MODE = os.environ.get("TTS_MODE", "edge")

# ── Hardware: FFmpeg Audio Processing ─────────────────────────────────────────
# Sama dengan video_engine — sisakan 4 thread untuk OS + Edge TTS async
AUDIO_FFMPEG_THREADS    = 8         # i7-13H punya 12 thread, pakai 8
AUDIO_OUTPUT_SAMPLERATE = 48000     # Hz — standar YouTube/TikTok
AUDIO_OUTPUT_BITRATE    = "192k"    # MP3 bitrate — kualitas bagus, file kecil
AUDIO_LOUDNORM_TARGET   = -14       # LUFS — standar YouTube
AUDIO_LOUDNORM_TP       = -1.5      # True Peak max (dBTP)
AUDIO_LOUDNORM_LRA      = 11        # Loudness Range target

# ── Hardware: F5-TTS (Local Voice Cloning) ────────────────────────────────────
# F5-TTS berjalan di CPU agar tidak konflik dengan NVENC yang butuh VRAM
# Speed 1.2 = 20% lebih cepat dari normal, masih natural
F5_TTS_SPEED            = 1.2       # Kecepatan bicara F5-TTS
F5_TTS_SEED             = -1        # -1 = random seed (variasi suara)
F5_TTS_PAUSE_WORD       = "    "    # Spasi extra setelah titik/koma untuk jeda

# ── Gemini TTS ────────────────────────────────────────────────────────────────
GEMINI_TTS_MODEL        = "gemini-2.5-flash-preview-tts"
GEMINI_TTS_VOICE_EN     = "Puck"    # Suara English Gemini
GEMINI_TTS_VOICE_ID     = "Kore"    # Suara Indonesia Gemini
GEMINI_TTS_SAMPLERATE   = 24000     # Hz output Gemini TTS
GEMINI_TTS_CHANNELS     = 1         # Mono
GEMINI_TTS_SAMPWIDTH    = 2         # 16-bit
GEMINI_TTS_TIMEOUT_SEC  = 120       # Timeout request Gemini

# ── Dynamic Tag Filters ────────────────────────────────────────────────────────
# Tag yang bisa disisipkan langsung di naskah: [LOUD], [QUIET], [SLOW]
# Pipeline: parse tag → render segmen terpisah → apply FFmpeg filter → concat
TAG_AUDIO_FILTERS: dict = {
    "LOUD":   "volume=1.5",                 # Naikan volume 50%
    "QUIET":  "volume=0.6,lowpass=f=3000",  # Turunkan + bisikan effect
    "SLOW":   "atempo=0.8",                 # Perlambat 20%
    "NORMAL": None,                         # Tidak ada filter
}

# ── Smart Pause Markers ────────────────────────────────────────────────────────
# Pattern regex → string jeda yang disisipkan setelah match
# Edge TTS membaca titik tunggal sebagai jeda pendek (~0.3 detik)
PAUSE_TRIGGER_PATTERNS: dict = {
    r"\b(tiba-tiba|suddenly|unexpectedly)\b":              " . . ",
    r"\b(namun|however|tetapi|but)\b":                     " . . ",
    r"\b(kenyataannya|in fact|ternyata|it turns out)\b":   " . ",
    r"\b(yang paling mengejutkan|most shocking|shocking)\b":" . . ",
    r"\b(ingat|remember|bayangkan|imagine)\b":             " . ",
    r"\b(tragisnya|tragically|ironisnya|ironically)\b":    " . ",
    r":\s":                                                 ": . ",
    r"\?\s":                                                "? . ",
}

# ── Kata Kunci yang Di-KAPITALKAN Per Niche ────────────────────────────────────
# Kata KAPITAL diucapkan Edge TTS dengan penekanan lebih kuat
# Maks 2x per kata agar tidak berlebihan
CAPS_KEYWORDS_HORROR_ID: list = [
    "hilang", "mati", "gelap", "hantu", "kutukan", "misteri",
    "mengerikan", "seram", "bunuh", "darah", "iblis", "terkutuk",
]
CAPS_KEYWORDS_HORROR_EN: list = [
    "disappeared", "dead", "dark", "ghost", "curse", "mystery",
    "terrifying", "haunted", "killed", "blood", "demon", "cursed",
]
CAPS_KEYWORDS_PSYCHO_ID: list = [
    "penting", "kunci", "rahasia", "fakta", "terbukti",
    "selalu", "tidak pernah", "sebenarnya", "tersembunyi",
]
CAPS_KEYWORDS_PSYCHO_EN: list = [
    "important", "key", "secret", "fact", "proven",
    "always", "never", "actually", "hidden",
]

# ── Em-Dash Konjungsi (Horror/Drama) ─────────────────────────────────────────
# Konjungsi setelah koma diubah jadi em-dash untuk efek dramatis di TTS
EM_DASH_CONJUNCTIONS_PATTERN = (
    r"\b(dan|tapi|namun|tetapi|but|however|and|yet|padahal|bahkan)\b"
)

# ── Smart Pause Cap ────────────────────────────────────────────────────────────
# Maksimal 2 titik berurutan agar jeda tidak terlalu panjang (>0.4 detik membosankan)
PAUSE_MAX_DOTS      = 2             # Jeda maksimal ". ." (2 titik)
PAUSE_SINGLE        = " . "         # Jeda pendek — 1 titik
PAUSE_DOUBLE        = " . . "       # Jeda panjang — 2 titik

# ── Intro Hook (Long Form only) ────────────────────────────────────────────────
# Kalimat pembuka otomatis di awal Long Form sebelum naskah utama
CHANNEL_INTRO_HOOKS: dict = {
    "horror_facts": {
        "id": "Selamat datang kembali di Horror Facts ID. Tempat di mana kita menggali sisi tergelap dari dunia ini. . . ",
        "en": "Welcome back to Horror Facts EN. The place where we dig into the darkest sides of our world. . . "
    },
    "psychology": {
        "id": "Pernahkah Anda bertanya-tanya, apa yang sebenarnya terjadi di pikiran kita? Selamat datang di Psikologi ID. . . ",
        "en": "Have you ever wondered what truly goes on inside the human mind? Welcome to Psychology EN. . . "
    },
}

# ── Voice Map Per Niche ────────────────────────────────────────────────────────
# Format: list suara per bahasa — index 0 = primary, sisanya = fallback berurutan.
# Jika primary gagal, _run_edge_tts_async() otomatis coba voice berikutnya.
#
# EN voices yang VALID di edge-tts (terverifikasi dari daftar resmi):
#   en-US-GuyNeural         Male   Passion          ← terbaik untuk horror
#   en-US-ChristopherNeural Male   Reliable, Auth
#   en-US-EricNeural        Male   Rational
#   en-US-SteffanNeural     Male   Rational
#   en-US-RogerNeural       Male   Lively
#   en-GB-RyanNeural        Male   Friendly (paling natural)
#   en-GB-ThomasNeural      Male   Friendly
#   en-US-AriaNeural        Female Positive, Confident
#   en-US-JennyNeural       Female Friendly, Comfort
#   en-US-MichelleNeural    Female Friendly, Pleasant
#
# CATATAN: en-US-DavisNeural & en-US-AndrewNeural TIDAK ADA di daftar resmi
#          edge-tts — sudah dihapus dari voice map ini.
#
# ID voices yang VALID (hanya 2 resmi di edge-tts):
#   id-ID-ArdiNeural   Male   Friendly, Positive
#   id-ID-GadisNeural  Female Friendly, Positive
NICHE_VOICE_MAP: dict = {
    "horror_facts": {
        "en": [
            "en-US-GuyNeural",          # Primary: Passion — paling cocok untuk horror
            "en-GB-RyanNeural",         # Fallback 1: natural & tidak robotic
            "en-US-ChristopherNeural",  # Fallback 2: authoritative
            "en-US-EricNeural",         # Fallback 3: rational
            "en-US-SteffanNeural",      # Fallback 4: rational (last resort)
        ],
        "id": [
            "id-ID-ArdiNeural",         # Primary: satu-satunya male ID
            "id-ID-GadisNeural",        # Fallback: female ID
        ],
    },
    "drama": {
        "en": [
            "en-US-AriaNeural",         # Primary: Confident & expressive
            "en-US-JennyNeural",        # Fallback 1: Friendly, Comfort
            "en-GB-RyanNeural",         # Fallback 2
            "en-US-GuyNeural",          # Fallback 3
        ],
        "id": [
            "id-ID-GadisNeural",        # Primary: female — lebih emosional untuk drama
            "id-ID-ArdiNeural",         # Fallback
        ],
    },
    "psychology": {
        "en": [
            "en-US-ChristopherNeural",  # Primary: Reliable, Authority — cocok psych
            "en-US-EricNeural",         # Fallback 1: Rational
            "en-GB-RyanNeural",         # Fallback 2: natural
            "en-US-GuyNeural",          # Fallback 3
            "en-US-SteffanNeural",      # Fallback 4
        ],
        "id": [
            "id-ID-ArdiNeural",         # Primary
            "id-ID-GadisNeural",        # Fallback
        ],
    },
    "motivation": {
        "en": [
            "en-US-RogerNeural",        # Primary: Lively — energik untuk motivasi
            "en-US-GuyNeural",          # Fallback 1: Passion
            "en-US-ChristopherNeural",  # Fallback 2
            "en-GB-ThomasNeural",       # Fallback 3
        ],
        "id": [
            "id-ID-ArdiNeural",         # Primary
            "id-ID-GadisNeural",        # Fallback
        ],
    },
    "default": {
        "en": [
            "en-US-ChristopherNeural",  # Primary
            "en-US-GuyNeural",          # Fallback 1
            "en-GB-RyanNeural",         # Fallback 2
            "en-US-EricNeural",         # Fallback 3
            "en-US-SteffanNeural",      # Fallback 4
        ],
        "id": [
            "id-ID-ArdiNeural",         # Primary
            "id-ID-GadisNeural",        # Fallback
        ],
    },
}

# ── Rate Per Niche (Edge TTS) ─────────────────────────────────────────────────
# Long Form: lebih dramatis, horror lebih lambat
# Shorts: semua cepat untuk retensi maksimal
# Format Edge TTS: "+X%" atau "-X%"
NICHE_RATE_LONGFORM: dict = {
    "horror_facts": "-5%",   # Lambat & mencekam — efek maksimal di Long Form
    "drama":        "-3%",   # Emosional, mengalir pelan
    "psychology":   "+3%",   # Thoughtful, berwibawa tapi tidak membosankan
    "motivation":   "+8%",   # Energetik, penuh semangat
    "default":      "+5%",   # Baseline nyaman untuk konten umum
}
NICHE_RATE_SHORTS: dict = {
    "horror_facts": "+8%",   # Shorts harus cepat untuk retensi
    "drama":        "+8%",
    "psychology":   "+12%",  # Insight cepat
    "motivation":   "+15%",  # Hype maksimal
    "default":      "+10%",  # Baseline Shorts
}


# ══════════════════════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _get_voice(niche: str, language: str, fallback_index: int = 0) -> str:
    """
    Return Edge TTS voice name berdasarkan niche, bahasa, dan fallback index.

    Args:
        niche          : niche channel (horror_facts, psychology, dll)
        language       : "en" atau "id"
        fallback_index : 0 = primary, 1 = fallback pertama, dst.
                         Otomatis clamp ke voice terakhir jika index melebihi list.
    """
    for key in NICHE_VOICE_MAP:
        if key in niche.lower():
            voices = NICHE_VOICE_MAP[key].get(language, NICHE_VOICE_MAP[key]["en"])
            return voices[min(fallback_index, len(voices) - 1)]
    voices = NICHE_VOICE_MAP["default"].get(language, NICHE_VOICE_MAP["default"]["en"])
    return voices[min(fallback_index, len(voices) - 1)]


def _get_voice_count(niche: str, language: str) -> int:
    """Return jumlah voice yang tersedia untuk niche + bahasa ini."""
    for key in NICHE_VOICE_MAP:
        if key in niche.lower():
            return len(NICHE_VOICE_MAP[key].get(language, NICHE_VOICE_MAP[key]["en"]))
    return len(NICHE_VOICE_MAP["default"].get(language, NICHE_VOICE_MAP["default"]["en"]))


def _get_rate(niche: str, profile: str = "shorts") -> str:
    """Return Edge TTS rate string per niche dan profile (shorts/long_form)."""
    rate_map = NICHE_RATE_SHORTS if profile == "shorts" else NICHE_RATE_LONGFORM
    for key in rate_map:
        if key in niche.lower():
            return rate_map[key]
    return rate_map["default"]


def _has_dynamic_tags(text: str) -> bool:
    """Cek apakah naskah mengandung dynamic tag [LOUD]/[QUIET]/[SLOW]."""
    return bool(re.search(r"\[(LOUD|QUIET|SLOW)\]", text, re.IGNORECASE))


# ══════════════════════════════════════════════════════════════════════════════
#  SCRIPT PREPROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def preprocess_script(raw_text: str, niche: str, language: str,
                      profile: str = "shorts") -> str:
    """
    Transform naskah biasa → naskah TTS-optimized.
    Dipanggil sebelum teks dikirim ke engine manapun.

    Transformasi yang dilakukan:
    1. KAPITAL kata kunci per niche (penekanan TTS lebih kuat)
    2. Em-dash sebelum konjungsi dramatis (horror/drama)
    3. Sisipkan pause markers berdasarkan kata kunci emosi
    4. Smart pause: horror long_form = double pause, sisanya single
    5. Cap triple-dot ke max 2 titik (hindari jeda >0.4 detik)
    6. Normalisasi whitespace
    """
    text      = raw_text.strip()
    is_horror = any(k in niche.lower() for k in ("horror", "drama", "crime"))
    is_psycho = any(k in niche.lower() for k in ("psychology", "psycho", "motivation"))

    # ── Step 1: Kapitalisasi kata kunci per niche ─────────────────────────────
    if is_horror:
        caps_list = (CAPS_KEYWORDS_HORROR_ID if language == "id"
                     else CAPS_KEYWORDS_HORROR_EN)
        for word in caps_list:
            text = re.sub(
                rf'\b{re.escape(word)}\b',
                word.upper(), text,
                flags=re.IGNORECASE, count=2   # Maks 2x per naskah
            )

    if is_psycho:
        caps_list = (CAPS_KEYWORDS_PSYCHO_ID if language == "id"
                     else CAPS_KEYWORDS_PSYCHO_EN)
        for word in caps_list:
            text = re.sub(
                rf'\b{re.escape(word)}\b',
                word.upper(), text,
                flags=re.IGNORECASE, count=2
            )

    # ── Step 2: Em-dash sebelum konjungsi (horror/drama) ─────────────────────
    # ",  dan" → "— dan" (jeda dramatis)
    if is_horror:
        text = re.sub(
            r',\s+' + EM_DASH_CONJUNCTIONS_PATTERN,
            lambda m: f'— {m.group(1)} ',
            text,
            flags=re.IGNORECASE
        )

    # ── Step 3: Sisipkan pause markers ────────────────────────────────────────
    for pattern, pause in PAUSE_TRIGGER_PATTERNS.items():
        text = re.sub(
            pattern,
            lambda m, p=pause: m.group(0) + p,
            text,
            flags=re.IGNORECASE
        )

    # ── Step 4: Smart pause — cap per niche & profile ─────────────────────────
    if is_horror and profile == "long_form":
        # Horror long_form: pertahankan double pause (lebih mencekam)
        text = re.sub(r"(\. ){3,}", PAUSE_DOUBLE, text)   # Max 2 titik
    else:
        # Semua kasus lain: kurangi ke single pause (tidak terlalu lambat)
        text = text.replace(PAUSE_DOUBLE, PAUSE_SINGLE)
        text = re.sub(r"(\. ){3,}", PAUSE_SINGLE, text)

    # ── Step 5: Normalisasi ────────────────────────────────────────────────────
    text = re.sub(r",(?!\s*\.)", ", ", text)         # Spasi setelah koma
    text = re.sub(r"\n\n+", "\n . \n", text)         # Paragraf → pause
    text = re.sub(r"\n(?!\s*\.)", "\n ", text)        # Newline → spasi
    text = re.sub(r"  +", " ", text).strip()          # Multiple space

    logger.debug(
        f"[preprocess] niche={niche} lang={language} profile={profile} "
        f"horror={is_horror} psycho={is_psycho} "
        f"chars: {len(raw_text)} → {len(text)}"
    )
    return text


# ══════════════════════════════════════════════════════════════════════════════
#  DYNAMIC TAGGING SYSTEM
#  [LOUD] [QUIET] [SLOW] bisa disisipkan langsung di naskah
#  Pipeline: split → render per segmen → apply FFmpeg filter → concat
# ══════════════════════════════════════════════════════════════════════════════

_TAG_SPLIT_RE = re.compile(r"\[(LOUD|QUIET|SLOW)\]", re.IGNORECASE)


def _split_tagged_segments(text: str) -> List[Tuple[str, str]]:
    """
    Pecah teks berdasarkan tag [LOUD]/[QUIET]/[SLOW].
    Return: list of (tag, text_segment) — segmen tanpa tag = "NORMAL"

    Contoh:
      "Intro biasa. [LOUD] Ini sangat mengejutkan! [QUIET] Bisikan..."
      → [("NORMAL","Intro biasa."), ("LOUD","Ini sangat..."), ("QUIET","Bisikan...")]
    """
    parts       = _TAG_SPLIT_RE.split(text)
    segments: List[Tuple[str, str]] = []
    current_tag = "NORMAL"

    for part in parts:
        if part.upper() in TAG_AUDIO_FILTERS:
            current_tag = part.upper()
        else:
            clean = part.strip()
            if clean:
                segments.append((current_tag, clean))
            current_tag = "NORMAL"   # Reset setelah satu segmen

    return segments if segments else [("NORMAL", text.strip())]


def _apply_segment_audio_filter(src_path: str, dst_path: str, tag: str) -> None:
    """
    Apply FFmpeg audio filter per segmen berdasarkan tag.
    NORMAL → copy langsung (tidak ada processing)
    LOUD/QUIET/SLOW → apply filter dari TAG_AUDIO_FILTERS
    """
    audio_filter = TAG_AUDIO_FILTERS.get(tag.upper())

    if audio_filter is None:
        # NORMAL: copy tanpa processing (lebih cepat)
        shutil.copy2(src_path, dst_path)
        return

    cmd = [
        "ffmpeg", "-y", "-i", src_path,
        "-af", audio_filter,
        "-acodec", "libmp3lame", "-q:a", "2",
        "-threads", str(AUDIO_FFMPEG_THREADS),
        dst_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"[tag_filter][{tag}] warn: {result.stderr[-300:]}")


def _concat_audio_segments(segment_paths: List[str], out_path: str) -> None:
    """
    Gabung semua segmen MP3 → apply loudnorm final.
    Loudnorm HANYA dilakukan sekali di sini (bukan per-segmen)
    agar efek LOUD/QUIET tetap terasa relatif satu sama lain.
    """
    if len(segment_paths) == 1:
        # Satu segmen: langsung optimize tanpa concat
        _optimize_audio(segment_paths[0], out_path, apply_loudnorm=True)
        return

    # Tulis concat list file
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False,
                                     encoding="utf-8") as list_file:
        for seg_path in segment_paths:
            list_file.write(f"file '{seg_path}'\n")
        list_file_path = list_file.name

    concat_tmp = out_path + ".concat_tmp.mp3"
    cmd_concat = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", list_file_path,
        "-c", "copy",           # Stream copy — tidak re-encode, sangat cepat
        concat_tmp,
    ]
    result = subprocess.run(cmd_concat, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"[concat_segments] warn: {result.stderr[-400:]}")

    os.remove(list_file_path)

    # Apply loudnorm final — satu pass untuk semua segmen
    _optimize_audio(concat_tmp, out_path, apply_loudnorm=True)

    try:
        os.remove(concat_tmp)
    except OSError:
        pass


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def generate(script_data: dict, channel: dict) -> str:
    """
    Entry point TTS. Pilih engine → preprocess → render → return path MP3.

    Flow:
    1. Ambil naskah dari script_data
    2. Tambah intro hook (long_form)
    3. Preprocess: KAPITAL, em-dash, pause markers
    4. Route ke engine: edge / gemini / f5
    5. Return path file audio MP3
    """
    ch_id        = channel["id"]
    niche        = channel.get("niche", "horror_facts")
    language     = channel.get("language", "en")
    profile      = script_data.get("profile", "shorts")
    voice_sample = channel.get("voice_sample", "")    # Hanya untuk F5-TTS
    raw_script   = script_data.get("script", "")

    # Tambah intro hook untuk Long Form
    intro_hook  = CHANNEL_INTRO_HOOKS.get(niche, {}).get(language, "")
    base_script = (f"{intro_hook}\n\n{raw_script}"
                   if profile == "long_form" and intro_hook
                   else raw_script)

    # Preprocess naskah sebelum dikirim ke engine manapun
    processed_script = preprocess_script(base_script, niche, language, profile)

    out_dir  = channel_data_path(ch_id, "audio")
    out_path = f"{out_dir}/{timestamp()}.mp3"
    voice    = _get_voice(niche, language)
    rate     = _get_rate(niche, profile)

    logger.info(
        f"[{ch_id}] TTS engine={TTS_MODE.upper()} | niche={niche} | "
        f"profile={profile} | voice={voice} | rate={rate} | "
        f"tags={'YES' if _has_dynamic_tags(processed_script) else 'NO'} | "
        f"{len(processed_script.split())} kata"
    )

    # Route ke engine yang dipilih
    if TTS_MODE == "edge":
        try:
            _run_edge_tts(processed_script, niche, language, profile, out_path)
            logger.info(f"[{ch_id}] Edge TTS OK → {out_path}")
            return out_path
        except Exception as e:
            logger.warning(f"Edge TTS gagal: {e}, coba Gemini...")
            try:
                _run_gemini_tts(processed_script, language, out_path)
                return out_path
            except Exception as e2:
                logger.warning(f"Gemini TTS juga gagal: {e2}")

    elif TTS_MODE == "gemini":
        try:
            _run_gemini_tts(processed_script, language, out_path)
            logger.info(f"[{ch_id}] Gemini TTS OK")
            return out_path
        except Exception as e:
            logger.warning(f"Gemini TTS gagal: {e}, fallback Edge...")

    elif TTS_MODE == "f5":
        if voice_sample:
            try:
                _run_f5_tts(processed_script, voice_sample, language, out_path)
                logger.info(f"[{ch_id}] F5-TTS OK")
                return out_path
            except Exception as e:
                logger.warning(f"F5-TTS gagal: {e}, fallback Edge...")
        else:
            logger.warning("F5-TTS: tidak ada voice_sample, fallback Edge")

    # Final fallback: Edge TTS selalu tersedia
    _run_edge_tts(processed_script, niche, language, profile, out_path)
    return out_path


# ══════════════════════════════════════════════════════════════════════════════
#  EDGE TTS ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _run_edge_tts(text: str, niche: str, language: str,
                  profile: str, out_path: str) -> None:
    """Sinkron wrapper untuk async Edge TTS."""
    asyncio.run(_run_edge_tts_async(text, niche, language, profile, out_path))


async def _run_edge_tts_async(text: str, niche: str, language: str,
                               profile: str, out_path: str) -> None:
    """
    Render teks ke MP3 via Edge TTS dengan voice fallback otomatis.

    Jika primary voice gagal → otomatis coba voice berikutnya dari NICHE_VOICE_MAP
    sampai semua exhausted, baru raise error.

    Jika ada dynamic tags → pakai tagged pipeline (split-render per segmen).
    Jika tidak ada tag → single render (lebih cepat).
    """
    import edge_tts

    rate        = _get_rate(niche, profile)
    max_voices  = _get_voice_count(niche, language)
    last_error  = None

    for voice_idx in range(max_voices):
        voice = _get_voice(niche, language, fallback_index=voice_idx)
        try:
            if _has_dynamic_tags(text):
                # Tagged pipeline: render per segmen, apply filter, concat
                await _render_tagged_segments(text, voice, rate, out_path)
            else:
                # Single render: langsung ke file + loudnorm
                raw_mp3 = out_path + ".raw.mp3"
                communicator = edge_tts.Communicate(text, voice, rate=rate)
                await communicator.save(raw_mp3)
                _optimize_audio(raw_mp3, out_path, apply_loudnorm=True)
                try:
                    os.remove(raw_mp3)
                except OSError:
                    pass

            if voice_idx > 0:
                logger.info(
                    f"Edge TTS: voice fallback #{voice_idx} '{voice}' berhasil"
                )
            return  # Berhasil → keluar dari loop

        except Exception as e:
            last_error = e
            has_next   = voice_idx < max_voices - 1
            logger.warning(
                f"Edge TTS: voice '{voice}' (idx={voice_idx}) gagal: {e}"
                + (" → coba fallback berikutnya" if has_next else " → semua voice habis")
            )
            continue

    # Semua voice exhausted
    raise RuntimeError(
        f"Semua {max_voices} voice Edge TTS gagal "
        f"(niche='{niche}', lang='{language}'). "
        f"Error terakhir: {last_error}"
    )


async def _render_tagged_segments(text: str, voice: str,
                                   rate: str, out_path: str) -> None:
    """
    Pipeline tagged segments:
    1. Split teks berdasarkan [LOUD]/[QUIET]/[SLOW] tag
    2. Render setiap segmen via Edge TTS
    3. Apply FFmpeg filter per tag
    4. Concat semua segmen
    5. Loudnorm final (hanya sekali — satu pass)
    """
    import edge_tts

    segments   = _split_tagged_segments(text)
    seg_paths: List[str] = []
    tmp_dir    = tempfile.mkdtemp(prefix="tts_seg_")

    logger.info(f"[tagged_render] {len(segments)} segmen: {[s[0] for s in segments]}")

    for idx, (tag, seg_text) in enumerate(segments):
        raw_path    = os.path.join(tmp_dir, f"seg_{idx:03d}_raw.mp3")
        effect_path = os.path.join(tmp_dir, f"seg_{idx:03d}_fx.mp3")

        # Render segmen dengan Edge TTS
        communicator = edge_tts.Communicate(seg_text, voice, rate=rate)
        await communicator.save(raw_path)

        # Apply per-segmen filter (TANPA loudnorm — dilakukan di concat)
        _apply_segment_audio_filter(raw_path, effect_path, tag)
        seg_paths.append(effect_path)

        try:
            os.remove(raw_path)
        except OSError:
            pass

    # Gabung semua segmen + loudnorm final
    _concat_audio_segments(seg_paths, out_path)

    shutil.rmtree(tmp_dir, ignore_errors=True)


# ══════════════════════════════════════════════════════════════════════════════
#  GEMINI TTS ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _run_gemini_tts(text: str, language: str, out_path: str) -> None:
    """
    Render teks via Google Gemini TTS API.
    Butuh GEMINI_API_KEY di .env.
    Output: WAV dari API → convert ke MP3 dengan loudnorm.
    """
    api_key    = require_env("GEMINI_API_KEY")
    api_url    = (f"https://generativelanguage.googleapis.com/v1beta/models/"
                  f"{GEMINI_TTS_MODEL}:generateContent?key={api_key}")
    voice_name = GEMINI_TTS_VOICE_EN if language == "en" else GEMINI_TTS_VOICE_ID

    payload = {
        "contents": [{"parts": [{"text": text}]}],
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "speechConfig": {
                "voiceConfig": {
                    "prebuiltVoiceConfig": {"voiceName": voice_name}
                }
            }
        }
    }

    response = requests.post(api_url, json=payload, timeout=GEMINI_TTS_TIMEOUT_SEC)
    response.raise_for_status()

    # Decode audio base64 dari response
    audio_b64  = (response.json()["candidates"][0]["content"]
                  ["parts"][0]["inlineData"]["data"])
    audio_data = base64.b64decode(audio_b64)

    # Tulis WAV sementara → convert ke MP3
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_wav:
        with wave.open(tmp_wav.name, "wb") as wav_file:
            wav_file.setnchannels(GEMINI_TTS_CHANNELS)
            wav_file.setsampwidth(GEMINI_TTS_SAMPWIDTH)
            wav_file.setframerate(GEMINI_TTS_SAMPLERATE)
            wav_file.writeframes(audio_data)
        _optimize_audio(tmp_wav.name, out_path, apply_loudnorm=True)
        os.remove(tmp_wav.name)


# ══════════════════════════════════════════════════════════════════════════════
#  F5-TTS ENGINE (Local Voice Cloning)
# ══════════════════════════════════════════════════════════════════════════════

_f5_model_instance = None   # Singleton — load sekali, reuse untuk efisiensi RAM


def _run_f5_tts(text: str, voice_sample_path: str,
                language: str, out_path: str) -> None:
    """
    Render teks via F5-TTS dengan voice cloning dari audio sample.

    Hardware note: F5-TTS berjalan di CPU agar tidak konflik dengan
    NVENC video_engine.py yang butuh VRAM 4GB RTX 3050.
    Konsekuensi: lebih lambat (~3-5 menit per menit audio).

    Butuh:
    - f5-tts package terinstall
    - voice_sample_path: file .wav referensi suara
    - File teks referensi di path yang sama: {voice_sample}.txt (opsional)
    """
    import soundfile as sf
    global _f5_model_instance

    # Lazy load model — load sekali, simpan di memory untuk reuse
    if _f5_model_instance is None:
        from f5_tts.api import F5TTS
        _f5_model_instance = F5TTS()
        logger.info("F5-TTS model loaded")

    # Tambah spasi ekstra setelah titik/koma untuk jeda alami
    processed = (text
                 .replace(". ", f".{F5_TTS_PAUSE_WORD}")
                 .replace(", ", f",{F5_TTS_PAUSE_WORD[:2]}"))

    # Load teks referensi untuk voice cloning (opsional, meningkatkan kualitas)
    ref_text_path = os.path.splitext(voice_sample_path)[0] + ".txt"
    ref_text      = ""
    if os.path.exists(ref_text_path):
        with open(ref_text_path, "r", encoding="utf-8") as f:
            ref_text = f.read().strip()

    # Infer — generate audio
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_wav:
        wav_data, sample_rate, _ = _f5_model_instance.infer(
            ref_file=voice_sample_path,
            ref_text=ref_text,
            gen_text=processed,
            seed=F5_TTS_SEED,
            speed=F5_TTS_SPEED,
        )
        sf.write(tmp_wav.name, wav_data, sample_rate)
        _optimize_audio(tmp_wav.name, out_path, apply_loudnorm=True)
        os.remove(tmp_wav.name)


# ══════════════════════════════════════════════════════════════════════════════
#  AUDIO OPTIMIZATION (SHARED)
# ══════════════════════════════════════════════════════════════════════════════

def _optimize_audio(src_path: str, dst_path: str,
                    speed: float = 1.0,
                    apply_loudnorm: bool = True) -> None:
    """
    Konversi & optimasi audio ke standar broadcast YouTube:
    - Sample rate: AUDIO_OUTPUT_SAMPLERATE (48kHz)
    - Bitrate: AUDIO_OUTPUT_BITRATE (192kbps MP3)
    - Loudnorm: AUDIO_LOUDNORM_TARGET LUFS (hanya jika apply_loudnorm=True)
    - Speed adjustment (atempo) jika speed != 1.0

    apply_loudnorm=False dipakai untuk segmen individual sebelum concat
    agar efek LOUD/QUIET tetap terasa relatif satu sama lain.
    apply_loudnorm=True dipakai di tahap AKHIR (single render atau setelah concat).
    """
    filters: List[str] = []

    # Speed adjustment via atempo (range 0.5–2.0 per filter instance)
    if speed != 1.0:
        if 0.5 <= speed <= 2.0:
            filters.append(f"atempo={speed:.4f}")
        else:
            # Speed di luar range → chain dua atempo filter
            s = min(max(speed, 0.25), 4.0) ** 0.5
            filters.append(f"atempo={s:.4f},atempo={s:.4f}")

    if apply_loudnorm:
        filters.append(
            f"loudnorm=I={AUDIO_LOUDNORM_TARGET}"
            f":TP={AUDIO_LOUDNORM_TP}"
            f":LRA={AUDIO_LOUDNORM_LRA}"
        )

    cmd = ["ffmpeg", "-y", "-i", src_path]
    cmd += ["-af", ",".join(filters)] if filters else ["-c:a", "copy"]
    cmd += [
        "-ar", str(AUDIO_OUTPUT_SAMPLERATE),
        "-ab", AUDIO_OUTPUT_BITRATE,
        "-acodec", "libmp3lame",
        "-q:a", "2",
        "-threads", str(AUDIO_FFMPEG_THREADS),
        dst_path,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"[optimize_audio] warn: {result.stderr[-300:]}")


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def clear_model_cache() -> None:
    """
    Hapus F5-TTS model dari memory.
    Panggil setelah semua video selesai dirender untuk bebaskan RAM (~2GB).
    """
    global _f5_model_instance
    _f5_model_instance = None
    logger.info("F5-TTS model cache cleared")


# Backward compatibility alias
def _wav_to_mp3(wav_path: str, mp3_path: str, speed_multiplier: float = 1.0) -> None:
    """Alias untuk _optimize_audio — dipertahankan untuk kompatibilitas."""
    _optimize_audio(wav_path, mp3_path, speed=speed_multiplier, apply_loudnorm=True)