"""
thumbnail_engine.py - Generate thumbnail viral & high-CTR (YouTube 2026 Standards)
- Face Detection & Saliency: Auto-center, auto-exposure wajah/objek utama.
- Depth Trick (Blur BG): Background gaussian blur, foreground tajam.
- Rim Light Effect: Menambahkan pendaran cahaya di sekitar objek fokus (3D Pop).
- Grain Texture: Tekstur halus untuk mencegah degradasi warna saat kompresi YouTube.
- Niche Emoji Injection: Auto-overlay emoji viral sesuai topik (💀, 🧠, 😱).
- Text Anti-Crash & Multi-line: Auto-scaling dan pemisahan baris teks dinamis.
- Tilted Text & Vignette: Sudut teks miring (dinamis) & efek vignette (fokus tengah).
"""

import os
import re
import random
import textwrap
import numpy as np
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance
from engine.utils import get_logger, timestamp, channel_data_path
from engine.memory_engine import build_packaging_memory

# ── Thumbnail Intelligence (style library + anti-repetisi) ────────────────────
try:
    from engine.thumbnail_intelligence import pick_and_generate_text as _pick_style_text
    _THUMB_INTEL_AVAILABLE = True
except ImportError:
    _THUMB_INTEL_AVAILABLE = False

try:
    import cv2
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False

logger = get_logger("thumbnail_engine")

# Font Prioritas: Impact adalah Raja CTR di YouTube.
FONTS_WINDOWS = [
    "C:/Windows/Fonts/impact.ttf",     
    "C:/Windows/Fonts/arialbd.ttf",    
    "C:/Windows/Fonts/calibrib.ttf",
]

# Palet Varian CTR Pintar dengan skema warna komplementer
VARIANT_COLORS = {
    1: { # V1: Nuke Yellow (The CTR King)
        "accent": (255, 10, 10), "ctr_text": (255, 240, 0), "overlay": (15, 0, 0, 210)
    },
    2: { # V2: Neon Cyan vs Deep Navy (Modern Mystery)
        "accent": (0, 255, 255), "ctr_text": (0, 255, 200), "overlay": (0, 10, 40, 220)
    },
    3: { # V3: Extreme Red vs Black (High Danger)
        "accent": (255, 50, 0), "ctr_text": (255, 255, 255), "overlay": (40, 0, 0, 230)
    }
}

# Kamus Emoji per Niche
NICHE_EMOJIS = {
    "horror_facts": ["💀", "😱", "👻", "🔪", "🩸", "🌑"],
    "psychology": ["🧠", "🤯", "🤫", "👁️", "🧩", "🎭"],
    "default": ["🔥", "⚠️", "🚨", "📌"]
}

def _load_font(size: int) -> ImageFont.FreeTypeFont:
    for font_path in FONTS_WINDOWS:
        try:
            return ImageFont.truetype(font_path, size)
        except Exception:
            continue
    logger.warning("Font Windows tidak ditemukan, pakai default PIL")
    return ImageFont.load_default()

# ─── 1. Computer Vision: Face & Saliency ──────────────────────────────────────

def _find_focal_point(img: Image.Image) -> tuple:
    """Mencari titik fokus: Wajah (Prioritas 1) atau Saliency Edges (Prioritas 2)."""
    w, h = img.size
    default_pt = (w // 2, h // 2, 180, False)
    
    if not CV2_AVAILABLE:
        return default_pt

    cv_img = np.array(img.convert('RGB'))[:, :, ::-1].copy()
    gray   = cv2.cvtColor(cv_img, cv2.COLOR_BGR2GRAY)
    
    # Face Detection
    cascade_path = os.path.join(cv2.data.haarcascades, 'haarcascade_frontalface_default.xml')
    if os.path.exists(cascade_path):
        face_cascade = cv2.CascadeClassifier(cascade_path)
        faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(70, 70))
        
        if len(faces) > 0:
            faces = sorted(faces, key=lambda x: x[2] * x[3], reverse=True)
            x, y, fw, fh = faces[0]
            return (x + fw // 2, y + fh // 2, max(fw, fh), True)
            
    # Fallback to Saliency
    edges = cv2.Canny(gray, 100, 200)
    M = cv2.moments(edges)
    if M["m00"] != 0:
        cx = int(M["m10"] / M["m00"])
        cy = int(M["m01"] / M["m00"])
        return (cx, cy, 200, False)

    return default_pt

# ─── 2. Visual Enhancements (Rim Light, Depth, Vignette) ──────────────────────

def _apply_depth_and_enhance(img: Image.Image, focal_pt: tuple) -> Image.Image:
    """Efek 3D: Blur background, Rim Light di tepian objek, dan Sharp foreground."""
    cx, cy, r, is_face = focal_pt
    
    # Background: Blur + Low Brightness
    bg_blur = img.filter(ImageFilter.GaussianBlur(radius=5))
    bg_blur = ImageEnhance.Brightness(bg_blur).enhance(0.75)
    
    # Foreground: Super Sharp + High Saturation
    fg_sharp = img.filter(ImageFilter.UnsharpMask(radius=4, percent=220, threshold=2))
    fg_sharp = ImageEnhance.Contrast(fg_sharp).enhance(1.4)
    fg_sharp = ImageEnhance.Color(fg_sharp).enhance(1.5)
    
    if is_face:
        # UPGRADE: Color Pop & Extra Brightness khusus Wajah/Fokus
        fg_sharp = ImageEnhance.Brightness(fg_sharp).enhance(1.25)
        fg_sharp = ImageEnhance.Color(fg_sharp).enhance(1.4) # Kulit lebih merona/glowing
        
    # Buat Masking Radial untuk transisi
    mask = Image.new("L", img.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse([cx - r*2.0, cy - r*2.0, cx + r*2.0, cy + r*2.0], fill=255)
    mask = mask.filter(ImageFilter.GaussianBlur(radius=r*0.7))
    
    # GABUNGKAN
    composite = Image.composite(fg_sharp, bg_blur, mask)

    # TAMBAHKAN RIM LIGHT (Efek Pendaran Cahaya di tepian fokus)
    rim_layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    rim_draw  = ImageDraw.Draw(rim_layer)
    # Gambar outline putih/kuning pudar
    glow_color = (255, 255, 200, 150)
    rim_draw.ellipse([cx - r*1.9, cy - r*1.9, cx + r*1.9, cy + r*1.9], outline=glow_color, width=12)
    rim_layer = rim_layer.filter(ImageFilter.GaussianBlur(radius=15))
    
    composite.paste(rim_layer, (0,0), rim_layer)
    return composite

def _add_vignette(img: Image.Image, intensity: float = 0.5) -> Image.Image:
    """UPGRADE: Menambahkan gelap di sudut gambar untuk memaksa mata fokus ke tengah."""
    w, h = img.size
    x = np.linspace(-1, 1, w)
    y = np.linspace(-1, 1, h)
    X, Y = np.meshgrid(x, y)
    d = np.sqrt(X*X + Y*Y)
    
    mask = 1 - np.clip(d * intensity, 0, 1)
    img_arr = np.array(img.convert("RGBA"), dtype=np.float32)
    
    # Gelapkan RGB channel, biarkan Alpha utuh
    img_arr[:, :, :3] = img_arr[:, :, :3] * mask[:, :, np.newaxis]
    
    return Image.fromarray(img_arr.astype(np.uint8), "RGBA")

def _add_grain_texture(img: Image.Image, intensity: float = 0.06) -> Image.Image:
    """Menambahkan noise/grain halus agar gambar tidak terlihat 'flat'."""
    img_arr = np.array(img).astype(np.float32)
    noise = np.random.normal(0, intensity * 255, img_arr.shape)
    img_arr = np.clip(img_arr + noise, 0, 255).astype(np.uint8)
    return Image.fromarray(img_arr)

# ─── 3. Contextual Elements (Text & Emojis) ───────────────────────────────────

def _get_contextual_text(title: str, niche: str, language: str) -> str:
    impact_id = ["TERLARANG", "GILA", "DIBONGKAR", "RAHASIA", "MENCEKAM", "AWAS", "HANCUR", "MENGERIKAN", "JANGAN"]
    impact_en = ["BANNED", "INSANE", "LEAKED", "SECRET", "SCARY", "WARNING", "DESTROYED", "TERRIFYING", "STOP"]
    
    impact_words = impact_id if language == "id" else impact_en
    return f"{random.choice(impact_words)}!"

def _inject_niche_emojis(img_rgba: Image.Image, niche: str, w: int, h: int, is_shorts: bool) -> Image.Image:
    """Menambahkan emoji viral berukuran besar di posisi strategis."""
    emojis = NICHE_EMOJIS.get(niche, NICHE_EMOJIS["default"])
    emoji = random.choice(emojis)
    
    draw = ImageDraw.Draw(img_rgba)
    font = _load_font(180 if is_shorts else 140)
    
    # Posisi: Kanan Tengah atau Kanan Bawah
    ex = int(w * 0.75)
    ey = int(h * 0.65) if is_shorts else int(h * 0.5)
    
    # Shadow Emoji
    draw.text((ex+8, ey+8), emoji, font=font, fill=(0,0,0,150))
    # Main Emoji
    draw.text((ex, ey), emoji, font=font, fill=(255,255,255,255))
    
    return img_rgba

# ─── 4. Text Scaling & Rendering ──────────────────────────────────────────────

def _get_optimal_font_and_text(text: str, base_size: int, max_width: int) -> tuple:
    """UPGRADE: Memisahkan teks panjang menjadi 2 baris (Wrap) jika melebihi batas layar."""
    font_size = base_size
    font = _load_font(font_size)
    
    # Coba pecah jadi 2 baris jika teksnya lebih dari 1 kata
    if " " in text:
        test_wrapped = textwrap.fill(text, width=len(text)//2 + 1)
        lines = test_wrapped.split('\n')
        max_line_w = max(font.getbbox(line)[2] - font.getbbox(line)[0] for line in lines)
        
        # Jika setelah di-wrap ukurannya pas, gunakan text yang di-wrap
        if max_line_w <= max_width:
            return font, font_size, test_wrapped
        text = test_wrapped # Update ke teks berlapis untuk proses shrinking

    # Shrink (perkecil) jika masih terlalu lebar
    lines = text.split('\n')
    max_line_w = max(font.getbbox(line)[2] - font.getbbox(line)[0] for line in lines)
    
    while max_line_w > max_width and font_size > 70:
        font_size -= 10
        font = _load_font(font_size)
        max_line_w = max(font.getbbox(line)[2] - font.getbbox(line)[0] for line in lines)
        
    return font, font_size, text

def _draw_premium_text(img_rgba: Image.Image, text: str, font: ImageFont.FreeTypeFont,
                       container_w: int, y: int, color_main: tuple, stroke_width: int = 12, 
                       align: str = "center", x_offset: int = 0, tilt_angle: int = 6) -> Image.Image:
    """UPGRADE: Multi-line support & Tilted Text (Miring 6 Derajat)."""
    
    # Canvas transparan khusus untuk text (agar bisa di-rotate mandiri)
    txt_layer = Image.new("RGBA", img_rgba.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(txt_layer)
    
    bbox = draw.multiline_textbbox((0, 0), text, font=font, align=align)
    tw   = bbox[2] - bbox[0]
    th   = bbox[3] - bbox[1]
    x = (container_w - tw) // 2 if align == "center" else x_offset

    # Layer 1: Gaussian Blur Shadow (Depth)
    shadow_layer = Image.new("RGBA", img_rgba.size, (0, 0, 0, 0))
    s_draw = ImageDraw.Draw(shadow_layer)
    s_draw.multiline_text((x + 15, y + 15), text, font=font, fill=(0, 0, 0, 255), 
                          stroke_width=stroke_width+5, stroke_fill=(0, 0, 0, 255), align=align)
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(18))
    txt_layer = Image.alpha_composite(txt_layer, shadow_layer)

    # Layer 2: Hard Stroke & Text
    draw = ImageDraw.Draw(txt_layer)
    draw.multiline_text((x, y), text, font=font, fill=color_main, 
                        stroke_width=stroke_width, stroke_fill=(0,0,0,255), align=align)
    
    # UPGRADE: Putar Teks (Tilted) agar lebih dinamis & klik-able
    if tilt_angle != 0:
        txt_layer = txt_layer.rotate(tilt_angle, resample=Image.Resampling.BICUBIC, center=(x + tw/2, y + th/2))
        
    img_rgba = Image.alpha_composite(img_rgba, txt_layer)
    return img_rgba

# ─── Master Pipeline ──────────────────────────────────────────────────────────

def generate(script_data: dict, video_path: str, channel: dict, profile: str = "shorts") -> str:
    ch_id    = channel["id"]
    lang     = channel.get("language", "id")
    niche    = channel.get("niche", "default")
    title    = script_data.get("title", "")
    packaging_memory = build_packaging_memory(channel) if channel.get("niche") == "horror_facts" else {}
    out_dir  = channel_data_path(ch_id, "output")
    ts       = timestamp()

    logger.info(f"[{ch_id}] [{profile}] Generating Ultimate 2026 CTR Thumbnail Variants...")
    base_out_path = f"{out_dir}/{ts}_{profile}_thumb"

    is_sh = (profile != "long_form")

    # Menghasilkan 3 varian otomatis
    for v in [1, 2, 3]:
        path = f"{base_out_path}_v{v}.png"
        _build_layout(title, video_path, path, niche, lang, variant=v, is_shorts=is_sh,
                      script_data=script_data, packaging_memory=packaging_memory, channel=channel)

    return f"{base_out_path}_v1.png"

def _build_layout(title: str, video_path: str, out_path: str, niche: str, lang: str, variant: int, is_shorts: bool,
                  script_data: dict | None = None, packaging_memory: dict | None = None,
                  channel: dict | None = None):
    W, H   = (1080, 1920) if is_shorts else (1280, 720)
    colors = VARIANT_COLORS[variant]

    # 1. Capture & Saliency
    raw_bg = _extract_best_frame(video_path, W, H)
    if raw_bg is None: raw_bg = Image.new("RGB", (W, H), (20, 20, 25))
    
    focal_pt = _find_focal_point(raw_bg)
    
    # 2. Depth, Rim Light & Vignette (UPGRADED)
    bg = _apply_depth_and_enhance(raw_bg, focal_pt)
    bg = _add_vignette(bg, intensity=0.5) 
    
    # 3. Grain Texture
    bg = _add_grain_texture(bg, intensity=0.05).convert("RGBA")

    # 4. Shape Language (Circle/Bubble)
    if variant == 1: bg = _draw_clickbait_circle(bg, W, H, focal_pt)
    elif variant == 2: bg = _draw_zoom_bubble(bg, W, H, focal_pt)

    # 5. Overlay Cinematic
    overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    o_draw  = ImageDraw.Draw(overlay)
    ov_color = colors["overlay"]
    
    if is_shorts:
        # Darkening top & bottom
        for y in range(int(H * 0.45)):
            alpha = int(ov_color[3] * (1 - (y / (H * 0.45))))
            o_draw.line([(0, y), (W, y)], fill=(*ov_color[:3], alpha))
    else:
        # Left-heavy dark gradient
        for x in range(int(W * 0.65)):
            alpha = int(ov_color[3] * (1 - (x / (W * 0.65))))
            o_draw.line([(x, 0), (x, H)], fill=(*ov_color[:3], alpha))

    bg = Image.alpha_composite(bg, overlay)
    
    # 6. Emojis (Dipertahankan menggunakan Font/Text Draw)
    bg = _inject_niche_emojis(bg, niche, W, H, is_shorts)

    # 7. Dynamic Text (UPGRADED: Multi-line & Tilted)
    punchy_text = _resolve_thumbnail_text(title, niche, lang, script_data or {}, packaging_memory or {}, channel=channel)
    base_sz     = 240 if is_shorts else 190
    
    # Text Auto-Wrap Process
    font, final_sz, wrapped_text = _get_optimal_font_and_text(punchy_text, base_sz, int(W * 0.85))
    
    if is_shorts:
        # Tilted Text ke atas sedikit (Tilt angle = 6)
        bg = _draw_premium_text(bg, wrapped_text, font, W, int(H * 0.15), colors["ctr_text"], tilt_angle=6)
    else:
        bg = _draw_premium_text(bg, wrapped_text, font, W, (H-final_sz)//2, colors["ctr_text"], align="left", x_offset=70, tilt_angle=5)

    bg.convert("RGB").save(out_path, "PNG", optimize=True)


def _resolve_thumbnail_text(title: str, niche: str, language: str,
                            script_data: dict, packaging_memory: dict,
                            channel: dict | None = None) -> str:
    """
    Prioritas penentuan teks thumbnail:
    1. script creative_direction.thumbnail_text (dari LLM saat generate script)
    2. thumbnail_intelligence style library (anti-repetisi, pattern-driven)
    3. _get_contextual_text() — random impact word (existing fallback)
    """
    # Tier 1: creative_direction dari script
    creative = script_data.get("creative_direction", {})
    if isinstance(creative, dict):
        thumbnail_text = str(creative.get("thumbnail_text", "")).strip()
        if thumbnail_text and not _is_repetitive_thumbnail_text(thumbnail_text, packaging_memory):
            return thumbnail_text

    # Tier 2: thumbnail_intelligence style library
    if _THUMB_INTEL_AVAILABLE and channel:
        try:
            intel_text = _pick_style_text(channel, title, script_data)
            if intel_text:
                return intel_text
        except Exception as exc:
            logger.debug(f"thumbnail_intelligence fallback: {exc}")

    # Tier 3: random impact word (existing behavior)
    return _get_contextual_text(title, niche, language)


def _is_repetitive_thumbnail_text(text: str, packaging_memory: dict) -> bool:
    recent = [str(item).strip().lower() for item in packaging_memory.get("recent_thumbnail_texts", []) if str(item).strip()]
    target = text.strip().lower()
    older = recent[1:6] if recent else []
    return target in older

# ─── Helpers (Circle, Zoom, Frame) ────────────────────────────────────────────

def _draw_clickbait_circle(img_rgba: Image.Image, w: int, h: int, focal_pt: tuple) -> Image.Image:
    draw = ImageDraw.Draw(img_rgba)
    cx, cy, r, _ = focal_pt
    radius = min(r * 1.3, 350)
    draw.ellipse([cx-radius, cy-radius, cx+radius, cy+radius], outline=(255, 10, 10, 240), width=20)
    return img_rgba

def _draw_zoom_bubble(img_rgba: Image.Image, w: int, h: int, focal_pt: tuple) -> Image.Image:
    cx, cy, r, _ = focal_pt
    r_src, r_dst = int(r * 0.7), int(r * 1.4)
    box = (cx - r_src, cy - r_src, cx + r_src, cy + r_src)
    try:
        cropped = img_rgba.crop(box).resize((r_dst * 2, r_dst * 2), Image.Resampling.LANCZOS)
        mask = Image.new('L', (r_dst * 2, r_dst * 2), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, r_dst * 2, r_dst * 2), fill=255)
        paste_y = cy + int(r_src*1.8) if cy + r_dst*2 < h else cy - int(r_src*2.8)
        img_rgba.paste(cropped, (cx - r_src, paste_y), mask)
        ImageDraw.Draw(img_rgba).ellipse([cx-r_src, paste_y, cx-r_src+r_dst*2, paste_y+r_dst*2], outline=(255, 240, 0, 255), width=15)
    except: pass
    return img_rgba

def _extract_best_frame(video_path: str, w: int, h: int) -> Image.Image | None:
    import subprocess, tempfile
    for seek in ["00:00:04", "00:00:06", "00:00:02"]:
        try:
            tmp = tempfile.mktemp(suffix=".jpg")
            subprocess.run(["ffmpeg", "-y", "-ss", seek, "-i", video_path, "-vframes", "1",
                            "-vf", f"scale={w}:{h}:force_original_aspect_ratio=increase,crop={w}:{h}", tmp], capture_output=True)
            if os.path.exists(tmp):
                img = Image.open(tmp).convert("RGB")
                os.remove(tmp)
                return img
        except: continue
    return None
