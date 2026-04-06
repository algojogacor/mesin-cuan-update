"""
utils.py - Helper functions shared across all engines
"""
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─── Logger Setup ─────────────────────────────────────────────────────────────
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        import sys
        ch = logging.StreamHandler(
            stream=open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        os.makedirs("logs", exist_ok=True)
        fh = logging.FileHandler(
            f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


# ─── Config Loader ────────────────────────────────────────────────────────────
def load_settings() -> dict:
    with open("config/settings.json", "r", encoding="utf-8") as f:
        return json.load(f)


def get_channel_config(channel_id: str) -> dict:
    settings = load_settings()
    for ch in settings["channels"]:
        if ch["id"] == channel_id:
            return ch
    raise ValueError(f"Channel '{channel_id}' not found in settings.json")


def load_prompt(niche: str, language: str, profile: str = "shorts") -> str:
    """
    Load prompt template.
    - shorts   : templates/prompts/{niche_slug}_{language}.txt
    - long_form: templates/prompts/{niche_slug}_{language}_long.txt
    niche_slug: horror_facts → horror, psychology → psychology
    """
    niche_slug = niche.replace("_facts", "")
    suffix     = "_long" if profile == "long_form" else ""
    path       = f"templates/prompts/{niche_slug}_{language}{suffix}.txt"

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Prompt tidak ditemukan: {path}\n"
            f"Pastikan file ada di folder templates/prompts/"
        )

    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ─── File & Path Helpers ──────────────────────────────────────────────────────
def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def channel_data_path(channel_id: str, subfolder: str) -> str:
    path = f"data/{channel_id}/{subfolder}"
    os.makedirs(path, exist_ok=True)
    return path


def save_json(data: dict, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ─── Env Helpers ──────────────────────────────────────────────────────────────
def require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return val
