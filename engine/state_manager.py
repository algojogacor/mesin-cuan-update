"""
state_manager.py - SQLite database untuk tracking status pipeline
"""

import sqlite3
import os
from datetime import datetime
from engine.utils import get_logger

logger = get_logger("state_manager")
DB_PATH = "logs/pipeline.db"


def init_db():
    """Buat tabel kalau belum ada."""
    os.makedirs("logs", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id  TEXT NOT NULL,
            topic       TEXT,
            status      TEXT DEFAULT 'pending',
            script_path TEXT,
            audio_path  TEXT,
            video_path  TEXT,
            thumbnail_path TEXT,
            upload_url  TEXT,
            error_msg   TEXT,
            created_at  TEXT DEFAULT (datetime('now')),
            updated_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS footage_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id  TEXT NOT NULL,
            clip_url    TEXT NOT NULL,
            used_at     TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS upload_quota (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id  TEXT NOT NULL,
            upload_date TEXT NOT NULL,
            count       INTEGER DEFAULT 0,
            UNIQUE(channel_id, upload_date)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS video_registry (
            video_id             TEXT PRIMARY KEY,
            channel_id           TEXT NOT NULL,
            title                TEXT DEFAULT '',
            topic                TEXT DEFAULT '',
            niche                TEXT DEFAULT '',
            language             TEXT DEFAULT '',
            profile              TEXT DEFAULT 'shorts',
            published_at         TEXT DEFAULT '',
            views                INTEGER DEFAULT 0,
            likes                INTEGER DEFAULT 0,
            avg_view_duration_sec REAL DEFAULT 0,
            avg_view_percentage  REAL DEFAULT 0,
            analytics_fetched    INTEGER DEFAULT 0,
            basic_stats_fetched  INTEGER DEFAULT 0,
            source               TEXT DEFAULT '',
            synced_at            TEXT DEFAULT (datetime('now')),
            updated_at           TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()
    logger.info("Database initialized")


def create_run(channel_id: str, topic: str, profile: str = "shorts") -> int:
    """Buat record baru, return run_id."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO pipeline_runs (channel_id, topic, status) VALUES (?, ?, 'pending')",
        (channel_id, topic)
    )
    run_id = c.lastrowid
    conn.commit()
    conn.close()
    logger.debug(f"[{channel_id}] Created run #{run_id} for topic: {topic}")
    return run_id


def update_run(run_id: int, **kwargs):
    """Update field apapun di pipeline_runs."""
    kwargs["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fields = ", ".join([f"{k} = ?" for k in kwargs])
    values = list(kwargs.values()) + [run_id]
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(f"UPDATE pipeline_runs SET {fields} WHERE id = ?", values)
    conn.commit()
    conn.close()


def mark_failed(run_id: int, error_msg: str):
    update_run(run_id, status="failed", error_msg=error_msg)
    logger.error(f"Run #{run_id} FAILED: {error_msg}")


def mark_uploaded(run_id: int, upload_url: str):
    update_run(run_id, status="uploaded", upload_url=upload_url)
    logger.info(f"Run #{run_id} uploaded: {upload_url}")


def get_upload_count_today(channel_id: str) -> int:
    """Berapa video sudah diupload hari ini untuk channel ini."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT count FROM upload_quota WHERE channel_id = ? AND upload_date = ?",
        (channel_id, today)
    )
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0


def increment_upload_count(channel_id: str):
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO upload_quota (channel_id, upload_date, count) VALUES (?, ?, 1)
        ON CONFLICT(channel_id, upload_date) DO UPDATE SET count = count + 1
    """, (channel_id, today))
    conn.commit()
    conn.close()


def is_footage_used_recently(channel_id: str, clip_url: str, days: int = 30) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT id FROM footage_history
        WHERE channel_id = ? AND clip_url = ?
        AND used_at >= datetime('now', ?)
    """, (channel_id, clip_url, f"-{days} days"))
    row = c.fetchone()
    conn.close()
    return row is not None


def record_footage_used(channel_id: str, clip_url: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO footage_history (channel_id, clip_url) VALUES (?, ?)",
        (channel_id, clip_url)
    )
    conn.commit()
    conn.close()


# Inisialisasi otomatis saat module diimport
init_db()


# ─── Video Registry (untuk retention_engine) ──────────────────────────────────

def upsert_video(data: dict):
    """
    INSERT OR REPLACE video ke tabel video_registry.
    data harus berisi minimal: video_id, channel_id.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO video_registry (
            video_id, channel_id, title, topic, niche, language, profile,
            published_at, views, likes, avg_view_duration_sec,
            avg_view_percentage, analytics_fetched, basic_stats_fetched,
            source, synced_at, updated_at
        ) VALUES (
            :video_id, :channel_id, :title, :topic, :niche, :language, :profile,
            :published_at, :views, :likes, :avg_view_duration_sec,
            :avg_view_percentage, :analytics_fetched, :basic_stats_fetched,
            :source, :synced_at, :updated_at
        )
        ON CONFLICT(video_id) DO UPDATE SET
            title                = excluded.title,
            views                = excluded.views,
            likes                = excluded.likes,
            avg_view_duration_sec = excluded.avg_view_duration_sec,
            avg_view_percentage  = excluded.avg_view_percentage,
            analytics_fetched    = excluded.analytics_fetched,
            basic_stats_fetched  = excluded.basic_stats_fetched,
            source               = excluded.source,
            updated_at           = excluded.updated_at
    """, {
        "video_id":              data.get("video_id", ""),
        "channel_id":            data.get("channel_id", ""),
        "title":                 data.get("title", ""),
        "topic":                 data.get("topic", data.get("title", "")),
        "niche":                 data.get("niche", ""),
        "language":              data.get("language", ""),
        "profile":               data.get("profile", "shorts"),
        "published_at":          data.get("published_at", ""),
        "views":                 data.get("views", 0),
        "likes":                 data.get("likes", 0),
        "avg_view_duration_sec": data.get("avg_view_duration_sec", 0),
        "avg_view_percentage":   data.get("avg_view_percentage", 0),
        "analytics_fetched":     1 if data.get("analytics_fetched") else 0,
        "basic_stats_fetched":   1 if data.get("basic_stats_fetched") else 0,
        "source":                data.get("source", ""),
        "synced_at":             data.get("synced_at", datetime.now().isoformat()),
        "updated_at":            datetime.now().isoformat(),
    })
    conn.commit()
    conn.close()


def get_videos_for_channel(channel_id: str) -> list:
    """Ambil semua video untuk satu channel dari DB, return list of dict."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        "SELECT * FROM video_registry WHERE channel_id = ? ORDER BY published_at DESC",
        (channel_id,)
    )
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows


def get_video_ids_for_channel(channel_id: str) -> set:
    """Return set of video_id yang sudah ada di DB untuk channel ini."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT video_id FROM video_registry WHERE channel_id = ?",
        (channel_id,)
    )
    rows = {row[0] for row in c.fetchall()}
    conn.close()
    return rows


def count_videos_for_channel(channel_id: str) -> int:
    """Hitung jumlah video yang sudah di-register untuk channel ini."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT COUNT(*) FROM video_registry WHERE channel_id = ?",
        (channel_id,)
    )
    count = c.fetchone()[0]
    conn.close()
    return count
