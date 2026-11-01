import hashlib
import re
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "file_registry.db"


def init_mac_sync_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mac_device_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                device_name TEXT,
                last_seen REAL,
                free_disk_gb REAL,
                downloads_folder TEXT,
                uptime_str TEXT,
                is_online INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mac_local_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_key TEXT UNIQUE,
                folder_name TEXT,
                file_name TEXT,
                full_path TEXT,
                file_size INTEGER,
                updated_at REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mac_pending_commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                payload TEXT,
                status TEXT DEFAULT 'pending',
                created_at REAL NOT NULL,
                executed_at REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mac_handled_episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                title_norm TEXT NOT NULL,
                episode INTEGER NOT NULL,
                file_name TEXT,
                handled_at REAL NOT NULL,
                UNIQUE(chat_id, title_norm, episode)
            )
        """)
        conn.commit()


def update_mac_device_state(
    device_name: str,
    free_disk_gb: float,
    downloads_folder: str,
    uptime_str: str,
    files: List[Dict[str, Any]],
):
    """Оновлює стан Mac і повний список локальних відеофайлів."""
    init_mac_sync_db()
    now = time.time()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO mac_device_state (id, device_name, last_seen, free_disk_gb, downloads_folder, uptime_str, is_online)
            VALUES (1, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(id) DO UPDATE SET
                device_name = excluded.device_name,
                last_seen = excluded.last_seen,
                free_disk_gb = excluded.free_disk_gb,
                downloads_folder = excluded.downloads_folder,
                uptime_str = excluded.uptime_str,
                is_online = 1
        """, (device_name, now, free_disk_gb, downloads_folder, uptime_str))

        # Оновлюємо список файлів
        conn.execute("DELETE FROM mac_local_files")
        for f in files:
            f_key = f.get("key") or hashlib.md5(f["path"].encode("utf-8")).hexdigest()[:10]
            conn.execute("""
                INSERT OR REPLACE INTO mac_local_files (file_key, folder_name, file_name, full_path, file_size, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (f_key, f.get("folder", "Різне"), f.get("name", "video.mp4"), f.get("path", ""), f.get("size", 0), now))
        conn.commit()


def get_mac_device_state() -> Optional[Dict[str, Any]]:
    """Повертає стан підключення Mac."""
    init_mac_sync_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM mac_device_state WHERE id = 1").fetchone()
        if not row:
            return None
        res = dict(row)
        # Якщо від останнього пінгу пройшло більше 90 секунд — Mac спить або офлайн
        res["is_online"] = (time.time() - res["last_seen"]) < 90
        return res


def get_synced_mac_files() -> Dict[str, List[Dict[str, Any]]]:
    """Повертає згруповані відеофайли з Mac."""
    init_mac_sync_db()
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM mac_local_files ORDER BY folder_name, file_name").fetchall()
        for r in rows:
            f_dict = dict(r)
            grouped.setdefault(f_dict["folder_name"], []).append(f_dict)
    return grouped


def queue_mac_command(action: str, payload: str = "") -> int:
    """Ставить команду для виконання на Mac (наприклад, видалити, відкрити у Finder/IINA)."""
    init_mac_sync_db()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("""
            INSERT INTO mac_pending_commands (action, payload, status, created_at)
            VALUES (?, ?, 'pending', ?)
        """, (action, payload, time.time()))
        conn.commit()
        return cur.lastrowid


def get_pending_mac_commands() -> List[Dict[str, Any]]:
    """Забирає невиконані команди для Mac-агента."""
    init_mac_sync_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM mac_pending_commands WHERE status = 'pending' ORDER BY id ASC").fetchall()
        return [dict(r) for r in rows]


def mark_mac_command_done(cmd_id: int):
    init_mac_sync_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE mac_pending_commands SET status = 'done', executed_at = ? WHERE id = ?", (time.time(), cmd_id))
        conn.commit()


def is_episode_handled_or_downloaded(chat_id: int, title: str, ep_num: int) -> bool:
    """Перевіряє, чи ця серія вже була завантажена, поставлена в чергу або вже є на Mac."""
    if not title or not ep_num:
        return False
    from channel_monitor import normalize_title_for_match
    t_norm = normalize_title_for_match(title)
    init_mac_sync_db()
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Перевіряємо mac_handled_episodes (за останні 24 години)
        row = conn.execute(
            "SELECT handled_at FROM mac_handled_episodes WHERE chat_id = ? AND title_norm = ? AND episode = ?",
            (chat_id, t_norm, ep_num)
        ).fetchone()
        if row and (time.time() - row[0]) < 86400:
            return True

        # 2. Перевіряємо mac_local_files (файли, що вже лежать на Mac)
        ep_pats = [
            rf"\[0?{ep_num}\s*(?:з|/|-|\])",
            rf"S\d+E0?{ep_num}\b",
            rf"\b0?{ep_num}\s*сері",
        ]
        files = conn.execute("SELECT folder_name, file_name FROM mac_local_files").fetchall()
        for folder_name, file_name in files:
            f_norm = normalize_title_for_match(folder_name)
            if (f_norm and (f_norm in t_norm or t_norm in f_norm)) or (title.lower() in folder_name.lower()):
                if any(re.search(pat, file_name, re.IGNORECASE) for pat in ep_pats):
                    return True
    return False


def mark_episode_handled(chat_id: int, title: str, ep_num: int, file_name: str = ""):
    """Зберігає факт обробки серії, щоб запобігти подвійним сповіщенням та перезавантаженням."""
    if not title or not ep_num:
        return
    from channel_monitor import normalize_title_for_match
    t_norm = normalize_title_for_match(title)
    init_mac_sync_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO mac_handled_episodes (chat_id, title_norm, episode, file_name, handled_at)
            VALUES (?, ?, ?, ?, ?)
        """, (chat_id, t_norm, ep_num, file_name, time.time()))
        conn.commit()


