"""
Channel Monitor, Subscriptions & Smart Binge Engine for Anime Downloader Bot.
Provides:
- Real-time web-scraping of public Telegram anime release channels (e.g. t.me/s/ridnyiholos).
- Automatic filtering by release type: Dub (озвучення) vs Sub (субтитри) vs Any.
- Catalog parsing for seasonal ongoings with episode list.
- User settings: Quiet Mode (09:00 - 18:30) with evening digest & Auto-Binge storage limit.
- Proactive notifications with live progress bar and ETA calculation.
- Evening watch reminders scheduler (e.g. 19:30 or 21:10).
"""

import asyncio
import datetime
import logging
import re
import shutil
import sqlite3
import time
from html import unescape
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp
try:
    from fex import FexDownloader
except ImportError:
    from anime_downloader.fex import FexDownloader

logger = logging.getLogger("ChannelMonitor")

DB_PATH = Path(__file__).resolve().parent / "file_registry.db"
VIDEO_EXTS = (".mkv", ".mp4", ".webm", ".avi", ".mov", ".m4v")


def init_channel_monitor_db():
    """Ініціалізує таблиці підписок, налаштувань, переглянутих постів та вечірніх нагадувань."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS anime_subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    channel TEXT NOT NULL DEFAULT 'ridnyiholos',
                    pref_type TEXT NOT NULL DEFAULT 'any',
                    created_at REAL NOT NULL
                )
            """)
            # Міграція v3.0: пряме джерело підписки (FEX-роздача та/або гілка медіатеки)
            sub_cols = [c[1] for c in conn.execute("PRAGMA table_info(anime_subscriptions)").fetchall()]
            if "fex_url" not in sub_cols:
                conn.execute("ALTER TABLE anime_subscriptions ADD COLUMN fex_url TEXT")
            if "thread_url" not in sub_cols:
                conn.execute("ALTER TABLE anime_subscriptions ADD COLUMN thread_url TEXT")
            try:
                us_cols = [c[1] for c in conn.execute("PRAGMA table_info(user_settings)").fetchall()]
                if us_cols and "seeded" not in us_cols:
                    conn.execute("ALTER TABLE user_settings ADD COLUMN seeded INTEGER DEFAULT 0")
            except Exception:
                pass
            conn.execute("""
                CREATE TABLE IF NOT EXISTS channel_seen_posts (
                    channel TEXT NOT NULL,
                    post_id INTEGER NOT NULL,
                    seen_at REAL NOT NULL,
                    PRIMARY KEY (channel, post_id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS evening_reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    file_key TEXT NOT NULL,
                    title TEXT NOT NULL,
                    remind_at REAL NOT NULL,
                    status TEXT DEFAULT 'pending'
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    chat_id INTEGER PRIMARY KEY,
                    quiet_mode INTEGER DEFAULT 0,
                    quiet_start INTEGER DEFAULT 9,
                    quiet_end INTEGER DEFAULT 18,
                    quiet_end_min INTEGER DEFAULT 30,
                    auto_binge_limit INTEGER DEFAULT 2
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quiet_digest_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    ep_num INTEGER,
                    file_path TEXT NOT NULL,
                    downloaded_at REAL NOT NULL,
                    status TEXT DEFAULT 'pending'
                )
            """)
            conn.commit()
    except Exception as e:
        logger.error(f"Помилка створення таблиць ChannelMonitor: {e}")


def get_user_settings(chat_id: int) -> Dict[str, Any]:
    """Повертає налаштування користувача (Тихий режим, ліміт серій)."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM user_settings WHERE chat_id = ?", (chat_id,)).fetchone()
        if row:
            return dict(row)
        # Значення за замовчуванням
        default_settings = {
            "chat_id": chat_id,
            "quiet_mode": 0,
            "quiet_start": 9,
            "quiet_end": 18,
            "quiet_end_min": 30,
            "auto_binge_limit": 2
        }
        conn.execute(
            "INSERT OR IGNORE INTO user_settings (chat_id, quiet_mode, quiet_start, quiet_end, quiet_end_min, auto_binge_limit) VALUES (?, 0, 9, 18, 30, 2)",
            (chat_id,)
        )
        conn.commit()
        return default_settings


def set_user_setting(chat_id: int, key: str, value: Any):
    """Оновлює параметр налаштувань користувача."""
    init_channel_monitor_db()
    get_user_settings(chat_id)  # гарантує існування запису
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(f"UPDATE user_settings SET {key} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()


def is_in_quiet_hours(chat_id: int) -> bool:
    """Перевіряє, чи активний зараз тихий робочий режим для користувача."""
    settings = get_user_settings(chat_id)
    if not settings.get("quiet_mode"):
        return False
    now = datetime.datetime.now()
    start_time = now.replace(hour=settings.get("quiet_start", 9), minute=0, second=0)
    end_time = now.replace(hour=settings.get("quiet_end", 18), minute=settings.get("quiet_end_min", 30), second=0)
    return start_time <= now < end_time


def add_to_digest_queue(chat_id: int, title: str, ep_num: Optional[int], file_path: str):
    """Додає завантажену в тихий режим серію до вечірнього дайджесту."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO quiet_digest_queue (chat_id, title, ep_num, file_path, downloaded_at, status) VALUES (?, ?, ?, ?, ?, 'pending')",
            (chat_id, title, ep_num, file_path, time.time())
        )
        conn.commit()


def get_pending_digest(chat_id: int) -> List[Dict[str, Any]]:
    """Повертає список завантажень, накопичених за робочий день."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM quiet_digest_queue WHERE chat_id = ? AND status = 'pending' ORDER BY id ASC",
            (chat_id,)
        ).fetchall()
        return [dict(r) for r in rows]


def clear_pending_digest(chat_id: int):
    """Позначає накопичений дайджест як відправлений."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE quiet_digest_queue SET status = 'sent' WHERE chat_id = ?", (chat_id,))
        conn.commit()


def add_subscription(chat_id: int, title: str, channel: str = "ridnyiholos", pref_type: str = "any") -> int:
    """Додає або оновлює підписку на тайтл із фільтром (dub, sub, any)."""
    init_channel_monitor_db()
    clean_title = title.strip()
    with sqlite3.connect(DB_PATH) as conn:
        # Перевіряємо чи є вже підписка на схожий тайтл
        existing = conn.execute(
            "SELECT id FROM anime_subscriptions WHERE chat_id = ? AND LOWER(title) = LOWER(?)",
            (chat_id, clean_title)
        ).fetchone()
        if existing:
            sub_id = existing[0]
            conn.execute(
                "UPDATE anime_subscriptions SET pref_type = ?, channel = ? WHERE id = ?",
                (pref_type.strip().lower(), channel.strip().lower(), sub_id)
            )
            conn.commit()
            return sub_id
        else:
            cursor = conn.execute(
                "INSERT INTO anime_subscriptions (chat_id, title, channel, pref_type, created_at) VALUES (?, ?, ?, ?, ?)",
                (chat_id, clean_title, channel.strip().lower(), pref_type.strip().lower(), time.time())
            )
            conn.commit()
            return cursor.lastrowid


def set_subscription_sources(chat_id: int, title: str, fex_url: Optional[str] = None, thread_url: Optional[str] = None) -> bool:
    """Зберігає пряме джерело (FEX / гілка медіатеки) для підписки на тайтл."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT id FROM anime_subscriptions WHERE chat_id = ? AND LOWER(title) = LOWER(?)",
            (chat_id, title.strip())
        ).fetchone()
        if not row:
            return False
        if fex_url:
            conn.execute("UPDATE anime_subscriptions SET fex_url = ? WHERE id = ?", (fex_url, row[0]))
        if thread_url:
            conn.execute("UPDATE anime_subscriptions SET thread_url = ? WHERE id = ?", (thread_url, row[0]))
        conn.commit()
        return True


def get_fex_seen(chat_id: int, share_key: str) -> Optional[set]:
    """Повертає множину вже оброблених імен файлів роздачі. None — якщо роздачу ще ніколи не сканували (baseline)."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fex_seen_files (
                chat_id INTEGER NOT NULL,
                share_key TEXT NOT NULL,
                file_name TEXT NOT NULL,
                seen_at REAL NOT NULL,
                PRIMARY KEY (chat_id, share_key, file_name)
            )
        """)
        rows = conn.execute(
            "SELECT file_name FROM fex_seen_files WHERE chat_id = ? AND share_key = ?", (chat_id, share_key)
        ).fetchall()
        if not rows:
            return None
        return {r[0] for r in rows}


def mark_fex_seen(chat_id: int, share_key: str, file_names: List[str]):
    """Позначає файли роздачі як оброблені (завантажені або внесені в baseline)."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fex_seen_files (
                chat_id INTEGER NOT NULL,
                share_key TEXT NOT NULL,
                file_name TEXT NOT NULL,
                seen_at REAL NOT NULL,
                PRIMARY KEY (chat_id, share_key, file_name)
            )
        """)
        now = time.time()
        conn.executemany(
            "INSERT OR IGNORE INTO fex_seen_files (chat_id, share_key, file_name, seen_at) VALUES (?, ?, ?, ?)",
            [(chat_id, share_key, n, now) for n in file_names]
        )
        conn.commit()


def pick_new_fex_episodes(files: List[Dict[str, Any]], seen: Optional[set], pref: str = "any") -> List[Dict[str, Any]]:
    """
    З файлів роздачі обирає по одному (найкращому за якістю) файлу для кожної НОВОЇ серії.
    Серія вважається новою, якщо жоден її файл ще не позначений як seen. Враховує фільтр dub/sub.
    """
    seen = seen or set()
    ep_re = re.compile(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]")
    by_ep: Dict[int, List[Dict[str, Any]]] = {}
    for f in files:
        m = ep_re.search(f.get("name", ""))
        if not m:
            continue
        by_ep.setdefault(int(m.group(1)), []).append(f)

    result = []
    for ep, ep_files in sorted(by_ep.items()):
        if any(f.get("name") in seen for f in ep_files):
            continue
        pool = ep_files
        if pref == "dub":
            pool = [f for f in ep_files if any(w in f.get("name", "") for w in ("MVO", "Озвуч", "DUB"))] or ep_files
        elif pref == "sub":
            pool = [f for f in ep_files if any(w in f.get("name", "") for w in ("SUB", "Саб"))] or ep_files
        p4k = [f for f in pool if any(q in f.get("name", "").lower() for q in ("2160p", "4k"))]
        p1080 = [f for f in pool if "1080p" in f.get("name", "").lower()]
        best = p4k[0] if p4k else (p1080[0] if p1080 else pool[0])
        result.append({"episode": ep, "file": best, "all_names": [f.get("name") for f in ep_files]})
    return result


def get_subscriptions(chat_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Повертає список активних підписок."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if chat_id:
            rows = conn.execute("SELECT * FROM anime_subscriptions WHERE chat_id = ? ORDER BY id DESC", (chat_id,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM anime_subscriptions ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]


def get_subscription_by_title(chat_id: int, title: str) -> Optional[Dict[str, Any]]:
    """Знаходить підписку за назвою тайтлу."""
    init_channel_monitor_db()
    clean_title_low = title.strip().lower()
    subs = get_subscriptions(chat_id)
    for s in subs:
        s_low = s["title"].lower()
        if s_low in clean_title_low or clean_title_low in s_low:
            return s
    return None


def delete_subscription(sub_id: int, chat_id: Optional[int] = None) -> bool:
    """Видаляє підписку за ID."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        if chat_id:
            cursor = conn.execute("DELETE FROM anime_subscriptions WHERE id = ? AND chat_id = ?", (sub_id, chat_id))
        else:
            cursor = conn.execute("DELETE FROM anime_subscriptions WHERE id = ?", (sub_id,))
        conn.commit()
        return cursor.rowcount > 0


def delete_subscription_by_title(chat_id: int, title: str) -> bool:
    """Видаляє підписку за назвою тайтлу."""
    init_channel_monitor_db()
    clean_title_low = title.strip().lower()
    subs = get_subscriptions(chat_id)
    deleted = False
    for s in subs:
        if s["title"].lower() in clean_title_low or clean_title_low in s["title"].lower():
            delete_subscription(s["id"], chat_id)
            deleted = True
    return deleted


def is_post_seen(channel: str, post_id: int) -> bool:
    """Перевіряє, чи цей пост уже оброблявся."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM channel_seen_posts WHERE channel = ? AND post_id = ?",
            (channel.lower(), post_id)
        ).fetchone()
        return row is not None


def mark_post_seen(channel: str, post_id: int):
    """Фіксує пост як оброблений."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO channel_seen_posts (channel, post_id, seen_at) VALUES (?, ?, ?)",
            (channel.lower(), post_id, time.time())
        )
        conn.commit()


def schedule_evening_reminder(chat_id: int, file_key: str, title: str, target_hour: int = 19, target_minute: int = 30) -> float:
    """Планує нагадування на вечірній час (наприклад, 19:30). Повертає timestamp."""
    init_channel_monitor_db()
    now = datetime.datetime.now()
    target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if target_time <= now:
        target_time += datetime.timedelta(days=1)
    
    remind_ts = target_time.timestamp()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO evening_reminders (chat_id, file_key, title, remind_at, status) VALUES (?, ?, ?, ?, 'pending')",
            (chat_id, file_key, title, remind_ts)
        )
        conn.commit()
    return remind_ts


def get_due_reminders() -> List[Dict[str, Any]]:
    """Повертає нагадування, час яких настав."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        now_ts = time.time()
        rows = conn.execute(
            "SELECT * FROM evening_reminders WHERE status = 'pending' AND remind_at <= ?",
            (now_ts,)
        ).fetchall()
        return [dict(r) for r in rows]


def mark_reminder_done(reminder_id: int):
    """Позначає нагадування як виконане."""
    init_channel_monitor_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE evening_reminders SET status = 'done' WHERE id = ?", (reminder_id,))
        conn.commit()


def _parse_html_blocks(html: str, channel: str) -> List[Dict[str, Any]]:
    parsed_posts = []
    blocks = html.split("tgme_widget_message_wrap")

    for block in blocks:
        post_id_m = re.search(r'data-post=\"[^/]+/(\d+)\"', block)
        if not post_id_m:
            continue
        post_id = int(post_id_m.group(1))

        text_m = re.search(r'tgme_widget_message_text[^>]*>(.*?)</div>', block, re.DOTALL)
        raw_html = text_m.group(1) if text_m else ""
        clean_text = unescape(re.sub(r'<[^>]+>', ' ', raw_html)).strip()

        # Витягуємо посилання
        raw_links = re.findall(r'<a[^>]+href=\"([^\"]+)\"[^>]*>(.*?)</a>', block, re.DOTALL)
        links = []
        for href, anchor_html in raw_links:
            anchor_clean = unescape(re.sub(r'<[^>]+>', '', anchor_html)).strip()
            links.append((anchor_clean, href))

        photo_m = re.search(r'background-image:url\(\'([^\']+)\'\)', block)
        photo_url = photo_m.group(1) if photo_m else None

        lower_text = clean_text.lower()
        if "з озвученням" in lower_text or "озвучили" in lower_text or "mvo" in lower_text:
            rel_type = "dub"
        elif "з субтитрами" in lower_text or "субтитри" in lower_text or "sub" in lower_text:
            rel_type = "sub"
        else:
            rel_type = "any"

        lines = [l.strip() for l in clean_text.splitlines() if l.strip()]
        first_line = lines[0] if lines else ""
        first_line_clean = re.sub(r"^[^\w\s]+", "", first_line).strip()
        title_parts = first_line_clean.split("/")
        parsed_title = title_parts[0].strip() if title_parts else first_line_clean

        # Ігноруємо дайджести "доброго ранку", збори та багатотайтлові списки серій
        if any(ign in parsed_title.lower() for ign in ["доброго ран", "з днюхою", "баночк", "дякуєм", "донателло", "із субтитрами", "із озвученням", "усього"]):
            continue
        if "🟣" in clean_text:
            continue

        ep_m = re.search(r"\[(\d+)\s*з|(\d+)\s*сері", clean_text, re.IGNORECASE)
        ep_num = int(ep_m.group(1) or ep_m.group(2)) if ep_m else None

        parsed_posts.append({
            "post_id": post_id,
            "channel": channel,
            "title": parsed_title,
            "full_title_line": first_line_clean,
            "ep_num": ep_num,
            "release_type": rel_type,
            "clean_text": clean_text,
            "links": links,
            "photo_url": photo_url
        })
    return parsed_posts


async def fetch_channel_posts(channel: str = "ridnyiholos", pages: int = 2) -> List[Dict[str, Any]]:
    """
    Парсить публічні пости з каналу Telegram (підтримує мультисторінковий збір до 40-50 постів).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    }

    all_posts = []
    oldest_id = None

    try:
        async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as session:
            for page in range(pages):
                url = f"https://t.me/s/{channel}"
                if oldest_id:
                    url += f"?before={oldest_id}"

                async with session.get(url) as resp:
                    if resp.status != 200:
                        break
                    html = await resp.text()

                posts = _parse_html_blocks(html, channel)
                if not posts:
                    break
                all_posts.extend(posts)
                oldest_id = min(p["post_id"] for p in posts)
    except Exception as e:
        logger.error(f"Помилка з'єднання при парсингу каналу {channel}: {e}")

    # Прибираємо дублікати за post_id
    seen_ids = set()
    unique_posts = []
    for p in all_posts:
        if p["post_id"] not in seen_ids:
            seen_ids.add(p["post_id"])
            unique_posts.append(p)

    return sorted(unique_posts, key=lambda x: x["post_id"], reverse=True)


_CATALOG_CACHE: Dict[str, Any] = {"data": None, "timestamp": 0}


async def fetch_ongoings_catalog(channel: str = "ridnyiholos", force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Формує каталог актуальних онґоїнгів сезону з доступними номерами серій та посиланнями.
    Кешує результат на 3 хвилини для миттєвої навігації в інтерфейсі Telegram.
    """
    global _CATALOG_CACHE
    now = time.time()
    if not force_refresh and _CATALOG_CACHE["data"] and (now - _CATALOG_CACHE["timestamp"] < 180):
        return _CATALOG_CACHE["data"]

    posts = await fetch_channel_posts(channel, pages=7)
    catalog: Dict[str, Dict[str, Any]] = {}

    for p in posts:
        t = p["title"]
        if not t or len(t) < 3:
            continue
        # Нормалізуємо для групування: відсікаємо дужки та номери сезонів (2, II, 2 сезон)
        norm_key = re.sub(r"\(.*?\)|\[.*?\]", "", t).strip()
        norm_key = re.sub(r"\b(\d+|[IVXLCDM]+)\s*(?:сезон)?$", "", norm_key, flags=re.IGNORECASE).strip()
        norm_key = re.sub(r"\s+", " ", norm_key).strip()
        if not norm_key:
            norm_key = t

        if norm_key not in catalog:
            catalog[norm_key] = {
                "title": norm_key,
                "display_name": t,
                "channel": channel,
                "episodes": set(),
                "episodes_dub": set(),
                "episodes_sub": set(),
                "types": set(),
                "poster_url": p.get("photo_url"),
                "posts": []
            }

        ep_val = p.get("ep_num")
        rel_val = p.get("release_type", "any")
        if ep_val:
            catalog[norm_key]["episodes"].add(ep_val)
            if rel_val == "dub":
                catalog[norm_key]["episodes_dub"].add(ep_val)
            elif rel_val == "sub":
                catalog[norm_key]["episodes_sub"].add(ep_val)
            else:
                catalog[norm_key]["episodes_dub"].add(ep_val)
                catalog[norm_key]["episodes_sub"].add(ep_val)

        catalog[norm_key]["types"].add(rel_val)
        if not catalog[norm_key]["poster_url"] and p.get("photo_url"):
            catalog[norm_key]["poster_url"] = p.get("photo_url")
        catalog[norm_key]["posts"].append(p)

    # Перетворюємо множини у відсортовані списки та визначаємо режим доступності (has_fex / media_url)
    for k, item in catalog.items():
        item["episodes"] = sorted(list(item["episodes"]))
        item["episodes_dub"] = sorted(list(item["episodes_dub"]))
        item["episodes_sub"] = sorted(list(item["episodes_sub"]))
        item["types"] = sorted(list(item["types"]))

        has_fex = False
        media_url = None
        fex_share_key = None
        for p in item["posts"]:
            for l_lbl, l_url in p["links"]:
                if "fex.net" in l_url:
                    has_fex = True
                    if not fex_share_key:
                        fex_share_key = FexDownloader.extract_share_key(l_url)
                elif "mediareser" in l_url and not media_url:
                    media_url = l_url

        item["has_fex"] = has_fex
        item["fex_share_key"] = fex_share_key
        item["media_url"] = media_url
        item["delivery_mode"] = "auto_fex" if has_fex else "tg_notify"

    _CATALOG_CACHE["data"] = catalog
    _CATALOG_CACHE["timestamp"] = time.time()
    return catalog


def enforce_auto_binge_limit(folder_path: Path, max_keep: int = 2) -> List[str]:
    """
    Auto-Binge режим: залишає на Mac тільки max_keep найсвіжіших серій тайтлу,
    автоматично видаляючи старіші переглянуті епізоди для збереження вільного місця.
    Повертає список видалених файлів.
    """
    if not folder_path.exists() or max_keep <= 0:
        return []

    video_files = [
        f for f in folder_path.iterdir()
        if f.is_file() and f.suffix.lower() in VIDEO_EXTS and not f.name.endswith(".part")
    ]

    if len(video_files) <= max_keep:
        return []

    # Сортуємо за часом зміни або номером серії (найстаріші першими)
    video_files.sort(key=lambda x: x.stat().st_mtime)

    deleted_names = []
    excess_count = len(video_files) - max_keep
    files_to_remove = video_files[:excess_count]

    for vf in files_to_remove:
        stem = vf.stem
        # Видаляємо відео
        vf.unlink(missing_ok=True)
        # Видаляємо thumb
        thumb = vf.with_name(f"{stem}.thumb.jpg")
        if thumb.exists():
            thumb.unlink(missing_ok=True)
        deleted_names.append(vf.name)
        logger.info(f"Auto-Binge: видалено старий епізод {vf.name}")

    return deleted_names


def calculate_download_eta(size_bytes: int = 500 * 1024 * 1024, avg_speed_bps: float = 12 * 1024 * 1024) -> str:
    """Розраховує орієнтовний час завантаження на основі типової швидкості Mac (12 МБ/с)."""
    if avg_speed_bps <= 0:
        avg_speed_bps = 10 * 1024 * 1024
    seconds = max(5, int(size_bytes / avg_speed_bps))
    if seconds < 60:
        return f"{seconds} сек"
    minutes = seconds // 60
    rem_sec = seconds % 60
    return f"{minutes} хв {rem_sec} сек" if rem_sec else f"{minutes} хв"
