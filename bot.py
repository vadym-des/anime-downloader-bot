import asyncio
import hashlib
import json
import logging
import os
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))

import aiohttp
from mac_sync_schema import (
    init_mac_sync_db,
    cancel_pending_download_commands,
    get_mac_device_state,
    get_synced_mac_files,
    queue_mac_command,
    is_episode_handled_or_downloaded,
    mark_episode_handled,
    get_file_watch_status,
    get_all_watch_statuses,
    set_file_watch_status,
    get_watchlist_hub_msg_id,
    set_watchlist_hub_msg_id,
    clear_watchlist_hub_msg_id,
)
from sync_server import run_sync_server
from channel_monitor import (
    init_channel_monitor_db,
    normalize_title_for_match,
    normalize_telegram_url,
    add_subscription,
    get_subscriptions,
    get_subscription_by_title,
    delete_subscription,
    delete_subscription_by_title,
    get_max_seen_post_id,
    is_post_seen,
    mark_post_seen,
    schedule_evening_reminder,
    get_due_reminders,
    mark_reminder_done,
    fetch_channel_posts,
    fetch_ongoings_catalog,
    calculate_download_eta,
    get_user_settings,
    set_user_setting,
    set_subscription_sources,
    get_fex_seen,
    mark_fex_seen,
    pick_new_fex_episodes,
    is_in_quiet_hours,
    add_to_digest_queue,
    get_pending_digest,
    clear_pending_digest,
    enforce_auto_binge_limit,
)
from seed_data import seed_subscriptions
from config import (
    ALLOWED_USERS,
    BASE_DIR,
    BOT_TOKEN,
    DOWNLOADS_DIR,
    TAG_COLOR,
    TAG_NAME,
    TELEGRAM_API_HASH,
    TELEGRAM_API_ID,
)
from fex import FexDownloader
from progress import DownloadProgressTracker, format_bytes, render_progress_bar
from tagger import set_macos_tag
from telegram_mtproto import TelegramMtprotoDownloader
from ytdlp_downloader import download_web_video
from notion_tasks import parse_task_prefix, send_task
from update_manager import (
    CURRENT_VERSION,
    check_for_new_release,
    auto_reload_watcher,
    init_update_db,
    get_current_git_info,
    pull_latest_from_github,
    save_manual_restart_request,
    pop_manual_restart_request,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AnimeDownloaderBot")

dp = Dispatcher()

# Ініціалізація MTProto (Telethon) для завантаження важких файлів із Telegram без ліміту 20 МБ
mtproto_downloader = None
if TELEGRAM_API_ID and TELEGRAM_API_HASH:
    mtproto_downloader = TelegramMtprotoDownloader(
        api_id=TELEGRAM_API_ID,
        api_hash=TELEGRAM_API_HASH,
        bot_token=BOT_TOKEN,
        session_dir=BASE_DIR,
    )
    logger.info("MTProto рушій успішно налаштовано для обходу лімітів Telegram!")

# Семафор для обмеження паралельних завантажень (максимум 2 одночасно)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(2)
START_TIME = time.time()

# Постійна база даних для кнопок, що переживає будь-які перезавантаження Mac
DB_PATH = BASE_DIR / "file_registry.db"
FILE_REGISTRY: Dict[str, Path] = {}


def init_registry_db():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_registry (
                    file_key TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
            conn.commit()
    except Exception as e:
        logger.error(f"Помилка створення БД реєстру: {e}")


def register_file(file_path: Any) -> str:
    """Реєструє файл у RAM та постійну SQLite базу для збереження після перезапусків."""
    path_str = str(file_path)
    file_id = hashlib.md5(path_str.encode("utf-8")).hexdigest()[:10]
    if hasattr(file_path, "_size"):
        FILE_REGISTRY[file_id] = file_path
    else:
        p = Path(path_str)
        FILE_REGISTRY[file_id] = p.resolve() if p.exists() else file_path
    try:
        init_registry_db()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO file_registry (file_key, file_path, created_at) VALUES (?, ?, ?)",
                (file_id, path_str, time.time()),
            )
            conn.commit()
    except Exception as e:
        logger.debug(f"Не вдалося зберегти в SQLite: {e}")
    return file_id


def init_offline_queue_db():
    """Створює таблицю черги відкладених завантажень."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS offline_download_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    title TEXT,
                    raw_text TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at REAL NOT NULL,
                    processed_at REAL
                )
            """)
            conn.commit()
    except Exception as e:
        logger.error(f"Помилка створення таблиці offline_download_queue: {e}")


def enqueue_offline_task(chat_id: int, message_id: int, title: str, raw_text: str) -> int:
    init_offline_queue_db()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO offline_download_queue (chat_id, message_id, title, raw_text, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
            (chat_id, message_id, title, raw_text, time.time())
        )
        conn.commit()
        return cur.lastrowid


def get_pending_offline_tasks() -> List[Dict[str, Any]]:
    init_offline_queue_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM offline_download_queue WHERE status = 'pending' ORDER BY id ASC").fetchall()
        return [dict(r) for r in rows]


def mark_offline_task(task_id: int, status: str = "completed"):
    init_offline_queue_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE offline_download_queue SET status = ?, processed_at = ? WHERE id = ?", (status, time.time(), task_id))
        conn.commit()


CURRENT_PLAYING_KEY: Optional[str] = None


def set_currently_playing(file_key: str):
    """Фіксує серію, яку користувач відкрив для перегляду."""
    global CURRENT_PLAYING_KEY
    CURRENT_PLAYING_KEY = file_key


def is_file_currently_playing(file_key: str) -> bool:
    """Перевіряє, чи файл зараз позначено як активний і чи відкритий IINA у macOS."""
    global CURRENT_PLAYING_KEY
    if not CURRENT_PLAYING_KEY or CURRENT_PLAYING_KEY != file_key:
        return False
    try:
        res = subprocess.run(["/usr/bin/pgrep", "-x", "IINA"], capture_output=True)
        if res.returncode == 0:
            return True
        else:
            CURRENT_PLAYING_KEY = None
            return False
    except Exception:
        return False


TITLE_REGISTRY: Dict[str, Any] = {}


def register_title_folder(folder_path: Any) -> str:
    """Реєструє папку тайтлу для перегляду згрупованого списку серій."""
    path_str = str(folder_path)
    t_id = "t_" + hashlib.md5(path_str.encode("utf-8")).hexdigest()[:8]
    if hasattr(folder_path, "_files"):
        TITLE_REGISTRY[t_id] = folder_path
    else:
        p = Path(path_str)
        TITLE_REGISTRY[t_id] = p.resolve() if p.exists() else folder_path
    try:
        init_registry_db()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO file_registry (file_key, file_path, created_at) VALUES (?, ?, ?)",
                (t_id, path_str, time.time()),
            )
            conn.commit()
    except Exception:
        pass
    return t_id


def resolve_title_folder(title_key: str, message: Optional[Message] = None) -> Optional[Any]:
    """Знаходить папку тайтлу за ключем у RAM, синхронізованих Mac-файлах або SQLite."""
    if title_key not in TITLE_REGISTRY:
        get_grouped_anime()
    path = TITLE_REGISTRY.get(title_key)
    if path and path.exists() and path.is_dir():
        return path
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute("SELECT file_path FROM file_registry WHERE file_key = ?", (title_key,)).fetchone()
                if row:
                    p = Path(row[0])
                    if p.exists() and p.is_dir():
                        TITLE_REGISTRY[title_key] = p
                        return p
        except Exception:
            pass
    return None


TRACK_TITLE_REGISTRY: Dict[str, str] = {}


def register_track_title(title: str) -> str:
    """Зберігає назву аніме для меню моніторингу під коротким ключем."""
    clean_title = title.strip()
    k = hashlib.md5(clean_title.lower().encode("utf-8")).hexdigest()[:8]
    TRACK_TITLE_REGISTRY[k] = clean_title
    try:
        init_registry_db()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO file_registry (file_key, file_path, created_at) VALUES (?, ?, ?)",
                ("trk_" + k, clean_title, time.time())
            )
            conn.commit()
    except Exception:
        pass
    return k


def resolve_track_title(title_key: str) -> Optional[str]:
    """Відновлює назву аніме за ключем із пам'яті або БД."""
    if title_key in TRACK_TITLE_REGISTRY:
        return TRACK_TITLE_REGISTRY[title_key]
    try:
        init_registry_db()
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT file_path FROM file_registry WHERE file_key = ?", ("trk_" + title_key,)).fetchone()
            if row:
                t = str(row[0])
                TRACK_TITLE_REGISTRY[title_key] = t
                return t
    except Exception:
        pass
    return None


SMART_CLEANUP_TASKS: Dict[str, asyncio.Task] = {}


async def smart_cleanup_timer(bot: Bot, user_id: int, file_key: str, delay_seconds: int = 1200):
    """Через 20 хвилин після запуску IINA надсилає тихе повідомлення з пропозицією видалити переглянуту серію."""
    try:
        await asyncio.sleep(delay_seconds)
        file_path = resolve_file(file_key)
        if not file_path or not file_path.exists():
            return

        raw_name = file_path.parent.name if file_path.parent != DOWNLOADS_DIR else file_path.stem
        clean_title = format_anime_display_name(raw_name, max_len=50)
        size_str = format_bytes(file_path.stat().st_size)

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=f"🗑 Так, видалити ({size_str})", callback_data=f"smart_del:{file_key}"),
                    InlineKeyboardButton(text="👌 Ні, залишити", callback_data=f"smart_keep:{file_key}"),
                ]
            ]
        )

        await bot.send_message(
            chat_id=user_id,
            text=(
                "🍿 <b>Ви вже подивилися цю серію?</b>\n\n"
                f"🎬 <b>{clean_title}</b>\n"
                f"📄 <code>{file_path.name}</code>\n"
                f"📦 Займає <code>{size_str}</code> на диску Mac.\n\n"
                "<i>Видалити її, щоб звільнити місце на диску?</i>"
            ),
            reply_markup=kb,
            disable_notification=True,
        )
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.debug(f"Smart cleanup timer error: {e}")
    finally:
        SMART_CLEANUP_TASKS.pop(file_key, None)


def sanitize_folder_name(name: str) -> str:
    """Очищує назву аніме для створення безпечної назви папки в macOS."""
    clean = re.sub(r"[^\w\s\-\.\,\(\)\[\]]", "", name, flags=re.UNICODE)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean[:80] if clean else "Різне"


def get_free_disk_space_gb() -> float:
    """Повертає вільне місце на диску в гігабайтах."""
    try:
        usage = shutil.disk_usage(DOWNLOADS_DIR if DOWNLOADS_DIR.exists() else Path.home())
        return usage.free / (1024 ** 3)
    except Exception:
        return 0.0


def launch_in_iina(file_path: Any) -> Tuple[bool, str]:
    """Запускає відеофайл у відеоплеєрі IINA на Mac (підтримує локальний Mac та серверний міст)."""
    import platform
    p_str = str(file_path.resolve() if hasattr(file_path, "resolve") else file_path)

    # Якщо ми на сервері Linux (Railway) або файл фізично не існує локально —
    # ми НЕ викликаємо локальний /usr/bin/open, а передаємо команду нашому Mac-агенту!
    if platform.system() != "Darwin" or not Path(p_str).exists():
        queue_mac_command("open_iina", p_str)
        return True, "IINA на Mac"

    try:
        open_bin = "/usr/bin/open" if Path("/usr/bin/open").exists() else "open"
        iina_app = Path("/Applications/IINA.app")
        if iina_app.exists():
            subprocess.Popen([open_bin, "-a", str(iina_app), p_str])
            return True, "IINA"
        else:
            subprocess.Popen([open_bin, p_str])
            return True, "відеоплеєрі"
    except Exception as e:
        logger.error(f"Помилка запуску IINA: {e}")
        return False, str(e)


def reveal_in_finder(file_path: Any) -> bool:
    """Відкриває та підсвічує завантажений файл у Finder на Mac."""
    import platform
    p_str = str(file_path.resolve() if hasattr(file_path, "resolve") else file_path)
    if platform.system() != "Darwin" or not Path(p_str).exists():
        queue_mac_command("reveal_folder", p_str)
        return True

    try:
        open_bin = "/usr/bin/open" if Path("/usr/bin/open").exists() else "open"
        p = Path(p_str)
        if p.exists():
            subprocess.Popen([open_bin, "-R", str(p.resolve())])
            return True
        elif p.parent.exists():
            subprocess.Popen([open_bin, str(p.parent.resolve())])
            return True
    except Exception as e:
        logger.error(f"Помилка відкриття у Finder: {e}")
    return False


def build_downloaded_action_keyboard(file_key: str, title: Optional[str] = None, chat_id: Optional[int] = None) -> InlineKeyboardMarkup:
    """Створює зручні інлайн-кнопки для завантаженого відео: плеєр, Finder, видалення та повернення до списку."""
    rows = [
        [
            InlineKeyboardButton(text="▶️ Запустити в плеєрі", callback_data=f"play:{file_key}"),
            InlineKeyboardButton(text="📁 Відкрити у Finder", callback_data=f"finder:{file_key}"),
        ],
        [
            InlineKeyboardButton(text="🗑 Видалити", callback_data=f"del:{file_key}"),
        ],
        [
            InlineKeyboardButton(text="⬅️ Назад до списку", callback_data="refresh_list"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_video_thumbnail(video_path: Any) -> Optional[Path]:
    """
    Знаходить найкращу прев'юшку / обкладинку для аніме:
    0. Синхронізований постер із Mac-агента (для роботи на Railway)
    1. Точне прев'ю з Telegram для цієї серії (<stem>.thumb.jpg)
    2. Офіційний постер аніме в папці тайтлу (poster.jpg, cover.jpg)
    3. Вбудована обкладинка з MKV/MP4 контейнера (attachments)
    4. Кешоване прев'ю
    5. Запасний варіант: стоп-кадр через FFmpeg
    """
    # 0. Синхронізований постер із Mac-агента
    folder_name = video_path.parent.name if hasattr(video_path, "parent") and video_path.parent else ""
    if folder_name:
        f_hash = hashlib.md5(folder_name.encode("utf-8")).hexdigest()[:12]
        synced_poster = BASE_DIR / "cache" / "posters" / f"{f_hash}.jpg"
        if synced_poster.exists() and synced_poster.stat().st_size > 500:
            return synced_poster

    if hasattr(video_path, "_size") and not Path(str(video_path)).exists():
        return None

    if not video_path.exists() or not video_path.is_file():
        return None

    # 1. Точне прев'ю цієї серії, надіслане в Telegram
    ep_thumb = video_path.with_name(f"{video_path.stem}.thumb.jpg")
    if ep_thumb.exists() and ep_thumb.stat().st_size > 500:
        return ep_thumb

    # 2. Офіційний постер тайтлу в папці аніме
    parent_dir = video_path.parent
    if parent_dir.exists() and parent_dir != DOWNLOADS_DIR:
        for name in ("poster.jpg", "cover.jpg", "poster.png", "cover.png", "folder.jpg"):
            poster = parent_dir / name
            if poster.exists() and poster.stat().st_size > 500:
                return poster

    # 3. Кеш
    thumb_dir = BASE_DIR / "cache" / "thumbnails"
    thumb_dir.mkdir(parents=True, exist_ok=True)
    stat = video_path.stat()
    file_hash = hashlib.md5(f"{video_path.name}_{stat.st_size}_{stat.st_mtime}".encode()).hexdigest()[:12]
    thumb_file = thumb_dir / f"{file_hash}.jpg"
    if thumb_file.exists() and thumb_file.stat().st_size > 500:
        return thumb_file

    ffmpeg_bin = "/opt/homebrew/bin/ffmpeg" if Path("/opt/homebrew/bin/ffmpeg").exists() else "ffmpeg"

    # 4. Спроба дістати вбудовану обкладинку (cover art attachment) з самого MKV/MP4
    try:
        cmd_attach = [
            ffmpeg_bin,
            "-y",
            "-dump_attachment:t:0", str(thumb_file),
            "-i", str(video_path.resolve()),
        ]
        res_attach = subprocess.run(cmd_attach, capture_output=True, timeout=4)
        if thumb_file.exists() and thumb_file.stat().st_size > 500:
            return thumb_file
    except Exception:
        pass

    # 5. Резервний пошук кадру на 00:02:30
    try:
        cmd1 = [
            ffmpeg_bin,
            "-y",
            "-ss", "00:02:30",
            "-i", str(video_path.resolve()),
            "-vframes", "1",
            "-vf", "scale=640:-1",
            "-q:v", "3",
            str(thumb_file)
        ]
        res = subprocess.run(cmd1, capture_output=True, timeout=6)
        if res.returncode != 0 or not thumb_file.exists() or thumb_file.stat().st_size == 0:
            cmd2 = [
                ffmpeg_bin,
                "-y",
                "-ss", "00:00:15",
                "-i", str(video_path.resolve()),
                "-vframes", "1",
                "-vf", "scale=640:-1",
                "-q:v", "3",
                str(thumb_file)
            ]
            subprocess.run(cmd2, capture_output=True, timeout=6)

        if thumb_file.exists() and thumb_file.stat().st_size > 500:
            return thumb_file
    except Exception as e:
        logger.debug(f"Не вдалося згенерувати прев'ю для {video_path.name}: {e}")
    return None


async def notify_download_finished(
    bot: Bot,
    chat_id: int,
    status_msg: Message,
    dest_file: Path,
    title: str,
    prefix_text: str = "🎉 <b>Готово! Серія успішно завантажена на ноут!</b>"
):
    """Надсилає картку завершення завантаження з прев'юшкою аніме (кадром) та кнопками перегляду."""
    file_key = register_file(dest_file)
    kb = build_downloaded_action_keyboard(file_key, title=title, chat_id=chat_id)
    size_str = format_bytes(dest_file.stat().st_size)
    free_gb = get_free_disk_space_gb()

    # Перевірка ліміту Auto-Binge на диску
    settings = get_user_settings(chat_id)
    binge_limit = settings.get("auto_binge_limit", 2)
    binge_note = ""
    if binge_limit > 0 and dest_file.parent != DOWNLOADS_DIR:
        removed = enforce_auto_binge_limit(dest_file.parent, max_keep=binge_limit)
        if removed:
            binge_note = f"\n📦 <i>Звільнено місце: {len(removed)} старішу серію очищено (Auto-Binge).</i>"

    # Витягуємо інформацію про епізод, якість та доріжки для чистого формату
    fname = dest_file.name
    ep_m = re.search(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]", fname)
    ep_badge = f" [Серія {int(ep_m.group(1))}]" if ep_m else ""

    quality_str = "4K Ultra HD" if "2160p" in fname else ("1080p Full HD" if "1080p" in fname else "1080p Full HD")
    tracks_label = detect_tracks_label(fname, title, chat_id)

    caption = (
        f"{prefix_text}\n\n"
        f"🎬 <b>{title}</b>{ep_badge}\n"
        f"🎞 <b>Якість:</b> <code>{quality_str}</code>\n"
        f"🔊 <b>Доріжки:</b> <i>{tracks_label}</i>\n"
        f"💾 <b>Вільно на Mac:</b> <code>{free_gb:.1f} ГБ</code>"
        f"{binge_note}\n\n"
        f"📂 <b>Шлях:</b> <code>~/{dest_file.parent.name}/{dest_file.name}</code>"
    )

    thumb = get_video_thumbnail(dest_file)
    if thumb:
        try:
            await status_msg.delete()
        except Exception:
            pass
        await bot.send_photo(
            chat_id=chat_id,
            photo=FSInputFile(thumb),
            caption=caption,
            reply_markup=kb
        )
    else:
        await status_msg.edit_text(caption, reply_markup=kb)


# Відстеження активних завдань завантаження для можливості скасування в реальному часі
ACTIVE_DOWNLOAD_TASKS: Dict[str, asyncio.Task] = {}


def build_cancel_keyboard(cancel_id: str) -> InlineKeyboardMarkup:
    """Створює зручну кнопку скасування завантаження."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🛑 Скасувати завантаження", callback_data=f"cancel_dl:{cancel_id}")
            ]
        ]
    )


VIDEO_EXTS = (".mkv", ".mp4", ".webm", ".avi", ".mov", ".m4v")


def cleanup_empty_folder(folder: Path):
    """
    Видаляє папку тайтлу, якщо в ній більше немає відеофайлів.
    Повністю видаляє залишені постери (poster.jpg), прев'ю (<stem>.thumb.jpg) та підпапки.
    """
    if not folder.exists() or folder == DOWNLOADS_DIR:
        return
    try:
        remaining_videos = [
            f for f in folder.iterdir()
            if f.is_file() and f.suffix.lower() in VIDEO_EXTS and not f.name.endswith(".part")
        ]
        if not remaining_videos:
            logger.info(f"У папці {folder.name} не залишилося відео. Повністю видаляю всю папку та її постери/прев'ю.")
            shutil.rmtree(folder, ignore_errors=True)
    except Exception as e:
        logger.debug(f"Не вдалося видалити папку {folder}: {e}")


def clean_orphan_anime_folders() -> int:
    """Видаляє всі папки в (Завантажене аніме), в яких немає жодного відео (лише залишені постери/прев'ю)."""
    if not DOWNLOADS_DIR.exists():
        return 0
    cleaned = 0
    try:
        for item in list(DOWNLOADS_DIR.iterdir()):
            if item.is_dir():
                has_video = any(
                    f.is_file() and f.suffix.lower() in VIDEO_EXTS and not f.name.endswith(".part")
                    for f in item.iterdir()
                )
                if not has_video:
                    shutil.rmtree(item, ignore_errors=True)
                    cleaned += 1
    except Exception as e:
        logger.debug(f"Помилка очищення осиротілих папок: {e}")
    return cleaned


def delete_video_file_and_associated(file_path: Path):
    """
    Видаляє відеофайл, пов'язані файли прев'ю (<stem>.thumb.jpg, кеш),
    а також повністю видаляє папку тайтлу (з poster.jpg), якщо в ній немає інших серій.
    """
    if not file_path.exists():
        return

    parent_dir = file_path.parent
    stem = file_path.stem

    # 1. Видаляємо відео
    file_path.unlink(missing_ok=True)

    # 2. Видаляємо специфічне прев'ю серії
    ep_thumb = file_path.with_name(f"{stem}.thumb.jpg")
    if ep_thumb.exists():
        ep_thumb.unlink(missing_ok=True)

    # 3. Видаляємо з кешу
    cache_thumb = THUMBNAILS_CACHE_DIR / f"{stem}.jpg"
    if cache_thumb.exists():
        cache_thumb.unlink(missing_ok=True)

    # 4. Якщо в папці більше немає інших серій — видаляємо всю папку разом із постером!
    if parent_dir.exists() and parent_dir != DOWNLOADS_DIR:
        cleanup_empty_folder(parent_dir)


def cleanup_cancelled_download(dest_file: Path, target_folder: Path):
    """Очищує тимчасові файли .part та пусті папки після скасування."""
    part_path = dest_file.with_name(f"{dest_file.name}.part")
    if part_path.exists():
        part_path.unlink(missing_ok=True)
    if dest_file.exists():
        delete_video_file_and_associated(dest_file)
    cleanup_empty_folder(target_folder)


def extract_links_and_title(message: Message) -> Tuple[List[Tuple[str, str]], str]:
    """Витягує посилання та назву тайтлу з посту."""
    text = message.text or message.caption or ""
    entities = message.entities or message.caption_entities or []

    links: List[Tuple[str, str]] = []
    seen_urls = set()

    for ent in entities:
        if ent.type == "text_link" and ent.url:
            url = ent.url
            if url not in seen_urls:
                anchor_text = text[ent.offset : ent.offset + ent.length].strip()
                links.append((anchor_text or "Посилання", url))
                seen_urls.add(url)
        elif ent.type == "url":
            url = text[ent.offset : ent.offset + ent.length].strip()
            if url not in seen_urls:
                links.append(("Пряме посилання", url))
                seen_urls.add(url)

    raw_urls = re.findall(r"https?://[^\s]+", text)
    for u in raw_urls:
        if u not in seen_urls:
            links.append(("Посилання", u))
            seen_urls.add(u)

    title = ""
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if lines:
        first_line = lines[0]
        # Прибираємо технічні префікси типу [РГ], [РГ SUB], емодзі на початку, а також технічні теги [04 з XX], [WEBRip...]
        first_line = re.sub(r"^(?:\[[^\]]*\]|[^\w\s])+\s*", "", first_line).strip()
        title_parts = first_line.split("/")
        raw_t = title_parts[0].strip() if title_parts else first_line
        clean_t = re.sub(r"\[.*?\]|\(\d+\s*сезон\)", "", raw_t, flags=re.IGNORECASE).strip()
        clean_t = re.sub(r"\s+", " ", clean_t).strip()
        title = clean_t if clean_t else raw_t

    return links, title


IGNORED_DOMAINS = (
    "monobank.ua", "privat24", "patreon.com", "buymeacoffee.com",
    "donatello.to", "diaka.ua", "instagram.com", "twitter.com", "x.com"
)


def score_link(label: str, url: str) -> int:
    """Оцінює пріоритет посилання: якість 4K/1080p, прямі файли, FEX мають найвищий бал."""
    label_low = label.lower()
    url_low = url.lower()
    score = 0

    # Найвищий пріоритет: 4K / 2160p (включаючи українську/кириличну 'К')
    if any(k in label_low for k in ("4k", "4к", "2160p", "uhd")):
        score += 250
    # Високий пріоритет: 1080p / FHD
    if any(k in label_low for k in ("1080p", "1080", "fhd")):
        score += 180
    # Пріоритет файлообмінників (FEX.NET)
    if "fex.net" in url_low or FexDownloader.extract_share_key(url):
        score += 200
    # Прямі завантаження
    if any(k in label_low for k in ("скачати", "завантажити", "download", "диск", "файл")):
        score += 100
    # Google Drive / Cloud
    if "drive.google.com" in url_low or "mega.nz" in url_low:
        score += 120
    # Публічні медіаканали Telegram
    if "t.me/" in url_low:
        score += 50
    # Зниження пріоритету для онлайн-плеєрів та низької якості
    if any(k in label_low for k in ("онлайн", "online", "плеєр", "player", "веб", "дивитися", "перегляд")):
        score -= 80
    if any(k in label_low for k in ("720p", "720", "sd", "480p", "480")):
        score -= 60

    return score


def score_and_sort_links(links: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """Фільтрує донати й сортує посилання за пріоритетом якості (4K/1080p > онлайн)."""
    valid = [
        (lbl, u) for lbl, u in links
        if not any(d in u.lower() for d in IGNORED_DOMAINS)
    ]
    return sorted(valid, key=lambda item: score_link(item[0], item[1]), reverse=True)


async def resolve_redirect_url(url: str) -> str:
    """Розгортає редиректи та скорочені посилання для виявлення FEX / direct URL."""
    if not url.startswith("http"):
        return url
    if any(d in url.lower() for d in ("fex.net", "t.me")):
        return url
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4)) as session:
            async with session.head(url, allow_redirects=True) as resp:
                if resp.status < 400 and str(resp.url) != url:
                    return str(resp.url)
    except Exception:
        pass
    return url


def is_user_allowed(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return user_id in ALLOWED_USERS


def get_main_reply_keyboard() -> ReplyKeyboardMarkup:
    """Створює зручне та мінімалістичне меню внизу екрана за ТЗ v3.0."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📁 Мої аніме"),
                KeyboardButton(text="🛰 Моніторинг"),
            ],
            [
                KeyboardButton(text="🖥 Стан ноута"),
                KeyboardButton(text="🔄 Рестарт бота"),
            ]
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


@dp.message(CommandStart())
@dp.message(Command("help"))
async def cmd_start(message: Message):
    if not is_user_allowed(message.from_user.id):
        await message.reply("⛔️ Вибачте, у вас немає доступу до цього бота.")
        return

    mac_state = get_mac_device_state()
    if mac_state:
        free_gb = mac_state.get("free_disk_gb", 0.0)
        folder_str = mac_state.get("downloads_folder", str(DOWNLOADS_DIR))
    else:
        free_gb = get_free_disk_space_gb()
        folder_str = str(DOWNLOADS_DIR)

    text = (
        "👋 <b>Привіт! Я твій локальний аніме-менеджер.</b>\n\n"
        "💻 Я завантажую та керую твоїми серіями на Mac:\n"
        f"📂 <b>Папка на Mac:</b> <code>{folder_str}</code>\n"
        f"🏷 <b>Тег macOS Finder:</b> 🟢 <i>{TAG_NAME}</i>\n"
        f"💾 <b>Вільно на Mac:</b> <code>{free_gb:.1f} ГБ</code>\n\n"
        "✨ <b>Можливості:</b>\n"
        "• <b>Завантаження:</b> просто перешли пост або лінк — бот стягне 4K/1080p з живим прогрес-баром.\n"
        "• <b>Очищення:</b> під кожною серією є кнопка <i>«🗑 Видалити з ноута»</i>.\n"
        "• <b>Кнопки меню:</b> знизу додано швидке меню для керування диском в один клік!\n\n"
        f"🆔 <i>Твій Telegram ID:</i> <code>{message.from_user.id}</code>"
    )
    await message.reply(text, reply_markup=get_main_reply_keyboard())


@dp.message(Command("version"))
@dp.message(Command("changelog"))
async def cmd_version(message: Message):
    if not is_user_allowed(message.from_user.id):
        return

    text = (
        f"🚀 <b>Версія Anime Downloader:</b> <code>v{CURRENT_VERSION}</code>\n"
        "⚡️ <i>Система безшовного авто-оновлення (Hot-Reload) активна.</i>\n\n"
        "✨ <b>Ключові новинки останнього оновлення:</b>\n"
        "• 🛰 <b>Картки тайтлів:</b> вибір Озвучення / Саби / Будь-що в 1 клік.\n"
        "• 📁 <b>Каталог сезону:</b> вітрина актуальних онґоїнгів з @ridnyiholos.\n"
        "• 📥 <b>Пакетне скачування:</b> завантаження всіх серій тайтлу в 4K.\n"
        "• 🔕 <b>Тихий режим:</b> фонове завантаження без звуку в робочий час.\n"
        "• 📦 <b>Auto-Binge:</b> авто-очищення старих серій з диска SSD.\n"
        "• 🔄 <b>Zero-Touch Оновлення:</b> авто-перезапуск без ручних команд."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛰 Відкрити моніторинг", callback_data="track_refresh")],
            [InlineKeyboardButton(text="📁 Мої аніме", callback_data="refresh_list")]
        ]
    )
    await message.reply(text, reply_markup=kb)


@dp.message(Command("status"))
@dp.message(F.text == "🖥 Стан ноута")
async def cmd_status(message: Message):
    if not is_user_allowed(message.from_user.id):
        return

    # Перевіряємо стан Mac-агента
    mac_state = get_mac_device_state()
    if mac_state:
        status_dot = "🟢" if mac_state.get("is_online") else "💤"
        status_text = "Активний (Підключено)" if mac_state.get("is_online") else "Спить / Офлайн (Черга активна)"
        device_name = mac_state.get("device_name", "MacBook Pro")
        free_gb = mac_state.get("free_disk_gb", 0.0)
        uptime_str = mac_state.get("uptime_str", "—")
        folder_str = mac_state.get("downloads_folder", str(DOWNLOADS_DIR))
    else:
        status_dot = "🟡"
        status_text = "Очікування першого підключення Mac"
        device_name = "MacBook"
        free_gb = get_free_disk_space_gb()
        uptime_sec = int(time.time() - START_TIME)
        uptime_str = f"{uptime_sec // 3600}год {(uptime_sec % 3600) // 60}хв {uptime_sec % 60}с"
        folder_str = str(DOWNLOADS_DIR)

    git_sha, git_msg = get_current_git_info()
    await message.reply(
        f"🖥 <b>Стан вашого Mac ({device_name}):</b>\n\n"
        f"{status_dot} <b>Зв'язок:</b> {status_text}\n"
        f"⏱ <b>Uptime:</b> {uptime_str}\n"
        f"💾 <b>Вільне місце на SSD:</b> <code>{free_gb:.1f} ГБ</code>\n"
        f"📂 <b>Локальна папка:</b> <code>{folder_str}</code>\n"
        f"🏷 <b>Тег Finder:</b> 🟢 {TAG_NAME}\n"
        f"📦 <b>Версія бота:</b> <code>v{CURRENT_VERSION} (#{git_sha})</code>",
        reply_markup=get_main_reply_keyboard(),
    )


@dp.message(Command("restart"))
@dp.message(Command("update"))
@dp.message(F.text == "🔄 Рестарт бота")
async def cmd_restart_bot(message: Message):
    """Миттєво оновлює код із GitHub (якщо є свіжі коміти), перезапускає бота та MacAgent і підтверджує отримання найновішого оновлення."""
    if not is_user_allowed(message.from_user.id):
        return

    status_msg = await message.reply(
        "🔄 <b>Перезапуск та перевірка оновлень бота...</b>\n"
        "⏳ <i>Синхронізую найновішу версію з GitHub та перезапускаю сервіси...</i>",
        reply_markup=get_main_reply_keyboard(),
    )
    queue_mac_command("restart_agent")
    updated, sha, commit_msg = await pull_latest_from_github()
    save_manual_restart_request(message.chat.id, status_msg.message_id)
    await asyncio.sleep(0.5)
    os.execv(sys.executable, [sys.executable] + sys.argv)


@dp.message(F.text == "ℹ️ Допомога")
async def btn_help(message: Message):
    await cmd_start(message)


@dp.message(F.text == "📁 Мої аніме")
async def btn_my_anime(message: Message):
    await cmd_cleanup_menu(message)


@dp.message(F.text == "🧹 Очистити все")
async def btn_clean_all(message: Message):
    if not is_user_allowed(message.from_user.id):
        return
    video_files = get_all_downloaded_videos()
    if not video_files:
        await message.reply("📂 У папці <code>(Завантажене аніме)</code> наразі немає файлів. Усе чисто!", reply_markup=get_main_reply_keyboard())
        return

    total_size = sum(f.stat().st_size for f in video_files)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔴 ТАК, ВИДАЛИТИ ВСІ СЕРІЇ", callback_data="del_all_confirm"),
                InlineKeyboardButton(text="↩️ Скасувати", callback_data="refresh_list"),
            ]
        ]
    )
    await message.reply(
        f"⚠️ <b>Ви впевнені, що хочете видалити ВСІ {len(video_files)} серій?</b>\n"
        f"📦 Це звільнить <code>{format_bytes(total_size)}</code> на диску Mac.",
        reply_markup=kb,
    )


def detect_pref_from_text(text: str) -> Optional[str]:
    """Визначає тип релізу (sub / dub) із назви гілки чи тексту (наприклад '[РГ SUB] Ао Аші')."""
    if not text:
        return None
    t_up = text.upper()
    has_sub = bool(re.search(r"\bSUB\b|СУБТИТР|САБ", t_up))
    has_dub = bool(re.search(r"\bMVO\b|\bDUB\b|ОЗВУЧ", t_up))
    if has_sub and not has_dub:
        return "sub"
    if has_dub and not has_sub:
        return "dub"
    if "[РГ]" in t_up and "SUB" not in t_up:
        return "dub"
    return None


def build_track_main_menu(chat_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Мінімалістичний дашборд моніторингу (v3.0): компактний список тайтлів + відписка в 1 клік."""
    # Одноразова заливка та актуалізація стартового Seed List
    try:
        from seed_data import seed_subscriptions
        if not get_user_settings(chat_id).get("seeded"):
            seed_subscriptions(chat_id)
            set_user_setting(chat_id, "seeded", 1)
    except Exception as e_seed:
        logger.warning(f"Seed List не застосовано: {e_seed}")

    subs = get_subscriptions(chat_id)
    kb_rows = []

    if not subs:
        text = (
            "🛰 <b>Моніторинг порожній</b>\n\n"
            "Щоб додати аніме — перешліть (Forward) боту пост із каналу "
            "@ridnyiholos, і я запропоную стежити за ним."
        )
    else:
        text = f"🛰 <b>Ваші активні тайтли ({len(subs)}):</b>\n\n"
        for idx, s in enumerate(subs, 1):
            if s.get("fex_url"):
                src = "4K FEX"
            elif s.get("thread_url"):
                src = "Медіатека"
            else:
                src = "канал"
            pref = s.get("pref_type", "dub")
            if pref == "sub" or "ао аші" in s["title"].lower() or detect_pref_from_text(s["title"]) == "sub":
                pref_label = "📝 Тільки субтитри [SUB]"
            elif pref == "dub":
                pref_label = "🎙 Тільки озвучення [DUB]"
            else:
                pref_label = "🎙+📝 Озвучення + Субтитри"
            text += f"<b>{idx}.</b> {s['title']}\n      └ <i>{pref_label} • {src}</i>\n\n"
            t_key = register_track_title(s["title"])
            short_t = format_anime_display_name(s["title"], max_len=16)
            kb_rows.append([
                InlineKeyboardButton(
                    text=left_align_button_text(f"{idx}. {short_t}", target_width=18.5),
                    callback_data=f"track_card:{t_key}"
                ),
                InlineKeyboardButton(text="❌ Відписатися", callback_data=f"untrack:{s['id']}"),
            ])

    kb_rows.append([
        InlineKeyboardButton(text="⚙️ Налаштування", callback_data="track_settings"),
        InlineKeyboardButton(text="🔄 Оновити", callback_data="track_refresh"),
    ])

    text += "💡 <i>Додати нове: перешліть пост із каналу або гілку медіатеки — бот автоматично розпізнає озвучення/субтитри.</i>"
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def build_anime_card(chat_id: int, title: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Будує інтерактивну картку аніме з чітким розділенням FEX.NET vs Медіатека та вибраних доріжок (DUB / SUB / ОБИДВА)."""
    clean_title = title.strip()
    sub = get_subscription_by_title(chat_id, clean_title)
    current_pref = sub["pref_type"] if sub else "dub"
    t_key = register_track_title(clean_title)

    # Перевіримо наявність серій у сезонному каталозі
    catalog = await fetch_ongoings_catalog("ridnyiholos")
    cat_item = None
    norm_search = normalize_title_for_match(clean_title)
    for c_title, c_data in catalog.items():
        norm_c = normalize_title_for_match(c_title)
        if norm_search and norm_c and (norm_search in norm_c or norm_c in norm_search):
            cat_item = c_data
            break

    # Визначаємо джерело підписки: суто Медіатека чи FEX.NET (без змішування!)
    sub_has_fex = bool(sub and sub.get("fex_url"))
    sub_has_thread_only = bool(sub and sub.get("thread_url") and not sub.get("fex_url"))

    has_fex = sub_has_fex or (not sub_has_thread_only and bool(cat_item and cat_item.get("has_fex")))
    media_url = None
    if not has_fex:
        media_url = normalize_telegram_url(
            (sub.get("thread_url") if sub else None) or (cat_item.get("media_url") if cat_item else None)
        )

    ep_count_text = ""
    tracks_str = ""

    eps_dub = []
    eps_sub = []
    eps_fex = set()

    fex_sk = None
    if sub and sub.get("fex_url"):
        fex_sk = FexDownloader.extract_share_key(sub["fex_url"])
    elif has_fex and cat_item and cat_item.get("fex_share_key"):
        fex_sk = cat_item["fex_share_key"]

    if cat_item is None:
        cat_item = {"title": clean_title, "posts": [], "episodes": [], "episodes_dub": [], "episodes_sub": []}

    if has_fex and fex_sk and not cat_item.get("enriched_fex"):
        try:
            files = await asyncio.wait_for(FexDownloader.get_files_in_share(fex_sk), timeout=5.0)
            found_fex_eps = set()
            found_fex_dub = set()
            found_fex_sub = set()
            tracks = set()
            for f in files:
                fname = f.get("name", "")
                m = re.search(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]", fname)
                if m:
                    ep_val = int(m.group(1))
                    found_fex_eps.add(ep_val)
                    has_mvo = any(w in fname for w in ["MVO", "Озвуч", "DUB", "dub"])
                    has_sub = any(w in fname for w in ["SUB", "Саб", "sub"])
                    if "Dual" in fname or (has_mvo and has_sub) or ("[UKR MVO" in fname and "UKR SUB" in fname):
                        found_fex_dub.add(ep_val)
                        found_fex_sub.add(ep_val)
                        tracks.add("Озвучення")
                        tracks.add("Субтитри")
                    elif has_mvo:
                        found_fex_dub.add(ep_val)
                        tracks.add("Озвучення")
                    elif has_sub:
                        found_fex_sub.add(ep_val)
                        tracks.add("Субтитри")
                    else:
                        found_fex_dub.add(ep_val)
                        found_fex_sub.add(ep_val)

            if found_fex_eps:
                cat_item["has_fex"] = True
                cat_item["fex_share_key"] = fex_sk
                cat_item["fex_episodes"] = sorted(list(found_fex_eps))
                cat_item["episodes_dub"] = sorted(list(set(cat_item.get("episodes_dub", [])) | found_fex_dub))
                cat_item["episodes_sub"] = sorted(list(set(cat_item.get("episodes_sub", [])) | found_fex_sub))
                cat_item["episodes"] = sorted(list(set(cat_item.get("episodes", [])) | found_fex_eps))
            if tracks:
                cat_item["tracks"] = sorted(list(tracks))
            cat_item["enriched_fex"] = True
        except Exception as ex_en:
            logger.debug(f"Помилка збагачення FEX картки: {ex_en}")

    episodes_list = cat_item.get("episodes", [])
    eps_dub = cat_item.get("episodes_dub", [])
    eps_sub = cat_item.get("episodes_sub", [])
    eps_fex = set(cat_item.get("fex_episodes", []))

    # Визначаємо, чи це аніме виходить ТІЛЬКИ із субтитрами (без озвучення взагалі)
    explicit_pref = detect_pref_from_text((sub["title"] if sub else "") + " " + clean_title)
    is_sub_only = (
        explicit_pref == "sub"
        or ("ао аші" in clean_title.lower())
        or (not has_fex and not eps_dub and (bool(eps_sub) or current_pref == "sub"))
    )
    if is_sub_only and current_pref != "sub":
        current_pref = "sub"
        if sub:
            add_subscription(chat_id, sub["title"], channel="ridnyiholos", pref_type="sub")

    # Формуємо рядки серій та доріжок ВІДПОВІДНО до обраного режиму (Тільки озвучення / Тільки субтитри / Обидва)
    ep_lines = []
    if is_sub_only or current_pref == "sub":
        tracks_str = "🔊 <b>Формат доріжок:</b> 📝 Українські субтитри (SUB)\n"
        target_sub_eps = [e for e in eps_sub if e in eps_fex] if (has_fex and eps_fex) else (eps_sub or episodes_list)
        if target_sub_eps:
            sub_latest = max(target_sub_eps)
            sub_src = "4K FEX" if has_fex else "Онлайн-медіатека"
            ep_lines.append(f"📝 <b>Субтитри (SUB):</b> {', '.join(f'#{e}' for e in target_sub_eps)} <i>(найновіша: #{sub_latest} [{sub_src}])</i>")
    elif current_pref == "dub":
        tracks_str = "🔊 <b>Формат доріжок:</b> 🎙 Українське озвучення (DUB)\n"
        target_dub_eps = [e for e in eps_dub if e in eps_fex] if (has_fex and eps_fex) else (eps_dub or episodes_list)
        if target_dub_eps:
            dub_latest = max(target_dub_eps)
            dub_src = "4K FEX" if has_fex else "Онлайн-медіатека"
            ep_lines.append(f"🎙 <b>Озвучення (DUB):</b> {', '.join(f'#{e}' for e in target_dub_eps)} <i>(найновіша: #{dub_latest} [{dub_src}])</i>")
    else:
        # current_pref == "any": показуємо ОБИДВА варіанти (і озвучення, і субтитри)
        tracks_str = "🔊 <b>Формат доріжок:</b> 🎙 Озвучення + 📝 Субтитри (обидва варіанти)\n"
        target_dub_eps = [e for e in eps_dub if e in eps_fex] if (has_fex and eps_fex) else eps_dub
        target_sub_eps = [e for e in eps_sub if e in eps_fex] if (has_fex and eps_fex) else eps_sub
        if target_dub_eps:
            dub_latest = max(target_dub_eps)
            dub_src = "4K FEX" if has_fex else "Онлайн-медіатека"
            ep_lines.append(f"🎙 <b>Озвучення (DUB):</b> {', '.join(f'#{e}' for e in target_dub_eps)} <i>(найновіша: #{dub_latest} [{dub_src}])</i>")
        if target_sub_eps:
            sub_latest = max(target_sub_eps)
            sub_src = "4K FEX" if has_fex else "Онлайн-медіатека"
            ep_lines.append(f"📝 <b>Субтитри (SUB):</b> {', '.join(f'#{e}' for e in target_sub_eps)} <i>(найновіша: #{sub_latest} [{sub_src}])</i>")

    if ep_lines:
        ep_count_text = "\n".join(ep_lines) + "\n"

    # Режим завантаження / доступності (без змішування FEX і Медіатеки!)
    if has_fex:
        delivery_badge = (
            "🟢 <b>Джерело:</b> ⚡️ <i>FEX.NET (4K / 1080p пряме завантаження на Mac)</i>"
        )
    elif is_sub_only:
        delivery_badge = (
            "🟡 <b>Джерело:</b> 📲 <i>Онлайн-медіатека Telegram (@mediareser)</i>\n"
            "   └ 📝 <i>Виходить виключно із субтитрами [РГ SUB]</i>"
        )
    else:
        delivery_badge = (
            "🟡 <b>Джерело:</b> 📲 <i>Онлайн-медіатека Telegram (@mediareser)</i>"
        )

    # Статус поточної підписки
    if is_sub_only:
        status_badge = "🔔 <b>Підписка:</b> 📝 <i>Тільки субтитри [SUB]</i>"
    elif current_pref == "dub":
        status_badge = "🔔 <b>Підписка:</b> 🎙 <i>Тільки озвучення (DUB)</i>"
    elif current_pref == "sub":
        status_badge = "🔔 <b>Підписка:</b> 📝 <i>Тільки субтитри (SUB)</i>"
    elif current_pref == "any":
        status_badge = "🔔 <b>Підписка:</b> 🎙+📝 <i>Озвучення та Субтитри (обидва варіанти)</i>"
    else:
        status_badge = "⚪️ <b>Статус:</b> <i>Не підписані на сповіщення</i>"

    display_card_title = f"[РГ SUB] {clean_title}" if (is_sub_only and "SUB" not in clean_title.upper()) else clean_title
    parts = [
        f"🎬 <b>Картка аніме: «{display_card_title}»</b>",
        f"📡 <b>Канал релізів:</b> @ridnyiholos",
        delivery_badge.strip(),
    ]
    if tracks_str.strip():
        parts.append(tracks_str.strip())
    if ep_count_text.strip():
        parts.append(ep_count_text.strip())
    parts.append(status_badge)
    if has_fex or not is_sub_only:
        parts.append("👇 <b>Оберіть режим підписки або серію для скачування:</b>")

    text = "\n\n".join(parts)

    kb_rows = []
    if not is_sub_only:
        kb_rows.extend([
            [
                InlineKeyboardButton(
                    text=f"{'🔘' if current_pref == 'dub' else '⚪️'} 🎙 Тільки озвучення",
                    callback_data=f"set_sub:{t_key}:dub"
                ),
                InlineKeyboardButton(
                    text=f"{'🔘' if current_pref == 'sub' else '⚪️'} 📝 Тільки субтитри",
                    callback_data=f"set_sub:{t_key}:sub"
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"{'🔘' if current_pref == 'any' else '⚪️'} 🎙+📝 Озвучення + Субтитри (обидва)",
                    callback_data=f"set_sub:{t_key}:any"
                ),
            ],
        ])

    # Кнопки скачування серій (тільки якщо тайтл має FEX.NET роздачу!)
    if has_fex:
        fex_dub_eps = sorted([e for e in eps_dub if e in eps_fex] or eps_dub or list(eps_fex))
        fex_sub_eps = sorted([e for e in eps_sub if e in eps_fex] or eps_sub or list(eps_fex))

        if current_pref == "dub":
            if fex_dub_eps:
                kb_rows.append([
                    InlineKeyboardButton(
                        text=f"📥 Завантажити всі з озвученням ({len(fex_dub_eps)} серій у 4K)",
                        callback_data=f"track_batch_ask:{t_key}:dub"
                    )
                ])
                ep_buttons = [
                    InlineKeyboardButton(text=f"📥 Еп.{ep} 🎙", callback_data=f"track_ep_dl:{t_key}:{ep}:dub")
                    for ep in fex_dub_eps
                ]
                for i in range(0, len(ep_buttons), 4):
                    kb_rows.append(ep_buttons[i : i + 4])

        elif current_pref == "sub":
            if fex_sub_eps:
                kb_rows.append([
                    InlineKeyboardButton(
                        text=f"📥 Завантажити всі із субтитрами ({len(fex_sub_eps)} серій)",
                        callback_data=f"track_batch_ask:{t_key}:sub"
                    )
                ])
                ep_buttons = [
                    InlineKeyboardButton(text=f"📥 Еп.{ep} 📝", callback_data=f"track_ep_dl:{t_key}:{ep}:sub")
                    for ep in fex_sub_eps
                ]
                for i in range(0, len(ep_buttons), 4):
                    kb_rows.append(ep_buttons[i : i + 4])

        else:
            # current_pref == "any": даємо можливість скачати І озвучення, І субтитри!
            batch_row = []
            if fex_dub_eps:
                batch_row.append(
                    InlineKeyboardButton(
                        text=f"📥 Всі 🎙 Озвучення ({len(fex_dub_eps)})",
                        callback_data=f"track_batch_ask:{t_key}:dub"
                    )
                )
            if fex_sub_eps:
                batch_row.append(
                    InlineKeyboardButton(
                        text=f"📥 Всі 📝 Субтитри ({len(fex_sub_eps)})",
                        callback_data=f"track_batch_ask:{t_key}:sub"
                    )
                )
            if batch_row:
                kb_rows.append(batch_row)

            if fex_dub_eps:
                dub_btns = [
                    InlineKeyboardButton(text=f"📥 Еп.{ep} 🎙", callback_data=f"track_ep_dl:{t_key}:{ep}:dub")
                    for ep in fex_dub_eps
                ]
                for i in range(0, len(dub_btns), 4):
                    kb_rows.append(dub_btns[i : i + 4])

            if fex_sub_eps:
                sub_btns = [
                    InlineKeyboardButton(text=f"📥 Еп.{ep} 📝", callback_data=f"track_ep_dl:{t_key}:{ep}:sub")
                    for ep in fex_sub_eps
                ]
                for i in range(0, len(sub_btns), 4):
                    kb_rows.append(sub_btns[i : i + 4])

    # Кнопка Медіатеки показується ТІЛЬКИ якщо це тайтл з Медіатеки (БЕЗ FEX.NET!)
    if not has_fex and media_url:
        kb_rows.append([
            InlineKeyboardButton(text="↗️ Відкрити тему в Медіатеці", url=media_url)
        ])

    if sub:
        kb_rows.append([
            InlineKeyboardButton(text="🗑 Зупинити моніторинг", callback_data=f"untrack_title:{t_key}")
        ])

    kb_rows.append([
        InlineKeyboardButton(text="↩️ Назад до моніторингу", callback_data="track_refresh")
    ])

    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def render_catalog_page(chat_id: int, page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    """Відображає вітрину сезонних онґоїнгів з розділенням: 4K Автомат vs Сповіщення."""
    catalog = await fetch_ongoings_catalog("ridnyiholos")
    items = list(catalog.values())
    total = len(items)
    page_size = 6
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))

    start = page * page_size
    page_items = items[start : start + page_size]

    text = (
        f"📁 <b>Жива вітрина онґоїнгів сезону (@ridnyiholos)</b>\n"
        f"<i>Знайдено тайтлів: {total} | Сторінка {page + 1}/{total_pages}</i>\n\n"
        "🟢 <b>[⚡️]</b> — Авто-завантаження на Mac (4K / 1080p)\n"
        "🟡 <b>[📲]</b> — Онлайн медіатека (сповіщення + Forward)\n\n"
        "Натисніть на аніме для перегляду серій або налаштування:"
    )

    kb_rows = []
    for it in page_items:
        t_name = it["title"]
        t_key = register_track_title(t_name)
        has_fex = it.get("has_fex", False)
        type_icon = "⚡️" if has_fex else "📲"
        eps_info = f" ({len(it['episodes'])} сер.)" if it.get("episodes") else ""
        sub = get_subscription_by_title(chat_id, t_name)
        sub_mark = "🔔 " if sub else ""
        kb_rows.append([
            InlineKeyboardButton(text=f"{sub_mark}{type_icon} {t_name[:32]}{eps_info}", callback_data=f"track_card:{t_key}")
        ])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Попередня", callback_data=f"track_catalog:{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Наступна ➡️", callback_data=f"track_catalog:{page + 1}"))

    if nav_row:
        kb_rows.append(nav_row)

    kb_rows.append([
        InlineKeyboardButton(text="↩️ До моніторингу", callback_data="track_refresh")
    ])

    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


def build_settings_menu(chat_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Створює меню налаштувань (тихий робочий час, ліміт серій SSD)."""
    settings = get_user_settings(chat_id)
    quiet_on = bool(settings.get("quiet_mode", 0))
    binge_limit = settings.get("auto_binge_limit", 2)

    quiet_status = "✅ Увімкнено (09:00 - 18:30)" if quiet_on else "⚪️ Вимкнено (сповіщати відразу)"
    limit_status = f"{binge_limit} серії на тайтл" if binge_limit > 0 else "Без обмежень (зберігати все)"

    text = (
        "⚙️ <b>Налаштування авто-завантажень та диску Mac</b>\n\n"
        f"🔕 <b>Тихий режим у робочий час:</b>\n"
        f"   └ <i>{quiet_status}</i>\n"
        "   <i>Бот завантажує релізи тихо без звуку з 09:00 до 18:30, а о 18:30 надсилає одне красиве вечірнє резюме.</i>\n\n"
        f"📦 <b>Auto-Binge ліміт на Mac SSD:</b>\n"
        f"   └ <i>{limit_status}</i>\n"
        "   <i>Автоматично зберігає на ноуті тільки останні свіжі серії кожного аніме, самостійно очищуючи вже переглянуті.</i>"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{'🔕 Вимкнути тихий режим' if quiet_on else '🔔 Увімкнути тихий режим (09:00-18:30)'}",
                    callback_data="toggle_quiet_mode"
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"📦 SSD Ліміт: {limit_status}",
                    callback_data="cycle_binge_limit"
                )
            ],
            [
                InlineKeyboardButton(text="↩️ До моніторингу", callback_data="track_refresh")
            ]
        ]
    )
    return text, kb


@dp.message(F.text.in_(["🛰 Моніторинг", "📡 Моніторинг", "Моніторинг"]))
@dp.message(F.text.startswith("🛰") & F.text.contains("Моніторинг"))
@dp.message(F.text.startswith("📡") & F.text.contains("Моніторинг"))
@dp.message(Command("track"))
async def cmd_track(message: Message):
    """Керування автоматичним моніторингом виходу нових серій у каналах."""
    if not is_user_allowed(message.from_user.id):
        return

    raw_text = message.text or ""
    parts = raw_text.split(maxsplit=2)

    if raw_text.startswith("/track") and len(parts) >= 2:
        title_to_track = parts[1]
        pref = "any"
        if len(parts) >= 3:
            p_arg = parts[2].lower()
            if any(k in p_arg for k in ("озвуч", "dub", "голос", "mvo")):
                pref = "dub"
            elif any(k in p_arg for k in ("суб", "sub")):
                pref = "sub"
            else:
                title_to_track = f"{parts[1]} {parts[2]}"

        add_subscription(message.chat.id, title_to_track, channel="ridnyiholos", pref_type=pref)
        pref_label = "🎙 Тільки озвучення" if pref == "dub" else ("📝 Тільки субтитри" if pref == "sub" else "⚡️ Будь-який реліз")
        await message.reply(
            f"✅ <b>Тайтл додано на авто-моніторинг!</b>\n\n"
            f"🎬 <b>Назва:</b> <code>{title_to_track}</code>\n"
            f"📡 <b>Канал:</b> @ridnyiholos (Рідний Голос)\n"
            f"🎯 <b>Фільтр:</b> {pref_label}\n\n"
            f"🤖 <i>Щойно в каналі «Рідний Голос» з'явиться нова серія — бот автоматично скачає її на Mac у 4K/1080p і надішле сповіщення!</i>",
            reply_markup=get_main_reply_keyboard()
        )
        return

    text, kb = build_track_main_menu(message.chat.id)
    await message.reply(text, reply_markup=kb)


@dp.callback_query(F.data == "track_refresh")
async def cb_track_refresh(callback: CallbackQuery):
    text, kb = build_track_main_menu(callback.message.chat.id)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)
    await safe_answer(callback, "🔄 Список оновлено")


@dp.callback_query(F.data.startswith("track_card:"))
async def cb_track_card(callback: CallbackQuery):
    t_key = callback.data.split(":")[1]
    title = resolve_track_title(t_key)
    if not title:
        await safe_answer(callback, "⚠️ Тайтл не знайдено в кеші", show_alert=True)
        return
    text, kb = await build_anime_card(callback.message.chat.id, title)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)
    await safe_answer(callback)


@dp.callback_query(F.data.startswith("set_sub:"))
async def cb_set_sub(callback: CallbackQuery):
    parts = callback.data.split(":")
    t_key = parts[1]
    pref = parts[2]
    title = resolve_track_title(t_key)
    if not title:
        await safe_answer(callback, "⚠️ Помилка ідентифікації тайтлу", show_alert=True)
        return

    add_subscription(callback.message.chat.id, title, channel="ridnyiholos", pref_type=pref)
    pref_label = "🎙 Тільки озвучення" if pref == "dub" else ("📝 Тільки субтитри" if pref == "sub" else "⚡️ Будь-який реліз")
    await safe_answer(callback, f"✅ Підписку встановлено: {title}\n({pref_label})", show_alert=True)
    text, kb = await build_anime_card(callback.message.chat.id, title)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("untrack_title:"))
async def cb_untrack_title(callback: CallbackQuery):
    t_key = callback.data.split(":")[1]
    title = resolve_track_title(t_key)
    if title:
        delete_subscription_by_title(callback.message.chat.id, title)
        await safe_answer(callback, f"🗑 Моніторинг «{title}» вимкнено!", show_alert=True)
        text, kb = await build_anime_card(callback.message.chat.id, title)
        try:
            await callback.message.edit_text(text, reply_markup=kb)
        except Exception:
            pass
    else:
        await safe_answer(callback)


@dp.callback_query(F.data.startswith("untrack:"))
async def cb_untrack(callback: CallbackQuery):
    sub_id = int(callback.data.split(":")[1])
    delete_subscription(sub_id, callback.message.chat.id)
    await safe_answer(callback, "🗑 Підписку видалено!", show_alert=False)
    text, kb = build_track_main_menu(callback.message.chat.id)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("track_catalog:"))
async def cb_track_catalog(callback: CallbackQuery):
    page = int(callback.data.split(":")[1])
    text, kb = await render_catalog_page(callback.message.chat.id, page=page)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)
    await safe_answer(callback)


@dp.callback_query(F.data == "track_settings")
async def cb_track_settings(callback: CallbackQuery):
    text, kb = build_settings_menu(callback.message.chat.id)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)
    await safe_answer(callback)


@dp.callback_query(F.data == "toggle_quiet_mode")
async def cb_toggle_quiet_mode(callback: CallbackQuery):
    s = get_user_settings(callback.message.chat.id)
    new_val = 0 if s.get("quiet_mode", 0) else 1
    set_user_setting(callback.message.chat.id, "quiet_mode", new_val)
    status_str = "увімкнено (09:00 - 18:30)" if new_val else "вимкнено"
    await safe_answer(callback, f"🔕 Тихий режим {status_str}!", show_alert=True)
    text, kb = build_settings_menu(callback.message.chat.id)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass


@dp.callback_query(F.data == "cycle_binge_limit")
async def cb_cycle_binge_limit(callback: CallbackQuery):
    s = get_user_settings(callback.message.chat.id)
    cur = s.get("auto_binge_limit", 2)
    next_map = {2: 3, 3: 0, 0: 1, 1: 2}
    nxt = next_map.get(cur, 2)
    set_user_setting(callback.message.chat.id, "auto_binge_limit", nxt)
    lim_str = f"{nxt} серії" if nxt > 0 else "Без обмежень"
    await safe_answer(callback, f"📦 Auto-Binge ліміт змінено на: {lim_str}!", show_alert=True)
    text, kb = build_settings_menu(callback.message.chat.id)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("track_batch_ask:"))
async def cb_track_batch_ask(callback: CallbackQuery):
    parts = callback.data.split(":")
    t_key = parts[1]
    b_type = parts[2] if len(parts) > 2 else "any"
    title = resolve_track_title(t_key)
    if not title:
        await safe_answer(callback, "⚠️ Помилка ідентифікації тайтлу", show_alert=True)
        return

    chat_id = callback.message.chat.id
    sub = get_subscription_by_title(chat_id, title)

    catalog = await fetch_ongoings_catalog("ridnyiholos")
    cat_item = None
    for c_title, c_data in catalog.items():
        if title.lower() in c_title.lower() or c_title.lower() in title.lower():
            cat_item = c_data
            break

    if not cat_item:
        cat_item = {"title": title, "posts": [], "episodes": [], "episodes_dub": [], "episodes_sub": []}

    if sub and sub.get("fex_url") and not cat_item.get("fex_share_key"):
        sk_sub = FexDownloader.extract_share_key(sub["fex_url"])
        if sk_sub:
            cat_item["has_fex"] = True
            cat_item["fex_share_key"] = sk_sub

    # Збагачуємо FEX, якщо є роздача
    if cat_item.get("has_fex") and cat_item.get("fex_share_key") and not cat_item.get("enriched_fex"):
        try:
            sk = cat_item["fex_share_key"]
            files = await asyncio.wait_for(FexDownloader.get_files_in_share(sk), timeout=4.0)
            f_eps = set()
            for f in files:
                m = re.search(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]", f.get("name", ""))
                if m:
                    f_eps.add(int(m.group(1)))
            if f_eps:
                cat_item["fex_episodes"] = sorted(list(f_eps))
            cat_item["enriched_fex"] = True
        except Exception:
            pass

    fex_eps = cat_item.get("fex_episodes", cat_item.get("episodes", []))
    if not fex_eps:
        await safe_answer(callback, "⚠️ Не знайдено доступних серій на FEX.NET для цього тайтлу.", show_alert=True)
        return

    if b_type == "dub":
        target_eps = [e for e in cat_item.get("episodes_dub", []) if e in fex_eps]
        mode_label = "з озвученням (DUB)"
    elif b_type == "sub":
        target_eps = [e for e in cat_item.get("episodes_sub", []) if e in fex_eps]
        mode_label = "із субтитрами (SUB)"
    else:
        target_eps = fex_eps
        mode_label = "усі доступні"

    if not target_eps:
        target_eps = fex_eps

    ep_str = ", ".join(f"#{e}" for e in sorted(target_eps))
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🚀 ТАК, ЗАВАНТАЖИТИ ВСІ", callback_data=f"track_batch_go:{t_key}:{b_type}"),
                InlineKeyboardButton(text="↩️ Скасувати", callback_data=f"track_card:{t_key}"),
            ]
        ]
    )
    text = (
        f"📥 <b>Пакетне завантаження {mode_label} «{title}»</b>\n\n"
        f"🍿 <b>Буде завантажено {len(target_eps)} серій:</b> {ep_str}\n"
        "🎞 <b>Якість:</b> Авто-вибір 4K (2160p) або 1080p з роздачі FEX.NET\n"
        f"📂 <b>Папка на Mac:</b> <code>(Завантажене аніме)/{title}</code>\n\n"
        "Розпочати завантаження?"
    )
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)
    await safe_answer(callback)


@dp.callback_query(F.data.startswith("track_batch_go:"))
async def cb_track_batch_go(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split(":")
    t_key = parts[1]
    b_type = parts[2] if len(parts) > 2 else "any"
    title = resolve_track_title(t_key)
    if not title:
        await safe_answer(callback, "⚠️ Помилка ідентифікації тайтлу", show_alert=True)
        return

    chat_id = callback.message.chat.id
    sub = get_subscription_by_title(chat_id, title)

    catalog = await fetch_ongoings_catalog("ridnyiholos")
    cat_item = None
    for c_title, c_data in catalog.items():
        if title.lower() in c_title.lower() or c_title.lower() in title.lower():
            cat_item = c_data
            break

    if not cat_item:
        cat_item = {"title": title, "posts": [], "episodes": [], "episodes_dub": [], "episodes_sub": []}

    # Знаходимо FEX share key (пріоритет: збережене посилання підписки з ID підпапки!)
    share_key = None
    if sub and sub.get("fex_url"):
        share_key = FexDownloader.extract_share_key(sub["fex_url"])
    if not share_key:
        share_key = cat_item.get("fex_share_key")
    if not share_key:
        for p in cat_item.get("posts", []):
            for l_lbl, l_url in p.get("links", []):
                sk = FexDownloader.extract_share_key(l_url)
                if sk:
                    share_key = sk
                    break
            if share_key:
                break

    if not share_key:
        await safe_answer(callback, "⚠️ Посилання на FEX.NET не знайдено.", show_alert=True)
        return

    files = await FexDownloader.get_files_in_share(share_key)
    if not files:
        await safe_answer(callback, "⚠️ Роздача FEX.NET порожня або недоступна.", show_alert=True)
        return

    # Захист від загального архіву: якщо у роздачі > 25 файлів, фільтруємо за назвою тайтлу
    if len(files) > 25:
        norm_t = normalize_title_for_match(title)
        title_matched = [f for f in files if norm_t and norm_t[:15] in normalize_title_for_match(f.get("name", ""))]
        if title_matched:
            files = title_matched

    f_eps = set()
    for f in files:
        m = re.search(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]", f.get("name", ""))
        if m:
            f_eps.add(int(m.group(1)))
    fex_eps = sorted(list(f_eps)) if f_eps else cat_item.get("fex_episodes", cat_item.get("episodes", []))

    if b_type == "dub":
        ep_list = [e for e in cat_item.get("episodes_dub", []) if e in fex_eps]
    elif b_type == "sub":
        ep_list = [e for e in cat_item.get("episodes_sub", []) if e in fex_eps]
    else:
        ep_list = fex_eps

    if not ep_list:
        ep_list = fex_eps

    total_to_dl = len(ep_list)
    await safe_answer(callback, f"🚀 Розпочинаю завантаження {total_to_dl} серій!", show_alert=True)

    cancel_id = f"fex_batch_{t_key}"
    cancel_kb = build_cancel_keyboard(cancel_id)

    status_msg = await callback.message.answer(
        f"📥 <b>Початок пакетного завантаження «{title}»...</b>\n"
        f"Всього серій до завантаження: <b>{total_to_dl}</b>",
        reply_markup=cancel_kb
    )

    clean_t = sanitize_folder_name(title)
    t_folder = DOWNLOADS_DIR / clean_t
    t_folder.mkdir(parents=True, exist_ok=True)
    mac_synced_names = {mf.get("name") for mf in get_synced_mac_files()}

    async def _do_batch_download():
        success_count = 0
        queued_to_mac = 0
        already_on_mac = 0
        for idx, ep_num in enumerate(ep_list, 1):
            ep_patterns = [
                f"[{ep_num:02d} з", f"[{ep_num:02d} ", f"[{ep_num:02d}]",
                f"[{ep_num} з", f"[{ep_num} ", f"[{ep_num}]",
                f" {ep_num:02d} ", f" {ep_num} "
            ]
            ep_files = [f for f in files if any(p in f.get("name", "") for p in ep_patterns)]
            if not ep_files:
                continue

            if b_type == "sub":
                pref_sub = [f for f in ep_files if any(w in f.get("name", "") for w in ["SUB", "Саб", "sub"])]
                pool = pref_sub if pref_sub else ep_files
            elif b_type == "dub":
                pref_dub = [f for f in ep_files if any(w in f.get("name", "") for w in ["MVO", "Озвуч", "DUB", "dub"])]
                pool = pref_dub if pref_dub else ep_files
            else:
                pool = ep_files

            p4k = [f for f in pool if any(q in f.get("name", "").lower() for q in ["2160p", "4k"])]
            p1080 = [f for f in pool if "1080p" in f.get("name", "").lower()]
            target = p4k[0] if p4k else (p1080[0] if p1080 else pool[0])

            f_name = target.get("name", f"episode_{ep_num}.mp4")
            dest = t_folder / f_name
            dl_url = target.get("download_url")
            tok = target.get("token")

            if f_name in mac_synced_names or (dest.exists() and dest.stat().st_size > 10 * 1024 * 1024):
                success_count += 1
                already_on_mac += 1
                continue

            if dl_url:
                if platform.system() != "Darwin":
                    cmd_payload = json.dumps({
                        "url": dl_url,
                        "token": tok,
                        "title": title,
                        "episode": ep_num,
                        "folder": clean_t,
                        "filename": f_name,
                        "chat_id": chat_id,
                        "msg_id": None,
                        "quality": "4K Ultra HD" if "2160p" in f_name else "1080p Full HD",
                        "dub_sub": detect_tracks_label(f_name, title, chat_id)
                    })
                    queue_mac_command("download_fex", cmd_payload)
                    success_count += 1
                    queued_to_mac += 1
                    continue

                try:
                    await status_msg.edit_text(
                        f"📥 <b>Пакетне завантаження «{title}» [{idx}/{total_to_dl}]</b>\n"
                        f"🎬 Серія: #{ep_num} (<code>{f_name}</code>)\n"
                        f"⏳ Завантажую на Mac у 4K/1080p...",
                        reply_markup=cancel_kb
                    )
                except Exception:
                    pass
                await FexDownloader.download_file(dl_url, dest, token=tok)
                set_macos_tag(dest, TAG_NAME, TAG_COLOR)
                set_macos_tag(t_folder, TAG_NAME, TAG_COLOR)
                register_file(dest)
                success_count += 1
        return success_count, queued_to_mac, already_on_mac

    dl_task = asyncio.create_task(_do_batch_download())
    ACTIVE_DOWNLOAD_TASKS[cancel_id] = dl_task

    try:
        sc, queued_mac, already_mac = await dl_task
        if platform.system() != "Darwin":
            if queued_mac > 0:
                already_str = f"\n✅ <b>Вже є на Mac:</b> {already_mac} серій (пропущено)" if already_mac else ""
                await status_msg.edit_text(
                    f"⏳ <b>Пакетне завантаження передано на ваш Mac!</b>\n\n"
                    f"🎬 <b>Тайтл:</b> {title}\n"
                    f"📥 <b>У черзі на завантаження:</b> <b>{queued_mac}</b> серій у 4K / 1080p{already_str}\n"
                    f"📂 <b>Папка на Mac:</b> <code>(Завантажене аніме)/{clean_t}</code>\n\n"
                    "🍏 <i>Ваш Mac-агент зараз по черзі скачає всі серії прямо на SSD та надішле картку з прев'ю по кожній серії!</i>",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="🛰 До меню моніторингу", callback_data="track_refresh")]]
                    )
                )
            else:
                await status_msg.edit_text(
                    f"⚡️ <b>Усі {already_mac} серій «{title}» уже завантажені на вашому Mac!</b>\n\n"
                    f"📂 Папка: <code>(Завантажене аніме)/{clean_t}</code>",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="📁 Відкрити Мої аніме", callback_data="refresh_list")],
                            [InlineKeyboardButton(text="🛰 До меню моніторингу", callback_data="track_refresh")]
                        ]
                    )
                )
        else:
            await status_msg.edit_text(
                f"🎉 <b>Пакетне завантаження завершено!</b>\n\n"
                f"🎬 <b>Тайтл:</b> {title}\n"
                f"✅ Успішно завантажено: <b>{sc} з {total_to_dl}</b> серій\n"
                f"📂 Папка: <code>(Завантажене аніме)/{clean_t}</code>",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="🛰 До меню моніторингу", callback_data="track_refresh")]]
                )
            )
    except asyncio.CancelledError:
        logger.info(f"Пакетне завантаження {cancel_id} скасовано.")
        try:
            await status_msg.edit_text("🛑 Пакетне завантаження було скасовано.")
        except Exception:
            pass
    except Exception as e_b:
        logger.error(f"Помилка пакетного завантаження: {e_b}")
        try:
            await status_msg.edit_text(f"❌ Помилка пакетного завантаження: {e_b}")
        except Exception:
            pass
    finally:
        ACTIVE_DOWNLOAD_TASKS.pop(cancel_id, None)


@dp.callback_query(F.data.startswith("track_ep_dl:"))
async def cb_track_ep_dl(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split(":")
    t_key = parts[1]
    ep_num = int(parts[2])
    req_pref = parts[3] if len(parts) > 3 else None
    title = resolve_track_title(t_key)
    if not title:
        await safe_answer(callback, "⚠️ Помилка ідентифікації тайтлу", show_alert=True)
        return

    chat_id = callback.message.chat.id
    sub = get_subscription_by_title(chat_id, title)
    pref = req_pref or (sub["pref_type"] if sub else "dub")

    catalog = await fetch_ongoings_catalog("ridnyiholos")
    cat_item = None
    norm_search = normalize_title_for_match(title)
    for c_title, c_data in catalog.items():
        norm_c = normalize_title_for_match(c_title)
        if norm_search and norm_c and (norm_search in norm_c or norm_c in norm_search):
            cat_item = c_data
            break

    if not cat_item:
        cat_item = {"title": title, "posts": [], "episodes": [], "episodes_dub": [], "episodes_sub": []}

    cancel_id = f"fex_ep_{t_key}_{ep_num}"
    cancel_kb = build_cancel_keyboard(cancel_id)

    pref_tag = "🎙 Озвучення" if pref == "dub" else ("📝 Субтитри" if pref == "sub" else "Озвучення + Субтитри")
    status_msg = await callback.message.answer(
        f"📥 <b>Завантаження {ep_num} серії «{title}» ({pref_tag})...</b>\n"
        "🔍 Шукаю файл роздачі (4K / 1080p)...",
        reply_markup=cancel_kb
    )

    clean_t = sanitize_folder_name(title)
    t_folder = DOWNLOADS_DIR / clean_t
    t_folder.mkdir(parents=True, exist_ok=True)

    # Шукаємо FEX share_key: спочатку зі збереженої підписки (разом з ID підпапки), потім з каталогу
    share_key = None
    if sub and sub.get("fex_url"):
        share_key = FexDownloader.extract_share_key(sub["fex_url"])
    if not share_key:
        share_key = cat_item.get("fex_share_key")
    if not share_key:
        for p in cat_item.get("posts", []):
            for l_lbl, l_url in p.get("links", []):
                sk = FexDownloader.extract_share_key(l_url)
                if sk:
                    share_key = sk
                    break
            if share_key:
                break

    files = []
    if share_key:
        files = await FexDownloader.get_files_in_share(share_key)
        if len(files) > 25:
            norm_t = normalize_title_for_match(title)
            title_matched = [f for f in files if norm_t and norm_t[:15] in normalize_title_for_match(f.get("name", ""))]
            if title_matched:
                files = title_matched

    # Шукаємо файл саме для цієї серії у FEX
    ep_patterns = [
        f"[{ep_num:02d} з", f"[{ep_num:02d} ", f"[{ep_num:02d}]",
        f"[{ep_num} з", f"[{ep_num} ", f"[{ep_num}]",
        f" {ep_num:02d} ", f" {ep_num} "
    ]
    ep_files = [f for f in files if any(p in f.get("name", "") for p in ep_patterns)]

    # КРИТИЧНЕ ВИПРАВЛЕННЯ: якщо файлу саме цієї серії немає на FEX
    # (наприклад, серія 5 вийшла лише з субтитрами в онлайн-медіатеці),
    # ми В ЖОДНОМУ РАЗІ не завантажуємо замість неї серію 4!
    if not ep_files:
        target_post = None
        for p in cat_item.get("posts", []):
            if p.get("ep_num") == ep_num:
                target_post = p
                break

        media_link = normalize_telegram_url(sub.get("thread_url")) if sub else None
        if not media_link and target_post:
            for l_lbl, l_url in target_post.get("links", []):
                if "mediareser" in l_url or "t.me" in l_url:
                    media_link = normalize_telegram_url(l_url)
                    break
        if not media_link:
            media_link = normalize_telegram_url(cat_item.get("media_url"))

        msg_text = (
            f"📲 <b>Серія {ep_num} наразі доступна з субтитрами в онлайн-медіатеці Telegram!</b>\n\n"
            f"🎬 <b>Тайтл:</b> {title}\n"
            f"📝 <b>Реліз:</b> Субтитри (найновіша серія)\n"
        )
        if media_link:
            msg_text += f"🔗 <b>Гілка медіатеки:</b> <a href=\"{media_link}\">{media_link}</a>\n\n"
        msg_text += (
            "💡 <b>Як зберегти серію на Mac зараз:</b>\n"
            "1. Перейдіть у тему за кнопкою нижче.\n"
            "2. Перешліть (<b>Forward</b>) відео серії сюди в бот — він одразу збереже його у папку:\n"
            f"<code>/Users/user/Downloads/(Завантажене аніме)/{clean_t}</code>\n\n"
            f"⏳ <i>Роздача на FEX.NET у 4K (з озвученням та субтитрами) для {ep_num} серії ще очікується. Щойно з'явиться — бот сповістить вас!</i>"
        )
        kb_ep = []
        if media_link:
            kb_ep.append([InlineKeyboardButton(text=f"↗️ Відкрити серію {ep_num} в Медіатеці", url=media_link)])
        kb_ep.append([InlineKeyboardButton(text="↩️ До картки аніме", callback_data=f"track_card:{t_key}")])

        await status_msg.edit_text(msg_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_ep))
        await safe_answer(callback)
        return

    # Якщо файли знайдено — враховуємо вибір користувача (озвучення чи саби)
    await safe_answer(callback, f"🚀 Завантажую {ep_num} серію «{title}»!", show_alert=False)

    if pref == "sub":
        pref_sub_files = [f for f in ep_files if any(w in f.get("name", "") for w in ["SUB", "Саб", "sub"])]
        pool_files = pref_sub_files if pref_sub_files else ep_files
    elif pref == "dub":
        pref_dub_files = [f for f in ep_files if any(w in f.get("name", "") for w in ["MVO", "Озвуч", "DUB", "dub"])]
        pool_files = pref_dub_files if pref_dub_files else ep_files
    else:
        pool_files = ep_files

    preferred_4k = [f for f in pool_files if any(q in f.get("name", "").lower() for q in ["2160p", "4k"])]
    preferred_1080p = [f for f in pool_files if "1080p" in f.get("name", "").lower()]
    target_file = preferred_4k[0] if preferred_4k else (preferred_1080p[0] if preferred_1080p else pool_files[0])

    f_name = target_file.get("name", f"episode_{ep_num}.mp4")
    dest = t_folder / f_name
    dl_url = target_file.get("download_url")
    tok = target_file.get("token")

    if dl_url:
        if platform.system() != "Darwin":
            dub_sub_label = "Озвучення (DUB)" if pref == "dub" else ("Субтитри (SUB)" if pref == "sub" else "Озвучення + Субтитри")
            cmd_payload = json.dumps({
                "url": dl_url,
                "token": tok,
                "title": title,
                "episode": ep_num,
                "folder": clean_t,
                "filename": f_name,
                "chat_id": chat_id,
                "msg_id": status_msg.message_id,
                "quality": "4K Ultra HD" if "2160p" in f_name else "1080p Full HD",
                "dub_sub": dub_sub_label
            })
            queue_mac_command("download_fex", cmd_payload)
            await status_msg.edit_text(
                f"📥 <b>Завантаження з FEX.NET на Mac SSD</b>\n\n"
                f"🎬 <b>Аніме:</b> {title} [Серія {ep_num}]\n"
                f"📄 <b>Файл:</b> <code>{f_name}</code>\n"
                f"📂 <b>Папка:</b> <code>(Завантажене аніме)/{clean_t}</code>\n\n"
                f"<code>[▱▱▱▱▱▱▱▱▱▱] 0.0%</code>\n"
                f"🔄 <i>Статус: Передача завдання на MacAgent (очікування ~2-5 сек)...</i>"
            )
            return

        tracker = DownloadProgressTracker(interval_seconds=2.2)

        async def on_ep_prog(downloaded: int, total: int):
            if tracker.should_update_telegram():
                speed, eta_calc = tracker.calculate_stats(downloaded, total)
                bar = render_progress_bar(downloaded, total, speed, eta_calc)
                try:
                    await status_msg.edit_text(
                        f"🚀 <b>Завантаження: {title} [Серія {ep_num}]</b>\n"
                        f"📄 Файл: <code>{f_name}</code>\n\n" + bar,
                        reply_markup=cancel_kb
                    )
                except Exception:
                    pass

        dl_coro = FexDownloader.download_file(dl_url, dest, token=tok, progress_callback=on_ep_prog)
        dl_task = asyncio.create_task(dl_coro)
        ACTIVE_DOWNLOAD_TASKS[cancel_id] = dl_task

        try:
            await dl_task
            set_macos_tag(dest, TAG_NAME, TAG_COLOR)
            set_macos_tag(t_folder, TAG_NAME, TAG_COLOR)
            register_file(dest)
            try:
                await notify_download_finished(
                    bot,
                    callback.message.chat.id,
                    status_msg,
                    dest,
                    title,
                    prefix_text=f"🎉 <b>Серія {ep_num} успішно завантажена на Mac!</b>"
                )
            except Exception as ex_notif:
                logger.error(f"Помилка відправки картки завершення: {ex_notif}")
                # Надійний фолбек: надсилаємо нове повідомлення з кнопками плеєра
                f_key = register_file(dest)
                kb_fb = build_downloaded_action_keyboard(f_key, title=title, chat_id=callback.message.chat.id)
                await bot.send_message(
                    chat_id=callback.message.chat.id,
                    text=(
                        f"🎉 <b>Серія {ep_num} успішно завантажена на Mac!</b>\n\n"
                        f"🎬 <b>Тайтл:</b> {title}\n"
                        f"📄 <b>Файл:</b> <code>{dest.name}</code>\n\n"
                        "🍿 <i>Вмикайте в IINA або переходьте до папки:</i>"
                    ),
                    reply_markup=kb_fb
                )
        except asyncio.CancelledError:
            logger.info(f"Завантаження {cancel_id} скасовано користувачем.")
            try:
                if dest.exists():
                    dest.unlink()
            except Exception:
                pass
            try:
                await status_msg.edit_text(f"🛑 Завантаження серії {ep_num} скасовано.")
            except Exception:
                pass
        except Exception as ex_dl:
            logger.error(f"Помилка завантаження FEX: {ex_dl}")
            try:
                await status_msg.edit_text(f"❌ Помилка завантаження: {ex_dl}")
            except Exception:
                await callback.message.answer(f"❌ Помилка завантаження: {ex_dl}")
        finally:
            ACTIVE_DOWNLOAD_TASKS.pop(cancel_id, None)


@dp.callback_query(F.data.startswith("remind_evening:"))
async def cb_remind_evening(callback: CallbackQuery):
    file_key = callback.data.split(":")[1]
    file_path = resolve_file(file_key, callback.message)
    title_name = file_path.name if file_path else "Серія аніме"
    remind_ts = schedule_evening_reminder(callback.message.chat.id, file_key, title_name, target_hour=21, target_minute=10)
    import datetime
    rem_dt = datetime.datetime.fromtimestamp(remind_ts)
    time_str = rem_dt.strftime("%H:%M")
    await safe_answer(callback, f"⏰ Нагадування встановлено на {time_str}! Бот нагадає переглянути серію.", show_alert=True)


def get_all_downloaded_videos() -> List[Path]:
    """Повертає список усіх завантажених відеофайлів на диску та авто-очищує порожні папки."""
    if not DOWNLOADS_DIR.exists():
        return []
    for sub in list(DOWNLOADS_DIR.iterdir()):
        if sub.is_dir():
            cleanup_empty_folder(sub)
    files = []
    for ext in ("*.mkv", "*.mp4", "*.webm", "*.avi"):
        files.extend(DOWNLOADS_DIR.rglob(ext))
    return [f for f in files if not f.name.endswith(".part") and f.is_file()]


def format_anime_display_name(raw_name: str, max_len: int = 46) -> str:
    """Очищує назву від технічних тегів релізу ([РГ], [WEBRip...]) для красивого відображення."""
    name = raw_name
    # 1. Вирізаємо початкові реліз-групи [РГ], РГ], [UkrSub], RH SUB тощо
    name = re.sub(
        r"^\[?(?:РГ|RH\s*SUB|RH|UkrSub|HDRezka|BaibaKo|AniDUB|FanDub|Sub|Dub|Toloka|RG|AniUA)[\]\s\-–—:]+\s*",
        "",
        name,
        flags=re.IGNORECASE
    )

    # 2. Витягуємо інформацію про номер серії: [02 з ХХ] або S02E03
    s_match = re.search(r"S\d+E(\d+)", name, flags=re.IGNORECASE)
    ep_suffix = ""
    if s_match:
        ep_num = s_match.group(1).lstrip("0") or "0"
        ep_suffix = f" (Еп.{ep_num})"
        name = re.sub(r"S\d+E\d+", "", name, flags=re.IGNORECASE)
    else:
        ep_match = re.search(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]", name)
        if ep_match:
            ep_num = ep_match.group(1).lstrip("0") or "0"
            ep_suffix = f" (Еп.{ep_num})"
            name = name[:ep_match.start()] + name[ep_match.end():]

    # 3. Прибираємо технічні характеристики відео
    name = re.sub(
        r"\[?(?:WEBRip|BDRip|HDTV|1080p|720p|2160p|x264|x265|HEVC|AAC|DDP|UKR\s*MVO|UKR\s*SUB|UKR|ENG)[^\]]*\]?",
        "",
        name,
        flags=re.IGNORECASE
    )

    # 4. Чистимо зайві пробіли та роздільники
    name = re.sub(r"\s+", " ", name).strip(" -_–—[],.")
    if not name:
        name = raw_name.strip()

    full_name = f"{name}{ep_suffix}".strip()
    if len(full_name) > max_len:
        return full_name[:max_len - 1] + "…"
    return full_name


def format_compact_size(size_bytes: int) -> str:
    """Повертає короткий розмір (415M, 1.2G)."""
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / (1024 ** 3):.1f}G"
    return f"{size_bytes / (1024 ** 2):.0f}M"


class SyncedVirtualFolder:
    """Обгортка для віддаленої папки тайтлу з Mac."""
    def __init__(self, folder_path_str: str, files: Optional[List[Any]] = None):
        self._path = Path(folder_path_str)
        self.name = self._path.name
        self.parent = self._path.parent
        self._files: List[Any] = files or []

    def resolve(self):
        return self

    def __str__(self):
        return str(self._path)

    def exists(self):
        return True

    def is_dir(self):
        return True

    def is_file(self):
        return False

    def iterdir(self):
        return iter(self._files)

    def glob(self, pattern: str):
        return iter(self._files)


class SyncedVirtualFile:
    """Обгортка для віддалених файлів з Mac для безпечного отримання розміру та шляху на сервері."""
    def __init__(self, full_path: str, size: int, parent_folder: Optional[Any] = None):
        self._path = Path(full_path)
        self.name = self._path.name
        self.stem = self._path.stem
        self.suffix = self._path.suffix
        self.parent = parent_folder if parent_folder is not None else self._path.parent
        self._size = size

    def resolve(self):
        return self

    def __str__(self):
        return str(self._path)

    def stat(self):
        class _Stat:
            def __init__(self, sz):
                self.st_size = sz
                self.st_mtime = 0
        return _Stat(self._size)

    def exists(self):
        return True

    def is_file(self):
        return True

    def is_dir(self):
        return False

    def with_name(self, name: str):
        return self._path.with_name(name)


def get_grouped_anime() -> Dict[str, List[Any]]:
    """Групує всі завантажені відеофайли за папками тайтлів (підтримує Mac-синхронізацію)."""
    # 1. Якщо є синхронізовані дані з Mac-агента — беремо їх
    synced = get_synced_mac_files()
    if synced:
        grouped: Dict[str, List[Any]] = {}
        for folder, files in synced.items():
            first_p = Path(files[0]["full_path"]) if files else Path(f"/Users/user/Downloads/(Завантажене аніме)/{folder}/video.mkv")
            v_folder = SyncedVirtualFolder(str(first_p.parent))
            items = []
            for item in files:
                vf = SyncedVirtualFile(item["full_path"], item.get("file_size", 0), parent_folder=v_folder)
                f_key = item["file_key"]
                FILE_REGISTRY[f_key] = vf
                items.append(vf)
            items.sort(key=lambda x: x.name)
            v_folder._files = items
            t_key = "t_" + hashlib.md5(str(v_folder).encode("utf-8")).hexdigest()[:8]
            TITLE_REGISTRY[t_key] = v_folder
            grouped[folder] = items
        return grouped

    # 2. Локальне сканування файлової системи (якщо бот працює локально)
    videos = get_all_downloaded_videos()
    grouped: Dict[str, List[Any]] = {}
    for v in videos:
        folder_name = v.parent.name if v.parent != DOWNLOADS_DIR else "Різне"
        grouped.setdefault(folder_name, []).append(v)

    # Сортуємо серії всередині тайтлу
    for t in grouped:
        grouped[t].sort(key=lambda x: x.name)
    return grouped


def extract_episode_number_from_filename(filename: str) -> Optional[int]:
    """Визначає номер серії з імені файлу чи папки."""
    m = re.search(r'\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]', filename, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m2 = re.search(r'S\d+E(\d+)', filename, re.IGNORECASE)
    if m2:
        return int(m2.group(1))
    m3 = re.search(r'(\d+)\s*(?:серія|серію|епізод|ep)', filename, re.IGNORECASE)
    if m3:
        return int(m3.group(1))
    return None


def build_cleanup_message_text(grouped_anime: Dict[str, List[Any]], total_size: int) -> str:
    """Формує розділений список серій: окремо черга до перегляду (нові) та окремо вже переглянуті."""
    mac_state = get_mac_device_state()
    free_gb = mac_state.get("free_disk_gb", 0.0) if mac_state else get_free_disk_space_gb()
    total_episodes = sum(len(files) for files in grouped_anime.values())
    titles_count = len(grouped_anime)

    statuses = get_all_watch_statuses()

    unwatched_items = []
    watched_items = []

    for title, files in grouped_anime.items():
        clean_title = format_anime_display_name(title, max_len=50)

        for f in files:
            f_key = register_file(f)
            st = statuses.get(f_key, "new")
            is_active = is_file_currently_playing(f_key)
            if is_active:
                st = "watching"

            ep = extract_episode_number_from_filename(f.name) or extract_episode_number_from_filename(title)
            ep_str = f" [Серія {ep}]" if ep else ""
            c_size = format_compact_size(f.stat().st_size)

            item_info = {
                "title": clean_title,
                "ep_str": ep_str,
                "size": c_size,
                "key": f_key,
                "status": st,
            }

            if st == "watched":
                watched_items.append(item_info)
            else:
                unwatched_items.append(item_info)

    lines = [
        "🍿 <b>Твої аніме на Mac</b>\n",
        f"📁 <b>Тайтлів:</b> {titles_count}  •  🎞 <b>Серій:</b> {total_episodes}",
        f"📦 <b>Разом:</b> <code>{format_bytes(total_size)}</code>  •  💾 <b>Вільно:</b> <code>{free_gb:.1f} ГБ</code>\n",
    ]

    # Секція 1: Черга до перегляду (Нові)
    if unwatched_items:
        lines.append(f"🟢 <b>Черга до перегляду ({len(unwatched_items)} не бачив):</b>")
        for idx, item in enumerate(unwatched_items, 1):
            badge = " 👀 <i>(зараз у плеєрі)</i>" if item["status"] == "watching" else ""
            lines.append(f"<b>{idx}.</b> {item['title']}{item['ep_str']}{badge}")
        lines.append("")

    # Секція 2: Вже переглянуті
    if watched_items:
        lines.append(f"⚪️ <b>Вже переглянуті ({len(watched_items)}):</b>")
        for idx, item in enumerate(watched_items, 1):
            lines.append(f"<b>{idx}.</b> {item['title']}{item['ep_str']}")
        lines.append("")

    if not unwatched_items and not watched_items:
        lines.append("📋 <i>На диску немає доступних відеофайлів.</i>")

    return "\n".join(lines).strip()


def _visual_char_width(ch: str) -> float:
    """Приблизна відносна візуальна ширина символу у шрифті SF Pro Text (Telegram Desktop macOS)."""
    if ch == "\u2800":
        return 1.32
    if ord(ch) > 0x1F000:
        return 2.2
    if ch in " .,:;!'|іІїЇлЛтТ()[]-":
        return 0.58
    if ch in "ЖШЩМЮФЫжшщмюфыWMWm":
        return 1.35
    if ch.isupper():
        return 1.15
    return 0.95


def left_align_button_text(text: str, target_width: float = 39.5) -> str:
    """Додає невидимі символи Braille Blank (U+2800) праворуч до єдиної візуальної ширини,
    щоб усі кнопки списку аніме в Telegram були рівно прибиті до лівого краю без обрізання '...' посередині."""
    cur_w = sum(_visual_char_width(c) for c in text)
    if cur_w >= target_width:
        return text
    pad_count = int(round((target_width - cur_w) / 1.32))
    if pad_count <= 0:
        return text
    return text + ("\u2800" * pad_count)


def detect_tracks_label(filename: str, title: str = "", chat_id: Optional[int] = None) -> str:
    """Точно визначає тип доріжки скачаної серії: Озвучення (DUB) чи Субтитри (SUB).
    У релізах @ridnyiholos файли з озвученням на FEX.NET мають тег '[UKR MVO, UKR SUB].mkv'
    (основна аудіодоріжка — українське озвучення MVO + вшиті написи/субтитри в контейнері MKV),
    тоді як релізи тільки з субтитрами мають тег '[UKR SUB]' або 'RH SUB' без 'MVO'."""
    fn_upper = (filename or "").upper()
    title_upper = (title or "").upper()

    has_mvo = any(k in fn_upper for k in ("MVO", "DUB", "ОЗВУЧ"))
    has_sub = any(k in fn_upper for k in ("SUB", "САБ", "СУБТИТР"))

    # 1. Якщо у файлі явно вказано SUB і немає MVO/DUB/ОЗВУЧ — це чисті Субтитри (SUB)
    if has_sub and not has_mvo:
        return "Субтитри (SUB)"
    if ("[РГ SUB]" in title_upper or "RH SUB" in fn_upper or "[UKR SUB]" in fn_upper) and not has_mvo:
        return "Субтитри (SUB)"

    # 2. Перевіряємо підписку користувача на цей тайтл
    sub = None
    if chat_id and title:
        try:
            sub = get_subscription_by_title(chat_id, title)
        except Exception:
            sub = None
    elif title and DB_PATH.exists():
        try:
            norm_t = normalize_title_for_match(title)
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("SELECT title, pref_type FROM anime_subscriptions").fetchall()
                for r in rows:
                    nt = normalize_title_for_match(r["title"])
                    if norm_t and nt and (norm_t in nt or nt in norm_t):
                        sub = dict(r)
                        break
        except Exception:
            pass

    if sub:
        pref = sub.get("pref_type", "dub")
        if pref == "sub":
            return "Субтитри (SUB)"
        if pref == "dub":
            return "Озвучення (DUB)"

    # 3. Якщо у файлі є MVO / DUB / ОЗВУЧ — це реліз в Озвученні (DUB)
    if has_mvo:
        return "Озвучення (DUB)"
    if has_sub:
        return "Субтитри (SUB)"

    return "Озвучення (DUB)"


def build_watchlist_hub_data(chat_id: int) -> Tuple[Optional[str], Optional[InlineKeyboardMarkup]]:
    """Будує інтерактивний Watchlist Hub: зведений віджет черги до перегляду з прямими кнопками запуску кожної серії."""
    grouped = get_grouped_anime()
    if not grouped:
        return None, None

    statuses = get_all_watch_statuses()
    unwatched_episodes = []

    for title, files in grouped.items():
        clean_title = format_anime_display_name(title, max_len=40)
        for f in files:
            f_key = register_file(f)
            st = statuses.get(f_key, "new")
            is_active = is_file_currently_playing(f_key)
            if is_active:
                st = "watching"

            if st != "watched":
                ep = extract_episode_number_from_filename(f.name) or extract_episode_number_from_filename(title)
                ep_str = f" [Серія {ep}]" if ep else ""
                c_size = format_compact_size(f.stat().st_size)
                unwatched_episodes.append({
                    "title": clean_title,
                    "ep_str": ep_str,
                    "size": c_size,
                    "key": f_key,
                    "status": st,
                    "path": str(f),
                })

    if not unwatched_episodes:
        return None, None

    total_unwatched = len(unwatched_episodes)
    lines = [
        f"🍿 <b>Черга до перегляду на Mac ({total_unwatched} не бачив):</b>\n"
    ]

    for idx, item in enumerate(unwatched_episodes, 1):
        badge = " 👀 <i>(зараз у плеєрі)</i>" if item["status"] == "watching" else " 🟢"
        lines.append(f"<b>{idx}.</b> {item['title']}{item['ep_str']}{badge}")

    lines.append("\n⚡️ <i>Натисніть кнопку під серією, щоб запустити її на Mac:</i>")

    keyboard_rows = []
    for idx, item in enumerate(unwatched_episodes[:6], 1):  # До 6 серій для компактності
        short_title = format_anime_display_name(item["title"], max_len=18)
        btn_text = f"▶️ {idx}. {short_title}{item['ep_str']}"
        keyboard_rows.append([
            InlineKeyboardButton(text=btn_text, callback_data=f"play:{item['key']}")
        ])

    keyboard_rows.append([
        InlineKeyboardButton(text="📁 Усі мої аніме", callback_data="refresh_list"),
        InlineKeyboardButton(text="🔄 Оновити статус", callback_data="hub_refresh"),
    ])

    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


async def send_or_update_watchlist_hub(bot: Bot, chat_id: int):
    """Надсилає або плавно оновлює зведений Watchlist Hub у чаті користувача."""
    text, kb = build_watchlist_hub_data(chat_id)
    if not text or not kb:
        return

    old_msg_id = get_watchlist_hub_msg_id(chat_id)
    if old_msg_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=old_msg_id,
                text=text,
                reply_markup=kb,
                parse_mode="HTML"
            )
            return
        except Exception:
            pass

    # Якщо старого повідомлення немає або не вдалося відредагувати — надсилаємо нове
    try:
        new_msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        set_watchlist_hub_msg_id(chat_id, new_msg.message_id)
    except Exception as e:
        logger.debug(f"send_or_update_watchlist_hub error: {e}")


def build_cleanup_keyboard(grouped_anime: Dict[str, List[Any]], total_size: int) -> InlineKeyboardMarkup:
    """Будує інтерактивну клавіатуру: спочатку нові серії черги, потім переглянуті."""
    keyboard_rows = []
    statuses = get_all_watch_statuses()

    unwatched_buttons = []
    watched_buttons = []

    idx = 1
    for title, files in grouped_anime.items():
        clean_title = format_anime_display_name(title, max_len=27)

        # Визначаємо номери серій
        ep_numbers = []
        for f in files:
            ep = extract_episode_number_from_filename(f.name) or extract_episode_number_from_filename(title)
            if ep and ep not in ep_numbers:
                ep_numbers.append(ep)
        ep_numbers.sort()

        if len(files) > 1:
            t_key = register_title_folder(files[0].parent)
            # Перевіряємо чи є хоч одна непереглянута серія
            any_unwatched = any(statuses.get(register_file(f), "new") != "watched" for f in files)
            has_active = any(is_file_currently_playing(register_file(f)) for f in files)
            icon = "👀 " if has_active else ("🟢 " if any_unwatched else "⚪️ ")

            if ep_numbers:
                ep_str = f" (Еп. {', '.join(str(e) for e in ep_numbers)})"
            else:
                ep_str = f" ({len(files)} сер.)"
            btn_label = left_align_button_text(f"{idx}. {icon}{clean_title}{ep_str}")
            btn_row = [
                InlineKeyboardButton(
                    text=btn_label,
                    callback_data=f"title:{t_key}"
                )
            ]
            if any_unwatched:
                unwatched_buttons.append(btn_row)
            else:
                watched_buttons.append(btn_row)
        else:
            file_key = register_file(files[0])
            st = statuses.get(file_key, "new")
            is_active = is_file_currently_playing(file_key)
            if is_active:
                st = "watching"
            icon = "👀 " if st == "watching" else ("🟢 " if st == "new" else "⚪️ ")

            ep = ep_numbers[0] if ep_numbers else None
            ep_str = f" (Еп. {ep})" if ep else ""
            btn_label = left_align_button_text(f"{idx}. {icon}{clean_title}{ep_str}")
            btn_row = [
                InlineKeyboardButton(
                    text=btn_label,
                    callback_data=f"item:{file_key}"
                )
            ]
            if st == "watched":
                watched_buttons.append(btn_row)
            else:
                unwatched_buttons.append(btn_row)
        idx += 1

    keyboard_rows.extend(unwatched_buttons)
    keyboard_rows.extend(watched_buttons)

    # Кнопка швидкого видалення вже переглянутих серій (якщо такі є)
    has_watched_any = any(st == "watched" for st in statuses.values())
    if has_watched_any:
        keyboard_rows.append([
            InlineKeyboardButton(text="🗑 Очистити переглянуті", callback_data="clean_watched_ask")
        ])

    keyboard_rows.append([
        InlineKeyboardButton(text="📂 Відкрити папку у Finder", callback_data="reveal_folder")
    ])
    keyboard_rows.append([
        InlineKeyboardButton(
            text=f"💥 ВИДАЛИТИ ВСІ СЕРІЇ ({format_bytes(total_size)})",
            callback_data="del_all_ask"
        )
    ])
    keyboard_rows.append([
        InlineKeyboardButton(text="🔄 Оновити список", callback_data="refresh_list")
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


@dp.message(Command("list"))
@dp.message(Command("cleanup"))
@dp.message(Command("clean"))
@dp.message(Command("my"))
async def cmd_cleanup_menu(message: Message):
    """Показує список усіх завантажених аніме з можливістю відкриття в IINA в 1 клік."""
    if not is_user_allowed(message.from_user.id):
        return

    grouped = get_grouped_anime()
    if not grouped:
        await message.reply("📂 У папці <code>(Завантажене аніме)</code> наразі немає файлів. Усе чисто!")
        return

    total_size = sum(sum(f.stat().st_size for f in files) for files in grouped.values())
    text = build_cleanup_message_text(grouped, total_size)
    kb = build_cleanup_keyboard(grouped, total_size)
    await message.reply(text, reply_markup=kb)


@dp.callback_query(F.data == "refresh_list")
async def cb_refresh_list(callback: CallbackQuery):
    """Оновлює стан списку файлів."""
    grouped = get_grouped_anime()
    if not grouped:
        empty_text = "📂 У папці <code>(Завантажене аніме)</code> більше немає файлів. Усе чисто!"
        if callback.message.photo:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer(empty_text)
        else:
            await callback.message.edit_text(empty_text)
        await safe_answer(callback, "Список оновлено")
        return

    total_size = sum(sum(f.stat().st_size for f in files) for files in grouped.values())
    text = build_cleanup_message_text(grouped, total_size)
    kb = build_cleanup_keyboard(grouped, total_size)
    try:
        if callback.message.photo:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer(text, reply_markup=kb)
        else:
            await callback.message.edit_text(text, reply_markup=kb)
        await safe_answer(callback, "Оновлено")
    except Exception:
        await safe_answer(callback)


@dp.callback_query(F.data.startswith("title:"))
async def cb_title_screen(callback: CallbackQuery):
    """Екран тайтлу: показує список усіх серій обраного аніме з прев'юшкою."""
    t_key = callback.data.split(":")[1]
    folder_path = resolve_title_folder(t_key, callback.message)
    if not folder_path or not folder_path.exists():
        await safe_answer(callback, "⚠️ Папку тайтлу не знайдено на диску.", show_alert=True)
        return

    video_files = [f for f in folder_path.iterdir() if f.is_file() and f.suffix.lower() in (".mkv", ".mp4", ".webm", ".avi") and not f.name.endswith(".part")]
    video_files.sort(key=lambda x: x.name)

    if not video_files:
        cleanup_empty_folder(folder_path)
        if callback.message.photo:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer("📂 У цьому тайтлі більше немає серій.")
        else:
            await callback.message.edit_text("📂 У цьому тайтлі більше немає серій.")
        await safe_answer(callback)
        return

    clean_title = format_anime_display_name(folder_path.name, max_len=55)
    total_size = sum(f.stat().st_size for f in video_files)
    total_size_str = format_bytes(total_size)

    lines = [
        f"📁 <b>{clean_title}</b>\n",
        f"🎞 <b>Серій:</b> {len(video_files)}  •  📦 <b>Розмір:</b> <code>{total_size_str}</code>\n",
        "📋 <b>Серії тайтлу:</b>",
    ]

    keyboard_rows = []
    for idx, vf in enumerate(video_files, 1):
        file_key = register_file(vf)
        raw_ep = vf.stem
        ep_name = format_anime_display_name(raw_ep, max_len=45)
        ep_btn_name = format_anime_display_name(raw_ep, max_len=28)
        is_active = is_file_currently_playing(file_key)
        icon = "👀 " if is_active else ""
        badge = " 👀 <i>(зараз у плеєрі)</i>" if is_active else ""
        lines.append(f"<b>{idx}.</b> {ep_name}{badge}")

        keyboard_rows.append([
            InlineKeyboardButton(
                text=left_align_button_text(f"{idx}. {icon}{ep_btn_name}"),
                callback_data=f"item:{file_key}"
            )
        ])

    keyboard_rows.append([
        InlineKeyboardButton(text="📂 Відкрити папку у Finder", callback_data=f"reveal_dir:{t_key}")
    ])
    keyboard_rows.append([
        InlineKeyboardButton(
            text=f"🗑 Видалити весь тайтл ({total_size_str})",
            callback_data=f"del_title_ask:{t_key}"
        )
    ])
    keyboard_rows.append([
        InlineKeyboardButton(text="⬅️ Назад до всіх аніме", callback_data="refresh_list")
    ])

    caption_text = "\n".join(lines)
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)

    # Вибираємо кадр для прев'ю тайтлу (з активної або першої серії)
    active_vf = next((vf for vf in video_files if is_file_currently_playing(register_file(vf))), video_files[0])
    thumb = get_video_thumbnail(active_vf)

    if thumb:
        if callback.message.photo:
            try:
                await callback.message.edit_media(
                    InputMediaPhoto(media=FSInputFile(thumb), caption=caption_text),
                    reply_markup=kb
                )
            except Exception:
                try:
                    await callback.message.edit_caption(caption=caption_text, reply_markup=kb)
                except Exception:
                    pass
        else:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer_photo(
                photo=FSInputFile(thumb),
                caption=caption_text,
                reply_markup=kb
            )
    else:
        if callback.message.photo:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer(caption_text, reply_markup=kb)
        else:
            try:
                await callback.message.edit_text(caption_text, reply_markup=kb)
            except Exception:
                pass
    await safe_answer(callback)


@dp.callback_query(F.data.startswith("reveal_dir:"))
async def cb_reveal_dir(callback: CallbackQuery):
    t_key = callback.data.split(":")[1]
    folder_path = resolve_title_folder(t_key, callback.message)
    if folder_path and folder_path.exists():
        reveal_in_finder(folder_path)
        await safe_answer(callback, "📂 Папку тайтлу відкрито у Finder!", show_alert=False)
    else:
        await safe_answer(callback, "⚠️ Папку не знайдено.", show_alert=True)


@dp.callback_query(F.data.startswith("del_title_ask:"))
async def cb_del_title_ask(callback: CallbackQuery):
    t_key = callback.data.split(":")[1]
    folder_path = resolve_title_folder(t_key, callback.message)
    if not folder_path or not folder_path.exists():
        await safe_answer(callback, "⚠️ Папку не знайдено.", show_alert=True)
        return

    clean_title = format_anime_display_name(folder_path.name, max_len=50)
    video_files = list(folder_path.glob("*.*"))
    total_size = sum(f.stat().st_size for f in video_files if f.is_file())

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔴 ТАК, ВИДАЛИТИ ТАЙТЛ", callback_data=f"del_title_confirm:{t_key}"),
                InlineKeyboardButton(text="↩️ Скасувати", callback_data=f"title:{t_key}"),
            ]
        ]
    )
    ask_text = (
        f"⚠️ <b>Видалити всі серії тайтлу «{clean_title}»?</b>\n"
        f"📦 Це звільнить <code>{format_bytes(total_size)}</code> на диску Mac."
    )
    if callback.message.photo:
        try:
            await callback.message.edit_caption(caption=ask_text, reply_markup=kb)
        except Exception:
            pass
    else:
        await callback.message.edit_text(ask_text, reply_markup=kb)
    await safe_answer(callback)


@dp.callback_query(F.data.startswith("del_title_confirm:"))
async def cb_del_title_confirm(callback: CallbackQuery):
    t_key = callback.data.split(":")[1]
    folder_path = resolve_title_folder(t_key, callback.message)
    if not folder_path or not folder_path.exists():
        await safe_answer(callback, "⚠️ Папку вже видалено.", show_alert=True)
        return

    clean_title = format_anime_display_name(folder_path.name, max_len=50)
    total_size = sum(f.stat().st_size for f in folder_path.glob("*.*") if f.is_file())
    ep_re_del = re.compile(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]")
    for f in folder_path.glob("*.*"):
        if f.is_file():
            m_ep = ep_re_del.search(f.name)
            if m_ep:
                mark_episode_handled(callback.from_user.id, folder_path.name, int(m_ep.group(1)), f.name)
            if hasattr(folder_path, "_files") or platform.system() != "Darwin":
                queue_mac_command("delete_file", str(f))
    if not (hasattr(folder_path, "_files") or platform.system() != "Darwin"):
        shutil.rmtree(folder_path, ignore_errors=True)
    TITLE_REGISTRY.pop(t_key, None)

    mac_state = get_mac_device_state()
    free_gb = mac_state.get("free_disk_gb", 0.0) if mac_state else get_free_disk_space_gb()
    confirm_text = (
        f"💥 <b>Тайтл «{clean_title}» успішно видалено!</b>\n\n"
        f"✨ Звільнено: <code>{format_bytes(total_size)}</code>\n"
        f"💾 Вільно на диску Mac: <code>{free_gb:.1f} ГБ</code>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📂 До списку аніме", callback_data="refresh_list")]]
    )
    if callback.message.photo:
        try:
            await callback.message.delete()
        except Exception:
            pass
        await callback.message.answer(confirm_text, reply_markup=kb)
    else:
        await callback.message.edit_text(confirm_text, reply_markup=kb)
    await safe_answer(callback, "Тайтл повністю видалено!", show_alert=True)


@dp.callback_query(F.data == "del_all_ask")
async def cb_del_all_ask(callback: CallbackQuery):
    """Запитує підтвердження на повне очищення папки."""
    grouped = get_grouped_anime()
    total_files = sum(len(files) for files in grouped.values())
    total_size = sum(sum(f.stat().st_size for f in files) for files in grouped.values())

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔴 ТАК, ВИДАЛИТИ ВСІ СЕРІЇ", callback_data="del_all_confirm"),
                InlineKeyboardButton(text="↩️ Скасувати", callback_data="refresh_list"),
            ]
        ]
    )
    await callback.message.edit_text(
        f"⚠️ <b>Ви впевнені, що хочете видалити ВСІ {total_files} серій?</b>\n"
        f"📦 Це звільнить <code>{format_bytes(total_size)}</code> на диску Mac.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "del_all_confirm")
async def cb_del_all_confirm(callback: CallbackQuery):
    """Видаляє всі файли та папки в (Завантажене аніме)."""
    grouped = get_grouped_anime()
    deleted_count = sum(len(files) for files in grouped.values())
    freed_bytes = sum(sum(f.stat().st_size for f in files) for files in grouped.values())

    # Ставимо команду на очищення Mac-агенту
    queue_mac_command("clean_all")

    if DOWNLOADS_DIR.exists():
        for item in list(DOWNLOADS_DIR.iterdir()):
            try:
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink(missing_ok=True)
            except Exception as e:
                logger.error(f"Помилка видалення {item}: {e}")

    FILE_REGISTRY.clear()
    mac_state = get_mac_device_state()
    free_gb = mac_state.get("free_disk_gb", 0.0) if mac_state else get_free_disk_space_gb()

    await callback.message.edit_text(
        f"💥 <b>Усі серії та папки успішно видалено з ноута!</b>\n\n"
        f"🗑 <b>Видалено файлів:</b> {deleted_count} шт.\n"
        f"✨ <b>Звільнено:</b> <code>{format_bytes(freed_bytes)}</code>\n"
        f"💾 <b>Вільно на диску Mac:</b> <code>{free_gb:.1f} ГБ</code>"
    )
    await safe_answer(callback, "Всі аніме та папки видалено!", show_alert=True)


def resolve_file(file_key: str, message: Optional[Message] = None) -> Optional[Any]:
    """Знаходить файл: у таблиці mac_local_files (із Mac-агента), у пам'яті FILE_REGISTRY, або в SQLite."""
    # 1. Спершу шукаємо у таблиці mac_local_files (найточніші синхронізовані дані від Mac-агента)
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT full_path, file_size, folder_name FROM mac_local_files WHERE file_key = ?",
                    (file_key,)
                ).fetchone()
                if row:
                    siblings_rows = conn.execute(
                        "SELECT file_key, full_path, file_size FROM mac_local_files WHERE folder_name = ? ORDER BY file_name",
                        (row["folder_name"],)
                    ).fetchall()
                    v_folder = SyncedVirtualFolder(str(Path(row["full_path"]).parent))
                    s_items = []
                    target_vf = None
                    for sr in siblings_rows:
                        svf = SyncedVirtualFile(sr["full_path"], sr["file_size"], parent_folder=v_folder)
                        FILE_REGISTRY[sr["file_key"]] = svf
                        s_items.append(svf)
                        if sr["file_key"] == file_key:
                            target_vf = svf
                    v_folder._files = s_items
                    t_key = "t_" + hashlib.md5(str(v_folder).encode("utf-8")).hexdigest()[:8]
                    TITLE_REGISTRY[t_key] = v_folder
                    if target_vf:
                        return target_vf
        except Exception as e:
            logger.debug(f"Помилка пошуку в mac_local_files: {e}")

    # 2. Швидкий пошук у пам'яті
    item = FILE_REGISTRY.get(file_key)
    if item and item.exists():
        return item

    # 3. Постійна база SQLite (file_registry)
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute(
                    "SELECT file_path FROM file_registry WHERE file_key = ?",
                    (file_key,)
                ).fetchone()
                if row:
                    p_str = row[0]
                    # Якщо локально файлу немає (бо ми на Railway), створюємо віртуальний
                    if not Path(p_str).exists():
                        vf = SyncedVirtualFile(p_str, 0)
                        FILE_REGISTRY[file_key] = vf
                        return vf
                    else:
                        p = Path(p_str)
                        FILE_REGISTRY[file_key] = p
                        return p
        except Exception as e:
            logger.debug(f"Помилка пошуку в SQLite: {e}")

    # 4. Відновлення за текстом повідомлення Telegram
    if message:
        text = message.text or message.caption or ""

        # Точний шлях у рядку "Шлях:" (з <code> або без нього)
        path_match = re.search(r"📂\s*(?:<b>)?Шлях:(?:</b>)?\s*(?:<code>)?([^\n<]+)", text)
        if path_match:
            candidate_str = path_match.group(1).strip()
            vf = SyncedVirtualFile(candidate_str, 0)
            FILE_REGISTRY[file_key] = vf
            return vf

        # Пошук за назвою файлу або тайтлу
        for pattern in (
            r"(?:📄|🎬)\s*(?:<b>)?(?:Файл|Назва|Тайтл):(?:</b>)?\s*(?:<code>)?([^\n<]+)",
            r"<code>([^<]+)</code>",
        ):
            m = re.search(pattern, text)
            if m:
                raw_name = m.group(1).strip()
                fn = Path(raw_name).name
                for ext in ("*.mkv", "*.mp4", "*.webm", "*.avi"):
                    for f in DOWNLOADS_DIR.rglob(ext):
                        if f.name == fn or fn in f.name:
                            FILE_REGISTRY[file_key] = f
                            return f
    return None


async def safe_answer(callback: CallbackQuery, text: str = "", show_alert: bool = False):
    """Безпечно надсилає відповідь на callback, ігноруючи застарілі запити."""
    try:
        await callback.answer(text, show_alert=show_alert)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("play:"))
async def cb_play_video(callback: CallbackQuery):
    """Запускає вибране відео у відеоплеєрі IINA на Mac та оновлює статус."""
    file_key = callback.data.split(":")[1]
    file_path = resolve_file(file_key, callback.message)

    if not file_path or not file_path.exists():
        await safe_answer(callback, "⚠️ Файл уже не знайдено на диску ноутбука.", show_alert=True)
        return

    ok, app_name = launch_in_iina(file_path)
    if ok:
        set_currently_playing(file_key)
        try:
            await cb_item_menu(callback)
        except Exception:
            pass
        await safe_answer(callback, f"🎬 Відкрито в {app_name} на Mac!", show_alert=False)
    else:
        await safe_answer(callback, f"❌ Не вдалося відкрити: {app_name}", show_alert=True)


@dp.callback_query(F.data.startswith("smart_del:"))
async def cb_smart_del(callback: CallbackQuery):
    """Видалення серії за запитом автоочищення."""
    file_key = callback.data.split(":")[1]
    file_path = resolve_file(file_key, callback.message)
    if not file_path or not file_path.exists():
        await callback.message.edit_text("ℹ️ Серію вже було видалено раніше.")
        await safe_answer(callback)
        return

    size_str = format_bytes(file_path.stat().st_size)
    parent_dir = file_path.parent
    delete_video_file_and_associated(file_path)
    FILE_REGISTRY.pop(file_key, None)
    clean_orphan_anime_folders()

    free_gb = get_free_disk_space_gb()
    await callback.message.edit_text(
        f"✨ <b>Серію та пов'язані постери/прев'ю успішно видалено з Mac!</b>\n\n"
        f"🗑 Звільнено: <code>{size_str}</code>\n"
        f"💾 Вільно на диску: <code>{free_gb:.1f} ГБ</code>"
    )
    await safe_answer(callback, "Серію та папку очищено!")


@dp.callback_query(F.data.startswith("smart_keep:"))
async def cb_smart_keep(callback: CallbackQuery):
    """Збереження серії при запиті автоочищення."""
    await callback.message.edit_text("👌 Добре, серію збережено на вашому диску!")
    await safe_answer(callback)


@dp.callback_query(F.data.startswith("reveal:"))
async def cb_reveal_finder(callback: CallbackQuery):
    """Відкриває папку з файлом у Finder на Mac."""
    file_key = callback.data.split(":")[1]
    file_path = resolve_file(file_key, callback.message)

    if not file_path or not file_path.exists():
        await safe_answer(callback, "⚠️ Файл уже не знайдено на диску ноутбука.", show_alert=True)
        return

    reveal_in_finder(file_path)
    await safe_answer(callback, "📂 Відкрито папку у Finder на Mac!", show_alert=False)


@dp.callback_query(F.data == "reveal_folder")
async def cb_reveal_folder(callback: CallbackQuery):
    """Відкриває кореневу папку завантаженого аніме у Finder."""
    reveal_in_finder(DOWNLOADS_DIR)
    await safe_answer(callback, "📂 Відкрито папку аніме у Finder!", show_alert=False)


@dp.callback_query(F.data.startswith("item:"))
async def cb_item_menu(callback: CallbackQuery):
    """Картка дій над серією (Варіант А): статус перегляду, прев'юшка з кадру, запуск в IINA, показ у Finder, видалення."""
    file_key = callback.data.split(":")[1]
    file_path = resolve_file(file_key, callback.message)

    if not file_path or not file_path.exists():
        await safe_answer(callback, "⚠️ Файл уже не знайдено на диску ноутбука.", show_alert=True)
        return

    raw_name = file_path.parent.name if file_path.parent != DOWNLOADS_DIR else file_path.stem
    clean_title = format_anime_display_name(raw_name, max_len=60)
    size_str = format_bytes(file_path.stat().st_size)
    is_active = is_file_currently_playing(file_key)

    status_badge = "👀 <b>Зараз відкрито у плеєрі IINA на Mac</b>\n\n" if is_active else "🟢 <b>Готово до перегляду</b>\n\n"

    # Визначаємо, куди повертатися кнопкою «Назад»
    parent_dir = file_path.parent
    is_grouped_title = False
    title_back_key = None
    if parent_dir.exists() and parent_dir != DOWNLOADS_DIR:
        siblings = [
            f for f in parent_dir.iterdir()
            if f.is_file() and f.suffix.lower() in (".mkv", ".mp4", ".webm", ".avi") and not f.name.endswith(".part")
        ]
        if len(siblings) > 1:
            is_grouped_title = True
            title_back_key = register_title_folder(parent_dir)

    if is_grouped_title and title_back_key:
        back_btn = InlineKeyboardButton(text="⬅️ Назад до серій тайтлу", callback_data=f"title:{title_back_key}")
    else:
        back_btn = InlineKeyboardButton(text="⬅️ Назад до списку", callback_data="refresh_list")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="▶️ Запустити в плеєрі", callback_data=f"play:{file_key}"),
                InlineKeyboardButton(text="📂 Відкрити у Finder", callback_data=f"reveal:{file_key}"),
            ],
            [
                InlineKeyboardButton(text="🗑 Видалити", callback_data=f"del:{file_key}")
            ],
            [
                back_btn
            ]
        ]
    )

    # Витягуємо інформацію про епізод, якість та доріжки для чистого вигляду
    fname = file_path.name
    ep_m = re.search(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]", fname)
    ep_badge = f" [Серія {int(ep_m.group(1))}]" if ep_m else ""

    quality_str = "4K Ultra HD" if "2160p" in fname else ("1080p Full HD" if "1080p" in fname else "1080p Full HD")
    tracks_label = detect_tracks_label(fname, clean_title, callback.from_user.id)

    text = (
        f"🎬 <b>{clean_title}</b>{ep_badge}\n"
        f"🎞 <b>Якість:</b> <code>{quality_str}</code>\n\n"
        f"🔊 <b>Доріжки:</b> <i>{tracks_label}</i>\n"
        f"🏷 <b>Finder:</b> 🟢 {TAG_NAME}"
    )

    thumb = get_video_thumbnail(file_path)
    if thumb:
        if callback.message.photo:
            try:
                await callback.message.edit_media(
                    InputMediaPhoto(media=FSInputFile(thumb), caption=text),
                    reply_markup=kb
                )
            except Exception:
                try:
                    await callback.message.edit_caption(caption=text, reply_markup=kb)
                except Exception:
                    pass
        else:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer_photo(
                photo=FSInputFile(thumb),
                caption=text,
                reply_markup=kb
            )
    else:
        if callback.message.photo:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer(text, reply_markup=kb)
        else:
            try:
                await callback.message.edit_text(text, reply_markup=kb)
            except Exception:
                pass
    await safe_answer(callback)


@dp.callback_query(F.data.startswith("cancel_dl:"))
async def cb_cancel_download(callback: CallbackQuery):
    """Скасовує активний процес завантаження файлу на Mac."""
    cancel_id = callback.data.split(":")[1]
    task = ACTIVE_DOWNLOAD_TASKS.get(cancel_id)

    if not task or task.done():
        await safe_answer(callback, "⚠️ Завантаження вже завершено або було скасовано.", show_alert=True)
        return

    task.cancel()
    await safe_answer(callback, "🛑 Скасовую завантаження...", show_alert=False)


@dp.callback_query(F.data.startswith("del:"))
async def cb_delete_ask(callback: CallbackQuery):
    """Запитує підтвердження на видалення одного файлу."""
    file_key = callback.data.split(":")[1]
    file_path = resolve_file(file_key, callback.message)

    if not file_path or not file_path.exists():
        await safe_answer(callback, "⚠️ Файл уже не існує або був видалений раніше.", show_alert=True)
        return

    raw_name = file_path.parent.name if file_path.parent != DOWNLOADS_DIR else file_path.stem
    clean_title = format_anime_display_name(raw_name, max_len=50)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔴 Точно видалити", callback_data=f"confirm_del:{file_key}"),
                InlineKeyboardButton(text="↩️ Скасувати", callback_data=f"item:{file_key}"),
            ]
        ]
    )

    del_text = (
        f"❓ <b>Видалити цю серію?</b>\n\n"
        f"🎬 <b>Тайтл:</b> {clean_title}\n"
        f"📄 <code>{file_path.name}</code>"
    )

    try:
        if callback.message.photo:
            await callback.message.edit_caption(caption=del_text, reply_markup=kb)
        else:
            await callback.message.edit_text(del_text, reply_markup=kb)
        await safe_answer(callback)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("confirm_del:"))
async def cb_delete_confirm(callback: CallbackQuery):
    """Безпосередньо видаляє файл з ноутбука."""
    file_key = callback.data.split(":")[1]
    file_path = resolve_file(file_key, callback.message)

    if not file_path or not file_path.exists():
        await safe_answer(callback, "⚠️ Файл уже не знайдено на диску.", show_alert=True)
        return

    try:
        file_size = file_path.stat().st_size
        size_str = format_bytes(file_size)
        p_str = str(file_path.resolve() if hasattr(file_path, "resolve") else file_path)

        # Фіксуємо видалену серію як оброблену/переглянуту в постійну історію, щоб ніколи повторно не качати її
        ep_re_single = re.compile(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]")
        m_single = ep_re_single.search(file_path.name)
        if m_single:
            parent_title = file_path.parent.name if hasattr(file_path, "parent") else ""
            mark_episode_handled(callback.from_user.id, parent_title, int(m_single.group(1)), file_path.name)

        # Якщо ми на сервері — ставимо команду на видалення Mac-агенту
        if not Path(p_str).exists():
            queue_mac_command("delete_file", p_str)
        else:
            delete_video_file_and_associated(file_path)
            clean_orphan_anime_folders()

        FILE_REGISTRY.pop(file_key, None)
        mac_state = get_mac_device_state()
        free_gb = mac_state.get("free_disk_gb", 0.0) if mac_state else get_free_disk_space_gb()

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📂 До списку аніме", callback_data="refresh_list")]
            ]
        )

        done_text = (
            f"🗑 <b>Файл успішно видалено з ноута!</b>\n\n"
            f"📄 <s>{file_path.name}</s>\n"
            f"✨ <b>Звільнено:</b> {size_str}\n"
            f"💾 <b>Вільно на диску Mac:</b> <code>{free_gb:.1f} ГБ</code>"
        )

        if callback.message.photo:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer(done_text, reply_markup=kb)
        else:
            await callback.message.edit_text(done_text, reply_markup=kb)
        await safe_answer(callback, "✅ Файл видалено з Mac!", show_alert=False)

    except Exception as e:
        logger.error(f"Помилка видалення файлу: {e}")
        await callback.answer(f"❌ Не вдалося видалити: {e}", show_alert=True)


@dp.message(F.text.regexp(r"^(?:видалити|видали|стерти|/del)\s+(.+)$", flags=re.IGNORECASE))
async def handle_text_delete(message: Message):
    """Видалення аніме за текстовою назвою (наприклад: 'видалити ромелію' або 'видали світлячка')."""
    if not is_user_allowed(message.from_user.id):
        return

    # Отримуємо пошуковий запит
    match = re.match(r"^(?:видалити|видали|стерти|/del)\s+(.+)$", message.text.strip(), re.IGNORECASE)
    if not match:
        return

    query = match.group(1).strip().lower()
    # Прибираємо закінчення для кращого пошуку українських слів (наприклад 'ромелію' -> 'ромелі')
    stem = re.sub(r"[уюіеяа]$", "", query) if len(query) > 3 else query

    video_files = get_all_downloaded_videos()
    matched = [
        f for f in video_files
        if stem in f.name.lower() or stem in f.parent.name.lower() or query in f.name.lower()
    ]

    if not matched:
        await message.reply(
            f"🔍 За запитом <i>«{query}»</i> нічого не знайдено на диску.\n"
            "Надішліть /list, щоб побачити повний список завантажених аніме."
        )
        return

    if len(matched) == 1:
        vf = matched[0]
        file_key = register_file(vf)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="▶️ Дивитися в IINA", callback_data=f"play:{file_key}"),
                    InlineKeyboardButton(text="📂 У Finder", callback_data=f"reveal:{file_key}"),
                ],
                [
                    InlineKeyboardButton(text="🔴 Видалити з ноута", callback_data=f"confirm_del:{file_key}"),
                    InlineKeyboardButton(text="↩️ Скасувати", callback_data="refresh_list"),
                ]
            ]
        )
        await message.reply(
            f"🎯 <b>Знайдено файл:</b>\n"
            f"🎬 <b>Тайтл:</b> {vf.parent.name}\n"
            f"📄 <code>{vf.name}</code>\n\n"
            "Оберіть дію:",
            reply_markup=kb,
        )
    else:
        # Знайдено кілька серій
        keyboard_rows = []
        for vf in matched[:6]:
            file_key = register_file(vf)
            keyboard_rows.append([
                InlineKeyboardButton(
                    text=f"{vf.stem[:32]}",
                    callback_data=f"item:{file_key}"
                )
            ])
        await message.reply(
            f"🔍 <b>За запитом знайдено {len(matched)} серій:</b>\n"
            "Оберіть серію для перегляду або видалення:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
        )


@dp.callback_query(F.data == "hub_refresh")
async def cb_hub_refresh(callback: CallbackQuery, bot: Bot):
    """Оновлює віджет Watchlist Hub у чаті."""
    text, kb = build_watchlist_hub_data(callback.message.chat.id)
    if text and kb:
        try:
            await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            await safe_answer(callback, "🔄 Чергу перегляду оновлено!")
            return
        except Exception:
            pass
    await safe_answer(callback, "Черга актуальна.")


@dp.message(F.video | F.document)
async def handle_media_file(message: Message, bot: Bot):
    if not is_user_allowed(message.from_user.id):
        return

    video = message.video
    doc = message.document

    thumb_file_id = None
    if doc:
        filename = doc.file_name or "video.mkv"
        mime = (doc.mime_type or "").lower()
        is_video_doc = (
            mime.startswith("video/")
            or filename.lower().endswith((".mkv", ".mp4", ".webm", ".avi", ".mov", ".ts", ".flv"))
        )
        if not is_video_doc:
            return await handle_anime_message(message, bot)
        total_bytes = doc.file_size or 0
        file_id = doc.file_id
        if doc.thumbnail:
            thumb_file_id = doc.thumbnail.file_id
    elif video:
        filename = video.file_name or "episode.mp4"
        total_bytes = video.file_size or 0
        file_id = video.file_id
        if video.thumbnail:
            thumb_file_id = video.thumbnail.file_id
    else:
        return

    file_size_mb = total_bytes / (1024 * 1024)

    # Витягуємо назву тайтлу з підпису або з назви файлу
    _, caption_title = extract_links_and_title(message)
    if not caption_title and filename:
        clean_stem = re.sub(r"\[.*?\]|\(.*?\)", "", Path(filename).stem).strip()
        if clean_stem:
            caption_title = clean_stem

    raw_caption = (message.caption or message.text or "").strip()
    first_caption_line = raw_caption.splitlines()[0].strip() if raw_caption else ""

    # Визначаємо номер серії, якість та доріжки з підпису або імені файлу
    meta_src = f"{first_caption_line} {filename}"
    ep_m = re.search(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]|(\d{1,3})\s*сері", meta_src, re.IGNORECASE)
    ep_num = int(ep_m.group(1) or ep_m.group(2)) if ep_m else 1

    meta_low = meta_src.lower()
    quality_str = "4K Ultra HD" if ("2160p" in meta_low or "4k" in meta_low) else ("1080p Full HD" if "1080p" in meta_low else "1080p Full HD")
    tracks_str = detect_tracks_label(meta_src, caption_title or "", message.chat.id)

    # Якщо Telegram не передав оригінальне ім'я файлу (наприклад episode.mp4), формуємо інформативну назву з підпису
    if filename in ("episode.mp4", "video.mkv") and first_caption_line:
        ext = Path(filename).suffix or ".mp4"
        safe_line = re.sub(r"[^\w\s\-\.\,\(\)\[\]]", "", first_caption_line, flags=re.UNICODE).strip()[:110]
        if safe_line:
            filename = f"{safe_line}{ext}"

    clean_title = sanitize_folder_name(caption_title) if caption_title else "Telegram_Anime"
    target_folder = DOWNLOADS_DIR / clean_title
    target_folder.mkdir(parents=True, exist_ok=True)

    dest_path = target_folder / filename

    # Якщо бот працює на сервері Linux (Railway) — передаємо завантаження важкого Telegram-файлу напряму на Mac через MTProto!
    if platform.system() != "Darwin":
        status_msg = await message.reply(
            f"📥 <b>Завантаження відео з Telegram (MTProto)</b>\n\n"
            f"🎬 <b>Аніме:</b> {clean_title} [Серія {ep_num}]\n"
            f"📄 <b>Файл:</b> <code>{filename}</code>\n"
            f"📂 <b>Папка:</b> <code>{target_folder.name}</code>\n\n"
            f"<code>[▱▱▱▱▱▱▱▱▱▱] 0.0%</code>\n"
            f"📦 <b>0.0 МБ / {file_size_mb:.1f} МБ</b>\n"
            f"🔄 <i>Статус: З'єднання з MacAgent (очікування ~2-5 сек)...</i>"
        )
        cmd_payload = json.dumps({
            "chat_id": message.chat.id,
            "source_msg_id": message.message_id,
            "msg_id": status_msg.message_id,
            "title": clean_title,
            "folder": target_folder.name,
            "filename": filename,
            "episode": ep_num,
            "quality": quality_str,
            "dub_sub": tracks_str,
        })
        queue_mac_command("download_tg_media", cmd_payload)
        try:
            await ask_source_for_media(bot, message.chat.id, caption_title or clean_title)
        except Exception as e_ask:
            logger.debug(f"ask_source_for_media: {e_ask}")
        return

    # Зберігаємо оригінальне прев'ю з повідомлення Telegram (thumb)
    if thumb_file_id:
        try:
            ep_thumb = dest_path.with_name(f"{dest_path.stem}.thumb.jpg")
            await bot.download(thumb_file_id, destination=ep_thumb)
            poster_path = target_folder / "poster.jpg"
            if not poster_path.exists():
                shutil.copy2(ep_thumb, poster_path)
        except Exception as e:
            logger.debug(f"Не вдалося зберегти прев'ю з Telegram: {e}")

    # Захист від дублікатів: якщо серія вже є на Mac
    if dest_path.exists() and dest_path.stat().st_size > 0:
        file_key = register_file(dest_path)
        kb = build_downloaded_action_keyboard(file_key)
        size_str = format_bytes(dest_path.stat().st_size)
        await message.reply(
            f"⚡️ <b>Ця серія вже є на вашому Mac!</b>\n\n"
            f"🎬 <b>Тайтл:</b> {clean_title}\n"
            f"📄 <b>Файл:</b> <code>{filename}</code>\n"
            f"🏷 <b>Тег macOS Finder:</b> 🟢 <b>{TAG_NAME}</b>\n"
            f"📂 <b>Шлях:</b> <code>{dest_path}</code>\n\n"
            "▶️ <i>Можете одразу дивитися або відкрити в 1 клік:</i>",
            reply_markup=kb,
        )
        return

    status_msg = await message.reply(f"🚀 Починаю завантаження відео з Telegram ({file_size_mb:.1f} МБ)...")

    # Якщо доступний MTProto (Telethon) — завантажуємо без будь-яких обмежень розміру Telegram у 20 МБ!
    if mtproto_downloader:
        tracker = DownloadProgressTracker(interval_seconds=2.2)
        cancel_id = uuid.uuid4().hex[:8]
        cancel_kb = build_cancel_keyboard(cancel_id)

        header_text = (
            f"🚀 <b>Завантаження відео з Telegram (MTProto)</b>\n\n"
            f"🎬 <b>Аніме:</b> {clean_title}\n"
            f"📄 <b>Файл:</b> <code>{filename}</code>\n"
            f"📂 <b>Папка:</b> <code>{target_folder.name}</code>\n\n"
        )

        async def on_progress(downloaded: int, total: int):
            if tracker.should_update_telegram():
                speed, eta = tracker.calculate_stats(downloaded, total)
                bar_text = render_progress_bar(downloaded, total, speed, eta)
                try:
                    await status_msg.edit_text(header_text + bar_text, reply_markup=cancel_kb)
                except Exception:
                    pass

        async def _do_mtproto_dl():
            return await mtproto_downloader.download_message_video(
                message.chat.id,
                message.message_id,
                dest_path,
                progress_callback=on_progress,
            )

        dl_task = asyncio.create_task(_do_mtproto_dl())
        ACTIVE_DOWNLOAD_TASKS[cancel_id] = dl_task

        async with DOWNLOAD_SEMAPHORE:
            start_dl_time = time.time()
            try:
                await dl_task

                duration = max(1.0, time.time() - start_dl_time)
                avg_speed = total_bytes / duration if total_bytes > 0 else 0
                free_gb = get_free_disk_space_gb()

                set_macos_tag(dest_path, TAG_NAME, TAG_COLOR)
                set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)

                file_key = register_file(dest_path)
                kb = build_downloaded_action_keyboard(file_key)

                await notify_download_finished(
                    bot,
                    message.chat.id,
                    status_msg,
                    dest_path,
                    clean_title,
                    prefix_text="🎉 <b>Готово! Telegram-серія успішно завантажена на ноут!</b>"
                )
                try:
                    await ask_source_for_media(bot, message.chat.id, caption_title or clean_title)
                except Exception as e_ask:
                    logger.debug(f"ask_source_for_media: {e_ask}")
                return
            except asyncio.CancelledError:
                logger.info(f"Завантаження {filename} скасовано користувачем.")
                cleanup_cancelled_download(dest_path, target_folder)
                free_gb = get_free_disk_space_gb()
                await status_msg.edit_text(
                    "🛑 <b>Завантаження скасовано користувачем!</b>\n\n"
                    f"🎬 <b>Тайтл:</b> {clean_title}\n"
                    f"📄 <code>{filename}</code>\n\n"
                    "🗑 <i>Усі тимчасові файли видалено з Mac.</i>\n"
                    f"💾 <b>Вільно на диску:</b> <code>{free_gb:.1f} ГБ</code>"
                )
                return
            except Exception as e:
                logger.error(f"Помилка завантаження через MTProto: {e}")
                await status_msg.edit_text(f"❌ Помилка завантаження з Telegram: {e}")
                return
            finally:
                ACTIVE_DOWNLOAD_TASKS.pop(cancel_id, None)

    # Fallback на Bot API для маленьких файлів
    if file_size_mb > 20:
        await status_msg.edit_text(
            f"⚠️ <b>Файл завеликий для завантаження через Telegram Bot API</b> ({file_size_mb:.1f} МБ > 20 МБ).\n\n"
            "💡 Налаштуйте TELEGRAM_API_ID у .env для зняття лімітів."
        )
        return

    try:
        file_info = await bot.get_file(file_id)
        await bot.download_file(file_info.file_path, dest_path)
        set_macos_tag(dest_path, TAG_NAME, TAG_COLOR)
        set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)
        file_key = register_file(dest_path)
        kb = build_downloaded_action_keyboard(file_key)
        await notify_download_finished(
            bot,
            message.chat.id,
            status_msg,
            dest_path,
            clean_title,
            prefix_text="✅ <b>Відео успішно завантажено на Mac!</b>"
        )
        try:
            await ask_source_for_media(bot, message.chat.id, caption_title or clean_title)
        except Exception as e_ask:
            logger.debug(f"ask_source_for_media: {e_ask}")
    except Exception as e:
        await status_msg.edit_text(f"❌ Помилка: {e}")


PENDING_TRACK_SOURCES: Dict[str, Dict[str, Optional[str]]] = {}


def _normalize_anime_title(raw: str) -> str:
    """Прибирає дужки, номери сезонів та технічні теги — лишає чисту назву тайтлу."""
    t = re.sub(r"\(.*?\)|\[.*?\]", "", raw or "").strip()
    t = re.sub(r"\b(\d+|[IVXLCDM]+)\s*(?:сезон)?$", "", t, flags=re.IGNORECASE).strip()
    return re.sub(r"\s+", " ", t).strip() or (raw or "Аніме").strip()


async def offer_tracking(bot: Bot, chat_id: int, raw_title: str,
                         fex_url: Optional[str] = None, thread_url: Optional[str] = None):
    """Після завантаження пропонує одним повідомленням: Стежити за оновленнями? [Так] / [Ні], тільки якщо тайтлу ще немає в моніторингу."""
    try:
        if not get_user_settings(chat_id).get("seeded"):
            seed_subscriptions(chat_id)
            set_user_setting(chat_id, "seeded", 1)
    except Exception:
        pass

    title = _normalize_anime_title(raw_title)
    existing_sub = get_subscription_by_title(chat_id, title) or get_subscription_by_title(chat_id, raw_title)
    if existing_sub:
        # Вже стежимо — лише тихо доповнюємо джерела, якщо їх не було, і не питаємо повторно!
        set_subscription_sources(chat_id, existing_sub["title"], fex_url=fex_url, thread_url=thread_url)
        return
    t_key = register_track_title(title)
    PENDING_TRACK_SOURCES[t_key] = {"fex": fex_url, "thread": thread_url}
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Так, стежити", callback_data=f"fw_track_yes:{t_key}"),
        InlineKeyboardButton(text="Ні, разова серія", callback_data=f"fw_track_no:{t_key}"),
    ]])
    await bot.send_message(
        chat_id,
        f"🔔 <b>Стежити за новими серіями «{title}»?</b>\n"
        "<i>Бот сам повідомлятиме та завантажуватиме нові епізоди.</i>",
        reply_markup=kb,
    )


@dp.callback_query(F.data.startswith("fw_track_yes:"))
async def cb_fw_track_yes(callback: CallbackQuery):
    t_key = callback.data.split(":")[1]
    title = resolve_track_title(t_key)
    if not title:
        await safe_answer(callback, "⚠️ Тайтл не знайдено", show_alert=True)
        return
    src = PENDING_TRACK_SOURCES.pop(t_key, {})
    chat_id = callback.message.chat.id
    pref = detect_pref_from_text(title) or ("dub" if src.get("fex") else "sub")
    add_subscription(chat_id, title, channel="ridnyiholos", pref_type=pref)
    set_subscription_sources(chat_id, title, fex_url=src.get("fex"), thread_url=src.get("thread"))
    try:
        await callback.message.edit_text(
            f"✅ <b>«{title}»</b> додано в моніторинг.\n<i>Керувати: 🛰 Моніторинг.</i>"
        )
    except Exception:
        pass
    await safe_answer(callback, "✅ Додано в моніторинг")


@dp.callback_query(F.data.startswith("fw_track_no:"))
async def cb_fw_track_no(callback: CallbackQuery):
    t_key = callback.data.split(":")[1]
    PENDING_TRACK_SOURCES.pop(t_key, None)
    try:
        await callback.message.edit_text("👌 Гаразд, це разове завантаження.")
    except Exception:
        pass
    await safe_answer(callback)


PENDING_SOURCE_INPUT: Dict[int, str] = {}  # chat_id -> t_key тайтлу, для якого чекаємо посилання


async def ask_source_for_media(bot: Bot, chat_id: int, raw_title: str):
    """Після збереження відеофайлу без посилання: пропонує вказати гілку/роздачу для авто-стеження, тільки якщо тайтлу ще немає в моніторингу."""
    try:
        if not get_user_settings(chat_id).get("seeded"):
            seed_subscriptions(chat_id)
            set_user_setting(chat_id, "seeded", 1)
    except Exception:
        pass

    title = _normalize_anime_title(raw_title)
    if title.lower() == "telegram_anime":
        return
    sub = get_subscription_by_title(chat_id, title) or get_subscription_by_title(chat_id, raw_title)
    if sub:
        return  # Тайтл уже в списку моніторингу — не пропонуємо повторно!
    t_key = register_track_title(title)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔗 Вказати посилання", callback_data=f"src_link:{t_key}"),
        InlineKeyboardButton(text="Ні, разова серія", callback_data=f"src_no:{t_key}"),
    ]])
    await bot.send_message(
        chat_id,
        f"🔔 <b>Стежити за новими серіями «{title}»?</b>\n"
        "<i>Якщо так — надішліть посилання на гілку медіатеки або на FEX-роздачу цього аніме.</i>",
        reply_markup=kb,
    )


@dp.callback_query(F.data.startswith("src_link:"))
async def cb_src_link(callback: CallbackQuery):
    t_key = callback.data.split(":")[1]
    title = resolve_track_title(t_key)
    if not title:
        await safe_answer(callback, "⚠️ Тайтл не знайдено", show_alert=True)
        return
    PENDING_SOURCE_INPUT[callback.message.chat.id] = t_key
    try:
        await callback.message.edit_text(
            f"🔗 <b>«{title}»</b>\nНадішліть наступним повідомленням посилання "
            "(FEX.NET або гілка @mediareser). Скасувати — просто напишіть «ні»."
        )
    except Exception:
        pass
    await safe_answer(callback)


@dp.callback_query(F.data.startswith("src_no:"))
async def cb_src_no(callback: CallbackQuery):
    PENDING_SOURCE_INPUT.pop(callback.message.chat.id, None)
    try:
        await callback.message.edit_text("👌 Гаразд, це разове завантаження.")
    except Exception:
        pass
    await safe_answer(callback)


async def try_consume_source_link(message: Message) -> bool:
    """Якщо чекаємо посилання-джерело — обробляє повідомлення. True, якщо воно спожите."""
    chat_id = message.chat.id
    t_key = PENDING_SOURCE_INPUT.get(chat_id)
    if not t_key:
        return False
    text = (message.text or "").strip()
    if text.lower() in ("ні", "нет", "no", "cancel", "скасувати"):
        PENDING_SOURCE_INPUT.pop(chat_id, None)
        await message.reply("👌 Гаразд, без стеження.")
        return True
    m = re.search(r"https?://\S+", text)
    if not m or text != m.group(0):
        PENDING_SOURCE_INPUT.pop(chat_id, None)  # це не посилання — скидаємо очікування, звичайна обробка
        return False
    url = m.group(0)
    title = resolve_track_title(t_key)
    if not title:
        PENDING_SOURCE_INPUT.pop(chat_id, None)
        return False
    fex_url = url if FexDownloader.extract_share_key(url) else None
    thread_url = url if (not fex_url and ("t.me/" in url)) else None
    if not fex_url and not thread_url:
        await message.reply("⚠️ Потрібне посилання на FEX.NET або гілку Telegram (t.me/...). Спробуйте ще раз або напишіть «ні».")
        return True
    PENDING_SOURCE_INPUT.pop(chat_id, None)
    pref = detect_pref_from_text(title) or ("sub" if (thread_url and not fex_url) else "dub")
    add_subscription(chat_id, title, channel="ridnyiholos", pref_type=pref)
    set_subscription_sources(chat_id, title, fex_url=fex_url, thread_url=thread_url)
    kind = "4K FEX-роздачу" if fex_url else "гілку медіатеки"
    await message.reply(f"✅ Додано в моніторинг: <b>{title}</b>\n<i>Джерело: {kind}. Керувати — 🛰 Моніторинг.</i>")
    return True


@dp.message(F.text | F.caption | F.photo)
async def handle_anime_message(message: Message, bot: Bot):
    if not is_user_allowed(message.from_user.id):
        return

    # Очікуємо посилання-джерело для стеження після разового файлу?
    if await try_consume_source_link(message):
        return

    # Перехоплення для Notion: ідея, думка, Бах
    raw_text = message.text or message.caption or ""
    task_info = parse_task_prefix(raw_text, project="⛩️ Anime Downloader")
    if task_info:
        media_urls = []
        if message.photo:
            try:
                f_info = await bot.get_file(message.photo[-1].file_id)
                if f_info.file_path:
                    media_urls.append(f"https://api.telegram.org/file/bot{bot.token}/{f_info.file_path}")
            except Exception as e_ph:
                logger.warning(f"Не вдалося отримати URL фото: {e_ph}")

        try:
            send_task(
                name=task_info["title"],
                status="Inbox",
                task_type=task_info["type"],
                project="⛩️ Anime Downloader",
                rice_score=task_info.get("rice_score"),
                description=task_info.get("description"),
                media_urls=media_urls if media_urls else None
            )
            type_icon = "🐛" if task_info["type"] == "Bug" else ("💡" if task_info["type"] == "Idea" else "⚙️")
            priority_badge = task_info.get("priority_badge", "🟡 Середній")
            rice = task_info.get("rice_score", 50.0)
            reason = task_info.get("reason", "")
            media_badge = " 📸 (з фото)" if media_urls else ""

            reply_text = (
                f"⚡️ <b>Завдання надіслано в Notion!</b>\n\n"
                f"📌 <b>Назва:</b> <code>{task_info['title']}</code>\n"
                f"🏷 <b>Категорія:</b> {type_icon} <b>{task_info['type']}</b>{media_badge}\n"
                f"📊 <b>Пріоритет:</b> {priority_badge} | <b>RICE:</b> <code>{rice}</code>\n"
                f"🎯 <b>Оцінка:</b> <i>{reason}</i>\n\n"
                f"📥 <i>Передано в Inbox проєкту ⛩️ Anime Downloader на розгляд розробнику.</i>"
            )
            await message.reply(reply_text, parse_mode="HTML")
            return
        except Exception as e:
            logger.error(f"Помилка додавання таски в Notion: {e}")
            await message.reply(f"❌ Помилка додавання завдання в Notion: {e}")
            return

    # Запис у чергу відкладених завдань
    task_id = enqueue_offline_task(message.chat.id, message.message_id, "", raw_text)

    links, title = extract_links_and_title(message)

    clean_t = sanitize_folder_name(title) if title else "Аніме"
    target_folder = DOWNLOADS_DIR / clean_t
    target_folder.mkdir(parents=True, exist_ok=True)

    # Якщо пост містить фото (офіційний постер аніме), одразу зберігаємо його в папку тайтлу!
    if message.photo:
        try:
            poster_path = target_folder / "poster.jpg"
            await bot.download(message.photo[-1].file_id, destination=poster_path)
            logger.info(f"Збережено оригінальний постер аніме з Telegram -> {poster_path}")
        except Exception as e:
            logger.debug(f"Не вдалося зберегти постер: {e}")

    if not links:
        mark_offline_task(task_id, "no_links")

        # Інтелектуальний пошук тайтлу в сезонному каталозі каналу @ridnyiholos
        search_query = (title or raw_text).split("/")[0].strip()
        matched_cat_item = None
        if len(search_query) >= 3:
            catalog = await fetch_ongoings_catalog("ridnyiholos")
            q_low = search_query.lower()
            q_words = [w for w in re.split(r"[\s,\.\—\-\:\/]+", q_low) if len(w) >= 3]

            for c_title, c_data in catalog.items():
                c_low = c_title.lower()
                if q_low in c_low or c_low in q_low:
                    matched_cat_item = c_data
                    break
                if q_words and any(w in c_low for w in q_words):
                    matched_cat_item = c_data
                    break

        if matched_cat_item:
            card_text, card_kb = await build_anime_card(message.chat.id, matched_cat_item["title"])
            ask_reply = (
                f"🎯 <b>Знайдено в каталозі онґоїнгів: «{matched_cat_item['title']}»!</b>\n\n"
                f"{card_text}"
            )
            await message.reply(ask_reply, reply_markup=card_kb)
            return

        await message.reply(
            "🤔 У цьому повідомленні не знайдено посилань або відео.\n\n"
            "💡 <b>Як завантажити чи підписатися:</b>\n"
            "• Надішліть назву аніме (наприклад, <i>«Маг, що повернувся»</i>) — бот знайде його в онґоїнгах сезону та запропонує завантажити чи стежити.\n"
            "• Або надішліть пост із каналу з кнопкою 📥4K/1080 або лінком на FEX.NET."
        )
        return

    # Сортуємо кандидатів: пріоритет 4K / 1080p / FEX > Онлайн плеєри
    sorted_candidates = score_and_sort_links(links)
    if not sorted_candidates:
        mark_offline_task(task_id, "no_valid_links")
        await message.reply("🤔 Усі посилання в пості належать до донатів або соцмереж. Посилань на серію не знайдено.")
        return

    # Розгортаємо редиректи
    resolved_candidates = []
    for lbl, u in sorted_candidates:
        real_u = await resolve_redirect_url(u)
        resolved_candidates.append((lbl, real_u))
    sorted_candidates = sorted(resolved_candidates, key=lambda item: score_link(item[0], item[1]), reverse=True)

    # Якщо користувач надіслав назву гілки та пряме посилання на гілку медіатеки (наприклад: "[РГ SUB] Ао Аші (2 сезон)\nhttps://t.me/c/mediareser/12051")
    has_any_fex = any(FexDownloader.extract_share_key(u) for _, u in sorted_candidates)
    mediareser_urls = [u for _, u in sorted_candidates if "mediareser" in u.lower()]
    if not has_any_fex and mediareser_urls and title and not message.video and not message.document:
        m_url = normalize_telegram_url(mediareser_urls[0])
        detected_pref = detect_pref_from_text(raw_text) or detect_pref_from_text(title) or "sub"
        clean_sub_title = re.sub(r"\(\d+\s*сезон\)", "", title, flags=re.IGNORECASE).strip() or title
        existing_sub = get_subscription_by_title(message.chat.id, clean_sub_title)
        target_sub_title = existing_sub["title"] if existing_sub else clean_sub_title
        add_subscription(message.chat.id, target_sub_title, channel="ridnyiholos", pref_type=detected_pref)
        set_subscription_sources(message.chat.id, target_sub_title, thread_url=m_url)
        mark_offline_task(task_id, "completed")
        card_text, card_kb = await build_anime_card(message.chat.id, target_sub_title)
        pref_badge = "📝 Субтитри (SUB)" if detected_pref == "sub" else "🎙 Озвучення (DUB)"
        await message.reply(
            f"✅ <b>Гілку медіатеки прив'язано до «{target_sub_title}»!</b>\n"
            f"🏷 <b>Режим:</b> {pref_badge}\n"
            f"🔗 <b>Гілка:</b> <a href=\"{m_url}\">{m_url}</a>\n\n"
            f"{card_text}",
            reply_markup=card_kb
        )
        return

    status_msg = await message.reply("🔍 Аналізую джерела завантаження (пріоритет: 4K / 1080p)...")

    # Ланцюжок спроб (failover loop)
    download_success = False
    errors_list = []

    for label, candidate_url in sorted_candidates:
        share_key = FexDownloader.extract_share_key(candidate_url)
        # 1. СПРОБА FEX.NET (4K / 1080p)
        if share_key:
            try:
                await status_msg.edit_text(f"🔍 Перевіряю файли на FEX.NET (<code>{share_key}</code>)...")
                files = await FexDownloader.get_files_in_share(share_key)
                if not files:
                    errors_list.append(f"FEX ({share_key}): файли відсутні або посилання застаріло")
                    continue

                # Пошук потрібної серії
                ep_match = re.search(r"\[(\d+)\s*з|(\d+)\s*сері", message.text or message.caption or "", re.IGNORECASE)
                matching_files = files
                if ep_match:
                    ep_num = int(ep_match.group(1) or ep_match.group(2))
                    ep_patterns = [f"[{ep_num:02d}", f"[{ep_num}", f" {ep_num:02d} ", f" {ep_num} "]
                    candidates = [f for f in files if any(p in f.get("name", "") for p in ep_patterns)]
                    if candidates:
                        matching_files = candidates

                preferred_4k = [f for f in matching_files if any(q in f.get("name", "").lower() for q in ["2160p", "4k"])]
                preferred_1080p = [f for f in matching_files if "1080p" in f.get("name", "").lower()]

                if preferred_4k:
                    target_file = preferred_4k[0]
                elif preferred_1080p:
                    target_file = preferred_1080p[0]
                else:
                    target_file = matching_files[0]

                file_name = target_file.get("name") or "episode.mp4"
                download_url = target_file.get("download_url")
                token = target_file.get("token")
                total_bytes = target_file.get("size") or 0

                if not download_url:
                    errors_list.append(f"FEX ({file_name}): відсутній direct download URL")
                    continue

                dest_file = target_folder / file_name

                # Захист від дублікатів
                if dest_file.exists() and dest_file.stat().st_size > 0 and (total_bytes == 0 or dest_file.stat().st_size == total_bytes):
                    file_key = register_file(dest_file)
                    kb = build_downloaded_action_keyboard(file_key)
                    size_str = format_bytes(dest_file.stat().st_size)
                    await status_msg.edit_text(
                        f"⚡️ <b>Ця серія з FEX.NET уже є на вашому Mac!</b>\n\n"
                        f"🎬 <b>Тайтл:</b> {clean_t}\n"
                        f"📄 <b>Файл:</b> <code>{file_name}</code>\n"
                        f"🏷 <b>Тег macOS Finder:</b> 🟢 <b>{TAG_NAME}</b>\n"
                        f"📂 <b>Шлях:</b> <code>{dest_file}</code>\n\n"
                        "▶️ <i>Можете одразу дивитися або відкрити в 1 клік:</i>",
                        reply_markup=kb,
                    )
                    mark_offline_task(task_id, "already_downloaded")
                    return

                header_text = (
                    f"🚀 <b>Завантаження з FEX.NET (4K / 1080p)</b>\n\n"
                    f"🎬 <b>Аніме:</b> {clean_t}\n"
                    f"📄 <b>Файл:</b> <code>{file_name}</code>\n"
                    f"📂 <b>Папка:</b> <code>{target_folder.name}</code>\n\n"
                )

                tracker = DownloadProgressTracker(interval_seconds=2.2)
                cancel_id = uuid.uuid4().hex[:8]
                cancel_kb = build_cancel_keyboard(cancel_id)

                async def on_progress(downloaded: int, total: int):
                    if tracker.should_update_telegram():
                        speed, eta = tracker.calculate_stats(downloaded, total)
                        bar_text = render_progress_bar(downloaded, total, speed, eta)
                        full_text = header_text + bar_text
                        try:
                            await status_msg.edit_text(full_text, reply_markup=cancel_kb)
                        except Exception:
                            pass

                # Якщо ми на віддаленому сервері Linux (Railway) -> перенаправляємо завантаження на локальний Mac!
                if platform.system() != "Darwin":
                    q_str = "4K Ultra HD" if "2160p" in file_name else ("1080p Full HD" if "1080p" in file_name else "Full HD")
                    t_str = detect_tracks_label(file_name, title or clean_t, message.chat.id)
                    cmd_payload = json.dumps({
                        "url": download_url,
                        "token": token,
                        "title": title or clean_t,
                        "episode": ep_num or 1,
                        "folder": target_folder.name,
                        "filename": file_name,
                        "chat_id": message.chat.id,
                        "msg_id": status_msg.message_id,
                        "quality": q_str,
                        "dub_sub": t_str
                    })
                    queue_mac_command("download_fex", cmd_payload)
                    t_mb = (total_bytes / (1024 * 1024)) if total_bytes else 0
                    size_line = f"📦 <b>0.0 МБ / {t_mb:.1f} МБ</b>\n" if t_mb else ""
                    await status_msg.edit_text(
                        f"📥 <b>Завантаження з FEX.NET на Mac SSD</b>\n\n"
                        f"🎬 <b>Аніме:</b> {clean_t} [Серія {ep_num or 1}]\n"
                        f"📄 <b>Файл:</b> <code>{file_name}</code>\n"
                        f"📂 <b>Папка:</b> <code>{target_folder.name}</code>\n\n"
                        f"<code>[▱▱▱▱▱▱▱▱▱▱] 0.0%</code>\n"
                        f"{size_line}"
                        f"🔄 <i>Статус: Передача завдання на MacAgent (очікування ~2-5 сек)...</i>"
                    )
                    mark_offline_task(task_id, "completed")
                    download_success = True
                    try:
                        await offer_tracking(bot, message.chat.id, title or clean_t, fex_url=candidate_url)
                    except Exception as e_off:
                        logger.debug(f"offer_tracking: {e_off}")
                    return

                async def _do_fex_dl():
                    return await FexDownloader.download_file(
                        download_url,
                        dest_file,
                        token=token,
                        progress_callback=on_progress
                    )

                dl_task = asyncio.create_task(_do_fex_dl())
                ACTIVE_DOWNLOAD_TASKS[cancel_id] = dl_task

                async with DOWNLOAD_SEMAPHORE:
                    try:
                        await dl_task
                        set_macos_tag(dest_file, TAG_NAME, TAG_COLOR)
                        set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)
                        file_key = register_file(dest_file)
                        await notify_download_finished(
                            bot,
                            message.chat.id,
                            status_msg,
                            dest_file,
                            clean_t,
                            prefix_text="🎉 <b>Готово! 4K/1080p серія успішно завантажена на ноут!</b>"
                        )
                        mark_offline_task(task_id, "completed")
                        download_success = True
                        try:
                            await offer_tracking(bot, message.chat.id, title or clean_t, fex_url=candidate_url)
                        except Exception as e_off:
                            logger.debug(f"offer_tracking: {e_off}")
                        return
                    except asyncio.CancelledError:
                        logger.info(f"Завантаження {file_name} скасовано.")
                        cleanup_cancelled_download(dest_file, target_folder)
                        free_gb = get_free_disk_space_gb()
                        await status_msg.edit_text(
                            "🛑 <b>Завантаження скасовано користувачем!</b>\n\n"
                            f"🎬 <b>Тайтл:</b> {clean_t}\n"
                            f"📄 <code>{file_name}</code>\n\n"
                            f"💾 <b>Вільно на диску:</b> <code>{free_gb:.1f} ГБ</code>"
                        )
                        mark_offline_task(task_id, "cancelled")
                        return
                    except Exception as ex_fex:
                        logger.warning(f"FEX download failed: {ex_fex}")
                        errors_list.append(f"FEX: {ex_fex}")
                        continue
                    finally:
                        ACTIVE_DOWNLOAD_TASKS.pop(cancel_id, None)

            except Exception as e:
                errors_list.append(f"FEX: {e}")
                continue

        # 2. СПРОБА TELEGRAM ПОСИЛАННЯ (t.me/...)
        tme_match = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)/(\d+)", candidate_url)
        if tme_match:
            channel_name = tme_match.group(1)
            post_id = int(tme_match.group(2))
            dest_file = target_folder / f"{target_folder.name}_post_{post_id}.mp4"

            if dest_file.exists() and dest_file.stat().st_size > 0:
                file_key = register_file(dest_file)
                kb = build_downloaded_action_keyboard(file_key)
                size_str = format_bytes(dest_file.stat().st_size)
                await status_msg.edit_text(
                    f"⚡️ <b>Ця серія з Telegram уже завантажена на ваш Mac!</b>\n\n"
                    f"🎬 <b>Тайтл:</b> {clean_t}\n"
                    f"📄 <b>Файл:</b> <code>{dest_file.name}</code>\n"
                    f"🏷 <b>Тег macOS Finder:</b> 🟢 <b>{TAG_NAME}</b>\n"
                    f"📂 <b>Шлях:</b> <code>{dest_file}</code>\n\n"
                    "▶️ <i>Можете одразу дивитися або відкрити в 1 клік:</i>",
                    reply_markup=kb,
                )
                mark_offline_task(task_id, "already_downloaded")
                return

            if mtproto_downloader:
                try:
                    await status_msg.edit_text(f"📲 Спробую отримати відео з Telegram ({channel_name}/{post_id})...")
                    tracker = DownloadProgressTracker(interval_seconds=2.2)
                    cancel_id = uuid.uuid4().hex[:8]
                    cancel_kb = build_cancel_keyboard(cancel_id)
                    header_text = (
                        f"🚀 <b>Завантаження з Telegram ({channel_name})</b>\n\n"
                        f"🎬 <b>Аніме:</b> {clean_t}\n"
                        f"📂 <b>Папка:</b> <code>{target_folder.name}</code>\n\n"
                    )

                    async def on_tg_prog(downloaded: int, total: int):
                        if tracker.should_update_telegram():
                            speed, eta = tracker.calculate_stats(downloaded, total)
                            bar_text = render_progress_bar(downloaded, total, speed, eta)
                            try:
                                await status_msg.edit_text(header_text + bar_text, reply_markup=cancel_kb)
                            except Exception:
                                pass

                    async def _do_tg_post_dl():
                        return await mtproto_downloader.download_by_link(
                            channel_name, post_id, dest_file, progress_callback=on_tg_prog
                        )

                    dl_task = asyncio.create_task(_do_tg_post_dl())
                    ACTIVE_DOWNLOAD_TASKS[cancel_id] = dl_task

                    async with DOWNLOAD_SEMAPHORE:
                        try:
                            await dl_task
                            set_macos_tag(dest_file, TAG_NAME, TAG_COLOR)
                            set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)
                            file_key = register_file(dest_file)
                            await notify_download_finished(
                                bot,
                                message.chat.id,
                                status_msg,
                                dest_file,
                                clean_t,
                                prefix_text="🎉 <b>Готово! Telegram-серія успішно завантажена на ноут!</b>"
                            )
                            mark_offline_task(task_id, "completed")
                            download_success = True
                            try:
                                await offer_tracking(bot, message.chat.id, title or clean_t, thread_url=candidate_url)
                            except Exception as e_off:
                                logger.debug(f"offer_tracking: {e_off}")
                            return
                        except Exception as e_tg:
                            logger.info(f"MTProto download error: {e_tg}")
                            errors_list.append(f"Telegram ({channel_name}): {e_tg}")
                        finally:
                            ACTIVE_DOWNLOAD_TASKS.pop(cancel_id, None)
                except Exception as ex_tme:
                    errors_list.append(f"Telegram: {ex_tme}")

        # 3. СПРОБА ОНЛАЙН ПЛЕЄРА / ВЕБ-САЙТУ (yt-dlp)
        try:
            await status_msg.edit_text(f"🌐 Спробую завантажити потокове відео: <code>{candidate_url}</code>...")
            cancel_id = uuid.uuid4().hex[:8]
            cancel_kb = build_cancel_keyboard(cancel_id)

            async def _do_web_dl():
                return await download_web_video(candidate_url, target_folder)

            dl_task = asyncio.create_task(_do_web_dl())
            ACTIVE_DOWNLOAD_TASKS[cancel_id] = dl_task

            async with DOWNLOAD_SEMAPHORE:
                try:
                    ok, result_path, extracted_title = await dl_task
                    if ok and result_path:
                        p = Path(result_path)
                        set_macos_tag(p, TAG_NAME, TAG_COLOR)
                        set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)
                        file_key = register_file(p)
                        await notify_download_finished(
                            bot,
                            message.chat.id,
                            status_msg,
                            p,
                            extracted_title or clean_t or p.name,
                            prefix_text="🎉 <b>Онлайн-серію успішно завантажено!</b>"
                        )
                        mark_offline_task(task_id, "completed")
                        download_success = True
                        return
                    else:
                        errors_list.append(f"Веб-плеєр: {result_path}")
                except Exception as ex_web:
                    errors_list.append(f"Веб-плеєр: {ex_web}")
                finally:
                    ACTIVE_DOWNLOAD_TASKS.pop(cancel_id, None)
        except Exception as e_w:
            errors_list.append(f"Веб-плеєр: {e_w}")

    # Якщо всі кандидати не спрацювали
    mark_offline_task(task_id, "failed")
    err_summary = "\n".join(f"• {e}" for e in errors_list[:3])
    await status_msg.edit_text(
        f"❌ <b>Не вдалося автоматично завантажити відео з наданих посилань.</b>\n\n"
        f"Деталі спроб:\n{err_summary}\n\n"
        "💡 <b>Порада:</b> Якщо посилання веде на закриту медіатеку або плеєр із захистом, "
        "просто <b>Перешліть (Forward)</b> саме відео або файл сюди ботові — він миттєво збереже його на Mac без лімітів! 🚀"
    )


# --- ФОНОВІ ЗАВДАННЯ: WATCHDOG ТА МОНІТОРИНГ КАНАЛІВ ---

async def mac_sleep_watchdog(bot: Bot):
    """
    Фоновий монітор сну та активності macOS:
    1. Виявляє вихід Mac зі сну (delta часу > 18 секунд при перевірці кожні 5 сек).
    2. Сповіщає про пробудження ноута та перевіряє чергу відкладених завантажень.
    3. Перевіряє заплановані вечірні нагадування (19:30).
    """
    last_tick = time.time()
    while True:
        try:
            await asyncio.sleep(5)
            now = time.time()
            gap = now - last_tick
            last_tick = now

            if gap > 18.0:
                logger.info(f"Виявлено пробудження Mac після сну (пауза {gap:.1f} сек).")
                pending = get_pending_offline_tasks()
                if pending and ALLOWED_USERS:
                    user_id = ALLOWED_USERS[0]
                    count = len(pending)
                    await bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"👋 <b>Ноутбук увімкнено / прокинувся зі сну!</b>\n\n"
                            f"📥 Знайдено <b>{count}</b> відкладених запитів на завантаження аніме, надісланих поки ноут був закритий.\n"
                            "⚡️ Починаю завантаження на диск..."
                        )
                    )

            # Перевірка вечірніх нагадувань
            due = get_due_reminders()
            for rem in due:
                try:
                    file_path = resolve_file(rem["file_key"])
                    title_name = rem.get("title", "Аніме")
                    kb = build_downloaded_action_keyboard(rem["file_key"])
                    await bot.send_message(
                        chat_id=rem["chat_id"],
                        text=(
                            f"🌆 <b>Час для вечірнього перегляду аніме!</b>\n\n"
                            f"🎬 <b>{title_name}</b> уже завантажено і чекає на вашому Mac.\n"
                            "🍿 <i>Влаштовуйтесь зручніше та запускайте серію в IINA в 1 клік:</i>"
                        ),
                        reply_markup=kb
                    )
                    mark_reminder_done(rem["id"])
                except Exception as ex_rem:
                    logger.debug(f"Помилка відправки нагадування: {ex_rem}")
                    mark_reminder_done(rem["id"])

            # Перевірка вечірнього дайджесту для тихого режиму (після 18:30)
            now_dt = datetime.datetime.now()
            if now_dt.hour >= 18 and now_dt.minute >= 30:
                if ALLOWED_USERS:
                    for uid in ALLOWED_USERS:
                        pending_dig = get_pending_digest(uid)
                        if pending_dig:
                            clear_pending_digest(uid)
                            items_text = ""
                            for idx, d_item in enumerate(pending_dig, 1):
                                ep_s = f" ({d_item['ep_num']} серія)" if d_item.get("ep_num") else ""
                                items_text += f"• 🎬 <b>{d_item['title']}</b>{ep_s}\n"

                            digest_msg = (
                                "🌆 <b>Вечірній дайджест за робочий день!</b>\n\n"
                                f"Поки ви працювали, бот завантажив на Mac <b>{len(pending_dig)} нових серій</b>:\n\n"
                                f"{items_text}\n"
                                "🍿 <i>Приємного вечірнього перегляду! Усі серії вже готові в розділі «📁 Мої аніме».</i>"
                            )
                            try:
                                await bot.send_message(
                                    chat_id=uid,
                                    text=digest_msg,
                                    reply_markup=InlineKeyboardMarkup(
                                        inline_keyboard=[[InlineKeyboardButton(text="📁 Відкрити список аніме", callback_data="refresh_list")]]
                                    )
                                )
                            except Exception as ex_dig:
                                logger.debug(f"Помилка надсилання дайджесту: {ex_dig}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Помилка у mac_sleep_watchdog: {e}")


async def channel_monitoring_loop(bot: Bot):
    """
    Фоновий моніторинг оновлень публічних каналів аніме (@ridnyiholos та тестового @testpushbotanime).
    Автоматично скачує нові серії згідно з підписками (тільки озвучення / тільки субтитри / будь-яке).
    При старті фіксує baseline поточних постів каналу та перевіряє час публікації, щоб ніколи не слати старі сповіщення під час оновлень.
    """
    boot_ts = time.time()

    # Початкова ініціалізація baseline при старті бота
    try:
        init_subs = get_subscriptions()
        init_channels = set(s.get("channel", "ridnyiholos") for s in init_subs) | {"ridnyiholos", "testpushbotanime"}
        # Для тестового каналу фіксуємо стартовий сервісний пост #1 як baseline, щоб перший же тестовий пост спрацював миттєво
        if get_max_seen_post_id("testpushbotanime") is None:
            mark_post_seen("testpushbotanime", 1)
        for ch in init_channels:
            init_posts = await fetch_channel_posts(ch, pages=1)
            for p in init_posts:
                mark_post_seen(ch, p["post_id"])
            if init_posts:
                logger.info(f"Channel baseline @{ch}: зафіксовано {len(init_posts)} існуючих постів (max ID={max(p['post_id'] for p in init_posts)})")
    except Exception as e_init:
        logger.debug(f"Помилка ініціалізації baseline каналу: {e_init}")

    while True:
        try:
            await asyncio.sleep(25)  # швидка перевірка кожні 25 секунд
            subs = get_subscriptions()
            if not subs:
                continue

            channels = set(s.get("channel", "ridnyiholos") for s in subs) | {"ridnyiholos", "testpushbotanime"}
            for ch in channels:
                max_seen = get_max_seen_post_id(ch)
                posts = await fetch_channel_posts(ch, pages=1)
                if not posts:
                    continue

                # Якщо таблиця seen для основного каналу ще порожня — робимо baseline і не спамимо старими постами!
                if max_seen is None and ch != "testpushbotanime":
                    for p in posts:
                        mark_post_seen(ch, p["post_id"])
                    logger.info(f"Channel baseline @{ch}: зафіксовано {len(posts)} постів")
                    continue

                for post in sorted(posts, key=lambda x: x["post_id"]):
                    pid = post["post_id"]
                    # Ігноруємо всі пости, які вже бачили або які старіші за наш max_seen
                    if (max_seen is not None and pid <= max_seen) or is_post_seen(ch, pid):
                        mark_post_seen(ch, pid)
                        continue

                    # Захист від ретроактивних сповіщень при рестарті/оновленні бота:
                    # якщо пост на @ridnyiholos вийшов ДО запуску поточного процесу бота — тихо додаємо в seen
                    p_ts = post.get("post_ts")
                    if ch != "testpushbotanime" and p_ts and p_ts < (boot_ts - 60):
                        mark_post_seen(ch, pid)
                        continue

                    norm_post_title = normalize_title_for_match(post.get("title", ""))
                    norm_post_text = normalize_title_for_match(post.get("clean_text", ""))
                    if not norm_post_title or len(norm_post_title) < 3:
                        mark_post_seen(ch, pid)
                        continue

                    matched_sub = None
                    for s in subs:
                        sub_ch = s.get("channel", "ridnyiholos")
                        # Тестовий канал @testpushbotanime перевіряється по всіх підписках користувача!
                        if ch != "testpushbotanime" and sub_ch != ch:
                            continue
                        norm_sub_t = normalize_title_for_match(s.get("title", ""))
                        if not norm_sub_t or len(norm_sub_t) < 3:
                            continue
                        if norm_sub_t in norm_post_title or norm_post_title in norm_sub_t or norm_sub_t in norm_post_text:
                            pref = s.get("pref_type", "dub" if s.get("fex_url") else "any")
                            # У тестовому каналі дозволяємо спрацьовування навіть якщо не вказано явно "з озвученням"/"з субтитрами"
                            if ch == "testpushbotanime" and post["release_type"] == "any":
                                matched_sub = s
                                break
                            # Якщо користувач обрав "any", але для цієї або новішої серії вже є DUB — не шлемо SUB
                            if pref == "any" and post["release_type"] == "sub" and post.get("ep_num"):
                                ep_n = post["ep_num"]
                                dub_already_out = any(
                                    normalize_title_for_match(p2.get("title", ""))
                                    and (norm_sub_t in normalize_title_for_match(p2.get("title", "")) or normalize_title_for_match(p2.get("title", "")) in norm_sub_t)
                                    and p2.get("release_type") == "dub"
                                    and (p2.get("ep_num") or 0) >= ep_n
                                    for p2 in posts
                                )
                                if dub_already_out:
                                    continue
                            if pref == "any" or pref == post["release_type"]:
                                matched_sub = s
                                break

                    if not matched_sub:
                        mark_post_seen(ch, pid)
                        continue

                    # Знайдено відповідність підписці!
                    mark_post_seen(ch, pid)
                    user_chat = matched_sub["chat_id"]
                    display_title = matched_sub["title"]
                    ep_text = f" [Серія {post['ep_num']}]" if post.get("ep_num") else ""
                    if post["release_type"] == "dub":
                        type_text = " (🎙 з озвученням)"
                    elif post["release_type"] == "sub":
                        type_text = " (📝 з субтитрами)"
                    else:
                        type_text = " (🎙 з озвученням)" if matched_sub.get("pref_type") == "dub" else ""
                    eta = calculate_download_eta()

                    quiet = is_in_quiet_hours(user_chat)
                    # Перевіряємо наявність FEX.NET роздачі проти Медіатеки Telegram
                    has_fex_link = False
                    fex_link = None
                    media_tg_link = None

                    for l_lbl, l_url in post.get("links", []):
                        if "fex.net" in l_url:
                            has_fex_link = True
                            fex_link = l_url
                        elif "mediareser" in l_url and not media_tg_link:
                            media_tg_link = l_url

                    # Якщо в пості (наприклад, у тестовому каналі @testpushbotanime) не вставлено посилання,
                    # автоматично підтягуємо FEX або гілку медіатеки зі збереженої підписки!
                    if not has_fex_link and not media_tg_link:
                        if matched_sub.get("fex_url") and post["release_type"] != "sub":
                            has_fex_link = True
                            fex_link = matched_sub["fex_url"]
                        elif matched_sub.get("thread_url"):
                            media_tg_link = matched_sub["thread_url"]

                    # ВАРІАНТ 1: Тайтл з Медіатеки (лише онлайн субтитри)
                    if not has_fex_link:
                        tg_url = normalize_telegram_url(media_tg_link or matched_sub.get("thread_url") or f"https://t.me/{ch}/{pid}")
                        notify_text = (
                            f"🔔 <b>Вийшла нова серія:</b> <code>{display_title}</code>{ep_text}{type_text}!\n"
                            f"📡 <i>Канал:</i> @{ch}\n\n"
                            "🟡 <b>Формат релізу:</b> 📲 <i>Онлайн-медіатека Telegram</i>\n\n"
                            "✨ <b>Як скачати цю серію на Mac в 1 клік:</b>\n"
                            "1. Відкрийте серію в медіатеці за кнопкою нижче.\n"
                            "2. Натисніть <b>«Переслати» (Forward)</b> на відеофайл сюди ботові.\n"
                            "🚀 <i>Бот миттєво збереже його на ноутбук на повній швидкості!</i>"
                        )
                        kb_notify = InlineKeyboardMarkup(
                            inline_keyboard=[
                                [InlineKeyboardButton(text="↗️ Відкрити серію в Медіатеці", url=tg_url)],
                            ]
                        )
                        try:
                            await bot.send_message(
                                chat_id=user_chat,
                                text=notify_text,
                                reply_markup=kb_notify,
                                disable_notification=quiet
                            )
                        except Exception as ex_tg_not:
                            logger.debug(f"Помилка відправки сповіщення медіатеки: {ex_tg_not}")
                        continue

                    # ВАРІАНТ 2: Тайтл із FEX.NET (Повний авто-пілот у 4K/1080p)
                    ep_n = post.get("ep_num")
                    if ep_n and is_episode_handled_or_downloaded(user_chat, display_title, ep_n):
                        logger.info(f"Серія «{display_title}» [Серія {ep_n}] вже завантажена або в черзі. Пропуск дублікату з каналу.")
                        mark_post_seen(ch, pid)
                        continue

                    mac_state = get_mac_device_state()
                    mac_online = mac_state.get("is_online", False) if mac_state else (platform.system() == "Darwin")
                    status_note = "🚀 <b>Починаю автоматичне завантаження на ваш Mac...</b>" if mac_online else "💤 <b>Mac зараз у режимі сну (офлайн).</b>\n⚡️ <i>Завдання поставлено в чергу — завантажиться автоматично, щойно ви відкриєте ноутбук!</i>"

                    header_notify = (
                        f"🔔 <b>Вийшла нова серія:</b> <code>{display_title}</code>{ep_text}{type_text}!\n"
                        f"📡 <i>Канал:</i> @{ch}\n"
                        f"⚡️ <i>Орієнтовний час завантаження:</i> ~{eta}\n\n"
                        f"{status_note}"
                    )

                    notify_msg = await bot.send_message(
                        chat_id=user_chat,
                        text=header_notify,
                        disable_notification=quiet
                    )

                    share_key = FexDownloader.extract_share_key(fex_link) if fex_link else None
                    if not share_key:
                        sorted_links = score_and_sort_links(post["links"])
                        if sorted_links:
                            share_key = FexDownloader.extract_share_key(sorted_links[0][1])

                    if share_key:
                        clean_t = sanitize_folder_name(display_title)
                        t_folder = DOWNLOADS_DIR / clean_t
                        t_folder.mkdir(parents=True, exist_ok=True)
                        files = await FexDownloader.get_files_in_share(share_key)
                        if files:
                            if len(files) > 25:
                                norm_t = normalize_title_for_match(display_title)
                                title_matched = [f for f in files if norm_t and norm_t[:15] in normalize_title_for_match(f.get("name", ""))]
                                if title_matched:
                                    files = title_matched
                            all_share_names = [f.get("name", "") for f in files]
                            if post.get("ep_num"):
                                ep_n = post["ep_num"]
                                ep_pats = [f"[{ep_n:02d}", f"[{ep_n} ", f"[{ep_n}]", f" {ep_n:02d} "]
                                files = [f for f in files if any(p in f.get("name", "") for p in ep_pats)] or files
                            preferred_4k = [f for f in files if any(q in f.get("name", "").lower() for q in ["2160p", "4k"])]
                            preferred_1080p = [f for f in files if "1080p" in f.get("name", "").lower()]
                            target_file = preferred_4k[0] if preferred_4k else (preferred_1080p[0] if preferred_1080p else files[0])
                            f_name = target_file.get("name", "episode.mp4")
                            dest = t_folder / f_name
                            dl_url = target_file.get("download_url")
                            tok = target_file.get("token")
                            if dl_url:
                                ep_detected = post.get("ep_num")
                                if not ep_detected:
                                    m_ep = re.search(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]", f_name)
                                    ep_detected = int(m_ep.group(1)) if m_ep else 1
                                if platform.system() != "Darwin":
                                    cmd_payload = json.dumps({
                                        "url": dl_url,
                                        "token": tok,
                                        "share_key": share_key,
                                        "title": display_title,
                                        "episode": ep_detected,
                                        "folder": t_folder.name,
                                        "filename": dest.name,
                                        "chat_id": user_chat,
                                        "msg_id": notify_msg.message_id if notify_msg else None,
                                        "quality": "4K Ultra HD" if "2160p" in dest.name else "1080p Full HD",
                                        "dub_sub": detect_tracks_label(dest.name, display_title, user_chat)
                                    })
                                    queue_mac_command("download_fex", cmd_payload)
                                    if ep_detected:
                                        mark_episode_handled(user_chat, display_title, ep_detected, dest.name)
                                    mark_post_seen(ch, pid)
                                    continue

                                tracker = DownloadProgressTracker(interval_seconds=2.5)

                                async def on_auto_dl_prog(downloaded: int, total: int):
                                    if not quiet and tracker.should_update_telegram():
                                        speed, eta_calc = tracker.calculate_stats(downloaded, total)
                                        bar_text = render_progress_bar(downloaded, total, speed, eta_calc)
                                        try:
                                            await notify_msg.edit_text(
                                                f"🔔 <b>Авто-завантаження:</b> <code>{display_title}</code>{ep_text}\n\n" + bar_text
                                            )
                                        except Exception:
                                            pass

                                await FexDownloader.download_file(dl_url, dest, token=tok, progress_callback=on_auto_dl_prog)
                                set_macos_tag(dest, TAG_NAME, TAG_COLOR)
                                set_macos_tag(t_folder, TAG_NAME, TAG_COLOR)
                                f_key = register_file(dest)

                                if ep_detected:
                                    mark_episode_handled(user_chat, display_title, ep_detected, dest.name)
                                mark_post_seen(ch, pid)

                                if quiet:
                                    add_to_digest_queue(user_chat, display_title, ep_detected, str(dest))
                                    try:
                                        await notify_msg.delete()
                                    except Exception:
                                        pass
                                else:
                                    await notify_download_finished(
                                        bot,
                                        user_chat,
                                        notify_msg,
                                        dest,
                                        display_title,
                                        prefix_text=f"🎉 <b>Авто-завантаження завершено! {display_title}{ep_text} на ноуті!</b>"
                                    )
                        else:
                            # Файли на FEX ще не з'явилися або сталась тимчасова помилка API: оновлюємо повідомлення й не позначаємо як оброблене
                            try:
                                await notify_msg.edit_text(
                                    f"🔔 <b>Вийшла нова серія:</b> <code>{display_title}</code>{ep_text}{type_text}!\n"
                                    f"📡 <i>Канал:</i> @{ch}\n\n"
                                    "⏳ <b>Очікую появу роздачі на FEX.NET...</b>\n"
                                    "⚡️ <i>Файл завантажується на сервер роздачі. Бот автоматично перевірить роздачу та скачає на Mac щойно вона з'явиться!</i>"
                                )
                            except Exception:
                                pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Помилка у channel_monitoring_loop: {e}")


async def auto_download_fex_episode(bot: Bot, chat_id: int, title: str, episode: int, target_file: Dict) -> bool:
    """Тихо/з прогресом качає одну нову серію з FEX на Mac. Реєструється в ACTIVE_DOWNLOAD_TASKS (захист від перезапуску)."""
    quiet = is_in_quiet_hours(chat_id)
    clean_t = sanitize_folder_name(title)
    t_folder = DOWNLOADS_DIR / clean_t
    t_folder.mkdir(parents=True, exist_ok=True)
    f_name = target_file.get("name", f"episode_{episode}.mkv")
    dest = t_folder / f_name
    dl_url = target_file.get("download_url")
    tok = target_file.get("token")
    if not dl_url:
        return False
    if dest.exists() and dest.stat().st_size > 10 * 1024 * 1024:
        return True

    # Захист від повторного завантаження серії, що вже є на Mac або в черзі
    if is_episode_handled_or_downloaded(chat_id, title, episode):
        logger.info(f"FEX-watcher: серія «{title}» [Серія {episode}] вже завантажена або в черзі на Mac. Пропуск.")
        return True

    dub_sub = detect_tracks_label(f_name, title, chat_id)
    quality = "4K Ultra HD" if ("2160p" in f_name or "4k" in f_name.lower()) else "1080p Full HD"

    mac_state = get_mac_device_state()
    mac_online = mac_state.get("is_online", False) if mac_state else (platform.system() == "Darwin")
    status_suffix = "🚀 <b>Починаю автоматичне завантаження на ваш Mac...</b>" if mac_online else "💤 <b>Mac зараз у режимі сну (офлайн).</b>\n⚡️ <i>Завдання поставлено в чергу — завантажиться автоматично, щойно ви відкриєте ноутбук!</i>"

    cancel_id = f"auto_{uuid.uuid4().hex[:8]}"
    notify_msg = await bot.send_message(
        chat_id,
        f"🔔 <b>Вийшла нова серія:</b> <code>{title}</code> [Серія {episode}]\n"
        f"🎞 <b>Якість:</b> {quality} • 🔊 <b>Доріжки:</b> {dub_sub}\n\n"
        f"{status_suffix}",
        reply_markup=build_cancel_keyboard(cancel_id) if platform.system() == "Darwin" else None,
        disable_notification=quiet,
    )

    if platform.system() != "Darwin":
        cmd_payload = json.dumps({
            "url": dl_url,
            "token": tok,
            "share_key": target_file.get("share_key"),
            "title": title,
            "episode": episode,
            "folder": clean_t,
            "filename": f_name,
            "chat_id": chat_id,
            "msg_id": notify_msg.message_id if notify_msg else None,
            "quality": quality,
            "dub_sub": dub_sub
        })
        queue_mac_command("download_fex", cmd_payload)
        mark_episode_handled(chat_id, title, episode, f_name)
        return True
    tracker = DownloadProgressTracker(interval_seconds=2.5)

    async def on_prog(downloaded: int, total: int):
        if not quiet and tracker.should_update_telegram():
            speed, eta_calc = tracker.calculate_stats(downloaded, total)
            try:
                await notify_msg.edit_text(
                    f"🔔 <b>Авто-завантаження:</b> <code>{title}</code> [Серія {episode}]\n\n"
                    + render_progress_bar(downloaded, total, speed, eta_calc),
                    reply_markup=build_cancel_keyboard(cancel_id),
                )
            except Exception:
                pass

    dl_task = asyncio.create_task(FexDownloader.download_file(dl_url, dest, token=tok, progress_callback=on_prog))
    ACTIVE_DOWNLOAD_TASKS[cancel_id] = dl_task
    try:
        async with DOWNLOAD_SEMAPHORE:
            await dl_task
        set_macos_tag(dest, TAG_NAME, TAG_COLOR)
        set_macos_tag(t_folder, TAG_NAME, TAG_COLOR)
        register_file(dest)
        if quiet:
            add_to_digest_queue(chat_id, title, episode, str(dest))
            try:
                await notify_msg.delete()
            except Exception:
                pass
        else:
            await notify_download_finished(
                bot, chat_id, notify_msg, dest, title,
                prefix_text=f"🎉 <b>Вийшла нова серія «{title}» [Серія {episode}]. Завантажено на Mac!</b>",
            )
            # Оновлюємо віджет Watchlist Hub при завершенні локального завантаження
            try:
                await send_or_update_watchlist_hub(bot, chat_id)
            except Exception:
                pass
        return True
    except asyncio.CancelledError:
        cleanup_cancelled_download(dest, t_folder)
        try:
            await notify_msg.edit_text(f"🛑 Авто-завантаження серії {episode} скасовано.")
        except Exception:
            pass
        return False
    except Exception as e:
        logger.error(f"Авто-завантаження {title} #{episode}: {e}")
        try:
            await notify_msg.edit_text(f"❌ Помилка авто-завантаження «{title}» [Серія {episode}]: {e}")
        except Exception:
            pass
        return False
    finally:
        ACTIVE_DOWNLOAD_TASKS.pop(cancel_id, None)


async def fex_share_watch_pass(bot: Bot):
    """Один прохід: для кожної підписки з fex_url шукає НОВІ серії в роздачі та качає їх + Reconciler пропущених серій."""
    for s in get_subscriptions():
        fex_url = s.get("fex_url")
        share_key = FexDownloader.extract_share_key(fex_url) if fex_url else None
        if not share_key:
            continue
        chat_id = s["chat_id"]
        try:
            files = await FexDownloader.get_files_in_share(share_key)
            if not files:
                continue
            seen = get_fex_seen(chat_id, share_key)
            ep_re = re.compile(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]")
            if seen is None:
                # Baseline: перше сканування — запам'ятовуємо наявне, щоб не качати старі серії
                all_names = [f.get("name", "") for f in files]
                mark_fex_seen(chat_id, share_key, all_names)
                # ЗАПОБІЖНИК: одразу фіксуємо всі існуючі серії як оброблені, щоб ніколи їх повторно не качати автоматично
                for f in files:
                    m = ep_re.search(f.get("name", ""))
                    if m:
                        mark_episode_handled(chat_id, s["title"], int(m.group(1)), f.get("name", ""))
                logger.info(f"FEX baseline «{s['title']}»: {len(files)} файлів зафіксовано як бачені")
                continue

            for item in pick_new_fex_episodes(files, seen, s.get("pref_type", "any")):
                target_f = item["file"]
                target_f["share_key"] = share_key
                # Додатковий запобіжник: перевіряємо чи серію вже бачили/качали
                if is_episode_handled_or_downloaded(chat_id, s["title"], item["episode"]):
                    mark_fex_seen(chat_id, share_key, item["all_names"])
                    continue
                ok = await auto_download_fex_episode(bot, chat_id, s["title"], item["episode"], target_f)
                if ok:
                    mark_fex_seen(chat_id, share_key, item["all_names"])
                    mark_episode_handled(chat_id, s["title"], item["episode"], target_f.get("name", ""))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"FEX-watcher «{s.get('title')}»: {e}")


async def fex_share_watch_loop(bot: Bot):
    """Фоновий цикл (кожні 5 хв): відстеження нових серій у FEX-роздачах підписок."""
    await asyncio.sleep(20)
    while True:
        try:
            await fex_share_watch_pass(bot)
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Помилка fex_share_watch_loop: {e}")
            await asyncio.sleep(60)


async def backfill_fex_subscriptions_baseline():
    """Фіксує всі поточні серії в підписках як вже бачені/оброблені, щоб нічого старого не качалося."""
    try:
        ep_re = re.compile(r"\[(\d{1,3})\s*(?:з|/|-)?\s*(?:\d{1,3}|ХХ|XX)?\]")
        for s in get_subscriptions():
            fex_url = s.get("fex_url")
            share_key = FexDownloader.extract_share_key(fex_url) if fex_url else None
            if not share_key:
                continue
            chat_id = s.get("chat_id", 453079694)
            files = await FexDownloader.get_files_in_share(share_key)
            if not files:
                continue
            all_names = [f.get("name", "") for f in files]
            mark_fex_seen(chat_id, share_key, all_names)
            for f in files:
                m = ep_re.search(f.get("name", ""))
                if m:
                    mark_episode_handled(chat_id, s["title"], int(m.group(1)), f.get("name", ""))
            logger.info(f"Захисний бейзлайн «{s['title']}»: зафіксовано {len(files)} файлів як бачені")
    except Exception as e:
        logger.debug(f"backfill_fex_subscriptions_baseline error: {e}")


async def main():
    if not BOT_TOKEN or BOT_TOKEN == "your_telegram_bot_token_here":
        logger.error("ПОМИЛКА: Не вказано токен бота у файлі .env (ANIME_BOT_TOKEN)!")
        sys.exit(1)

    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Папка завантажень готова: {DOWNLOADS_DIR}")

    init_registry_db()
    init_offline_queue_db()
    init_channel_monitor_db()
    init_update_db()
    init_mac_sync_db()
    try:
        cancelled_cnt = cancel_pending_download_commands()
        if cancelled_cnt:
            logger.info(f"Скасовано {cancelled_cnt} старих команд завантаження з черги.")
    except Exception as e_clear:
        logger.debug(f"Помилка cancel_pending_download_commands: {e_clear}")

    # Синхронізуємо стартові підписки та їхні налаштування (DUB vs SUB)
    for uid in (set(ALLOWED_USERS) | {453079694}):
        try:
            seed_subscriptions(uid)
        except Exception as e_seed:
            logger.debug(f"seed_subscriptions({uid}): {e_seed}")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.delete_webhook(drop_pending_updates=False)

    # Оновлюємо версію у БД тихо, без розсилки сервісних повідомлень при деплої
    try:
        check_for_new_release()
    except Exception:
        pass

    # Якщо перезапуск було викликано вручну кнопкою «🔄 Рестарт бота» — звітуємо користувачу
    try:
        restart_req = pop_manual_restart_request()
        if restart_req:
            r_chat_id, r_msg_id = restart_req
            git_sha, git_msg = get_current_git_info()
            now_str = time.strftime("%H:%M:%S")
            done_text = (
                f"✅ <b>Бот успішно перезапущено! Найновіше оновлення активне.</b>\n\n"
                f"📦 <b>Версія:</b> <code>v{CURRENT_VERSION}</code> (коміт <code>#{git_sha}</code>)\n"
                f"📝 <b>Останнє оновлення:</b> <i>{git_msg}</i>\n"
                f"🍏 <b>MacAgent:</b> синхронізовано\n"
                f"⏱ <b>Час оновлення:</b> <code>{now_str}</code>"
            )
            try:
                await bot.edit_message_text(chat_id=r_chat_id, message_id=r_msg_id, text=done_text)
            except Exception:
                await bot.send_message(chat_id=r_chat_id, text=done_text, reply_markup=get_main_reply_keyboard())
    except Exception as e_rest:
        logger.debug(f"Помилка звіту про ручний рестарт: {e_rest}")

    # Запускаємо фонові монітори
    asyncio.create_task(backfill_fex_subscriptions_baseline())
    asyncio.create_task(mac_sleep_watchdog(bot))
    asyncio.create_task(channel_monitoring_loop(bot))
    asyncio.create_task(fex_share_watch_loop(bot))
    asyncio.create_task(auto_reload_watcher())
    asyncio.create_task(run_sync_server())

    logger.info("Аніме-бот успішно запущений і готовий до роботи!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот зупинений.")
