#!/usr/bin/env python3
"""
Mac Local Downloader & Library Agent
Автономно працює на macOS:
1. Сканує папку ~/Downloads/(Завантажене аніме).
2. Відправляє список файлів та вільне місце на сервер Railway (кожні 5 сек).
3. Отримує команди від сервера (завантажити серію, видалити файл, відкрити у Finder/IINA).
4. Завантажує 4K/1080p серії на Mac та ставить кольоровий тег Finder.
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Any

import aiohttp

# Завантажуємо конфігурацію проєкту
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import BASE_DIR, BOT_TOKEN, DOWNLOADS_DIR, TAG_NAME, TAG_COLOR, TELEGRAM_API_ID, TELEGRAM_API_HASH
from fex import FexDownloader
from telegram_mtproto import TelegramMtprotoDownloader
from tagger import set_macos_tag
from progress import format_bytes

mtproto_downloader: Optional[TelegramMtprotoDownloader] = None
if TELEGRAM_API_ID and TELEGRAM_API_HASH and BOT_TOKEN:
    mtproto_downloader = TelegramMtprotoDownloader(
        api_id=TELEGRAM_API_ID,
        api_hash=TELEGRAM_API_HASH,
        bot_token=BOT_TOKEN,
        session_dir=BASE_DIR,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [MacAgent] %(levelname)s - %(message)s"
)
logger = logging.getLogger("MacAgent")

VIDEO_EXTS = (".mkv", ".mp4", ".webm", ".avi", ".mov", ".m4v")
START_TIME = time.time()

# URL сервера (встановлюється в .env як RAILWAY_API_URL або береться публічний домен Railway)
SERVER_URL = os.getenv("RAILWAY_API_URL", "https://anime-downloader-bot-production.up.railway.app").rstrip("/")
SYNC_SECRET = os.getenv("MAC_SYNC_SECRET") or (BOT_TOKEN[:16] if BOT_TOKEN else "default_secret")


def scan_local_anime() -> List[Dict[str, Any]]:
    """Сканує папку ~/Downloads/(Завантажене аніме) та повертає чистий список файлів."""
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    try:
        for p in DOWNLOADS_DIR.rglob("*"):
            if p.is_file() and p.suffix.lower() in VIDEO_EXTS and not p.name.endswith(".part"):
                folder = p.parent.name if p.parent != DOWNLOADS_DIR else "Різне"
                f_key = hashlib.md5(str(p).encode("utf-8")).hexdigest()[:10]
                results.append({
                    "key": f_key,
                    "folder": folder,
                    "name": p.name,
                    "path": str(p.resolve()),
                    "size": p.stat().st_size,
                })
    except Exception as e:
        logger.error(f"Помилка сканування папки: {e}")
    return results


def get_mac_stats():
    """Отримує показники MacBook."""
    try:
        usage = shutil.disk_usage(DOWNLOADS_DIR)
        free_gb = usage.free / (1024 ** 3)
    except Exception:
        free_gb = 0.0

    uptime_sec = int(time.time() - START_TIME)
    uptime_str = f"{uptime_sec // 3600}год {(uptime_sec % 3600) // 60}хв {uptime_sec % 60}с"
    device_name = socket.gethostname().replace(".local", "")

    return device_name, free_gb, uptime_str


def find_file_on_mac(payload: str) -> Optional[Path]:
    """Гнучко та надійно знаходить файл на Mac за шляхом або назвою без збоїв через дужки []."""
    if not payload:
        return None

    # 1. Прямий шлях
    p = Path(payload)
    if p.exists() and p.is_file():
        return p

    # 2. Якщо шлях починався з /root/Downloads або іншого префіксу (із сервера Linux)
    if "(Завантажене аніме)" in payload:
        rel_part = payload.split("(Завантажене аніме)")[-1].lstrip("/\\")
        candidate = DOWNLOADS_DIR / rel_part
        if candidate.exists() and candidate.is_file():
            return candidate

    # 3. Точний пошук за назвою файлу (без glob, що ламається через [])
    fname = p.name
    if fname:
        for root, _, files in os.walk(DOWNLOADS_DIR):
            if fname in files:
                return Path(root) / fname

    # 4. Нечіткий пошук
    if fname:
        clean_fname = fname.lower()
        for root, _, files in os.walk(DOWNLOADS_DIR):
            for f in files:
                if f.endswith(".part"):
                    continue
                if clean_fname in f.lower() or f.lower() in clean_fname:
                    return Path(root) / f

    return None


async def execute_command(cmd: Dict[str, Any]) -> bool:
    """Виконує отриману від сервера команду на MacBook."""
    action = cmd.get("action")
    payload = cmd.get("payload", "").strip()
    logger.info(f"Виконання команди від сервера: {action} (payload: {payload})")

    try:
        if action == "reveal_folder":
            target = find_file_on_mac(payload) if payload else DOWNLOADS_DIR
            if target and target.exists():
                if target.is_file():
                    subprocess.run(["open", "-R", str(target)], check=False)
                else:
                    subprocess.run(["open", str(target)], check=False)
            else:
                subprocess.run(["open", str(DOWNLOADS_DIR)], check=False)
            return True

        elif action == "open_iina":
            target_file = find_file_on_mac(payload)
            if target_file and target_file.exists():
                iina_app = Path("/Applications/IINA.app")
                if iina_app.exists():
                    subprocess.run(["open", "-a", "IINA", str(target_file)], check=False)
                else:
                    subprocess.run(["open", str(target_file)], check=False)
                logger.info(f"Успішно відкрито у плеєрі: {target_file}")
                return True
            else:
                logger.warning(f"open_iina: файл не знайдено на Mac: {payload}")
                return True

        elif action == "delete_file":
            target_file = find_file_on_mac(payload)
            if target_file and target_file.exists():
                parent_dir = target_file.parent
                target_file.unlink(missing_ok=True)
                if parent_dir != DOWNLOADS_DIR:
                    remaining = [f for f in parent_dir.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTS]
                    if not remaining:
                        shutil.rmtree(parent_dir, ignore_errors=True)
                return True
            return True

        elif action == "clean_all":
            for item in DOWNLOADS_DIR.iterdir():
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                elif item.is_file() and item.suffix.lower() in VIDEO_EXTS:
                    item.unlink(missing_ok=True)
            return True

        elif action == "download_fex":
            data = json.loads(payload)
            dl_url = data.get("url")
            title = data.get("title", "Аніме")
            folder_name = data.get("folder") or title
            f_name = data.get("filename") or f"episode_{data.get('episode', 1)}.mkv"
            tok = data.get("token")
            chat_id = data.get("chat_id")
            msg_id = data.get("msg_id")
            poster_url = data.get("poster_url")
            quality = data.get("quality", "4K Ultra HD")
            dub_sub = data.get("dub_sub", "Озвучення (DUB) або Субтитри (SUB)")

            target_folder = DOWNLOADS_DIR / folder_name
            target_folder.mkdir(parents=True, exist_ok=True)
            dest_file = target_folder / f_name

            # Спробуємо завантажити постер
            poster_path = target_folder / "poster.jpg"
            if poster_url and not poster_path.exists():
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(poster_url) as resp:
                            if resp.status == 200:
                                poster_path.write_bytes(await resp.read())
                except Exception:
                    pass

            bot_token = os.environ.get("BOT_TOKEN") or BOT_TOKEN
            if dest_file.exists() and dest_file.stat().st_size > 10 * 1024 * 1024:
                logger.info(f"Файл {dest_file.name} вже існує на Mac. Пропуск мережевого завантаження, відправляю картку.")
            else:
                logger.info(f"Початок локального завантаження 4K на Mac SSD: {f_name}")
                last_tg_update = 0.0

                async def on_mac_progress(downloaded: int, total: int):
                    nonlocal last_tg_update
                    now = time.time()
                    if (now - last_tg_update) > 2.5 and bot_token and chat_id and msg_id:
                        last_tg_update = now
                        pct = (downloaded / total * 100) if total else 0
                        bar_blocks = int(pct / 10)
                        bar = "▰" * bar_blocks + "▱" * (10 - bar_blocks)
                        d_mb = downloaded / (1024 * 1024)
                        t_mb = total / (1024 * 1024)
                        text = (
                            f"🚀 <b>Завантаження відео на Mac (SSD)</b>\n\n"
                            f"🎬 <b>Аніме:</b> {title}\n"
                            f"📄 <b>Файл:</b> <code>{f_name}</code>\n"
                            f"📂 <b>Папка:</b> <code>{folder_name}</code>\n\n"
                            f"[{bar}] {pct:.1f}%\n"
                            f"📦 {d_mb:.1f} МБ / {t_mb:.1f} МБ • ⚡️ Швидкісне збереження на SSD"
                        )
                        try:
                            async with aiohttp.ClientSession() as s:
                                url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
                                await s.post(url, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": text,
                                    "parse_mode": "HTML"
                                }, timeout=aiohttp.ClientTimeout(total=2))
                        except Exception:
                            pass

                await FexDownloader.download_file(dl_url, dest_file, token=tok, progress_callback=on_mac_progress)
                set_macos_tag(dest_file, TAG_NAME, TAG_COLOR)
                set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)

            # Якщо постер досі не збережено — згенеруємо через ffmpeg з кадру
            if not poster_path.exists():
                try:
                    subprocess.run([
                        "/opt/homebrew/bin/ffmpeg", "-y", "-ss", "00:03:00",
                        "-i", str(dest_file), "-vframes", "1", "-q:v", "2", str(poster_path)
                    ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
                    pass

            logger.info(f"Успішно завантажено на Mac: {dest_file.name} з тегом {TAG_NAME}")

            # Надсилаємо картку аніме в Telegram за ТЗ користувача
            if bot_token and chat_id:
                size_str = format_bytes(dest_file.stat().st_size)
                f_key = hashlib.md5(str(dest_file).encode("utf-8")).hexdigest()[:10]
                ep_num = data.get("episode", 1)

                card_text = (
                    f"🎬 <b>{title} [Серія {ep_num}]</b>\n"
                    f"🎞 <b>Якість:</b> {quality}\n\n"
                    f"🔊 <b>Доріжки:</b> {dub_sub}\n"
                    f"🏷 <b>Finder:</b> 🟢 На черзі глянути\n\n"
                    f"📦 <b>Розмір:</b> {size_str}"
                )

                keyboard = {
                    "inline_keyboard": [
                        [
                            {"text": "▶️ Запустити в плеєрі", "callback_data": f"play:{f_key}"},
                            {"text": "📁 Відкрити у Finder", "callback_data": f"finder:{f_key}"}
                        ],
                        [
                            {"text": f"🗑 Видалити ({size_str})", "callback_data": f"del:{f_key}"}
                        ],
                        [
                            {"text": "⬅️ Назад до списку", "callback_data": "refresh_list"}
                        ]
                    ]
                }

                try:
                    async with aiohttp.ClientSession() as s:
                        if poster_path.exists():
                            data_form = aiohttp.FormData()
                            data_form.add_field("chat_id", str(chat_id))
                            data_form.add_field("caption", card_text)
                            data_form.add_field("parse_mode", "HTML")
                            data_form.add_field("reply_markup", json.dumps(keyboard))
                            data_form.add_field("photo", open(poster_path, "rb"), filename="poster.jpg")
                            await s.post(f"https://api.telegram.org/bot{bot_token}/sendPhoto", data=data_form)
                            if msg_id:
                                try:
                                    await s.post(f"https://api.telegram.org/bot{bot_token}/deleteMessage", json={
                                        "chat_id": chat_id,
                                        "message_id": msg_id
                                    })
                                except Exception:
                                    pass
                        else:
                            if msg_id:
                                await s.post(f"https://api.telegram.org/bot{bot_token}/editMessageText", json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": card_text,
                                    "parse_mode": "HTML",
                                    "reply_markup": keyboard
                                })
                            else:
                                await s.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json={
                                    "chat_id": chat_id,
                                    "text": card_text,
                                    "parse_mode": "HTML",
                                    "reply_markup": keyboard
                                })
                except Exception as e_tg:
                    logger.warning(f"Не вдалося оновити повідомлення в Telegram: {e_tg}")

            return True

        elif action == "download_tg_media":
            data = json.loads(payload)
            chat_id = data.get("chat_id")
            source_msg_id = data.get("source_msg_id")
            msg_id = data.get("msg_id")
            title = data.get("title", "Аніме")
            folder_name = data.get("folder") or title
            f_name = data.get("filename") or f"episode_{data.get('episode', 1)}.mp4"
            quality = data.get("quality", "1080p Full HD")
            dub_sub = data.get("dub_sub", "Субтитри (SUB)")
            ep_num = data.get("episode", 1)

            target_folder = DOWNLOADS_DIR / folder_name
            target_folder.mkdir(parents=True, exist_ok=True)
            dest_file = target_folder / f_name
            poster_path = target_folder / "poster.jpg"
            bot_token = os.environ.get("BOT_TOKEN") or BOT_TOKEN

            if not mtproto_downloader:
                logger.error("MTProto не налаштований на MacAgent!")
                return True

            if dest_file.exists() and dest_file.stat().st_size > 5 * 1024 * 1024:
                logger.info(f"Файл {dest_file.name} вже існує на Mac. Відправляю картку.")
            else:
                logger.info(f"Початок MTProto завантаження з Telegram на Mac SSD: {f_name}")
                last_tg_update = 0.0

                async def on_tg_mac_progress(downloaded: int, total: int):
                    nonlocal last_tg_update
                    now = time.time()
                    if (now - last_tg_update) > 2.2 and bot_token and chat_id and msg_id:
                        last_tg_update = now
                        pct = (downloaded / total * 100) if total else 0
                        bar_blocks = int(pct / 10)
                        bar = "▰" * bar_blocks + "▱" * (10 - bar_blocks)
                        d_mb = downloaded / (1024 * 1024)
                        t_mb = total / (1024 * 1024)
                        text = (
                            f"🚀 <b>Завантаження відео з Telegram (MTProto)</b>\n\n"
                            f"🎬 <b>Аніме:</b> {title} [Серія {ep_num}]\n"
                            f"📄 <b>Файл:</b> <code>{f_name}</code>\n"
                            f"📂 <b>Папка:</b> <code>{folder_name}</code>\n\n"
                            f"[{bar}] {pct:.1f}%\n"
                            f"📦 {d_mb:.1f} МБ / {t_mb:.1f} МБ • ⚡️ Швидкісне збереження на SSD"
                        )
                        try:
                            async with aiohttp.ClientSession() as s:
                                url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
                                await s.post(url, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": text,
                                    "parse_mode": "HTML"
                                }, timeout=aiohttp.ClientTimeout(total=2))
                        except Exception:
                            pass

                await mtproto_downloader.download_message_video(
                    chat_id=chat_id,
                    message_id=source_msg_id,
                    destination_path=dest_file,
                    progress_callback=on_tg_mac_progress,
                )
                set_macos_tag(dest_file, TAG_NAME, TAG_COLOR)
                set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)

            if not poster_path.exists():
                try:
                    subprocess.run([
                        "/opt/homebrew/bin/ffmpeg", "-y", "-ss", "00:03:00",
                        "-i", str(dest_file), "-vframes", "1", "-q:v", "2", str(poster_path)
                    ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
                    pass

            if bot_token and chat_id and dest_file.exists():
                size_str = format_bytes(dest_file.stat().st_size)
                f_key = hashlib.md5(str(dest_file).encode("utf-8")).hexdigest()[:10]
                card_text = (
                    f"🎬 <b>{title} [Серія {ep_num}]</b>\n"
                    f"🎞 <b>Якість:</b> {quality}\n\n"
                    f"🔊 <b>Доріжки:</b> {dub_sub}\n"
                    f"🏷 <b>Finder:</b> 🟢 На черзі глянути\n\n"
                    f"📦 <b>Розмір:</b> {size_str}"
                )
                keyboard = {
                    "inline_keyboard": [
                        [
                            {"text": "▶️ Запустити в плеєрі", "callback_data": f"play:{f_key}"},
                            {"text": "📁 Відкрити у Finder", "callback_data": f"finder:{f_key}"}
                        ],
                        [
                            {"text": f"🗑 Видалити ({size_str})", "callback_data": f"del:{f_key}"}
                        ],
                        [
                            {"text": "⬅️ Назад до списку", "callback_data": "refresh_list"}
                        ]
                    ]
                }
                try:
                    async with aiohttp.ClientSession() as s:
                        if poster_path.exists():
                            data_form = aiohttp.FormData()
                            data_form.add_field("chat_id", str(chat_id))
                            data_form.add_field("caption", card_text)
                            data_form.add_field("parse_mode", "HTML")
                            data_form.add_field("reply_markup", json.dumps(keyboard))
                            data_form.add_field("photo", open(poster_path, "rb"), filename="poster.jpg")
                            await s.post(f"https://api.telegram.org/bot{bot_token}/sendPhoto", data=data_form)
                            if msg_id:
                                try:
                                    await s.post(f"https://api.telegram.org/bot{bot_token}/deleteMessage", json={
                                        "chat_id": chat_id,
                                        "message_id": msg_id
                                    })
                                except Exception:
                                    pass
                        else:
                            if msg_id:
                                await s.post(f"https://api.telegram.org/bot{bot_token}/editMessageText", json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": card_text,
                                    "parse_mode": "HTML",
                                    "reply_markup": keyboard
                                })
                            else:
                                await s.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json={
                                    "chat_id": chat_id,
                                    "text": card_text,
                                    "parse_mode": "HTML",
                                    "reply_markup": keyboard
                                })
                except Exception as e_tg:
                    logger.warning(f"Не вдалося оновити повідомлення в Telegram: {e_tg}")

            return True

    except Exception as e:
        logger.error(f"Помилка виконання команди {action}: {e}")
        return True

    return True


def get_posters_base64(folders: List[str]) -> Dict[str, str]:
    """Збирає poster.jpg (або генерує з кадру) для вказаних папок у форматі base64."""
    result: Dict[str, str] = {}
    for folder_name in folders:
        folder_path = DOWNLOADS_DIR / folder_name
        if not folder_path.exists() or not folder_path.is_dir():
            continue
        poster = folder_path / "poster.jpg"
        if not poster.exists():
            # Спробуємо знайти будь-який .thumb.jpg або згенерувати через ffmpeg
            thumbs = list(folder_path.glob("*.jpg"))
            if thumbs:
                poster = thumbs[0]
            else:
                vids = [f for f in folder_path.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTS and not f.name.endswith(".part")]
                if vids:
                    try:
                        subprocess.run([
                            "/opt/homebrew/bin/ffmpeg", "-y", "-ss", "00:03:00",
                            "-i", str(vids[0]), "-vframes", "1", "-vf", "scale=640:-1", "-q:v", "3", str(poster)
                        ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                    except Exception:
                        pass
        if poster.exists() and poster.stat().st_size > 500:
            try:
                result[folder_name] = base64.b64encode(poster.read_bytes()).decode("ascii")
            except Exception:
                pass
    return result


async def run_agent_loop():
    logger.info("Запуск локального Mac-агента...")
    logger.info(f"Папка завантажень на Mac: {DOWNLOADS_DIR}")

    executed_ids: List[int] = []
    folders_needing_posters: List[str] = []

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if SERVER_URL:
                    device_name, free_gb, uptime_str = get_mac_stats()
                    files = scan_local_anime()

                    posters_payload = get_posters_base64(folders_needing_posters) if folders_needing_posters else None

                    payload = {
                        "device_name": device_name,
                        "free_disk_gb": free_gb,
                        "downloads_folder": str(DOWNLOADS_DIR),
                        "uptime_str": uptime_str,
                        "files": files,
                        "executed_command_ids": executed_ids,
                        "posters": posters_payload,
                    }

                    headers = {
                        "X-Sync-Secret": SYNC_SECRET,
                        "Content-Type": "application/json"
                    }

                    async with session.post(
                        f"{SERVER_URL}/api/mac/heartbeat",
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            executed_ids.clear()
                            folders_needing_posters = data.get("missing_posters", [])
                            commands = data.get("commands", [])
                            for cmd in commands:
                                success = await execute_command(cmd)
                                if success:
                                    executed_ids.append(cmd["id"])

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Помилка пінгу сервера: {e}")

            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(run_agent_loop())
    except KeyboardInterrupt:
        logger.info("MacAgent зупинено.")
