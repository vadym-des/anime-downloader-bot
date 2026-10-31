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
import html
import json
import logging
import os
import re
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
        session_name="mac_agent_mtproto_session",
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


def format_eta(seconds: float) -> str:
    """Форматує залишковий час у зручний вигляд."""
    if seconds <= 0 or seconds > 86400:
        return "розрахунок..."
    sec = int(seconds)
    if sec < 60:
        return f"~{sec} сек"
    mins = sec // 60
    rem_s = sec % 60
    return f"~{mins} хв {rem_s:02d} сек"


def find_existing_episode_in_folder(target_folder: Path, dest_file: Path, ep_num: int) -> Optional[Path]:
    """Перевіряє, чи ця серія вже завантажена у папку (навіть під альтернативною назвою)."""
    if dest_file.exists() and dest_file.stat().st_size > 5 * 1024 * 1024:
        return dest_file
    if not target_folder.exists():
        return None
    ep_pats = [
        rf"\[0?{ep_num}\s*(?:з|/|-|\])",
        rf"S\d+E0?{ep_num}\b",
        rf"\b0?{ep_num}\s*сері",
    ]
    for f in target_folder.iterdir():
        if f.is_file() and f.suffix.lower() in VIDEO_EXTS and not f.name.endswith(".part"):
            if f.stat().st_size > 5 * 1024 * 1024:
                if any(re.search(pat, f.name, re.IGNORECASE) for pat in ep_pats):
                    return f
    return None


async def edit_tg_status(bot_token: str, chat_id: int, msg_id: Optional[int], text: str):
    """Надійно оновлює повідомлення статусу в Telegram."""
    if not (bot_token and chat_id and msg_id):
        return
    try:
        async with aiohttp.ClientSession() as s:
            url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
            async with s.post(
                url,
                json={
                    "chat_id": chat_id,
                    "message_id": msg_id,
                    "text": text,
                    "parse_mode": "HTML",
                },
                timeout=aiohttp.ClientTimeout(total=4),
            ) as resp:
                if resp.status != 200:
                    err_body = await resp.text()
                    if "message is not modified" not in err_body:
                        logger.warning(f"Telegram editMessageText ({resp.status}): {err_body}")
    except Exception as e:
        logger.debug(f"edit_tg_status error: {e}")


async def send_completed_anime_card(
    bot_token: str,
    chat_id: int,
    msg_id: Optional[int],
    dest_file: Path,
    poster_path: Path,
    title: str,
    ep_num: int,
    quality: str,
    dub_sub: str,
):
    """Надсилає фінальну картку серії з постером та кнопками керування і прибирає повідомлення прогресу."""
    if not (bot_token and chat_id and dest_file.exists()):
        return

    size_str = format_bytes(dest_file.stat().st_size)
    f_key = hashlib.md5(str(dest_file).encode("utf-8")).hexdigest()[:10]
    card_text = (
        f"✅ <b>Завантаження завершено!</b>\n\n"
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
                {"text": "📁 Відкрити у Finder", "callback_data": f"finder:{f_key}"},
            ],
            [
                {"text": f"🗑 Видалити ({size_str})", "callback_data": f"del:{f_key}"},
            ],
            [
                {"text": "⬅️ Назад до списку", "callback_data": "refresh_list"},
            ],
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
                with open(poster_path, "rb") as pf:
                    data_form.add_field("photo", pf.read(), filename="poster.jpg")
                await s.post(f"https://api.telegram.org/bot{bot_token}/sendPhoto", data=data_form)
                if msg_id:
                    try:
                        await s.post(
                            f"https://api.telegram.org/bot{bot_token}/deleteMessage",
                            json={"chat_id": chat_id, "message_id": msg_id},
                        )
                    except Exception:
                        pass
            else:
                if msg_id:
                    await s.post(
                        f"https://api.telegram.org/bot{bot_token}/editMessageText",
                        json={
                            "chat_id": chat_id,
                            "message_id": msg_id,
                            "text": card_text,
                            "parse_mode": "HTML",
                            "reply_markup": keyboard,
                        },
                    )
                else:
                    await s.post(
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json={
                            "chat_id": chat_id,
                            "text": card_text,
                            "parse_mode": "HTML",
                            "reply_markup": keyboard,
                        },
                    )
    except Exception as e_tg:
        logger.warning(f"Не вдалося оновити повідомлення в Telegram: {e_tg}")


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
            raw_dub_sub = data.get("dub_sub", "")
            fn_up = f_name.upper()
            if raw_dub_sub in ("", "Озвучення + Субтитри", "Озвучення (DUB) або Субтитри (SUB)"):
                if "SUB" in fn_up and not any(k in fn_up for k in ("MVO", "DUB", "ОЗВУЧ")):
                    dub_sub = "Субтитри (SUB)"
                else:
                    dub_sub = "Озвучення (DUB)"
            else:
                dub_sub = raw_dub_sub
            ep_num = data.get("episode", 1)

            target_folder = DOWNLOADS_DIR / folder_name
            target_folder.mkdir(parents=True, exist_ok=True)
            dest_file = target_folder / f_name
            poster_path = target_folder / "poster.jpg"
            bot_token = BOT_TOKEN or os.environ.get("ANIME_BOT_TOKEN") or os.environ.get("BOT_TOKEN")

            if poster_url and not poster_path.exists():
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(poster_url) as resp:
                            if resp.status == 200:
                                poster_path.write_bytes(await resp.read())
                except Exception:
                    pass

            existing_ep = find_existing_episode_in_folder(target_folder, dest_file, ep_num)
            if existing_ep:
                dest_file = existing_ep
                logger.info(f"Файл {dest_file.name} вже існує на Mac. Пропуск завантаження, відправляю картку.")
            else:
                logger.info(f"Початок локального завантаження 4K на Mac SSD: {f_name}")
                await edit_tg_status(
                    bot_token,
                    chat_id,
                    msg_id,
                    (
                        f"📥 <b>Завантаження відео на Mac SSD (FEX.NET)...</b>\n\n"
                        f"🎬 <b>Аніме:</b> {title} [Серія {ep_num}]\n"
                        f"📄 <b>Файл:</b> <code>{f_name}</code>\n"
                        f"📂 <b>Папка:</b> <code>{folder_name}</code>\n\n"
                        f"<code>[▱▱▱▱▱▱▱▱▱▱] 1.0%</code>\n"
                        f"📦 <b>Підключення до сервера FEX.NET...</b>\n"
                        f"⚡️ <i>Статус: MacAgent прийняв завдання, починаю скачування...</i>"
                    ),
                )
                last_tg_update = 0.0
                dl_start_ts = time.time()
                spin_idx = 0
                spinners = ["⏳", "⚡️", "🚀", "📥"]

                async def on_mac_progress(downloaded: int, total: int):
                    nonlocal last_tg_update, spin_idx
                    now = time.time()
                    if ((now - last_tg_update) >= 2.0 or (total and downloaded >= total)) and bot_token and chat_id and msg_id:
                        last_tg_update = now
                        pct = (downloaded / total * 100) if total else 0
                        bar_blocks = min(10, max(0, int(pct / 10)))
                        bar = "▰" * bar_blocks + "▱" * (10 - bar_blocks)
                        d_mb = downloaded / (1024 * 1024)
                        t_mb = total / (1024 * 1024)
                        elapsed = max(0.5, now - dl_start_ts)
                        speed_mb = d_mb / elapsed
                        rem_sec = ((t_mb - d_mb) / speed_mb) if speed_mb > 0.05 and total else 0
                        eta_str = format_eta(rem_sec)
                        sp = spinners[spin_idx % len(spinners)]
                        spin_idx += 1
                        text = (
                            f"{sp} <b>Завантаження на Mac SSD (4K / 1080p)...</b>\n\n"
                            f"🎬 <b>Аніме:</b> {title} [Серія {ep_num}]\n"
                            f"📄 <b>Файл:</b> <code>{f_name}</code>\n"
                            f"📂 <b>Папка:</b> <code>{folder_name}</code>\n\n"
                            f"<code>[{bar}] {pct:.1f}%</code>\n"
                            f"📦 <b>{d_mb:.1f} МБ / {t_mb:.1f} МБ</b>\n"
                            f"🚀 <b>Швидкість:</b> {speed_mb:.1f} МБ/с • ⏱ <b>Залишилось:</b> {eta_str}"
                        )
                        await edit_tg_status(bot_token, chat_id, msg_id, text)

                try:
                    await FexDownloader.download_file(dl_url, dest_file, token=tok, progress_callback=on_mac_progress)
                    set_macos_tag(dest_file, TAG_NAME, TAG_COLOR)
                    set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)
                except Exception as e_dl:
                    logger.error(f"Помилка FEX завантаження на Mac: {e_dl}")
                    await edit_tg_status(
                        bot_token,
                        chat_id,
                        msg_id,
                        f"❌ <b>Помилка завантаження на Mac:</b>\n<code>{html.escape(str(e_dl))}</code>",
                    )
                    return True

            if not poster_path.exists():
                try:
                    subprocess.run([
                        "/opt/homebrew/bin/ffmpeg", "-y", "-ss", "00:03:00",
                        "-i", str(dest_file), "-vframes", "1", "-q:v", "2", str(poster_path)
                    ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
                    pass

            logger.info(f"Успішно завантажено на Mac: {dest_file.name} з тегом {TAG_NAME}")
            await send_completed_anime_card(
                bot_token=bot_token,
                chat_id=chat_id,
                msg_id=msg_id,
                dest_file=dest_file,
                poster_path=poster_path,
                title=title,
                ep_num=ep_num,
                quality=quality,
                dub_sub=dub_sub,
            )
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
            raw_dub_sub = data.get("dub_sub", "")
            fn_up = f_name.upper()
            if raw_dub_sub in ("", "Озвучення + Субтитри", "Озвучення (DUB) або Субтитри (SUB)"):
                if any(k in fn_up for k in ("MVO", "DUB", "ОЗВУЧ")):
                    dub_sub = "Озвучення (DUB)"
                else:
                    dub_sub = "Субтитри (SUB)"
            else:
                dub_sub = raw_dub_sub
            ep_num = data.get("episode", 1)

            target_folder = DOWNLOADS_DIR / folder_name
            target_folder.mkdir(parents=True, exist_ok=True)
            dest_file = target_folder / f_name
            poster_path = target_folder / "poster.jpg"
            bot_token = BOT_TOKEN or os.environ.get("ANIME_BOT_TOKEN") or os.environ.get("BOT_TOKEN")

            existing_ep = find_existing_episode_in_folder(target_folder, dest_file, ep_num)
            if existing_ep:
                dest_file = existing_ep
                logger.info(f"Файл {dest_file.name} вже існує на Mac. Відправляю картку.")
            else:
                if not mtproto_downloader:
                    logger.error("MTProto не налаштований на MacAgent!")
                    await edit_tg_status(
                        bot_token,
                        chat_id,
                        msg_id,
                        "❌ <b>Помилка:</b> MTProto клієнт на Mac не налаштований (перевірте TELEGRAM_API_ID та TELEGRAM_API_HASH).",
                    )
                    return True

                logger.info(f"Початок MTProto завантаження з Telegram на Mac SSD: {f_name}")
                await edit_tg_status(
                    bot_token,
                    chat_id,
                    msg_id,
                    (
                        f"📥 <b>Завантаження відео з Telegram (MTProto)...</b>\n\n"
                        f"🎬 <b>Аніме:</b> {title} [Серія {ep_num}]\n"
                        f"📄 <b>Файл:</b> <code>{f_name}</code>\n"
                        f"📂 <b>Папка:</b> <code>{folder_name}</code>\n\n"
                        f"<code>[▱▱▱▱▱▱▱▱▱▱] 1.0%</code>\n"
                        f"📦 <b>Підключення до серверів Telegram...</b>\n"
                        f"⚡️ <i>Статус: MacAgent прийняв завдання, починаю скачування на SSD...</i>"
                    ),
                )
                last_tg_update = 0.0
                dl_start_ts = time.time()
                spin_idx = 0
                spinners = ["⏳", "⚡️", "🚀", "📥"]

                async def on_tg_mac_progress(downloaded: int, total: int):
                    nonlocal last_tg_update, spin_idx
                    now = time.time()
                    if ((now - last_tg_update) >= 2.0 or (total and downloaded >= total)) and bot_token and chat_id and msg_id:
                        last_tg_update = now
                        pct = (downloaded / total * 100) if total else 0
                        bar_blocks = min(10, max(0, int(pct / 10)))
                        bar = "▰" * bar_blocks + "▱" * (10 - bar_blocks)
                        d_mb = downloaded / (1024 * 1024)
                        t_mb = total / (1024 * 1024)
                        elapsed = max(0.5, now - dl_start_ts)
                        speed_mb = d_mb / elapsed
                        rem_sec = ((t_mb - d_mb) / speed_mb) if speed_mb > 0.05 and total else 0
                        eta_str = format_eta(rem_sec)
                        sp = spinners[spin_idx % len(spinners)]
                        spin_idx += 1
                        text = (
                            f"{sp} <b>Завантаження з Telegram на Mac SSD...</b>\n\n"
                            f"🎬 <b>Аніме:</b> {title} [Серія {ep_num}]\n"
                            f"📄 <b>Файл:</b> <code>{f_name}</code>\n"
                            f"📂 <b>Папка:</b> <code>{folder_name}</code>\n\n"
                            f"<code>[{bar}] {pct:.1f}%</code>\n"
                            f"📦 <b>{d_mb:.1f} МБ / {t_mb:.1f} МБ</b>\n"
                            f"🚀 <b>Швидкість:</b> {speed_mb:.1f} МБ/с • ⏱ <b>Залишилось:</b> {eta_str}"
                        )
                        await edit_tg_status(bot_token, chat_id, msg_id, text)

                try:
                    await mtproto_downloader.download_message_video(
                        chat_id=chat_id,
                        message_id=source_msg_id,
                        destination_path=dest_file,
                        progress_callback=on_tg_mac_progress,
                    )
                    set_macos_tag(dest_file, TAG_NAME, TAG_COLOR)
                    set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)
                except Exception as e_tg_dl:
                    logger.error(f"Помилка MTProto завантаження на Mac: {e_tg_dl}")
                    await edit_tg_status(
                        bot_token,
                        chat_id,
                        msg_id,
                        f"❌ <b>Помилка завантаження з Telegram на Mac:</b>\n<code>{html.escape(str(e_tg_dl))}</code>",
                    )
                    return True

            if not poster_path.exists():
                try:
                    subprocess.run([
                        "/opt/homebrew/bin/ffmpeg", "-y", "-ss", "00:03:00",
                        "-i", str(dest_file), "-vframes", "1", "-q:v", "2", str(poster_path)
                    ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
                    pass

            await send_completed_anime_card(
                bot_token=bot_token,
                chat_id=chat_id,
                msg_id=msg_id,
                dest_file=dest_file,
                poster_path=poster_path,
                title=title,
                ep_num=ep_num,
                quality=quality,
                dub_sub=dub_sub,
            )
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
