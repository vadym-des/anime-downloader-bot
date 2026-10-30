#!/usr/bin/env python3
"""
Mac Local Downloader & Library Agent
Автономно працює на macOS:
1. Сканує папку ~/Downloads/(Завантажене аніме).
2. Відправляє список файлів та вільне місце на сервер Railway (кожні 10-20 сек).
3. Отримує команди від сервера (завантажити серію, видалити файл, відкрити у Finder/IINA).
4. Завантажує 4K/1080p серії на Mac та ставить кольоровий тег Finder.
"""

import asyncio
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
from config import BOT_TOKEN, DOWNLOADS_DIR, TAG_NAME, TAG_COLOR
from fex import FexDownloader
from tagger import set_macos_tag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [MacAgent] %(levelname)s - %(message)s"
)
logger = logging.getLogger("MacAgent")

VIDEO_EXTS = (".mkv", ".mp4", ".webm", ".avi", ".mov", ".m4v")
START_TIME = time.time()

# URL сервера (встановлюється в .env як RAILWAY_API_URL або береться публічний домен Railway)
SERVER_URL = os.getenv("RAILWAY_API_URL", "").rstrip("/")
SYNC_SECRET = os.getenv("MAC_SYNC_SECRET") or (BOT_TOKEN[:16] if BOT_TOKEN else "default_secret")


def scan_local_anime() -> List[Dict[str, Any]]:
    """Сканує папку ~/Downloads/(Завантажене аніме) та повертає чистий список файлів."""
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    try:
        for p in DOWNLOADS_DIR.rglob("*"):
            if p.is_file() and p.suffix.lower() in VIDEO_EXTS and not p.name.endswith(".part"):
                folder = p.parent.name if p.parent != DOWNLOADS_DIR else "Різне"
                rel_path = str(p.relative_to(DOWNLOADS_DIR))
                f_key = hashlib.md5(str(p).encode("utf-8")).hexdigest()[:10]
                results.append({
                    "key": f_key,
                    "folder": folder,
                    "name": p.name,
                    "path": str(p),
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


async def execute_command(cmd: Dict[str, Any]) -> bool:
    """Виконує отриману від сервера команду на MacBook."""
    action = cmd.get("action")
    payload = cmd.get("payload", "")
    logger.info(f"Виконання команди від сервера: {action} (payload: {payload})")

    try:
        if action == "reveal_folder":
            subprocess.run(["open", str(DOWNLOADS_DIR)], check=False)
            return True

        elif action == "open_iina":
            target_path = Path(payload)
            if target_path.exists():
                subprocess.run(["open", "-a", "IINA", str(target_path)], check=False)
                return True

        elif action == "delete_file":
            target_path = Path(payload)
            if target_path.exists():
                target_path.unlink(missing_ok=True)
                # Перевіряємо чи папку не треба прибрати
                parent_dir = target_path.parent
                if parent_dir != DOWNLOADS_DIR:
                    remaining = [f for f in parent_dir.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTS]
                    if not remaining:
                        shutil.rmtree(parent_dir, ignore_errors=True)
                return True

        elif action == "clean_all":
            for item in DOWNLOADS_DIR.iterdir():
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                elif item.is_file() and item.suffix.lower() in VIDEO_EXTS:
                    item.unlink(missing_ok=True)
            return True

        elif action == "download_fex":
            # payload у форматі JSON: {url, title, episode, token, folder}
            data = json.loads(payload)
            dl_url = data.get("url")
            title = data.get("title", "Аніме")
            folder_name = data.get("folder") or title
            f_name = data.get("filename") or f"episode_{data.get('episode', 1)}.mkv"
            tok = data.get("token")

            target_folder = DOWNLOADS_DIR / folder_name
            target_folder.mkdir(parents=True, exist_ok=True)
            dest_file = target_folder / f_name

            if dest_file.exists() and dest_file.stat().st_size > 10 * 1024 * 1024:
                logger.info(f"Файл {dest_file.name} вже існує на Mac. Пропуск.")
                return True

            logger.info(f"Початок локального завантаження 4K: {f_name}")
            await FexDownloader.download_file(dl_url, dest_file, token=tok)
            set_macos_tag(dest_file, TAG_NAME, TAG_COLOR)
            set_macos_tag(target_folder, TAG_NAME, TAG_COLOR)
            logger.info(f"Успішно завантажено на Mac: {dest_file.name} з тегом {TAG_NAME}")
            return True

    except Exception as e:
        logger.error(f"Помилка виконання команди {action}: {e}")
        return False

    return False


async def run_agent_loop():
    logger.info("Запуск локального Mac-агента...")
    logger.info(f"Папка завантажень на Mac: {DOWNLOADS_DIR}")

    if not SERVER_URL:
        logger.warning(
            "УВАГА: Не вказано RAILWAY_API_URL у .env або конфігу!\n"
            "Вкажіть публічну адресу вашого сервера на Railway (наприклад: https://xxx.up.railway.app)."
        )

    executed_ids: List[int] = []

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if SERVER_URL:
                    device_name, free_gb, uptime_str = get_mac_stats()
                    files = scan_local_anime()

                    payload = {
                        "device_name": device_name,
                        "free_disk_gb": free_gb,
                        "downloads_folder": str(DOWNLOADS_DIR),
                        "uptime_str": uptime_str,
                        "files": files,
                        "executed_command_ids": executed_ids,
                    }

                    headers = {
                        "X-Sync-Secret": SYNC_SECRET,
                        "Content-Type": "application/json"
                    }

                    async with session.post(
                        f"{SERVER_URL}/api/mac/heartbeat",
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            executed_ids.clear()
                            commands = data.get("commands", [])
                            for cmd in commands:
                                success = await execute_command(cmd)
                                if success:
                                    executed_ids.append(cmd["id"])
                        else:
                            text = await resp.text()
                            logger.debug(f"Сервер повернув статус {resp.status}: {text}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Помилка пінгу сервера (можливо Mac без мережі): {e}")

            await asyncio.sleep(15)


if __name__ == "__main__":
    try:
        asyncio.run(run_agent_loop())
    except KeyboardInterrupt:
        logger.info("MacAgent зупинено.")

