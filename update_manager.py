import asyncio
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger("UpdateManager")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "file_registry.db"
ROOT_DIR = BASE_DIR.parent
CURRENT_VERSION = "2.5.0"
GITHUB_REPO = "vadym-des/anime-downloader-bot"

# Список спостережуваних файлів коду для автоперезапуску та хмарного Hot-Pull
SYNC_CODE_FILES = [
    "bot.py",
    "channel_monitor.py",
    "fex.py",
    "progress.py",
    "update_manager.py",
    "notion_tasks.py",
    "seed_data.py",
    "sync_server.py",
    "mac_sync_schema.py",
    "config.py",
    "mac_agent.py",
]
WATCH_FILES = [BASE_DIR / fn for fn in SYNC_CODE_FILES]


def init_update_db():
    """Створює або розширює таблицю для збереження версій та історії оновлень бота."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_version_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_version TEXT NOT NULL,
                    last_git_hash TEXT,
                    last_notified_at REAL,
                    release_notes TEXT,
                    pending_notify INTEGER DEFAULT 0,
                    pending_reason TEXT
                )
            """)
            cursor = conn.execute("PRAGMA table_info(bot_version_state)")
            cols = [c[1] for c in cursor.fetchall()]
            if "pending_notify" not in cols:
                conn.execute("ALTER TABLE bot_version_state ADD COLUMN pending_notify INTEGER DEFAULT 0")
            if "pending_reason" not in cols:
                conn.execute("ALTER TABLE bot_version_state ADD COLUMN pending_reason TEXT")
            if "restart_chat_id" not in cols:
                conn.execute("ALTER TABLE bot_version_state ADD COLUMN restart_chat_id INTEGER")
            if "restart_msg_id" not in cols:
                conn.execute("ALTER TABLE bot_version_state ADD COLUMN restart_msg_id INTEGER")
            conn.commit()
    except Exception as e:
        logger.error(f"Помилка створення таблиці оновлень: {e}")


def mark_pending_update(reason: str = "Оновлення коду бота"):
    """Записує в БД прапорець, що заплановано перезапуск через оновлення."""
    try:
        init_update_db()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE bot_version_state SET pending_notify = 1, pending_reason = ? WHERE id = 1",
                (reason,)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Помилка позначки оновлення в БД: {e}")


def get_current_git_info() -> Tuple[str, str]:
    """Повертає хеш останнього коміту та його повідомлення (з git або зі збереженого стану в БД)."""
    try:
        res = subprocess.run(
            ["git", "log", "-1", "--format=%h|%s"],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=True
        )
        parts = res.stdout.strip().split("|", 1)
        if len(parts) == 2 and parts[0]:
            return parts[0], parts[1]
    except Exception:
        pass
    try:
        init_update_db()
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT last_git_hash, release_notes FROM bot_version_state WHERE id = 1").fetchone()
            if row and row[0]:
                return row[0], row[1] or "Оновлення коду"
    except Exception:
        pass
    return "latest", "Свіже оновлення компонентів"


def save_manual_restart_request(chat_id: int, msg_id: int):
    """Зберігає ID повідомлення в БД, щоб після перезапуску процесу бот відредагував його зі звітом."""
    try:
        init_update_db()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE bot_version_state SET restart_chat_id = ?, restart_msg_id = ? WHERE id = 1",
                (chat_id, msg_id)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Помилка збереження restart_request: {e}")


def pop_manual_restart_request() -> Optional[Tuple[int, int]]:
    """Повертає (chat_id, msg_id), якщо бот щойно перезапустився за кнопкою «Рестарт бота», і очищає поля."""
    try:
        init_update_db()
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT restart_chat_id, restart_msg_id FROM bot_version_state WHERE id = 1"
            ).fetchone()
            if row and row[0] and row[1]:
                conn.execute(
                    "UPDATE bot_version_state SET restart_chat_id = NULL, restart_msg_id = NULL WHERE id = 1"
                )
                conn.commit()
                return int(row[0]), int(row[1])
    except Exception as e:
        logger.error(f"Помилка читання restart_request: {e}")
    return None


async def pull_latest_from_github() -> Tuple[bool, str, str]:
    """
    Завантажує найсвіжіші файли коду безпосередньо з GitHub (гілка main),
    щоб оновлення застосовувались за 1-2 секунди навіть без очікування Docker-білду на Railway.
    Повертає (updated: bool, short_sha: str, commit_message: str).
    """
    short_sha, commit_msg = get_current_git_info()
    updated_files = 0
    try:
        headers = {"Cache-Control": "no-cache", "User-Agent": "AnimeDownloaderBot-Updater/2.5"}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(
                f"https://api.github.com/repos/{GITHUB_REPO}/commits/main",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    full_sha = data.get("sha", "")
                    if full_sha:
                        short_sha = full_sha[:7]
                    c_msg = (data.get("commit") or {}).get("message", "")
                    if c_msg:
                        commit_msg = c_msg.splitlines()[0].strip()

            ts = int(time.time())
            for fn in SYNC_CODE_FILES:
                raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/{fn}?t={ts}"
                async with session.get(raw_url, timeout=aiohttp.ClientTimeout(total=8)) as f_resp:
                    if f_resp.status == 200:
                        new_text = await f_resp.text()
                        target_path = BASE_DIR / fn
                        old_text = target_path.read_text(encoding="utf-8") if target_path.exists() else ""
                        if new_text and new_text != old_text:
                            target_path.write_text(new_text, encoding="utf-8")
                            updated_files += 1

        init_update_db()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE bot_version_state SET last_version = ?, last_git_hash = ?, release_notes = ?, last_notified_at = ? WHERE id = 1",
                (CURRENT_VERSION, short_sha, commit_msg, time.time())
            )
            conn.commit()
    except Exception as e:
        logger.warning(f"Помилка pull_latest_from_github: {e}")

    return (updated_files > 0), short_sha, commit_msg


def check_for_new_release() -> Optional[Dict[str, str]]:
    """
    Перевіряє при старті бота, чи відбулося оновлення версії, коду чи запланований хот-релоад.
    Якщо так — повертає деталі релізу для відправки сповіщення.
    """
    init_update_db()
    current_hash, commit_msg = get_current_git_info()

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM bot_version_state WHERE id = 1").fetchone()

        if not row:
            # Перший запуск із цією системою
            conn.execute(
                "INSERT INTO bot_version_state (id, last_version, last_git_hash, last_notified_at, release_notes, pending_notify, pending_reason) VALUES (1, ?, ?, ?, ?, 0, NULL)",
                (CURRENT_VERSION, current_hash, time.time(), commit_msg)
            )
            conn.commit()
            return {
                "version": CURRENT_VERSION,
                "git_hash": current_hash,
                "title": "Вітаємо в оновленому Anime Downloader Bot!",
                "notes": [
                    "🛰 Інтерактивні картки онґоїнгів з фільтрами Озвучення / Саби / Будь-що",
                    "📁 Жива вітрина та каталог актуальних аніме сезону з @ridnyiholos",
                    "📥 Пакетне завантаження всіх серій тайтлу в один клік",
                    "🔕 Тихий режим у робочий час (09:00 - 18:30) з вечірнім дайджестом",
                    "📦 Авто-Binge ліміт дисків Mac SSD (самоочищення старих серій)",
                    "🚀 Автоматичне оновлення бота без потреби ручного перезапуску"
                ]
            }

        prev_version = row["last_version"]
        prev_hash = row["last_git_hash"]
        pending_notify = row["pending_notify"] if "pending_notify" in row.keys() else 0
        pending_reason = row["pending_reason"] if "pending_reason" in row.keys() else None

        # Якщо версія, коміт змінилися або був запланований хот-релоад
        if prev_version != CURRENT_VERSION or prev_hash != current_hash or pending_notify == 1:
            conn.execute(
                "UPDATE bot_version_state SET last_version = ?, last_git_hash = ?, last_notified_at = ?, release_notes = ?, pending_notify = 0, pending_reason = NULL WHERE id = 1",
                (CURRENT_VERSION, current_hash, time.time(), commit_msg)
            )
            conn.commit()

            notes = [
                "🛡 Захист від переривання завантажень: активні файли ніколи не обриваються при пошуку та повідомленнях",
                "🎯 Повна підтримка 4K / 1080p роздач FEX.NET для онґоїнгів (глибокий пошук усіх епізодів від #1 до N)",
                "🎬 Кнопки скачування кожної окремої серії в один клік безпосередньо з картки аніме",
                "🛑 Жива кнопка скасування активного завантаження без зависань",
                "🚀 Авто-оновлення: бот миттєво сповіщає про вдале оновлення компонентів"
            ]
            if pending_reason:
                notes.insert(0, f"⚡️ Причина оновлення: {pending_reason}")
            elif commit_msg and not commit_msg.startswith("Merge"):
                notes.insert(0, f"📌 Останні зміни: {commit_msg}")

            return {
                "version": CURRENT_VERSION,
                "git_hash": current_hash,
                "title": f"Бот успішно оновлено до версії {CURRENT_VERSION}!",
                "notes": notes
            }

    return None


def get_latest_file_mtimes() -> Dict[Path, float]:
    """Повертає час останньої модифікації файлів коду."""
    mtimes = {}
    for f in WATCH_FILES:
        if f.exists():
            mtimes[f] = f.stat().st_mtime
    return mtimes


async def auto_reload_watcher():
    """
    Фоновий процес (Hot-Reload Watcher):
    Стежить за файлами коду. Якщо будь-який файл змінено (наприклад, збережено новий код):
    1. Позначає оновлення в базі (mark_pending_update).
    2. Перевіряє, чи не виконуються активні завантаження користувача (ACTIVE_DOWNLOAD_TASKS).
       Якщо так — терпляче очікує завершення завантаження всіх файлів, щоб нічого не перервати!
    3. Плавно виходить (sys.exit(0)).
    Служба launchd (KeepAlive=true) моментально перезапускає бота за 0.5 секунди,
    і бот відправляє сповіщення про успішне оновлення в Telegram!
    """
    initial_mtimes = get_latest_file_mtimes()
    last_gh_check = time.time()
    known_sha, _ = get_current_git_info()
    logger.info(f"Auto-Reload Watcher активовано (відстеження локальних змін та GitHub main, поточний SHA={known_sha}).")

    while True:
        try:
            await asyncio.sleep(4)
            current_mtimes = get_latest_file_mtimes()

            should_restart = False
            restart_reason = ""

            for f_path, last_time in initial_mtimes.items():
                cur_time = current_mtimes.get(f_path)
                if cur_time and cur_time > last_time:
                    should_restart = True
                    restart_reason = f"Оновлено файл {f_path.name}"
                    break

            # Перевіряємо GitHub кожні 20 секунд (миттєвий Hot-Pull для хмарного сервера Railway)
            if not should_restart and (time.time() - last_gh_check) >= 20.0:
                last_gh_check = time.time()
                try:
                    headers = {"Cache-Control": "no-cache", "User-Agent": "AnimeDownloaderBot-Updater/2.5"}
                    async with aiohttp.ClientSession(headers=headers) as session:
                        async with session.get(
                            f"https://api.github.com/repos/{GITHUB_REPO}/commits/main",
                            timeout=aiohttp.ClientTimeout(total=6)
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                remote_sha = (data.get("sha") or "")[:7]
                                if remote_sha and known_sha and remote_sha != known_sha:
                                    logger.info(f"Виявлено новий коміт на GitHub: {remote_sha} (був {known_sha}). Завантажую оновлення...")
                                    changed, new_sha, c_msg = await pull_latest_from_github()
                                    known_sha = new_sha
                                    if changed:
                                        should_restart = True
                                        restart_reason = f"GitHub Hot-Update #{new_sha}: {c_msg}"
                except Exception as e_gh:
                    logger.debug(f"Перевірка GitHub у auto_reload_watcher: {e_gh}")

            if should_restart:
                logger.info(f"Підготовка до авто-перезапуску ({restart_reason})...")
                mark_pending_update(restart_reason)

                try:
                    from bot import ACTIVE_DOWNLOAD_TASKS
                    if ACTIVE_DOWNLOAD_TASKS:
                        logger.info(
                            f"Виявлено {len(ACTIVE_DOWNLOAD_TASKS)} активних завантажень. "
                            f"Перезапуск відкладено до повного завершення передачі файлів..."
                        )
                        while ACTIVE_DOWNLOAD_TASKS:
                            await asyncio.sleep(2.0)
                        logger.info("Усі завантаження завершено. Продовжую перезапуск...")
                except Exception as e_chk:
                    logger.debug(f"Помилка перевірки ACTIVE_DOWNLOAD_TASKS: {e_chk}")

                await asyncio.sleep(1.0)
                os.execv(sys.executable, [sys.executable] + sys.argv)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Помилка у auto_reload_watcher: {e}")
