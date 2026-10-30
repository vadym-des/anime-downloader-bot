import asyncio
import logging
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("UpdateManager")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "file_registry.db"
ROOT_DIR = BASE_DIR.parent
CURRENT_VERSION = "2.4.1"

# Список спостережуваних файлів коду для автоперезапуску
WATCH_FILES = [
    BASE_DIR / "bot.py",
    BASE_DIR / "channel_monitor.py",
    BASE_DIR / "fex.py",
    BASE_DIR / "progress.py",
    BASE_DIR / "update_manager.py",
    BASE_DIR / "notion_tasks.py",
]


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
    """Повертає хеш останнього коміту та його повідомлення."""
    try:
        res = subprocess.run(
            ["git", "log", "-1", "--format=%h|%s"],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=True
        )
        parts = res.stdout.strip().split("|", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return res.stdout.strip()[:7], "Оновлення коду"
    except Exception:
        return "latest", "Свіже оновлення компонентів"


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
    logger.info("Auto-Reload Watcher активовано (відстеження змін коду 24/7).")

    while True:
        try:
            await asyncio.sleep(4)
            current_mtimes = get_latest_file_mtimes()

            for f_path, last_time in initial_mtimes.items():
                cur_time = current_mtimes.get(f_path)
                if cur_time and cur_time > last_time:
                    logger.info(f"Файл {f_path.name} було змінено. Підготовка до авто-перезапуску...")
                    mark_pending_update(f"Оновлено файл {f_path.name}")

                    # Безпечна перевірка: чи тривають активні завантаження?
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

                    await asyncio.sleep(1.0)  # пауза для завершення запису
                    sys.exit(0)  # launchd автоматично підніме новий процес
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Помилка у auto_reload_watcher: {e}")
