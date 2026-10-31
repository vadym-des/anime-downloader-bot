"""Початковий список тайтлів (Seed List v3.0) для автоматичного моніторингу."""
import logging
import sqlite3
from typing import Dict, List, Optional

from channel_monitor import (
    DB_PATH,
    add_subscription,
    get_subscription_by_title,
    set_subscription_sources,
    init_channel_monitor_db,
)

logger = logging.getLogger("SeedData")

# title, pref_type, fex_url, thread_url
SEED_TITLES: List[Dict[str, Optional[str]]] = [
    {"title": "Вигнаний важкоозброєний лицар стає непереможним завдяки знанням гри", "pref": "dub",
     "fex": "https://fex.net/uk/s/mleyd4o/6005554951", "thread": None},
    {"title": "Маг, що повернувся, — особливий", "pref": "dub",
     "fex": "https://fex.net/s/rntczlx", "thread": None},
    {"title": "Я переродився гобліном, питання є?", "pref": "dub",
     "fex": "https://fex.net/s/tckn4vo", "thread": None},
    {"title": "Переродження мечем", "pref": "dub",
     "fex": "https://fex.net/s/xlcmnmt", "thread": None},
    {"title": "Божевільна Ніа Лістон", "pref": "dub",
     "fex": "https://fex.net/s/tandrve", "thread": None},
    {"title": "Готель нелюдей", "pref": "dub",
     "fex": "https://fex.net/s/dbkffm5", "thread": None},
    # Ао Аші — виходить тільки із субтитрами [РГ SUB] у медіатеці
    {"title": "Ао Аші", "pref": "sub",
     "fex": None, "thread": "https://t.me/mediareser/12051"},
    {"title": "З’явився дикий останній бос", "pref": "dub",
     "fex": "https://fex.net/s/klakz3l", "thread": None},
    {"title": "Військові хроніки Ромелії", "pref": "dub",
     "fex": "https://fex.net/s/5a8oskn", "thread": None},
    {"title": "Весілля світлячка", "pref": "dub",
     "fex": "https://fex.net/s/mynzyl5", "thread": None},
]


def seed_subscriptions(chat_id: int) -> int:
    """Заливає та актуалізує Seed List для користувача. Повертає кількість доданих."""
    init_channel_monitor_db()
    added = 0
    for item in SEED_TITLES:
        existing = get_subscription_by_title(chat_id, item["title"])
        if existing:
            # Не перезаписуємо вибір користувача (pref_type), якщо він уже змінив його вручну в боті,
            # окрім випадку Ао Аші (яке суто SUB)
            cur_pref = "sub" if item["pref"] == "sub" else (existing.get("pref_type") or item["pref"] or "dub")
            add_subscription(chat_id, existing["title"], channel="ridnyiholos", pref_type=cur_pref)
            set_subscription_sources(chat_id, existing["title"], fex_url=item["fex"], thread_url=item["thread"])
            if item["thread"] is None and existing.get("thread_url"):
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute("UPDATE anime_subscriptions SET thread_url = NULL WHERE id = ?", (existing["id"],))
                    conn.commit()
            if item["fex"] is None and existing.get("fex_url"):
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute("UPDATE anime_subscriptions SET fex_url = NULL WHERE id = ?", (existing["id"],))
                    conn.commit()
            continue
        add_subscription(chat_id, item["title"], channel="ridnyiholos", pref_type=item["pref"] or "dub")
        set_subscription_sources(chat_id, item["title"], fex_url=item["fex"], thread_url=item["thread"])
        added += 1
    logger.info(f"Seed List: актуалізовано (додано {added}) тайтлів для {chat_id}")
    return added
