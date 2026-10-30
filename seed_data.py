"""Початковий список тайтлів (Seed List v3.0) для автоматичного моніторингу."""
import logging
from typing import Dict, List, Optional

from channel_monitor import add_subscription, get_subscription_by_title, set_subscription_sources, init_channel_monitor_db

logger = logging.getLogger("SeedData")

# title, pref_type, fex_url, thread_url
SEED_TITLES: List[Dict[str, Optional[str]]] = [
    {"title": "Вигнаний важкоозброєний лицар стає непереможним завдяки знанням гри", "pref": "dub",
     "fex": None, "thread": "https://t.me/c/mediareser/364"},
    {"title": "Маг, що повернувся, — особливий", "pref": "sub",
     "fex": "https://fex.net/s/rntczlx", "thread": None},
    {"title": "Я переродився гобліном, питання є?", "pref": "any",
     "fex": "https://fex.net/s/tckn4vo", "thread": None},
    {"title": "Переродження мечем", "pref": "any",
     "fex": "https://fex.net/s/xlcmnmt", "thread": None},
    {"title": "Божевільна Ніа Лістон", "pref": "any",
     "fex": "https://fex.net/s/tandrve", "thread": None},
    {"title": "Готель нелюдей", "pref": "any",
     "fex": "https://fex.net/s/dbkffm5", "thread": None},
    # Для «Ао Аші» точне FEX-посилання не вказано в ТЗ — додасться через Forward / вручну
    {"title": "Ао Аші", "pref": "any", "fex": None, "thread": None},
    {"title": "З’явився дикий останній бос", "pref": "any",
     "fex": "https://fex.net/s/klakz3l", "thread": "https://t.me/c/mediareser/12051"},
    {"title": "Військові хроніки Ромелії", "pref": "any",
     "fex": "https://fex.net/s/5a8oskn", "thread": None},
]


def seed_subscriptions(chat_id: int) -> int:
    """Один раз заливає Seed List для користувача. Існуючі підписки не чіпає. Повертає кількість доданих."""
    init_channel_monitor_db()
    added = 0
    for item in SEED_TITLES:
        if get_subscription_by_title(chat_id, item["title"]):
            # Підписка вже є — лише доповнюємо джерела з Seed List
            set_subscription_sources(chat_id, item["title"], fex_url=item["fex"], thread_url=item["thread"])
            continue
        add_subscription(chat_id, item["title"], channel="ridnyiholos", pref_type=item["pref"] or "any")
        set_subscription_sources(chat_id, item["title"], fex_url=item["fex"], thread_url=item["thread"])
        added += 1
    logger.info(f"Seed List: додано {added} тайтлів для {chat_id}")
    return added
