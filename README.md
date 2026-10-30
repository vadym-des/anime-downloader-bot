# 🎬 Anime Downloader Bot (Cloud Dispatcher & Mac Worker)

Персональний агентний менеджер та автозавантажувач аніме за гібридною архітектурою (хмарний сервер для цілодобового моніторингу + локальний Mac для зберігання 4K/1080p файлів та перегляду).

## 🚀 Швидкий запуск на Railway (Хмарний сервер 24/7)

1. Завантажте цей репозиторій на свій GitHub:
   ```bash
   git remote add origin https://github.com/vadym-des/anime-downloader-bot.git
   git branch -M main
   git push -u origin main
   ```
2. Увійдіть у [Railway](https://railway.com) і натисніть **Deploy from GitHub Repo**.
3. Виберіть репозиторій `anime-downloader-bot`.
4. У вкладці **Variables** додайте обов'язкові змінні:
   - `BOT_TOKEN` — токен вашого Telegram бота (отриманий у @BotFather).
   - `ALLOWED_USERS` — ваш Telegram ID (наприклад: `453079694`).

## 📁 Структура проєкту

- `bot.py` — головний контролер інтерфейсу Telegram, черги та взаємодії.
- `channel_monitor.py` — модуль відстеження релізів, збереження та підписок SQLite.
- `fex.py` — робота з FEX.NET (4K релізи, парсинг оновлень серій).
- `seed_data.py` — стартовий список тайтлів для автоматичного відстеження.
- `Dockerfile` & `railway.json` — оптимізована конфігурація для безперебійної роботи в контейнері Railway.
