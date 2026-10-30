import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

# Завантажуємо змінні оточення виключно з локального .env проєкту anime-downloader-bot
env_file = BASE_DIR / ".env"
if env_file.exists():
    load_dotenv(dotenv_path=env_file, override=True)
else:
    load_dotenv()

# Токен нового бота (можна вказати в .env як ANIME_BOT_TOKEN або BOT_TOKEN)
BOT_TOKEN = os.getenv("ANIME_BOT_TOKEN") or os.getenv("BOT_TOKEN", "").strip()

# Базова папка для завантаження в Завантаженнях
DEFAULT_DOWNLOADS = Path.home() / "Downloads" / "(Завантажене аніме)"
DOWNLOADS_DIR = Path(os.getenv("ANIME_DOWNLOAD_DIR", str(DEFAULT_DOWNLOADS)))

# Тег macOS Finder
# Кольори: 0: Без кольору, 1: Сірий, 2: Зелений, 3: Фіолетовий, 4: Синій, 5: Жовтий, 6: Червоний, 7: Помаранчевий
TAG_NAME = os.getenv("ANIME_TAG_NAME", "На черзі глянути")
TAG_COLOR = int(os.getenv("ANIME_TAG_COLOR", "2"))  # 2 = Зелений

# Список дозволених Telegram User ID (для безпеки). Якщо пусто, дозволено всім, хто пише боту.
ALLOWED_USERS_RAW = os.getenv("ALLOWED_USERS", "").strip()
ALLOWED_USERS = set(int(uid.strip()) for uid in ALLOWED_USERS_RAW.split(",") if uid.strip().isdigit())

# Ключі для MTProto (Telethon) для завантаження важких файлів без ліміту 20 МБ
API_ID_RAW = os.getenv("TELEGRAM_API_ID", "").strip()
TELEGRAM_API_ID = int(API_ID_RAW) if API_ID_RAW.isdigit() else None
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip() or None

