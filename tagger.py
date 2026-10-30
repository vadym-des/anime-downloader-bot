import logging
import plistlib
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def set_macos_tag(target_path: Path | str, tag_name: str, color_code: int = 2) -> bool:
    """
    Встановлює кольоровий тег Finder на файл або папку в macOS.
    Кольори Finder:
    0 - Без кольору
    1 - Сірий
    2 - Зелений
    3 - Фіолетовий
    4 - Синій
    5 - Жовтий
    6 - Червоний
    7 - Помаранчевий
    """
    target = Path(target_path)
    if not target.exists():
        logger.warning(f"Неможливо встановити тег: шлях '{target}' не знайдено.")
        return False

    try:
        # macOS зберігає теги у форматі "Назва\nКодКольору" всередині бінарного plist
        tag_entry = f"{tag_name}\n{color_code}"
        tag_binary = plistlib.dumps([tag_entry], fmt=plistlib.FMT_BINARY)
        hex_data = tag_binary.hex()

        # Використовуємо стандартну утиліту macOS xattr
        subprocess.run(
            ["/usr/bin/xattr", "-wx", "com.apple.metadata:_kMDItemUserTags", hex_data, str(target)],
            check=True,
            capture_output=True
        )
        logger.info(f"Успішно встановлено тег '{tag_name}' на '{target.name}'")
        return True
    except FileNotFoundError:
        logger.warning("Утиліту /usr/bin/xattr не знайдено (можливо, система не macOS). Пропускаємо тегування.")
        return False
    except Exception as e:
        logger.error(f"Помилка при встановленні тегу на '{target}': {e}")
        return False
