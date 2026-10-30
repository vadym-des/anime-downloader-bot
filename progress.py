import time
from typing import Optional, Tuple


def format_bytes(bytes_count: float) -> str:
    """Форматує кількість байтів у зручний для читання вигляд."""
    if bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.1f} КБ"
    elif bytes_count < 1024 * 1024 * 1024:
        return f"{bytes_count / (1024 * 1024):.1f} МБ"
    return f"{bytes_count / (1024 * 1024 * 1024):.2f} ГБ"


def format_seconds(seconds: float) -> str:
    """Форматує секунди у хвилини та секунди (наприклад: 1хв 24с)."""
    secs = int(max(0, seconds))
    if secs < 60:
        return f"{secs}с"
    mins = secs // 60
    rem_secs = secs % 60
    return f"{mins}хв {rem_secs}с"


def render_progress_bar(
    downloaded: int,
    total: int,
    speed_bps: float,
    eta_seconds: Optional[float] = None
) -> str:
    """
    Генерує візуальний блок прогресу для повідомлення в Telegram:
    ▰▰▰▰▰▰▱▱▱▱ 60%
    📦 355.0 МБ / 591.9 МБ • ⚡️ 18.4 МБ/с • ⏱ Залишилось: ~12с
    """
    percent = (downloaded / total * 100) if total > 0 else 0.0
    percent = max(0.0, min(100.0, percent))

    total_blocks = 10
    filled = int(round((percent / 100.0) * total_blocks))
    empty = total_blocks - filled
    bar = "▰" * filled + "▱" * empty

    down_str = format_bytes(downloaded)
    total_str = format_bytes(total) if total > 0 else "..."
    speed_str = f"⚡️ {format_bytes(speed_bps)}/с" if speed_bps > 0 else "⚡️ Розрахунок..."

    eta_str = f" • ⏱ ~{format_seconds(eta_seconds)}" if eta_seconds is not None and eta_seconds > 0 else ""

    return (
        f"<code>[{bar}]</code> <b>{percent:.1f}%</b>\n"
        f"📦 <code>{down_str} / {total_str}</code> • {speed_str}{eta_str}"
    )


class DownloadProgressTracker:
    """
    Відстежує прогрес, швидкість та ETA завантаження,
    і контролює частоту оновлення повідомлень у Telegram (throttling).
    """

    def __init__(self, interval_seconds: float = 2.0):
        self.interval = interval_seconds
        self.start_time = time.time()
        self.last_update_time = 0.0
        self.last_downloaded = 0
        self.last_speed = 0.0

    def calculate_stats(self, downloaded: int, total: int) -> Tuple[float, Optional[float]]:
        """Обчислює згладжену швидкість та орієнтовний час до завершення (ETA)."""
        now = time.time()
        elapsed_since_last = now - self.last_update_time
        
        if elapsed_since_last >= 1.0:
            bytes_delta = downloaded - self.last_downloaded
            current_speed = bytes_delta / elapsed_since_last
            # Експоненційне згладжування швидкості для плавного відображення
            if self.last_speed > 0:
                self.last_speed = 0.7 * current_speed + 0.3 * self.last_speed
            else:
                self.last_speed = current_speed
            self.last_downloaded = downloaded

        eta = None
        if self.last_speed > 0 and total > downloaded:
            eta = (total - downloaded) / self.last_speed

        return self.last_speed, eta

    def should_update_telegram(self, force: bool = False) -> bool:
        """Перевіряє, чи пройшов мінімальний інтервал для редагування повідомлення."""
        now = time.time()
        if force or (now - self.last_update_time >= self.interval):
            self.last_update_time = now
            return True
        return False
