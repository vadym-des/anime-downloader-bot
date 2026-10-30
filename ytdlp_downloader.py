import asyncio
import logging
from pathlib import Path
from typing import Optional, Tuple
import yt_dlp

logger = logging.getLogger(__name__)


def _run_ytdlp(url: str, output_dir: Path) -> Tuple[bool, str, Optional[str]]:
    """Синхронна функція завантаження через yt-dlp."""
    output_dir.mkdir(parents=True, exist_ok=True)
    outtmpl = str(output_dir / "%(title)s.%(ext)s")

    ydl_opts = {
        "outtmpl": outtmpl,
        "format": "bestvideo+bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "noplaylist": True,
        "merge_output_format": "mp4",
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if not info:
                return False, "Відеопотік не знайдено на цій сторінці (плеєр може вимагати авторизації або прямий iframe-лінк).", None
            filename = ydl.prepare_filename(info)
            # Якщо було конвертовано в mp4
            p = Path(filename)
            mp4_path = p.with_suffix(".mp4")
            final_path = mp4_path if mp4_path.exists() else p

            title = info.get("title", final_path.name)
            return True, str(final_path), title
    except Exception as e:
        logger.error(f"yt-dlp помилка: {e}")
        return False, str(e), None


async def download_web_video(url: str, output_dir: Path) -> Tuple[bool, str, Optional[str]]:
    """Асинхронна обгортка для завантаження потокових відео або сайтів."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _run_ytdlp, url, output_dir)
