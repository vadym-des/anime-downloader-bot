import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import Callable, Optional

from telethon import TelegramClient

logger = logging.getLogger(__name__)


class TelegramMtprotoDownloader:
    """
    Завантажувач великих файлів із Telegram через нативний протокол MTProto (Telethon).
    Знімає обмеження HTTP Bot API у 20 МБ (дозволяє до 2000 МБ / 2 ГБ).
    """

    def __init__(self, api_id: int, api_hash: str, bot_token: str, session_dir: Path):
        self.api_id = api_id
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.session_path = str(session_dir / "bot_mtproto_session")
        self.client: Optional[TelegramClient] = None
        self._lock = asyncio.Lock()

    async def get_client(self) -> TelegramClient:
        async with self._lock:
            if self.client is None or not self.client.is_connected():
                self.client = TelegramClient(self.session_path, self.api_id, self.api_hash)
                await self.client.start(bot_token=self.bot_token)
                logger.info("Telethon MTProto клієнт успішно підключено як бот!")
            return self.client

    async def download_message_video(
        self,
        chat_id: int,
        message_id: int,
        destination_path: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """
        Завантажує медіафайл із повідомлення без обмежень розміру Telegram Bot API,
        а також зберігає оригінальну прев'юшку (обкладинку) Telegram.
        """
        client = await self.get_client()
        try:
            entity = await client.get_entity(chat_id)
        except Exception:
            entity = chat_id
        message = await client.get_messages(entity, ids=message_id)

        if not message or not message.media:
            raise RuntimeError("Не вдалося знайти медіафайл у цьому повідомленні.")

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        # Зберігаємо оригінальне прев'ю/обкладинку з відео в Telegram
        try:
            thumb_path = destination_path.with_name(f"{destination_path.stem}.thumb.jpg")
            await client.download_media(message, thumb=-1, file=str(thumb_path))
            if thumb_path.exists() and thumb_path.stat().st_size > 0:
                poster_path = destination_path.parent / "poster.jpg"
                if not poster_path.exists():
                    shutil.copy2(thumb_path, poster_path)
        except Exception as e:
            logger.debug(f"Не вдалося витягнути обкладинку через MTProto: {e}")

        part_path = destination_path.with_name(f"{destination_path.name}.part")

        async def sync_progress(current, total):
            if progress_callback:
                try:
                    res = progress_callback(current, total)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    pass

        try:
            await client.download_media(
                message,
                file=str(part_path),
                progress_callback=sync_progress,
            )

            if part_path.exists():
                os.replace(part_path, destination_path)

            return destination_path
        except (Exception, asyncio.CancelledError) as e:
            if part_path.exists():
                try:
                    part_path.unlink(missing_ok=True)
                except Exception:
                    pass
            raise e

    async def download_by_link(
        self,
        channel_username: str,
        post_id: int,
        destination_path: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """
        Завантажує відео за публічним лінком каналу (наприклад t.me/channel/123)
        разом із його оригінальним прев'ю.
        """
        client = await self.get_client()
        try:
            entity = await client.get_entity(channel_username)
        except Exception:
            entity = channel_username
        message = await client.get_messages(entity, ids=post_id)

        if not message or not message.media:
            raise RuntimeError(f"Не знайдено відео в пості {channel_username}/{post_id}")

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        # Зберігаємо оригінальне прев'ю/обкладинку з Telegram
        try:
            thumb_path = destination_path.with_name(f"{destination_path.stem}.thumb.jpg")
            await client.download_media(message, thumb=-1, file=str(thumb_path))
            if thumb_path.exists() and thumb_path.stat().st_size > 0:
                poster_path = destination_path.parent / "poster.jpg"
                if not poster_path.exists():
                    shutil.copy2(thumb_path, poster_path)
        except Exception as e:
            logger.debug(f"Не вдалося витягнути обкладинку через MTProto: {e}")

        part_path = destination_path.with_name(f"{destination_path.name}.part")

        async def sync_progress(current, total):
            if progress_callback:
                try:
                    res = progress_callback(current, total)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    pass

        try:
            await client.download_media(
                message,
                file=str(part_path),
                progress_callback=sync_progress,
            )

            if part_path.exists():
                os.replace(part_path, destination_path)

            return destination_path
        except (Exception, asyncio.CancelledError) as e:
            if part_path.exists():
                try:
                    part_path.unlink(missing_ok=True)
                except Exception:
                    pass
            raise e
