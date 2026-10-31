import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Callable, Dict, List, Optional
import aiohttp

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
CHUNK_SIZE = 1024 * 1024  # 1 МБ блоками для максимальної швидкості мережі


class FexDownloader:
    """Оптимізований клієнт для роботи з FEX.NET API."""

    @staticmethod
    def extract_share_key(url: str) -> Optional[str]:
        """Витягує ключ поширення з посилань FEX.NET разом із ID підпапки, якщо він є (наприклад, mleyd4o/6005554951)."""
        match = re.search(r"fex\.net/(?:[a-z]{2}/)?s/([a-zA-Z0-9]+(?:/\d+)?)", url or "")
        if match:
            return match.group(1)
        return None

    @staticmethod
    async def get_anonymous_token(session: aiohttp.ClientSession) -> Optional[str]:
        """Отримує анонімний токен доступу від FEX.NET."""
        url = "https://api.fex.net/api/v1/config/anonymous"
        headers = {"User-Agent": USER_AGENT}
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    anon_data = data.get("anonymous") or {}
                    token = anon_data.get("anonym_token") or data.get("anonym_token")
                    return token
        except Exception as e:
            logger.error(f"Помилка отримання токена FEX: {e}")
        return None

    @classmethod
    async def get_files_in_share(cls, share_key: str) -> List[Dict]:
        """
        Рекурсивно повертає список усіх файлів у публічній папці FEX.NET.
        """
        connector = aiohttp.TCPConnector(limit=10, enable_cleanup_closed=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            token = await cls.get_anonymous_token(session)
            if not token:
                logger.error("Не вдалося отримати анонімний токен FEX.NET")
                return []

            headers = {
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {token}",
            }

            url = f"https://api.fex.net/api/v1/file/share/{share_key}"
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status != 200:
                        logger.error(f"FEX повернув статус {resp.status} для ключа {share_key}")
                        return []
                    data = await resp.json()
            except Exception as e:
                logger.error(f"Помилка запиту до кореня FEX: {e}")
                return []

            files: List[Dict] = []
            tree = data.get("tree") or {}
            base_share_key = share_key.split("/")[0]
            await cls._crawl_tree(session, headers, base_share_key, tree, files, token)
            return files

    @classmethod
    async def _crawl_tree(
        cls,
        session: aiohttp.ClientSession,
        headers: dict,
        share_key: str,
        current_node: dict,
        result_files: list,
        token: str
    ):
        """Рекурсивно обходить дерево папок FEX.NET."""
        children = current_node.get("children") or []
        for item in children:
            is_dir = item.get("is_dir", False)
            item_id = item.get("id")
            name = item.get("name", "")
            size = item.get("size") or 0
            download_url = item.get("download_url")

            if is_dir and item_id:
                sub_url = f"https://api.fex.net/api/v1/file/share/{share_key}/{item_id}"
                try:
                    async with session.get(sub_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        if resp.status == 200:
                            sub_data = await resp.json()
                            sub_tree = sub_data.get("tree") or {}
                            await cls._crawl_tree(session, headers, share_key, sub_tree, result_files, token)
                except Exception as e:
                    logger.warning(f"Не вдалося прочитати підпапку FEX '{name}': {e}")
            else:
                if download_url:
                    result_files.append({
                        "id": item_id,
                        "name": name,
                        "size": size,
                        "download_url": download_url,
                        "token": token
                    })

    @classmethod
    async def download_file(
        cls,
        download_url: str,
        destination_path: Path,
        token: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Path:
        """
        Атомарно завантажує файл із підтримкою докачування (HTTP Range) та авто-повторів при обриві мережі:
        1. Качає у тимчасовий файл destination_path.part.
        2. Якщо з'єднання обірвалося (ConnectionResetError / ContentLengthError) — автоматично докачує з місця зупинки (до 6 спроб).
        3. Лише після 100% завантаження перейменовує у цільовий файл.
        """
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        part_path = destination_path.with_name(f"{destination_path.name}.part")

        max_retries = 6
        total_size = 0
        last_error: Optional[Exception] = None

        try:
            for attempt in range(1, max_retries + 1):
                downloaded = part_path.stat().st_size if part_path.exists() else 0
                if total_size > 0 and downloaded >= total_size:
                    break

                headers = {"User-Agent": USER_AGENT}
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                if downloaded > 0:
                    headers["Range"] = f"bytes={downloaded}-"

                timeout = aiohttp.ClientTimeout(connect=20, sock_read=90, total=7200)
                connector = aiohttp.TCPConnector(limit=5, enable_cleanup_closed=True)

                try:
                    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                        # Якщо це повторна спроба і токен міг застаріти — оновимо токен
                        if attempt > 1:
                            new_tok = await cls.get_anonymous_token(session)
                            if new_tok:
                                token = new_tok
                                headers["Authorization"] = f"Bearer {token}"

                        async with session.get(download_url, headers=headers) as resp:
                            content_type = resp.headers.get("Content-Type", "").lower()
                            if "text/html" in content_type:
                                raise RuntimeError("FEX повернув веб-сторінку замість відеофайлу (відхилено токен доступу).")

                            if resp.status == 206:
                                content_len = int(resp.headers.get("Content-Length", 0))
                                if total_size == 0 and content_len > 0:
                                    total_size = downloaded + content_len
                                file_mode = "ab"
                            elif resp.status == 200:
                                # Сервер не підтримав Range — качаємо з нуля
                                downloaded = 0
                                total_size = int(resp.headers.get("Content-Length", 0))
                                file_mode = "wb"
                            elif resp.status == 416 and total_size > 0 and downloaded >= total_size:
                                # Файл уже повністю завантажений
                                break
                            else:
                                raise RuntimeError(f"Помилка завантаження файлу з FEX (HTTP {resp.status})")

                            with open(part_path, file_mode, buffering=2 * CHUNK_SIZE) as f:
                                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                                    if not chunk:
                                        break
                                    f.write(chunk)
                                    downloaded += len(chunk)
                                    if progress_callback and total_size > 0:
                                        try:
                                            res = progress_callback(downloaded, total_size)
                                            if asyncio.iscoroutine(res):
                                                await res
                                        except Exception:
                                            pass

                    # Перевіряємо, чи завантажили всі байти
                    actual_size = part_path.stat().st_size if part_path.exists() else 0
                    if total_size == 0 or actual_size >= total_size:
                        break
                    else:
                        raise RuntimeError(f"Неповне завантаження ({actual_size} / {total_size} байт), докачую...")

                except asyncio.CancelledError:
                    raise
                except Exception as e_attempt:
                    last_error = e_attempt
                    logger.warning(f"FEX обрив на спробі {attempt}/{max_retries} ({destination_path.name}): {e_attempt}")
                    if attempt < max_retries:
                        await asyncio.sleep(min(3 * attempt, 12))
                    else:
                        raise last_error

            # Успішно завантажено — атомарно замінюємо на фінальний файл
            if part_path.exists():
                os.replace(part_path, destination_path)

            return destination_path

        except asyncio.CancelledError as e:
            if part_path.exists():
                try:
                    part_path.unlink(missing_ok=True)
                except Exception:
                    pass
            raise e
        except Exception as e:
            if part_path.exists():
                try:
                    part_path.unlink(missing_ok=True)
                except Exception:
                    pass
            raise e
