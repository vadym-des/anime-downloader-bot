import asyncio
import base64
import hashlib
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
import uvicorn

from mac_sync_schema import (
    update_mac_device_state,
    get_pending_mac_commands,
    mark_mac_command_done,
    set_file_watch_status,
)
from config import BOT_TOKEN

logger = logging.getLogger("SyncServer")
api_app = FastAPI(title="Anime Downloader Mac Sync Bridge")

SYNC_SECRET = os.getenv("MAC_SYNC_SECRET") or BOT_TOKEN[:16]
POSTERS_DIR = Path(__file__).resolve().parent / "cache" / "posters"


class FileItem(BaseModel):
    key: Optional[str] = None
    folder: str
    name: str
    path: str
    size: int
    status: Optional[str] = "new"


class HeartbeatPayload(BaseModel):
    device_name: str
    free_disk_gb: float
    downloads_folder: str
    uptime_str: str
    files: List[FileItem]
    executed_command_ids: Optional[List[int]] = None
    posters: Optional[Dict[str, str]] = None
    watch_updates: Optional[List[Dict[str, Any]]] = None


@api_app.get("/health")
async def health_check():
    from update_manager import CURRENT_VERSION, get_current_git_info
    sha, msg = get_current_git_info()
    return {"status": "ok", "service": "anime-mac-bridge", "version": CURRENT_VERSION, "git_sha": sha, "commit_msg": msg}


@api_app.get("/api/mac/debug")
async def debug_mac(x_sync_secret: Optional[str] = Header(None)):
    from mac_sync_schema import get_mac_device_state, get_synced_mac_files
    st = get_mac_device_state()
    fl = get_synced_mac_files()
    return {"state": st, "files_count": sum(len(v) for v in fl.values()), "folders": list(fl.keys())}


@api_app.post("/api/mac/heartbeat")
async def mac_heartbeat(payload: HeartbeatPayload, x_sync_secret: Optional[str] = Header(None)):
    if x_sync_secret != SYNC_SECRET:
        raise HTTPException(status_code=403, detail="Invalid sync secret")

    # Позначаємо виконані команди
    if payload.executed_command_ids:
        for cid in payload.executed_command_ids:
            mark_mac_command_done(cid)

    # Зберігаємо передані постери з Mac
    POSTERS_DIR.mkdir(parents=True, exist_ok=True)
    if payload.posters:
        for folder_name, b64_str in payload.posters.items():
            try:
                f_hash = hashlib.md5(folder_name.encode("utf-8")).hexdigest()[:12]
                (POSTERS_DIR / f"{f_hash}.jpg").write_bytes(base64.b64decode(b64_str))
            except Exception as e:
                logger.debug(f"Помилка збереження постера для {folder_name}: {e}")

    # Оновлюємо статуси перегляду (відкрив/подивився)
    if payload.watch_updates:
        for upd in payload.watch_updates:
            k = upd.get("file_key")
            st = upd.get("status")
            if k and st:
                set_file_watch_status(file_key=k, status=st, file_path=upd.get("path", ""), file_name=upd.get("name", ""))

    # Зберігаємо актуальний список файлів
    files_data = [f.model_dump() for f in payload.files]
    update_mac_device_state(
        device_name=payload.device_name,
        free_disk_gb=payload.free_disk_gb,
        downloads_folder=payload.downloads_folder,
        uptime_str=payload.uptime_str,
        files=files_data,
    )

    # Перевіряємо, яких постерів не вистачає на сервері
    missing_posters = []
    seen_folders = {f.folder for f in payload.files if f.folder}
    for folder_name in seen_folders:
        f_hash = hashlib.md5(folder_name.encode("utf-8")).hexdigest()[:12]
        p_file = POSTERS_DIR / f"{f_hash}.jpg"
        if not p_file.exists() or p_file.stat().st_size < 500:
            missing_posters.append(folder_name)

    # Віддаємо нові команди для Mac (наприклад завантаження нової серії чи видалення)
    pending_cmds = get_pending_mac_commands()
    return {
        "status": "success",
        "commands": pending_cmds,
        "missing_posters": missing_posters,
    }


async def run_sync_server():
    port = int(os.getenv("PORT", "8080"))
    config = uvicorn.Config(app=api_app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    logger.info(f"Запуск FastAPI Mac Sync Bridge на порту {port}...")
    await server.serve()
