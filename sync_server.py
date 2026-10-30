import asyncio
import logging
import os
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
import uvicorn

from mac_sync_schema import (
    update_mac_device_state,
    get_pending_mac_commands,
    mark_mac_command_done,
)
from config import BOT_TOKEN

logger = logging.getLogger("SyncServer")
api_app = FastAPI(title="Anime Downloader Mac Sync Bridge")

SYNC_SECRET = os.getenv("MAC_SYNC_SECRET") or BOT_TOKEN[:16]


class FileItem(BaseModel):
    key: Optional[str] = None
    folder: str
    name: str
    path: str
    size: int


class HeartbeatPayload(BaseModel):
    device_name: str
    free_disk_gb: float
    downloads_folder: str
    uptime_str: str
    files: List[FileItem]
    executed_command_ids: Optional[List[int]] = None


@api_app.get("/health")
async def health_check():
    return {"status": "ok", "service": "anime-mac-bridge"}


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

    # Зберігаємо актуальний список файлів
    files_data = [f.model_dump() for f in payload.files]
    update_mac_device_state(
        device_name=payload.device_name,
        free_disk_gb=payload.free_disk_gb,
        downloads_folder=payload.downloads_folder,
        uptime_str=payload.uptime_str,
        files=files_data,
    )

    # Віддаємо нові команди для Mac (наприклад завантаження нової серії чи видалення)
    pending_cmds = get_pending_mac_commands()
    return {"status": "success", "commands": pending_cmds}


async def run_sync_server():
    port = int(os.getenv("PORT", "8080"))
    config = uvicorn.Config(app=api_app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    logger.info(f"Запуск FastAPI Mac Sync Bridge на порту {port}...")
    await server.serve()

