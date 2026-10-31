import urllib.request
import time

ts = str(int(time.time()))
base = "https://raw.githubusercontent.com/vadym-des/anime-downloader-bot/main/"
files = (
    "bot.py",
    "channel_monitor.py",
    "fex.py",
    "progress.py",
    "update_manager.py",
    "notion_tasks.py",
    "seed_data.py",
    "sync_server.py",
    "mac_sync_schema.py",
    "config.py",
    "mac_agent.py",
)
for f in files:
    try:
        urllib.request.urlretrieve(f"{base}{f}?t={ts}", f"/app/{f}")
    except Exception:
        pass
