"""Small, failure-safe client used by TG-zayavki to record funnel events."""
import asyncio
import logging
import os
import time
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)


def _setting(name: str, default: str = "") -> str:
    """Read the running bot environment first, then its existing .env file."""
    if value := os.getenv(name):
        return value
    env_file = Path(__file__).with_name(".env")
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return default


async def record_event(project: str, event_type: str, event_key: str, payload: dict | None = None) -> None:
    endpoint = _setting("ANALYTICS_INGEST_URL", "http://127.0.0.1:8071/events")
    token = _setting("ANALYTICS_INGEST_TOKEN")
    body = {
        "source": "tg_zayavki",
        "project": project,
        "event_type": event_type,
        "event_key": event_key,
        "occurred_at": int(time.time()),
        "payload": payload or {},
    }
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
            async with session.post(endpoint, json=body, headers=headers) as response:
                if response.status >= 300:
                    logger.warning("Analytics event rejected: %s", response.status)
    except Exception:
        # Reporting must never interrupt approval or customer communication.
        logger.exception("Could not submit analytics event")
