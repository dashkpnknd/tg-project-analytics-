#!/usr/bin/env python3
"""Live tender funnel notifications for the connected Telegram report chat."""
import html
import json
import os
import sqlite3
import time
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

BASE = Path("/opt/tg-project-analytics")
load_dotenv(BASE / ".env")
ANALYTICS_DB = BASE / "data/analytics.sqlite3"
STATE_DB = BASE / "data/tenders-feed.sqlite3"
TOKEN = os.environ["BOT_TOKEN"]


def connect(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    return db


state = connect(STATE_DB)
state.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
state.execute("CREATE TABLE IF NOT EXISTS delivered (event_key TEXT PRIMARY KEY)")
state.commit()


def get_setting(key: str) -> str:
    row = state.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else ""


def set_setting(key: str, value: str) -> None:
    state.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    state.commit()


def was_delivered(key: str) -> bool:
    return state.execute("SELECT 1 FROM delivered WHERE event_key=?", (key,)).fetchone() is not None


def mark_delivered(key: str) -> None:
    state.execute("INSERT OR IGNORE INTO delivered(event_key) VALUES(?)", (key,))
    state.commit()


def report_chat_id() -> int | None:
    db = sqlite3.connect(ANALYTICS_DB)
    row = db.execute("SELECT value FROM settings WHERE key=?", ("tenders_report_chat_id",)).fetchone()
    return int(row[0]) if row else None


def send(text: str) -> bool:
    chat_id = report_chat_id()
    if not chat_id:
        return False
    body = json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "HTML"}).encode()
    request = urllib.request.Request(
        "https://api.telegram.org/bot" + TOKEN + "/sendMessage",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    return bool(json.load(urllib.request.urlopen(request, timeout=20)).get("ok"))


def identity(payload: dict) -> str:
    username = str(payload.get("username") or "").lstrip("@")
    if username:
        return "@" + html.escape(username)
    user_id = payload.get("user_id")
    return "Telegram ID: <code>" + html.escape(str(user_id or "не указан")) + "</code>"


def new_events(started_at: int):
    db = sqlite3.connect(ANALYTICS_DB)
    db.row_factory = sqlite3.Row
    return db.execute(
        "SELECT id,event_type,payload FROM events WHERE project=? AND occurred_at>=? AND event_type IN (?,?) ORDER BY id",
        ("AI РАЗБОР ТЕНДЕРОВ", started_at, "second_message_sent", "third_message_sent"),
    ).fetchall()


def notification(event_type: str, payload: dict) -> str:
    who = identity(payload)
    if event_type == "second_message_sent":
        return "💬➡️ <b>Ответ + второе сообщение</b>\n" + who + "\nВторое сообщение отправлено"
    return "📥🔗 <b>Заявка + третье сообщение</b>\n" + who + "\nЗаявка в канал обработана\nТретье сообщение со ссылкой отправлено"


def main() -> None:
    if not get_setting("started_at"):
        set_setting("started_at", str(int(time.time())))
        send("✅ <b>Сбор по Госзакупкам включён</b>\nС этого момента будут приходить только будущие действия: ответ + второе сообщение и заявка + третье сообщение.")
    while True:
        for event in new_events(int(get_setting("started_at"))):
            key = "event:" + str(event["id"])
            if was_delivered(key):
                continue
            try:
                payload = json.loads(event["payload"])
                if send(notification(event["event_type"], payload)):
                    mark_delivered(key)
            except Exception:
                pass
        time.sleep(20)


if __name__ == "__main__":
    main()
