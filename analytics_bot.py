#!/usr/bin/env python3
"""Unified Telegram project analytics.

Reads outreach data from Dialog Hub, accepts durable events from TG-zayavki,
and publishes daily/weekly/monthly reports to a configured Telegram chat.
"""
import asyncio
import datetime as dt
import html
import json
import logging
import os
import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("project-analytics")


@dataclass(frozen=True)
class Settings:
    token: str
    db_path: Path
    dialoghub_db: Path
    timezone: ZoneInfo
    api_host: str
    api_port: int
    ingest_token: str

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.getenv("BOT_TOKEN", "")
        if not token:
            raise RuntimeError("BOT_TOKEN is required")
        return cls(
            token=token,
            db_path=Path(os.getenv("DATABASE_PATH", "data/analytics.sqlite3")),
            dialoghub_db=Path(os.getenv("DIALOGHUB_DATABASE_PATH", "/opt/dialoghub/data/dialoghub.sqlite3")),
            timezone=ZoneInfo(os.getenv("REPORT_TIMEZONE", "Europe/Moscow")),
            api_host=os.getenv("INGEST_HOST", "127.0.0.1"),
            api_port=int(os.getenv("INGEST_PORT", "8071")),
            ingest_token=os.getenv("INGEST_TOKEN", ""),
        )


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            event_key TEXT NOT NULL UNIQUE,
            project TEXT NOT NULL,
            event_type TEXT NOT NULL,
            occurred_at INTEGER NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_events_period ON events(occurred_at, project, event_type);
        CREATE TABLE IF NOT EXISTS sent_reports (
            kind TEXT NOT NULL,
            period_start TEXT NOT NULL,
            PRIMARY KEY(kind, period_start)
        );
        """)
        self.db.commit()

    def get(self, key: str, default: str = "") -> str:
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set(self, key: str, value: str) -> None:
        self.db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.db.commit()

    def add_event(self, event: dict[str, Any]) -> bool:
        try:
            self.db.execute(
                "INSERT INTO events(source,event_key,project,event_type,occurred_at,payload) VALUES(?,?,?,?,?,?)",
                (event["source"], event["event_key"], event["project"], event["event_type"], event["occurred_at"], json.dumps(event.get("payload", {}), ensure_ascii=False)),
            )
            self.db.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def event_rows(self, start: int, end: int | None) -> list[sqlite3.Row]:
        sql = "SELECT project,event_type,COUNT(*) count FROM events WHERE occurred_at>=?"
        args: list[Any] = [start]
        if end is not None:
            sql += " AND occurred_at<?"; args.append(end)
        sql += " GROUP BY project,event_type"
        return self.db.execute(sql, args).fetchall()

    def report_sent(self, kind: str, period_start: str) -> bool:
        return self.db.execute("SELECT 1 FROM sent_reports WHERE kind=? AND period_start=?", (kind, period_start)).fetchone() is not None

    def mark_report_sent(self, kind: str, period_start: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO sent_reports(kind,period_start) VALUES(?,?)", (kind, period_start)); self.db.commit()


class Metrics:
    """Combines direct Dialog Hub outreach facts with submitted funnel events."""
    def __init__(self, store: Store, hub_db: Path):
        self.store, self.hub_db = store, hub_db

    @staticmethod
    def project_name(name: str) -> str:
        aliases = {"apple": "АЙФОНЫ", "AI РАЗБОР ТЕНДЕРОВ": "ГОСЗАКУПКИ"}
        return aliases.get(name, name)

    def _hub_rows(self, start: int, end: int | None) -> list[sqlite3.Row]:
        if not self.hub_db.exists():
            log.warning("Dialog Hub database not found: %s", self.hub_db)
            return []
        end_condition = "" if end is None else " AND (o.sent_at<? OR o.replied_at<?)"
        args: list[Any] = [start, start]
        if end is not None: args.extend([end, end])
        query = f"""
          SELECT COALESCE(p.name, 'Без проекта') project,
                 SUM(CASE WHEN o.sent_at>=? {'' if end is None else 'AND o.sent_at<?'} THEN 1 ELSE 0 END) sent,
                 SUM(CASE WHEN o.replied_at>=? {'' if end is None else 'AND o.replied_at<?'} THEN 1 ELSE 0 END) replied
          FROM outreach_messages o LEFT JOIN projects p ON p.id=o.project_id
          WHERE o.sent_at>=? OR o.replied_at>=? {end_condition}
          GROUP BY COALESCE(p.name, 'Без проекта')
        """
        # Use named explicit range query instead: SQLite placeholders stay clear and auditable.
        query = """
          SELECT COALESCE(p.name, 'Без проекта') project,
            SUM(CASE WHEN o.sent_at>=:start AND (:end IS NULL OR o.sent_at<:end) THEN 1 ELSE 0 END) sent,
            SUM(CASE WHEN o.replied_at>=:start AND (:end IS NULL OR o.replied_at<:end) THEN 1 ELSE 0 END) replied
          FROM outreach_messages o LEFT JOIN projects p ON p.id=o.project_id
          WHERE o.sent_at>=:start OR o.replied_at>=:start
          GROUP BY COALESCE(p.name, 'Без проекта')
        """
        try:
            with closing(sqlite3.connect(f"file:{self.hub_db}?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                return db.execute(query, {"start": start, "end": end}).fetchall()
        except sqlite3.Error:
            log.exception("Could not read Dialog Hub metrics")
            return []

    def summary(self, start: int, end: int | None) -> dict[str, dict[str, int]]:
        data: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for row in self._hub_rows(start, end):
            project = self.project_name(row["project"])
            data[project]["sent"] += row["sent"] or 0
            data[project]["replied"] += row["replied"] or 0
        names = {"join_request": "requests", "join_approved": "approved", "second_message_sent": "second_sent", "third_message_sent": "third_sent", "site_registration": "registrations", "lead": "leads"}
        for row in self.store.event_rows(start, end):
            project = self.project_name(row["project"])
            data[project][names.get(row["event_type"], row["event_type"])] += row["count"]
        return data

    def outreach_texts(self, project: str, start: int, end: int | None) -> list[sqlite3.Row]:
        """Message-level results: which first outreach texts create replies."""
        if not self.hub_db.exists(): return []
        query = """
          SELECT o.script_label, COUNT(*) sent,
            SUM(CASE WHEN o.replied_at>=:start AND (:end IS NULL OR o.replied_at<:end) THEN 1 ELSE 0 END) replied
          FROM outreach_messages o JOIN projects p ON p.id=o.project_id
          WHERE p.name=:project AND o.sent_at>=:start AND (:end IS NULL OR o.sent_at<:end)
          GROUP BY o.script_label ORDER BY sent DESC, replied DESC, o.script_label
        """
        try:
            with closing(sqlite3.connect(f"file:{self.hub_db}?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                return db.execute(query, {"project": project, "start": start, "end": end}).fetchall()
        except sqlite3.Error:
            log.exception("Could not read outreach text metrics")
            return []


class TelegramAPI:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"
        self.http: aiohttp.ClientSession | None = None

    async def start(self): self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    async def close(self):
        if self.http: await self.http.close()

    async def call(self, method: str, **payload):
        assert self.http
        async with self.http.post(f"{self.base}/{method}", json=payload) as response:
            data = await response.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description", "Telegram API error"))
        return data["result"]

    async def send(self, chat_id: int, text: str, keyboard: dict | None = None):
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if keyboard: payload["reply_markup"] = keyboard
        return await self.call("sendMessage", **payload)

    async def poll(self, offset: int):
        return await self.call("getUpdates", offset=offset, timeout=45, allowed_updates=["message", "callback_query"])


class AnalyticsBot:
    def __init__(self, settings: Settings):
        self.s = settings
        self.store = Store(settings.db_path)
        self.metrics = Metrics(self.store, settings.dialoghub_db)
        self.tg = TelegramAPI(settings.token)
        self.offset = int(self.store.get("telegram_offset", "0"))

    @staticmethod
    def keyboard() -> dict:
        return {"inline_keyboard": [
            [
                {"text": "◀️ Вчера", "callback_data": "day"},
                {"text": "📅 Эта неделя", "callback_data": "week"},
                {"text": "♾ За всё время", "callback_data": "all"},
            ],
            [{"text": "🧪 Скрипты", "callback_data": "scripts_menu"}],
        ]}

    @staticmethod
    def scripts_keyboard() -> dict:
        return {"inline_keyboard": [
            [
                {"text": "Госзакупки", "callback_data": "scripts_tenders"},
                {"text": "Трейдинг", "callback_data": "scripts_trading"},
            ],
            [{"text": "← К отчётам", "callback_data": "reports_menu"}],
        ]}

    def report_chat_id(self) -> int | None:
        value = self.store.get("report_chat_id")
        return int(value) if value else None

    def is_admin(self, user_id: int) -> bool:
        admins = {int(x) for x in self.store.get("admin_ids").split(",") if x.strip()}
        return user_id in admins

    def period(self, kind: str) -> tuple[int, int | None, str]:
        now = dt.datetime.now(self.s.timezone)
        if kind == "day":
            day = now.date() - dt.timedelta(days=1)
            start = dt.datetime.combine(day, dt.time.min, self.s.timezone); end = start + dt.timedelta(days=1)
            return int(start.timestamp()), int(end.timestamp()), day.strftime("%d.%m.%Y")
        if kind == "week":
            start = dt.datetime.combine(now.date() - dt.timedelta(days=now.weekday()), dt.time.min, self.s.timezone)
            return int(start.timestamp()), int(now.timestamp()) + 1, f"{start:%d.%m}–{now:%d.%m.%Y}"
        if kind == "month":
            start = dt.datetime(now.year, now.month, 1, tzinfo=self.s.timezone)
            return int(start.timestamp()), int(now.timestamp()) + 1, now.strftime("%B %Y")
        return 0, None, "за всё время"

    def format_report(self, kind: str, start: int | None = None, end: int | None = None, label: str | None = None) -> str:
        if start is None:
            start, end, label = self.period(kind)
        assert label is not None
        titles = {"day": "Ежедневная статистика", "week": "Статистика за неделю", "month": "Статистика за месяц", "all": "Статистика за всё время"}
        result = [f"<b>{titles[kind]}</b>", f"Период: {html.escape(label)}"]
        summary = self.metrics.summary(start, end)
        if not summary:
            return "\n".join(result + ["\nПока нет данных за этот период."])
        for project in sorted(summary, key=str.casefold):
            m = summary[project]
            sent, replied = m["sent"], m["replied"]
            conversion = f"{replied / sent * 100:.1f}%" if sent else "—"
            result.extend([f"\n<b>{html.escape(project)}</b>", f"• Отправлено: <b>{sent}</b>", f"• Ответили: <b>{replied}</b> ({conversion})"])
            result.append(f"• Заявки в канал: <b>{m['requests']}</b>")
            if m["second_sent"] or m["third_sent"]:
                result.append(f"• 2-е сообщение (ссылка на канал): <b>{m['second_sent']}</b>")
                result.append(f"• 3-е сообщение (ссылка на сайт): <b>{m['third_sent']}</b>")
            if project == "ГОСЗАКУПКИ":
                site_conversion = f"{m['registrations'] / m['third_sent'] * 100:.1f}%" if m["third_sent"] else "—"
                result.append(f"• Регистрации на сайте: <b>{m['registrations']}</b> ({site_conversion} от 3-го сообщения)")
            if m["leads"]: result.append(f"• Лиды: <b>{m['leads']}</b>")
        return "\n".join(result)

    def format_scripts(self, project: str) -> str:
        rows = [row for row in self.metrics.outreach_texts(project, 0, None) if row["script_label"] != "[медиа/файл]"]
        if not rows:
            return f"<b>Скрипты · {html.escape(project)}</b>\n\nПока нет данных по текстам."
        items = [{"text": row["script_label"], "sent": row["sent"], "replied": row["replied"], "rate": row["replied"] / row["sent"] if row["sent"] else 0} for row in rows]
        total_sent, total_replied = sum(x["sent"] for x in items), sum(x["replied"] for x in items)
        average = total_replied / total_sent if total_sent else 0
        reliable = [x for x in items if x["sent"] >= 20]
        leaders = sorted([x for x in reliable if x["rate"] >= average], key=lambda x: (x["rate"], x["sent"]), reverse=True)[:3]
        weak = sorted([x for x in reliable if x["rate"] < average], key=lambda x: (x["rate"], -x["sent"]))[:3]
        tests = [x for x in items if x["sent"] < 20]
        def show(index: int, item: dict) -> str:
            text = html.escape(" ".join(item["text"].split()))
            return f"{index}. {text}\n   <b>{item['replied']}/{item['sent']}</b> ответов · {item['rate'] * 100:.1f}%"
        result = [
            f"<b>Скрипты · {html.escape(project)}</b>",
            "Период: всё время",
            f"Всего по текстовым скриптам: <b>{total_replied}/{total_sent}</b> ответов · {average * 100:.1f}%",
        ]
        if leaders:
            result.append("\n<b>🏆 Рабочие скрипты</b>\nДостаточная выборка (от 20 отправок), выше или на уровне среднего.")
            result.extend(show(i, x) for i, x in enumerate(leaders, 1))
        if weak:
            result.append("\n<b>📉 Нужна доработка</b>\nДостаточная выборка, но конверсия ниже средней.")
            result.extend(show(i, x) for i, x in enumerate(weak, 1))
        if tests:
            no_replies = sum(1 for x in tests if not x["replied"])
            result.append(f"\n<b>🧪 Ещё тестируем</b>\n{len(tests)} скриптов имеют меньше 20 отправок; из них {no_replies} пока без ответов. Рано делать выводы — нужно добрать выборку.")
        return "\n".join(result)

    async def send_report(self, kind: str, start: int | None = None, end: int | None = None, label: str | None = None) -> None:
        chat_id = self.report_chat_id()
        if chat_id: await self.tg.send(chat_id, self.format_report(kind, start, end, label), self.keyboard())

    async def handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message")
        callback = update.get("callback_query")
        if callback:
            user_id = callback["from"]["id"]
            if not self.is_admin(user_id): return
            await self.tg.call("answerCallbackQuery", callback_query_id=callback["id"])
            chat_id = callback["message"]["chat"]["id"]
            action = callback["data"]
            if action == "scripts_menu":
                await self.tg.send(chat_id, "<b>Скрипты</b>\nВыбери проект. Аналитика всегда за всё время.", self.scripts_keyboard())
            elif action == "scripts_tenders":
                await self.tg.send(chat_id, self.format_scripts("ГОСЗАКУПКИ"), self.scripts_keyboard())
            elif action == "scripts_trading":
                await self.tg.send(chat_id, self.format_scripts("ТРЕЙДИНГ"), self.scripts_keyboard())
            elif action == "reports_menu":
                await self.tg.send(chat_id, "Отчёты по проектам.", self.keyboard())
            else:
                await self.tg.send(chat_id, self.format_report(action), self.keyboard())
            return
        if not message or not message.get("text"): return
        user_id, chat_id = message["from"]["id"], message["chat"]["id"]
        command = message["text"].split()[0].split("@", 1)[0]
        if command == "/start":
            if not self.store.get("admin_ids"):
                self.store.set("admin_ids", str(user_id)); self.store.set("report_chat_id", str(chat_id))
                await self.tg.send(chat_id, "✅ Этот чат назначен для отчётов. Доступ получили вы.", self.keyboard()); return
            if self.is_admin(user_id): await self.tg.send(chat_id, "Бот статистики работает.", self.keyboard())
            return
        if not self.is_admin(user_id): return
        if command == "/set_report_chat":
            self.store.set("report_chat_id", str(chat_id)); await self.tg.send(chat_id, "✅ Этот чат назначен для автоматических отчётов.", self.keyboard())
        elif command == "/yesterday": await self.tg.send(chat_id, self.format_report("day"), self.keyboard())
        elif command == "/week": await self.tg.send(chat_id, self.format_report("week"), self.keyboard())
        elif command == "/all": await self.tg.send(chat_id, self.format_report("all"), self.keyboard())
        elif command == "/help": await self.tg.send(chat_id, "Команды: /yesterday, /week, /all, /set_report_chat", self.keyboard())

    async def polling_loop(self):
        while True:
            try:
                for update in await self.tg.poll(self.offset):
                    self.offset = update["update_id"] + 1; self.store.set("telegram_offset", str(self.offset))
                    await self.handle_update(update)
            except asyncio.CancelledError: raise
            except Exception:
                log.exception("Polling failed"); await asyncio.sleep(5)

    async def schedule_loop(self):
        while True:
            now = dt.datetime.now(self.s.timezone)
            # At 00:01 Moscow time the previous calendar period is complete.
            if now.hour == 0 and now.minute == 1:
                yesterday = (now.date() - dt.timedelta(days=1)).isoformat()
                if not self.store.report_sent("day", yesterday):
                    await self.send_report("day"); self.store.mark_report_sent("day", yesterday)
                if now.weekday() == 0:
                    week_start = now.date() - dt.timedelta(days=7)
                    week_end = dt.datetime.combine(now.date(), dt.time.min, self.s.timezone)
                    week = week_start.isoformat()
                    if not self.store.report_sent("week", week):
                        await self.send_report("week", int(dt.datetime.combine(week_start, dt.time.min, self.s.timezone).timestamp()), int(week_end.timestamp()), f"{week_start:%d.%m}–{(now.date() - dt.timedelta(days=1)):%d.%m.%Y}")
                        self.store.mark_report_sent("week", week)
                if now.day == 1:
                    month_end = dt.datetime(now.year, now.month, 1, tzinfo=self.s.timezone)
                    previous_month_start = (month_end - dt.timedelta(days=1)).replace(day=1)
                    previous_month = previous_month_start.strftime("%Y-%m")
                    if not self.store.report_sent("month", previous_month):
                        await self.send_report("month", int(previous_month_start.timestamp()), int(month_end.timestamp()), previous_month_start.strftime("%B %Y"))
                        self.store.mark_report_sent("month", previous_month)
            await asyncio.sleep(30)

    async def ingest(self, request: web.Request) -> web.Response:
        if self.s.ingest_token and request.headers.get("Authorization") != f"Bearer {self.s.ingest_token}":
            raise web.HTTPUnauthorized()
        payload = await request.json()
        required = {"source", "event_key", "project", "event_type"}
        if not required <= payload.keys(): raise web.HTTPBadRequest(text="Missing event fields")
        payload["occurred_at"] = int(payload.get("occurred_at", dt.datetime.now(dt.timezone.utc).timestamp()))
        inserted = self.store.add_event(payload)
        return web.json_response({"ok": True, "inserted": inserted})

    async def run(self):
        await self.tg.start()
        app = web.Application(); app.router.add_post("/events", self.ingest)
        runner = web.AppRunner(app); await runner.setup()
        site = web.TCPSite(runner, self.s.api_host, self.s.api_port); await site.start()
        log.info("Analytics ingestion listening on %s:%s", self.s.api_host, self.s.api_port)
        try: await asyncio.gather(self.polling_loop(), self.schedule_loop())
        finally: await runner.cleanup(); await self.tg.close()


if __name__ == "__main__":
    asyncio.run(AnalyticsBot(Settings.from_env()).run())
