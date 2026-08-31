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
import re
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
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            started_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS report_deliveries (
            kind TEXT NOT NULL,
            period_start TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY(kind, period_start, user_id)
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

    def funnel_events(self, start: int = 0, end: int | None = None) -> list[sqlite3.Row]:
        sql = "SELECT project,event_type,event_key,payload FROM events WHERE event_type IN ('second_message_sent','join_request') AND occurred_at>=?"
        args: list[Any] = [start]
        if end is not None:
            sql += " AND occurred_at<?"; args.append(end)
        return self.db.execute(sql, args).fetchall()

    def report_sent(self, kind: str, period_start: str) -> bool:
        return self.db.execute("SELECT 1 FROM sent_reports WHERE kind=? AND period_start=?", (kind, period_start)).fetchone() is not None

    def mark_report_sent(self, kind: str, period_start: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO sent_reports(kind,period_start) VALUES(?,?)", (kind, period_start)); self.db.commit()

    def add_user(self, user_id: int) -> None:
        self.db.execute("INSERT OR IGNORE INTO users(user_id,started_at) VALUES(?,?)", (user_id, int(dt.datetime.now(dt.timezone.utc).timestamp())))
        self.db.commit()

    def has_user(self, user_id: int) -> bool:
        return self.db.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)).fetchone() is not None

    def user_ids(self) -> list[int]:
        return [row["user_id"] for row in self.db.execute("SELECT user_id FROM users ORDER BY started_at")]

    def report_delivered(self, kind: str, period_start: str, user_id: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM report_deliveries WHERE kind=? AND period_start=? AND user_id=?",
            (kind, period_start, user_id),
        ).fetchone() is not None

    def mark_report_delivered(self, kind: str, period_start: str, user_id: int) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO report_deliveries(kind,period_start,user_id) VALUES(?,?,?)",
            (kind, period_start, user_id),
        )
        self.db.commit()

    def apple_leads_channel_id(self) -> int | None:
        value = self.get("apple_leads_channel_id")
        return int(value) if value else None

    def set_apple_leads_channel_id(self, chat_id: int) -> None:
        self.set("apple_leads_channel_id", str(chat_id))


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
            SUM(CASE WHEN o.sent_at>=:start AND (:end IS NULL OR o.sent_at<:end)
                      AND o.replied_at IS NOT NULL AND (:end IS NULL OR o.replied_at<:end) THEN 1 ELSE 0 END) replied,
            SUM(CASE WHEN o.replied_at>=:start AND (:end IS NULL OR o.replied_at<:end) THEN 1 ELSE 0 END) replies_received
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
            data[project]["replies_received"] += row["replies_received"] or 0
        names = {"join_request": "requests", "join_approved": "approved", "second_message_sent": "second_sent", "third_message_sent": "third_sent", "lead": "leads"}
        for row in self.store.event_rows(start, end):
            project = self.project_name(row["project"])
            data[project][names.get(row["event_type"], row["event_type"])] += row["count"]
        return data

    def second_message_variants(self, project: str, start: int = 0, end: int | None = None) -> list[dict[str, Any]]:
        events = self.store.funnel_events(start, end)
        requests = {(row["project"], row["event_key"]) for row in events if row["event_type"] == "join_request"}
        variants: dict[str, dict[str, int]] = {}
        for row in events:
            if row["event_type"] != "second_message_sent" or self.project_name(row["project"]) != project:
                continue
            try:
                label = json.loads(row["payload"]).get("message_text", "").strip()
            except (TypeError, json.JSONDecodeError):
                label = ""
            if not label:
                continue
            data = variants.setdefault(label, {"sent": 0, "requests": 0})
            data["sent"] += 1
            request_key = "join-request:" + row["event_key"].removeprefix("second-message:")
            if (row["project"], request_key) in requests:
                data["requests"] += 1
        return [{"text": text, **data} for text, data in sorted(variants.items(), key=lambda item: (-item[1]["sent"], item[0]))]

    def apple_base_summary(self, start: int, end: int | None) -> dict[str, dict[str, int]]:
        """Aggregate the user-approved Apple accounts into the two source bases."""
        bases = {
            "TGK": (15, 11, 10),       # Даниил 🎧, 📞, 💿
            "INST": (16, 14, 13, 12, 9, 19, 18),  # Даниил 📱, 🖥️, 💻, ⌚️, 🧑‍💻, 🗓️, 💼
        }
        result = {name: {"sent": 0, "replied": 0} for name in bases}
        if not self.hub_db.exists():
            return result
        account_to_base = {account_id: name for name, account_ids in bases.items() for account_id in account_ids}
        placeholders = ",".join("?" for _ in account_to_base)
        query = f"""
          SELECT o.account_id, COUNT(*) sent,
            SUM(CASE WHEN o.replied_at IS NOT NULL AND (:end IS NULL OR o.replied_at<:end) THEN 1 ELSE 0 END) replied
          FROM outreach_messages o JOIN projects p ON p.id=o.project_id
          WHERE p.name='АЙФОНЫ' AND o.account_id IN ({placeholders})
            AND o.sent_at>=:start AND (:end IS NULL OR o.sent_at<:end)
          GROUP BY o.account_id
        """
        try:
            with closing(sqlite3.connect(f"file:{self.hub_db}?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                params: dict[str, Any] = {"start": start, "end": end}
                # sqlite supports named and positional parameters together only
                # awkwardly, so interpolate validated integer IDs from this map.
                safe_ids = ",".join(str(account_id) for account_id in account_to_base)
                rows = db.execute(query.replace(placeholders, safe_ids), params).fetchall()
        except sqlite3.Error:
            log.exception("Could not read Apple base metrics")
            return result
        for row in rows:
            bucket = result[account_to_base[row["account_id"]]]
            bucket["sent"] += row["sent"] or 0
            bucket["replied"] += row["replied"] or 0
        return result

    def outreach_texts(self, project: str, start: int, end: int | None) -> list[sqlite3.Row]:
        """Message-level results: which first outreach texts create replies."""
        if not self.hub_db.exists(): return []
        query = """
          SELECT o.script_label, COUNT(*) sent, MAX(o.sent_at) last_sent_at,
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
        return await self.call("getUpdates", offset=offset, timeout=45, allowed_updates=["message", "callback_query", "channel_post"])


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
                {"text": "\U0001F4C5 \u042D\u0442\u0430 \u043D\u0435\u0434\u0435\u043B\u044F", "callback_data": "week"},
            ],
            [
                {"text": "\U0001F4C6 \u041F\u0440\u0435\u0434\u044B\u0434\u0443\u0449\u0430\u044F \u043D\u0435\u0434\u0435\u043B\u044F", "callback_data": "previous_week"},
                {"text": "♾ За всё время", "callback_data": "all"},
            ],
            [{"text": "\U0001F9EA \u0421\u043A\u0440\u0438\u043F\u0442\u044B", "callback_data": "scripts_menu"}],
            [{"text": "\U0001F4C8 2-е сообщение", "callback_data": "second_message_analytics"}],
            [{"text": "\U0001F4DA \u041F\u043E \u0434\u043D\u044F\u043C \u043D\u0435\u0434\u0435\u043B\u0438", "callback_data": "week_days"}],
            [{"text": "\U0001F4CC \u041E\u0442\u0434\u0435\u043B\u044C\u043D\u044B\u0439 \u043F\u0440\u043E\u0435\u043A\u0442", "callback_data": "project_menu"}],
        ]}



    @staticmethod
    def project_menu_keyboard() -> dict:
        return {"inline_keyboard": [
            [{"text": "АЙФОНЫ", "callback_data": "project:apple"}, {"text": "ГОСЗАКУПКИ", "callback_data": "project:tenders"}],
            [{"text": "МОРЕПРОДУКТЫ", "callback_data": "project:seafood"}, {"text": "ТРЕЙДИНГ", "callback_data": "project:trading"}],
            [{"text": "← К отчётам", "callback_data": "reports_menu"}],
        ]}



    @staticmethod
    def project_keyboard(code: str) -> dict:
        return {"inline_keyboard": [
            [{"text": "◀️ Вчера", "callback_data": f"project_report:{code}:day"}, {"text": "\U0001F4C5 \u042D\u0442\u0430 \u043D\u0435\u0434\u0435\u043B\u044F", "callback_data": f"project_report:{code}:week"}],
            [{"text": "\U0001F4C6 \u041F\u0440\u0435\u0434\u044B\u0434\u0443\u0449\u0430\u044F \u043D\u0435\u0434\u0435\u043B\u044F", "callback_data": f"project_report:{code}:previous_week"}, {"text": "♾ За всё время", "callback_data": f"project_report:{code}:all"}],
            [{"text": "\U0001F4DA \u041F\u043E \u0434\u043D\u044F\u043C \u043D\u0435\u0434\u0435\u043B\u0438", "callback_data": f"project_week_days:{code}"}],
            [{"text": "← К проектам", "callback_data": "project_menu"}],
        ]}



    @staticmethod
    def scripts_keyboard(project: str | None = None) -> dict:
        rows = []
        if project:
            code = "tenders" if project == "ГОСЗАКУПКИ" else "trading"
            rows.append([{"text": "\U0001F5C2 \u0412\u0441\u0435 \u0441\u043A\u0440\u0438\u043F\u0442\u044B \u0437\u0430 \u0432\u0441\u0451 \u0432\u0440\u0435\u043C\u044F", "callback_data": f"scripts_all_{code}"}])
        rows.append([
            {"text": "Госзакупки", "callback_data": "scripts_tenders"},
            {"text": "Трейдинг", "callback_data": "scripts_trading"},
        ])
        rows.append([{"text": "← К отчётам", "callback_data": "reports_menu"}])
        return {"inline_keyboard": [
            *rows,
        ]}

    def report_chat_id(self) -> int | None:
        value = self.store.get("report_chat_id")
        return int(value) if value else None

    def is_admin(self, user_id: int) -> bool:
        admins = {int(x) for x in self.store.get("admin_ids").split(",") if x.strip()}
        return user_id in admins

    def has_access(self, user_id: int) -> bool:
        return self.is_admin(user_id) or self.store.has_user(user_id)

    def period(self, kind: str) -> tuple[int, int | None, str]:
        now = dt.datetime.now(self.s.timezone)
        if kind == "day":
            day = now.date() - dt.timedelta(days=1)
            start = dt.datetime.combine(day, dt.time.min, self.s.timezone); end = start + dt.timedelta(days=1)
            return int(start.timestamp()), int(end.timestamp()), day.strftime("%d.%m.%Y")
        if kind == "week":
            start = dt.datetime.combine(now.date() - dt.timedelta(days=now.weekday()), dt.time.min, self.s.timezone)
            return int(start.timestamp()), int(now.timestamp()) + 1, f"{start:%d.%m}–{now:%d.%m.%Y}"
        if kind == "previous_week":
            this_week_start = now.date() - dt.timedelta(days=now.weekday())
            previous_week_start = this_week_start - dt.timedelta(days=7)
            start = dt.datetime.combine(previous_week_start, dt.time.min, self.s.timezone)
            end = dt.datetime.combine(this_week_start, dt.time.min, self.s.timezone)
            return int(start.timestamp()), int(end.timestamp()), f"{previous_week_start:%d.%m}–{(this_week_start - dt.timedelta(days=1)):%d.%m.%Y}"
        if kind == "month":
            start = dt.datetime(now.year, now.month, 1, tzinfo=self.s.timezone)
            return int(start.timestamp()), int(now.timestamp()) + 1, now.strftime("%B %Y")
        return 0, None, "за всё время"

    def format_report(self, kind: str, start: int | None = None, end: int | None = None, label: str | None = None, project_filter: str | None = None) -> str:
        if start is None:
            start, end, label = self.period(kind)
        assert label is not None
        titles = {"day": "Ежедневная статистика", "week": "Статистика за неделю", "previous_week": "Статистика за предыдущую неделю", "month": "Статистика за месяц", "all": "Статистика за всё время"}
        result = [
            f"<b>{titles[kind]}</b>",
            f"Период: {html.escape(label)}",
        ]
        summary = self.metrics.summary(start, end)
        # A report must have a stable layout.  Zero is operationally useful:
        # it means no activity, not a missing or random metric.
        core_projects = ("АЙФОНЫ", "ГОСЗАКУПКИ", "МОРЕПРОДУКТЫ", "ТРЕЙДИНГ")
        projects = (project_filter,) if project_filter else list(core_projects) + sorted((name for name in summary if name not in core_projects), key=str.casefold)
        project_icons = {
            "АЙФОНЫ": "🍏",
            "ГОСЗАКУПКИ": "🏛️",
            "МОРЕПРОДУКТЫ": "🦐",
            "ТРЕЙДИНГ": "📈",
        }
        for project in projects:
            m = summary[project]
            sent, replied = m["sent"], m["replied"]
            conversion = f"{replied / sent * 100:.1f}%" if sent else "—"
            result.extend([
                f"\n<b>{project_icons.get(project, '📊')} {html.escape(project)}</b>",
                f"• Отправлено: <b>{sent}</b>",
                f"• Ответили: <b>{replied}</b> ({conversion})",
            ])
            def dual_conversion(value: int) -> str:
                from_sent = f"{value / sent * 100:.1f}%" if sent else "—"
                from_replied = f"{value / replied * 100:.1f}%" if replied else "—"
                return f"(из отправленных: {from_sent}, из ответивших: {from_replied})"

            requests = m["requests"]
            if project == "ГОСЗАКУПКИ":
                result.append(f"• 2-е сообщение (ссылка на канал): <b>{m['second_sent']}</b>")
            result.append(f"• Заявки в канал: <b>{requests}</b> {dual_conversion(requests)}")
            if project == "ГОСЗАКУПКИ":
                result.append(f"• 3-е сообщение (ссылка на сайт): <b>{m['third_sent']}</b>")
            if project == "АЙФОНЫ":
                result.append(f"• Лиды Маши: <b>{m['leads']}</b> {dual_conversion(m['leads'])}")
                result.append("<b>Базы Apple</b>")
                for base, values in self.metrics.apple_base_summary(start, end).items():
                    rate = f"{values['replied'] / values['sent'] * 100:.1f}%" if values["sent"] else "—"
                    result.append(f"• {base}: отправлено <b>{values['sent']}</b>, ответили <b>{values['replied']}</b> ({rate})")
            elif m["leads"]:
                result.append(f"• Лиды: <b>{m['leads']}</b> {dual_conversion(m['leads'])}")
        return "\n".join(result)

    def format_second_message_analytics(self) -> str:
        rows = self.metrics.second_message_variants("ГОСЗАКУПКИ")
        header = "<b>\U0001F4C8 Конверсия 2-го сообщения · ГОСЗАКУПКИ</b>\nНовые отправки с 25.08.2026. Заявка привязывается к тому же получателю и кампании."
        if not rows:
            return header + "\n\nПока недостаточно новых размеченных отправок для сравнения вариантов."
        result = [header]
        for index, row in enumerate(rows, 1):
            rate = row["requests"] / row["sent"] * 100 if row["sent"] else 0
            text = html.escape(" ".join(row["text"].split()))
            result.append(f"\n<b>{index}. {row['requests']}/{row['sent']} заявок — {rate:.1f}%</b>\n<blockquote>{text}</blockquote>")
        return "\n".join(result)

    def script_pages(self, project: str, active_only: bool = True) -> list[str]:
        rows = [row for row in self.metrics.outreach_texts(project, 0, None) if row["script_label"] != "[медиа/файл]"]
        cutoff = int((dt.datetime.now(self.s.timezone) - dt.timedelta(days=2)).timestamp())
        if active_only:
            rows = [row for row in rows if row["last_sent_at"] >= cutoff]
        period_label = "Актуальные: отправлялись за последние 2 дня" if active_only else "Все скрипты за всё время, включая отключённые"
        if not rows:
            return [f"<b>Скрипты · {html.escape(project)}</b>\n{period_label}\n\nПока нет подходящих скриптов."]
        items = [{"text": row["script_label"], "sent": row["sent"], "replied": row["replied"], "rate": row["replied"] / row["sent"] if row["sent"] else 0} for row in rows]
        total_sent, total_replied = sum(x["sent"] for x in items), sum(x["replied"] for x in items)
        average = total_replied / total_sent if total_sent else 0
        reliable = [x for x in items if x["sent"] >= 20]
        # Absolute labels are misleading: what is strong differs by project.
        # A top script must beat the overall rate by 25% (or by 5 pp); a
        # working script is at/above average, and the remainder needs work.
        top_threshold = max(average * 1.25, average + 0.05)
        top = sorted([x for x in reliable if x["rate"] >= top_threshold], key=lambda x: (x["rate"], x["sent"]), reverse=True)
        working = sorted([x for x in reliable if average <= x["rate"] < top_threshold], key=lambda x: (x["rate"], x["sent"]), reverse=True)
        weak = sorted([x for x in reliable if x["rate"] < average and x["replied"] > 0], key=lambda x: (x["rate"], -x["sent"]))
        tests = sorted([x for x in items if x["sent"] < 20 and x["replied"] > 0], key=lambda x: (x["rate"], x["sent"]), reverse=True)
        zero_tests = [x for x in items if x["sent"] < 20 and not x["replied"]]
        zero_reliable = [x for x in reliable if not x["replied"]]
        def show(index: int, item: dict) -> str:
            text = html.escape(" ".join(item["text"].split()))
            return f"<b>{index}. {item['replied']}/{item['sent']} ответов — {item['rate'] * 100:.1f}%</b>\n<blockquote>{text}</blockquote>"
        header = "\n".join([
            f"<b>Скрипты · {html.escape(project)}</b>",
            period_label,
            f"Всего по текстовым скриптам: <b>{total_replied}/{total_sent}</b> ответов · <b>{average * 100:.1f}%</b>",
        ])
        sections: list[tuple[str, list[dict]]] = [
            (f"🏆 <b>Топовые скрипты</b>\nВыборка от 20 отправок; конверсия от {top_threshold * 100:.1f}% — минимум на 25% выше средней.", top),
            ("✅ <b>Рабочие скрипты</b>\nВыборка от 20 отправок; конверсия на уровне или выше средней.", working),
            ("📉 <b>Нужна доработка</b>\nВыборка от 20 отправок, но конверсия ниже средней.", weak),
            (f"🧪 <b>Ещё тестируем</b>\n{len(tests)} скриптов с ответами, но выборка меньше 20 отправок — выводы делать рано.", tests),
        ]
        blocks = [header]
        for title, group in sections:
            if not group: continue
            blocks.append(title)
            blocks.extend(show(index, item) for index, item in enumerate(group, 1))
        zero_items = sorted(zero_reliable + zero_tests, key=lambda x: x["sent"], reverse=True)
        zero_count = len(zero_items)
        if zero_count:
            zero_sent = sum(item["sent"] for item in zero_items)
            if active_only:
                blocks.append(f"⚪ <b>Без ответов</b>\n{zero_count} актуальных скриптов: 0 ответов на {zero_sent} отправок.")
                blocks.extend(show(index, item) for index, item in enumerate(zero_items, 1))
            else:
                blocks.append(f"⚪ <b>Без ответов</b>\nЗа всё время: <b>{zero_count}</b> скриптов не получили ответов после <b>{zero_sent}</b> отправок. Тексты этих скриптов скрыты, чтобы отчёт оставался компактным.")
        pages: list[str] = []
        current = ""
        for block in blocks:
            candidate = f"{current}\n\n{block}" if current else block
            if current and len(candidate) > 3500:
                pages.append(current); current = block
            else:
                current = candidate
        if current: pages.append(current)
        return pages

    def format_scripts(self, project: str, active_only: bool = True) -> str:
        """Kept for diagnostic use; Telegram sends all pages via the callback."""
        return "\n\n".join(self.script_pages(project, active_only))

    async def send_script_pages(self, chat_id: int, project: str, active_only: bool = True) -> None:
        pages = self.script_pages(project, active_only)
        for page_number, page in enumerate(pages, 1):
            suffix = f"\n\n<i>Страница {page_number}/{len(pages)}</i>" if len(pages) > 1 else ""
            await self.tg.send(chat_id, page + suffix, self.scripts_keyboard(project))

    async def send_report(self, kind: str, start: int | None = None, end: int | None = None, label: str | None = None, period_start: str | None = None) -> bool:
        recipients = self.store.user_ids()
        if not recipients:
            log.warning("Report not sent: there are no subscribed users")
            return False
        text = self.format_report(kind, start, end, label)
        delivered_to_all = True
        for user_id in recipients:
            if period_start and self.store.report_delivered(kind, period_start, user_id):
                continue
            try:
                await self.tg.send(user_id, text, self.keyboard())
                if period_start:
                    self.store.mark_report_delivered(kind, period_start, user_id)
            except Exception:
                delivered_to_all = False
                log.exception("Could not send %s report to user %s", kind, user_id)
        return delivered_to_all

    async def send_week_days(self, chat_id: int, project_filter: str | None = None, keyboard: dict | None = None) -> None:
        now = dt.datetime.now(self.s.timezone)
        monday = now.date() - dt.timedelta(days=now.weekday())
        last_day = now.date() - dt.timedelta(days=1)
        if last_day < monday:
            await self.tg.send(chat_id, "За текущую неделю ещё нет завершённых дней.", keyboard or self.keyboard())
            return
        for offset in range((last_day - monday).days + 1):
            day = monday + dt.timedelta(days=offset)
            start = dt.datetime.combine(day, dt.time.min, self.s.timezone)
            end = start + dt.timedelta(days=1)
            await self.tg.send(chat_id, self.format_report("day", int(start.timestamp()), int(end.timestamp()), day.strftime("%d.%m.%Y"), project_filter), keyboard or self.keyboard())

    async def handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message")
        callback = update.get("callback_query")
        channel_post = update.get("channel_post")
        if channel_post:
            await self.handle_apple_lead_post(channel_post)
            return
        if callback:
            user_id = callback["from"]["id"]
            if not self.has_access(user_id): return
            await self.tg.call("answerCallbackQuery", callback_query_id=callback["id"])
            chat_id = callback["message"]["chat"]["id"]
            action = callback["data"]
            project_codes = {"apple": "АЙФОНЫ", "tenders": "ГОСЗАКУПКИ", "seafood": "МОРЕПРОДУКТЫ", "trading": "ТРЕЙДИНГ"}
            if action == "week_days":
                await self.send_week_days(chat_id)
            elif action == "project_menu":
                await self.tg.send(chat_id, "<b>Отдельный проект</b>\nВыберите проект.", self.project_menu_keyboard())
            elif action.startswith("project:"):
                code = action.split(":", 1)[1]
                project = project_codes.get(code)
                if project:
                    await self.tg.send(chat_id, f"<b>{html.escape(project)}</b>\nВыберите период.", self.project_keyboard(code))
            elif action.startswith("project_report:"):
                _, code, kind = action.split(":", 2)
                project = project_codes.get(code)
                if project and kind in {"day", "week", "previous_week", "all"}:
                    await self.tg.send(chat_id, self.format_report(kind, project_filter=project), self.project_keyboard(code))
            elif action.startswith("project_week_days:"):
                code = action.split(":", 1)[1]
                project = project_codes.get(code)
                if project:
                    await self.send_week_days(chat_id, project, self.project_keyboard(code))
            elif action == "second_message_analytics":
                await self.tg.send(chat_id, self.format_second_message_analytics(), self.keyboard())
            elif action == "scripts_menu":
                await self.tg.send(chat_id, "<b>Скрипты</b>\nВыбери проект. Сначала показываются только актуальные варианты за последние 2 дня.", self.scripts_keyboard())
            elif action == "scripts_tenders":
                await self.send_script_pages(chat_id, "ГОСЗАКУПКИ")
            elif action == "scripts_trading":
                await self.send_script_pages(chat_id, "ТРЕЙДИНГ")
            elif action == "scripts_all_tenders":
                await self.send_script_pages(chat_id, "ГОСЗАКУПКИ", active_only=False)
            elif action == "scripts_all_trading":
                await self.send_script_pages(chat_id, "ТРЕЙДИНГ", active_only=False)
            elif action == "reports_menu":
                await self.tg.send(chat_id, "Отчёты по проектам.", self.keyboard())
            else:
                await self.tg.send(chat_id, self.format_report(action), self.keyboard())
            return
        if not message or not message.get("text"): return
        user_id, chat_id = message["from"]["id"], message["chat"]["id"]
        command = message["text"].split()[0].split("@", 1)[0]
        if command == "/start":
            self.store.add_user(user_id)
            if not self.store.get("admin_ids"):
                self.store.set("admin_ids", str(user_id)); self.store.set("report_chat_id", str(chat_id))
                await self.tg.send(chat_id, "✅ Этот чат назначен для автоматических отчётов. Доступ получили вы.", self.keyboard()); return
            await self.tg.send(chat_id, "✅ Доступ к статистике открыт.", self.keyboard())
            return
        if not self.has_access(user_id): return
        if command == "/set_report_chat":
            if self.is_admin(user_id):
                self.store.set("report_chat_id", str(chat_id)); await self.tg.send(chat_id, "✅ Этот чат назначен для автоматических отчётов.", self.keyboard())
        elif command == "/yesterday": await self.tg.send(chat_id, self.format_report("day"), self.keyboard())
        elif command == "/week": await self.tg.send(chat_id, self.format_report("week"), self.keyboard())
        elif command == "/previous_week": await self.tg.send(chat_id, self.format_report("previous_week"), self.keyboard())
        elif command == "/all": await self.tg.send(chat_id, self.format_report("all"), self.keyboard())
        elif command == "/help": await self.tg.send(chat_id, "Команды: /yesterday, /week, /previous_week, /all, /set_report_chat", self.keyboard())

    async def handle_apple_lead_post(self, post: dict[str, Any]) -> None:
        """Count every photo post in Masha's configured lead channel as one lead."""
        chat_id = post["chat"]["id"]
        configured = self.store.apple_leads_channel_id()
        if configured is None:
            configured = chat_id
        elif configured != chat_id:
            return
        if not post.get("photo"):
            log.debug("Ignored non-photo post %s in Apple lead channel", post.get("message_id"))
            return
        if self.store.apple_leads_channel_id() is None:
            self.store.set_apple_leads_channel_id(chat_id)
            log.info("Apple lead channel bound to %s", chat_id)
        inserted = self.store.add_event({
            "source": "masha_apple_leads",
            "event_key": f"masha-apple-lead:{chat_id}:{post['message_id']}",
            "project": "АЙФОНЫ",
            "event_type": "lead",
            "occurred_at": post.get("date", int(dt.datetime.now(dt.timezone.utc).timestamp())),
            "payload": {"channel_id": chat_id, "message_id": post["message_id"]},
        })
        log.info("Apple photo lead %s: post=%s", "recorded" if inserted else "already present", post["message_id"])

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
            # Retry after midnight if a restart or connection failure skipped 00:01.
            # sent_reports makes each delivery idempotent.
            if now.time() >= dt.time(0, 1):
                yesterday = (now.date() - dt.timedelta(days=1)).isoformat()
                if not self.store.report_sent("day", yesterday):
                    if await self.send_report("day", period_start=yesterday):
                        self.store.mark_report_sent("day", yesterday)
                if now.weekday() == 0:
                    week_start = now.date() - dt.timedelta(days=7)
                    week_end = dt.datetime.combine(now.date(), dt.time.min, self.s.timezone)
                    week = week_start.isoformat()
                    if not self.store.report_sent("week", week):
                        if await self.send_report("week", int(dt.datetime.combine(week_start, dt.time.min, self.s.timezone).timestamp()), int(week_end.timestamp()), f"{week_start:%d.%m}–{(now.date() - dt.timedelta(days=1)):%d.%m.%Y}", week):
                            self.store.mark_report_sent("week", week)
                if now.day == 1:
                    month_end = dt.datetime(now.year, now.month, 1, tzinfo=self.s.timezone)
                    previous_month_start = (month_end - dt.timedelta(days=1)).replace(day=1)
                    previous_month = previous_month_start.strftime("%Y-%m")
                    if not self.store.report_sent("month", previous_month):
                        if await self.send_report("month", int(previous_month_start.timestamp()), int(month_end.timestamp()), previous_month_start.strftime("%B %Y"), previous_month):
                            self.store.mark_report_sent("month", previous_month)
            await asyncio.sleep(30)

    async def ingest(self, request: web.Request) -> web.Response:
        if self.s.ingest_token and request.headers.get("Authorization") != f"Bearer {self.s.ingest_token}":
            log.warning("Rejected analytics event: invalid or missing bearer token from %s", request.remote)
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
