# -*- coding: utf-8 -*-
"""
Планировщик задач — сервер Mini App.

Что делает:
  • раздаёт index.html (само приложение);
  • REST API /api/tasks — хранит задачи каждого пользователя в SQLite;
  • проверяет подпись Telegram initData (никто не подделает чужой user_id);
  • принимает вебхук бота: /start (кнопка открытия приложения),
    /stats (статистика по всем пользователям — только для администратора);
  • раз в день присылает напоминание о просроченных задачах.

Переменные окружения:
  BOT_TOKEN          — токен бота от BotFather (обязательно)
  PUBLIC_URL         — публичный адрес сервера, напр. https://myapp.up.railway.app
                       (если задан, вебхук настроится автоматически при старте)
  ADMIN_ID           — ваш Telegram ID для команды /stats (необязательно)
  REMINDER_HOUR_UTC  — час отправки напоминаний по UTC, по умолчанию 6 (≈9:00 МСК)

Запуск локально:
  pip install -r requirements.txt
  BOT_TOKEN=123:abc uvicorn server:app --host 0.0.0.0 --port 8000
"""

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0") or 0)
REMINDER_HOUR_UTC = int(os.environ.get("REMINDER_HOUR_UTC", "6"))
WEBHOOK_SECRET = hashlib.sha256(("wh" + BOT_TOKEN).encode()).hexdigest()[:32]
DB_PATH = os.environ.get("DB_PATH", "planner.db")
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
BASE_DIR = Path(__file__).parent

MAX_TASKS = 500          # лимит задач на пользователя
INIT_DATA_TTL = 86400    # initData старше суток не принимаем

# ---------------------------------------------------------------- база данных

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            user_id    INTEGER PRIMARY KEY,
            first_name TEXT,
            last_seen  TEXT
        );
        CREATE TABLE IF NOT EXISTS tasks(
            user_id      INTEGER NOT NULL,
            id           TEXT    NOT NULL,
            title        TEXT    NOT NULL,
            date         TEXT    NOT NULL,
            prio         TEXT    NOT NULL DEFAULT 'mid',
            done         INTEGER NOT NULL DEFAULT 0,
            created_at   TEXT,
            completed_at TEXT,
            PRIMARY KEY (user_id, id)
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_overdue ON tasks(done, date);
        CREATE TABLE IF NOT EXISTS reminders(
            user_id   INTEGER PRIMARY KEY,
            last_sent TEXT
        );
        """)

# ------------------------------------------------- проверка Telegram initData

def validate_init_data(init_data: str) -> dict | None:
    """Проверяет подпись initData по алгоритму Telegram. Возвращает user или None."""
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None
        check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, received_hash):
            return None
        if time.time() - int(parsed.get("auth_date", "0")) > INIT_DATA_TTL:
            return None
        user = json.loads(parsed.get("user", "{}"))
        return user if user.get("id") else None
    except Exception:
        return None

def require_user(x_init_data: str | None) -> dict:
    user = validate_init_data(x_init_data or "")
    if not user:
        raise HTTPException(status_code=401, detail="Invalid Telegram initData")
    with db() as c:
        c.execute(
            "INSERT INTO users(user_id, first_name, last_seen) VALUES(?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET first_name=excluded.first_name, last_seen=excluded.last_seen",
            (user["id"], user.get("first_name", ""), datetime.now(timezone.utc).isoformat()),
        )
    return user

# --------------------------------------------------------------- Telegram API

async def tg_call(method: str, payload: dict) -> None:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(f"{TG_API}/{method}", json=payload)
    except Exception as e:
        print(f"tg_call {method} error: {e}")

def open_app_keyboard() -> dict:
    return {"inline_keyboard": [[{
        "text": "📋 Открыть планировщик",
        "web_app": {"url": PUBLIC_URL or "https://example.com"},
    }]]}

# ------------------------------------------------------------------ жизненный цикл

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if PUBLIC_URL and BOT_TOKEN:
        await tg_call("setWebhook", {
            "url": f"{PUBLIC_URL}/webhook",
            "secret_token": WEBHOOK_SECRET,
            "allowed_updates": ["message"],
        })
        print(f"Webhook: {PUBLIC_URL}/webhook")
    task = asyncio.create_task(reminder_loop())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

# ------------------------------------------------------------------ статика

@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "index.html")

@app.get("/health")
async def health():
    return {"ok": True}

# ------------------------------------------------------------------ API задач

def row_to_task(r: sqlite3.Row) -> dict:
    return {
        "id": r["id"], "title": r["title"], "date": r["date"], "prio": r["prio"],
        "done": bool(r["done"]), "createdAt": r["created_at"], "completedAt": r["completed_at"],
    }

@app.get("/api/tasks")
async def get_tasks(x_init_data: str | None = Header(default=None)):
    user = require_user(x_init_data)
    with db() as c:
        rows = c.execute("SELECT * FROM tasks WHERE user_id=?", (user["id"],)).fetchall()
    return [row_to_task(r) for r in rows]

@app.put("/api/tasks")
async def put_tasks(request: Request, x_init_data: str | None = Header(default=None)):
    user = require_user(x_init_data)
    try:
        tasks = await request.json()
        assert isinstance(tasks, list)
    except Exception:
        raise HTTPException(status_code=400, detail="Expected JSON array")
    if len(tasks) > MAX_TASKS:
        raise HTTPException(status_code=413, detail=f"Too many tasks (max {MAX_TASKS})")

    clean = []
    for t in tasks:
        if not isinstance(t, dict):
            continue
        title = str(t.get("title", ""))[:200].strip()
        date = str(t.get("date", ""))[:10]
        if not title or len(date) != 10:
            continue
        prio = t.get("prio") if t.get("prio") in ("low", "mid", "high") else "mid"
        clean.append((
            user["id"],
            str(t.get("id", secrets.token_hex(6)))[:32],
            title, date, prio,
            1 if t.get("done") else 0,
            str(t.get("createdAt") or "")[:32] or None,
            str(t.get("completedAt") or "")[:32] or None,
        ))
    with db() as c:
        c.execute("DELETE FROM tasks WHERE user_id=?", (user["id"],))
        c.executemany(
            "INSERT OR REPLACE INTO tasks(user_id,id,title,date,prio,done,created_at,completed_at) "
            "VALUES(?,?,?,?,?,?,?,?)", clean)
    return {"saved": len(clean)}

# ------------------------------------------------------------------ вебхук бота

@app.post("/webhook")
async def webhook(request: Request,
                  x_telegram_bot_api_secret_token: str | None = Header(default=None)):
    if x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Bad secret")
    update = await request.json()
    msg = update.get("message") or {}
    text = (msg.get("text") or "").strip()
    chat_id = (msg.get("chat") or {}).get("id")
    from_id = (msg.get("from") or {}).get("id")
    if not chat_id:
        return JSONResponse({"ok": True})

    if text.startswith("/start"):
        await tg_call("sendMessage", {
            "chat_id": chat_id,
            "text": "Привет! Это планировщик задач.\n\n"
                    "Планируйте день, отмечайте выполненное и следите за прогрессом. "
                    "Каждое утро я напомню о просроченных задачах.",
            "reply_markup": open_app_keyboard(),
        })
    elif text.startswith("/stats") and ADMIN_ID and from_id == ADMIN_ID:
        with db() as c:
            users = c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
            total = c.execute("SELECT COUNT(*) n FROM tasks").fetchone()["n"]
            done = c.execute("SELECT COUNT(*) n FROM tasks WHERE done=1").fetchone()["n"]
            week = c.execute(
                "SELECT COUNT(*) n FROM tasks WHERE done=1 AND completed_at >= ?",
                (datetime.now(timezone.utc).strftime("%Y-%m-%d"),)).fetchone()["n"]
        rate = round(done / total * 100) if total else 0
        await tg_call("sendMessage", {
            "chat_id": chat_id,
            "text": f"📊 Статистика приложения\n\n"
                    f"👥 Пользователей: {users}\n"
                    f"📝 Всего задач: {total}\n"
                    f"✅ Выполнено: {done} ({rate}%)\n"
                    f"🔥 Выполнено сегодня: {week}",
        })
    return JSONResponse({"ok": True})

# ------------------------------------------------------------------ напоминания

async def send_reminders() -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with db() as c:
        rows = c.execute("""
            SELECT t.user_id, COUNT(*) n
            FROM tasks t
            LEFT JOIN reminders r ON r.user_id = t.user_id
            WHERE t.done = 0 AND t.date < ?
              AND (r.last_sent IS NULL OR r.last_sent < ?)
            GROUP BY t.user_id
        """, (today, today)).fetchall()
    for r in rows:
        n = r["n"]
        word = "задача" if n % 10 == 1 and n % 100 != 11 else \
               "задачи" if 2 <= n % 10 <= 4 and not 10 <= n % 100 <= 20 else "задач"
        await tg_call("sendMessage", {
            "chat_id": r["user_id"],
            "text": f"⏰ У вас {n} просроченных {word}. Загляните в планировщик!",
            "reply_markup": open_app_keyboard(),
        })
        with db() as c:
            c.execute("INSERT OR REPLACE INTO reminders(user_id,last_sent) VALUES(?,?)",
                      (r["user_id"], today))
        await asyncio.sleep(0.05)  # бережём лимиты Bot API

async def reminder_loop() -> None:
    while True:
        try:
            if datetime.now(timezone.utc).hour == REMINDER_HOUR_UTC:
                await send_reminders()
        except Exception as e:
            print(f"reminder error: {e}")
        await asyncio.sleep(1800)  # проверяем каждые 30 минут
