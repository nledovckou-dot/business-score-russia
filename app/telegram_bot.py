"""Telegram bot for BSR -- send company URL, get business report link.

Uses raw urllib (no python-telegram-bot dependency).
Runs as a separate process alongside the main FastAPI app:
    python -m app.telegram_bot

Environment variables:
    TELEGRAM_BOT_TOKEN  -- Telegram Bot API token (required)
    BSR_API_URL         -- base URL of the BSR FastAPI app (default: http://localhost:8083)
    BSR_PUBLIC_URL      -- public URL for report links (default: http://89.167.19.68:8090)
    TELEGRAM_ALLOWED_USERS -- comma-separated Telegram user IDs (empty = allow all)
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger("bsr.telegram_bot")

# ── Config ────────────────────────────────────────────────────────────────────

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
BSR_API_URL = os.environ.get("BSR_API_URL", "http://localhost:8083").rstrip("/")
BSR_PUBLIC_URL = os.environ.get("BSR_PUBLIC_URL", "http://89.167.19.68:8090").rstrip("/")

_allowed_raw = os.environ.get("TELEGRAM_ALLOWED_USERS", "")
ALLOWED_USERS: set[int] = set()
if _allowed_raw.strip():
    for uid in _allowed_raw.split(","):
        uid = uid.strip()
        if uid.isdigit():
            ALLOWED_USERS.add(int(uid))

# Timeouts (seconds)
ANALYSIS_TIMEOUT = 600       # 10 minutes max for full pipeline
POLL_INTERVAL = 3            # session status poll interval
TYPING_INTERVAL = 4          # re-send typing every N seconds

# URL pattern (simplified)
URL_RE = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)
DOMAIN_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# ── Telegram API helpers ──────────────────────────────────────────────────────

TG_API = "https://api.telegram.org/bot{token}/{method}"


def _tg_request(method: str, payload: dict[str, Any] | None = None) -> dict:
    """Call Telegram Bot API method. Returns parsed JSON response."""
    url = TG_API.format(token=BOT_TOKEN, method=method)
    if payload is None:
        payload = {}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=35) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        logger.error("Telegram API error %s %s: %s", method, e.code, body)
        return {"ok": False, "description": body}
    except Exception as e:
        logger.error("Telegram API request failed %s: %s", method, e)
        return {"ok": False, "description": str(e)}


def send_message(
    chat_id: int,
    text: str,
    parse_mode: str = "HTML",
    reply_markup: dict | None = None,
    disable_web_page_preview: bool = False,
) -> dict:
    """Send a text message to a Telegram chat."""
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if disable_web_page_preview:
        payload["disable_web_page_preview"] = True
    return _tg_request("sendMessage", payload)


def edit_message(
    chat_id: int,
    message_id: int,
    text: str,
    parse_mode: str = "HTML",
    reply_markup: dict | None = None,
) -> dict:
    """Edit an existing message."""
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return _tg_request("editMessageText", payload)


def answer_callback(callback_query_id: str, text: str = "") -> dict:
    """Answer a callback query (dismiss the loading indicator)."""
    payload: dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    return _tg_request("answerCallbackQuery", payload)


def send_typing(chat_id: int) -> None:
    """Send 'typing...' indicator."""
    _tg_request("sendChatAction", {"chat_id": chat_id, "action": "typing"})


# ── BSR API helpers ───────────────────────────────────────────────────────────


def _bsr_post(endpoint: str, payload: dict | None = None) -> dict:
    """POST to BSR API. Returns parsed JSON."""
    url = f"{BSR_API_URL}{endpoint}"
    data = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        logger.error("BSR API error %s %s: %s", endpoint, e.code, body)
        try:
            return json.loads(body)
        except Exception:
            return {"ok": False, "error": body}
    except Exception as e:
        logger.error("BSR API request failed %s: %s", endpoint, e)
        return {"ok": False, "error": str(e)}


def _bsr_get(endpoint: str) -> dict:
    """GET from BSR API. Returns parsed JSON."""
    url = f"{BSR_API_URL}{endpoint}"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        logger.error("BSR API GET error %s %s: %s", endpoint, e.code, body)
        try:
            return json.loads(body)
        except Exception:
            return {"ok": False, "error": body}
    except Exception as e:
        logger.error("BSR API GET request failed %s: %s", endpoint, e)
        return {"ok": False, "error": str(e)}


def _bsr_session_status(sid: str) -> dict:
    """Poll session status via GET /api/session/{sid}."""
    return _bsr_get(f"/api/session/{sid}")


# ── Handlers ──────────────────────────────────────────────────────────────────


def handle_start(chat_id: int, user_name: str) -> None:
    """Handle /start command."""
    text = (
        f"Привет, {user_name}! Я -- бот <b>Анализ бизнеса 360</b>.\n\n"
        "Отправь мне ссылку на сайт компании, и я подготовлю полный бизнес-отчёт:\n\n"
        "  - Финансы (ФНС)\n"
        "  - Конкуренты\n"
        "  - SWOT-анализ\n"
        "  - HR и зарплаты\n"
        "  - AI-совет директоров\n\n"
        "Пример: <code>https://example.com</code>\n\n"
        "Также можно отправить домен без https:// -- я пойму."
    )
    send_message(chat_id, text)


def handle_help(chat_id: int) -> None:
    """Handle /help command."""
    text = (
        "<b>Команды:</b>\n"
        "/start -- приветствие\n"
        "/help -- эта справка\n"
        "/status -- проверить работоспособность API\n\n"
        "<b>Как использовать:</b>\n"
        "Отправь ссылку на сайт компании (например <code>https://example.com</code>) "
        "и я запущу полный анализ. Через несколько минут пришлю ссылку на готовый отчёт.\n\n"
        "Анализ включает 10+ шагов: скрапинг сайта, определение компании, "
        "поиск в ФНС, поиск конкурентов, глубокий анализ, верификация, "
        "ревью AI-советом директоров и сборка HTML-отчёта."
    )
    send_message(chat_id, text)


def handle_status(chat_id: int) -> None:
    """Handle /status -- check BSR API health."""
    try:
        url = f"{BSR_API_URL}/api/stats"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        if data.get("ok"):
            total = data.get("total_reports", "?")
            text = (
                f"API работает.\n"
                f"Всего отчётов: {total}\n"
                f"URL: <code>{BSR_API_URL}</code>"
            )
        else:
            text = f"API ответил с ошибкой: {data.get('error', '?')}"
    except Exception as e:
        text = f"API недоступен: {e}\nURL: <code>{BSR_API_URL}</code>"
    send_message(chat_id, text)


def _normalize_url(text: str) -> str | None:
    """Try to extract/normalize a URL from user input.

    Returns normalized URL or None if input is not a valid URL/domain.
    """
    text = text.strip()

    # Already a proper URL?
    if URL_RE.match(text):
        return text

    # Bare domain like example.com or www.example.com?
    if DOMAIN_RE.match(text):
        return f"https://{text}"

    # Maybe starts with www.?
    if text.startswith("www.") and DOMAIN_RE.match(text):
        return f"https://{text}"

    return None


def handle_url(chat_id: int, url: str) -> None:
    """Full pipeline: URL -> BSR API -> report link.

    Flow:
    1. POST /api/start -> session_id
    2. Poll GET /api/session/{sid} for status changes
    3. Auto-confirm company when status = waiting_company
    4. Auto-confirm competitors when status = waiting_competitors
    5. Send report link when status = done
    """
    # 1. Start session
    send_typing(chat_id)
    progress_msg = send_message(
        chat_id,
        "Начинаю анализ...\n"
        f"URL: <code>{_escape_html(url)}</code>\n\n"
        "Шаг 1/7: Загрузка сайта...",
    )
    progress_msg_id = progress_msg.get("result", {}).get("message_id")

    start_resp = _bsr_post("/api/start", {"url": url})
    if not start_resp.get("ok"):
        error = start_resp.get("error", "Неизвестная ошибка")
        _update_progress(chat_id, progress_msg_id, f"Ошибка при запуске анализа:\n{_escape_html(error)}")
        return

    sid = start_resp["session_id"]
    logger.info("Session started: %s for URL %s (chat %s)", sid, url, chat_id)

    # 2. Poll session status through the full pipeline
    _run_pipeline_loop(chat_id, progress_msg_id, sid)


def _run_pipeline_loop(chat_id: int, progress_msg_id: int | None, sid: str) -> None:
    """Poll session status via GET /api/session/{sid} and drive the pipeline."""
    deadline = time.time() + ANALYSIS_TIMEOUT
    last_typing = 0.0
    last_step_text = ""
    last_event_count = 0
    confirmed_company = False
    confirmed_competitors = False

    while time.time() < deadline:
        # Send typing indicator periodically
        now = time.time()
        if now - last_typing > TYPING_INTERVAL:
            send_typing(chat_id)
            last_typing = now

        # Poll session status
        resp = _bsr_session_status(sid)
        if not resp.get("ok"):
            error = resp.get("error", "Сессия не найдена")
            _update_progress(
                chat_id, progress_msg_id,
                f"Ошибка: {_escape_html(error)}",
            )
            return

        status = resp.get("status", "")
        events = resp.get("events", [])
        data = resp.get("data", {})

        # Process new step events for progress updates
        if len(events) > last_event_count:
            for ev in events[last_event_count:]:
                ev_name = ev.get("event", "")
                ev_data = ev.get("data") or {}

                if ev_name == "step":
                    step_num = ev_data.get("num", "?")
                    step_text = ev_data.get("text", "")
                    step_status = ev_data.get("status", "")
                    if step_text and step_text != last_step_text:
                        last_step_text = step_text
                        status_icon = {
                            "active": "...",
                            "done": "OK",
                            "warning": "(!)",
                            "fail": "FAIL",
                        }.get(step_status, "")
                        _update_progress(
                            chat_id,
                            progress_msg_id,
                            f"Анализ <code>{sid}</code>\n"
                            f"Шаг {step_num}: {_escape_html(step_text)} {status_icon}",
                        )
            last_event_count = len(events)

        # React to status transitions
        if status == "waiting_company" and not confirmed_company:
            confirmed_company = True
            company_info = data.get("company_info", {})
            fns_data = data.get("fns_data", {})
            fns_company = fns_data.get("fns_company", {})

            # Auto-confirm with detected data
            confirm_data = {
                "name": company_info.get("name", ""),
                "inn": fns_company.get("inn", company_info.get("inn", "")),
                "legal_name": fns_company.get("full_name", company_info.get("legal_name", "")),
                "address": fns_company.get("address", company_info.get("address", "")),
                "business_type_guess": company_info.get("business_type_guess", "B2B_SERVICE"),
            }

            company_name = confirm_data["name"] or "?"
            inn = confirm_data["inn"] or "не найден"

            _update_progress(
                chat_id,
                progress_msg_id,
                f"Компания определена: <b>{_escape_html(company_name)}</b>\n"
                f"ИНН: <code>{_escape_html(inn)}</code>\n"
                f"Тип: {_escape_html(confirm_data['business_type_guess'])}\n\n"
                "Подтверждаю автоматически, ищу конкурентов...",
            )

            _bsr_post(f"/api/confirm-company/{sid}", confirm_data)
            logger.info("Auto-confirmed company: %s (INN: %s)", company_name, inn)

        elif status == "waiting_competitors" and not confirmed_competitors:
            confirmed_competitors = True
            competitors = data.get("competitors", [])
            market_name = data.get("market_name", "")

            # Build competitor list text
            comp_lines = []
            for i, c in enumerate(competitors[:10], 1):
                name = c.get("name", "?")
                threat = c.get("threat_level", "med")
                verified = c.get("verified", True)
                ver_mark = "" if verified else " (?)"
                comp_lines.append(f"  {i}. {_escape_html(name)} [{threat}]{ver_mark}")

            comp_text = "\n".join(comp_lines) if comp_lines else "  (не найдены)"

            _update_progress(
                chat_id,
                progress_msg_id,
                f"Найдено {len(competitors)} конкурентов"
                + (f" ({_escape_html(market_name)})" if market_name else "")
                + f":\n{comp_text}\n\n"
                "Подтверждаю автоматически, запускаю глубокий анализ...",
            )

            _bsr_post(f"/api/confirm-competitors/{sid}", {"competitors": competitors})
            logger.info("Auto-confirmed %d competitors", len(competitors))

        elif status == "done":
            report_data = data.get("report", {})
            report_url_path = report_data.get("url", "")
            company_name = report_data.get("company", "")
            size_kb = report_data.get("size_kb", 0)
            report_status = report_data.get("report_status", "draft")
            blocking_issues = report_data.get("blocking_issues", [])

            # Build full public URL
            report_full_url = f"{BSR_PUBLIC_URL}{report_url_path}"

            _update_progress(
                chat_id,
                progress_msg_id,
                f"{'Отчёт готов' if report_status == 'publishable' else 'Черновик отчёта готов'}! ({size_kb} KB)",
            )

            # Send the report link as a separate message
            send_message(
                chat_id,
                f"<b>{'Отчёт готов' if report_status == 'publishable' else 'Черновик отчёта'}</b>"
                + (f": {_escape_html(company_name)}" if company_name else "")
                + f"\n\n{report_full_url}\n\n"
                f"Размер: {size_kb} KB\n"
                + (
                    "Статус: publishable\n"
                    if report_status == "publishable"
                    else "Статус: draft\n"
                )
                + (
                    "Блокеры:\n- " + "\n- ".join(_escape_html(x) for x in blocking_issues[:3]) + "\n\n"
                    if blocking_issues else ""
                )
                + "Отправь ещё одну ссылку для нового анализа.",
                disable_web_page_preview=True,
            )
            logger.info("Report ready: %s for session %s", report_full_url, sid)
            return

        elif status == "error":
            error_msg = data.get("error", "Неизвестная ошибка")
            _update_progress(
                chat_id,
                progress_msg_id,
                f"Ошибка при анализе:\n{_escape_html(error_msg)}\n\n"
                "Попробуй отправить ссылку ещё раз.",
            )
            logger.error("Analysis error for session %s: %s", sid, error_msg)
            return

        # Delay before next poll
        time.sleep(POLL_INTERVAL)

    # Timeout
    _update_progress(
        chat_id,
        progress_msg_id,
        "Анализ занимает слишком долго (превышен таймаут 10 мин).\n"
        "Попробуй позже или обратись к администратору.",
    )
    logger.warning("Analysis timeout for session %s", sid)


def _update_progress(chat_id: int, message_id: int | None, text: str) -> None:
    """Update the progress message or send a new one."""
    if message_id:
        result = edit_message(chat_id, message_id, text)
        # If edit failed (message too old, deleted, etc.), send new
        if not result.get("ok"):
            send_message(chat_id, text)
    else:
        send_message(chat_id, text)


def _escape_html(text: str) -> str:
    """Escape HTML special chars for Telegram HTML parse mode."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ── Message routing ───────────────────────────────────────────────────────────

# Track active sessions per chat to prevent concurrent analyses
_active_chats: dict[int, str] = {}  # chat_id -> session_id


def handle_message(update: dict) -> None:
    """Route an incoming message to the appropriate handler."""
    message = update.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()
    user_id = message.get("from", {}).get("id")
    user_name = message.get("from", {}).get("first_name", "")

    if not chat_id or not text:
        return

    # Access control
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        send_message(chat_id, "Доступ ограничен. Обратитесь к администратору.")
        return

    # Commands
    if text.startswith("/"):
        cmd = text.split()[0].lower().split("@")[0]  # handle /start@botname
        if cmd == "/start":
            handle_start(chat_id, user_name)
        elif cmd == "/help":
            handle_help(chat_id)
        elif cmd == "/status":
            handle_status(chat_id)
        else:
            send_message(chat_id, "Неизвестная команда. Используй /help для справки.")
        return

    # Check if it looks like a URL
    url = _normalize_url(text)
    if url:
        # Check for concurrent analysis
        if chat_id in _active_chats:
            send_message(
                chat_id,
                "Предыдущий анализ ещё выполняется. Дождись завершения или подожди несколько минут.",
            )
            return

        _active_chats[chat_id] = "running"
        try:
            handle_url(chat_id, url)
        finally:
            _active_chats.pop(chat_id, None)
    else:
        send_message(
            chat_id,
            "Это не похоже на URL. Отправь ссылку на сайт компании, например:\n"
            "<code>https://example.com</code>\n\n"
            "Или домен: <code>example.com</code>",
        )


def handle_callback(update: dict) -> None:
    """Handle inline keyboard callback queries.

    Currently unused since we auto-confirm, but ready for future interactive mode.
    """
    callback = update.get("callback_query", {})
    callback_id = callback.get("id", "")
    data = callback.get("data", "")
    # chat_id = callback.get("message", {}).get("chat", {}).get("id")
    # message_id = callback.get("message", {}).get("message_id")

    if data.startswith("confirm:"):
        answer_callback(callback_id, "Подтверждено")
    elif data.startswith("cancel:"):
        answer_callback(callback_id, "Отменено")
    else:
        answer_callback(callback_id)


# ── Long polling loop ─────────────────────────────────────────────────────────


def poll_updates() -> None:
    """Main long-polling loop. Blocks forever."""
    offset = 0
    logger.info(
        "Telegram bot started. Polling for updates... "
        "(BSR API: %s, Public URL: %s, Allowed users: %s)",
        BSR_API_URL,
        BSR_PUBLIC_URL,
        ALLOWED_USERS or "all",
    )

    while True:
        try:
            params = urllib.parse.urlencode({
                "offset": offset,
                "timeout": 30,
                "allowed_updates": json.dumps(["message", "callback_query"]),
            })
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?{params}"
            req = urllib.request.Request(url)

            with urllib.request.urlopen(req, timeout=35) as resp:
                data = json.loads(resp.read())

            if not data.get("ok"):
                logger.error("getUpdates failed: %s", data.get("description"))
                time.sleep(5)
                continue

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                try:
                    if "callback_query" in update:
                        handle_callback(update)
                    elif "message" in update:
                        handle_message(update)
                except Exception:
                    logger.exception("Error handling update %s", update.get("update_id"))

        except urllib.error.URLError as e:
            logger.error("Polling network error: %s", e)
            time.sleep(5)
        except Exception:
            logger.exception("Unexpected polling error")
            time.sleep(5)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    if not BOT_TOKEN:
        print("ERROR: Set TELEGRAM_BOT_TOKEN environment variable")
        print("  export TELEGRAM_BOT_TOKEN=123456:ABC-DEF...")
        raise SystemExit(1)

    print(f"BSR Telegram Bot starting...")
    print(f"  BSR API: {BSR_API_URL}")
    print(f"  Public URL: {BSR_PUBLIC_URL}")
    print(f"  Allowed users: {ALLOWED_USERS or 'all'}")
    print()

    poll_updates()
