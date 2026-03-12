"""FastAPI app: interactive multi-step business analysis pipeline."""

from __future__ import annotations

import logging
import os
import time
import uuid
import json
import threading
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.auth import AuthManager
from app.config import REPORTS_DIR, BusinessType
from app.landing import LANDING_HTML
from app.metrics import MetricsCollector, get_aggregate_stats
from app.security import (
    check_rate_limit_request,
    check_rate_limit_report,
    validate_url,
    sanitize_text,
    sanitize_dict,
    sanitize_error,
    get_client_ip,
)
from app.admin import router as admin_router
from app.session_store import get_store

load_dotenv()

logger = logging.getLogger("bsr.app")

IS_PRODUCTION = os.getenv("BSR_ENV", "production").lower() == "production"

app = FastAPI(
    title="Бизнес-анализ 360",
    version="0.3.0",
    # Don't expose docs in production
    docs_url=None if IS_PRODUCTION else "/docs",
    redoc_url=None if IS_PRODUCTION else "/redoc",
)

# ── CORS ──
# Allow only same-origin; in production the app serves its own frontend
_allowed_origins = os.getenv("BSR_CORS_ORIGINS", "").split(",") if os.getenv("BSR_CORS_ORIGINS") else []
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,  # empty = no cross-origin allowed (same-origin only)
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

# ── Auth ──
auth_manager = AuthManager()


# ── Rate Limiting Middleware ──
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Global per-IP rate limit: 30 requests/minute."""
    # Skip static files
    if request.url.path.startswith("/reports/"):
        return await call_next(request)

    client_ip = get_client_ip(request)
    error = check_rate_limit_request(client_ip)
    if error:
        return JSONResponse(
            {"ok": False, "error": error},
            status_code=429,
            headers={"Retry-After": "60"},
        )
    return await call_next(request)


# ── Error Handling Middleware ──
@app.middleware("http")
async def error_sanitization_middleware(request: Request, call_next):
    """Catch unhandled exceptions, sanitize before returning to client."""
    try:
        return await call_next(request)
    except Exception as exc:
        logger.exception("Unhandled error on %s %s", request.method, request.url.path)
        safe_message = sanitize_error(exc, include_details=not IS_PRODUCTION)
        return JSONResponse(
            {"ok": False, "error": safe_message},
            status_code=500,
        )

REPORTS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR)), name="reports")

# ── Admin dashboard ──
app.include_router(admin_router)

# ── Session storage (T1: pluggable backend via STORE_BACKEND env var) ──

store = get_store()

# Background cleanup thread for expired sessions
_cleanup_stop = threading.Event()
_cleanup_thread: threading.Thread | None = None
_CLEANUP_INTERVAL = 300  # 5 minutes


@app.on_event("startup")
async def _startup():
    global _cleanup_thread

    def _cleanup_loop():
        while not _cleanup_stop.wait(_CLEANUP_INTERVAL):
            store.cleanup_expired()

    _cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True, name="session-cleanup")
    _cleanup_thread.start()
    logger.info("Session cleanup thread started (interval=%ds)", _CLEANUP_INTERVAL)


@app.on_event("shutdown")
async def _shutdown():
    _cleanup_stop.set()
    if _cleanup_thread and _cleanup_thread.is_alive():
        _cleanup_thread.join(timeout=5)


def _new_session() -> str:
    sid = uuid.uuid4().hex[:12]
    store.set(sid, {"status": "created", "events": [], "data": {}, "created_at": time.time()})
    return sid


def _push_event(sid: str, event: str, data: Any = None):
    session = store.get(sid)
    if session is not None:
        session["events"].append({"event": event, "data": data})


# ── Auth helpers ──

COOKIE_NAME = "bsr_token"
COOKIE_MAX_AGE = 30 * 24 * 3600  # 30 days


def _get_auth_token(request: Request) -> str | None:
    """Extract auth token from cookie."""
    return request.cookies.get(COOKIE_NAME)


def _set_auth_cookie(response: JSONResponse, token: str) -> JSONResponse:
    """Set auth cookie on response."""
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=IS_PRODUCTION,
    )
    return response


# ── Routes ──

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(content=LANDING_HTML)


# ── Auth endpoints ──

@app.post("/api/auth/register")
async def auth_register(request: Request):
    """Register a new user. Sets auth cookie on success."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Некорректный запрос"}, status_code=400)

    email = sanitize_text(str(body.get("email", "")).strip(), max_length=254)
    password = str(body.get("password", ""))

    # Don't sanitize password (it may contain special chars), just limit length
    if len(password) > 128:
        return JSONResponse({"ok": False, "error": "Пароль слишком длинный"}, status_code=400)

    try:
        result = auth_manager.register(email, password)
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("Registration error")
        return JSONResponse(
            {"ok": False, "error": sanitize_error(e, include_details=not IS_PRODUCTION)},
            status_code=500,
        )

    resp = JSONResponse({
        "ok": True,
        "email": result["email"],
        "reports_used": result["reports_used"],
        "reports_remaining": result["reports_remaining"],
    })
    return _set_auth_cookie(resp, result["token"])


@app.post("/api/auth/login")
async def auth_login(request: Request):
    """Login. Sets auth cookie on success."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Некорректный запрос"}, status_code=400)

    email = str(body.get("email", "")).strip()
    password = str(body.get("password", ""))

    result = auth_manager.login(email, password)
    if not result:
        return JSONResponse({"ok": False, "error": "Неверный email или пароль"}, status_code=401)

    resp = JSONResponse({
        "ok": True,
        "email": result["email"],
        "reports_used": result["reports_used"],
        "reports_remaining": result["reports_remaining"],
    })
    return _set_auth_cookie(resp, result["token"])


@app.get("/api/auth/me")
async def auth_me(request: Request):
    """Get current user info from cookie."""
    token = _get_auth_token(request)
    if not token:
        return JSONResponse({"ok": False, "authenticated": False})

    user = auth_manager.check_token(token)
    if not user:
        return JSONResponse({"ok": False, "authenticated": False})

    return {
        "ok": True,
        "authenticated": True,
        "email": user["email"],
        "reports_used": user["reports_used"],
        "reports_remaining": user["reports_remaining"],
    }


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    """Logout: revoke token and clear cookie."""
    token = _get_auth_token(request)
    if token:
        auth_manager.logout(token)

    resp = JSONResponse({"ok": True})
    resp.delete_cookie(key=COOKIE_NAME)
    return resp


@app.post("/api/start")
async def start_session(request: Request):
    """Start a new analysis session. Returns session_id."""
    # Rate limit: max 5 reports per hour per IP
    client_ip = get_client_ip(request)
    report_error = check_rate_limit_report(client_ip)
    if report_error:
        return JSONResponse(
            {"ok": False, "error": report_error},
            status_code=429,
            headers={"Retry-After": "3600"},
        )

    # Auth: check freemium quota (if logged in)
    auth_token = _get_auth_token(request)
    can_gen, reason = auth_manager.can_generate_report(auth_token)
    if not can_gen:
        return JSONResponse(
            {"ok": False, "error": reason, "quota_exceeded": True},
            status_code=403,
        )

    body = await request.json()
    raw_url = (body.get("url") or "").strip()

    # Validate & sanitize URL
    is_valid, url, url_error = validate_url(raw_url)
    if not is_valid:
        return JSONResponse({"ok": False, "error": url_error}, status_code=400)

    sid = _new_session()
    session = store.get(sid)
    session["data"]["url"] = url
    session["data"]["_auth_token"] = auth_token  # track for quota
    session["status"] = "scraping"

    # Run steps 1-3 in background thread
    thread = threading.Thread(target=_run_initial_steps, args=(sid, url), daemon=True)
    thread.start()

    return {"ok": True, "session_id": sid}


@app.get("/api/events/{sid}")
async def stream_events(sid: str):
    """SSE endpoint: stream events to frontend."""
    sid = sanitize_text(sid, max_length=20)
    if not store.exists(sid):
        return JSONResponse({"error": "Session not found"}, status_code=404)

    import asyncio

    async def event_generator():
        last_idx = 0
        while True:
            session = store.get(sid)
            if session is None:
                break
            events = session.get("events", [])
            while last_idx < len(events):
                ev = events[last_idx]
                yield f"event: {ev['event']}\ndata: {json.dumps(ev.get('data', {}), ensure_ascii=False)}\n\n"
                last_idx += 1
                # If terminal event, stop
                if ev["event"] in ("done", "error", "waiting_company", "waiting_competitors"):
                    pass  # keep connection open for further events
            status = session.get("status", "")
            if status in ("done", "error"):
                # Final check for any remaining events
                events = session.get("events", [])
                while last_idx < len(events):
                    ev = events[last_idx]
                    yield f"event: {ev['event']}\ndata: {json.dumps(ev.get('data', {}), ensure_ascii=False)}\n\n"
                    last_idx += 1
                break
            await asyncio.sleep(0.3)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/confirm-company/{sid}")
async def confirm_company(sid: str, request: Request):
    """User confirms/edits company identification."""
    sid = sanitize_text(sid, max_length=20)
    if not store.exists(sid):
        return JSONResponse({"error": "Session not found"}, status_code=404)

    body = await request.json()
    # Sanitize all user-editable text fields
    body = sanitize_dict(body, ("name", "legal_name", "address"), max_length=300)
    # Validate INN: only digits, 10 or 12 chars
    inn = (body.get("inn") or "").strip()
    if inn and (not inn.isdigit() or len(inn) not in (10, 12)):
        return JSONResponse({"ok": False, "error": "ИНН должен содержать 10 или 12 цифр"}, status_code=400)
    body["inn"] = inn
    # Validate business_type_guess against allowed values
    valid_types = {e.value for e in BusinessType}
    bt = body.get("business_type_guess", "")
    if bt and bt not in valid_types:
        body["business_type_guess"] = "B2B_SERVICE"

    session = store.get(sid)
    session["data"]["confirmed_company"] = body
    session["status"] = "finding_competitors"

    # Continue pipeline
    thread = threading.Thread(target=_run_competitor_steps, args=(sid,), daemon=True)
    thread.start()

    return {"ok": True}


@app.post("/api/confirm-competitors/{sid}")
async def confirm_competitors(sid: str, request: Request):
    """User confirms/edits competitor list."""
    sid = sanitize_text(sid, max_length=20)
    if not store.exists(sid):
        return JSONResponse({"error": "Session not found"}, status_code=404)

    body = await request.json()
    competitors = body.get("competitors", [])
    # Sanitize competitor text fields (names, descriptions)
    for comp in competitors:
        if isinstance(comp, dict):
            for field in ("name", "description", "why_competitor", "verification_notes"):
                if field in comp and isinstance(comp[field], str):
                    comp[field] = sanitize_text(comp[field], max_length=500)

    session = store.get(sid)
    session["data"]["confirmed_competitors"] = competitors
    session["status"] = "analyzing"

    # Continue pipeline
    thread = threading.Thread(target=_run_analysis_steps, args=(sid,), daemon=True)
    thread.start()

    return {"ok": True}


# ── Background pipeline steps ──

def _run_initial_steps(sid: str, url: str):
    """Steps 1-3: Scrape -> Identify -> FNS lookup."""
    session = store.get(sid)
    if session is None:
        return

    # T7: Initialize metrics collector and bind to this thread
    mc = MetricsCollector(session_id=sid)
    session["_metrics"] = mc
    from app.pipeline.llm_client import set_metrics_collector, refresh_models
    set_metrics_collector(mc)

    try:
        # Step 0: Auto-select best available models from all providers
        _push_event(sid, "step", {"num": 0, "status": "active", "text": "Выбираю лучшие AI-модели..."})
        try:
            probe_results = refresh_models()
            from app.pipeline.llm_client import MODEL_MAIN, MODEL_OPUS, MODEL_FAST
            models_text = f"AI: {MODEL_MAIN} + {MODEL_OPUS} + {MODEL_FAST}"
            _push_event(sid, "step", {"num": 0, "status": "done", "text": models_text})
            session["data"]["selected_models"] = probe_results
        except Exception as e:
            logger.warning("Model probe failed, using defaults: %s", str(e)[:200])
            _push_event(sid, "step", {"num": 0, "status": "warning", "text": "AI-модели: defaults"})

        # Step 1: Scrape
        _push_event(sid, "step", {"num": 1, "status": "active", "text": "Загрузка и скрапинг сайта..."})
        mc.start_timer("step1_scrape")
        from app.pipeline.steps.step1_scrape import run as scrape
        scraped = scrape(url)
        mc.stop_timer("step1_scrape")
        session["data"]["scraped"] = scraped
        scrape_method = scraped.get("scrape_method", "requests")
        if scrape_method == "scrapling":
            method_hint = " (Scrapling fallback)"
        elif scrape_method == "minimal":
            method_hint = " (minimal fallback)"
        else:
            method_hint = ""
        _push_event(sid, "step", {"num": 1, "status": "done", "text": f"Сайт загружен{method_hint}: {scraped.get('title', '')}"})

        # Step 2: Identify company
        _push_event(sid, "step", {"num": 2, "status": "active", "text": "Определяю компанию..."})
        mc.start_timer("step2_identify")
        from app.pipeline.steps.step2_identify import run as identify
        company_info = identify(scraped)
        mc.stop_timer("step2_identify")
        session["data"]["company_info"] = company_info
        _push_event(sid, "step", {"num": 2, "status": "done", "text": f"Компания: {company_info.get('name', '?')}"})

        # Step 3: FNS lookup
        _push_event(sid, "step", {"num": 3, "status": "active", "text": "Поиск в ФНС..."})
        mc.start_timer("step3_fns")
        fns_ok = False
        try:
            from app.pipeline.steps.step3_fns import run as fns_lookup
            fns_data = fns_lookup(company_info)
            session["data"]["fns_data"] = fns_data
            fns_ok = bool(fns_data.get("fns_company", {}).get("inn"))
        except Exception as e:
            session["data"]["fns_data"] = {"fns_error": str(e)}
        mc.stop_timer("step3_fns")

        if fns_ok:
            fc = fns_data["fns_company"]
            _push_event(sid, "step", {"num": 3, "status": "done",
                "text": f"ФНС: {fc.get('name', '')} | ИНН {fc.get('inn', '')}"})
        else:
            _push_event(sid, "step", {"num": 3, "status": "warning",
                "text": "ФНС: юрлицо не найдено автоматически"})

        # PAUSE: send data to frontend for user verification
        session["status"] = "waiting_company"
        _push_event(sid, "waiting_company", {
            "company_info": company_info,
            "fns_data": session["data"].get("fns_data", {}),
        })
        store.save(sid)  # persist checkpoint

    except Exception as e:
        logger.exception("Error in initial steps for session %s", sid)
        session = store.get(sid)
        if session:
            session["status"] = "error"
        _push_event(sid, "error", {
            "message": sanitize_error(e, include_details=not IS_PRODUCTION),
        })
        store.save(sid)


def _run_competitor_steps(sid: str):
    """Step 4: Find competitors."""
    session = store.get(sid)
    if session is None:
        return

    # T7: Restore metrics collector for this thread
    mc = session.get("_metrics")
    if mc:
        from app.pipeline.llm_client import set_metrics_collector
        set_metrics_collector(mc)

    try:
        data = session["data"]
        confirmed = data.get("confirmed_company", {})

        # Update company name in metrics if available
        if mc and confirmed.get("name"):
            mc.company = confirmed["name"]

        # If user provided INN, re-fetch FNS
        if confirmed.get("inn") and confirmed["inn"] != data.get("fns_data", {}).get("fns_company", {}).get("inn"):
            _push_event(sid, "step", {"num": 3, "status": "active", "text": f"Обновляю данные ФНС по ИНН {confirmed['inn']}..."})
            if mc:
                mc.start_timer("step3_fns_refetch")
            try:
                from app.pipeline.steps.step3_fns import run as fns_lookup
                fns_data = fns_lookup(data.get("company_info", {}), confirmed_inn=confirmed["inn"])
                data["fns_data"] = fns_data
                _push_event(sid, "step", {"num": 3, "status": "done", "text": "Данные ФНС обновлены"})
            except Exception:
                pass
            if mc:
                mc.stop_timer("step3_fns_refetch")

        # Merge confirmed data into company_info
        company_info = data.get("company_info", {})
        for key in ("name", "legal_name", "inn", "address", "business_type_guess"):
            if confirmed.get(key):
                company_info[key] = confirmed[key]
        data["company_info"] = company_info

        # Step 4: Find competitors + verify via web search
        _push_event(sid, "step", {"num": 4, "status": "active", "text": "Ищу конкурентов (GPT-5.2 Pro)..."})
        if mc:
            mc.start_timer("step4_competitors")
        from app.pipeline.steps.step4_competitors import run as find_competitors
        comp_result = find_competitors(
            data.get("scraped", {}),
            company_info,
            data.get("fns_data", {}),
        )
        if mc:
            mc.stop_timer("step4_competitors")
        data["market_info"] = comp_result

        # Build verification summary for the step status
        comps = comp_result.get("competitors", [])
        verified_count = sum(1 for c in comps if c.get("verified"))
        total_count = len(comps)
        if total_count > 0 and verified_count < total_count:
            step4_text = (
                f"Найдено {total_count} конкурентов "
                f"({verified_count} подтверждены, "
                f"{total_count - verified_count} не подтверждены)"
            )
        else:
            step4_text = f"Найдено {total_count} конкурентов"
            if verified_count == total_count and total_count > 0:
                step4_text += " (все подтверждены)"
        _push_event(sid, "step", {"num": 4, "status": "done", "text": step4_text})

        # PAUSE: send competitors for user editing
        session["status"] = "waiting_competitors"
        _push_event(sid, "waiting_competitors", {
            "market_name": comp_result.get("market_name", ""),
            "competitors": comp_result.get("competitors", []),
        })
        store.save(sid)  # persist checkpoint

    except Exception as e:
        logger.exception("Error in competitor steps for session %s", sid)
        session = store.get(sid)
        if session:
            session["status"] = "error"
        _push_event(sid, "error", {
            "message": sanitize_error(e, include_details=not IS_PRODUCTION),
        })
        store.save(sid)


def _generate_factcheck_items(report_data: dict, fns_data: dict, company_info: dict) -> dict:
    """Generate basic factcheck items from FNS and other verified data sources."""
    factcheck = report_data.get("factcheck", [])

    # FNS financial facts
    egrul = fns_data.get("egrul", {})
    financials = fns_data.get("financials", [])

    if egrul.get("inn"):
        factcheck.append({
            "fact": f"ИНН: {egrul['inn']}",
            "sources_count": 2,
            "verified": True,
            "sources": ["ФНС", "ЕГРЮЛ"],
        })
    if egrul.get("full_name"):
        factcheck.append({
            "fact": f"Юридическое лицо: {egrul['full_name']}",
            "sources_count": 2,
            "verified": True,
            "sources": ["ФНС", "ЕГРЮЛ"],
        })
    if egrul.get("okved"):
        factcheck.append({
            "fact": f"Основной ОКВЭД: {egrul['okved']}",
            "sources_count": 1,
            "verified": True,
            "sources": ["ЕГРЮЛ"],
        })

    if financials:
        latest = financials[-1]
        year = latest.get("year", "?")
        try:
            rev = latest.get("revenue")
            if rev is not None:
                rev = float(rev)
                factcheck.append({
                    "fact": f"Выручка {year}: {rev:,.0f} тыс. ₽",
                    "sources_count": 1,
                    "verified": True,
                    "sources": ["ФНС (бухотчётность)"],
                })
            profit = latest.get("net_profit")
            if profit is not None:
                profit = float(profit)
                factcheck.append({
                    "fact": f"Чистая прибыль {year}: {profit:,.0f} тыс. ₽",
                    "sources_count": 1,
                    "verified": True,
                    "sources": ["ФНС (бухотчётность)"],
                })
            emp = latest.get("employees")
            if emp:
                factcheck.append({
                    "fact": f"Сотрудников {year}: {emp}",
                    "sources_count": 1,
                    "verified": True,
                    "sources": ["ФНС"],
                })
        except (TypeError, ValueError):
            pass  # skip malformed financial data

    # Company website
    company = report_data.get("company", {})
    if company.get("website"):
        factcheck.append({
            "fact": f"Сайт компании: {company['website']}",
            "sources_count": 1,
            "verified": True,
            "sources": ["Прямая проверка"],
        })

    report_data["factcheck"] = factcheck
    return report_data


def _generate_digital_verification(report_data: dict, company_info: dict, competitors: list) -> dict:
    """Generate digital_verification table from digital data and competitors."""
    digital = report_data.get("digital") or {}
    dv_items = []

    # Company's own digital
    company_name = company_info.get("name", report_data.get("company", {}).get("name", "Компания"))
    social = digital.get("social_accounts", [])

    company_item = {
        "company": company_name,
        "is_target": True,
        "instagram": "—",
        "telegram": "—",
        "vk": "—",
        "total_followers": 0,
        "avg_er": 0,
    }
    total = 0
    er_values = []
    for acc in (social if isinstance(social, list) else []):
        if not isinstance(acc, dict):
            continue
        platform = (acc.get("platform") or "").lower()
        handle = acc.get("handle", "—")
        followers = acc.get("followers") or 0
        er = acc.get("engagement_rate")
        er_str = f", ER {er:.1f}%" if er else ""
        if "instagram" in platform:
            company_item["instagram"] = f"{handle} ({followers:,}{er_str})" if followers else handle
        elif "telegram" in platform:
            company_item["telegram"] = f"{handle} ({followers:,}{er_str})" if followers else handle
        elif "vk" in platform or "вк" in platform:
            company_item["vk"] = f"{handle} ({followers:,}{er_str})" if followers else handle
        total += followers if isinstance(followers, (int, float)) else 0
        if er and isinstance(er, (int, float)):
            er_values.append(er)
    company_item["total_followers"] = f"{int(total):,}" if total else "⚠ нет данных"
    company_item["avg_er"] = round(sum(er_values) / len(er_values), 1) if er_values else 0
    dv_items.append(company_item)

    # Competitors
    for comp in (competitors or []):
        if isinstance(comp, dict):
            comp_name = comp.get("name", "?")
            dv_items.append({
                "company": comp_name,
                "is_target": False,
                "instagram": "⚠ не проверено",
                "telegram": "⚠ не проверено",
                "vk": "⚠ не проверено",
                "total_followers": "⚠ оценка",
                "avg_er": 0,
            })

    if dv_items:
        report_data["digital_verification"] = dv_items
    return report_data


def _run_analysis_steps(sid: str):
    """Steps 1b, 1c, 5, 2a, 2b, 6: Extended v2.0 pipeline."""
    session = store.get(sid)
    if session is None:
        return

    # T7: Restore metrics collector for this thread
    mc = session.get("_metrics")
    if mc:
        from app.pipeline.llm_client import set_metrics_collector
        set_metrics_collector(mc)

    try:
        data = session["data"]
        confirmed_competitors = data.get("confirmed_competitors", [])
        company_info = data.get("company_info", {})
        bt = company_info.get("business_type_guess", "")

        # Step 1b: Marketplace analysis (conditional)
        marketplace_data = None
        if bt in ("B2C_PRODUCT", "PLATFORM", "B2B_B2C_HYBRID"):
            _push_event(sid, "step", {"num": "1b", "status": "active", "text": "Анализ маркетплейсов..."})
            if mc:
                mc.start_timer("step1b_marketplace")
            try:
                from app.pipeline.steps.step1b_marketplace import run as marketplace_analysis
                marketplace_data = marketplace_analysis(
                    company_info=company_info,
                    scraped=data.get("scraped", {}),
                    competitors=confirmed_competitors,
                )
                _push_event(sid, "step", {"num": "1b", "status": "done", "text": "Маркетплейсы проанализированы"})
            except Exception as e:
                _push_event(sid, "step", {"num": "1b", "status": "warning", "text": f"Маркетплейсы: {e}"})
            if mc:
                mc.stop_timer("step1b_marketplace")

        # Step 1c: Deep models (lifecycle + channels)
        deep_models = None
        _push_event(sid, "step", {"num": "1c", "status": "active", "text": "Жизненный цикл и каналы продаж..."})
        if mc:
            mc.start_timer("step1c_deep_models")
        try:
            from app.pipeline.steps.step1c_deep_models import run as deep_models_analysis
            deep_models = deep_models_analysis(
                company_info=company_info,
                competitors=confirmed_competitors,
                fns_data=data.get("fns_data", {}),
                market_info=data.get("market_info", {}),
            )
            lc_count = len(deep_models.get("lifecycles", {}))
            ch_count = len(deep_models.get("channels", {}))
            _push_event(sid, "step", {"num": "1c", "status": "done",
                "text": f"Lifecycle: {lc_count} компаний, каналы: {ch_count}"})
        except Exception as e:
            _push_event(sid, "step", {"num": "1c", "status": "warning", "text": f"Deep models: {e}"})
        if mc:
            mc.stop_timer("step1c_deep_models")

        # Step 4.5: HH.ru API (real HR data)
        hh_data = None
        company_name = company_info.get("name", "")
        if company_name:
            _push_event(sid, "step", {"num": "4h", "status": "active", "text": "HH.ru API — вакансии и зарплаты..."})
            try:
                from app.pipeline.sources.hh_api import get_hr_data_for_company
                hh_data = get_hr_data_for_company(
                    company_name=company_name,
                    industry_keywords=bt,
                )
                vcount = (hh_data or {}).get("vacancies_count", 0)
                _push_event(sid, "step", {"num": "4h", "status": "done",
                    "text": f"HH.ru: {vcount} вакансий найдено"})
            except Exception as e:
                _push_event(sid, "step", {"num": "4h", "status": "warning", "text": f"HH.ru: {e}"})

        # Step 5: Deep analysis with GPT-5.2 Pro (7 секций параллельно)
        _push_event(sid, "step", {"num": 5, "status": "active", "text": "Глубокий анализ (7 секций параллельно)..."})

        # Progress callback: транслирует статусы секций в SSE-события
        def _step5_progress(section_name: str, status: str):
            status_map = {"started": "active", "done": "done", "error": "warning"}
            sse_status = status_map.get(status, "active")
            _push_event(sid, "step", {
                "num": "5",
                "status": sse_status,
                "text": f"{section_name}: {status}",
                "sub_section": section_name,
            })

        if mc:
            mc.start_timer("step5_deep_analysis")
        from app.pipeline.steps.step5_deep_analysis import run as deep_analysis
        report_data = deep_analysis(
            scraped=data.get("scraped", {}),
            company_info=company_info,
            fns_data=data.get("fns_data", {}),
            competitors=confirmed_competitors,
            market_info=data.get("market_info", {}),
            deep_models=deep_models,
            marketplace_data=marketplace_data,
            progress_callback=_step5_progress,
            hh_data=hh_data,
        )
        if mc:
            mc.stop_timer("step5_deep_analysis")
        _push_event(sid, "step", {"num": 5, "status": "done", "text": "Анализ завершён"})

        # Sanitize LLM output first
        report_data = _sanitize_llm_output(report_data)

        # Step 2a: Verification (pure Python)
        _push_event(sid, "step", {"num": "2a", "status": "active", "text": "Верификация расчётов..."})
        if mc:
            mc.start_timer("step2a_verify")
        try:
            from app.pipeline.steps.step2a_verify import run as verify
            report_data = verify(report_data)
            corrections = sum(1 for f in report_data.get("factcheck", [])
                            if isinstance(f, dict) and f.get("correction"))
            _push_event(sid, "step", {"num": "2a", "status": "done",
                "text": f"Верификация: {corrections} корректировок"})
        except Exception as e:
            _push_event(sid, "step", {"num": "2a", "status": "warning", "text": f"Верификация: {e}"})
        if mc:
            mc.stop_timer("step2a_verify")

        # Step 2a+: Generate basic factcheck & digital_verification from existing data
        report_data = _generate_factcheck_items(report_data, data.get("fns_data", {}), company_info)
        report_data = _generate_digital_verification(report_data, company_info, confirmed_competitors)

        # Step 2b: Relevance gate (pure Python)
        _push_event(sid, "step", {"num": "2b", "status": "active", "text": "Section Relevance Gate..."})
        if mc:
            mc.start_timer("step2b_relevance_gate")
        try:
            from app.pipeline.steps.step2b_relevance_gate import run as relevance_gate
            report_data = relevance_gate(report_data)
            gates = report_data.get("section_gates", {})
            disabled = sum(1 for v in gates.values() if not v)
            _push_event(sid, "step", {"num": "2b", "status": "done",
                "text": f"Gate: {disabled} секций отключено"})
        except Exception as e:
            _push_event(sid, "step", {"num": "2b", "status": "warning", "text": f"Gate: {e}"})
        if mc:
            mc.stop_timer("step2b_relevance_gate")

        # Step 6: Board of Directors review (T25/T26)
        _push_event(sid, "step", {"num": "6a", "status": "active", "text": "Совет директоров — 5 AI-экспертов..."})
        if mc:
            mc.start_timer("step6_board")
        try:
            from app.pipeline.steps.step6_board import form_panel, run_review, apply_revisions
            panel = form_panel(report_data, company_info)
            review_result = run_review(report_data, panel)
            report_data = apply_revisions(report_data, review_result)
            consensus = review_result.get("consensus", {})
            approved = consensus.get("approved", False)
            critiques = consensus.get("total_critiques", 0)
            status_text = "Одобрен" if approved else f"Замечания: {critiques}"
            _push_event(sid, "step", {"num": "6a", "status": "done",
                "text": f"Совет директоров: {status_text}"})
        except Exception as e:
            logger.warning("Board review failed: %s", e)
            _push_event(sid, "step", {"num": "6a", "status": "warning",
                "text": f"Совет директоров: {e}"})
        if mc:
            mc.stop_timer("step6_board")

        # Step Quality: Auto quality check (T10)
        _push_event(sid, "step", {"num": "qa", "status": "active", "text": "Проверка качества отчёта..."})
        if mc:
            mc.start_timer("step_quality")
        try:
            from app.pipeline.steps.step_quality import check_report_quality
            quality_result = check_report_quality(report_data, company_info)
            q_score = quality_result.get("score", 0)
            q_passed = quality_result.get("passed", False)
            q_critical = len(quality_result.get("critical_failures", []))
            q_warnings = len(quality_result.get("warnings", []))

            # Add quality warnings to report_data if critical failures exist
            if q_critical > 0:
                existing_questions = report_data.get("open_questions", [])
                for cf in quality_result["critical_failures"]:
                    existing_questions.append(f"[QA] {cf}")
                report_data["open_questions"] = existing_questions

            status_text = f"Качество: {q_score}/100"
            if q_critical > 0:
                status_text += f" ({q_critical} критических)"
            if q_warnings > 0:
                status_text += f", {q_warnings} предупреждений"

            _push_event(sid, "step", {"num": "qa", "status": "done", "text": status_text})

            # Send quality score via SSE event
            _push_event(sid, "quality", {
                "score": q_score,
                "passed": q_passed,
                "critical_failures": quality_result.get("critical_failures", []),
                "warnings": quality_result.get("warnings", []),
                "checks_count": len(quality_result.get("checks", [])),
            })

            logger.info(
                "Quality check: session=%s, score=%.1f, passed=%s, critical=%d, warnings=%d",
                sid, q_score, q_passed, q_critical, q_warnings,
            )
        except Exception as e:
            logger.warning("Quality check failed: %s", e)
            _push_event(sid, "step", {"num": "qa", "status": "warning",
                "text": f"Проверка качества: {e}"})
        if mc:
            mc.stop_timer("step_quality")

        # Step 7: Build report
        _push_event(sid, "step", {"num": 7, "status": "active", "text": "Сборка отчёта..."})
        if mc:
            mc.start_timer("step7_build_report")

        from app.models import ReportData
        from app.report.builder import save_report

        rd = ReportData(**report_data)
        filename = f"report_{uuid.uuid4().hex[:8]}.html"
        path = save_report(rd, filename=filename)
        size_kb = round(path.stat().st_size / 1024)

        if mc:
            mc.stop_timer("step7_build_report")
        _push_event(sid, "step", {"num": 7, "status": "done", "text": f"Отчёт собран ({size_kb} KB)"})

        # T7: Finalize metrics and log
        if mc:
            mc.company = report_data.get("company", {}).get("name", mc.company)
            metrics_record = mc.finalize()
            logger.info(
                "Report metrics: session=%s, total_time=%.1fs, llm_calls=%d, cost=$%.4f",
                sid,
                metrics_record.get("total_time_sec", 0),
                metrics_record.get("llm_calls", 0),
                metrics_record.get("total_cost_usd", 0),
            )

        # Auth: increment report count for logged-in users
        auth_token = data.get("_auth_token")
        if auth_token:
            auth_manager.increment_report_count(auth_token, report_id=filename)

        # Done!
        session["status"] = "done"
        done_data = {
            "url": f"/reports/{filename}",
            "size_kb": size_kb,
            "company": report_data.get("company", {}).get("name", ""),
        }
        # Include remaining reports if user is authenticated
        if auth_token:
            user_info = auth_manager.check_token(auth_token)
            if user_info:
                done_data["reports_remaining"] = user_info["reports_remaining"]
        _push_event(sid, "done", done_data)
        store.save(sid)  # persist final state

    except Exception as e:
        import traceback
        tb_str = traceback.format_exc()
        logger.exception("Error in analysis steps for session %s", sid)
        # T7: Finalize metrics even on error
        session = store.get(sid)
        mc = session.get("_metrics") if session else None
        if mc and not mc._finalized:
            mc.finalize()
        if session:
            session["status"] = "error"
        _push_event(sid, "error", {
            "message": sanitize_error(e, include_details=not IS_PRODUCTION),
            "traceback": tb_str,  # temporary: for debugging type errors
        })
        store.save(sid)


def _sanitize_llm_output(d: dict) -> dict:
    """Clean up LLM JSON so it passes Pydantic validation."""

    # --- company.business_type ---
    comp = d.get("company") or {}
    bt = comp.get("business_type", "B2B_SERVICE")
    valid_types = {e.value for e in BusinessType}
    if bt not in valid_types:
        bt_map = {
            "B2C": "B2C_SERVICE", "B2B": "B2B_SERVICE",
            "SAAS": "B2B_SERVICE", "ECOMMERCE": "B2C_PRODUCT",
            "RETAIL": "B2C_PRODUCT", "RESTAURANT": "B2C_SERVICE",
            "HYBRID": "B2B_B2C_HYBRID", "B2B_B2C": "B2B_B2C_HYBRID",
        }
        bt = bt_map.get(bt.upper().replace(" ", "_"), "B2B_SERVICE")
    comp["business_type"] = bt
    d["company"] = comp

    # --- digital: ensure numeric fields ---
    digital = d.get("digital") or {}
    if "social_accounts" in digital:
        clean_accounts = []
        for acc in digital["social_accounts"]:
            if not isinstance(acc, dict) or not acc.get("platform"):
                continue
            fol = acc.get("followers")
            if fol is not None:
                try:
                    acc["followers"] = int(float(str(fol).replace(" ", "").replace(",", "")))
                except (ValueError, TypeError):
                    acc["followers"] = 0
            # Sanitize ER fields
            for er_field in ("engagement_rate", "avg_likes", "avg_comments", "avg_views"):
                val = acc.get(er_field)
                if val is not None:
                    try:
                        cleaned = float(str(val).replace(" ", "").replace(",", "").replace("%", ""))
                        acc[er_field] = cleaned if er_field == "engagement_rate" else int(cleaned)
                    except (ValueError, TypeError):
                        acc[er_field] = None
            clean_accounts.append(acc)
        digital["social_accounts"] = clean_accounts
    mt = digital.get("monthly_traffic")
    if mt is not None:
        try:
            digital["monthly_traffic"] = int(float(str(mt).replace(" ", "").replace(",", "")))
        except (ValueError, TypeError):
            digital["monthly_traffic"] = 0
    d["digital"] = digital

    # --- competitors ---
    for c in d.get("competitors") or []:
        if not isinstance(c, dict):
            continue
        if not c.get("name"):
            c["name"] = "Неизвестный"
        # Ensure x, y are numeric
        for coord in ("x", "y"):
            val = c.get(coord)
            if val is not None:
                try:
                    c[coord] = float(val)
                except (ValueError, TypeError):
                    c[coord] = 50.0
        rs = c.get("radar_scores") or {}
        clean_rs: dict[str, float] = {}
        for k, v in rs.items():
            try:
                clean_rs[k] = float(v) if v is not None else 5.0
            except (ValueError, TypeError):
                clean_rs[k] = 5.0
        c["radar_scores"] = clean_rs
        tl = str(c.get("threat_level", "med")).lower()
        c["threat_level"] = tl if tl in ("high", "med", "low") else "med"
        # v2.1: verification fields — ensure defaults
        c.setdefault("verified", True)
        vc = str(c.get("verification_confidence", "unverified")).lower()
        c["verification_confidence"] = vc if vc in ("high", "medium", "low", "unverified") else "unverified"
        if not isinstance(c.get("verification_sources"), list):
            c["verification_sources"] = []

    # --- financials: ensure numeric fields are float/int, not str ---
    clean_fin = []
    for f in (d.get("financials") or []):
        if not isinstance(f, dict) or not f.get("year"):
            continue
        for fld in ("revenue", "net_profit", "assets", "equity", "liabilities"):
            val = f.get(fld)
            if val is not None:
                try:
                    if isinstance(val, str):
                        val = val.replace(" ", "").replace(",", ".").replace("₽", "").replace("тыс", "").replace("руб", "")
                    f[fld] = float(val)
                except (ValueError, TypeError):
                    f[fld] = None
        emp = f.get("employees")
        if emp is not None:
            try:
                f["employees"] = int(float(str(emp).replace(" ", "")))
            except (ValueError, TypeError):
                f["employees"] = None
        clean_fin.append(f)
    d["financials"] = clean_fin

    # --- recommendations ---
    for r in d.get("recommendations") or []:
        if isinstance(r, dict):
            if not r.get("title"):
                r["title"] = "Рекомендация"
            if not r.get("description"):
                r["description"] = r.get("title", "")

    # --- scenarios: ensure metric values are float ---
    for sc in d.get("scenarios") or []:
        if isinstance(sc, dict):
            metrics = sc.get("metrics") or {}
            clean_m: dict[str, float] = {}
            for k, v in metrics.items():
                try:
                    if isinstance(v, str):
                        v = v.replace(" ", "").replace(",", ".").replace("%", "")
                    clean_m[k] = float(v) if v is not None else 0.0
                except (ValueError, TypeError):
                    clean_m[k] = 0.0
            sc["metrics"] = clean_m

    # --- market_share: ensure float values ---
    ms = d.get("market_share") or {}
    clean_ms: dict[str, float] = {}
    for k, v in ms.items():
        try:
            if isinstance(v, str):
                v = v.replace(" ", "").replace(",", ".").replace("%", "")
            clean_ms[k] = float(v) if v is not None else 0.0
        except (ValueError, TypeError):
            clean_ms[k] = 0.0
    d["market_share"] = clean_ms

    # --- opinions ---
    d["opinions"] = [o for o in (d.get("opinions") or []) if isinstance(o, dict) and o.get("author") and o.get("quote")]

    # --- founders ---
    d["founders"] = [f for f in (d.get("founders") or []) if isinstance(f, dict) and f.get("name")]

    # --- kpi_benchmarks: ensure current/benchmark are float or None ---
    clean_kpis = []
    for k in (d.get("kpi_benchmarks") or []):
        if not isinstance(k, dict) or not k.get("name"):
            continue
        for field in ("current", "benchmark"):
            val = k.get(field)
            if val is not None:
                try:
                    # Remove spaces, currency symbols, percent signs
                    if isinstance(val, str):
                        val = val.replace(" ", "").replace(",", ".").replace("%", "").replace("₽", "").replace("тыс", "").replace("руб", "")
                    k[field] = float(val)
                except (ValueError, TypeError):
                    k[field] = None
        clean_kpis.append(k)
    d["kpi_benchmarks"] = clean_kpis

    # --- v2.0: lifecycle in competitors ---
    valid_stages = {"startup", "growth", "investment", "mature"}
    for c in d.get("competitors") or []:
        if not isinstance(c, dict):
            continue
        lc = c.get("lifecycle")
        if isinstance(lc, dict):
            stage = str(lc.get("stage", "mature")).lower()
            if stage not in valid_stages:
                stage = "mature"
            lc["stage"] = stage
            if not isinstance(lc.get("evidence"), list):
                lc["evidence"] = []
        # Sanitize sales_channels
        channels = c.get("sales_channels")
        if isinstance(channels, list):
            c["sales_channels"] = [
                ch for ch in channels
                if isinstance(ch, dict) and ch.get("channel_name")
            ]

    # --- v2.0: calc_traces ---
    valid_confidence = {"FACT", "CALC", "ESTIMATE"}
    sanitized_traces = []
    for ct in d.get("calc_traces") or []:
        if isinstance(ct, dict) and ct.get("metric_name"):
            conf = str(ct.get("confidence", "ESTIMATE")).upper()
            if conf not in valid_confidence:
                conf = "ESTIMATE"
            ct["confidence"] = conf
            sanitized_traces.append(ct)
    d["calc_traces"] = sanitized_traces

    # --- v2.0: methodology ---
    if not isinstance(d.get("methodology"), dict):
        d["methodology"] = {}

    return d


# ── Metrics endpoint (T7) ──

@app.get("/api/stats")
async def pipeline_stats():
    """Return aggregate pipeline metrics: total reports, avg time, cost, LLM usage."""
    try:
        stats = get_aggregate_stats()
        return {"ok": True, **stats}
    except Exception as e:
        logger.exception("Error computing stats")
        return JSONResponse(
            {"ok": False, "error": sanitize_error(e, include_details=not IS_PRODUCTION)},
            status_code=500,
        )


# ── Session status endpoint (T22: for Telegram bot polling) ──

@app.get("/api/session/{sid}")
async def session_status(sid: str):
    """Return current session status + accumulated events.

    Used by the Telegram bot to poll session state without SSE.
    Returns:
        status: created | scraping | waiting_company | finding_competitors |
                waiting_competitors | analyzing | done | error
        events: all accumulated events
        data: relevant session data (company_info, fns_data, competitors, report_url)
    """
    sid = sanitize_text(sid, max_length=20)
    session = store.get(sid)
    if session is None:
        return JSONResponse({"ok": False, "error": "Session not found"}, status_code=404)

    status = session.get("status", "created")
    all_events = session.get("events", [])

    # Build a snapshot of relevant data depending on current status
    result_data: dict[str, Any] = {}
    if status == "waiting_company":
        result_data["company_info"] = session.get("data", {}).get("company_info", {})
        result_data["fns_data"] = session.get("data", {}).get("fns_data", {})
    elif status == "waiting_competitors":
        market_info = session.get("data", {}).get("market_info", {})
        result_data["market_name"] = market_info.get("market_name", "")
        result_data["competitors"] = market_info.get("competitors", [])
    elif status == "done":
        # Find the 'done' event to extract report URL
        for ev in reversed(all_events):
            if ev.get("event") == "done":
                result_data["report"] = ev.get("data", {})
                break
    elif status == "error":
        for ev in reversed(all_events):
            if ev.get("event") == "error":
                err_data = ev.get("data", {})
                result_data["error"] = err_data.get("message", "Unknown error")
                if err_data.get("traceback"):
                    result_data["traceback"] = err_data["traceback"]
                break

    return {
        "ok": True,
        "status": status,
        "events": all_events,
        "data": result_data,
    }


# ── Legacy endpoint (simple, no interactive) ──

@app.post("/api/analyze")
async def analyze_simple(request: Request):
    """Simple non-interactive endpoint (backward compat)."""
    # Rate limit: max 5 reports per hour per IP
    client_ip = get_client_ip(request)
    report_error = check_rate_limit_report(client_ip)
    if report_error:
        return JSONResponse(
            {"ok": False, "error": report_error},
            status_code=429,
            headers={"Retry-After": "3600"},
        )

    # Auth: check freemium quota
    auth_token = _get_auth_token(request)
    can_gen, reason = auth_manager.can_generate_report(auth_token)
    if not can_gen:
        return JSONResponse(
            {"ok": False, "error": reason, "quota_exceeded": True},
            status_code=403,
        )

    body = await request.json()
    raw_url = (body.get("url") or "").strip()

    # Validate & sanitize URL
    is_valid, url, url_error = validate_url(raw_url)
    if not is_valid:
        return JSONResponse({"ok": False, "error": url_error}, status_code=400)

    try:
        from app.pipeline.steps.step1_scrape import run as scrape
        scraped = scrape(url)
    except Exception as e:
        logger.exception("Scrape error for %s", url)
        return {"ok": False, "error": f"Ошибка скрапинга: {sanitize_error(e, include_details=not IS_PRODUCTION)}", "step": 1}

    try:
        from app.pipeline.llm_analyzer import analyze_with_llm
        report_data = analyze_with_llm(scraped)
    except Exception as e:
        logger.exception("LLM analysis error")
        return {"ok": False, "error": f"Ошибка AI: {sanitize_error(e, include_details=not IS_PRODUCTION)}", "step": 2}

    report_data = _sanitize_llm_output(report_data)

    try:
        from app.models import ReportData
        from app.report.builder import save_report
        rd = ReportData(**report_data)
        filename = f"report_{uuid.uuid4().hex[:8]}.html"
        path = save_report(rd, filename=filename)
        size_kb = round(path.stat().st_size / 1024)
    except Exception as e:
        logger.exception("Report build error")
        return {"ok": False, "error": f"Ошибка сборки: {sanitize_error(e, include_details=not IS_PRODUCTION)}", "step": 3}

    # Auth: increment report count
    if auth_token:
        auth_manager.increment_report_count(auth_token, report_id=filename)

    return {"ok": True, "url": f"/reports/{filename}", "size_kb": size_kb,
            "company": report_data.get("company", {}).get("name", "")}


# ── LANDING_HTML imported from app/landing.py ──
_LANDING_HTML_REMOVED = True  # old inline HTML removed, see app/landing.py
