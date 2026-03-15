"""FastAPI app: interactive multi-step business analysis pipeline."""

from __future__ import annotations

import logging
import os
import time
import uuid
import json
import threading
import urllib.parse
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.auth import AuthManager
from app.config import REPORTS_DIR, BusinessType
from app.landing import LANDING_HTML  # fallback

# New Next.js landing
_FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "out")
_FRONTEND_INDEX = os.path.join(_FRONTEND_DIR, "index.html")
_USE_NEXT_LANDING = os.path.exists(_FRONTEND_INDEX)
from app.metrics import MetricsCollector, get_aggregate_stats
from app.pipeline.release import add_blocking_issue, finalize_release, set_report_status
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

APP_START_TIME = time.monotonic()

# ── Version ──
_version_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "VERSION")
try:
    with open(_version_file) as _f:
        APP_VERSION = _f.read().strip()
except FileNotFoundError:
    APP_VERSION = "0.0.0"

app = FastAPI(
    title="Бизнес-анализ 360",
    version=APP_VERSION,
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


# ── Auth gate: require login ──
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
YANDEX_CLIENT_ID = os.getenv("YANDEX_CLIENT_ID", "")

# Beta gate: only these emails can start analysis (empty = open for all)
BETA_EMAILS = set(
    e.strip().lower()
    for e in os.getenv("BSR_BETA_EMAILS", "").split(",")
    if e.strip()
)

# Paths that don't require authentication
PUBLIC_PATHS = {"/", "/app", "/login", "/api/auth/login", "/api/auth/register", "/api/auth/google", "/api/auth/yandex", "/api/auth/yandex/callback", "/api/auth/config", "/api/health", "/api/analyze", "/api/debug-rate"}
PUBLIC_PREFIXES = ("/reports/", "/static/", "/_next/", "/api/analyze/", "/logo.png", "/pavel.jpg")

LOGIN_PAGE_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Вход — Росскор</title>
<script src="https://accounts.google.com/gsi/client" async></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{min-height:100vh;display:flex;align-items:center;justify-content:center;
background:#0D0B0E;color:#fff;font-family:-apple-system,BlinkMacSystemFont,sans-serif}
.card{background:#1A1620;border-radius:16px;padding:48px;text-align:center;
max-width:400px;width:90%;box-shadow:0 8px 32px rgba(0,0,0,.4)}
h1{font-size:24px;margin-bottom:8px;color:#C9A44C}
p{color:#888;font-size:14px;margin-bottom:32px}
.logo{font-size:48px;margin-bottom:16px}
#g_id_onload{display:flex;justify-content:center}
.error{color:#D44040;font-size:13px;margin-top:16px;display:none}
</style>
</head>
<body>
<div class="card">
<div class="logo">📊</div>
<h1>Росскор</h1>
<p>Анализ бизнеса 360°</p>
<div id="g_id_onload"
  data-client_id="__GOOGLE_CLIENT_ID__"
  data-context="signin"
  data-ux_mode="popup"
  data-callback="onGoogleSignIn"
  data-auto_prompt="false">
</div>
<div class="g_id_signin"
  data-type="standard"
  data-shape="rectangular"
  data-theme="filled_black"
  data-text="signin_with"
  data-size="large"
  data-locale="ru"
  data-logo_alignment="left">
</div>
<div class="error" id="error"></div>
<div style="color:#555;font-size:11px;margin-top:24px">v__APP_VERSION__</div>
</div>
<script>
function onGoogleSignIn(response) {
  fetch('/api/auth/google', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({credential: response.credential})
  })
  .then(r => r.json())
  .then(data => {
    if (data.ok) {
      window.location.href = '/';
    } else {
      const el = document.getElementById('error');
      el.textContent = data.error || 'Ошибка входа';
      el.style.display = 'block';
    }
  })
  .catch(() => {
    const el = document.getElementById('error');
    el.textContent = 'Ошибка сети';
    el.style.display = 'block';
  });
}
</script>
</body>
</html>"""


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    """Login page with Google Sign-In button."""
    html = LOGIN_PAGE_HTML.replace("__GOOGLE_CLIENT_ID__", GOOGLE_CLIENT_ID).replace("__APP_VERSION__", APP_VERSION)
    return HTMLResponse(content=html)


@app.middleware("http")
async def auth_gate_middleware(request: Request, call_next):
    """Require authentication for all pages except login and public paths."""
    path = request.url.path

    # Allow public paths
    if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
        return await call_next(request)

    # Allow admin with token param (admin panel + API)
    admin_token = request.query_params.get("token")
    if admin_token:
        from app.admin import ADMIN_TOKEN
        if admin_token == ADMIN_TOKEN:
            return await call_next(request)

    # Check auth cookie
    token = request.cookies.get(COOKIE_NAME)
    if token and auth_manager.check_token(token):
        return await call_next(request)

    # Not authenticated — redirect to login (for pages) or 401 (for API)
    if path.startswith("/api/"):
        return JSONResponse({"ok": False, "error": "Требуется авторизация"}, status_code=401)

    from starlette.responses import RedirectResponse
    return RedirectResponse(url="/login", status_code=302)


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

# Serve Next.js static assets (JS, CSS, images)
if _USE_NEXT_LANDING:
    app.mount("/_next", StaticFiles(directory=os.path.join(_FRONTEND_DIR, "_next")), name="next-static")

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

@app.get("/api/health")
async def health():
    uptime_sec = int(time.monotonic() - APP_START_TIME)
    return {"ok": True, "version": APP_VERSION, "uptime_sec": uptime_sec}


@app.get("/api/auth/config")
async def auth_config():
    """Public auth config — client IDs for social login buttons."""
    return {
        "google_client_id": GOOGLE_CLIENT_ID,
        "yandex_client_id": YANDEX_CLIENT_ID,
    }


@app.get("/api/diag-admin")
async def diag_admin():
    """Debug: check admin HTML encoding on this server."""
    from app.admin import _DASHBOARD_HTML
    html = _DASHBOARD_HTML
    surrogates = []
    for i, ch in enumerate(html):
        if 0xD800 <= ord(ch) <= 0xDFFF:
            surrogates.append({"pos": i, "code": f"U+{ord(ch):04X}"})
    try:
        html.encode("utf-8")
        return {"ok": True, "length": len(html), "surrogates": surrogates}
    except UnicodeEncodeError as e:
        return {"ok": False, "error": str(e), "surrogates": surrogates, "length": len(html)}


@app.get("/", response_class=HTMLResponse)
async def index():
    if _USE_NEXT_LANDING:
        with open(_FRONTEND_INDEX, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content=LANDING_HTML)


if _USE_NEXT_LANDING:
    from starlette.responses import FileResponse as _FileResponse

    @app.get("/logo.png")
    async def frontend_logo():
        return _FileResponse(os.path.join(_FRONTEND_DIR, "logo.png"), media_type="image/png")

    @app.get("/pavel.jpg")
    async def frontend_pavel():
        return _FileResponse(os.path.join(_FRONTEND_DIR, "pavel.jpg"), media_type="image/jpeg")


@app.get("/app", response_class=HTMLResponse)
async def app_page():
    """Analysis pipeline UI (old landing with URL input + SSE progress)."""
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
    consent_data = bool(body.get("consent_data", False))
    consent_marketing = bool(body.get("consent_marketing", False))

    # Don't sanitize password (it may contain special chars), just limit length
    if len(password) > 128:
        return JSONResponse({"ok": False, "error": "Пароль слишком длинный"}, status_code=400)

    client_ip = get_client_ip(request)

    try:
        result = auth_manager.register(
            email, password,
            consent_data=consent_data,
            consent_marketing=consent_marketing,
            client_ip=client_ip,
        )
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


@app.post("/api/auth/google")
async def auth_google(request: Request):
    """Login via Google Sign-In. Verifies ID token, auto-registers."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Некорректный запрос"}, status_code=400)

    credential = str(body.get("credential", ""))
    if not credential:
        return JSONResponse({"ok": False, "error": "Нет токена Google"}, status_code=400)

    result = auth_manager.google_login(credential)
    if not result:
        return JSONResponse(
            {"ok": False, "error": "Ошибка входа через Google. Попробуйте ещё раз."},
            status_code=403,
        )

    resp = JSONResponse({
        "ok": True,
        "email": result["email"],
        "name": result.get("name", ""),
        "reports_used": result["reports_used"],
        "reports_remaining": result["reports_remaining"],
    })
    return _set_auth_cookie(resp, result["token"])


@app.get("/api/auth/yandex")
async def auth_yandex_redirect(request: Request):
    """Redirect user to Yandex OAuth consent screen."""
    if not YANDEX_CLIENT_ID:
        return JSONResponse({"ok": False, "error": "Яндекс OAuth не настроен"}, status_code=500)
    # Build Yandex OAuth URL
    import urllib.parse
    # Determine redirect URI from request
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", request.url.netloc)
    redirect_uri = f"{scheme}://{host}/api/auth/yandex/callback"
    # Save pendingUrl in state param if provided
    state = request.query_params.get("pending_url", "")
    params = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": YANDEX_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "state": state,
    })
    from starlette.responses import RedirectResponse
    return RedirectResponse(url=f"https://oauth.yandex.ru/authorize?{params}")


@app.get("/api/auth/yandex/callback")
async def auth_yandex_callback(request: Request):
    """Handle Yandex OAuth callback — exchange code, set cookie, redirect to landing."""
    code = request.query_params.get("code", "")
    state = request.query_params.get("state", "")  # pendingUrl
    if not code:
        return HTMLResponse("<h3>Ошибка: нет кода авторизации от Яндекса</h3>", status_code=400)

    result = auth_manager.yandex_login(code)
    if not result:
        return HTMLResponse("<h3>Ошибка входа через Яндекс. Попробуйте ещё раз.</h3>", status_code=403)

    # Redirect to landing with success indicator
    from starlette.responses import RedirectResponse
    redirect_url = "/?yandex_auth=ok"
    if state:
        redirect_url += "&pending_url=" + urllib.parse.quote(state, safe="")
    resp = RedirectResponse(url=redirect_url, status_code=302)
    resp.set_cookie(
        key=COOKIE_NAME,
        value=result["token"],
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=IS_PRODUCTION,
    )
    return resp


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

    # Beta gate: only allowed emails can start analysis
    if BETA_EMAILS:
        user_info = auth_manager.check_token(auth_token) if auth_token else None
        user_email = (user_info.get("email", "") if user_info else "").lower()
        logger.info("Beta gate check: token=%s, user_email=%r, allowed=%s",
                     auth_token[:8] if auth_token else "none", user_email, user_email in BETA_EMAILS)
        if not user_info:
            return JSONResponse(
                {"ok": False, "error": "Требуется авторизация", "auth_required": True},
                status_code=401,
            )
        if user_email not in BETA_EMAILS:
            return JSONResponse(
                {"ok": False, "error": "Сервис в режиме закрытого тестирования. Скоро откроем для всех!"},
                status_code=403,
            )

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

    # Filter out self-competitor (company analyzing itself)
    company_info = session["data"].get("company_info", {})
    target_name = (company_info.get("name") or "").lower().strip()
    target_inn = (company_info.get("inn") or "").strip()
    competitors = [
        c for c in competitors
        if isinstance(c, dict) and
        (c.get("name", "").lower().strip() != target_name) and
        (not target_inn or (c.get("inn") or "") != target_inn)
    ]

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
        # Step 0: Prepare the research route
        _push_event(sid, "step", {"num": 0, "status": "active", "text": "Собираем исходный контур..."})
        try:
            probe_results = refresh_models()
            session["data"]["selected_models"] = probe_results
            _push_event(sid, "step", {"num": 0, "status": "done", "text": "Маршрут исследования готов"})
        except Exception as e:
            logger.warning("Model probe failed, using defaults: %s", str(e)[:200])
            _push_event(sid, "step", {"num": 0, "status": "warning", "text": "Базовый маршрут исследования готов"})

        # Step 1: Scrape
        _push_event(sid, "step", {"num": 1, "status": "active", "text": "Изучаем сайт и продукт..."})
        mc.start_timer("step1_scrape")
        from app.pipeline.steps.step1_scrape import run as scrape
        scraped = scrape(url)
        mc.stop_timer("step1_scrape")
        session["data"]["scraped"] = scraped
        scrape_method = scraped.get("scrape_method", "requests")
        if scrape_method == "playwright":
            method_hint = " (Playwright fallback)"
        elif scrape_method == "minimal":
            method_hint = " (minimal fallback)"
        elif scrape_method == "web_search":
            method_hint = " (web search fallback)"
        elif scrape_method == "domain_only":
            method_hint = " (только домен)"
        else:
            method_hint = ""
        site_title = scraped.get("title", "") or "основные материалы собраны"
        _push_event(sid, "step", {"num": 1, "status": "done", "text": f"Сайт изучен{method_hint}: {site_title}"})

        # Step 2: Identify company
        _push_event(sid, "step", {"num": 2, "status": "active", "text": "Уточняем профиль компании..."})
        mc.start_timer("step2_identify")
        from app.pipeline.steps.step2_identify import run as identify
        company_info = identify(scraped)
        mc.stop_timer("step2_identify")
        session["data"]["company_info"] = company_info
        _push_event(sid, "step", {"num": 2, "status": "done", "text": f"Контур компании: {company_info.get('name', '?')}"})

        # Step 3: FNS lookup
        _push_event(sid, "step", {"num": 3, "status": "active", "text": "Собираем официальные сведения..."})
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
                "text": f"Подтверждено юрлицо: {fc.get('name', '')} | ИНН {fc.get('inn', '')}"})
        else:
            _push_event(sid, "step", {"num": 3, "status": "warning",
                "text": "Официальный контур не определился автоматически"})

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
            _push_event(sid, "step", {"num": 3, "status": "active", "text": f"Уточняем официальный контур по ИНН {confirmed['inn']}..."})
            if mc:
                mc.start_timer("step3_fns_refetch")
            try:
                from app.pipeline.steps.step3_fns import run as fns_lookup
                fns_data = fns_lookup(data.get("company_info", {}), confirmed_inn=confirmed["inn"])
                data["fns_data"] = fns_data
                _push_event(sid, "step", {"num": 3, "status": "done", "text": "Официальный контур обновлён"})
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

        # Step 3c: Checko.ru enrichment for main company (primary data source)
        inn = company_info.get("inn") or data.get("fns_data", {}).get("fns_company", {}).get("inn", "")
        # If no INN yet — try Checko search by company name (Russian + English)
        if not inn:
            try:
                from app.pipeline.enrichment.checko import search_company as checko_search
                company_name = company_info.get("name", "")
                legal_name = company_info.get("legal_name", "")
                for search_term in [legal_name, company_name, company_name.upper()]:
                    if not search_term or len(search_term) < 4:
                        continue
                    found = checko_search(search_term, limit=3)
                    if found:
                        inn = found[0].get("inn", "")
                        if inn:
                            company_info["inn"] = inn
                            data["company_info"] = company_info
                            logger.info("Checko search found INN %s for '%s'", inn, search_term)
                            break
            except Exception as e:
                logger.warning("Checko INN search failed: %s", str(e)[:200])
        if inn:
            try:
                from app.pipeline.enrichment.checko import get_company as checko_company, get_finances as checko_finances
                _push_event(sid, "step", {"num": "3c", "status": "active", "text": "Дополняем официальный контур..."})

                checko_data = checko_company(inn)
                if checko_data:
                    data["checko_company"] = checko_data
                    # Enrich company_info with Checko contacts
                    if checko_data.get("contacts", {}).get("website"):
                        company_info["website"] = company_info.get("website") or checko_data["contacts"]["website"]
                    if checko_data.get("employees"):
                        company_info["employees"] = checko_data["employees"]
                    if checko_data.get("contacts", {}).get("phones"):
                        company_info["phones"] = checko_data["contacts"]["phones"]
                    data["company_info"] = company_info

                checko_fin = checko_finances(inn)
                if checko_fin:
                    data["checko_finances"] = checko_fin
                    # Convert to FNS-compatible format and merge into fns_data
                    fns_data = data.get("fns_data", {})
                    checko_financials = []
                    for year_str, vals in sorted(checko_fin.items()):
                        rev = vals.get("revenue")
                        if rev is not None:
                            rev = rev / 1000  # rubles → thousands
                        profit = vals.get("net_profit")
                        if profit is not None:
                            profit = profit / 1000
                        assets_val = vals.get("assets")
                        if assets_val is not None:
                            assets_val = assets_val / 1000
                        checko_financials.append({
                            "year": vals.get("year", int(year_str)),
                            "revenue": rev,
                            "net_profit": profit,
                            "assets": assets_val,
                            "source": "checko",
                        })
                    if checko_financials:
                        fns_data["financials"] = checko_financials
                        data["fns_data"] = fns_data

                details = []
                if checko_data:
                    details.append(f"сотрудники: {checko_data.get('employees', '?')}")
                    details.append(f"факторы риска: {checko_data.get('risk_count', 0)}")
                if checko_fin:
                    details.append(f"финансы: {len(checko_fin)} лет")
                detail_text = ", ".join(details) if details else "ключевые сведения собраны"
                _push_event(sid, "step", {"num": "3c", "status": "done",
                    "text": f"Официальный контур дополнен: {detail_text}"})
                logger.info("Checko OK for company INN %s: %s", inn, ", ".join(details))
            except Exception as e:
                logger.warning("Checko failed for company INN %s: %s", inn, str(e)[:200])
                _push_event(sid, "step", {"num": "3c", "status": "warning", "text": f"Официальный контур: {e}"})

        # Step 4: Find competitors + verify via web search
        _push_event(sid, "step", {"num": 4, "status": "active", "text": "Формируем конкурентное поле..."})
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
                f"Собрали поле из {total_count} игроков "
                f"({verified_count} подтверждены, "
                f"{total_count - verified_count} требуют проверки)"
            )
        else:
            step4_text = f"Собрали конкурентное поле: {total_count} игроков"
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
            _push_event(sid, "step", {"num": "1b", "status": "active", "text": "Проверяем площадки продаж..."})
            if mc:
                mc.start_timer("step1b_marketplace")
            try:
                from app.pipeline.steps.step1b_marketplace import run as marketplace_analysis
                marketplace_data = marketplace_analysis(
                    company_info=company_info,
                    scraped=data.get("scraped", {}),
                    competitors=confirmed_competitors,
                )
                _push_event(sid, "step", {"num": "1b", "status": "done", "text": "Площадки продаж собраны"})
            except Exception as e:
                _push_event(sid, "step", {"num": "1b", "status": "warning", "text": f"Площадки продаж: {e}"})
            if mc:
                mc.stop_timer("step1b_marketplace")

        # Step 1c: Deep models (lifecycle + channels)
        deep_models = None
        _push_event(sid, "step", {"num": "1c", "status": "active", "text": "Разбираем каналы продаж..."})
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
                "text": f"Каналы и роли игроков: {lc_count} профилей, {ch_count} каналов"})
        except Exception as e:
            _push_event(sid, "step", {"num": "1c", "status": "warning", "text": f"Каналы продаж: {e}"})
        if mc:
            mc.stop_timer("step1c_deep_models")

        # Step 4.5: HH.ru API (real HR data)
        hh_data = None
        company_name = company_info.get("name", "")
        if company_name:
            _push_event(sid, "step", {"num": "4h", "status": "active", "text": "Смотрим кадровые сигналы..."})
            try:
                from app.pipeline.sources.hh_api import get_hr_data_for_company
                hh_data = get_hr_data_for_company(
                    company_name=company_name,
                    industry_keywords=bt,
                )
                vcount = (hh_data or {}).get("vacancies_count", 0)
                _push_event(sid, "step", {"num": "4h", "status": "done",
                    "text": f"Кадровые сигналы: {vcount} вакансий"})
            except Exception as e:
                _push_event(sid, "step", {"num": "4h", "status": "warning", "text": f"Кадровые сигналы: {e}"})

        # Step 4.5k: Keys.so SEO analytics (company — full suite)
        keyso_data = None
        company_domain = ""
        try:
            raw_url = data.get("url", "")
            if raw_url:
                from urllib.parse import urlparse
                company_domain = urlparse(raw_url).hostname or ""
                if company_domain:
                    data["_company_domain"] = company_domain
                    _push_event(sid, "step", {"num": "4k", "status": "active", "text": "Смотрим поисковую видимость..."})
                    from app.pipeline.enrichment.keyso import (
                        get_domain_dashboard, get_organic_competitors,
                        get_organic_keywords, get_context_ads,
                    )

                    # 1. Dashboard (DR, visibility, top keywords, history)
                    keyso_data = get_domain_dashboard(company_domain)

                    if keyso_data:
                        # 2. Full organic competitors (up to 25)
                        seo_comps = get_organic_competitors(company_domain, limit=25)
                        if seo_comps:
                            keyso_data["organic_competitors_full"] = seo_comps

                        # 3. Top organic keywords (up to 30)
                        seo_keywords = get_organic_keywords(company_domain, limit=30)
                        if seo_keywords:
                            keyso_data["organic_keywords_full"] = seo_keywords

                        # 4. Context ads
                        ads_list = get_context_ads(company_domain, limit=15)
                        if ads_list:
                            keyso_data["context_ads"] = ads_list

                        data["keyso"] = keyso_data
                        vis = keyso_data.get("seo_metrics", {}).get("visibility", 0)
                        dr = keyso_data.get("seo_metrics", {}).get("dr", 0)
                        n_comps = len(seo_comps) if seo_comps else 0
                        n_keys = len(seo_keywords) if seo_keywords else 0
                        n_ads = len(ads_list) if ads_list else 0
                        logger.info(
                            "Keys.so OK: domain=%s, DR=%d, vis=%d, competitors=%d, keywords=%d, ads=%d",
                            company_domain, dr, vis, n_comps, n_keys, n_ads,
                        )
                        _push_event(sid, "step", {"num": "4k", "status": "done",
                            "text": f"Поиск: DR={dr}, видимость={vis}, {n_comps} игроков, {n_keys} запросов, {n_ads} объявлений"})
                    else:
                        logger.warning("Keys.so returned None for domain=%s", company_domain)
                        _push_event(sid, "step", {"num": "4k", "status": "warning",
                            "text": "Поисковая видимость: домен не найден"})
        except Exception as e:
            import traceback
            logger.error("Keys.so failed for domain=%s: %s\n%s", company_domain, str(e)[:300], traceback.format_exc()[-500:])
            _push_event(sid, "step", {"num": "4k", "status": "warning", "text": f"Поисковая видимость: {e}"})

        # Step 4.5: Enrich competitors (T42) — real FNS data, scraping, social
        _push_event(sid, "step", {"num": "4e", "status": "active", "text": "Обогащаем профили игроков..."})
        if mc:
            mc.start_timer("step4_5_enrich")
        try:
            from app.pipeline.steps.step4_5_enrich_competitors import run as enrich_competitors
            def _enrichment_progress(text: str, status: str):
                if status == "done":
                    event_status = "done"
                    event_text = "Профили игроков собраны"
                else:
                    event_status = "active"
                    event_text = "Собираем данные по игрокам"
                _push_event(sid, "step", {"num": "4e", "status": event_status, "text": event_text})

            confirmed_competitors = enrich_competitors(
                competitors=confirmed_competitors,
                progress_callback=_enrichment_progress,
            )
            with_fns = sum(1 for c in confirmed_competitors if c.get("fns_financials"))
            _push_event(sid, "step", {"num": "4e", "status": "done",
                "text": f"Профили игроков собраны: {with_fns}/{len(confirmed_competitors)} с финансовым контуром"})
        except Exception as e:
            _push_event(sid, "step", {"num": "4e", "status": "warning", "text": f"Профили игроков: {e}"})
        if mc:
            mc.stop_timer("step4_5_enrich")

        # Step 5: Build the management picture
        _push_event(sid, "step", {"num": 5, "status": "active", "text": "Собираем управленческую картину..."})

        # Progress callback: транслирует статусы секций в SSE-события
        def _step5_progress(section_name: str, status: str):
            status_map = {"started": "active", "done": "done", "error": "warning"}
            sse_status = status_map.get(status, "active")
            section_labels = {
                "Анализ рынка": "рынок",
                "Глубокий анализ конкурентов": "игроки и сравнение",
                "Анализ компании": "компания",
                "Стратегический анализ": "стратегия",
                "Приложения": "прозрачность",
                "Фаундеры и мнения": "мнения и сигналы",
                "HR-анализ": "команда и найм",
                "Продукты и услуги": "продукты и офферы",
            }
            section_label = section_labels.get(section_name, section_name.lower())
            status_text_map = {
                "started": f"Собираем блок: {section_label}",
                "done": f"Готов блок: {section_label}",
                "error": f"Требует внимания блок: {section_label}",
            }
            _push_event(sid, "step", {
                "num": "5",
                "status": sse_status,
                "text": status_text_map.get(status, f"Собираем блок: {section_label}"),
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
        _push_event(sid, "step", {"num": 5, "status": "done", "text": "Управленческая картина собрана"})

        # Sanitize LLM output first
        report_data = _sanitize_llm_output(report_data)

        # CRITICAL: If LLM returned 0 competitors, inject enriched data from step4.5
        llm_competitors = report_data.get("competitors", [])
        if not llm_competitors and confirmed_competitors:
            logger.warning("LLM returned 0 competitors, injecting %d from step4.5 enrichment", len(confirmed_competitors))
            fallback_comps = []
            for c in confirmed_competitors:
                fc = {
                    "name": c.get("name", "?"),
                    "description": c.get("description") or c.get("why_competitor", ""),
                    "legal_name": c.get("legal_name"),
                    "inn": c.get("inn"),
                    "website": c.get("website", ""),
                    "address": c.get("address") or c.get("city", ""),
                    "x": float(c.get("x", 50)),
                    "y": float(c.get("y", 50)),
                    "threat_level": c.get("threat_level", "med"),
                    "radar_scores": c.get("radar_scores", {}),
                    "metrics": c.get("metrics", {}),
                    "verified": c.get("verified", True),
                    "verification_confidence": c.get("verification_confidence", "medium"),
                }
                # Add enriched data
                if c.get("fns_financials"):
                    fc["financials"] = c["fns_financials"]
                fallback_comps.append(fc)
            report_data["competitors"] = fallback_comps
            logger.info("Injected %d fallback competitors from enrichment data", len(fallback_comps))

        # Inject real SEO data into digital section
        keyso_data = data.get("keyso")
        digital = report_data.get("digital") or {}
        company_domain = data.get("_company_domain", "")

        if keyso_data:
            # Company IS indexed — show real SEO metrics
            seo = keyso_data.get("seo_metrics", {})
            ads = keyso_data.get("ad_metrics", {})
            if seo.get("visibility"):
                digital["monthly_traffic"] = seo["visibility"] * 50
                digital["seo_score"] = min(seo.get("dr", 50), 100)
            digital["keyso"] = {
                "dr": seo.get("dr", 0),
                "visibility": seo.get("visibility", 0),
                "keywords_top10": seo.get("it10", 0),
                "keywords_total": seo.get("total_keywords", 0),
                "pages_in_index": seo.get("pages_in_index", 0),
                "ads_count": ads.get("ads_count", 0),
                "ad_budget_avg": ads.get("ad_budget_avg", 0),
                "top_keywords": keyso_data.get("organic_keywords_full", keyso_data.get("top_keywords", []))[:10],
                "seo_competitors": keyso_data.get("organic_competitors_full", keyso_data.get("seo_competitors", []))[:10],
                "context_ads": keyso_data.get("context_ads", [])[:5],
                "history": keyso_data.get("history", [])[-6:],
            }
            logger.info("Injected SEO data into digital section")
        else:
            # Company NOT indexed — critical finding + show competitor SEO data
            digital["keyso_not_indexed"] = True
            digital["keyso_domain"] = company_domain
            # Collect SEO data from enriched competitors
            comp_seo = []
            for comp in confirmed_competitors:
                k = comp.get("keyso")
                if k and k.get("seo_metrics", {}).get("visibility", 0) > 0:
                    s = k["seo_metrics"]
                    a = k.get("ad_metrics", {})
                    comp_seo.append({
                        "domain": comp.get("website", "").replace("https://", "").replace("http://", "").rstrip("/"),
                        "name": comp.get("name", ""),
                        "dr": s.get("dr", 0),
                        "visibility": s.get("visibility", 0),
                        "keywords_top10": s.get("it10", 0),
                        "ads_count": a.get("ads_count", 0),
                        "ad_budget": a.get("ad_budget_avg", 0),
                    })
            comp_seo.sort(key=lambda x: x["visibility"], reverse=True)
            if comp_seo:
                digital["keyso_competitor_comparison"] = comp_seo
                logger.info("Company not indexed, showing %d competitor SEO profiles", len(comp_seo))
            else:
                logger.info("Company not indexed, no competitor SEO data available")

        report_data["digital"] = digital

        # Short summary is generated after the full report so it can rely on final findings.
        _push_event(sid, "step", {"num": "es", "status": "active", "text": "Собираем короткий вывод..."})
        try:
            from app.pipeline.steps.step5_deep_analysis import analyze_executive_summary

            # Convert Pydantic models to dicts if needed
            swot_data = report_data.get("swot")
            if swot_data and hasattr(swot_data, "model_dump"):
                swot_data = swot_data.model_dump()
            elif swot_data and hasattr(swot_data, "dict"):
                swot_data = swot_data.dict()

            recs_data = report_data.get("recommendations")
            if recs_data and isinstance(recs_data, list):
                clean_recs = []
                for r in recs_data:
                    if hasattr(r, "model_dump"):
                        clean_recs.append(r.model_dump())
                    elif hasattr(r, "dict"):
                        clean_recs.append(r.dict())
                    elif isinstance(r, dict):
                        clean_recs.append(r)
                recs_data = clean_recs

            exec_summary = analyze_executive_summary(
                scraped=data.get("scraped", {}),
                company_info=company_info,
                fns_data=data.get("fns_data", {}),
                competitors=confirmed_competitors,
                market_info=data.get("market_info", {}),
                swot=swot_data,
                recommendations=recs_data,
            )
            if exec_summary and exec_summary.get("executive_summary"):
                report_data["executive_summary"] = exec_summary["executive_summary"]
                logger.info("Executive summary generated OK")
                _push_event(sid, "step", {"num": "es", "status": "done", "text": "Короткий вывод готов"})
            else:
                logger.warning("Executive summary empty: %s", str(exec_summary)[:200])
                _push_event(sid, "step", {"num": "es", "status": "warning", "text": "Короткий вывод требует внимания"})
        except Exception as e:
            import traceback
            logger.error("Executive summary error: %s\n%s", str(e)[:300], traceback.format_exc()[-500:])
            _push_event(sid, "step", {"num": "es", "status": "warning", "text": "Короткий вывод требует внимания"})

        # Step 2a: Verification (pure Python)
        _push_event(sid, "step", {"num": "2a", "status": "active", "text": "Сверяем факты и расчёты..."})
        if mc:
            mc.start_timer("step2a_verify")
        try:
            from app.pipeline.steps.step2a_verify import run as verify
            report_data = verify(report_data)
            corrections = sum(1 for f in report_data.get("factcheck", [])
                            if isinstance(f, dict) and f.get("correction"))
            _push_event(sid, "step", {"num": "2a", "status": "done",
                "text": f"Проверено: {corrections} корректировок"})
        except Exception as e:
            _push_event(sid, "step", {"num": "2a", "status": "warning", "text": f"Проверка фактов: {e}"})
        if mc:
            mc.stop_timer("step2a_verify")

        # Step 2a+: Generate basic factcheck & digital_verification from existing data
        report_data = _generate_factcheck_items(report_data, data.get("fns_data", {}), company_info)
        report_data = _generate_digital_verification(report_data, company_info, confirmed_competitors)

        # Step 2b: Relevance gate (pure Python)
        _push_event(sid, "step", {"num": "2b", "status": "active", "text": "Убираем лишний шум..."})
        if mc:
            mc.start_timer("step2b_relevance_gate")
        try:
            from app.pipeline.steps.step2b_relevance_gate import run as relevance_gate
            report_data = relevance_gate(report_data)
            gates = report_data.get("section_gates", {})
            disabled = sum(1 for v in gates.values() if not v)
            _push_event(sid, "step", {"num": "2b", "status": "done",
                "text": f"Фокус отчёта: {disabled} второстепенных секций скрыто"})
        except Exception as e:
            _push_event(sid, "step", {"num": "2b", "status": "warning", "text": f"Фокус отчёта: {e}"})
        if mc:
            mc.stop_timer("step2b_relevance_gate")

        board_review_data = {}
        quality_result: dict[str, Any] | None = None

        # Step 6: Board of Directors review (T25/T26)
        _push_event(sid, "step", {"num": "6a", "status": "active", "text": "Формируем финальный вердикт..."})
        if mc:
            mc.start_timer("step6_board")
        try:
            from app.pipeline.steps.step6_board import form_panel, run_review, apply_revisions
            panel = form_panel(report_data, company_info)
            review_result = run_review(report_data, panel)
            report_data = apply_revisions(report_data, review_result)
            board_review_data = report_data.get("board_review", {})
            consensus = review_result.get("consensus", {})
            approved = consensus.get("approved", False)
            critiques = consensus.get("total_critiques", 0)
            status_text = "согласован" if approved else f"{critiques} спорных мест"
            _push_event(sid, "step", {"num": "6a", "status": "done",
                "text": f"Финальный вердикт: {status_text}"})
        except Exception as e:
            logger.warning("Board review failed: %s", e)
            add_blocking_issue(report_data, f"Board review failed: {e}")
            set_report_status(report_data, "draft")
            _push_event(sid, "step", {"num": "6a", "status": "warning",
                "text": f"Финальный вердикт: {e}"})
        if mc:
            mc.stop_timer("step6_board")

        # Step 6b: Revision — apply board critiques to fix report data
        if board_review_data and board_review_data.get("consensus", {}).get("total_critiques", 0) > 0:
            _push_event(sid, "step", {"num": "6b", "status": "active", "text": "Уточняем спорные места..."})
            if mc:
                mc.start_timer("step6b_revise")
            try:
                from app.pipeline.steps.step7_revise import revise_report
                report_data = revise_report(report_data, board_review_data, company_info)
                _push_event(sid, "step", {"num": "6b", "status": "done",
                    "text": "Спорные места доработаны"})
            except Exception as e:
                logger.warning("Revision step failed: %s", e)
                add_blocking_issue(report_data, f"Revision step failed: {e}")
                set_report_status(report_data, "draft")
                _push_event(sid, "step", {"num": "6b", "status": "warning",
                    "text": f"Уточнение выводов: {e}"})
            if mc:
                mc.stop_timer("step6b_revise")

        # Step Quality: Auto quality check (T10)
        _push_event(sid, "step", {"num": "qa", "status": "active", "text": "Проверяем силу отчёта..."})
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

            status_text = f"Сила отчёта: {q_score}/100"
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
            add_blocking_issue(report_data, f"Quality check failed: {e}")
            set_report_status(report_data, "draft")
            _push_event(sid, "step", {"num": "qa", "status": "warning",
                "text": f"Проверка силы отчёта: {e}"})
        if mc:
            mc.stop_timer("step_quality")

        report_data = finalize_release(
            report_data,
            board_review=board_review_data,
            quality_result=quality_result,
        )

        # Step 7: Build report
        _push_event(sid, "step", {"num": 7, "status": "active", "text": "Готовим итоговый документ..."})
        if mc:
            mc.start_timer("step7_build_report")

        from app.models import ReportData, Competitor
        from app.report.builder import save_report

        # v4.2: Validate competitors individually — fix invalid fields instead of dropping
        raw_competitors = report_data.get("competitors", [])
        if raw_competitors:
            valid_competitors = []
            for i, comp_dict in enumerate(raw_competitors):
                if not isinstance(comp_dict, dict) or not comp_dict.get("name"):
                    continue
                try:
                    Competitor(**comp_dict)
                    valid_competitors.append(comp_dict)
                except Exception as comp_err:
                    # Try to salvage: strip problematic nested fields
                    logger.warning(
                        "Competitor %d (%s) failed validation, attempting fix: %s",
                        i, comp_dict.get("name", "?"), str(comp_err)[:200],
                    )
                    # Start with minimal valid competitor, then add optional fields
                    salvaged = {
                        "name": comp_dict.get("name", "Неизвестный"),
                        "description": comp_dict.get("description"),
                        "legal_name": comp_dict.get("legal_name"),
                        "inn": comp_dict.get("inn"),
                        "website": comp_dict.get("website"),
                        "address": comp_dict.get("address"),
                        "x": float(comp_dict.get("x", 50)),
                        "y": float(comp_dict.get("y", 50)),
                        "threat_level": comp_dict.get("threat_level", "med"),
                        "radar_scores": comp_dict.get("radar_scores", {}),
                        "metrics": comp_dict.get("metrics") if isinstance(comp_dict.get("metrics"), dict) else {},
                        "verified": True,
                    }
                    # Try adding optional nested fields one by one
                    for opt_field in ("lifecycle", "financials", "sales_channels"):
                        if opt_field in comp_dict:
                            test = dict(salvaged)
                            test[opt_field] = comp_dict[opt_field]
                            try:
                                Competitor(**test)
                                salvaged[opt_field] = comp_dict[opt_field]
                            except Exception:
                                pass  # skip this field
                    try:
                        Competitor(**salvaged)
                        valid_competitors.append(salvaged)
                        logger.info("Competitor %d (%s) salvaged with stripped fields", i, salvaged["name"])
                    except Exception:
                        logger.warning("Competitor %d (%s) unsalvageable, dropping", i, comp_dict.get("name", "?"))
            if len(valid_competitors) < len(raw_competitors):
                logger.info(
                    "Competitors: %d/%d passed validation",
                    len(valid_competitors), len(raw_competitors),
                )
            report_data["competitors"] = valid_competitors

        # Ensure company is a proper dict (LLM sometimes returns just a string name)
        if not isinstance(report_data.get("company"), dict):
            report_data["company"] = {
                "name": company_info.get("name", str(report_data.get("company", "?"))),
                "legal_name": company_info.get("legal_name", ""),
                "inn": company_info.get("inn", ""),
                "business_type": company_info.get("business_type_guess", "B2B_SERVICE"),
                "address": company_info.get("address", ""),
                "website": data.get("url", ""),
                "description": company_info.get("description", ""),
            }
            logger.warning("Rebuilt company field from company_info (was %s)", type(report_data.get("company")).__name__)

        rd = ReportData(**report_data)
        filename = f"report_{uuid.uuid4().hex[:8]}.html"
        path = save_report(rd, filename=filename)
        size_kb = round(path.stat().st_size / 1024)
        report_status = report_data.get("report_status", "draft")
        blocking_issues = report_data.get("blocking_issues", [])

        if mc:
            mc.stop_timer("step7_build_report")
        _push_event(sid, "step", {
            "num": 7,
            "status": "done",
            "text": f"Итоговый документ готов (статус: {report_status})",
        })

        # T7: Finalize metrics and log
        metrics_record = {}
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

        # Save per-report metadata (.meta.json) for persistent cost tracking
        try:
            meta_path = path.with_suffix(".meta.json")
            meta = {
                "company": company_info.get("name", ""),
                "inn": company_info.get("inn", ""),
                "url": data.get("url", ""),
                "created_at": time.time(),
                "duration_sec": round(metrics_record.get("total_time_sec", 0), 1),
                "report_status": report_status,
                "quality_score": report_data.get("quality_summary", {}).get("score", 0),
                "llm_cost_usd": metrics_record.get("total_cost_usd", 0),
                "llm_calls": metrics_record.get("llm_calls", 0),
                "total_tokens": metrics_record.get("total_tokens_in", 0) + metrics_record.get("total_tokens_out", 0),
                "models_used": list(metrics_record.get("model_totals", {}).keys()),
            }
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("Report meta saved: %s", meta_path.name)
        except Exception:
            logger.exception("Failed to save report meta for %s", filename)

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
            "report_status": report_status,
            "blocking_issues": blocking_issues[:5],
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
        c["threat_level"] = tl if tl in ("high", "med", "low", "self") else "med"
        # v2.1: verification fields — ensure defaults
        c.setdefault("verified", True)
        vc = str(c.get("verification_confidence", "unverified")).lower()
        c["verification_confidence"] = vc if vc in ("high", "medium", "low", "unverified") else "unverified"
        if not isinstance(c.get("verification_sources"), list):
            c["verification_sources"] = []
        # v4.1: sanitize lifecycle stage — must match LifecycleStage enum
        lifecycle = c.get("lifecycle")
        if isinstance(lifecycle, dict):
            stage = str(lifecycle.get("stage", "")).lower().strip()
            valid_stages = {"startup", "growth", "investment", "mature"}
            stage_aliases = {
                "maturity": "mature", "established": "mature", "stable": "mature",
                "decline": "mature", "seed": "startup", "pre-seed": "startup",
                "expansion": "growth", "scaling": "growth", "scale": "growth",
                "investing": "investment", "capex": "investment",
            }
            if stage not in valid_stages:
                stage = stage_aliases.get(stage, "mature")
            lifecycle["stage"] = stage
            # Ensure evidence is a list
            if not isinstance(lifecycle.get("evidence"), list):
                lifecycle["evidence"] = []
            c["lifecycle"] = lifecycle
        elif lifecycle is not None:
            # Invalid lifecycle type — remove it
            c["lifecycle"] = None
        # Ensure metrics is a dict
        if not isinstance(c.get("metrics"), dict):
            c["metrics"] = {}
        # Ensure financials is a list with valid year fields
        if "financials" in c and not isinstance(c["financials"], list):
            c["financials"] = []
        if c.get("financials"):
            clean_comp_fin = []
            for cf in c["financials"]:
                if isinstance(cf, dict) and cf.get("year"):
                    try:
                        cf["year"] = int(cf["year"])
                        clean_comp_fin.append(cf)
                    except (ValueError, TypeError):
                        pass
            c["financials"] = clean_comp_fin
        # Ensure sales_channels is a list with valid channel_name
        if "sales_channels" in c and not isinstance(c["sales_channels"], list):
            c["sales_channels"] = []
        if c.get("sales_channels"):
            c["sales_channels"] = [
                sc for sc in c["sales_channels"]
                if isinstance(sc, dict) and sc.get("channel_name")
            ]

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

    # --- v2.1: defensive string→list normalization for list-of-string fields ---
    # Prevents char-by-char rendering when Jinja2 {% for %} iterates over a string
    def _ensure_list_of_str(obj: Any, key: str) -> None:
        val = obj.get(key)
        if isinstance(val, str):
            obj[key] = [val] if val else []

    # Top-level list-of-string fields
    _ensure_list_of_str(d, "tech_trends")
    _ensure_list_of_str(d, "open_questions")

    # Company badges
    company = d.get("company")
    if isinstance(company, dict):
        _ensure_list_of_str(company, "badges")

    # Market trends and sources
    market = d.get("market")
    if isinstance(market, dict):
        _ensure_list_of_str(market, "trends")
        _ensure_list_of_str(market, "sources")

    # SWOT quadrants
    swot = d.get("swot")
    if isinstance(swot, dict):
        for quad in ("strengths", "weaknesses", "opportunities", "threats"):
            _ensure_list_of_str(swot, quad)

    # HR data notes and sources
    hr = d.get("hr_data")
    if isinstance(hr, dict):
        _ensure_list_of_str(hr, "notes")
        _ensure_list_of_str(hr, "sources")

    # Calc traces sources
    for ct in d.get("calc_traces") or []:
        if isinstance(ct, dict):
            _ensure_list_of_str(ct, "sources")

    # Product features
    for prod in d.get("products") or []:
        if isinstance(prod, dict):
            _ensure_list_of_str(prod, "features")

    # Competitor lifecycle evidence
    for c in d.get("competitors") or []:
        if isinstance(c, dict):
            lc = c.get("lifecycle")
            if isinstance(lc, dict):
                _ensure_list_of_str(lc, "evidence")

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


# ── Auto pipeline (full pipeline without interactive pauses) ──


def _run_full_pipeline_auto(sid: str, url: str):
    """Full pipeline without interactive pauses (auto-confirm company & competitors).

    Runs ALL steps: scrape → identify → FNS → competitors → enrich → deep analysis
    → verify → board review → quality check → build report.
    Same quality as interactive pipeline, but auto-confirms at each pause point.
    """
    session = store.get(sid)
    if session is None:
        return

    mc = MetricsCollector(session_id=sid)
    session["_metrics"] = mc
    from app.pipeline.llm_client import set_metrics_collector, refresh_models
    set_metrics_collector(mc)

    try:
        # Step 0: Prepare the research route
        _push_event(sid, "step", {"num": 0, "status": "active", "text": "Собираем исходный контур..."})
        try:
            refresh_models()
        except Exception:
            pass
        _push_event(sid, "step", {"num": 0, "status": "done", "text": "Маршрут исследования готов"})

        # Step 1: Scrape
        _push_event(sid, "step", {"num": 1, "status": "active", "text": "Изучаем сайт и продукт..."})
        mc.start_timer("step1_scrape")
        from app.pipeline.steps.step1_scrape import run as scrape
        scraped = scrape(url)
        mc.stop_timer("step1_scrape")
        session["data"]["scraped"] = scraped
        _push_event(sid, "step", {"num": 1, "status": "done", "text": f"Сайт изучен: {scraped.get('title', '') or 'основные материалы собраны'}"})

        # Step 2: Identify
        _push_event(sid, "step", {"num": 2, "status": "active", "text": "Уточняем профиль компании..."})
        mc.start_timer("step2_identify")
        from app.pipeline.steps.step2_identify import run as identify
        company_info = identify(scraped)
        mc.stop_timer("step2_identify")
        session["data"]["company_info"] = company_info
        _push_event(sid, "step", {"num": 2, "status": "done", "text": f"Контур компании: {company_info.get('name', '?')}"})

        # Step 3: FNS lookup
        _push_event(sid, "step", {"num": 3, "status": "active", "text": "Собираем официальные сведения..."})
        mc.start_timer("step3_fns")
        try:
            from app.pipeline.steps.step3_fns import run as fns_lookup
            fns_data = fns_lookup(company_info)
            session["data"]["fns_data"] = fns_data
        except Exception as e:
            logger.warning("FNS lookup failed: %s", str(e)[:200])
            session["data"]["fns_data"] = {"fns_error": str(e)}
        mc.stop_timer("step3_fns")
        fc = session["data"].get("fns_data", {}).get("fns_company", {})
        if fc.get("inn"):
            _push_event(sid, "step", {"num": 3, "status": "done", "text": f"Подтверждено юрлицо: {fc.get('name', '')} | ИНН {fc.get('inn', '')}"})
        else:
            _push_event(sid, "step", {"num": 3, "status": "warning", "text": "Официальный контур не определился автоматически"})

        # AUTO-CONFIRM company (no user pause)
        session["data"]["confirmed_company"] = company_info

        # Step 3c: Checko.ru enrichment for company
        inn = company_info.get("inn") or session["data"].get("fns_data", {}).get("fns_company", {}).get("inn", "")
        if not inn:
            try:
                from app.pipeline.enrichment.checko import search_company as checko_search
                for term in [company_info.get("legal_name",""), company_info.get("name",""), company_info.get("name","").upper()]:
                    if not term or len(term) < 4:
                        continue
                    found = checko_search(term, limit=3)
                    if found and found[0].get("inn"):
                        inn = found[0]["inn"]
                        company_info["inn"] = inn
                        session["data"]["company_info"] = company_info
                        logger.info("Checko auto search found INN %s for '%s'", inn, term)
                        break
            except Exception as e:
                logger.warning("Checko auto INN search failed: %s", str(e)[:200])
        if inn:
            try:
                from app.pipeline.enrichment.checko import get_company as checko_company, get_finances as checko_finances
                _push_event(sid, "step", {"num": "3c", "status": "active", "text": "Дополняем официальный контур..."})
                checko_data = checko_company(inn)
                if checko_data:
                    session["data"]["checko_company"] = checko_data
                    if checko_data.get("employees"):
                        company_info["employees"] = checko_data["employees"]
                    session["data"]["company_info"] = company_info
                checko_fin = checko_finances(inn)
                if checko_fin:
                    session["data"]["checko_finances"] = checko_fin
                    fns_data = session["data"].get("fns_data", {})
                    checko_financials = []
                    for year_str, vals in sorted(checko_fin.items()):
                        rev = vals.get("revenue")
                        profit = vals.get("net_profit")
                        assets_val = vals.get("assets")
                        checko_financials.append({
                            "year": vals.get("year", int(year_str)),
                            "revenue": rev / 1000 if rev is not None else None,
                            "net_profit": profit / 1000 if profit is not None else None,
                            "assets": assets_val / 1000 if assets_val is not None else None,
                            "source": "checko",
                        })
                    if checko_financials:
                        fns_data["financials"] = checko_financials
                        session["data"]["fns_data"] = fns_data
                details = []
                if checko_data:
                    details.append(f"сотрудники: {checko_data.get('employees', '?')}")
                if checko_fin:
                    details.append(f"финансы: {len(checko_fin)} лет")
                detail_text = ", ".join(details) if details else "ключевые сведения собраны"
                _push_event(sid, "step", {"num": "3c", "status": "done", "text": f"Официальный контур дополнен: {detail_text}"})
            except Exception as e:
                logger.warning("Checko auto failed: %s", str(e)[:200])
                _push_event(sid, "step", {"num": "3c", "status": "warning", "text": f"Официальный контур: {e}"})

        # Step 4: Find competitors
        _push_event(sid, "step", {"num": 4, "status": "active", "text": "Формируем конкурентное поле..."})
        mc.start_timer("step4_competitors")
        from app.pipeline.steps.step4_competitors import run as find_competitors
        comp_result = find_competitors(
            session["data"].get("scraped", {}),
            company_info,
            session["data"].get("fns_data", {}),
        )
        mc.stop_timer("step4_competitors")
        session["data"]["market_info"] = comp_result
        comps = comp_result.get("competitors", [])
        _push_event(sid, "step", {"num": 4, "status": "done", "text": f"Собрали конкурентное поле: {len(comps)} игроков"})

        # AUTO-CONFIRM competitors (no user pause)
        session["data"]["confirmed_competitors"] = comps

        # Now run the full analysis pipeline (same as _run_analysis_steps)
        session["status"] = "analyzing"
        _run_analysis_steps(sid)

    except Exception as e:
        import traceback
        tb_str = traceback.format_exc()
        logger.exception("Error in auto pipeline for session %s", sid)
        session = store.get(sid)
        mc_ref = session.get("_metrics") if session else None
        if mc_ref and not mc_ref._finalized:
            mc_ref.finalize()
        if session:
            session["status"] = "error"
        _push_event(sid, "error", {
            "message": sanitize_error(e, include_details=True),  # full details for debugging
            "traceback": tb_str,
        })
        store.save(sid)


@app.get("/api/debug-rate")
async def debug_rate():
    """Debug: show rate limit config."""
    from app.security import REPORTS_PER_HOUR, _report_log
    return {
        "reports_per_hour": REPORTS_PER_HOUR,
        "report_log_entries": {k: len(v) for k, v in _report_log.items()},
        "commit": "226ef19",
    }


@app.post("/api/analyze")
async def analyze_simple(request: Request):
    """Non-interactive endpoint: full pipeline with real FNS data (no user pauses)."""
    client_ip = get_client_ip(request)
    report_error = check_rate_limit_report(client_ip)
    if report_error:
        return JSONResponse(
            {"ok": False, "error": report_error},
            status_code=429,
            headers={"Retry-After": "3600"},
        )

    body = await request.json()
    raw_url = (body.get("url") or "").strip()

    # Validate & sanitize URL
    is_valid, url, url_error = validate_url(raw_url)
    if not is_valid:
        return JSONResponse({"ok": False, "error": url_error}, status_code=400)

    # Create session and run full pipeline in background (non-blocking)
    auth_token = _get_auth_token(request)
    sid = _new_session()
    session = store.get(sid)
    session["data"]["url"] = url
    session["data"]["_auth_token"] = auth_token

    thread = threading.Thread(target=_run_full_pipeline_auto, args=(sid, url), daemon=True)
    thread.start()

    return {
        "ok": True,
        "session_id": sid,
        "message": "Исследование запущено. Проверяйте статус по session_id.",
        "poll_url": f"/api/analyze/{sid}",
    }


@app.get("/api/analyze/{sid}")
async def analyze_poll(sid: str):
    """Poll analysis status. Returns result when done."""
    session = store.get(sid)
    if session is None:
        return JSONResponse({"ok": False, "error": "Сессия не найдена"}, status_code=404)

    status = session.get("status", "created")

    if status == "done":
        events = session.get("events", [])
        done_event = next((e for e in reversed(events) if e.get("event") == "done"), None)
        if done_event and done_event.get("data"):
            result = done_event["data"]
            return {
                "ok": True,
                "status": "done",
                "url": result.get("url", ""),
                "size_kb": result.get("size_kb", 0),
                "company": result.get("company", ""),
                "report_status": result.get("report_status", "draft"),
                "blocking_issues": result.get("blocking_issues", []),
            }
        return {"ok": True, "status": "done", "url": "", "company": "", "report_status": "draft", "blocking_issues": []}

    if status == "error":
        err_events = session.get("events", [])
        error_event = next((e for e in reversed(err_events) if e.get("event") == "error"), None)
        error_msg = "Ошибка анализа"
        result = {"ok": False, "status": "error"}
        if error_event and error_event.get("data"):
            error_msg = error_event["data"].get("message", error_msg)
            if error_event["data"].get("traceback"):
                result["traceback"] = error_event["data"]["traceback"]
        result["error"] = error_msg
        return result

    # Still running — return progress info
    events = session.get("events", [])
    last_step = None
    for e in reversed(events):
        if e.get("event") == "step":
            last_step = e.get("data", {}).get("text", "")
            break

    return {
        "ok": True,
        "status": "running",
        "current_step": last_step or "Собираем исходный контур...",
    }


# ── LANDING_HTML imported from app/landing.py ──
_LANDING_HTML_REMOVED = True  # old inline HTML removed, see app/landing.py
