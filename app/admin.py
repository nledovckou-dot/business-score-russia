"""Admin dashboard: view all sessions, reports, metrics.

Simple admin panel with env-based auth (BSR_ADMIN_TOKEN).
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse

from app.session_store import get_store
from app.metrics import get_aggregate_stats

logger = logging.getLogger("bsr.admin")

router = APIRouter(prefix="/admin", tags=["admin"])

ADMIN_TOKEN = os.environ.get("BSR_ADMIN_TOKEN", "bsr-admin-2026")


def _check_admin(request: Request):
    """Simple admin auth: check token in cookie or query param."""
    token = request.cookies.get("bsr_admin") or request.query_params.get("token")
    if token != ADMIN_TOKEN:
        return False
    return True


def _format_time(ts: float | None) -> str:
    """Format unix timestamp to human-readable."""
    if not ts:
        return "—"
    from datetime import datetime
    return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")


def _format_duration(seconds: float | None) -> str:
    if not seconds:
        return "—"
    if seconds < 60:
        return f"{seconds:.0f}с"
    m = int(seconds) // 60
    s = int(seconds) % 60
    return f"{m}м {s}с"


@router.get("/api/sessions")
async def admin_sessions(request: Request):
    """List all active sessions with details."""
    if not _check_admin(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    store = get_store()
    session_ids = store.list_sessions()
    sessions = []

    for sid in session_ids:
        s = store.get(sid)
        if not s:
            continue

        data = s.get("data", {})
        events = s.get("events", [])

        # Extract key info
        company_info = data.get("company_info", {})
        confirmed = data.get("confirmed_company", {})
        fns = data.get("fns_data", {}).get("fns_company", {})

        # Find report URL if done
        report_url = None
        report_size = None
        for ev in reversed(events):
            if ev.get("event") == "done":
                report_url = ev.get("data", {}).get("url")
                report_size = ev.get("data", {}).get("size_kb")
                break

        # Find error if any
        error_msg = None
        for ev in reversed(events):
            if ev.get("event") == "error":
                error_msg = ev.get("data", {}).get("message", "")[:200]
                break

        # Compute duration
        created = s.get("created_at", 0)
        duration = None
        if s.get("status") == "done":
            # Find last event time
            for ev in reversed(events):
                if ev.get("event") == "done":
                    # estimate from created_at to now or step times
                    break
            # Use metrics if available
            mc = s.get("_metrics")
            if mc and hasattr(mc, "_finalized_data"):
                duration = mc._finalized_data.get("total_time_sec")

        company_name = confirmed.get("name") or company_info.get("name", "—")
        inn = confirmed.get("inn") or fns.get("inn") or company_info.get("inn", "")

        sessions.append({
            "sid": sid,
            "status": s.get("status", "created"),
            "company": company_name,
            "inn": inn,
            "url": data.get("url", ""),
            "created_at": created,
            "created_at_fmt": _format_time(created),
            "report_url": report_url,
            "report_size_kb": report_size,
            "error": error_msg,
            "steps_done": len({
                str(ev.get("data", {}).get("num"))
                for ev in events
                if ev.get("event") == "step" and ev.get("data", {}).get("status") == "done"
            }),
            "steps_total": 14,
        })

    # Sort by created_at descending
    sessions.sort(key=lambda x: x.get("created_at", 0), reverse=True)

    return {"ok": True, "sessions": sessions, "total": len(sessions)}


@router.get("/api/metrics")
async def admin_metrics(request: Request):
    """Aggregate metrics from metrics.jsonl."""
    if not _check_admin(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    try:
        stats = get_aggregate_stats()
        return {"ok": True, **stats}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:300]}, status_code=500)


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def admin_page(request: Request):
    """Admin dashboard HTML."""
    if not _check_admin(request):
        # Show login form
        return HTMLResponse(content=_LOGIN_HTML)
    return HTMLResponse(content=_DASHBOARD_HTML)


# ── Login form ──

_LOGIN_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BSR Admin</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; display: flex; align-items: center; justify-content: center; min-height: 100vh; }
.login { background: #fff; padding: 40px; border-radius: 12px; box-shadow: 0 2px 20px rgba(0,0,0,.08); width: 360px; }
h1 { font-size: 20px; margin-bottom: 24px; color: #111; }
input { width: 100%; padding: 12px 16px; border: 1px solid #ddd; border-radius: 8px; font-size: 15px; margin-bottom: 16px; outline: none; }
input:focus { border-color: #111; }
button { width: 100%; padding: 12px; background: #111; color: #fff; border: none; border-radius: 8px; font-size: 15px; cursor: pointer; }
button:hover { background: #333; }
.err { color: #d44; font-size: 13px; margin-bottom: 12px; display: none; }
</style>
</head>
<body>
<div class="login">
<h1>BSR Admin</h1>
<div class="err" id="err">Неверный токен</div>
<form onsubmit="return doLogin()">
<input type="password" id="token" placeholder="Admin token" autocomplete="off">
<button type="submit">Войти</button>
</form>
</div>
<script>
function doLogin() {
  const token = document.getElementById('token').value.trim();
  if (!token) return false;
  document.cookie = 'bsr_admin=' + token + ';path=/;max-age=2592000';
  location.reload();
  return false;
}
</script>
</body>
</html>"""


# ── Dashboard HTML ──

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BSR Admin Dashboard</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #111; }

.topbar { background: #111; color: #fff; padding: 14px 32px; display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 100; }
.topbar h1 { font-size: 17px; font-weight: 600; }
.topbar .right { display: flex; gap: 16px; align-items: center; }
.topbar a { color: #aaa; text-decoration: none; font-size: 13px; }
.topbar a:hover { color: #fff; }

.container { max-width: 1200px; margin: 0 auto; padding: 24px; }

/* Stats cards */
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 32px; }
.stat-card { background: #fff; border-radius: 12px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.06); }
.stat-card .label { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
.stat-card .value { font-size: 28px; font-weight: 700; color: #111; }
.stat-card .sub { font-size: 12px; color: #888; margin-top: 4px; }

/* Sessions table */
.section-title { font-size: 16px; font-weight: 600; margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }
.section-title .count { background: #111; color: #fff; font-size: 11px; padding: 2px 8px; border-radius: 10px; }

.sessions-table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,.06); }
.sessions-table th { text-align: left; padding: 12px 16px; font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid #eee; background: #fafafa; }
.sessions-table td { padding: 12px 16px; font-size: 14px; border-bottom: 1px solid #f0f0f0; }
.sessions-table tr:last-child td { border-bottom: none; }
.sessions-table tr:hover td { background: #f8f8f8; }

/* Status badges */
.badge { display: inline-block; padding: 3px 10px; border-radius: 6px; font-size: 12px; font-weight: 500; }
.badge.done { background: #e8f5e9; color: #2e7d32; }
.badge.error { background: #ffeef0; color: #d32f2f; }
.badge.analyzing { background: #e3f2fd; color: #1565c0; }
.badge.waiting_company, .badge.waiting_competitors { background: #fff3e0; color: #e65100; }
.badge.finding_competitors, .badge.scraping { background: #f3e5f5; color: #7b1fa2; }
.badge.created { background: #f5f5f5; color: #666; }

.report-link { color: #1565c0; text-decoration: none; font-weight: 500; }
.report-link:hover { text-decoration: underline; }

.inn { font-family: 'SF Mono', Monaco, Consolas, monospace; font-size: 12px; color: #666; }
.url { font-size: 12px; color: #888; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.error-hint { font-size: 11px; color: #d32f2f; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

.progress { display: flex; align-items: center; gap: 6px; }
.progress-bar { width: 60px; height: 6px; background: #eee; border-radius: 3px; overflow: hidden; }
.progress-bar .fill { height: 100%; background: #111; border-radius: 3px; transition: width 0.3s; }
.progress-text { font-size: 11px; color: #888; }

.empty { text-align: center; padding: 60px 20px; color: #888; }

/* Responsive */
@media (max-width: 768px) {
    .container { padding: 16px; }
    .stats { grid-template-columns: repeat(2, 1fr); }
    .sessions-table { font-size: 12px; }
    .sessions-table th, .sessions-table td { padding: 8px 10px; }
}

/* Loading */
.loading { text-align: center; padding: 40px; color: #888; }
#refreshBtn { background: none; border: 1px solid #555; color: #fff; padding: 4px 12px; border-radius: 6px; cursor: pointer; font-size: 12px; }
#refreshBtn:hover { background: #333; }
</style>
</head>
<body>

<div class="topbar">
    <h1>BSR Admin Dashboard</h1>
    <div class="right">
        <button id="refreshBtn" onclick="loadAll()">Обновить</button>
        <a href="/">На главную</a>
        <a href="#" onclick="document.cookie='bsr_admin=;path=/;max-age=0';location.reload()">Выйти</a>
    </div>
</div>

<div class="container">
    <div class="stats" id="stats">
        <div class="loading">Загрузка...</div>
    </div>

    <div class="section-title">
        Сессии <span class="count" id="sessionsCount">0</span>
    </div>
    <div id="sessionsContainer">
        <div class="loading">Загрузка...</div>
    </div>
</div>

<script>
const STATUS_LABELS = {
    created: 'Создан',
    scraping: 'Скрапинг',
    waiting_company: 'Ждёт компанию',
    finding_competitors: 'Ищет конкурентов',
    waiting_competitors: 'Ждёт конкурентов',
    analyzing: 'Анализ',
    done: 'Готов',
    error: 'Ошибка',
};

function loadAll() {
    loadMetrics();
    loadSessions();
}

async function loadMetrics() {
    try {
        const r = await fetch('/admin/api/metrics');
        const d = await r.json();
        if (!d.ok) throw new Error(d.error);
        renderStats(d);
    } catch (e) {
        document.getElementById('stats').innerHTML = '<div class="stat-card"><div class="label">Ошибка</div><div class="value">' + e.message + '</div></div>';
    }
}

function renderStats(d) {
    const html = `
        <div class="stat-card">
            <div class="label">Всего отчётов</div>
            <div class="value">${d.total_reports || 0}</div>
        </div>
        <div class="stat-card">
            <div class="label">Среднее время</div>
            <div class="value">${d.avg_time_sec ? Math.round(d.avg_time_sec) + 'с' : '—'}</div>
        </div>
        <div class="stat-card">
            <div class="label">Общая стоимость</div>
            <div class="value">$${(d.total_cost_usd || 0).toFixed(2)}</div>
        </div>
        <div class="stat-card">
            <div class="label">LLM вызовов</div>
            <div class="value">${d.total_llm_calls || 0}</div>
        </div>
        <div class="stat-card">
            <div class="label">Стоимость / отчёт</div>
            <div class="value">$${(d.avg_cost_per_report_usd || 0).toFixed(2)}</div>
        </div>
    `;
    document.getElementById('stats').innerHTML = html;
}

async function loadSessions() {
    try {
        const r = await fetch('/admin/api/sessions');
        const d = await r.json();
        if (!d.ok) throw new Error(d.error);
        document.getElementById('sessionsCount').textContent = d.total;
        renderSessions(d.sessions);
    } catch (e) {
        document.getElementById('sessionsContainer').innerHTML = '<div class="empty">Ошибка: ' + e.message + '</div>';
    }
}

function renderSessions(sessions) {
    if (!sessions.length) {
        document.getElementById('sessionsContainer').innerHTML = '<div class="empty">Нет сессий</div>';
        return;
    }

    let rows = '';
    for (const s of sessions) {
        const statusClass = s.status;
        const statusLabel = STATUS_LABELS[s.status] || s.status;
        const progress = Math.round((s.steps_done / s.steps_total) * 100);

        let reportCell = '—';
        if (s.report_url) {
            reportCell = `<a class="report-link" href="${s.report_url}" target="_blank">Отчёт (${s.report_size_kb || '?'} KB)</a>`;
        } else if (s.error) {
            reportCell = `<span class="error-hint" title="${s.error}">${s.error}</span>`;
        }

        rows += `<tr>
            <td><span class="badge ${statusClass}">${statusLabel}</span></td>
            <td><strong>${s.company || '—'}</strong><br><span class="inn">${s.inn || ''}</span></td>
            <td><span class="url" title="${s.url}">${s.url || '—'}</span></td>
            <td>
                <div class="progress">
                    <div class="progress-bar"><div class="fill" style="width:${progress}%"></div></div>
                    <span class="progress-text">${s.steps_done}/${s.steps_total}</span>
                </div>
            </td>
            <td>${reportCell}</td>
            <td>${s.created_at_fmt}</td>
            <td style="font-family:monospace;font-size:11px;color:#888">${s.sid}</td>
        </tr>`;
    }

    document.getElementById('sessionsContainer').innerHTML = `
        <table class="sessions-table">
            <thead>
                <tr>
                    <th>Статус</th>
                    <th>Компания</th>
                    <th>URL</th>
                    <th>Прогресс</th>
                    <th>Отчёт</th>
                    <th>Создан</th>
                    <th>ID</th>
                </tr>
            </thead>
            <tbody>${rows}</tbody>
        </table>
    `;
}

// Auto-refresh every 15 seconds
loadAll();
setInterval(loadAll, 15000);
</script>
</body>
</html>"""
