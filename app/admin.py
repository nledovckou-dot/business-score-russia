# -*- coding: utf-8 -*-
"""Admin dashboard: view all sessions, reports, metrics, board review.

Simple admin panel with env-based auth (BSR_ADMIN_TOKEN).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
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
        return {"ok": True, "_admin_version": "0.9.5-diag", **stats}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:300]}, status_code=500)


# ── Board of Directors review ──


def _html_to_text(html: str) -> str:
    """Strip HTML tags, styles, scripts to get plain text."""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _run_board_review_on_text(report_text: str, company_name: str = "") -> dict:
    """Run 6 AI experts (Board of Directors) on plain text report content."""
    from app.pipeline.llm_client import call_board_llm, call_board_llm_parallel, refresh_models
    from app.pipeline.steps.step6_board import (
        _EXPERT_CFO, _EXPERT_CMO, _EXPERT_INDUSTRY, _EXPERT_SKEPTIC,
        _EXPERT_QA_DIRECTOR, _EXPERT_CEO,
        _parse_expert_response,
    )

    # Ensure model defaults are fresh (probes APIs)
    try:
        refresh_models()
    except Exception as e:
        logger.warning("Model refresh failed: %s", str(e)[:200])

    t0 = time.monotonic()

    context_suffix = ""
    if company_name:
        context_suffix = f"\n\nКОНТЕКСТ: анализируемая компания — «{company_name}»."

    parallel_experts = [
        _EXPERT_CFO, _EXPERT_CMO, _EXPERT_INDUSTRY, _EXPERT_SKEPTIC, _EXPERT_QA_DIRECTOR,
    ]

    # Step 1: 5 experts in parallel
    prompts = []
    for expert in parallel_experts:
        prompt = (
            f"Ты — {expert['name']} ({expert['role']}). "
            f"Рецензируй бизнес-аналитический отчёт.\n\n"
            f"Твои области фокуса: {', '.join(expert['focus_areas'])}\n\n"
            f"=== ОТЧЁТ ===\n{report_text[:60000]}\n=== КОНЕЦ ОТЧЁТА ===\n\n"
            "Дай структурированную рецензию в формате JSON."
        )
        prompts.append({
            "prompt": prompt,
            "system": expert["system"] + context_suffix,
        })

    logger.info("Board review: launching 5 parallel experts for '%s'", company_name)
    responses = call_board_llm_parallel(prompts)

    reviews = []
    for expert, raw in zip(parallel_experts, responses):
        if raw.startswith("[Board LLM Error]"):
            parsed = {
                "approved": False,
                "critiques": [{
                    "section": "llm_error",
                    "issue": raw[:300],
                    "severity": "low",
                    "suggestion": "Повторить запрос позже",
                }],
                "summary": f"Эксперт {expert['role']} недоступен.",
            }
        else:
            parsed = _parse_expert_response(raw, expert["role"])

        reviews.append({
            "role": expert["role"],
            "name": expert["name"],
            "response": parsed,
        })

    elapsed_parallel = round(time.monotonic() - t0, 2)
    logger.info("Board review: 5 experts done in %.2fs", elapsed_parallel)

    # Step 2: CEO with results of first 5
    expert_summaries = []
    for review in reviews:
        resp = review["response"]
        critiques_text = ""
        for i, c in enumerate(resp.get("critiques", []), 1):
            critiques_text += (
                f"  {i}. [{c.get('severity', '?').upper()}] "
                f"Секция: {c.get('section', '?')} — {c.get('issue', '?')}\n"
                f"     Рекомендация: {c.get('suggestion', 'нет')}\n"
            )
        expert_summaries.append(
            f"### {review['name']} ({review['role']})\n"
            f"Вердикт: {'ОДОБРЕНО' if resp.get('approved') else 'НЕ ОДОБРЕНО'}\n"
            f"Итог: {resp.get('summary', 'нет итога')}\n"
            f"Замечания:\n{critiques_text or '  (нет замечаний)'}\n"
        )

    ceo = _EXPERT_CEO
    ceo_prompt = (
        f"Ты — {ceo['name']} ({ceo['role']}). "
        "Перед тобой бизнес-аналитический отчёт и рецензии пяти экспертов.\n\n"
        f"=== ОТЧЁТ (сокращённо) ===\n{report_text[:30000]}\n"
        "=== КОНЕЦ ОТЧЁТА ===\n\n"
        "=== РЕЦЕНЗИИ ЭКСПЕРТОВ ===\n"
        + "\n".join(expert_summaries)
        + "\n=== КОНЕЦ РЕЦЕНЗИЙ ===\n\n"
        "Синтезируй рецензии. Реши, какие замечания принять, "
        "какие отклонить. Дай финальный вердикт в формате JSON."
    )

    t1 = time.monotonic()
    logger.info("Board review: launching CEO")
    ceo_raw = call_board_llm(prompt=ceo_prompt, system=ceo["system"] + context_suffix)
    elapsed_ceo = round(time.monotonic() - t1, 2)

    if ceo_raw.startswith("[Board LLM Error]"):
        ceo_parsed = {
            "approved": False,
            "critiques": [],
            "summary": f"CEO недоступен: {ceo_raw[:300]}",
        }
    else:
        ceo_parsed = _parse_expert_response(ceo_raw, "CEO")

    reviews.append({
        "role": "CEO",
        "name": ceo["name"],
        "response": ceo_parsed,
    })

    logger.info("Board review: CEO done in %.2fs, verdict=%s", elapsed_ceo, ceo_parsed.get("approved"))

    # Aggregate
    all_critiques = []
    for r in reviews:
        for c in r["response"].get("critiques", []):
            c["from_expert"] = r["role"]
            all_critiques.append(c)

    critical = sum(1 for c in all_critiques if c.get("severity") == "high")
    elapsed_total = round(time.monotonic() - t0, 2)

    logger.info(
        "Board review complete: %.2fs, approved=%s, critical=%d, total=%d",
        elapsed_total, ceo_parsed.get("approved", False) and critical == 0,
        critical, len(all_critiques),
    )

    return {
        "reviews": [
            {
                "role": r["role"],
                "name": r["name"],
                "approved": r["response"].get("approved", False),
                "summary": r["response"].get("summary", ""),
                "critiques": r["response"].get("critiques", []),
            }
            for r in reviews
        ],
        "consensus": {
            "approved": ceo_parsed.get("approved", False) and critical == 0,
            "critical_issues": critical,
            "total_critiques": len(all_critiques),
        },
        "timing": {
            "parallel_sec": elapsed_parallel,
            "ceo_sec": elapsed_ceo,
            "total_sec": elapsed_total,
        },
    }


_BOARD_RESULTS_DIR = Path("data/board_reviews")


def _board_review_worker(report_file: str, report_text: str, company_name: str):
    """Background worker: runs board review and saves result to JSON file."""
    _BOARD_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    result_path = _BOARD_RESULTS_DIR / f"{report_file}.json"

    # Write "running" status
    result_path.write_text(json.dumps({
        "status": "running",
        "report_file": report_file,
        "company_name": company_name,
        "started_at": time.time(),
    }, ensure_ascii=False), encoding="utf-8")

    try:
        result = _run_board_review_on_text(report_text, company_name)
        output = {
            "status": "done",
            "ok": True,
            "report_file": report_file,
            "company_name": company_name,
            "finished_at": time.time(),
            **result,
        }
    except Exception as e:
        logger.exception("Board review worker failed for %s", report_file)
        output = {
            "status": "error",
            "ok": False,
            "report_file": report_file,
            "error": str(e)[:500],
            "finished_at": time.time(),
        }

    result_path.write_text(
        json.dumps(output, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    logger.info("Board review saved: %s → %s", report_file, output.get("status"))


@router.post("/api/board-review")
async def admin_board_review(request: Request):
    """Launch Board of Directors review in background.

    Body: {"report_file": "report_XXX.html", "company_name": "..."}
    Returns immediately. Poll GET /admin/api/board-review/{filename} for result.
    """
    if not _check_admin(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    report_file = body.get("report_file", "")
    company_name = body.get("company_name", "")

    if not report_file:
        return JSONResponse({"error": "report_file required"}, status_code=400)

    # Security: only allow filenames, no path traversal
    if "/" in report_file or "\\" in report_file or ".." in report_file:
        return JSONResponse({"error": "Invalid filename"}, status_code=400)

    reports_dir = Path("app/storage/reports")
    report_path = reports_dir / report_file
    if not report_path.exists():
        return JSONResponse({"error": f"Report not found: {report_file}"}, status_code=404)

    html_content = report_path.read_text(encoding="utf-8")
    report_text = _html_to_text(html_content)

    logger.info(
        "Board review requested (background): file=%s, company=%s, text_len=%d",
        report_file, company_name, len(report_text),
    )

    # Launch in background thread
    import threading
    t = threading.Thread(
        target=_board_review_worker,
        args=(report_file, report_text, company_name),
        daemon=True,
    )
    t.start()

    return {
        "ok": True,
        "message": "Board review started in background",
        "poll_url": f"/admin/api/board-review/{report_file}?token={ADMIN_TOKEN}",
    }


@router.get("/api/board-review/{report_file}")
async def admin_board_review_result(report_file: str, request: Request):
    """Get board review result (poll until status=done)."""
    if not _check_admin(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    result_path = _BOARD_RESULTS_DIR / f"{report_file}.json"
    if not result_path.exists():
        return JSONResponse({"error": "No board review found for this report"}, status_code=404)

    data = json.loads(result_path.read_text(encoding="utf-8"))
    return data


@router.get("/api/reports")
async def admin_list_reports(request: Request):
    """List all report files in storage with enriched metadata."""
    if not _check_admin(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    reports_dir = Path("app/storage/reports")
    if not reports_dir.exists():
        return {"ok": True, "reports": []}

    # Build lookup: session_id -> metrics record for cost/quality data
    from app.metrics import _read_all_records
    all_metrics = _read_all_records()
    metrics_by_session: dict[str, dict] = {}
    for rec in all_metrics:
        sid = rec.get("session_id", "")
        if sid:
            metrics_by_session[sid] = rec

    # Build lookup: report_filename -> board review quality score
    board_scores: dict[str, float | None] = {}
    if _BOARD_RESULTS_DIR.exists():
        for bp in _BOARD_RESULTS_DIR.glob("*.json"):
            try:
                bd = json.loads(bp.read_text(encoding="utf-8"))
                if bd.get("status") == "done":
                    report_file = bd.get("report_file", bp.stem)
                    consensus = bd.get("consensus", {})
                    total_critiques = consensus.get("total_critiques", 0)
                    critical = consensus.get("critical_issues", 0)
                    approved = consensus.get("approved", False)
                    # Quality score: 100 if approved with 0 critiques, deduct per critique
                    score = 100 - (critical * 15) - ((total_critiques - critical) * 5)
                    if not approved:
                        score = min(score, 70)
                    board_scores[report_file] = max(0, min(100, score))
            except (json.JSONDecodeError, OSError):
                pass

    # Match reports to sessions by session_id in filename (report_{sid}.html)
    store = get_store()
    session_ids = store.list_sessions()

    reports = []
    for f in sorted(reports_dir.glob("report_*.html"), key=lambda p: p.stat().st_mtime, reverse=True):
        stat = f.stat()
        # Extract session_id from filename: report_{sid}.html
        sid = f.stem.replace("report_", "", 1) if f.stem.startswith("report_") else ""

        # Try to get company name from session data
        company_name = ""
        llm_cost = None
        llm_calls = None

        # Read persistent .meta.json (survives restarts)
        meta_path = f.with_suffix(".meta.json")
        meta_data: dict = {}
        if meta_path.exists():
            try:
                meta_data = json.loads(meta_path.read_text(encoding="utf-8"))
                company_name = meta_data.get("company", "")
                llm_cost = meta_data.get("llm_cost_usd")
                llm_calls = meta_data.get("llm_calls")
            except (json.JSONDecodeError, OSError):
                pass

        # Fallback: check session store (for reports without .meta.json)
        if not company_name and sid:
            s = store.get(sid)
            if s:
                confirmed = s.get("data", {}).get("confirmed_company", {})
                company_info = s.get("data", {}).get("company_info", {})
                company_name = confirmed.get("name") or company_info.get("name", "")

        # Fallback: check metrics for cost (for reports without .meta.json)
        if llm_cost is None and sid and sid in metrics_by_session:
            m = metrics_by_session[sid]
            llm_cost = m.get("total_cost_usd")
            llm_calls = m.get("llm_calls")
            if not company_name:
                company_name = m.get("company", "")

        # Quality score: prefer .meta.json, fallback to board review
        quality_score = meta_data.get("quality_score") or board_scores.get(f.name)

        reports.append({
            "filename": f.name,
            "session_id": sid,
            "company": company_name or "—",
            "size_kb": round(stat.st_size / 1024, 1),
            "modified": _format_time(stat.st_mtime),
            "modified_ts": stat.st_mtime,
            "llm_cost_usd": round(llm_cost, 4) if llm_cost is not None else None,
            "llm_calls": llm_calls,
            "quality_score": quality_score,
        })

    return {"ok": True, "reports": reports}


def _parse_reset(val: str | None) -> str | None:
    """Parse ratelimit reset header to human-readable string."""
    if not val:
        return None
    # Could be seconds like "42" or duration like "1m30s"
    val = val.strip()
    if val.endswith("s") and val[:-1].replace(".", "").isdigit():
        s = float(val[:-1])
    elif val.replace(".", "").isdigit():
        s = float(val)
    else:
        return val  # already human-readable like "1m30s"
    if s < 60:
        return f"{int(s)}s"
    m = int(s) // 60
    sec = int(s) % 60
    return f"{m}m{sec}s" if sec else f"{m}m"


def _format_k(n: int | str | None) -> str | None:
    """Format large numbers: 40000 -> '40K'."""
    if n is None:
        return None
    try:
        n = int(n)
    except (ValueError, TypeError):
        return str(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1000:
        return f"{n / 1000:.0f}K"
    return str(n)


@router.get("/api/monitoring")
async def admin_monitoring(request: Request):
    """Probe all external APIs and return status."""
    if not _check_admin(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    import httpx

    TIMEOUT = 12

    async def probe(name: str, category: str, coro) -> dict:
        t0 = time.monotonic()
        try:
            result = await asyncio.wait_for(coro, timeout=TIMEOUT)
            ms = int((time.monotonic() - t0) * 1000)
            return {"name": name, "category": category, "ok": True, "ms": ms, **result}
        except asyncio.TimeoutError:
            ms = int((time.monotonic() - t0) * 1000)
            return {"name": name, "category": category, "ok": False, "ms": ms, "details": f"Timeout ({TIMEOUT}s)"}
        except Exception as e:
            ms = int((time.monotonic() - t0) * 1000)
            return {"name": name, "category": category, "ok": False, "ms": ms, "details": str(e)[:300]}

    async def probe_openai():
        key = os.environ.get("OPENAI_API_KEY") or os.environ.get("FALLBACK_LLM_API_KEY", "")
        if not key:
            return {"details": "Ключ не задан", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get("https://api.openai.com/v1/models", headers={"Authorization": f"Bearer {key}"}, timeout=TIMEOUT)
            r.raise_for_status()
            models = r.json().get("data", [])
            h = r.headers
            quota = {
                "requests": None, "tokens": None, "reset": None, "balance": None, "static": None,
            }
            rl_req_lim = h.get("x-ratelimit-limit-requests")
            rl_req_rem = h.get("x-ratelimit-remaining-requests")
            if rl_req_lim and rl_req_rem:
                quota["requests"] = {"remaining": int(rl_req_rem), "limit": int(rl_req_lim)}
            rl_tok_lim = h.get("x-ratelimit-limit-tokens")
            rl_tok_rem = h.get("x-ratelimit-remaining-tokens")
            if rl_tok_lim and rl_tok_rem:
                quota["tokens"] = {"remaining": int(rl_tok_rem), "limit": int(rl_tok_lim)}
            quota["reset"] = _parse_reset(h.get("x-ratelimit-reset-requests"))
            return {"details": f"{len(models)} моделей", "quota": quota}

    async def probe_gemini():
        key = os.environ.get("GEMINI_API_KEY", "")
        if not key:
            return {"details": "Ключ не задан", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get(f"https://generativelanguage.googleapis.com/v1beta/models?key={key}", timeout=TIMEOUT)
            r.raise_for_status()
            models = r.json().get("models", [])
            return {"details": f"{len(models)} моделей", "quota": {
                "requests": None, "tokens": None, "reset": None, "balance": None,
                "static": "1500 RPD / 15 RPM (free tier)",
            }}

    async def probe_anthropic():
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not key:
            return {"details": "Ключ не задан", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get("https://api.anthropic.com/v1/models", headers={
                "x-api-key": key, "anthropic-version": "2023-06-01",
            }, timeout=TIMEOUT)
            h = r.headers
            quota = {
                "requests": None, "tokens": None, "reset": None, "balance": None, "static": None,
            }
            # Anthropic uses both x-ratelimit-* and anthropic-ratelimit-* prefixes
            rl_req_lim = h.get("anthropic-ratelimit-requests-limit") or h.get("x-ratelimit-limit-requests")
            rl_req_rem = h.get("anthropic-ratelimit-requests-remaining") or h.get("x-ratelimit-remaining-requests")
            if rl_req_lim and rl_req_rem:
                quota["requests"] = {"remaining": int(rl_req_rem), "limit": int(rl_req_lim)}
            rl_tok_lim = h.get("anthropic-ratelimit-tokens-limit") or h.get("x-ratelimit-limit-tokens")
            rl_tok_rem = h.get("anthropic-ratelimit-tokens-remaining") or h.get("x-ratelimit-remaining-tokens")
            if rl_tok_lim and rl_tok_rem:
                quota["tokens"] = {"remaining": int(rl_tok_rem), "limit": int(rl_tok_lim)}
            quota["reset"] = _parse_reset(
                h.get("anthropic-ratelimit-requests-reset") or h.get("x-ratelimit-reset-requests")
            )
            if r.status_code == 200:
                models = r.json().get("data", [])
                return {"details": f"{len(models)} моделей", "quota": quota}
            return {"details": f"HTTP {r.status_code}", "quota": quota}

    async def probe_fns():
        key = os.environ.get("FNS_API_KEY", "")
        if not key:
            return {"details": "Ключ не задан", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get(f"https://api-fns.ru/api/egr?req=7707083893&key={key}", timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                items = data.get("items", [])
                return {"details": f"OK ({len(items)} записей)", "quota": {
                    "requests": None, "tokens": None, "reset": None, "balance": None,
                    "static": "100 req/day",
                }}
            return {"details": f"HTTP {r.status_code}", "ok": False, "quota": None}

    async def probe_hh():
        token = os.environ.get("HH_APP_TOKEN", "")
        if not token:
            return {"details": "Токен не задан", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get("https://api.hh.ru/vacancies", headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": "EspacePlatform/1.0 (n.a.ledovskoy@gmail.com)",
            }, params={"text": "python", "per_page": 1}, timeout=TIMEOUT)
            h = r.headers
            quota = {
                "requests": None, "tokens": None, "reset": None, "balance": None, "static": None,
            }
            rl_lim = h.get("x-ratelimit-limit")
            rl_rem = h.get("x-ratelimit-remaining")
            if rl_lim and rl_rem:
                quota["requests"] = {"remaining": int(rl_rem), "limit": int(rl_lim)}
            quota["reset"] = _parse_reset(h.get("x-ratelimit-reset"))
            if r.status_code == 200:
                found = r.json().get("found", 0)
                return {"details": f"OK ({found:,} вакансий)", "quota": quota}
            return {"details": f"HTTP {r.status_code}", "ok": r.status_code == 403, "quota": quota}

    async def probe_twogis():
        key = os.environ.get("TWOGIS_API_KEY", "")
        if not key:
            return {"details": "Ключ не задан", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get("https://catalog.api.2gis.com/3.0/items", params={
                "q": "test", "type": "branch", "key": key,
                "fields": "items.point", "page_size": 1,
            }, timeout=TIMEOUT)
            if r.status_code == 200:
                total = r.json().get("result", {}).get("total", 0)
                return {"details": f"OK ({total:,} результатов)", "quota": {
                    "requests": None, "tokens": None, "reset": None, "balance": None,
                    "static": "10 req/sec",
                }}
            return {"details": f"HTTP {r.status_code}", "ok": False, "quota": None}

    async def probe_keyso():
        token = os.environ.get(
            "KEYSO_API_TOKEN",
            "69b563282aa8e2.426513828814d70e40a4a42b1235ab9278ba2bda",
        )
        if not token:
            return {"details": "Key not set", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get("https://api.keys.so/report/simple/domain_dashboard", params={
                "domain": "sber.ru", "base": "msk",
            }, headers={"X-Keyso-TOKEN": token, "Accept": "application/json", "User-Agent": "BSR-Pipeline/1.0"}, timeout=TIMEOUT)
            static_quota = {
                "requests": None, "tokens": None, "reset": None, "balance": None,
                "static": "10 req/10s",
            }
            if r.status_code == 200:
                data = r.json()
                if data.get("name"):
                    vis = data.get("vis", 0)
                    dr = data.get("dr", 0)
                    return {"details": f"OK (test: DR={dr}, vis={vis})", "quota": static_quota}
                return {"details": "OK (empty)", "quota": static_quota}
            if r.status_code in (401, 403):
                return {"details": f"Token invalid ({r.status_code})", "ok": False, "quota": None}
            return {"details": f"HTTP {r.status_code}", "ok": False, "quota": None}

    async def probe_checko():
        key = os.environ.get("CHECKO_API_KEY", "dHL2dcu0gcn3Hqfz")
        if not key:
            return {"details": "Ключ не задан", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get(f"https://api.checko.ru/v2/company", params={
                "key": key, "inn": "7707083893",
            }, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                meta = data.get("meta", {})
                company = data.get("data", {})
                name = company.get("\u041d\u0430\u0438\u043c\u0421\u043e\u043a\u0440", "?")
                today_count = meta.get("today_request_count", 0)
                balance_raw = meta.get("balance", 0)
                try:
                    balance_val = float(balance_raw)
                    balance_str = f"{balance_val:.0f}\u20bd"
                except (ValueError, TypeError):
                    balance_str = str(balance_raw)
                return {"details": f"OK ({name})", "quota": {
                    "requests": {"remaining": None, "limit": None, "today": today_count},
                    "tokens": None, "reset": None,
                    "balance": balance_str, "static": None,
                }}
            return {"details": f"HTTP {r.status_code}", "ok": False, "quota": None}

    async def probe_telegram():
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        if not token:
            return {"details": "Токен не задан", "ok": False, "quota": None}
        async with httpx.AsyncClient() as c:
            r = await c.get(f"https://api.telegram.org/bot{token}/getMe", timeout=TIMEOUT)
            r.raise_for_status()
            bot = r.json().get("result", {})
            return {"details": f"@{bot.get('username', '?')}", "quota": {
                "requests": None, "tokens": None, "reset": None, "balance": None,
                "static": "30 msg/sec",
            }}

    async def probe_bsr_self():
        async with httpx.AsyncClient() as c:
            r = await c.get("http://localhost:8083/api/health", timeout=5)
            if r.status_code == 200:
                data = r.json()
                uptime = data.get("uptime_sec", 0)
                active = data.get("active_sessions", 0)
                # Format uptime
                if uptime >= 86400:
                    up_str = f"{uptime // 86400}d {(uptime % 86400) // 3600}h"
                elif uptime >= 3600:
                    up_str = f"{uptime // 3600}h {(uptime % 3600) // 60}m"
                else:
                    up_str = f"{uptime // 60}m"
                return {"details": f"v{data.get('version', '?')}", "quota": {
                    "requests": None, "tokens": None, "reset": None, "balance": None,
                    "static": f"uptime: {up_str}, active: {active}",
                }}
            return {"details": f"HTTP {r.status_code}", "ok": False, "quota": None}

    tasks = [
        probe("OpenAI / GPT", "LLM", probe_openai()),
        probe("Google Gemini", "LLM", probe_gemini()),
        probe("Anthropic Claude", "LLM", probe_anthropic()),
        probe("FNS API", "Data", probe_fns()),
        probe("HeadHunter", "Data", probe_hh()),
        probe("2GIS", "Data", probe_twogis()),
        probe("Keys.so", "SEO", probe_keyso()),
        probe("Checko.ru", "Data", probe_checko()),
        probe("Telegram Bot", "Infra", probe_telegram()),
        probe("BSR (self)", "Infra", probe_bsr_self()),
    ]
    results = await asyncio.gather(*tasks)

    return {"ok": True, "results": list(results), "timestamp": time.time()}


@router.get("/diag-html")
async def admin_diag_html(request: Request):
    """Debug: check _DASHBOARD_HTML encoding on this server."""
    html = _DASHBOARD_HTML
    length = len(html)
    # Find surrogates
    surrogates = []
    for i, ch in enumerate(html):
        if 0xD800 <= ord(ch) <= 0xDFFF:
            surrogates.append({"pos": i, "code": f"U+{ord(ch):04X}", "context": repr(html[max(0,i-5):i+5])})
    # Try encoding
    try:
        encoded = html.encode("utf-8")
        enc_ok = True
        enc_len = len(encoded)
        enc_err = None
    except UnicodeEncodeError as e:
        enc_ok = False
        enc_len = 0
        enc_err = str(e)
    return {
        "length": length,
        "surrogates": surrogates,
        "encode_ok": enc_ok,
        "encode_len": enc_len,
        "encode_error": enc_err,
        "chars_12255_12265": repr(html[12255:12265]) if length > 12265 else "too short",
    }


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def admin_page(request: Request):
    """Admin dashboard HTML."""
    from starlette.responses import Response
    if not _check_admin(request):
        body = _LOGIN_HTML_PATH.read_bytes() if _LOGIN_HTML_PATH.exists() else b"<h1>Login</h1>"
        return Response(content=body, media_type="text/html; charset=utf-8")
    body = _DASHBOARD_HTML_PATH.read_bytes() if _DASHBOARD_HTML_PATH.exists() else b"<h1>Dashboard not found</h1>"
    return Response(content=body, media_type="text/html; charset=utf-8")


# ── HTML files (loaded from files to avoid VPS LANG=C surrogate issues) ──

import pathlib as _pathlib
_LOGIN_HTML_PATH = _pathlib.Path(__file__).parent / "admin_login.html"
_DASHBOARD_HTML_PATH = _pathlib.Path(__file__).parent / "admin_dashboard.html"
_DASHBOARD_HTML = _DASHBOARD_HTML_PATH.read_text(encoding="utf-8") if _DASHBOARD_HTML_PATH.exists() else "<h1>admin_dashboard.html not found</h1>"

# Old inline dashboard HTML removed — see admin_dashboard.html
