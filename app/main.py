"""FastAPI app: interactive multi-step business analysis pipeline."""

from __future__ import annotations

import os
import uuid
import traceback
import json
import threading
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import REPORTS_DIR, BusinessType

load_dotenv()

app = FastAPI(title="Бизнес-анализ 360", version="0.3.0")

REPORTS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR)), name="reports")

# ── Session storage (in-memory, fine for MVP) ──

sessions: dict[str, dict[str, Any]] = {}


def _new_session() -> str:
    sid = uuid.uuid4().hex[:12]
    sessions[sid] = {"status": "created", "events": [], "data": {}}
    return sid


def _push_event(sid: str, event: str, data: Any = None):
    if sid in sessions:
        sessions[sid]["events"].append({"event": event, "data": data})


# ── Routes ──

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(content=LANDING_HTML)


@app.post("/api/start")
async def start_session(request: Request):
    """Start a new analysis session. Returns session_id."""
    body = await request.json()
    url = (body.get("url") or "").strip()
    if not url:
        return JSONResponse({"ok": False, "error": "URL не указан"}, status_code=400)
    if not url.startswith("http"):
        url = "https://" + url

    sid = _new_session()
    sessions[sid]["data"]["url"] = url
    sessions[sid]["status"] = "scraping"

    # Run steps 1-3 in background thread
    thread = threading.Thread(target=_run_initial_steps, args=(sid, url), daemon=True)
    thread.start()

    return {"ok": True, "session_id": sid}


@app.get("/api/events/{sid}")
async def stream_events(sid: str):
    """SSE endpoint: stream events to frontend."""
    if sid not in sessions:
        return JSONResponse({"error": "Session not found"}, status_code=404)

    import asyncio

    async def event_generator():
        last_idx = 0
        while True:
            events = sessions.get(sid, {}).get("events", [])
            while last_idx < len(events):
                ev = events[last_idx]
                yield f"event: {ev['event']}\ndata: {json.dumps(ev.get('data', {}), ensure_ascii=False)}\n\n"
                last_idx += 1
                # If terminal event, stop
                if ev["event"] in ("done", "error", "waiting_company", "waiting_competitors"):
                    pass  # keep connection open for further events
            status = sessions.get(sid, {}).get("status", "")
            if status in ("done", "error"):
                # Final check for any remaining events
                events = sessions.get(sid, {}).get("events", [])
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
    if sid not in sessions:
        return JSONResponse({"error": "Session not found"}, status_code=404)

    body = await request.json()
    sessions[sid]["data"]["confirmed_company"] = body
    sessions[sid]["status"] = "finding_competitors"

    # Continue pipeline
    thread = threading.Thread(target=_run_competitor_steps, args=(sid,), daemon=True)
    thread.start()

    return {"ok": True}


@app.post("/api/confirm-competitors/{sid}")
async def confirm_competitors(sid: str, request: Request):
    """User confirms/edits competitor list."""
    if sid not in sessions:
        return JSONResponse({"error": "Session not found"}, status_code=404)

    body = await request.json()
    sessions[sid]["data"]["confirmed_competitors"] = body.get("competitors", [])
    sessions[sid]["status"] = "analyzing"

    # Continue pipeline
    thread = threading.Thread(target=_run_analysis_steps, args=(sid,), daemon=True)
    thread.start()

    return {"ok": True}


# ── Background pipeline steps ──

def _run_initial_steps(sid: str, url: str):
    """Steps 1-3: Scrape → Identify → FNS lookup."""
    try:
        # Step 1: Scrape
        _push_event(sid, "step", {"num": 1, "status": "active", "text": "Загрузка и скрапинг сайта..."})
        from app.pipeline.steps.step1_scrape import run as scrape
        scraped = scrape(url)
        sessions[sid]["data"]["scraped"] = scraped
        method_hint = " (Scrapling fallback)" if scraped.get("scrape_method") == "scrapling" else ""
        _push_event(sid, "step", {"num": 1, "status": "done", "text": f"Сайт загружен{method_hint}: {scraped.get('title', '')}"})

        # Step 2: Identify company
        _push_event(sid, "step", {"num": 2, "status": "active", "text": "Определяю компанию..."})
        from app.pipeline.steps.step2_identify import run as identify
        company_info = identify(scraped)
        sessions[sid]["data"]["company_info"] = company_info
        _push_event(sid, "step", {"num": 2, "status": "done", "text": f"Компания: {company_info.get('name', '?')}"})

        # Step 3: FNS lookup
        _push_event(sid, "step", {"num": 3, "status": "active", "text": "Поиск в ФНС..."})
        fns_ok = False
        try:
            from app.pipeline.steps.step3_fns import run as fns_lookup
            fns_data = fns_lookup(company_info)
            sessions[sid]["data"]["fns_data"] = fns_data
            fns_ok = bool(fns_data.get("fns_company", {}).get("inn"))
        except Exception as e:
            sessions[sid]["data"]["fns_data"] = {"fns_error": str(e)}

        if fns_ok:
            fc = fns_data["fns_company"]
            _push_event(sid, "step", {"num": 3, "status": "done",
                "text": f"ФНС: {fc.get('name', '')} | ИНН {fc.get('inn', '')}"})
        else:
            _push_event(sid, "step", {"num": 3, "status": "warning",
                "text": "ФНС: юрлицо не найдено автоматически"})

        # PAUSE: send data to frontend for user verification
        sessions[sid]["status"] = "waiting_company"
        _push_event(sid, "waiting_company", {
            "company_info": company_info,
            "fns_data": sessions[sid]["data"].get("fns_data", {}),
        })

    except Exception as e:
        sessions[sid]["status"] = "error"
        _push_event(sid, "error", {"message": str(e), "details": traceback.format_exc()[-500:]})


def _run_competitor_steps(sid: str):
    """Step 4: Find competitors."""
    try:
        data = sessions[sid]["data"]
        confirmed = data.get("confirmed_company", {})

        # If user provided INN, re-fetch FNS
        if confirmed.get("inn") and confirmed["inn"] != data.get("fns_data", {}).get("fns_company", {}).get("inn"):
            _push_event(sid, "step", {"num": 3, "status": "active", "text": f"Обновляю данные ФНС по ИНН {confirmed['inn']}..."})
            try:
                from app.pipeline.steps.step3_fns import run as fns_lookup
                fns_data = fns_lookup(data.get("company_info", {}), confirmed_inn=confirmed["inn"])
                data["fns_data"] = fns_data
                _push_event(sid, "step", {"num": 3, "status": "done", "text": "Данные ФНС обновлены"})
            except Exception:
                pass

        # Merge confirmed data into company_info
        company_info = data.get("company_info", {})
        for key in ("name", "legal_name", "inn", "address", "business_type_guess"):
            if confirmed.get(key):
                company_info[key] = confirmed[key]
        data["company_info"] = company_info

        # Step 4: Find competitors
        _push_event(sid, "step", {"num": 4, "status": "active", "text": "Ищу конкурентов (GPT-5.2 Pro)..."})
        from app.pipeline.steps.step4_competitors import run as find_competitors
        comp_result = find_competitors(
            data.get("scraped", {}),
            company_info,
            data.get("fns_data", {}),
        )
        data["market_info"] = comp_result
        _push_event(sid, "step", {"num": 4, "status": "done",
            "text": f"Найдено {len(comp_result.get('competitors', []))} конкурентов"})

        # PAUSE: send competitors for user editing
        sessions[sid]["status"] = "waiting_competitors"
        _push_event(sid, "waiting_competitors", {
            "market_name": comp_result.get("market_name", ""),
            "competitors": comp_result.get("competitors", []),
        })

    except Exception as e:
        sessions[sid]["status"] = "error"
        _push_event(sid, "error", {"message": str(e), "details": traceback.format_exc()[-500:]})


def _run_analysis_steps(sid: str):
    """Steps 1b, 1c, 5, 2a, 2b, 6: Extended v2.0 pipeline."""
    try:
        data = sessions[sid]["data"]
        confirmed_competitors = data.get("confirmed_competitors", [])
        company_info = data.get("company_info", {})
        bt = company_info.get("business_type_guess", "")

        # Step 1b: Marketplace analysis (conditional)
        marketplace_data = None
        if bt in ("B2C_PRODUCT", "PLATFORM", "B2B_B2C_HYBRID"):
            _push_event(sid, "step", {"num": "1b", "status": "active", "text": "Анализ маркетплейсов..."})
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

        # Step 1c: Deep models (lifecycle + channels)
        deep_models = None
        _push_event(sid, "step", {"num": "1c", "status": "active", "text": "Жизненный цикл и каналы продаж..."})
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

        # Step 5: Deep analysis with GPT-5.2 Pro
        _push_event(sid, "step", {"num": 5, "status": "active", "text": "Глубокий анализ (GPT-5.2 Pro)..."})
        from app.pipeline.steps.step5_deep_analysis import run as deep_analysis
        report_data = deep_analysis(
            scraped=data.get("scraped", {}),
            company_info=company_info,
            fns_data=data.get("fns_data", {}),
            competitors=confirmed_competitors,
            market_info=data.get("market_info", {}),
            deep_models=deep_models,
            marketplace_data=marketplace_data,
        )
        _push_event(sid, "step", {"num": 5, "status": "done", "text": "Анализ завершён"})

        # Sanitize LLM output first
        report_data = _sanitize_llm_output(report_data)

        # Step 2a: Verification (pure Python)
        _push_event(sid, "step", {"num": "2a", "status": "active", "text": "Верификация расчётов..."})
        try:
            from app.pipeline.steps.step2a_verify import run as verify
            report_data = verify(report_data)
            corrections = sum(1 for f in report_data.get("factcheck", [])
                            if isinstance(f, dict) and f.get("correction"))
            _push_event(sid, "step", {"num": "2a", "status": "done",
                "text": f"Верификация: {corrections} корректировок"})
        except Exception as e:
            _push_event(sid, "step", {"num": "2a", "status": "warning", "text": f"Верификация: {e}"})

        # Step 2b: Relevance gate (pure Python)
        _push_event(sid, "step", {"num": "2b", "status": "active", "text": "Section Relevance Gate..."})
        try:
            from app.pipeline.steps.step2b_relevance_gate import run as relevance_gate
            report_data = relevance_gate(report_data)
            gates = report_data.get("section_gates", {})
            disabled = sum(1 for v in gates.values() if not v)
            _push_event(sid, "step", {"num": "2b", "status": "done",
                "text": f"Gate: {disabled} секций отключено"})
        except Exception as e:
            _push_event(sid, "step", {"num": "2b", "status": "warning", "text": f"Gate: {e}"})

        # Step 6: Build report
        _push_event(sid, "step", {"num": 6, "status": "active", "text": "Сборка отчёта..."})

        from app.models import ReportData
        from app.report.builder import save_report

        rd = ReportData(**report_data)
        filename = f"report_{uuid.uuid4().hex[:8]}.html"
        path = save_report(rd, filename=filename)
        size_kb = round(path.stat().st_size / 1024)

        _push_event(sid, "step", {"num": 6, "status": "done", "text": f"Отчёт собран ({size_kb} KB)"})

        # Done!
        sessions[sid]["status"] = "done"
        _push_event(sid, "done", {
            "url": f"/reports/{filename}",
            "size_kb": size_kb,
            "company": report_data.get("company", {}).get("name", ""),
        })

    except Exception as e:
        sessions[sid]["status"] = "error"
        _push_event(sid, "error", {"message": str(e), "details": traceback.format_exc()[-500:]})


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

    # --- digital.social_accounts ---
    digital = d.get("digital") or {}
    if "social_accounts" in digital:
        digital["social_accounts"] = [
            acc for acc in digital["social_accounts"]
            if isinstance(acc, dict) and acc.get("platform")
        ]
    d["digital"] = digital

    # --- competitors ---
    for c in d.get("competitors") or []:
        if not isinstance(c, dict):
            continue
        if not c.get("name"):
            c["name"] = "Неизвестный"
        rs = c.get("radar_scores") or {}
        c["radar_scores"] = {k: (v if v is not None else 5) for k, v in rs.items()}
        tl = str(c.get("threat_level", "med")).lower()
        c["threat_level"] = tl if tl in ("high", "med", "low") else "med"

    # --- financials ---
    d["financials"] = [f for f in (d.get("financials") or []) if isinstance(f, dict) and f.get("year")]

    # --- recommendations ---
    for r in d.get("recommendations") or []:
        if isinstance(r, dict):
            if not r.get("title"):
                r["title"] = "Рекомендация"
            if not r.get("description"):
                r["description"] = r.get("title", "")

    # --- scenarios ---
    for sc in d.get("scenarios") or []:
        if isinstance(sc, dict):
            metrics = sc.get("metrics") or {}
            sc["metrics"] = {
                k: float(v) if v is not None else 0.0
                for k, v in metrics.items()
                if isinstance(v, (int, float, type(None)))
            }

    # --- market_share ---
    ms = d.get("market_share") or {}
    d["market_share"] = {k: float(v) if v is not None else 0.0 for k, v in ms.items()}

    # --- opinions ---
    d["opinions"] = [o for o in (d.get("opinions") or []) if isinstance(o, dict) and o.get("author") and o.get("quote")]

    # --- founders ---
    d["founders"] = [f for f in (d.get("founders") or []) if isinstance(f, dict) and f.get("name")]

    # --- kpi_benchmarks ---
    d["kpi_benchmarks"] = [k for k in (d.get("kpi_benchmarks") or []) if isinstance(k, dict) and k.get("name")]

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


# ── Legacy endpoint (simple, no interactive) ──

@app.post("/api/analyze")
async def analyze_simple(request: Request):
    """Simple non-interactive endpoint (backward compat)."""
    body = await request.json()
    url = (body.get("url") or "").strip()
    if not url:
        return {"ok": False, "error": "URL не указан"}
    if not url.startswith("http"):
        url = "https://" + url

    try:
        from app.pipeline.steps.step1_scrape import run as scrape
        scraped = scrape(url)
    except Exception as e:
        return {"ok": False, "error": f"Ошибка скрапинга: {e}", "step": 1}

    try:
        from app.pipeline.llm_analyzer import analyze_with_llm
        report_data = analyze_with_llm(scraped)
    except Exception as e:
        return {"ok": False, "error": f"Ошибка AI: {e}", "step": 2}

    report_data = _sanitize_llm_output(report_data)

    try:
        from app.models import ReportData
        from app.report.builder import save_report
        rd = ReportData(**report_data)
        filename = f"report_{uuid.uuid4().hex[:8]}.html"
        path = save_report(rd, filename=filename)
        size_kb = round(path.stat().st_size / 1024)
    except Exception as e:
        return {"ok": False, "error": f"Ошибка сборки: {e}", "step": 3}

    return {"ok": True, "url": f"/reports/{filename}", "size_kb": size_kb,
            "company": report_data.get("company", {}).get("name", "")}


# ──────────────────────────────────────────────────────────
LANDING_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Анализ бизнеса 360</title>
<style>
:root{--bg:#101014;--card:#18181E;--border:#28283A;--text:#E4E4EE;--text2:#9898AA;--text3:#606070;--accent:#5B8DEF;--accent2:#7DACFF;--accent-bg:rgba(91,141,239,0.08);--accent-border:rgba(91,141,239,0.22);--red:#E05555;--green:#44C080;--orange:#E0A040}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:40px 20px}

.wrap{max-width:640px;width:100%}

/* Phase: URL input */
#phase-url{text-align:center;margin-top:15vh}
.logo{font-size:0.82em;color:var(--text3);text-transform:uppercase;letter-spacing:0.18em;margin-bottom:32px}
h1{color:var(--text);font-weight:600;font-size:1.9em;margin-bottom:10px;line-height:1.25}
h1 span{color:var(--accent)}
.sub{color:var(--text2);font-size:0.95em;margin-bottom:40px;line-height:1.6}
.input-row{display:flex;gap:10px;margin-bottom:12px}
.input-row input{flex:1;padding:14px 18px;background:var(--card);border:1.5px solid var(--border);border-radius:10px;color:var(--text);font-size:1em;font-family:inherit;transition:border-color 0.2s}
.input-row input:focus{outline:none;border-color:var(--accent)}
.input-row input::placeholder{color:var(--text3)}
.btn{padding:14px 28px;background:var(--accent);border:none;border-radius:10px;color:#fff;font-size:1em;font-weight:600;cursor:pointer;transition:all 0.2s;font-family:inherit;white-space:nowrap}
.btn:hover{background:#4A7CE0;transform:translateY(-1px)}
.btn:disabled{opacity:0.35;cursor:not-allowed;transform:none}
.btn-outline{background:transparent;border:1.5px solid var(--border);color:var(--text2)}
.btn-outline:hover{border-color:var(--accent);color:var(--accent);background:transparent}
.btn-sm{padding:8px 16px;font-size:0.85em;border-radius:8px}
.btn-red{background:var(--red)}
.btn-red:hover{background:#C04444}

/* Phase: Pipeline */
#phase-pipeline{display:none}
.pipeline-header{text-align:center;margin-bottom:24px}
.pipeline-header h2{font-size:1.3em;font-weight:500;margin-bottom:4px}
.pipeline-header .url-tag{color:var(--text3);font-size:0.85em}
.steps{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:6px 0;margin-bottom:20px}
.step{display:flex;align-items:center;gap:12px;padding:13px 22px;font-size:0.9em;color:var(--text3);transition:color 0.2s;border-bottom:1px solid rgba(40,40,58,0.4)}
.step:last-child{border-bottom:none}
.step.active{color:var(--accent)}
.step.done{color:var(--green)}
.step.fail{color:var(--red)}
.step.warning{color:var(--orange)}
.step-icon{width:24px;height:24px;border-radius:50%;border:2px solid currentColor;display:flex;align-items:center;justify-content:center;font-size:0.75em;flex-shrink:0;transition:all 0.2s}
.step.active .step-icon{animation:pulse 1.5s infinite}
.step.done .step-icon{background:var(--green);border-color:var(--green);color:var(--bg)}
.step.fail .step-icon{background:var(--red);border-color:var(--red);color:var(--bg)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.35}}

/* Interactive panels */
.panel{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:24px;margin-bottom:20px;display:none}
.panel h3{font-size:1.05em;font-weight:600;margin-bottom:16px;color:var(--accent2)}
.field{margin-bottom:14px}
.field label{display:block;font-size:0.8em;color:var(--text3);margin-bottom:5px;text-transform:uppercase;letter-spacing:0.04em}
.field input,.field select{width:100%;padding:10px 14px;background:var(--bg);border:1.5px solid var(--border);border-radius:8px;color:var(--text);font-size:0.92em;font-family:inherit}
.field input:focus,.field select:focus{outline:none;border-color:var(--accent)}
.field-row{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.fns-info{background:rgba(68,192,128,0.08);border:1px solid rgba(68,192,128,0.2);border-radius:10px;padding:14px 16px;margin-bottom:16px;font-size:0.88em;color:var(--green)}
.fns-warning{background:rgba(224,160,64,0.08);border:1px solid rgba(224,160,64,0.2);border-radius:10px;padding:14px 16px;margin-bottom:16px;font-size:0.88em;color:var(--orange)}

/* Competitor cards */
.comp-list{display:flex;flex-direction:column;gap:10px;margin-bottom:16px}
.comp-item{display:flex;align-items:center;gap:14px;background:var(--bg);border:1.5px solid var(--border);border-radius:10px;padding:14px 16px;transition:border-color 0.2s}
.comp-item.excluded{opacity:0.4;border-style:dashed}
.comp-item:not(.excluded):hover{border-color:var(--accent-border)}
.comp-toggle{width:22px;height:22px;border-radius:6px;border:2px solid var(--border);background:none;cursor:pointer;display:flex;align-items:center;justify-content:center;color:var(--green);font-size:0.9em;flex-shrink:0;transition:all 0.15s}
.comp-toggle.on{background:var(--green);border-color:var(--green);color:var(--bg)}
.comp-info{flex:1;min-width:0}
.comp-name{font-weight:600;font-size:0.95em;margin-bottom:2px}
.comp-desc{font-size:0.8em;color:var(--text3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.comp-threat{font-size:0.75em;font-weight:600;padding:3px 8px;border-radius:12px;flex-shrink:0}
.threat-high{background:rgba(224,85,85,0.12);color:var(--red)}
.threat-med{background:rgba(224,160,64,0.12);color:var(--orange)}
.threat-low{background:rgba(68,192,128,0.12);color:var(--green)}

/* Result */
.result{display:none;text-align:center;padding:40px 28px;background:var(--accent-bg);border:1px solid var(--accent-border);border-radius:14px}
.result h3{color:var(--accent2);font-weight:500;font-size:1.3em;margin-bottom:6px}
.result .company{color:var(--text2);font-size:0.9em;margin-bottom:20px}
.result a{display:inline-block;padding:14px 44px;background:var(--accent);color:#fff;font-weight:600;font-size:1.05em;border-radius:10px;text-decoration:none;transition:all 0.2s}
.result a:hover{transform:translateY(-1px);box-shadow:0 6px 24px rgba(91,141,239,0.3)}
.result .meta{color:var(--text3);font-size:0.78em;margin-top:14px}

.error{display:none;padding:14px 18px;background:rgba(224,85,85,0.08);border:1px solid rgba(224,85,85,0.2);border-radius:10px;color:var(--red);font-size:0.85em;margin-bottom:16px}
.again{display:inline-block;margin-top:16px;color:var(--text3);font-size:0.82em;cursor:pointer;text-decoration:underline;border:none;background:none;font-family:inherit}
.again:hover{color:var(--accent)}
</style>
</head>
<body>

<div class="wrap">
    <!-- Phase 1: URL Input -->
    <div id="phase-url">
        <div class="logo">Анализ бизнеса</div>
        <h1>Полный отчёт <span>за 2 минуты</span></h1>
        <p class="sub">Вставьте ссылку на сайт компании — мы найдём юрлицо, конкурентов и соберём отчёт с реальными данными</p>
        <div class="input-row">
            <input id="url" type="url" placeholder="https://example.com" autofocus
                   onkeydown="if(event.key==='Enter')startAnalysis()">
            <button class="btn" id="gobtn" onclick="startAnalysis()">Анализировать</button>
        </div>
    </div>

    <!-- Phase 2: Pipeline -->
    <div id="phase-pipeline">
        <div class="pipeline-header">
            <h2>Анализ</h2>
            <div class="url-tag" id="url-tag"></div>
        </div>

        <div class="steps">
            <div class="step" id="s1"><div class="step-icon">1</div><span>Загрузка сайта</span></div>
            <div class="step" id="s2"><div class="step-icon">2</div><span>Определение компании</span></div>
            <div class="step" id="s3"><div class="step-icon">3</div><span>Поиск в ФНС</span></div>
            <div class="step" id="s4"><div class="step-icon">4</div><span>Поиск конкурентов</span></div>
            <div class="step" id="s5"><div class="step-icon">5</div><span>Глубокий анализ</span></div>
            <div class="step" id="s6"><div class="step-icon">6</div><span>Сборка отчёта</span></div>
        </div>

        <!-- Panel: Verify Company -->
        <div class="panel" id="panel-company">
            <h3>Подтвердите компанию</h3>
            <div id="fns-status"></div>
            <div class="field-row">
                <div class="field">
                    <label>Название</label>
                    <input id="c-name" type="text">
                </div>
                <div class="field">
                    <label>ИНН</label>
                    <input id="c-inn" type="text" placeholder="10 или 12 цифр">
                </div>
            </div>
            <div class="field-row">
                <div class="field">
                    <label>Юрлицо</label>
                    <input id="c-legal" type="text" placeholder='ООО "..."'>
                </div>
                <div class="field">
                    <label>Тип бизнеса</label>
                    <select id="c-type">
                        <option value="B2C_SERVICE">B2C Услуги</option>
                        <option value="B2C_PRODUCT">B2C Товары</option>
                        <option value="B2B_SERVICE">B2B Услуги</option>
                        <option value="B2B_PRODUCT">B2B Товары</option>
                        <option value="PLATFORM">Платформа</option>
                        <option value="B2B_B2C_HYBRID">B2B+B2C Гибрид</option>
                    </select>
                </div>
            </div>
            <div class="field">
                <label>Адрес</label>
                <input id="c-address" type="text">
            </div>
            <div style="display:flex;gap:10px;margin-top:8px">
                <button class="btn" onclick="confirmCompany()">Подтвердить и продолжить</button>
            </div>
        </div>

        <!-- Panel: Edit Competitors -->
        <div class="panel" id="panel-competitors">
            <h3>Конкуренты <span id="market-name" style="font-weight:400;color:var(--text3);font-size:0.85em"></span></h3>
            <p style="font-size:0.85em;color:var(--text2);margin-bottom:16px">Уберите нерелевантных конкурентов нажатием на чекбокс</p>
            <div class="comp-list" id="comp-list"></div>
            <button class="btn" onclick="confirmCompetitors()">Подтвердить и запустить анализ</button>
        </div>

        <div class="error" id="error"></div>

        <div class="result" id="result">
            <h3>Отчёт готов</h3>
            <div class="company" id="rcompany"></div>
            <a id="rlink" href="#" target="_blank">Открыть отчёт</a>
            <div class="meta" id="rmeta"></div>
            <button class="again" onclick="location.reload()">Проанализировать другую компанию</button>
        </div>
    </div>
</div>

<script>
var SID = null;
var evtSource = null;
var competitorData = [];

function startAnalysis(){
    var url = document.getElementById('url').value.trim();
    if(!url){ document.getElementById('url').focus(); return; }
    if(!url.match(/^https?:\/\//)) url = 'https://' + url;

    document.getElementById('gobtn').disabled = true;
    document.getElementById('phase-url').style.display = 'none';
    document.getElementById('phase-pipeline').style.display = 'block';
    document.getElementById('url-tag').textContent = url;

    fetch('/api/start', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({url: url})
    })
    .then(function(r){ return r.json() })
    .then(function(res){
        if(!res.ok){ showError(res.error); return; }
        SID = res.session_id;
        listenSSE();
    })
    .catch(function(err){ showError('Ошибка сети: ' + err.message); });
}

function listenSSE(){
    evtSource = new EventSource('/api/events/' + SID);

    evtSource.addEventListener('step', function(e){
        var d = JSON.parse(e.data);
        setStep(d.num, d.status, d.text);
    });

    evtSource.addEventListener('waiting_company', function(e){
        var d = JSON.parse(e.data);
        showCompanyPanel(d);
    });

    evtSource.addEventListener('waiting_competitors', function(e){
        var d = JSON.parse(e.data);
        showCompetitorPanel(d);
    });

    evtSource.addEventListener('done', function(e){
        var d = JSON.parse(e.data);
        evtSource.close();
        document.getElementById('result').style.display = 'block';
        document.getElementById('rcompany').textContent = d.company || '';
        document.getElementById('rlink').href = d.url;
        document.getElementById('rmeta').textContent = d.size_kb + ' KB';
    });

    evtSource.addEventListener('error', function(e){
        try {
            var d = JSON.parse(e.data);
            showError(d.message || 'Неизвестная ошибка');
        } catch(ex) {
            showError('Соединение потеряно');
        }
        evtSource.close();
    });
}

function showCompanyPanel(d){
    var ci = d.company_info || {};
    var fns = d.fns_data || {};
    var fc = fns.fns_company || {};

    document.getElementById('c-name').value = ci.name || fc.name || '';
    document.getElementById('c-inn').value = fc.inn || ci.inn || '';
    document.getElementById('c-legal').value = fc.full_name || ci.legal_name || '';
    document.getElementById('c-address').value = fc.address || ci.address || '';

    var bt = ci.business_type_guess || 'B2B_SERVICE';
    var sel = document.getElementById('c-type');
    for(var i=0; i<sel.options.length; i++){
        if(sel.options[i].value === bt) sel.selectedIndex = i;
    }

    var statusDiv = document.getElementById('fns-status');
    if(fc.inn){
        statusDiv.className = 'fns-info';
        statusDiv.innerHTML = '\u2713 Найдено в ФНС: ' + (fc.name||'') + ' | ИНН ' + fc.inn +
            (fc.okved ? ' | ОКВЭД ' + fc.okved : '');
    } else {
        statusDiv.className = 'fns-warning';
        statusDiv.textContent = '\u26A0 Юрлицо не найдено автоматически. Введите ИНН вручную или продолжите без него.';
    }

    document.getElementById('panel-company').style.display = 'block';
}

function confirmCompany(){
    document.getElementById('panel-company').style.display = 'none';
    fetch('/api/confirm-company/' + SID, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            name: document.getElementById('c-name').value,
            inn: document.getElementById('c-inn').value,
            legal_name: document.getElementById('c-legal').value,
            address: document.getElementById('c-address').value,
            business_type_guess: document.getElementById('c-type').value
        })
    });
}

function showCompetitorPanel(d){
    competitorData = d.competitors || [];
    document.getElementById('market-name').textContent = d.market_name ? '| ' + d.market_name : '';
    renderCompetitors();
    document.getElementById('panel-competitors').style.display = 'block';
}

function renderCompetitors(){
    var html = '';
    for(var i=0; i<competitorData.length; i++){
        var c = competitorData[i];
        var on = c._enabled !== false;
        var threatCls = 'threat-' + (c.threat_level || 'med');
        html += '<div class="comp-item' + (on ? '' : ' excluded') + '">' +
            '<button class="comp-toggle ' + (on ? 'on' : '') + '" onclick="toggleComp(' + i + ')">' + (on ? '\u2713' : '') + '</button>' +
            '<div class="comp-info"><div class="comp-name">' + (c.name||'') + '</div>' +
            '<div class="comp-desc">' + (c.description||c.why_competitor||'') + '</div></div>' +
            '<span class="comp-threat ' + threatCls + '">' + (c.threat_level||'med') + '</span></div>';
    }
    document.getElementById('comp-list').innerHTML = html;
}

function toggleComp(idx){
    competitorData[idx]._enabled = competitorData[idx]._enabled === false ? true : false;
    renderCompetitors();
}

function confirmCompetitors(){
    var selected = competitorData.filter(function(c){ return c._enabled !== false; });
    document.getElementById('panel-competitors').style.display = 'none';
    fetch('/api/confirm-competitors/' + SID, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({competitors: selected})
    });
}

function setStep(n, status, text){
    var el = document.getElementById('s' + n);
    if(!el) return;
    el.className = 'step ' + status;
    if(text) el.querySelector('span').textContent = text;
    var icon = el.querySelector('.step-icon');
    if(status === 'done') icon.textContent = '\u2713';
    else if(status === 'fail') icon.textContent = '\u2717';
    else if(status === 'warning') icon.textContent = '!';
}

function showError(msg){
    var el = document.getElementById('error');
    el.style.display = 'block';
    el.textContent = msg;
}
</script>
</body>
</html>"""
