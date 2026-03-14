"""Step 7: safe revision after board review.

This step never invents new data. It only:
- turns board critiques into blocking issues/open questions;
- gates unreliable sections;
- removes obviously invalid or unsourced content.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from app.pipeline.release import add_blocking_issue, set_report_status

logger = logging.getLogger(__name__)

# ── Mapping board critiques → report_data keys ────────────────────────────────

_CRITIQUE_TO_KEYS: dict[str, list[str]] = {
    "финанс": ["financials"],
    "financial": ["financials"],
    "выручк": ["financials"],
    "revenue": ["financials"],
    "прибыл": ["financials"],
    "рентаб": ["financials"],
    "swot": ["swot"],
    "конкурент": ["competitors"],
    "competitor": ["competitors"],
    "перцепту": ["competitors"],
    "радар": ["competitors", "radar_dimensions"],
    "рынок": ["market"],
    "market": ["market"],
    "tam": ["market"],
    "sam": ["market"],
    "digital": ["digital"],
    "цифров": ["digital"],
    "соцсет": ["digital"],
    "instagram": ["digital"],
    "telegram": ["digital"],
    "hr": ["hr_data"],
    "кадр": ["hr_data"],
    "зарплат": ["hr_data"],
    "вакан": ["hr_data"],
    "kpi": ["kpi_benchmarks"],
    "бенчмарк": ["kpi_benchmarks"],
    "benchmark": ["kpi_benchmarks"],
    "рекоменда": ["recommendations"],
    "recommend": ["recommendations"],
    "сценар": ["scenarios"],
    "scenario": ["scenarios"],
    "глоссарий": ["glossary"],
    "glossary": ["glossary"],
    "фаундер": ["founders"],
    "founder": ["founders"],
    "владел": ["founders"],
    "мнени": ["opinions"],
    "opinion": ["opinions"],
    "цитат": ["opinions"],
    "доля рынка": ["market_share"],
    "market_share": ["market_share"],
    "методолог": ["methodology"],
    "фактчек": ["factcheck"],
    "factcheck": ["factcheck"],
    "продукт": ["products"],
    "услуг": ["products"],
}

_KEY_TO_BLOCKS: dict[str, list[str]] = {
    "financials": ["P2"],
    "swot": ["P3"],
    "digital": ["P4"],
    "products": ["P5"],
    "competitors": ["C1", "C2", "C3", "C4", "C7", "C8"],
    "market": ["M1"],
    "hr_data": ["M4"],
    "recommendations": ["S1"],
    "kpi_benchmarks": ["S2"],
    "scenarios": ["S3"],
    "factcheck": ["F1"],
    "founders": ["O1"],
    "opinions": ["O2"],
    "market_share": ["P10"],
    "glossary": ["A2"],
}

_UNTRUSTED_KEYWORDS = (
    "галлюцина", "выдуман", "фейк", "не существ", "несуществующ",
    "hallucin", "fabricat", "unverified", "невериф", "без источ",
    "без подтвержд", "не подтвержд", "outdated", "устарел",
)


def _map_critique_to_keys(critique: dict) -> set[str]:
    """Determine which report_data keys are affected by a critique."""
    section = (critique.get("section") or "").lower()
    issue = (critique.get("issue") or "").lower()
    suggestion = (critique.get("suggestion") or "").lower()
    combined = f"{section} {issue} {suggestion}"

    keys: set[str] = set()
    for keyword, data_keys in _CRITIQUE_TO_KEYS.items():
        if keyword in combined:
            keys.update(data_keys)
    return keys


def _collect_actionable_critiques(board_review: dict) -> list[dict]:
    """Collect high + medium critiques from all board reviews."""
    critiques = []
    for review in board_review.get("reviews", []):
        resp = review.get("response", review)
        for critique in resp.get("critiques", []):
            severity = critique.get("severity", "low").lower()
            if severity in ("high", "medium"):
                critique_copy = dict(critique)
                critique_copy["from_expert"] = review.get("role", "Unknown")
                critiques.append(critique_copy)
    return critiques


def _gate_blocks(report_data: dict, keys: set[str]) -> int:
    """Gate report blocks related to the affected keys."""
    section_gates = report_data.setdefault("section_gates", {})
    changed = 0
    for key in keys:
        for block_id in _KEY_TO_BLOCKS.get(key, []):
            if section_gates.get(block_id) is not False:
                section_gates[block_id] = False
                changed += 1
    report_data["failed_gates"] = sorted({k for k, v in section_gates.items() if not v})
    return changed


def _append_open_question(report_data: dict, text: str) -> None:
    """Append a unique open question."""
    question = (text or "").strip()
    if not question:
        return
    open_questions = report_data.setdefault("open_questions", [])
    if question not in open_questions:
        open_questions.append(question)


def _fix_readability(report_data: dict) -> int:
    """Remove JSON fragments and placeholders from text fields."""
    fixes = 0
    json_re = re.compile(r'\{"[^"]+":')
    placeholder_re = re.compile(
        r'\b(TODO|FIXME|Lorem ipsum|placeholder|undefined|N/A)\b',
        re.IGNORECASE,
    )
    template_re = re.compile(r'\{\{[^}]+\}\}')

    def clean_text(val: Any) -> tuple[Any, bool]:
        if not isinstance(val, str):
            return val, False
        changed = False
        if json_re.search(val):
            cleaned = re.sub(r'\{[^}]*\}', "", val).strip()
            if cleaned and len(cleaned) > 10:
                val = cleaned
                changed = True
        if placeholder_re.search(val):
            val = placeholder_re.sub("", val).strip()
            changed = True
        if template_re.search(val):
            val = template_re.sub("", val).strip()
            changed = True
        return val, changed

    def walk(obj: Any) -> None:
        nonlocal fixes
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str):
                    cleaned, changed = clean_text(value)
                    if changed:
                        obj[key] = cleaned
                        fixes += 1
                elif isinstance(value, (dict, list)):
                    walk(value)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                if isinstance(item, str):
                    cleaned, changed = clean_text(item)
                    if changed:
                        obj[idx] = cleaned
                        fixes += 1
                elif isinstance(item, (dict, list)):
                    walk(item)

    walk(report_data)
    return fixes


def _strip_invalid_radar_scores(report_data: dict) -> int:
    """Drop invalid radar scores instead of clamping values."""
    fixes = 0
    for comp in report_data.get("competitors") or []:
        if not isinstance(comp, dict):
            continue
        scores = comp.get("radar_scores") or {}
        for dim, value in list(scores.items()):
            try:
                numeric = float(value)
                if numeric < 0 or numeric > 10:
                    scores.pop(dim, None)
                    fixes += 1
            except (TypeError, ValueError):
                scores.pop(dim, None)
                fixes += 1
    return fixes


def _clean_swot(report_data: dict) -> int:
    """Remove blank SWOT items without fabricating replacements."""
    swot = report_data.get("swot")
    if not isinstance(swot, dict):
        return 0

    fixes = 0
    for quad in ("strengths", "weaknesses", "opportunities", "threats"):
        items = swot.get(quad)
        if not isinstance(items, list):
            continue
        cleaned = [item.strip() for item in items if isinstance(item, str) and item.strip()]
        if cleaned != items:
            swot[quad] = cleaned
            fixes += 1
    return fixes


def _clean_market_share(report_data: dict) -> int:
    """Drop non-numeric market share entries without renormalizing."""
    market_share = report_data.get("market_share")
    if not isinstance(market_share, dict):
        return 0

    fixes = 0
    for key, value in list(market_share.items()):
        if not isinstance(value, (int, float)):
            market_share.pop(key, None)
            fixes += 1
    return fixes


def _drop_empty_financial_rows(report_data: dict) -> int:
    """Drop finance rows that contain only a year and no values."""
    financials = report_data.get("financials")
    if not isinstance(financials, list):
        return 0

    cleaned = []
    for row in financials:
        if not isinstance(row, dict):
            continue
        metrics = [
            row.get("revenue"),
            row.get("net_profit"),
            row.get("assets"),
            row.get("equity"),
            row.get("liabilities"),
            row.get("employees"),
        ]
        if any(value is not None for value in metrics):
            cleaned.append(row)

    dropped = len(financials) - len(cleaned)
    if dropped:
        report_data["financials"] = cleaned
    return dropped


def _drop_unsourced_opinions(report_data: dict) -> int:
    """Drop opinion quotes without a source reference."""
    opinions = report_data.get("opinions")
    if not isinstance(opinions, list):
        return 0

    cleaned = [
        op for op in opinions
        if isinstance(op, dict) and (op.get("source") or op.get("source_url"))
    ]
    dropped = len(opinions) - len(cleaned)
    if dropped:
        report_data["opinions"] = cleaned
    return dropped


def _prune_competitors(report_data: dict) -> int:
    """Keep only minimally usable competitors and mark unsupported ones unverified."""
    competitors = report_data.get("competitors")
    if not isinstance(competitors, list):
        return 0

    cleaned = []
    dropped = 0
    for comp in competitors:
        if not isinstance(comp, dict) or not comp.get("name"):
            dropped += 1
            continue

        verification_sources = comp.get("verification_sources") or []
        has_support = bool(comp.get("website") or comp.get("description") or verification_sources)
        if not has_support:
            dropped += 1
            continue

        if not verification_sources:
            comp["verified"] = False
            comp["verification_confidence"] = "unverified"
        cleaned.append(comp)

    if dropped:
        report_data["competitors"] = cleaned
    return dropped


def revise_report(
    report_data: dict,
    board_review: dict,
    company_info: dict,
) -> dict:
    """Apply board critiques using deterministic cleanup + gating."""
    del company_info  # reserved for future provider-specific re-fetch hooks

    t0 = __import__("time").monotonic()
    critiques = _collect_actionable_critiques(board_review)
    if critiques:
        logger.info("Ревизия: %d actionable замечаний (high+medium) от борда", len(critiques))
    else:
        logger.info("Ревизия без actionable замечаний: выполняем только safe cleanup")

    gated_keys: set[str] = set()
    for critique in critiques:
        keys = _map_critique_to_keys(critique)
        severity = critique.get("severity", "low").lower()
        issue = (critique.get("issue") or "").strip()
        suggestion = (critique.get("suggestion") or "").strip()
        from_expert = critique.get("from_expert", "Board")

        if issue:
            _append_open_question(report_data, f"[Ревизия / {from_expert}] {issue}")
        if suggestion:
            _append_open_question(report_data, f"[Ревизия / {from_expert}] {suggestion}")

        issue_lower = issue.lower()
        if severity == "high" or any(keyword in issue_lower for keyword in _UNTRUSTED_KEYWORDS):
            if issue:
                add_blocking_issue(report_data, f"{from_expert}: {issue}")
            gated_keys.update(keys)

    if gated_keys:
        gated_blocks = _gate_blocks(report_data, gated_keys)
        logger.info("Ревизия: загейтено %d блоков по замечаниям борда", gated_blocks)

    fixes = 0
    fixes += _fix_readability(report_data)
    fixes += _strip_invalid_radar_scores(report_data)
    fixes += _clean_swot(report_data)
    fixes += _clean_market_share(report_data)
    fixes += _drop_empty_financial_rows(report_data)
    fixes += _drop_unsourced_opinions(report_data)
    fixes += _prune_competitors(report_data)

    if not report_data.get("financials"):
        _gate_blocks(report_data, {"financials"})
        add_blocking_issue(report_data, "Нет подтверждённых финансовых данных")

    if not report_data.get("market_share"):
        _gate_blocks(report_data, {"market_share"})

    if not report_data.get("opinions"):
        _gate_blocks(report_data, {"opinions"})

    if len(report_data.get("competitors") or []) < 3:
        _gate_blocks(report_data, {"competitors"})
        add_blocking_issue(report_data, "Недостаточно подтверждённых конкурентов для сравнительного анализа")

    total_share = sum(
        value for value in (report_data.get("market_share") or {}).values()
        if isinstance(value, (int, float))
    )
    if total_share and not 90 <= total_share <= 110:
        _gate_blocks(report_data, {"market_share"})
        add_blocking_issue(report_data, f"Доли рынка несогласованы: сумма {total_share:.1f}%")

    set_report_status(report_data, "draft")
    elapsed = round(__import__("time").monotonic() - t0, 2)
    logger.info(
        "Ревизия завершена за %.2fs: %d фиксов, gates=%d, blockers=%d",
        elapsed,
        fixes,
        len(report_data.get("failed_gates") or []),
        len(report_data.get("blocking_issues") or []),
    )
    return report_data
