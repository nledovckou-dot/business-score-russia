"""Helpers for report release status and blocking issues."""

from __future__ import annotations

from typing import Any


def add_blocking_issue(report_data: dict[str, Any], issue: str) -> None:
    """Append a unique blocking issue to report_data."""
    text = (issue or "").strip()
    if not text:
        return

    issues = report_data.setdefault("blocking_issues", [])
    if text not in issues:
        issues.append(text)


def set_report_status(report_data: dict[str, Any], status: str) -> None:
    """Set normalized report status and keep draft_mode in sync."""
    normalized = "publishable" if status == "publishable" else "draft"
    report_data["report_status"] = normalized
    report_data["draft_mode"] = normalized != "publishable"


def set_quality_summary(report_data: dict[str, Any], quality_result: dict[str, Any] | None) -> None:
    """Store a compact quality summary on report_data."""
    if not quality_result:
        report_data["quality_summary"] = {}
        return

    critical = quality_result.get("critical_failures", []) or []
    warnings = quality_result.get("warnings", []) or []
    report_data["quality_summary"] = {
        "score": quality_result.get("score", 0),
        "passed": bool(quality_result.get("passed", False)),
        "critical_count": len(critical),
        "warning_count": len(warnings),
        "critical_failures": critical[:5],
        "warnings": warnings[:5],
    }


def finalize_release(
    report_data: dict[str, Any],
    board_review: dict[str, Any] | None = None,
    quality_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Finalize report release status after board + quality checks."""
    if quality_result is not None:
        set_quality_summary(report_data, quality_result)

    consensus = (board_review or {}).get("consensus", {})
    board_approved = bool(consensus.get("approved", False))
    critical_issues = int(consensus.get("critical_issues", 0) or 0)

    if not board_review or not (board_review.get("reviews") or []):
        add_blocking_issue(report_data, "Совет директоров не выполнил рецензию отчёта")
    elif not board_approved:
        add_blocking_issue(
            report_data,
            f"Совет директоров не одобрил отчёт ({critical_issues} критических замечаний)",
        )

    if quality_result is None:
        add_blocking_issue(report_data, "Автоматическая проверка качества не выполнена")
    elif not quality_result.get("passed", False):
        for issue in (quality_result.get("critical_failures") or [])[:5]:
            add_blocking_issue(report_data, f"QA: {issue}")

    failed_gates = report_data.get("failed_gates") or []
    if len(failed_gates) > 3:
        add_blocking_issue(
            report_data,
            f"{len(failed_gates)} секций отключены relevance gate",
        )

    has_blockers = bool(report_data.get("blocking_issues"))
    set_report_status(report_data, "publishable" if board_approved and not has_blockers else "draft")
    return report_data
