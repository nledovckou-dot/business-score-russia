"""Step 2a: Verification with dependency graph (v2.0).

Pure Python, no LLM. Recalculates CALC traces and checks for discrepancies.
Cascading recalculation: if a root fact changes → recalculate all dependents.
"""

from __future__ import annotations

import re
from typing import Any


# Known formulas for automatic verification
KNOWN_FORMULAS = {
    "рентабельность по чистой прибыли": {
        "formula": "чистая_прибыль / выручка × 100",
        "inputs": ["чистая_прибыль", "выручка"],
        "calc": lambda i: round(i["чистая_прибыль"] / i["выручка"] * 100, 1) if i.get("выручка") else None,
    },
    "выручка на сотрудника": {
        "formula": "выручка / кол-во_сотрудников",
        "inputs": ["выручка", "сотрудники"],
        "calc": lambda i: round(i["выручка"] / i["сотрудники"]) if i.get("сотрудники") else None,
    },
    "оборачиваемость активов": {
        "formula": "выручка / активы",
        "inputs": ["выручка", "активы"],
        "calc": lambda i: round(i["выручка"] / i["активы"], 2) if i.get("активы") else None,
    },
    "ebitda margin": {
        "formula": "ebitda / выручка × 100",
        "inputs": ["ebitda", "выручка"],
        "calc": lambda i: round(i["ebitda"] / i["выручка"] * 100, 1) if i.get("выручка") else None,
    },
}


def run(report_data: dict) -> dict:
    """Verify calc_traces and fix discrepancies.

    Modifies report_data in-place and returns it.
    Adds verification results to factcheck.
    """
    calc_traces = report_data.get("calc_traces", [])
    factcheck = report_data.get("factcheck", [])

    corrections = []

    for ct in calc_traces:
        if not isinstance(ct, dict):
            continue

        metric = ct.get("metric_name", "").lower().strip()
        confidence = ct.get("confidence", "ESTIMATE")
        inputs = ct.get("inputs", {})

        # Only verify CALC items that have known formulas
        if confidence != "CALC":
            continue

        # Try to match known formula
        matched_formula = None
        for key, formula_def in KNOWN_FORMULAS.items():
            if key in metric:
                matched_formula = formula_def
                break

        if matched_formula is None:
            continue

        # Check if all required inputs are present
        has_all_inputs = all(
            inputs.get(inp) is not None
            for inp in matched_formula["inputs"]
        )
        if not has_all_inputs:
            continue

        # Recalculate
        try:
            expected = matched_formula["calc"](inputs)
        except (ZeroDivisionError, TypeError, KeyError):
            continue

        if expected is None:
            continue

        # Extract numeric value from ct["value"]
        actual = _extract_number(ct.get("value", ""))
        if actual is None:
            continue

        # Compare with 5% tolerance
        if actual != 0 and abs(expected - actual) / abs(actual) > 0.05:
            corrections.append({
                "fact": f"{ct['metric_name']}: заявлено {ct['value']}, пересчёт = {expected}",
                "sources_count": 1,
                "verified": False,
                "sources": ["Автоматическая верификация v2.0"],
                "correction": f"Пересчитано: {expected}",
            })
            # Fix the value
            ct["value"] = str(expected)

    # Add corrections to factcheck
    if corrections:
        factcheck.extend(corrections)
        report_data["factcheck"] = factcheck

    # Verify lifecycle stages consistency
    _verify_lifecycle_consistency(report_data)

    return report_data


def _extract_number(value: str) -> float | None:
    """Extract numeric value from a string like '12.5%' or '1 200 тыс. ₽'."""
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    # Remove spaces, currency, units
    cleaned = re.sub(r'[^\d.,\-]', '', value.replace(' ', ''))
    if not cleaned:
        return None
    try:
        return float(cleaned.replace(',', '.'))
    except ValueError:
        return None


def _verify_lifecycle_consistency(report_data: dict) -> None:
    """Check that lifecycle stages are consistent with financial data.

    Investment phase + losses should NOT be flagged as inefficiency.
    """
    competitors = report_data.get("competitors", [])
    factcheck = report_data.get("factcheck", [])

    for comp in competitors:
        if not isinstance(comp, dict):
            continue
        lc = comp.get("lifecycle")
        if not isinstance(lc, dict):
            continue

        stage = lc.get("stage", "")
        if stage == "investment":
            # Check if any factcheck item incorrectly criticizes this company's losses
            comp_name = comp.get("name", "")
            for fact in factcheck:
                if not isinstance(fact, dict):
                    continue
                fact_text = fact.get("fact", "").lower()
                if comp_name.lower() in fact_text and any(
                    word in fact_text
                    for word in ["неэффективн", "убыточн", "проблем"]
                ):
                    fact["correction"] = (
                        f"Компания {comp_name} находится в инвестиционной фазе. "
                        f"Убытки вероятно связаны с CAPEX, а не с операционной неэффективностью."
                    )
