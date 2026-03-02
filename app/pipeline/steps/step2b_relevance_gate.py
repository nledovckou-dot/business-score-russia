"""Step 2b: Section Relevance Gate (v2.0).

Pure Python, no LLM.
Blacklist: correlation N<15, Price vs Rating, single-company dynamics without comparison.
Mandatory: A2 (glossary), A3 (methodology), A4 (calc_traces).
Fills section_gates dict: block_id → True/False.
"""

from __future__ import annotations

from typing import Any


# Blocks that must always be included if data exists
MANDATORY_BLOCKS = {"A2", "A3", "A4"}

# Blocks that require minimum data thresholds
MIN_DATA_REQUIREMENTS = {
    "S4": {"field": "correlations", "min_items": 6},   # Correlations need N >= 6 pairs minimum
    "C5": {"field": "competitors", "min_items": 3},     # Coverage heatmap needs 3+ competitors
}


def run(report_data: dict) -> dict:
    """Apply section relevance gates and fill section_gates in report_data.

    Returns the modified report_data with section_gates populated.
    """
    gates: dict[str, bool] = {}

    # Check correlations (blacklist: N<15 for statistical significance)
    correlations = report_data.get("correlations", [])
    if len(correlations) < 6:
        gates["S4"] = False

    # Check if correlation contains Price vs Rating (blacklisted)
    for corr in correlations:
        if not isinstance(corr, dict):
            continue
        pair = (
            corr.get("metric_a", "").lower(),
            corr.get("metric_b", "").lower(),
        )
        if _is_blacklisted_correlation(pair):
            # Remove this correlation
            correlations.remove(corr)
    report_data["correlations"] = correlations

    # Mandatory blocks: always on
    for block_id in MANDATORY_BLOCKS:
        gates[block_id] = True

    # Check data requirements
    for block_id, req in MIN_DATA_REQUIREMENTS.items():
        field_data = report_data.get(req["field"], [])
        if len(field_data) < req["min_items"]:
            gates[block_id] = False

    # Lifecycle section: only show if any competitor has lifecycle data
    has_lifecycle = any(
        isinstance(c, dict) and c.get("lifecycle")
        for c in report_data.get("competitors", [])
    )
    gates["C7"] = has_lifecycle

    # Sales channels section: only show if any competitor has channel data
    has_channels = any(
        isinstance(c, dict) and c.get("sales_channels")
        for c in report_data.get("competitors", [])
    )
    gates["C8"] = has_channels

    # Methodology: only if data exists
    gates["A3"] = bool(report_data.get("methodology"))

    # Calc traces: only if data exists
    gates["A4"] = bool(report_data.get("calc_traces"))

    # Financial chart without comparison: check if only 1 year of data
    financials = report_data.get("financials", [])
    if len(financials) < 2:
        # Single year financials are still shown (P2 is important)
        # but add a note to methodology
        methodology = report_data.get("methodology", {})
        if isinstance(methodology, dict):
            methodology["Ограничения"] = (
                methodology.get("Ограничения", "")
                + " Финансовая динамика ограничена 1 годом данных."
            ).strip()
            report_data["methodology"] = methodology

    report_data["section_gates"] = gates
    return report_data


def _is_blacklisted_correlation(pair: tuple[str, str]) -> bool:
    """Check if a correlation pair is in the blacklist.

    Blacklisted:
    - Price vs Rating (no causal relationship on marketplaces)
    """
    blacklist_keywords = [
        ({"цена", "price", "стоимость"}, {"рейтинг", "rating", "оценка"}),
    ]
    a, b = pair
    for kw_set_a, kw_set_b in blacklist_keywords:
        if (any(k in a for k in kw_set_a) and any(k in b for k in kw_set_b)) or \
           (any(k in b for k in kw_set_a) and any(k in a for k in kw_set_b)):
            return True
    return False
