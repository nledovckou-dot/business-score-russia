"""Step Consistency: post-process report_data to fix internal contradictions.

Runs AFTER step5 (deep analysis) and BEFORE step2a (verification).
Pure Python — no LLM calls, no network requests.

The LLM generates different sections independently, so the same metric
(traffic, AOV, TAM) can appear with conflicting values across sections.
This step unifies them and recalculates dependent metrics.

Fixes:
1. Unify traffic values (digital.monthly_traffic vs calc_traces vs kpi_benchmarks)
2. Unify AOV / средний чек (kpi_benchmarks as primary)
3. Unify TAM (market.market_size vs calc_traces)
4. Recalculate dependent metrics (revenue, market share)
5. Deduplicate competitors by INN
6. Sanitize suspicious social media numbers
7. Fill empty radar_scores from available metrics
"""

from __future__ import annotations

import logging
import re
import statistics
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Metric name patterns ──

_TRAFFIC_PATTERNS = re.compile(
    r"трафик|traffic|посещаемость|визиты|visits",
    re.IGNORECASE,
)

_AOV_PATTERNS = re.compile(
    r"средн\w*\s*чек|aov|average\s*check|average\s*order",
    re.IGNORECASE,
)

_TAM_PATTERNS = re.compile(
    r"\btam\b|объ[её]м\s*рынка|market\s*size|ёмкость\s*рынка|емкость\s*рынка",
    re.IGNORECASE,
)

_CR_PATTERNS = re.compile(
    r"\bcr\b|конверси|conversion",
    re.IGNORECASE,
)

_REVENUE_PATTERNS = re.compile(
    r"выручк|revenue|оборот",
    re.IGNORECASE,
)

_MARKET_SHARE_PATTERNS = re.compile(
    r"доля\s*рынка|market\s*share",
    re.IGNORECASE,
)

# Federal brands threshold: revenue >= 10 billion RUB
_FEDERAL_BRAND_REVENUE_THRESHOLD = 10_000_000_000

# Suspicious social media subscriber count
_SUSPICIOUS_SUBSCRIBERS = 1_000_000


def run(report_data: dict, company_info: dict) -> dict:
    """Post-process report_data to fix internal contradictions.

    Args:
        report_data: dict compatible with ReportData model fields.
        company_info: original company info dict from identification step.

    Returns:
        Modified report_data dict with contradictions resolved.
    """
    fixes_applied = 0

    fixes_applied += _unify_traffic(report_data)
    fixes_applied += _unify_aov(report_data)
    fixes_applied += _unify_tam(report_data)
    fixes_applied += _recalculate_dependents(report_data)
    fixes_applied += _deduplicate_competitors_by_inn(report_data)
    fixes_applied += _sanitize_social_media(report_data, company_info)
    fixes_applied += _fill_empty_radar_scores(report_data)

    if fixes_applied:
        logger.info(
            "[Consistency] Applied %d fix(es) to report_data", fixes_applied
        )
    else:
        logger.info("[Consistency] No contradictions found — data is consistent")

    return report_data


# ── 1. Unify traffic ──


def _unify_traffic(report_data: dict) -> int:
    """Unify traffic values across digital, calc_traces, kpi_benchmarks.

    If Keys.so data exists, use keyso.visibility * 50 as ground truth.
    Otherwise pick the median of all found values.
    """
    digital = report_data.get("digital") or {}
    calc_traces = report_data.get("calc_traces") or []
    kpi_benchmarks = report_data.get("kpi_benchmarks") or []

    # Determine ground truth from keyso
    keyso = digital.get("keyso") or {}
    keyso_visibility = keyso.get("visibility")
    ground_truth: Optional[float] = None
    if keyso_visibility and isinstance(keyso_visibility, (int, float)) and keyso_visibility > 0:
        ground_truth = keyso_visibility * 50

    # Collect all traffic values
    all_values: list[float] = []

    # From digital.monthly_traffic
    mt = digital.get("monthly_traffic")
    if mt is not None:
        val = _to_number(mt)
        if val is not None and val > 0:
            all_values.append(val)

    # From calc_traces
    ct_traffic_indices = []
    for i, ct in enumerate(calc_traces):
        if not isinstance(ct, dict):
            continue
        name = ct.get("metric_name", "")
        if _TRAFFIC_PATTERNS.search(name):
            val = _to_number(ct.get("value"))
            if val is not None and val > 0:
                all_values.append(val)
            ct_traffic_indices.append(i)

    # From kpi_benchmarks
    kpi_traffic_indices = []
    for i, kpi in enumerate(kpi_benchmarks):
        if not isinstance(kpi, dict):
            continue
        name = kpi.get("name", "")
        if _TRAFFIC_PATTERNS.search(name):
            val = _to_number(kpi.get("current"))
            if val is not None and val > 0:
                all_values.append(val)
            kpi_traffic_indices.append(i)

    if not all_values:
        return 0

    # Pick unified value
    if ground_truth is not None:
        unified = ground_truth
        source = "Keys.so (visibility * 50)"
    else:
        unified = statistics.median(all_values)
        source = f"median of {len(all_values)} values"

    unified_int = int(round(unified))
    fixes = 0

    # Update digital.monthly_traffic
    old_mt = digital.get("monthly_traffic")
    if old_mt is None or _to_number(old_mt) != unified_int:
        digital["monthly_traffic"] = unified_int
        report_data["digital"] = digital
        logger.info(
            "[Consistency] Traffic unified: %s -> %d (%s)",
            old_mt, unified_int, source,
        )
        fixes += 1

    # Update calc_traces
    for idx in ct_traffic_indices:
        ct = calc_traces[idx]
        old_val = ct.get("value")
        if _to_number(old_val) != unified_int:
            ct["value"] = unified_int
            logger.info(
                "[Consistency] calc_trace '%s' traffic: %s -> %d",
                ct.get("metric_name"), old_val, unified_int,
            )
            fixes += 1

    # Update kpi_benchmarks
    for idx in kpi_traffic_indices:
        kpi = kpi_benchmarks[idx]
        old_val = kpi.get("current")
        if _to_number(old_val) != unified_int:
            kpi["current"] = float(unified_int)
            logger.info(
                "[Consistency] kpi_benchmark '%s' traffic: %s -> %d",
                kpi.get("name"), old_val, unified_int,
            )
            fixes += 1

    return fixes


# ── 2. Unify AOV (средний чек) ──


def _unify_aov(report_data: dict) -> int:
    """Unify AOV across calc_traces and kpi_benchmarks.

    Pick the value from kpi_benchmarks as primary (more carefully estimated).
    Update calc_traces to match.
    """
    calc_traces = report_data.get("calc_traces") or []
    kpi_benchmarks = report_data.get("kpi_benchmarks") or []

    # Find AOV in kpi_benchmarks (primary)
    primary_aov: Optional[float] = None
    for kpi in kpi_benchmarks:
        if not isinstance(kpi, dict):
            continue
        name = kpi.get("name", "")
        if _AOV_PATTERNS.search(name):
            val = _to_number(kpi.get("current"))
            if val is not None and val > 0:
                primary_aov = val
                break

    if primary_aov is None:
        return 0

    # Update calc_traces
    fixes = 0
    for ct in calc_traces:
        if not isinstance(ct, dict):
            continue
        name = ct.get("metric_name", "")
        if _AOV_PATTERNS.search(name):
            old_val = _to_number(ct.get("value"))
            if old_val is not None and old_val != primary_aov:
                ct["value"] = primary_aov
                logger.info(
                    "[Consistency] AOV in calc_trace '%s': %s -> %s (from kpi_benchmarks)",
                    name, old_val, primary_aov,
                )
                fixes += 1

    return fixes


# ── 3. Unify TAM ──


def _unify_tam(report_data: dict) -> int:
    """Unify TAM between market.market_size and calc_traces.

    If calc_trace has a formula-based TAM, prefer it. Update market.market_size.
    """
    market = report_data.get("market") or {}
    calc_traces = report_data.get("calc_traces") or []

    # Find TAM in calc_traces (prefer formula-based)
    ct_tam: Optional[dict] = None
    ct_tam_value: Optional[float] = None
    for ct in calc_traces:
        if not isinstance(ct, dict):
            continue
        name = ct.get("metric_name", "")
        if _TAM_PATTERNS.search(name):
            val = _to_number(ct.get("value"))
            if val is not None and val > 0:
                # Prefer CALC over ESTIMATE
                if ct_tam is None or ct.get("confidence") == "CALC":
                    ct_tam = ct
                    ct_tam_value = val

    if ct_tam_value is None:
        return 0

    # Check if market.market_size differs
    market_size_str = market.get("market_size", "")
    market_size_num = _to_number(market_size_str)

    if market_size_num is not None and market_size_num == ct_tam_value:
        return 0  # already consistent

    # Update market.market_size with the calc_trace value
    # Preserve format: try to keep human-readable string
    if ct_tam_value >= 1_000_000_000:
        new_size = f"{ct_tam_value / 1_000_000_000:.1f} млрд ₽"
    elif ct_tam_value >= 1_000_000:
        new_size = f"{ct_tam_value / 1_000_000:.0f} млн ₽"
    elif ct_tam_value >= 1_000:
        new_size = f"{ct_tam_value / 1_000:.0f} тыс. ₽"
    else:
        new_size = f"{ct_tam_value:.0f} ₽"

    old_size = market.get("market_size")
    market["market_size"] = new_size
    report_data["market"] = market

    logger.info(
        "[Consistency] TAM unified: market_size '%s' -> '%s' (from calc_trace formula)",
        old_size, new_size,
    )

    return 1


# ── 4. Recalculate dependent metrics ──


def _recalculate_dependents(report_data: dict) -> int:
    """Recalculate revenue, market share based on unified traffic/AOV/TAM.

    revenue = traffic * CR * AOV * 12
    market_share = revenue / TAM * 100
    """
    calc_traces = report_data.get("calc_traces") or []

    # Collect unified values
    traffic = _find_metric_value(calc_traces, _TRAFFIC_PATTERNS)
    if traffic is None:
        digital = report_data.get("digital") or {}
        traffic = _to_number(digital.get("monthly_traffic"))

    aov = _find_metric_value(calc_traces, _AOV_PATTERNS)
    cr = _find_metric_value(calc_traces, _CR_PATTERNS)
    tam = _find_metric_value(calc_traces, _TAM_PATTERNS)

    fixes = 0

    # Recalculate revenue if we have traffic + CR + AOV
    if traffic is not None and cr is not None and aov is not None:
        # CR might be in percent or decimal
        cr_decimal = cr / 100.0 if cr > 1 else cr
        new_revenue = traffic * cr_decimal * aov * 12

        for ct in calc_traces:
            if not isinstance(ct, dict):
                continue
            name = ct.get("metric_name", "")
            if _REVENUE_PATTERNS.search(name):
                old_val = _to_number(ct.get("value"))
                if old_val is not None and old_val > 0:
                    # Only update if differs by more than 5%
                    if abs(new_revenue - old_val) / old_val > 0.05:
                        ct["value"] = round(new_revenue, 0)
                        ct["inputs"] = ct.get("inputs", {})
                        ct["inputs"]["traffic"] = traffic
                        ct["inputs"]["cr"] = cr
                        ct["inputs"]["aov"] = aov
                        ct["formula"] = "traffic * CR * AOV * 12"
                        logger.info(
                            "[Consistency] Revenue recalculated: %s -> %s "
                            "(traffic=%s, CR=%s, AOV=%s)",
                            old_val, ct["value"], traffic, cr, aov,
                        )
                        fixes += 1
                break  # only first revenue entry

    # Recalculate market share
    revenue_val = _find_metric_value(calc_traces, _REVENUE_PATTERNS)
    if revenue_val is None:
        # Try from financials
        financials = report_data.get("financials") or []
        if financials:
            latest = max(
                (f for f in financials if isinstance(f, dict) and f.get("revenue")),
                key=lambda f: f.get("year", 0),
                default=None,
            )
            if latest:
                revenue_val = _to_number(latest.get("revenue"))

    if revenue_val is not None and tam is not None and tam > 0:
        new_share = round(revenue_val / tam * 100, 2)

        # Update calc_trace for market share
        for ct in calc_traces:
            if not isinstance(ct, dict):
                continue
            name = ct.get("metric_name", "")
            if _MARKET_SHARE_PATTERNS.search(name):
                old_val = _to_number(ct.get("value"))
                if old_val is not None and abs(new_share - old_val) > 0.1:
                    ct["value"] = new_share
                    logger.info(
                        "[Consistency] Market share recalculated: %s%% -> %s%%",
                        old_val, new_share,
                    )
                    fixes += 1
                break

        # Update market_share dict if company entry differs
        market_share = report_data.get("market_share") or {}
        company_data = report_data.get("company") or {}
        company_name = company_data.get("name", "")
        if company_name and company_name in market_share:
            old_share = market_share[company_name]
            if abs(new_share - old_share) > 0.1:
                market_share[company_name] = new_share
                report_data["market_share"] = market_share
                logger.info(
                    "[Consistency] market_share['%s']: %s -> %s",
                    company_name, old_share, new_share,
                )
                fixes += 1

    return fixes


# ── 5. Deduplicate competitors by INN ──


def _deduplicate_competitors_by_inn(report_data: dict) -> int:
    """If 2+ competitors share the same INN, merge into one entry.

    Keep the first occurrence, note others as affiliated brands.
    """
    competitors = report_data.get("competitors") or []
    if not competitors:
        return 0

    seen_inns: dict[str, int] = {}  # inn -> index of first occurrence
    duplicates: list[int] = []  # indices to remove

    for i, comp in enumerate(competitors):
        if not isinstance(comp, dict):
            continue
        inn = comp.get("inn")
        if not inn or not isinstance(inn, str) or not inn.strip():
            continue
        inn = inn.strip()
        if inn in seen_inns:
            first_idx = seen_inns[inn]
            first_comp = competitors[first_idx]
            # Note the duplicate as affiliated brand
            affiliated = first_comp.get("_affiliated_brands", [])
            affiliated.append(comp.get("name", "Unknown"))
            first_comp["_affiliated_brands"] = affiliated
            # Merge description if the duplicate has one
            dup_desc = comp.get("description", "")
            if dup_desc:
                orig_desc = first_comp.get("description", "")
                if orig_desc:
                    first_comp["description"] = (
                        f"{orig_desc} | Аффилированный бренд: {comp.get('name', '')}: {dup_desc}"
                    )
                else:
                    first_comp["description"] = dup_desc
            duplicates.append(i)
        else:
            seen_inns[inn] = i

    if not duplicates:
        return 0

    # Remove duplicates in reverse order to preserve indices
    for idx in sorted(duplicates, reverse=True):
        removed = competitors.pop(idx)
        logger.info(
            "[Consistency] Removed duplicate competitor '%s' (INN %s) — "
            "merged as affiliated brand into '%s'",
            removed.get("name"),
            removed.get("inn"),
            competitors[seen_inns[removed.get("inn", "")]].get("name")
            if removed.get("inn") in seen_inns
            else "?",
        )

    report_data["competitors"] = competitors
    return len(duplicates)


# ── 6. Sanitize social media numbers ──


def _sanitize_social_media(report_data: dict, company_info: dict) -> int:
    """Flag suspiciously high subscriber counts for non-federal brands.

    If a competitor has telegram/vk/instagram subscribers > 1M
    AND company revenue < 10 billion, set to null with a warning.
    """
    competitors = report_data.get("competitors") or []

    # Determine if the company is a federal brand by revenue
    is_federal = _is_federal_brand(report_data, company_info)
    if is_federal:
        return 0  # federal brands can legitimately have 1M+ subscribers

    fixes = 0

    for comp in competitors:
        if not isinstance(comp, dict):
            continue
        metrics = comp.get("metrics") or {}

        for platform in ("telegram", "vk", "instagram", "tg", "ig"):
            key_subscribers = f"{platform}_subscribers"
            key_followers = f"{platform}_followers"

            for key in (key_subscribers, key_followers):
                val = _to_number(metrics.get(key))
                if val is not None and val > _SUSPICIOUS_SUBSCRIBERS:
                    logger.warning(
                        "[Consistency] Suspicious %s=%d for '%s' "
                        "(non-federal brand, revenue < 10B). Setting to null.",
                        key, int(val), comp.get("name"),
                    )
                    metrics[key] = None
                    metrics[f"_{key}_warning"] = (
                        f"Подозрительное значение {int(val):,} — "
                        f"нехарактерно для компании данного масштаба. "
                        f"Требует верификации."
                    )
                    fixes += 1

        # Also check social_accounts list (Pydantic model uses this)
        social_accounts = comp.get("social_accounts") or []
        for acc in social_accounts:
            if not isinstance(acc, dict):
                continue
            followers = _to_number(acc.get("followers"))
            if followers is not None and followers > _SUSPICIOUS_SUBSCRIBERS:
                platform_name = acc.get("platform", "unknown")
                logger.warning(
                    "[Consistency] Suspicious followers=%d on %s for '%s'. "
                    "Setting to null.",
                    int(followers), platform_name, comp.get("name"),
                )
                acc["followers"] = None
                acc["_warning"] = (
                    f"Подозрительное кол-во подписчиков {int(followers):,} — "
                    f"нехарактерно для компании данного масштаба."
                )
                fixes += 1

    # Check company's own digital audit
    digital = report_data.get("digital") or {}
    social_accounts = digital.get("social_accounts") or []
    for acc in social_accounts:
        if not isinstance(acc, dict):
            continue
        followers = _to_number(acc.get("followers"))
        if followers is not None and followers > _SUSPICIOUS_SUBSCRIBERS:
            platform_name = acc.get("platform", "unknown")
            logger.warning(
                "[Consistency] Suspicious own followers=%d on %s. Setting to null.",
                int(followers), platform_name,
            )
            acc["followers"] = None
            acc["_warning"] = (
                f"Подозрительное кол-во подписчиков {int(followers):,} — "
                f"требует верификации."
            )
            fixes += 1

    return fixes


# ── 7. Fill empty radar_scores ──


def _fill_empty_radar_scores(report_data: dict) -> int:
    """Auto-fill radar_scores if ALL competitors have empty ones.

    Uses available metrics to derive scores on a 0-10 scale:
    - "Цена" -> based on revenue (higher revenue = higher score)
    - "Digital" -> based on keyso DR or SEO visibility
    - "Бренд" -> based on 2GIS rating or reviews count
    """
    competitors = report_data.get("competitors") or []
    if not competitors:
        return 0

    # Check if ALL competitors have empty radar_scores
    all_empty = all(
        not (comp.get("radar_scores") or {})
        for comp in competitors
        if isinstance(comp, dict)
    )
    if not all_empty:
        return 0

    digital = report_data.get("digital") or {}
    keyso = digital.get("keyso") or {}

    # Collect revenue values for scaling
    revenues: list[tuple[int, float]] = []
    for i, comp in enumerate(competitors):
        if not isinstance(comp, dict):
            continue
        # Try financials
        financials = comp.get("financials") or []
        if financials:
            latest = max(
                (f for f in financials if isinstance(f, dict) and f.get("revenue")),
                key=lambda f: f.get("year", 0),
                default=None,
            )
            if latest:
                rev = _to_number(latest.get("revenue"))
                if rev is not None and rev > 0:
                    revenues.append((i, rev))
        # Try metrics.revenue
        if not any(idx == i for idx, _ in revenues):
            metrics = comp.get("metrics") or {}
            rev = _to_number(metrics.get("revenue") or metrics.get("выручка"))
            if rev is not None and rev > 0:
                revenues.append((i, rev))

    fixes = 0

    for i, comp in enumerate(competitors):
        if not isinstance(comp, dict):
            continue

        scores: dict[str, float] = {}

        # "Цена" — based on revenue rank (higher revenue = higher score)
        if revenues:
            max_rev = max(r for _, r in revenues)
            comp_rev = next((r for idx, r in revenues if idx == i), None)
            if comp_rev is not None and max_rev > 0:
                scores["Цена"] = round(min(comp_rev / max_rev * 10, 10), 1)
            else:
                scores["Цена"] = 5.0
        else:
            scores["Цена"] = 5.0

        # "Digital" — based on keyso DR or seo_score
        metrics = comp.get("metrics") or {}
        dr = _to_number(metrics.get("dr") or metrics.get("keyso_dr"))
        if dr is not None:
            scores["Digital"] = round(min(dr / 10, 10), 1)  # DR 0-100 -> 0-10
        elif keyso.get("dr"):
            # Use company's own DR as reference (only for first/self entry)
            scores["Digital"] = 5.0
        else:
            seo = _to_number(metrics.get("seo_score"))
            if seo is not None:
                scores["Digital"] = round(min(seo / 10, 10), 1)
            else:
                scores["Digital"] = 5.0

        # "Бренд" — based on 2GIS rating or reviews count
        rating = _to_number(metrics.get("rating") or metrics.get("Rating"))
        reviews = _to_number(metrics.get("reviews") or metrics.get("Reviews") or metrics.get("reviews_count"))
        if rating is not None:
            scores["Бренд"] = round(rating * 2, 1)  # rating 1-5 -> 2-10
        elif reviews is not None and reviews > 0:
            # Rough scale: 100 reviews = 5, 1000 = 8, 5000 = 10
            import math
            scores["Бренд"] = round(min(math.log10(max(reviews, 1)) * 3, 10), 1)
        else:
            scores["Бренд"] = 5.0

        comp["radar_scores"] = scores
        fixes += 1
        logger.info(
            "[Consistency] Auto-filled radar_scores for '%s': %s",
            comp.get("name"), scores,
        )

    # Update radar_dimensions if not set
    if fixes > 0:
        existing_dims = report_data.get("radar_dimensions") or []
        if not existing_dims:
            report_data["radar_dimensions"] = ["Цена", "Digital", "Бренд"]
            logger.info("[Consistency] Set radar_dimensions to %s", report_data["radar_dimensions"])

    return fixes


# ── Utility functions ──


def _to_number(value: Any) -> Optional[float]:
    """Convert a value to float. Handles strings like '1 200', '12.5%', '100K'."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    # Remove spaces, currency symbols, units
    cleaned = value.strip()
    if not cleaned:
        return None

    # Handle multiplier suffixes
    multiplier = 1.0
    lower = cleaned.lower()
    if lower.endswith("k") or lower.endswith("к"):
        multiplier = 1_000
        cleaned = cleaned[:-1]
    elif lower.endswith("m") or lower.endswith("м"):
        multiplier = 1_000_000
        cleaned = cleaned[:-1]

    # Handle "млрд", "млн", "тыс"
    for suffix, mult in [("млрд", 1e9), ("млн", 1e6), ("тыс", 1e3)]:
        if suffix in lower:
            multiplier = mult
            cleaned = re.sub(rf"\s*{suffix}\.?\s*", "", cleaned, flags=re.IGNORECASE)
            break

    # Remove non-numeric chars except digits, dots, commas, minus
    cleaned = re.sub(r"[^\d.,\-]", "", cleaned.replace(" ", ""))
    if not cleaned:
        return None
    try:
        return float(cleaned.replace(",", ".")) * multiplier
    except ValueError:
        return None


def _find_metric_value(
    calc_traces: list[dict],
    pattern: re.Pattern,
) -> Optional[float]:
    """Find the first matching metric value in calc_traces."""
    for ct in calc_traces:
        if not isinstance(ct, dict):
            continue
        name = ct.get("metric_name", "")
        if pattern.search(name):
            val = _to_number(ct.get("value"))
            if val is not None and val > 0:
                return val
    return None


def _is_federal_brand(report_data: dict, company_info: dict) -> bool:
    """Check if the company is a major federal brand (revenue >= 10B RUB)."""
    # Check financials in report_data
    financials = report_data.get("financials") or []
    for f in financials:
        if not isinstance(f, dict):
            continue
        rev = _to_number(f.get("revenue"))
        if rev is not None and rev >= _FEDERAL_BRAND_REVENUE_THRESHOLD:
            return True

    # Check company_info
    rev = _to_number(company_info.get("revenue"))
    if rev is not None and rev >= _FEDERAL_BRAND_REVENUE_THRESHOLD:
        return True

    return False
