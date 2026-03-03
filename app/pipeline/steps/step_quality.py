"""Step Quality: automatic quality validation of assembled report data (T10).

Runs after board review (step 6) and before HTML report build (step 7).
Pure Python — no LLM calls, no network requests. Fast sanity checks that catch
common problems: hallucinations, missing data, inconsistencies, poor sourcing,
and competitor data issues.

Returns a quality report dict:
    {
        "passed": bool,
        "score": float (0-100),
        "checks": [{"name": ..., "status": "pass"|"warn"|"fail", "message": ...}],
        "critical_failures": [...],
        "warnings": [...],
    }
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ── Known hallucination patterns ──

_FAKE_PHONE_PATTERNS = [
    re.compile(r"\+7\s*\(?\s*999\s*\)?\s*999[\s-]*99[\s-]*99"),
    re.compile(r"\+7\s*\(?\s*000\s*\)?\s*000[\s-]*00[\s-]*00"),
    re.compile(r"\+7\s*\(?\s*123\s*\)?\s*456[\s-]*78[\s-]*9[0-9]"),
    re.compile(r"\+7\s*\(?\s*111\s*\)?\s*111[\s-]*11[\s-]*11"),
    re.compile(r"\+7\s*\(?\s*777\s*\)?\s*777[\s-]*77[\s-]*77"),
]

_FAKE_EMAIL_DOMAINS = {
    "example.com", "example.org", "example.net",
    "test.com", "test.ru", "test.org",
    "fake.com", "fake.ru",
    "mail.example.com",
    "company.example.com",
    "placeholder.com",
}

_FAKE_URL_PATTERNS = [
    re.compile(r"https?://(?:www\.)?example\.(com|org|net|ru)"),
    re.compile(r"https?://(?:www\.)?test\.(com|org|net|ru)"),
    re.compile(r"https?://(?:www\.)?fake\.(com|org|net|ru)"),
    re.compile(r"https?://(?:www\.)?placeholder\.(com|org|net|ru)"),
    re.compile(r"https?://(?:www\.)?domain\.(com|org|net|ru)"),
    re.compile(r"https?://(?:www\.)?website\.(com|org|net|ru)"),
]

# Suspiciously round revenue numbers (in thousands of rubles)
# e.g. exactly 1,000,000 (= 1 billion rubles) or 500,000 (= 500 million)
_SUSPICIOUS_ROUND_NUMBERS = {
    1_000_000, 2_000_000, 5_000_000, 10_000_000,
    500_000, 100_000, 50_000,
    1_000_000_000, 500_000_000, 100_000_000,
}


def check_report_quality(report_data: dict, company_info: dict) -> dict:
    """Run quality checks on assembled report data.

    Args:
        report_data: dict compatible with ReportData model fields.
        company_info: original company info dict from identification step.

    Returns:
        Quality report dict with passed, score, checks, critical_failures, warnings.
    """
    checks: list[dict] = []
    critical_failures: list[str] = []
    warnings: list[str] = []

    # Run all checks
    _hallucination_detector(report_data, checks, critical_failures, warnings)
    _completeness_check(report_data, company_info, checks, critical_failures, warnings)
    _consistency_check(report_data, checks, critical_failures, warnings)
    _source_coverage(report_data, checks, critical_failures, warnings)
    _competitor_validation(report_data, company_info, checks, critical_failures, warnings)

    # Calculate score
    total_checks = len(checks)
    if total_checks == 0:
        score = 0.0
    else:
        passed_checks = sum(1 for c in checks if c["status"] == "pass")
        warn_checks = sum(1 for c in checks if c["status"] == "warn")
        # pass = full points, warn = half points, fail = 0
        score = round((passed_checks + warn_checks * 0.5) / total_checks * 100, 1)

    passed = len(critical_failures) == 0

    result = {
        "passed": passed,
        "score": score,
        "checks": checks,
        "critical_failures": critical_failures,
        "warnings": warnings,
    }

    logger.info(
        "[quality] Score: %.1f/100, passed=%s, checks=%d, critical=%d, warnings=%d",
        score, passed, total_checks, len(critical_failures), len(warnings),
    )

    return result


# ════════════════════════════════════════════════════════
# 1. Hallucination Detector
# ════════════════════════════════════════════════════════

def _collect_text_fields(data: Any, depth: int = 0, max_depth: int = 6) -> list[str]:
    """Recursively collect all string values from a nested dict/list."""
    if depth > max_depth:
        return []
    texts: list[str] = []
    if isinstance(data, str):
        texts.append(data)
    elif isinstance(data, dict):
        for v in data.values():
            texts.extend(_collect_text_fields(v, depth + 1, max_depth))
    elif isinstance(data, list):
        for item in data:
            texts.extend(_collect_text_fields(item, depth + 1, max_depth))
    return texts


def _hallucination_detector(
    report_data: dict,
    checks: list[dict],
    critical_failures: list[str],
    warnings: list[str],
) -> None:
    """Scan text fields for known hallucination patterns."""
    all_texts = _collect_text_fields(report_data)
    combined = "\n".join(all_texts)

    # 1a. Fake phone numbers
    fake_phones_found = []
    for pattern in _FAKE_PHONE_PATTERNS:
        matches = pattern.findall(combined)
        fake_phones_found.extend(matches)

    if fake_phones_found:
        msg = f"Обнаружены подозрительные телефоны: {', '.join(fake_phones_found[:3])}"
        checks.append({"name": "hallucination_fake_phones", "status": "fail", "message": msg})
        critical_failures.append(msg)
    else:
        checks.append({"name": "hallucination_fake_phones", "status": "pass", "message": "Подозрительных телефонов не найдено"})

    # 1b. Fake emails
    email_pattern = re.compile(r"[\w.+-]+@([\w-]+\.[\w.-]+)")
    found_domains = set()
    for m in email_pattern.finditer(combined):
        domain = m.group(1).lower()
        if domain in _FAKE_EMAIL_DOMAINS:
            found_domains.add(domain)

    if found_domains:
        msg = f"Обнаружены email с фейковых доменов: {', '.join(sorted(found_domains))}"
        checks.append({"name": "hallucination_fake_emails", "status": "fail", "message": msg})
        critical_failures.append(msg)
    else:
        checks.append({"name": "hallucination_fake_emails", "status": "pass", "message": "Фейковых email не найдено"})

    # 1c. Fake URLs
    fake_urls_found = []
    for pattern in _FAKE_URL_PATTERNS:
        matches = pattern.findall(combined)
        if matches:
            fake_urls_found.extend(pattern.findall(combined))

    if fake_urls_found:
        msg = f"Обнаружены фейковые URL (example.com/test.ru и т.д.)"
        checks.append({"name": "hallucination_fake_urls", "status": "fail", "message": msg})
        critical_failures.append(msg)
    else:
        checks.append({"name": "hallucination_fake_urls", "status": "pass", "message": "Фейковых URL не найдено"})

    # 1d. Suspiciously round revenue/profit
    financials = report_data.get("financials") or []
    round_numbers_found = []
    for fy in financials:
        if not isinstance(fy, dict):
            continue
        for field in ("revenue", "net_profit", "assets"):
            val = fy.get(field)
            if val is not None and isinstance(val, (int, float)):
                int_val = int(val)
                if int_val in _SUSPICIOUS_ROUND_NUMBERS and int_val != 0:
                    round_numbers_found.append(f"{field}={int_val} ({fy.get('year', '?')})")

    if round_numbers_found:
        msg = f"Подозрительно круглые числа в финансах: {', '.join(round_numbers_found[:5])}"
        checks.append({"name": "hallucination_round_numbers", "status": "warn", "message": msg})
        warnings.append(msg)
    else:
        checks.append({"name": "hallucination_round_numbers", "status": "pass", "message": "Подозрительно круглых чисел нет"})

    # 1e. Opinions without source references
    opinions = report_data.get("opinions") or []
    unsourced_quotes = 0
    for op in opinions:
        if not isinstance(op, dict):
            continue
        has_source = bool(op.get("source") or op.get("source_url"))
        if not has_source:
            unsourced_quotes += 1

    if unsourced_quotes > 0:
        msg = f"{unsourced_quotes} из {len(opinions)} цитат без указания источника"
        if unsourced_quotes == len(opinions) and len(opinions) > 0:
            checks.append({"name": "hallucination_unsourced_quotes", "status": "fail", "message": msg})
            critical_failures.append(msg)
        else:
            checks.append({"name": "hallucination_unsourced_quotes", "status": "warn", "message": msg})
            warnings.append(msg)
    else:
        status = "pass" if opinions else "warn"
        msg = "Все цитаты имеют источники" if opinions else "Цитаты отсутствуют"
        checks.append({"name": "hallucination_unsourced_quotes", "status": status, "message": msg})
        if not opinions:
            warnings.append(msg)


# ════════════════════════════════════════════════════════
# 2. Completeness Check
# ════════════════════════════════════════════════════════

def _completeness_check(
    report_data: dict,
    company_info: dict,
    checks: list[dict],
    critical_failures: list[str],
    warnings: list[str],
) -> None:
    """Verify required sections have data."""

    # 2a. Company info has name, inn, industry
    company = report_data.get("company") or {}
    has_name = bool(company.get("name"))
    has_inn = bool(company.get("inn"))
    has_btype = bool(company.get("business_type"))

    missing_fields = []
    if not has_name:
        missing_fields.append("name")
    if not has_inn:
        missing_fields.append("inn")
    if not has_btype:
        missing_fields.append("business_type")

    if not missing_fields:
        checks.append({"name": "completeness_company_info", "status": "pass", "message": "Данные компании заполнены (name, inn, business_type)"})
    elif "name" in missing_fields:
        msg = f"Отсутствуют данные компании: {', '.join(missing_fields)}"
        checks.append({"name": "completeness_company_info", "status": "fail", "message": msg})
        critical_failures.append(msg)
    else:
        msg = f"Неполные данные компании: {', '.join(missing_fields)}"
        checks.append({"name": "completeness_company_info", "status": "warn", "message": msg})
        warnings.append(msg)

    # 2b. FNS data — at least has revenue
    financials = report_data.get("financials") or []
    has_revenue = any(
        isinstance(f, dict) and f.get("revenue") is not None
        for f in financials
    )

    if has_revenue:
        checks.append({"name": "completeness_fns_data", "status": "pass", "message": f"Финансовые данные есть ({len(financials)} лет)"})
    elif financials:
        msg = "Финансовые данные есть, но нет выручки"
        checks.append({"name": "completeness_fns_data", "status": "warn", "message": msg})
        warnings.append(msg)
    else:
        msg = "Финансовые данные отсутствуют"
        checks.append({"name": "completeness_fns_data", "status": "warn", "message": msg})
        warnings.append(msg)

    # 2c. Competitors list has >= 3 entries
    competitors = report_data.get("competitors") or []
    comp_count = len(competitors)

    if comp_count >= 3:
        checks.append({"name": "completeness_competitors", "status": "pass", "message": f"Конкурентов: {comp_count}"})
    elif comp_count > 0:
        msg = f"Мало конкурентов: {comp_count} (рекомендуется >= 3)"
        checks.append({"name": "completeness_competitors", "status": "warn", "message": msg})
        warnings.append(msg)
    else:
        msg = "Конкуренты отсутствуют"
        checks.append({"name": "completeness_competitors", "status": "fail", "message": msg})
        critical_failures.append(msg)

    # 2d. At least 3 of 7 deep analysis sections present
    deep_sections = {
        "market": report_data.get("market"),
        "swot": report_data.get("swot"),
        "recommendations": report_data.get("recommendations"),
        "scenarios": report_data.get("scenarios"),
        "glossary": report_data.get("glossary"),
        "founders": report_data.get("founders"),
        "hr_data": report_data.get("hr_data"),
    }

    present_sections = []
    missing_sections = []
    for section_name, section_data in deep_sections.items():
        if section_data:
            # For lists, check non-empty; for dicts, check has keys
            if isinstance(section_data, list) and len(section_data) > 0:
                present_sections.append(section_name)
            elif isinstance(section_data, dict) and len(section_data) > 0:
                present_sections.append(section_name)
            else:
                missing_sections.append(section_name)
        else:
            missing_sections.append(section_name)

    present_count = len(present_sections)
    if present_count >= 5:
        checks.append({"name": "completeness_deep_sections", "status": "pass", "message": f"Секций анализа: {present_count}/7"})
    elif present_count >= 3:
        msg = f"Секций анализа: {present_count}/7 (отсутствуют: {', '.join(missing_sections)})"
        checks.append({"name": "completeness_deep_sections", "status": "warn", "message": msg})
        warnings.append(msg)
    else:
        msg = f"Недостаточно секций: {present_count}/7 (минимум 3). Отсутствуют: {', '.join(missing_sections)}"
        checks.append({"name": "completeness_deep_sections", "status": "fail", "message": msg})
        critical_failures.append(msg)


# ════════════════════════════════════════════════════════
# 3. Consistency Check
# ════════════════════════════════════════════════════════

def _extract_revenue_from_financials(financials: list) -> float | None:
    """Get the latest revenue from financials list."""
    if not financials:
        return None
    for fy in reversed(financials):
        if isinstance(fy, dict) and fy.get("revenue") is not None:
            return float(fy["revenue"])
    return None


def _extract_employees_from_financials(financials: list) -> int | None:
    """Get the latest employee count from financials list."""
    if not financials:
        return None
    for fy in reversed(financials):
        if isinstance(fy, dict) and fy.get("employees") is not None:
            return int(fy["employees"])
    return None


def _consistency_check(
    report_data: dict,
    checks: list[dict],
    critical_failures: list[str],
    warnings: list[str],
) -> None:
    """Cross-validate numbers across sections."""

    financials = report_data.get("financials") or []

    # 3a. Revenue consistency: FNS financials vs market_share implications
    fns_revenue = _extract_revenue_from_financials(financials)
    market_share = report_data.get("market_share") or {}
    company_name = (report_data.get("company") or {}).get("name", "")

    if fns_revenue is not None and market_share and company_name:
        company_share = None
        for name, share in market_share.items():
            if name.lower() == company_name.lower():
                company_share = share
                break

        if company_share is not None and company_share > 0:
            # Implied market size from share
            implied_market = fns_revenue / (company_share / 100)
            # Check if market size in market overview is roughly consistent
            market_data = report_data.get("market") or {}
            market_size_str = market_data.get("market_size", "")
            # Just verify the share is within reasonable bounds (0-50%)
            if company_share > 50:
                msg = f"Доля рынка компании > 50% ({company_share}%) — подозрительно высоко"
                checks.append({"name": "consistency_market_share", "status": "warn", "message": msg})
                warnings.append(msg)
            else:
                checks.append({"name": "consistency_market_share", "status": "pass", "message": f"Доля рынка {company_share}% — в допустимых пределах"})
        else:
            checks.append({"name": "consistency_market_share", "status": "pass", "message": "Доля рынка: данные для проверки отсутствуют"})
    else:
        checks.append({"name": "consistency_market_share", "status": "pass", "message": "Перекрёстная проверка рыночных данных пропущена (нет данных)"})

    # 3b. Employee count consistency: financials vs hr_data
    fns_employees = _extract_employees_from_financials(financials)
    hr_data = report_data.get("hr_data") or {}
    hr_employees = hr_data.get("employees_count")

    if fns_employees is not None and hr_employees is not None:
        try:
            hr_emp_int = int(hr_employees)
            if hr_emp_int > 0 and fns_employees > 0:
                ratio = max(fns_employees, hr_emp_int) / min(fns_employees, hr_emp_int)
                if ratio > 3:
                    msg = (
                        f"Расхождение сотрудников: ФНС={fns_employees}, HR={hr_emp_int} "
                        f"(разница в {ratio:.1f}x)"
                    )
                    checks.append({"name": "consistency_employees", "status": "warn", "message": msg})
                    warnings.append(msg)
                else:
                    checks.append({"name": "consistency_employees", "status": "pass", "message": f"Сотрудники: ФНС={fns_employees}, HR={hr_emp_int} — согласовано"})
            else:
                checks.append({"name": "consistency_employees", "status": "pass", "message": "Данные по сотрудникам неполные для сверки"})
        except (ValueError, TypeError):
            checks.append({"name": "consistency_employees", "status": "pass", "message": "HR employee count не числовое, сверка пропущена"})
    else:
        checks.append({"name": "consistency_employees", "status": "pass", "message": "Сверка сотрудников пропущена (нет данных)"})

    # 3c. Scenarios consistency: base scenario revenue should be close to actual
    scenarios = report_data.get("scenarios") or []
    if fns_revenue is not None and scenarios:
        base_scenario = None
        for sc in scenarios:
            if isinstance(sc, dict) and sc.get("name") == "base":
                base_scenario = sc
                break

        if base_scenario:
            base_metrics = base_scenario.get("metrics") or {}
            # Try to find revenue in various key formats
            base_rev = None
            for key in base_metrics:
                if "выручка" in key.lower() or "revenue" in key.lower():
                    base_rev = base_metrics[key]
                    break

            if base_rev is not None and isinstance(base_rev, (int, float)) and base_rev > 0:
                ratio = base_rev / fns_revenue if fns_revenue != 0 else 0
                if ratio < 0.3 or ratio > 5:
                    msg = (
                        f"Базовый сценарий сильно отличается от текущей выручки: "
                        f"сценарий={base_rev:.0f}, ФНС={fns_revenue:.0f} (x{ratio:.1f})"
                    )
                    checks.append({"name": "consistency_scenarios", "status": "warn", "message": msg})
                    warnings.append(msg)
                else:
                    checks.append({"name": "consistency_scenarios", "status": "pass", "message": "Сценарии согласованы с текущими финансами"})
            else:
                checks.append({"name": "consistency_scenarios", "status": "pass", "message": "Сверка сценариев пропущена"})
        else:
            checks.append({"name": "consistency_scenarios", "status": "pass", "message": "Базовый сценарий отсутствует"})
    else:
        checks.append({"name": "consistency_scenarios", "status": "pass", "message": "Сверка сценариев пропущена (нет данных)"})


# ════════════════════════════════════════════════════════
# 4. Source Coverage
# ════════════════════════════════════════════════════════

def _source_coverage(
    report_data: dict,
    checks: list[dict],
    critical_failures: list[str],
    warnings: list[str],
) -> None:
    """Check data sourcing quality: FACT vs ESTIMATE ratio."""

    calc_traces = report_data.get("calc_traces") or []

    if not calc_traces:
        checks.append({"name": "source_coverage_traces", "status": "warn", "message": "Calc traces отсутствуют — прозрачность расчётов не обеспечена"})
        warnings.append("Calc traces отсутствуют")
        return

    fact_count = 0
    calc_count = 0
    estimate_count = 0
    critical_estimates = []

    for ct in calc_traces:
        if not isinstance(ct, dict):
            continue
        confidence = str(ct.get("confidence", "ESTIMATE")).upper()
        metric_name = ct.get("metric_name", "?")

        if confidence == "FACT":
            fact_count += 1
        elif confidence == "CALC":
            calc_count += 1
        else:
            estimate_count += 1
            # Check if this is a critical metric
            name_lower = metric_name.lower()
            if any(kw in name_lower for kw in ("выручка", "revenue", "прибыль", "profit", "сотрудник", "employee")):
                critical_estimates.append(metric_name)

    total = fact_count + calc_count + estimate_count
    if total == 0:
        checks.append({"name": "source_coverage_traces", "status": "warn", "message": "Calc traces пусты"})
        warnings.append("Calc traces пусты")
        return

    estimate_pct = estimate_count / total * 100

    # Check overall estimate percentage
    if estimate_pct > 60:
        msg = f">{estimate_pct:.0f}% данных — ESTIMATE ({estimate_count}/{total}). Рекомендуется < 60%"
        checks.append({"name": "source_coverage_estimate_ratio", "status": "warn", "message": msg})
        warnings.append(msg)
    else:
        checks.append({"name": "source_coverage_estimate_ratio", "status": "pass",
                        "message": f"FACT: {fact_count}, CALC: {calc_count}, ESTIMATE: {estimate_count} ({estimate_pct:.0f}%)"})

    # Check critical metrics
    if critical_estimates:
        msg = f"Критические метрики с ESTIMATE: {', '.join(critical_estimates[:5])}"
        checks.append({"name": "source_coverage_critical", "status": "warn", "message": msg})
        warnings.append(msg)
    else:
        checks.append({"name": "source_coverage_critical", "status": "pass",
                        "message": "Критические метрики (выручка, прибыль, сотрудники) не содержат ESTIMATE"})

    # Factcheck items coverage
    factcheck = report_data.get("factcheck") or []
    if factcheck:
        verified = sum(1 for f in factcheck if isinstance(f, dict) and f.get("verified"))
        multi_source = sum(1 for f in factcheck if isinstance(f, dict) and (f.get("sources_count", 0) >= 2))
        msg = f"Фактчек: {len(factcheck)} фактов, {verified} верифицированы, {multi_source} с 2+ источниками"
        status = "pass" if multi_source >= len(factcheck) * 0.5 else "warn"
        checks.append({"name": "source_coverage_factcheck", "status": status, "message": msg})
        if status == "warn":
            warnings.append(f"Менее 50% фактов имеют 2+ источника ({multi_source}/{len(factcheck)})")
    else:
        checks.append({"name": "source_coverage_factcheck", "status": "pass", "message": "Секция фактчека пуста (нормально если нет ручных проверок)"})


# ════════════════════════════════════════════════════════
# 5. Competitor Validation
# ════════════════════════════════════════════════════════

def _competitor_validation(
    report_data: dict,
    company_info: dict,
    checks: list[dict],
    critical_failures: list[str],
    warnings: list[str],
) -> None:
    """Basic competitor data validation."""

    competitors = report_data.get("competitors") or []
    company_name = (report_data.get("company") or {}).get("name", "")
    company_name_alt = company_info.get("name", "")

    if not competitors:
        # Already caught by completeness check; skip here
        return

    # 5a. All competitors have names
    nameless = [i for i, c in enumerate(competitors) if not isinstance(c, dict) or not c.get("name")]
    if nameless:
        msg = f"Конкуренты без названий: позиции {nameless}"
        checks.append({"name": "competitor_nameless", "status": "fail", "message": msg})
        critical_failures.append(msg)
    else:
        checks.append({"name": "competitor_nameless", "status": "pass", "message": "Все конкуренты имеют названия"})

    # 5b. No duplicate competitors
    names = [c.get("name", "").strip().lower() for c in competitors if isinstance(c, dict)]
    seen = set()
    duplicates = set()
    for n in names:
        if n in seen:
            duplicates.add(n)
        seen.add(n)

    if duplicates:
        msg = f"Дубликаты конкурентов: {', '.join(duplicates)}"
        checks.append({"name": "competitor_duplicates", "status": "fail", "message": msg})
        critical_failures.append(msg)
    else:
        checks.append({"name": "competitor_duplicates", "status": "pass", "message": "Дубликатов конкурентов нет"})

    # 5c. Company is not listed as its own competitor
    company_lower = company_name.lower().strip()
    company_alt_lower = company_name_alt.lower().strip()

    self_listed = False
    for c in competitors:
        if not isinstance(c, dict):
            continue
        cname = (c.get("name") or "").lower().strip()
        if cname and (cname == company_lower or cname == company_alt_lower):
            self_listed = True
            break

    if self_listed:
        msg = f"Компания '{company_name}' указана как собственный конкурент"
        checks.append({"name": "competitor_self_listed", "status": "fail", "message": msg})
        critical_failures.append(msg)
    else:
        checks.append({"name": "competitor_self_listed", "status": "pass", "message": "Компания не указана как свой конкурент"})

    # 5d. Radar scores completeness
    radar_dims = report_data.get("radar_dimensions") or []
    if radar_dims:
        incomplete_radar = 0
        for c in competitors:
            if not isinstance(c, dict):
                continue
            scores = c.get("radar_scores") or {}
            if len(scores) < len(radar_dims):
                incomplete_radar += 1

        if incomplete_radar > 0:
            msg = f"{incomplete_radar} конкурентов с неполными radar scores (ожидается {len(radar_dims)} параметров)"
            checks.append({"name": "competitor_radar", "status": "warn", "message": msg})
            warnings.append(msg)
        else:
            checks.append({"name": "competitor_radar", "status": "pass", "message": f"Radar scores заполнены ({len(radar_dims)} параметров)"})
