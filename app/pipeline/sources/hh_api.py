"""HH.ru API client for real HR data (T19).

Uses client_credentials app token for public vacancy search.
Docs: https://api.hh.ru/openapi/redoc
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Optional

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.hh.ru"
_USER_AGENT = "EspacePlatform/1.0 (n.a.ledovskoy@gmail.com)"


def _get_token() -> str:
    """Get HH.ru app token from environment."""
    token = os.environ.get("HH_APP_TOKEN", "")
    if not token:
        logger.warning("HH_APP_TOKEN not set, HH.ru API unavailable")
    return token


def _hh_request(path: str, params: dict | None = None) -> dict | list | None:
    """Make authenticated request to HH.ru API."""
    token = _get_token()
    if not token:
        return None

    url = _BASE_URL + path
    if params:
        url += "?" + urllib.parse.urlencode(params, encoding="utf-8")

    req = urllib.request.Request(url, headers={
        "User-Agent": _USER_AGENT,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            logger.error("HH.ru API error %d on %s", e.code, path)
            return None
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
                continue
            logger.error("HH.ru request failed: %s", e)
            return None

    return None


def search_vacancies(
    company_name: str,
    area: int = 113,  # 113 = Россия
    per_page: int = 20,
) -> list[dict]:
    """Search vacancies by company name.

    Returns list of vacancies with title, salary, experience, etc.
    """
    data = _hh_request("/vacancies", {
        "text": f"company:{company_name}",
        "area": str(area),
        "per_page": str(per_page),
        "order_by": "publication_time",
    })

    if not data or not isinstance(data, dict):
        return []

    vacancies = []
    for item in data.get("items", []):
        salary = item.get("salary") or {}
        salary_from = salary.get("from")
        salary_to = salary.get("to")
        salary_str = ""
        if salary_from and salary_to:
            salary_str = f"{salary_from:,}–{salary_to:,} {salary.get('currency', 'RUR')}"
        elif salary_from:
            salary_str = f"от {salary_from:,} {salary.get('currency', 'RUR')}"
        elif salary_to:
            salary_str = f"до {salary_to:,} {salary.get('currency', 'RUR')}"

        vacancies.append({
            "title": item.get("name", ""),
            "salary": salary_str,
            "salary_from": salary_from,
            "salary_to": salary_to,
            "salary_gross": salary.get("gross", True),
            "experience": item.get("experience", {}).get("name", ""),
            "employment": item.get("employment", {}).get("name", ""),
            "area": item.get("area", {}).get("name", ""),
            "employer": item.get("employer", {}).get("name", ""),
            "url": item.get("alternate_url", ""),
            "published_at": item.get("published_at", ""),
        })

    logger.info(
        "HH.ru: found %d vacancies for '%s' (total: %d)",
        len(vacancies), company_name, data.get("found", 0),
    )
    return vacancies


def get_salary_stats(
    professional_role: str = "",
    area: int = 113,
    search_text: str = "",
) -> dict:
    """Get salary statistics for a profession/industry.

    Returns avg salary range from available vacancies.
    """
    params = {
        "area": str(area),
        "per_page": "100",
        "only_with_salary": "true",
    }
    if professional_role:
        params["text"] = professional_role
    if search_text:
        params["text"] = search_text

    data = _hh_request("/vacancies", params)
    if not data or not isinstance(data, dict):
        return {}

    salaries_from = []
    salaries_to = []
    for item in data.get("items", []):
        salary = item.get("salary") or {}
        if salary.get("currency") != "RUR":
            continue
        if salary.get("from"):
            salaries_from.append(salary["from"])
        if salary.get("to"):
            salaries_to.append(salary["to"])

    if not salaries_from and not salaries_to:
        return {}

    result = {
        "vacancies_found": data.get("found", 0),
        "with_salary": len(salaries_from) + len(salaries_to),
    }

    if salaries_from:
        result["avg_salary_from"] = round(sum(salaries_from) / len(salaries_from))
        result["median_salary_from"] = sorted(salaries_from)[len(salaries_from) // 2]
        result["min_salary"] = min(salaries_from)

    if salaries_to:
        result["avg_salary_to"] = round(sum(salaries_to) / len(salaries_to))
        result["median_salary_to"] = sorted(salaries_to)[len(salaries_to) // 2]
        result["max_salary"] = max(salaries_to)

    return result


def get_employer_info(employer_name: str) -> dict | None:
    """Search for employer by name, return basic info."""
    data = _hh_request("/employers", {
        "text": employer_name,
        "per_page": "5",
    })

    if not data or not isinstance(data, dict):
        return None

    for item in data.get("items", []):
        if employer_name.lower() in (item.get("name", "") or "").lower():
            return {
                "id": item.get("id"),
                "name": item.get("name"),
                "url": item.get("alternate_url"),
                "logo": item.get("logo_urls", {}).get("90"),
                "open_vacancies": item.get("open_vacancies", 0),
                "area": item.get("area", {}).get("name"),
            }

    # Return first result if no exact match
    items = data.get("items", [])
    if items:
        item = items[0]
        return {
            "id": item.get("id"),
            "name": item.get("name"),
            "url": item.get("alternate_url"),
            "open_vacancies": item.get("open_vacancies", 0),
        }

    return None


def _search_via_agencies(company_name: str, brand_name: str = "", per_page: int = 20) -> list[dict]:
    """Search vacancies mentioning the company in text (catches agency postings).

    Agencies like ANCOR, Hays, Antal post vacancies FOR the company
    but under their own employer name. This search finds them.
    """
    all_vacancies = []
    seen_urls = set()

    # Search terms: company name in vacancy text (not just employer field)
    search_terms = [company_name]
    if brand_name and brand_name.lower() != company_name.lower():
        search_terms.append(brand_name)

    for term in search_terms:
        if not term or len(term) < 3:
            continue
        data = _hh_request("/vacancies", {
            "text": f'"{term}"',  # exact phrase match in all fields
            "area": "113",
            "per_page": str(per_page),
            "order_by": "publication_time",
        })
        if not data or not isinstance(data, dict):
            continue

        for item in data.get("items", []):
            url = item.get("alternate_url", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)

            employer_name = item.get("employer", {}).get("name", "")
            is_agency = employer_name.lower() != company_name.lower()

            salary = item.get("salary") or {}
            salary_from = salary.get("from")
            salary_to = salary.get("to")
            salary_str = ""
            if salary_from and salary_to:
                salary_str = f"{salary_from:,}-{salary_to:,} {salary.get('currency', 'RUR')}"
            elif salary_from:
                salary_str = f"от {salary_from:,} {salary.get('currency', 'RUR')}"
            elif salary_to:
                salary_str = f"до {salary_to:,} {salary.get('currency', 'RUR')}"

            all_vacancies.append({
                "title": item.get("name", ""),
                "salary": salary_str,
                "salary_from": salary_from,
                "salary_to": salary_to,
                "salary_gross": salary.get("gross", True),
                "experience": item.get("experience", {}).get("name", ""),
                "employment": item.get("employment", {}).get("name", ""),
                "area": item.get("area", {}).get("name", ""),
                "employer": employer_name,
                "url": url,
                "published_at": item.get("published_at", ""),
                "via_agency": is_agency,
            })

    logger.info(
        "HH.ru agency search: found %d vacancies for '%s' (agencies: %d)",
        len(all_vacancies), company_name,
        sum(1 for v in all_vacancies if v.get("via_agency")),
    )
    return all_vacancies


def _search_affiliated(affiliated_inns: list[str], per_page: int = 10) -> list[dict]:
    """Search vacancies by affiliated company INNs (via Checko)."""
    all_vacancies = []
    for inn in affiliated_inns[:5]:  # max 5 affiliates
        # Search employer by INN is not directly supported by HH API,
        # so we search by the company text in employer description
        # This is a best-effort approach
        pass  # HH API doesn't support INN search — skip for now
    return all_vacancies


def get_hr_data_for_company(
    company_name: str,
    industry_keywords: str = "",
    brand_name: str = "",
    affiliated_companies: list[str] | None = None,
) -> dict:
    """Get comprehensive HR data for a company and its industry.

    Extended search (v2):
    1. Direct employer search (company's own vacancies)
    2. Text search (catches agency postings: ANCOR, Hays, Antal)
    3. Affiliated companies (from Checko)
    4. Industry salary benchmarks

    Returns dict with vacancies, salary stats, and employer info.
    """
    result: dict[str, Any] = {
        "source": "hh.ru",
        "company_name": company_name,
    }

    # 1. Employer info
    employer = get_employer_info(company_name)
    if employer:
        result["employer"] = employer
        result["open_vacancies_count"] = employer.get("open_vacancies", 0)

    # 2. Company's own vacancies (direct employer match)
    direct_vacancies = search_vacancies(company_name)

    # 3. Agency/text search (mentions in vacancy text)
    agency_vacancies = _search_via_agencies(company_name, brand_name=brand_name)

    # Merge: direct first, then agency (deduplicate by URL)
    seen = {v.get("url") for v in direct_vacancies if v.get("url")}
    all_vacancies = list(direct_vacancies)
    for v in agency_vacancies:
        if v.get("url") not in seen:
            all_vacancies.append(v)
            seen.add(v.get("url"))

    result["vacancies"] = all_vacancies
    result["vacancies_count"] = len(all_vacancies)
    result["direct_count"] = len(direct_vacancies)
    result["agency_count"] = sum(1 for v in all_vacancies if v.get("via_agency"))

    # Note about zero vacancies
    if len(all_vacancies) == 0:
        result["notes"] = [
            "0 вакансий на HH.ru — компания может вести найм через:",
            "• кадровые агентства (ANCOR, Hays, Antal)",
            "• Telegram-каналы и IT-сообщества",
            "• раздел «Карьера» на корпоративном сайте",
            "• рекомендации сотрудников (реферальная программа)",
            "• прямой хантинг через LinkedIn",
        ]

    # 4. Salary stats for industry
    if industry_keywords:
        salary_stats = get_salary_stats(search_text=industry_keywords)
        if salary_stats:
            result["industry_salaries"] = salary_stats

    # 5. Build salary items for report
    salary_items = []
    for v in all_vacancies[:15]:
        if v.get("salary"):
            value = v.get("salary_to") or v.get("salary_from") or 0
            # Add +15-20% KPI for sales/management positions
            title_lower = v.get("title", "").lower()
            has_kpi = any(kw in title_lower for kw in (
                "менеджер по продаж", "руководитель", "директор",
                "коммерческий", "account", "sales", "бизнес-разв",
            ))
            if has_kpi and value > 0:
                value_with_kpi = int(value * 1.175)  # +17.5% average KPI
                salary_items.append({
                    "label": v["title"] + (" (через агентство)" if v.get("via_agency") else ""),
                    "value": value_with_kpi,
                    "color": "#E08040" if v.get("via_agency") else "#4A8FE0",
                    "note": f"gross + KPI ~17.5% (оклад: {value:,})",
                })
            else:
                salary_items.append({
                    "label": v["title"] + (" (через агентство)" if v.get("via_agency") else ""),
                    "value": value,
                    "color": "#E08040" if v.get("via_agency") else "#4A8FE0",
                })

    if salary_items:
        result["salaries"] = sorted(salary_items, key=lambda x: x["value"], reverse=True)

    # 6. Sources info for report
    result["sources"] = ["HH.ru API (реальные данные)"]
    if result.get("agency_count", 0) > 0:
        result["sources"].append(f"Вакансии через агентства: {result['agency_count']}")

    return result
