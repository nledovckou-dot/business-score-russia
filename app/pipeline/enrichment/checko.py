"""Checko.ru API client: company data, financials, contracts, risks.

Provides ЕГРЮЛ data, full financial statements (2014-2024),
government contracts, court cases, bankruptcy, enforcement proceedings.

Auth: key as query parameter.
Docs: https://checko.ru/integration/api
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

logger = logging.getLogger(__name__)

_API_BASE = "https://api.checko.ru/v2"
_lock = threading.Lock()
_last_call: float = 0.0
_MIN_INTERVAL = 0.5  # conservative rate limiting


def _api_key() -> str:
    return os.environ.get("CHECKO_API_KEY", "dHL2dcu0gcn3Hqfz")


def _get(endpoint: str, params: dict, timeout: int = 20) -> dict:
    """HTTP GET with rate limiting."""
    global _last_call

    params["key"] = _api_key()
    qs = urllib.parse.urlencode(params, encoding="utf-8")
    url = f"{_API_BASE}/{endpoint}?{qs}"

    with _lock:
        now = time.monotonic()
        wait = _MIN_INTERVAL - (now - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.monotonic()

    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "BSR-Pipeline/1.0",
    })

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            meta = data.get("meta", {})
            if meta.get("status") != "ok":
                logger.warning("[checko] Status not ok: %s", meta)
            return data
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8")[:300] if e.fp else ""
        except Exception:
            pass
        logger.error("[checko] HTTP %d on %s: %s", e.code, endpoint, body)
        raise RuntimeError(f"Checko API error {e.code}: {body}") from e
    except Exception as e:
        logger.error("[checko] Request failed: %s", str(e)[:200])
        raise RuntimeError(f"Checko request failed: {e}") from e


def get_company(inn: str) -> dict | None:
    """Get company data from ЕГРЮЛ: directors, founders, contacts, risks.

    Args:
        inn: Company INN (10 or 12 digits)

    Returns:
        Normalized dict or None.
    """
    if not inn or len(inn) < 10:
        return None

    try:
        raw = _get("company", {"inn": inn})
    except Exception as e:
        logger.warning("[checko] Company failed for INN %s: %s", inn, str(e)[:200])
        return None

    data = raw.get("data", {})
    if not data:
        return None

    # Parse directors
    directors = []
    for d in data.get("Руковод", []):
        directors.append({
            "name": d.get("ФИО", ""),
            "inn": d.get("ИНН", ""),
            "position": d.get("НаимДолжн", ""),
            "is_mass_director": d.get("МассРуковод", False),
            "is_disqualified": d.get("ДисквЛицо", False),
            "related_companies": d.get("СвязРуковод", []),
        })

    # Parse founders
    founders = []
    for f in data.get("Учред", {}).get("ФЛ", []):
        founders.append({
            "name": f.get("ФИО", ""),
            "inn": f.get("ИНН", ""),
            "share_percent": f.get("Доля", {}).get("Процент"),
            "share_nominal": f.get("Доля", {}).get("Номинал"),
            "type": "physical",
            "related_companies": f.get("СвязУчред", []),
        })
    for f in data.get("Учред", {}).get("РосОрг", []):
        founders.append({
            "name": f.get("НаимСокр") or f.get("НаимПолн", ""),
            "inn": f.get("ИНН", ""),
            "ogrn": f.get("ОГРН", ""),
            "share_percent": f.get("Доля", {}).get("Процент"),
            "type": "legal",
        })

    # Parse contacts
    contacts = data.get("Контакты", {})

    # Parse risks
    risks = {
        "mass_director": data.get("МассРуковод", False),
        "mass_founder": data.get("МассУчред", False),
        "mass_address": bool(data.get("ЮрАдрес", {}).get("МассАдрес")),
        "disqualified_person": data.get("ДисквЛица", False),
        "unfair_supplier": data.get("НедобПост", False),
        "illegal_finance": data.get("НелегалФин", False),
        "sanctions": data.get("Санкции", False),
        "sanctions_founder": data.get("СанкцУчр", False),
    }
    risk_count = sum(1 for v in risks.values() if v)

    result = {
        "inn": data.get("ИНН", inn),
        "ogrn": data.get("ОГРН", ""),
        "name_short": data.get("НаимСокр", ""),
        "name_full": data.get("НаимПолн", ""),
        "status": data.get("Статус", {}).get("Наим", ""),
        "reg_date": data.get("ДатаРег", ""),
        "okved": data.get("ОКВЭД", {}).get("Код", ""),
        "okved_name": data.get("ОКВЭД", {}).get("Наим", ""),
        "okved_extra_count": len(data.get("ОКВЭДДоп", [])),
        "capital": data.get("УстКап", {}).get("Сумма"),
        "employees": data.get("СЧР"),
        "employees_year": data.get("СЧРГод"),
        "address": data.get("ЮрАдрес", {}).get("АдресРФ", ""),
        "region": data.get("Регион", {}).get("Наим", ""),
        "directors": directors,
        "founders": founders,
        "contacts": {
            "phones": contacts.get("Тел", []),
            "emails": contacts.get("Емэйл", []),
            "website": contacts.get("ВебСайт", ""),
            "telegram": contacts.get("Телеграм", ""),
        },
        "risks": risks,
        "risk_count": risk_count,
        "msp_category": data.get("РМСП", {}).get("Категория", ""),
    }

    logger.info("[checko] Company '%s' (INN %s): status=%s, employees=%s, risks=%d",
                result["name_short"], inn, result["status"], result["employees"], risk_count)
    return result


def get_finances(inn: str) -> dict | None:
    """Get full financial statements (бухотчётность) for all available years.

    Returns dict: {year: {revenue, net_profit, assets, equity, ...}}
    """
    if not inn or len(inn) < 10:
        return None

    try:
        raw = _get("finances", {"inn": inn})
    except Exception as e:
        logger.warning("[checko] Finances failed for INN %s: %s", inn, str(e)[:200])
        return None

    data = raw.get("data", {})
    if not data or not isinstance(data, dict):
        return None

    result = {}
    for year, values in sorted(data.items()):
        if not isinstance(values, dict):
            continue
        try:
            year_int = int(year)
        except ValueError:
            continue

        # Key financial lines (in rubles, from бухотчётность)
        result[year] = {
            "year": year_int,
            "revenue": values.get("2110"),           # Выручка
            "cost_of_sales": values.get("2120"),     # Себестоимость
            "gross_profit": values.get("2100"),      # Валовая прибыль
            "operating_profit": values.get("2200"),  # Прибыль от продаж
            "ebitda_proxy": values.get("2300"),      # Прибыль до налогов (≈EBITDA)
            "net_profit": values.get("2400"),        # Чистая прибыль
            "assets": values.get("1600"),            # Активы
            "equity": values.get("1300"),            # Собственный капитал
            "long_term_debt": values.get("1400"),    # Долгосрочные обязательства
            "short_term_debt": values.get("1500"),   # Краткосрочные обязательства
            "fixed_assets": values.get("1150"),      # Основные средства
            "current_assets": values.get("1200"),    # Оборотные активы
            "cash": values.get("1250"),              # Денежные средства
        }

    if result:
        years = sorted(result.keys())
        logger.info("[checko] Finances for INN %s: %d years (%s-%s)",
                    inn, len(result), years[0], years[-1])
    else:
        logger.info("[checko] No financial data for INN %s", inn)

    return result if result else None


def get_contracts(inn: str, law: str = "44", limit: int = 20) -> list[dict]:
    """Get government contracts (госзакупки).

    Args:
        inn: Company INN
        law: "44" (44-ФЗ), "223" (223-ФЗ), or "94" (94-ФЗ)
        limit: Max results
    """
    if not inn:
        return []

    try:
        raw = _get("contracts", {"inn": inn, "law": law, "role": "supplier", "sort": "-date"})
    except Exception as e:
        logger.warning("[checko] Contracts failed for INN %s: %s", inn, str(e)[:200])
        return []

    records = raw.get("data", {}).get("Записи", [])
    results = []
    for r in records[:limit]:
        results.append({
            "reg_number": r.get("РегНомер", ""),
            "date": r.get("Дата", ""),
            "price": r.get("Цена", 0),
            "customer_name": r.get("Заказ", {}).get("НаимСокр", ""),
            "customer_inn": r.get("Заказ", {}).get("ИНН", ""),
            "objects": [o.get("Наим", "") for o in r.get("Объекты", [])],
            "url": r.get("СтрЕИС", ""),
        })

    logger.info("[checko] Contracts for INN %s (law=%s): %d found", inn, law, len(results))
    return results


def get_legal_cases(inn: str) -> list[dict]:
    """Get arbitration court cases."""
    if not inn:
        return []

    try:
        raw = _get("legal-cases", {"inn": inn})
    except Exception as e:
        logger.warning("[checko] Legal cases failed for INN %s: %s", inn, str(e)[:200])
        return []

    data = raw.get("data", {})
    if isinstance(data, list):
        cases = data
    elif isinstance(data, dict):
        cases = data.get("Записи", data.get("items", []))
    else:
        cases = []

    logger.info("[checko] Legal cases for INN %s: %d found", inn, len(cases))
    return cases[:20]


def search_company(name: str, limit: int = 5) -> list[dict]:
    """Search companies by name. Returns list of {inn, name, status, address}."""
    if not name:
        return []

    try:
        raw = _get("search", {"query": name, "limit": str(limit)})
    except Exception as e:
        logger.warning("[checko] Search failed for '%s': %s", name, str(e)[:200])
        return []

    items = raw.get("data", [])
    if not isinstance(items, list):
        items = []

    results = []
    for item in items[:limit]:
        results.append({
            "inn": item.get("ИНН", ""),
            "ogrn": item.get("ОГРН", ""),
            "name": item.get("НаимСокр") or item.get("НаимПолн", ""),
            "status": item.get("Статус", {}).get("Наим", "") if isinstance(item.get("Статус"), dict) else "",
            "address": item.get("ЮрАдрес", {}).get("АдресРФ", "") if isinstance(item.get("ЮрАдрес"), dict) else "",
            "reg_date": item.get("ДатаРег", ""),
        })

    logger.info("[checko] Search '%s': %d results", name, len(results))
    return results
