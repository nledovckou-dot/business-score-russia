"""FNS API client: search companies, get financials, founders, affiliates."""

from __future__ import annotations

import json
import urllib.request
import urllib.error
import urllib.parse
from typing import Any, Optional


FNS_SEARCH_URL = "https://api-fns.ru/api/search"
FNS_EGRUL_URL = "https://api-fns.ru/api/egr"
FNS_BO_URL = "https://api-fns.ru/api/bo"  # бухгалтерская отчётность


def _fns_key() -> str:
    """Get FNS API key from env."""
    import os
    key = os.environ.get("FNS_API_KEY", "")
    if not key:
        raise RuntimeError("FNS_API_KEY not set. Get one at https://api-fns.ru/")
    return key


def _get(url: str, params: dict) -> dict:
    """HTTP GET with params."""
    params["key"] = _fns_key()
    qs = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items())
    full_url = f"{url}?{qs}"
    req = urllib.request.Request(full_url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        raise RuntimeError(f"FNS API error {e.code}: {error_body[:300]}")


def search_company(query: str, limit: int = 5) -> list[dict]:
    """Search companies by name or INN.

    Returns list of dicts with keys:
    - inn, ogrn, name, full_name, okved_name, address, status, reg_date
    """
    data = _get(FNS_SEARCH_URL, {"q": query, "limit": limit})
    items = data.get("items", [])

    results = []
    for item in items:
        ul = item.get("ЮЛ") or item.get("ИП") or {}
        if not ul:
            continue

        result = {
            "inn": ul.get("ИНН", ""),
            "ogrn": ul.get("ОГРН", ""),
            "name": ul.get("НаимСокрЮЛ", ""),
            "full_name": ul.get("НаимПолнЮЛ", ""),
            "okved_name": ul.get("ОснВидДеят", ""),
            "address": ul.get("АдресПолн", ""),
            "status": ul.get("Статус", ""),
            "reg_date": ul.get("ДатаОГРН", ""),
        }

        # For ИП format
        if not result["name"] and "ФИОПолн" in ul:
            result["name"] = f"ИП {ul['ФИОПолн']}"

        results.append(result)

    return results


def get_egrul(inn: str) -> dict:
    """Get EGRUL data by INN: founders, capital, licenses, affiliates."""
    data = _get(FNS_EGRUL_URL, {"req": inn})
    items = data.get("items", [])
    if not items:
        return {}

    ul = items[0].get("ЮЛ") or items[0].get("ИП") or {}

    # Extract founders
    founders = []
    for f in ul.get("Учредители", []):
        if "УчрФЛ" in f:
            # Physical person
            fl = f["УчрФЛ"]
            founder = {
                "name": fl.get("ФИОПолн", ""),
                "inn": fl.get("ИНН", ""),
                "share": f.get("СуммаУК", ""),
                "share_percent": f.get("Процент", ""),
                "type": "physical",
            }
        elif "УчрЮЛ" in f:
            # Legal entity
            yl = f["УчрЮЛ"]
            founder = {
                "name": yl.get("НаимСокрЮЛ", yl.get("НаимПолнЮЛ", "")),
                "inn": yl.get("ИНН", ""),
                "share": f.get("СуммаУК", ""),
                "share_percent": f.get("Процент", ""),
                "type": "legal",
            }
        else:
            continue
        founders.append(founder)

    # Extract director
    director = {}
    if "Руководитель" in ul:
        d = ul["Руководитель"]
        director = {
            "name": d.get("ФИОПолн", ""),
            "inn": d.get("ИНН", ""),
            "position": d.get("Должн", "Генеральный директор"),
        }

    # Extract OKVED
    osn_vid = ul.get("ОснВидДеят", {})
    okved_code = osn_vid.get("Код", "") if isinstance(osn_vid, dict) else ""
    okved_name = osn_vid.get("Текст", "") if isinstance(osn_vid, dict) else str(osn_vid)

    # Capital
    capital_info = ul.get("Капитал", {})
    capital = capital_info.get("СумКап", "") if isinstance(capital_info, dict) else ""

    # Address
    addr = ul.get("Адрес", {})
    address_full = addr.get("АдресПолн", "") if isinstance(addr, dict) else ""

    return {
        "inn": ul.get("ИНН", ""),
        "ogrn": ul.get("ОГРН", ""),
        "name": ul.get("НаимСокрЮЛ", ""),
        "full_name": ul.get("НаимПолнЮЛ", ""),
        "okved": okved_code,
        "okved_name": okved_name,
        "capital": capital,
        "reg_date": ul.get("ДатаРег", ul.get("ДатаОГРН", "")),
        "status": ul.get("Статус", ""),
        "director": director,
        "founders": founders,
        "address": address_full,
    }


def get_financials(inn: str) -> list[dict]:
    """Get financial statements (бухотчётность) by INN.

    api-fns.ru/api/bo returns: {INN: {year: {code: value, ...}, ...}}
    Codes: 2110=выручка, 2400=чистая прибыль, 1600=активы, 1300=капитал,
           1500=краткосроч.обяз., 1400=долгосроч.обяз., 1150=осн.средства
    All values in thousands of rubles.
    """
    data = _get(FNS_BO_URL, {"req": inn})

    # Response format: {inn: {year: {code: value}}}
    inn_data = data.get(inn, {})
    if not inn_data:
        # Try first key if INN doesn't match exactly
        for key in data:
            if isinstance(data[key], dict) and any(y.isdigit() for y in data[key]):
                inn_data = data[key]
                break

    financials = []
    for year_str, codes in inn_data.items():
        if not year_str.isdigit() or not isinstance(codes, dict):
            continue
        year_data = {
            "year": int(year_str),
            "revenue": _parse_num(codes.get("2110")),
            "net_profit": _parse_num(codes.get("2400")),
            "assets": _parse_num(codes.get("1600")),
            "equity": _parse_num(codes.get("1300")),
            "liabilities": _parse_num(codes.get("1500")),
            "long_liabilities": _parse_num(codes.get("1400")),
            "fixed_assets": _parse_num(codes.get("1150")),
            "employees": _parse_num(codes.get("СОТР")),
        }
        financials.append(year_data)

    financials.sort(key=lambda x: x["year"])
    return financials


def get_affiliates(inn: str, founders: list[dict]) -> list[dict]:
    """Find affiliated companies through founders.

    For each founder with INN, search for other companies they own.
    """
    affiliates = []
    seen_inns = {inn}  # exclude the company itself

    for founder in founders:
        f_inn = founder.get("inn", "")
        f_name = founder.get("name", "")
        if not f_name:
            continue

        # Search by founder name
        try:
            results = search_company(f_name, limit=10)
            for r in results:
                if r["inn"] and r["inn"] not in seen_inns:
                    seen_inns.add(r["inn"])
                    affiliates.append({
                        "inn": r["inn"],
                        "name": r["name"] or r["full_name"],
                        "okved": r["okved"],
                        "okved_name": r["okved_name"],
                        "connection": f"Учредитель: {f_name}",
                        "status": r["status"],
                    })
        except Exception:
            pass

    return affiliates


def _parse_num(val: Any) -> Optional[float]:
    """Parse a numeric value from FNS response."""
    if val is None:
        return None
    try:
        return float(str(val).replace(" ", "").replace(",", "."))
    except (ValueError, TypeError):
        return None
