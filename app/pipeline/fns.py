"""FNS API client: search companies, get financials, founders, affiliates.

Rate-limited: global lock ensures min 1.5s between API calls to avoid 403.
Retries on 403 (rate limit) with exponential backoff.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
from typing import Any, Optional

logger = logging.getLogger(__name__)

FNS_SEARCH_URL = "https://api-fns.ru/api/search"
FNS_EGRUL_URL = "https://api-fns.ru/api/egr"
FNS_BO_URL = "https://api-fns.ru/api/bo"  # бухгалтерская отчётность

# ── Rate limiting ──
_fns_lock = threading.Lock()
_fns_last_call: float = 0.0
_FNS_MIN_INTERVAL = 1.5  # seconds between API calls
_FNS_MAX_RETRIES = 3


def _fns_key() -> str:
    """Get FNS API key from env."""
    import os
    key = os.environ.get("FNS_API_KEY", "")
    if not key:
        raise RuntimeError("FNS_API_KEY not set. Get one at https://api-fns.ru/")
    return key


def _get_via_proxy(full_url: str) -> dict | None:
    """Try FNS request via Russian proxy (Yandex VPS) to bypass geo-blocks."""
    import requests as _req
    try:
        resp = _req.post(
            "http://158.160.158.164:8888/scrape",
            json={"url": full_url},
            headers={"X-Proxy-Token": "bsr-proxy-2026", "Content-Type": "application/json"},
            timeout=20,
        )
        data = resp.json()
        status = data.get("status", 0)
        text = data.get("text") or data.get("html") or ""
        # Support both proxy formats: {ok, status, text} and {status, html, text}
        if status == 200 and text:
            return json.loads(text)
    except Exception as e:
        logger.debug("[FNS] proxy fallback failed: %s", e)
    return None


def _get(url: str, params: dict) -> dict:
    """HTTP GET with rate limiting, retry on 403, and Russian proxy fallback.

    Global lock ensures minimum interval between API calls across all threads.
    Retries up to 3 times on 403 (rate limit exceeded) with exponential backoff.
    If direct access fails — falls back to Russian proxy (Yandex VPS).
    """
    global _fns_last_call
    params["key"] = _fns_key()
    qs = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items())
    full_url = f"{url}?{qs}"

    for attempt in range(_FNS_MAX_RETRIES):
        # Rate limit: wait for minimum interval between calls
        with _fns_lock:
            now = time.monotonic()
            wait = _FNS_MIN_INTERVAL - (now - _fns_last_call)
            if wait > 0:
                time.sleep(wait)
            _fns_last_call = time.monotonic()

        req = urllib.request.Request(full_url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            # Rate limit — retry with backoff
            if e.code == 403 and attempt < _FNS_MAX_RETRIES - 1:
                backoff = (attempt + 1) * 5  # 5s, 10s
                logger.warning(
                    "FNS API rate limit (403), attempt %d/%d, waiting %ds: %s",
                    attempt + 1, _FNS_MAX_RETRIES, backoff, error_body[:100],
                )
                time.sleep(backoff)
                continue
            # Last attempt failed — try via Russian proxy
            if attempt == _FNS_MAX_RETRIES - 1:
                logger.info("[FNS] Direct access failed, trying Russian proxy...")
                proxy_result = _get_via_proxy(full_url)
                if proxy_result is not None:
                    logger.info("[FNS] OK via proxy for %s", url)
                    return proxy_result
            raise RuntimeError(f"FNS API error {e.code}: {error_body[:300]}")
        except Exception as e:
            # Network error — try proxy on last attempt
            if attempt == _FNS_MAX_RETRIES - 1:
                logger.info("[FNS] Network error, trying Russian proxy: %s", e)
                proxy_result = _get_via_proxy(full_url)
                if proxy_result is not None:
                    logger.info("[FNS] OK via proxy for %s", url)
                    return proxy_result
            if attempt < _FNS_MAX_RETRIES - 1:
                time.sleep((attempt + 1) * 3)
                continue
            raise

    raise RuntimeError("FNS API: все попытки исчерпаны")


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
