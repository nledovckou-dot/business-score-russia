"""Keys.so API client: SEO & ad analytics for domains.

Provides real traffic metrics, visibility, DR, ad budgets, SEO competitors,
and keyword data. One dashboard call per domain = all key metrics.

Auth: X-Keyso-TOKEN header.
Rate limit: 10 requests per 10 seconds (enforced via threading lock).
Docs: https://apidoc.keys.so
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

_API_BASE = "https://api.keys.so"
_lock = threading.Lock()
_last_call: float = 0.0
_MIN_INTERVAL = 1.1  # >1 sec between calls to stay well within 10/10s limit
_MAX_RETRIES = 2


def _api_token() -> str:
    return os.environ.get(
        "KEYSO_API_TOKEN",
        "69b55d3c5ad036.282600972fbfa13f85beaece8e7d215ad21351f1",
    )


def _get(endpoint: str, params: dict, timeout: int = 20) -> dict:
    """HTTP GET with rate limiting and retry."""
    global _last_call

    qs = urllib.parse.urlencode(params, encoding="utf-8")
    url = f"{_API_BASE}{endpoint}?{qs}"

    for attempt in range(_MAX_RETRIES):
        with _lock:
            now = time.monotonic()
            wait = _MIN_INTERVAL - (now - _last_call)
            if wait > 0:
                time.sleep(wait)
            _last_call = time.monotonic()

        req = urllib.request.Request(url, headers={
            "X-Keyso-TOKEN": _api_token(),
            "Accept": "application/json",
            "User-Agent": "BSR-Pipeline/1.0",
        })

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", "5"))
                logger.warning("[keyso] Rate limited, waiting %ds", retry_after)
                time.sleep(retry_after)
                continue
            if e.code in (500, 502, 503) and attempt < _MAX_RETRIES - 1:
                time.sleep(2)
                continue
            body = ""
            try:
                body = e.read().decode("utf-8")[:300] if e.fp else ""
            except Exception:
                pass
            logger.error("[keyso] HTTP %d on %s: %s", e.code, endpoint, body)
            raise RuntimeError(f"Keys.so API error {e.code}: {body}") from e
        except Exception as e:
            if attempt < _MAX_RETRIES - 1:
                time.sleep(1)
                continue
            raise RuntimeError(f"Keys.so request failed: {e}") from e

    raise RuntimeError("Keys.so: all retries exhausted")


def get_domain_dashboard(domain: str, base: str = "msk") -> dict | None:
    """Get comprehensive SEO/ad dashboard for a domain.

    This is the most cost-effective call: one request = all key metrics.

    Args:
        domain: Domain name (e.g., "amocrm.ru")
        base: Region code (msk, spb, nsk, ekb, etc.)

    Returns:
        Dict with keys:
        - seo_metrics: {it1, it3, it5, it10, it50, vis, dr, pages_in_index, total_keywords}
        - ad_metrics: {ads_count, ad_keywords, ad_budget_min, ad_budget_avg, ad_budget_max}
        - seo_competitors: [{name, keyword_overlap, visibility}]
        - ad_competitors: [{name, keyword_overlap}]
        - top_keywords: [{word, search_volume, position}]
        - top_pages: [{url, keywords_count}]
        - history: [{month, visibility, keywords_top10, ads_count}]
        - domain, base
    """
    if not domain:
        return None

    # Clean domain
    domain = domain.strip().lower()
    domain = domain.replace("https://", "").replace("http://", "").rstrip("/")
    if "/" in domain:
        domain = domain.split("/")[0]

    try:
        raw = _get("/report/simple/domain_dashboard", {"base": base, "domain": domain})
    except Exception as e:
        logger.warning("[keyso] Dashboard failed for '%s': %s", domain, str(e)[:200])
        return None

    if not raw or not raw.get("name"):
        logger.info("[keyso] No data for domain '%s'", domain)
        return None

    # Parse SEO metrics
    seo_metrics = {
        "it1": raw.get("it1", 0),
        "it3": raw.get("it3", 0),
        "it5": raw.get("it5", 0),
        "it10": raw.get("it10", 0),
        "it50": raw.get("it50", 0),
        "visibility": raw.get("vis", 0),
        "dr": raw.get("dr", 0),
        "pages_in_index": raw.get("pagesinindex", 0),
        "total_keywords": raw.get("topkeys", 0),
        "ai_answers": raw.get("aiAnswersCnt", 0),
    }

    # Parse ad metrics
    adcost = raw.get("adcost", {})
    ad_metrics = {
        "ads_count": raw.get("adscnt", 0),
        "ad_keywords": raw.get("adkeyscnt", 0),
        "ad_budget_min": adcost.get("bdg5", 0),
        "ad_budget_avg": adcost.get("average", 0),
        "ad_budget_max": adcost.get("bdg100", 0),
    }

    # Parse SEO competitors
    seo_competitors = []
    for c in (raw.get("concs") or [])[:10]:
        seo_competitors.append({
            "domain": c.get("name", ""),
            "keyword_overlap": c.get("cnt", 0),
            "visibility": c.get("vis", 0),
            "keywords_total": c.get("it50", 0),
        })

    # Parse ad competitors
    ad_competitors = []
    for c in (raw.get("adconcs") or [])[:10]:
        ad_competitors.append({
            "domain": c.get("name", ""),
            "keyword_overlap": c.get("cnt", 0),
            "ads_count": c.get("adscnt", 0),
        })

    # Parse top keywords
    top_keywords = []
    for k in (raw.get("keys") or [])[:10]:
        top_keywords.append({
            "word": k.get("word", ""),
            "search_volume": k.get("ws", 0),
            "position": k.get("pos", 0),
        })

    # Parse top pages
    top_pages = []
    for p in (raw.get("pages") or [])[:10]:
        top_pages.append({
            "url": p.get("url", ""),
            "keywords_count": p.get("it50", 0),
        })

    # Parse history (last 12 months)
    history = []
    for month, data in sorted((raw.get("history") or {}).items())[-12:]:
        history.append({
            "month": month,
            "visibility": data.get("visAvg", 0),
            "keywords_top10": data.get("it10", 0),
            "keywords_top50": data.get("it50", 0),
            "ads_count": data.get("adsCount", 0),
            "ad_keywords": data.get("adKeysCount", 0),
            "pages_in_index": data.get("pagesInIndex", 0),
        })

    result = {
        "domain": domain,
        "base": base,
        "seo_metrics": seo_metrics,
        "ad_metrics": ad_metrics,
        "seo_competitors": seo_competitors,
        "ad_competitors": ad_competitors,
        "top_keywords": top_keywords,
        "top_pages": top_pages,
        "history": history,
        "icon_url": raw.get("iconUrl", ""),
    }

    logger.info(
        "[keyso] Dashboard '%s': vis=%d, DR=%d, top10=%d, ads=%d, budget=%d",
        domain, seo_metrics["visibility"], seo_metrics["dr"],
        seo_metrics["it10"], ad_metrics["ads_count"], ad_metrics["ad_budget_avg"],
    )

    return result


def get_seo_comparison(domains: list[str], base: str = "msk") -> list[dict]:
    """Get SEO dashboards for multiple domains (for competitive comparison).

    Makes one API call per domain. Rate-limited to stay within Keys.so limits.

    Args:
        domains: List of domain names
        base: Region code

    Returns:
        List of dashboard results (same format as get_domain_dashboard)
    """
    results = []
    for domain in domains:
        dashboard = get_domain_dashboard(domain, base=base)
        if dashboard:
            results.append(dashboard)
        else:
            results.append({
                "domain": domain,
                "seo_metrics": {},
                "ad_metrics": {},
                "seo_competitors": [],
                "top_keywords": [],
                "history": [],
            })
    return results
