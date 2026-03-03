"""Web search verification: check if a competitor company actually exists.

Uses DuckDuckGo HTML search (free, no API key required).
Fallback: graceful degradation — mark as unverified if search fails.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_SEARCH_TIMEOUT = 10  # seconds per request
_DELAY_BETWEEN_REQUESTS = 2.5  # seconds between searches to avoid rate limiting

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Domains that indicate a real business presence
_AUTHORITY_DOMAINS = (
    "2gis.ru",
    "yandex.ru/maps",
    "maps.yandex.ru",
    "google.com/maps",
    "rusprofile.ru",
    "list-org.com",
    "checko.ru",
    "sbis.ru",
    "zoon.ru",
    "flamp.ru",
    "yell.ru",
    "cataloxy.ru",
    "hh.ru",
)


def _search_duckduckgo(query: str, max_retries: int = 2) -> list[dict]:
    """Search DuckDuckGo HTML version. Returns list of {title, url, snippet}.

    DuckDuckGo HTML version doesn't require API keys.
    Retries with exponential backoff if rate-limited (empty results or 429/202).
    """
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"

    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(
                url,
                headers=_HEADERS,
                timeout=_SEARCH_TIMEOUT,
                allow_redirects=True,
            )

            # Rate limited — retry
            if resp.status_code in (429, 202) and attempt < max_retries:
                wait = (attempt + 1) * 3
                logger.info(
                    "DuckDuckGo rate limited (HTTP %d), retrying in %ds...",
                    resp.status_code, wait,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warning("DuckDuckGo search failed for '%s': %s", query, e)
            if attempt < max_retries:
                time.sleep((attempt + 1) * 2)
                continue
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        results = []

        for result_div in soup.select(".result__body"):
            title_el = result_div.select_one(".result__a")
            snippet_el = result_div.select_one(".result__snippet")
            url_el = result_div.select_one(".result__url")

            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            href = title_el.get("href", "")
            snippet = snippet_el.get_text(strip=True) if snippet_el else ""
            display_url = url_el.get_text(strip=True) if url_el else ""

            # DuckDuckGo wraps URLs in a redirect — extract the actual URL
            actual_url = href
            if "uddg=" in href:
                m = re.search(r"uddg=([^&]+)", href)
                if m:
                    from urllib.parse import unquote
                    actual_url = unquote(m.group(1))

            results.append({
                "title": title,
                "url": actual_url,
                "display_url": display_url,
                "snippet": snippet,
            })

        # If got results — return immediately
        if results:
            return results

        # Empty results might be rate limiting — retry
        if attempt < max_retries:
            wait = (attempt + 1) * 3
            logger.info(
                "DuckDuckGo returned 0 results for '%s', retrying in %ds...",
                query, wait,
            )
            time.sleep(wait)

    return results


def _check_website_exists(url: str) -> bool:
    """Quick HEAD/GET check if a website responds (status 200-399)."""
    if not url or not url.startswith("http"):
        return False

    try:
        resp = requests.head(
            url,
            headers=_HEADERS,
            timeout=_SEARCH_TIMEOUT,
            allow_redirects=True,
        )
        return resp.status_code < 400
    except requests.exceptions.RequestException:
        # HEAD may be blocked — try GET with stream
        try:
            resp = requests.get(
                url,
                headers=_HEADERS,
                timeout=_SEARCH_TIMEOUT,
                allow_redirects=True,
                stream=True,
            )
            resp.close()
            return resp.status_code < 400
        except requests.exceptions.RequestException:
            return False


def verify_company_exists(
    name: str,
    city: Optional[str] = None,
    website: Optional[str] = None,
) -> dict:
    """Verify that a company exists using web search + website check.

    Args:
        name: Company name (e.g. "Додо Пицца")
        city: Optional city for more precise search
        website: Optional website URL from GPT to verify

    Returns:
        {
            "verified": True/False,
            "confidence": "high"/"medium"/"low"/"unverified",
            "url": "https://...",  # found website or verified GPT website
            "sources": ["2gis.ru", "rusprofile.ru", ...],
            "notes": "Найден в 2ГИС и Яндекс.Картах"
        }
    """
    result = {
        "verified": False,
        "confidence": "unverified",
        "url": None,
        "sources": [],
        "notes": "",
    }

    # ── Step 1: Search DuckDuckGo ──
    query = f"{name} компания"
    if city:
        query += f" {city}"

    search_results = _search_duckduckgo(query)

    if not search_results:
        # Fallback: if web search is empty but GPT provided a website, verify it
        if website:
            website_ok = _check_website_exists(website)
            if website_ok:
                result["verified"] = True
                result["confidence"] = "low"
                result["url"] = website
                result["sources"] = ["сайт компании"]
                result["notes"] = "Веб-поиск недоступен, но сайт компании отвечает"
                logger.info(
                    "Конкурент '%s': поиск пуст, но сайт %s отвечает", name, website
                )
                return result

        result["notes"] = "Веб-поиск не вернул результатов"
        logger.info("Конкурент '%s': веб-поиск не вернул результатов", name)
        return result

    # ── Step 2: Analyze search results ──
    found_authority = []  # authority domain matches
    found_relevant = []   # results mentioning company name
    found_website = None  # company's own website

    name_lower = name.lower()
    # Create name tokens for fuzzy matching (words 3+ chars)
    name_tokens = [
        w for w in re.split(r"[\s\-—/\\«»\"\']+", name_lower)
        if len(w) >= 3
    ]

    for sr in search_results[:15]:  # analyze top 15 results
        sr_title_lower = sr["title"].lower()
        sr_snippet_lower = sr["snippet"].lower()
        sr_url_lower = sr["url"].lower()
        combined_text = f"{sr_title_lower} {sr_snippet_lower}"

        # Check if result mentions the company name
        # Require at least half of name tokens to match
        if name_tokens:
            matched_tokens = sum(
                1 for t in name_tokens
                if t in combined_text
            )
            relevance = matched_tokens / len(name_tokens)
        else:
            relevance = 1.0 if name_lower in combined_text else 0.0

        if relevance < 0.5:
            continue

        found_relevant.append(sr)

        # Check for authority domains
        for domain in _AUTHORITY_DOMAINS:
            if domain in sr_url_lower:
                found_authority.append(domain.split("/")[0])
                break

        # Try to find the company's own website
        if not found_website:
            # Skip aggregator domains — we want the company's own site
            is_aggregator = any(
                d in sr_url_lower for d in (
                    "2gis.", "yandex.", "google.", "rusprofile.",
                    "list-org.", "checko.", "zoon.", "flamp.",
                    "hh.ru", "yell.", "cataloxy.", "sbis.",
                    "duckduckgo.", "wikipedia.",
                )
            )
            if not is_aggregator and sr["url"].startswith("http"):
                found_website = sr["url"]

    # ── Step 3: Check GPT-provided website ──
    website_ok = False
    if website:
        website_ok = _check_website_exists(website)
        if website_ok:
            found_website = website
            if "сайт" not in " ".join(result["sources"]):
                result["sources"].append("сайт компании")

    # ── Step 4: Determine confidence ──
    unique_authorities = list(set(found_authority))

    if len(unique_authorities) >= 2 or (len(unique_authorities) >= 1 and website_ok):
        result["verified"] = True
        result["confidence"] = "high"
        result["sources"] = unique_authorities
        if website_ok:
            result["sources"].append("сайт компании")
        result["notes"] = f"Подтверждён: найден в {', '.join(unique_authorities)}"

    elif len(unique_authorities) >= 1 or len(found_relevant) >= 3:
        result["verified"] = True
        result["confidence"] = "medium"
        result["sources"] = unique_authorities if unique_authorities else ["веб-поиск"]
        result["notes"] = (
            f"Найден в {', '.join(unique_authorities)}"
            if unique_authorities
            else f"Найдено {len(found_relevant)} релевантных результатов в поиске"
        )

    elif len(found_relevant) >= 1 or website_ok:
        result["verified"] = True
        result["confidence"] = "low"
        result["sources"] = ["веб-поиск"]
        if website_ok:
            result["sources"].append("сайт компании")
        result["notes"] = "Найден в поиске, но мало подтверждений"

    else:
        result["verified"] = False
        result["confidence"] = "unverified"
        result["notes"] = "Не найден в веб-поиске"

    result["url"] = found_website
    logger.info(
        "Конкурент '%s': %s (confidence=%s, sources=%s)",
        name,
        "проверен" if result["verified"] else "не найден",
        result["confidence"],
        result["sources"],
    )

    return result


def verify_competitors_batch(
    competitors: list[dict],
    delay: float = _DELAY_BETWEEN_REQUESTS,
) -> list[dict]:
    """Verify a batch of competitors. Adds verification fields to each.

    Modifies competitors in-place and returns them.
    Each competitor gets:
        - verified: bool
        - verification_confidence: "high"/"medium"/"low"/"unverified"
        - verification_url: str | None
        - verification_sources: list[str]
        - verification_notes: str
    """
    for i, comp in enumerate(competitors):
        name = comp.get("name", "")
        if not name:
            comp["verified"] = False
            comp["verification_confidence"] = "unverified"
            comp["verification_url"] = None
            comp["verification_sources"] = []
            comp["verification_notes"] = "Нет названия"
            continue

        city = comp.get("city")
        website = comp.get("website")

        try:
            vresult = verify_company_exists(name=name, city=city, website=website)
            comp["verified"] = vresult["verified"]
            comp["verification_confidence"] = vresult["confidence"]
            comp["verification_url"] = vresult["url"]
            comp["verification_sources"] = vresult["sources"]
            comp["verification_notes"] = vresult["notes"]
        except Exception as e:
            logger.error("Ошибка верификации конкурента '%s': %s", name, e)
            comp["verified"] = False
            comp["verification_confidence"] = "unverified"
            comp["verification_url"] = None
            comp["verification_sources"] = []
            comp["verification_notes"] = f"Ошибка проверки: {e}"

        # Delay between requests to avoid rate limiting
        if i < len(competitors) - 1:
            time.sleep(delay)

    return competitors
