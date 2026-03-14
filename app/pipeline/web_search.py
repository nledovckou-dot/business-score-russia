"""Web search verification: check if a competitor company actually exists.

Primary: DuckDuckGo HTML search (free, no API key required).
Fallback: Gemini with Google Search grounding (when DDG is blocked/timeout).
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.request
import urllib.error
from typing import Optional
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_SEARCH_TIMEOUT = 10  # seconds per request
_DELAY_BETWEEN_REQUESTS = 2.5  # seconds between searches to avoid rate limiting

# Track DDG failures to skip it early when it's consistently blocked
# Start at threshold: DDG is known blocked on VPS, skip direct attempts entirely.
# If DDG recovers, a successful proxy-free response will reset this to 0.
_DDG_FAILURE_THRESHOLD = 3
_ddg_consecutive_failures = _DDG_FAILURE_THRESHOLD

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


def _search_via_proxy(query: str) -> list[dict]:
    """Search Yandex via Russian proxy (Yandex Cloud VPS).

    Yandex search is guaranteed accessible from Yandex Cloud.
    Returns list of {title, url, display_url, snippet}.
    """
    proxy_url = "http://158.160.158.164:8888/scrape"
    yandex_url = f"https://yandex.ru/search/?text={quote_plus(query)}&lr=213"

    try:
        resp = requests.post(
            proxy_url,
            json={"url": yandex_url},
            headers={
                "X-Proxy-Token": "bsr-proxy-2026",
                "Content-Type": "application/json",
            },
            timeout=20,
        )
        data = resp.json()
        html = data.get("html") or data.get("text") or ""
        if not html or data.get("status", 0) != 200:
            logger.warning(
                "Proxy Yandex: status=%s, html_len=%d",
                data.get("status"), len(html),
            )
            return []

        soup = BeautifulSoup(html, "lxml")
        results = []

        # Yandex search result selectors (multiple formats)
        for selector in [
            "li.serp-item",
            "div.organic",
            "div[data-fast-name='organic']",
        ]:
            items = soup.select(selector)
            if items:
                for item in items[:15]:
                    # Title + URL
                    link = item.select_one("a[href]")
                    if not link:
                        continue
                    href = link.get("href", "")
                    title = link.get_text(strip=True)

                    # Skip Yandex internal links
                    if not href or "yandex.ru" in href and "/search" in href:
                        continue

                    # Extract real URL from Yandex redirect
                    if "/search/redirect?" in href or "clck.yandex.ru" in href:
                        url_match = re.search(r"url=([^&]+)", href)
                        if url_match:
                            from urllib.parse import unquote
                            href = unquote(url_match.group(1))

                    # Snippet
                    snippet_el = item.select_one(
                        "div.text-container, span.OrganicTextContentSpan, "
                        "div.organic__content-wrapper, div.TextContainer"
                    )
                    snippet = snippet_el.get_text(strip=True)[:300] if snippet_el else ""

                    if href.startswith("http"):
                        results.append({
                            "title": title[:200],
                            "url": href,
                            "display_url": href,
                            "snippet": snippet,
                        })

                if results:
                    break

        # Fallback: extract any external links from the page
        if not results:
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                if href.startswith("http") and "yandex" not in href:
                    title = a_tag.get_text(strip=True)
                    if title and len(title) > 5:
                        results.append({
                            "title": title[:200],
                            "url": href,
                            "display_url": href,
                            "snippet": "",
                        })
                        if len(results) >= 10:
                            break

        if results:
            logger.info("Proxy Yandex: %d results for '%s'", len(results), query[:80])
        else:
            logger.warning("Proxy Yandex: 0 results for '%s' (html_len=%d)", query[:80], len(html))
        return results

    except Exception as e:
        logger.warning("Proxy Yandex search failed for '%s': %s", query[:80], str(e)[:200])
        return []


def _search_via_gemini(query: str) -> list[dict]:
    """Search via Gemini with Google Search grounding.

    Returns list of {title, url, display_url, snippet} — same format as DDG.
    Uses Gemini 2.5 Flash with google_search tool for grounding.
    Prompt forces the model to actually search rather than answer from memory.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("GEMINI_API_KEY not set, cannot use Gemini search fallback")
        return []

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/"
        f"models/gemini-2.5-flash:generateContent?key={api_key}"
    )

    # Prompt designed to force actual web search — ask for current/specific data
    payload = {
        "contents": [{"parts": [{"text": (
            f"Выполни поиск в Google по запросу: {query}\n\n"
            "Перечисли топ-5 результатов поиска в формате:\n"
            "1. [Заголовок страницы] — URL — краткое описание\n"
            "2. ...\n\n"
            "Обязательно укажи реальные URL сайтов из результатов поиска."
        )}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": 1024,
        },
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode("utf-8"))

            candidate = body.get("candidates", [{}])[0]
            grounding = candidate.get("groundingMetadata", {})
            chunks = grounding.get("groundingChunks", [])
            supports = grounding.get("groundingSupports", [])

            results = []

            # Extract URLs and titles from grounding chunks
            for chunk in chunks:
                web = chunk.get("web", {})
                uri = web.get("uri", "")
                title = web.get("title", "")
                if uri:
                    results.append({
                        "title": title,
                        "url": uri,
                        "display_url": uri,
                        "snippet": "",
                    })

            # Enrich with snippets from grounding supports
            for support in supports:
                segment_text = support.get("segment", {}).get("text", "")
                chunk_indices = support.get("groundingChunkIndices", [])
                for idx in chunk_indices:
                    if idx < len(results) and segment_text:
                        if not results[idx]["snippet"]:
                            results[idx]["snippet"] = segment_text[:300]
                        else:
                            results[idx]["snippet"] += " " + segment_text[:200]

            # If no grounding chunks, parse URLs from the text response
            if not results:
                text = candidate.get("content", {}).get("parts", [{}])[0].get("text", "")
                if text:
                    # Try to extract URLs from text
                    url_matches = re.findall(
                        r'https?://[^\s\)\]\"\'<>,]+', text
                    )
                    if url_matches:
                        for found_url in url_matches[:10]:
                            # Clean trailing punctuation
                            found_url = found_url.rstrip(".")
                            results.append({
                                "title": "",
                                "url": found_url,
                                "display_url": found_url,
                                "snippet": "",
                            })
                    # Also return text as a snippet even without URLs
                    if not results:
                        results.append({
                            "title": query,
                            "url": "",
                            "display_url": "",
                            "snippet": text[:500],
                        })

            if results:
                logger.info(
                    "Gemini search: %d results (%d with URLs) for '%s'",
                    len(results),
                    sum(1 for r in results if r["url"]),
                    query[:80],
                )
            return results

        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < 1:
                time.sleep(3)
                continue
            logger.warning("Gemini search HTTP %d for '%s'", e.code, query[:80])
            return []
        except Exception as e:
            if attempt < 1:
                time.sleep(2)
                continue
            logger.warning("Gemini search failed for '%s': %s", query[:80], str(e)[:200])
            return []

    return []


def _raw_search_duckduckgo(query: str, max_retries: int = 1) -> list[dict]:
    """Search DuckDuckGo HTML version. Returns list of {title, url, snippet}.

    DuckDuckGo HTML version doesn't require API keys.
    Reduced retries since we have Gemini fallback.
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

            if resp.status_code in (429, 202) and attempt < max_retries:
                time.sleep((attempt + 1) * 3)
                continue

            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warning("DuckDuckGo search failed for '%s': %s", query[:80], e)
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

        if results:
            return results

        if attempt < max_retries:
            time.sleep((attempt + 1) * 3)

    return results


def _search_duckduckgo(query: str, max_retries: int = 2) -> list[dict]:
    """Search the web with automatic fallback chain.

    1. DuckDuckGo direct (free, fast — but blocked on VPS)
    2. DuckDuckGo via Russian proxy (bypasses VPS network blocks)
    3. Gemini with Google Search grounding (API-based, always works)

    Circuit breaker: after 3 consecutive DDG failures, skip DDG direct
    and go straight to proxy/Gemini.
    """
    global _ddg_consecutive_failures

    # 1. Try DDG direct (skip if circuit breaker tripped)
    if _ddg_consecutive_failures < _DDG_FAILURE_THRESHOLD:
        results = _raw_search_duckduckgo(query, max_retries=min(max_retries, 1))
        if results:
            _ddg_consecutive_failures = 0
            return results
        _ddg_consecutive_failures += 1
        if _ddg_consecutive_failures == _DDG_FAILURE_THRESHOLD:
            logger.warning(
                "DuckDuckGo direct failed %d times — switching to proxy/Gemini",
                _DDG_FAILURE_THRESHOLD,
            )

    # 2. Try DDG via Russian proxy
    results = _search_via_proxy(query)
    if results:
        return results

    # 3. Fallback to Gemini with Google Search grounding
    results = _search_via_gemini(query)
    if results:
        return results

    logger.warning("All search providers failed for '%s'", query[:80])
    return []


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
