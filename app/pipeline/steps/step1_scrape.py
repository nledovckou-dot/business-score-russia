"""Step 1: Scrape website and extract structured data.

Uses multi-level cascade: requests -> Playwright -> minimal -> web search -> domain fallback.
Never fails completely — always returns at least domain-based data for downstream steps.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from app.pipeline.scraper import scrape_website
from app.pipeline.web_search import _search_duckduckgo

logger = logging.getLogger(__name__)


def _domain_to_name(domain: str) -> str:
    """Extract a readable company name guess from domain.

    Examples: dodopizza.ru → Dodopizza, wildberries.ru → Wildberries
    """
    # Remove www. and TLD
    name = domain.replace("www.", "").split(".")[0]
    # Capitalize and add spaces before capital letters in camelCase
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    return name.capitalize() if name else domain


def _web_search_fallback(url: str) -> dict:
    """Build minimal scraped data from web search results.

    Used when direct scraping completely fails (geo-block, Cloudflare, etc.).
    """
    parsed = urlparse(url if url.startswith("http") else f"https://{url}")
    domain = parsed.netloc or url

    result = {
        "url": url if url.startswith("http") else f"https://{url}",
        "domain": domain,
        "title": "",
        "description": "",
        "headings": [],
        "text": "",
        "contacts": {},
        "social_links": [],
        "pages_text": {},
        "scrape_method": "web_search",
        "scrape_warnings": [
            "Прямой скрапинг не удался (geo-block/Cloudflare). "
            "Данные собраны через поисковую выдачу."
        ],
    }

    queries = [
        f"site:{domain}",
        domain,
        f"{_domain_to_name(domain)} компания ИНН",
    ]

    for query in queries:
        try:
            search_results = _search_duckduckgo(query)
            if not search_results:
                continue

            snippets = []
            for r in search_results[:10]:
                title = r.get("title", "")
                snippet = r.get("snippet", "")
                if not result["title"] and title:
                    result["title"] = title
                if snippet:
                    snippets.append(snippet)

            if snippets:
                if not result["description"]:
                    result["description"] = snippets[0]
                result["text"] = "\n".join(snippets)
                break  # Got useful data

        except Exception as e:
            logger.warning("[step1] Web search '%s' failed: %s", query, e)

    logger.info(
        "[step1] Web search fallback for %s: title=%r, text_len=%d",
        url, result.get("title", "")[:60], len(result.get("text", "")),
    )
    return result


def run(url: str) -> dict:
    """Scrape the URL and return structured content.

    Returns dict with: url, domain, title, description, headings, text,
    contacts, social_links, pages_text, scrape_method, scrape_warnings.

    Multi-level cascade (never raises — always returns something):
      1. requests (fast, 15s)
      2. Playwright headless (30s, JS rendering)
      3. Minimal fallback (title + meta description)
      4. Web search fallback (DuckDuckGo snippets)
      5. Domain-only fallback (just the domain name)
    """
    try:
        scraped = scrape_website(url)
    except Exception as e:
        logger.warning("[step1] scrape_website raised: %s. Using fallback.", e)
        scraped = {"error": str(e), "text": "", "title": "", "description": "",
                   "headings": [], "contacts": {}, "social_links": [], "pages_text": {},
                   "url": url, "domain": urlparse(url).netloc or url,
                   "scrape_method": "error", "scrape_warnings": [str(e)]}
    scrape_error = scraped.get("error")

    method = scraped.get("scrape_method", "requests")
    text_len = len(scraped.get("text", ""))
    title = scraped.get("title", "")
    description = scraped.get("description", "")
    has_content = text_len >= 50 or title or description

    # If scraping returned an error OR zero useful content, try web search
    if scrape_error or not has_content:
        logger.info(
            "[step1] Direct scraping insufficient for %s (error=%s, text=%d, title=%r). "
            "Trying web search fallback.",
            url, scrape_error, text_len, title[:40] if title else "",
        )
        ws_result = _web_search_fallback(url)
        ws_text_len = len(ws_result.get("text", ""))
        ws_title = ws_result.get("title", "")

        if ws_text_len > text_len or (ws_title and not title):
            ws_result["scrape_warnings"] = (
                scraped.get("scrape_warnings", []) + ws_result.get("scrape_warnings", [])
            )
            scraped = ws_result
            method = "web_search"
            text_len = ws_text_len
            title = ws_title
            description = ws_result.get("description", "")
            has_content = text_len >= 50 or title or description

    # If STILL no content — create domain-only fallback
    if not has_content:
        parsed = urlparse(url if url.startswith("http") else f"https://{url}")
        domain = parsed.netloc or url
        domain_name = _domain_to_name(domain)
        logger.warning(
            "[step1] All methods failed for %s. Using domain-only fallback.", url,
        )
        scraped = {
            "url": url if url.startswith("http") else f"https://{url}",
            "domain": domain,
            "title": domain_name,
            "description": f"Компания {domain_name} (сайт {domain})",
            "headings": [],
            "text": "",
            "contacts": {},
            "social_links": [],
            "pages_text": {},
            "scrape_method": "domain_only",
            "scrape_warnings": (
                scraped.get("scrape_warnings", []) + [
                    f"Все методы скрапинга не смогли получить данные с {domain}. "
                    f"Используется минимальный профиль на основе домена. "
                    f"Анализ будет ограничен."
                ]
            ),
        }
    elif text_len < 50:
        logger.warning(
            "[step1] Limited data for %s: text %d chars (method=%s) "
            "but title/description available.",
            url, text_len, method,
        )
        scraped.setdefault("scrape_warnings", []).append(
            f"Ограниченные данные: текст {text_len} символов (метод: {method}). "
            f"Анализ может быть ограничен."
        )

    return scraped
