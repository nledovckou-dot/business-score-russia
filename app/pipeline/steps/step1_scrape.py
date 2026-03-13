"""Step 1: Scrape website and extract structured data.

Uses three-level cascade: requests -> Playwright -> minimal fallback.
If all fail, uses web search to build a minimal profile.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from app.pipeline.scraper import scrape_website
from app.pipeline.web_search import _search_duckduckgo

logger = logging.getLogger(__name__)


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

    try:
        search_results = _search_duckduckgo(f"site:{domain}")
        if not search_results:
            search_results = _search_duckduckgo(domain)

        snippets = []
        for r in search_results[:10]:
            title = r.get("title", "")
            snippet = r.get("snippet", "")
            if not result["title"] and title and domain.replace("www.", "") in r.get("url", ""):
                result["title"] = title
            if snippet:
                snippets.append(snippet)

        if snippets:
            result["description"] = snippets[0]
            result["text"] = "\n".join(snippets)

        logger.info(
            "[step1] Web search fallback for %s: title=%r, text_len=%d",
            url, result["title"][:60], len(result["text"]),
        )
    except Exception as e:
        logger.warning("[step1] Web search fallback failed for %s: %s", url, e)

    return result


def run(url: str) -> dict:
    """Scrape the URL and return structured content.

    Returns dict with: url, domain, title, description, headings, text,
    contacts, social_links, pages_text, scrape_method, scrape_warnings.

    Four-level cascade:
      1-3. Direct scraping (requests -> Playwright -> minimal)
      4.   Web search fallback (DuckDuckGo snippets)

    Raises RuntimeError only if ALL methods fail completely.
    """
    scraped = scrape_website(url)
    scrape_error = scraped.get("error")

    method = scraped.get("scrape_method", "requests")
    text_len = len(scraped.get("text", ""))
    title = scraped.get("title", "")
    description = scraped.get("description", "")
    has_content = text_len >= 50 or title or description

    # If scraping returned an error OR zero useful content, try web search
    if scrape_error or (not has_content and method != "minimal"):
        logger.info(
            "[step1] Direct scraping insufficient for %s (error=%s, text=%d, title=%r). "
            "Trying web search fallback.",
            url, scrape_error, text_len, title[:40],
        )
        ws_result = _web_search_fallback(url)
        ws_text_len = len(ws_result.get("text", ""))
        ws_title = ws_result.get("title", "")

        if ws_text_len > text_len or (ws_title and not title):
            # Web search gave more data — use it, merge warnings
            ws_result["scrape_warnings"] = (
                scraped.get("scrape_warnings", []) + ws_result.get("scrape_warnings", [])
            )
            scraped = ws_result
            method = "web_search"
            text_len = ws_text_len
            title = ws_title
            description = ws_result.get("description", "")
            has_content = text_len >= 50 or title or description

    # Final gate
    if scraped.get("error") and not has_content:
        raise RuntimeError(f"Не удалось загрузить сайт: {scraped['error']}")

    if method == "minimal":
        if not title and not description:
            raise RuntimeError(
                "Сайт недоступен: не удалось извлечь даже title и description. "
                "Проверьте URL."
            )
        logger.warning(
            "[step1] Minimal scrape for %s: only title+description available "
            "(text_len=%d). Downstream analysis may be limited.",
            url, text_len,
        )
        return scraped

    if text_len < 50:
        if title or description:
            logger.warning(
                "[step1] Limited data for %s: text %d chars (method=%s) "
                "but title/description available. Allowing through.",
                url, text_len, method,
            )
            scraped["scrape_warnings"] = scraped.get("scrape_warnings", [])
            scraped["scrape_warnings"].append(
                f"Ограниченные данные: текст {text_len} символов (метод: {method}). "
                f"Анализ может быть ограничен."
            )
        else:
            raise RuntimeError(
                "Сайт вернул слишком мало текста "
                f"({text_len} символов, метод: {method}). Проверьте URL."
            )

    return scraped
