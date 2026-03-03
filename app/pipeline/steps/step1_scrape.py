"""Step 1: Scrape website and extract structured data.

Uses three-level cascade: requests -> Scrapling -> minimal fallback.
"""

from __future__ import annotations

import logging

from app.pipeline.scraper import scrape_website

logger = logging.getLogger(__name__)


def run(url: str) -> dict:
    """Scrape the URL and return structured content.

    Returns dict with: url, domain, title, description, headings, text,
    contacts, social_links, pages_text, scrape_method, scrape_warnings.

    Raises RuntimeError if scraping failed completely (all 3 methods).
    For "minimal" method, allows short text but logs a warning.
    """
    scraped = scrape_website(url)

    if scraped.get("error"):
        raise RuntimeError(f"Не удалось загрузить сайт: {scraped['error']}")

    method = scraped.get("scrape_method", "requests")
    text_len = len(scraped.get("text", ""))

    # Minimal fallback: title + meta description only -- allow through with warning
    if method == "minimal":
        if not scraped.get("title") and not scraped.get("description"):
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

    # Full scrape: require meaningful text
    if text_len < 50:
        raise RuntimeError(
            "Сайт вернул слишком мало текста "
            f"({text_len} символов, метод: {method}). Проверьте URL."
        )

    return scraped
