"""Step 1: Scrape website and extract structured data."""

from __future__ import annotations

from app.pipeline.scraper import scrape_website


def run(url: str) -> dict:
    """Scrape the URL and return structured content.

    Returns dict with: url, domain, title, description, headings, text,
    contacts, social_links, pages_text, scrape_method, scrape_warnings.
    """
    scraped = scrape_website(url)

    if scraped.get("error"):
        raise RuntimeError(f"Не удалось загрузить сайт: {scraped['error']}")

    if len(scraped.get("text", "")) < 50:
        raise RuntimeError("Сайт вернул слишком мало текста. Проверьте URL.")

    return scraped
