"""Step 4.5: Enrich competitor data with real sources (T42, T45).

For each competitor from step4:
1. Scrape website → extract social links, text
2. 2GIS → rating, reviews count, working hours
3. Find INN via FNS search (→ Rusprofile fallback)
4. Get FNS financials (revenue, employees) by INN
5. Get EGRUL data (year_founded, founders) by INN
6. Social media → real followers/subscribers (VK API, TG, IG)

Runs in parallel via ThreadPoolExecutor (max_workers=4).
"""

from __future__ import annotations

import logging
import re
import time
import concurrent.futures
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _find_inn_rusprofile(name: str, city: str | None = None) -> str | None:
    """Fallback: scrape Rusprofile to find INN when FNS search fails."""
    import urllib.parse

    try:
        from app.pipeline.scraper import scrape_website
    except ImportError:
        return None

    query = urllib.parse.quote(name)
    url = f"https://www.rusprofile.ru/search?query={query}&type=ul"
    try:
        scraped = scrape_website(url, timeout=15)
        text = scraped.get("text", "")
        if not text:
            return None

        # Rusprofile shows INN in format "ИНН 1234567890" or "ИНН: 1234567890"
        inn_patterns = [
            re.compile(r"ИНН[:\s]+(\d{10,12})"),
            re.compile(r"inn[:\s]+(\d{10,12})", re.IGNORECASE),
        ]
        for pattern in inn_patterns:
            match = pattern.search(text)
            if match:
                inn = match.group(1)
                logger.info("[step4.5] Rusprofile fallback: found INN %s for '%s'", inn, name)
                return inn
    except Exception as e:
        logger.warning("[step4.5] Rusprofile fallback failed for '%s': %s", name, str(e)[:200])

    return None


def _find_inn(name: str, city: str | None = None) -> str | None:
    """Find company INN using FNS search + web search fallback."""
    from app.pipeline.fns import search_company

    # Clean name for search
    clean_name = name.strip()
    if not clean_name:
        return None

    try:
        candidates = search_company(clean_name, limit=5)
    except Exception as e:
        logger.warning("FNS search failed for '%s': %s", clean_name, str(e)[:200])
        candidates = []

    if not candidates:
        return None

    # If city is specified, prefer match by city
    if city:
        city_lower = city.lower()
        for c in candidates:
            addr = (c.get("address") or "").lower()
            if city_lower in addr:
                inn = c.get("inn", "")
                if inn:
                    logger.info("Found INN for '%s' (city match '%s'): %s", name, city, inn)
                    return inn

    # Return first result
    first_inn = candidates[0].get("inn", "")
    if first_inn:
        logger.info("Found INN for '%s' (first result): %s", name, first_inn)
        return first_inn

    return None


def _enrich_one(comp: dict, index: int) -> dict:
    """Enrich a single competitor with real data. Returns enriched dict."""
    name = comp.get("name", "")
    website = comp.get("website", "")
    city = comp.get("city", "")
    t0 = time.monotonic()

    logger.info("[step4.5] Enriching %d: %s", index, name)

    # 1. Scrape website
    scraped_data = {}
    if website:
        try:
            from app.pipeline.scraper import scrape_website
            scraped_data = scrape_website(website, timeout=12)
            comp["scraped_text"] = (scraped_data.get("text") or "")[:3000]
            comp["scraped_social"] = scraped_data.get("social_links", [])
            logger.info("[step4.5] %s: scraped OK (%s)", name, scraped_data.get("scrape_method", "?"))
        except Exception as e:
            logger.warning("[step4.5] %s: scrape failed: %s", name, str(e)[:200])

    # 2. 2GIS: rating, reviews, working hours
    if city or name:
        try:
            from app.pipeline.enrichment.twogis import search_organization
            twogis_results = search_organization(name, city=city or None, page_size=3)
            if twogis_results:
                best = twogis_results[0]
                comp["rating_2gis"] = best.get("rating")
                comp["reviews_count_2gis"] = best.get("reviews_count", 0)
                comp["working_hours"] = best.get("working_hours", {})
                comp["coordinates"] = best.get("coordinates", {})
                comp["address_2gis"] = best.get("address", "")
                comp["branch_id_2gis"] = best.get("branch_id", "")
                comp["metrics"] = comp.get("metrics", {})
                if best.get("rating"):
                    comp["metrics"]["Рейтинг 2ГИС"] = f"{best['rating']:.1f}"
                if best.get("reviews_count"):
                    comp["metrics"]["Отзывы 2ГИС"] = str(best["reviews_count"])
                logger.info("[step4.5] %s: 2GIS OK (rating=%.1f, reviews=%d)",
                            name, best.get("rating") or 0, best.get("reviews_count") or 0)
        except Exception as e:
            logger.warning("[step4.5] %s: 2GIS failed: %s", name, str(e)[:200])

    # 3. Find INN (FNS → Rusprofile fallback)
    inn = comp.get("inn") or _find_inn(name, city)
    if not inn:
        inn = _find_inn_rusprofile(name, city)
    if inn:
        comp["inn"] = inn

        # 3. Get FNS financials
        try:
            from app.pipeline.fns import get_financials
            financials = get_financials(inn)
            if financials:
                comp["fns_financials"] = financials
                latest = financials[-1] if financials else {}
                comp["metrics"] = comp.get("metrics", {})
                rev = latest.get("revenue")
                if rev is not None:
                    if rev >= 1_000_000:
                        comp["metrics"]["Выручка"] = f"{rev / 1_000_000:.1f} млрд ₽"
                    elif rev >= 1000:
                        comp["metrics"]["Выручка"] = f"{rev / 1000:.1f} млн ₽"
                    else:
                        comp["metrics"]["Выручка"] = f"{rev:,.0f} тыс. ₽"
                emp = latest.get("employees")
                if emp:
                    comp["metrics"]["Сотрудники"] = str(emp)
                logger.info("[step4.5] %s: FNS financials OK (%d years)", name, len(financials))
        except Exception as e:
            logger.warning("[step4.5] %s: FNS financials failed: %s", name, str(e)[:200])

        # 4. Get EGRUL data
        try:
            from app.pipeline.fns import get_egrul
            egrul = get_egrul(inn)
            if egrul:
                comp["egrul"] = egrul
                reg_date = egrul.get("reg_date", "")
                if reg_date:
                    year_match = re.search(r"(\d{4})", reg_date)
                    if year_match:
                        comp["metrics"] = comp.get("metrics", {})
                        comp["metrics"]["Год основания"] = year_match.group(1)
                        comp["year_founded"] = year_match.group(1)
                # Legal name
                full_name = egrul.get("full_name", "")
                if full_name:
                    comp["legal_name"] = full_name
                logger.info("[step4.5] %s: EGRUL OK", name)
        except Exception as e:
            logger.warning("[step4.5] %s: EGRUL failed: %s", name, str(e)[:200])
    else:
        logger.info("[step4.5] %s: INN not found, skipping FNS", name)

    # 5. Social links from scraped data + real follower counts
    if scraped_data.get("social_links"):
        social_dict = {}
        for sl in scraped_data["social_links"]:
            platform = sl.get("platform", "")
            url = sl.get("url", "")
            handle = sl.get("handle", "")
            if platform and (url or handle):
                social_dict[platform] = {"url": url, "handle": handle}
        if social_dict:
            comp["social_media"] = social_dict

    # 6. Enrich social links with real follower/subscriber counts
    social_links = scraped_data.get("social_links", [])
    if social_links:
        try:
            from app.pipeline.enrichment.social_media import enrich_social_links
            enriched_links = enrich_social_links(social_links)
            # Update social_media dict with real counts
            for sl in enriched_links:
                platform = sl.get("platform", "")
                if platform and platform in comp.get("social_media", {}):
                    if sl.get("members"):
                        comp["social_media"][platform]["members"] = sl["members"]
                        comp["metrics"] = comp.get("metrics", {})
                        comp["metrics"][f"VK подписчики"] = f"{sl['members']:,}".replace(",", " ")
                    if sl.get("subscribers"):
                        comp["social_media"][platform]["subscribers"] = sl["subscribers"]
                        comp["metrics"] = comp.get("metrics", {})
                        comp["metrics"][f"Telegram подписчики"] = f"{sl['subscribers']:,}".replace(",", " ")
                    if sl.get("followers"):
                        comp["social_media"][platform]["followers"] = sl["followers"]
                        comp["metrics"] = comp.get("metrics", {})
                        comp["metrics"][f"Instagram подписчики"] = f"{sl['followers']:,}".replace(",", " ")
            logger.info("[step4.5] %s: social media enriched", name)
        except Exception as e:
            logger.warning("[step4.5] %s: social enrichment failed: %s", name, str(e)[:200])

    elapsed = round(time.monotonic() - t0, 1)
    enriched_fields = []
    if comp.get("inn"):
        enriched_fields.append("INN")
    if comp.get("fns_financials"):
        enriched_fields.append("FNS")
    if comp.get("rating_2gis"):
        enriched_fields.append("2GIS")
    if comp.get("social_media"):
        enriched_fields.append("social")
    if comp.get("scraped_text"):
        enriched_fields.append("web")

    logger.info(
        "[step4.5] %s: done in %.1fs, enriched: %s",
        name, elapsed, ", ".join(enriched_fields) or "none",
    )
    return comp


def run(
    competitors: list[dict],
    progress_callback=None,
) -> list[dict]:
    """Enrich all competitors in parallel.

    Args:
        competitors: list of competitor dicts from step4
        progress_callback: optional (text, status) callback

    Returns:
        list of enriched competitor dicts
    """
    if not competitors:
        return competitors

    total = len(competitors)
    logger.info("[step4.5] Enriching %d competitors...", total)
    t0 = time.monotonic()

    if progress_callback:
        try:
            progress_callback(f"Обогащение {total} конкурентов", "active")
        except Exception:
            pass

    enriched = [None] * total

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_enrich_one, dict(comp), i): i
            for i, comp in enumerate(competitors)
        }

        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                enriched[idx] = future.result()
            except Exception as e:
                logger.error("[step4.5] Competitor %d failed: %s", idx, str(e)[:300])
                enriched[idx] = competitors[idx]  # keep original

    result = [c for c in enriched if c is not None]
    elapsed = round(time.monotonic() - t0, 1)

    # Stats
    with_inn = sum(1 for c in result if c.get("inn"))
    with_fns = sum(1 for c in result if c.get("fns_financials"))
    with_social = sum(1 for c in result if c.get("social_media"))
    with_2gis = sum(1 for c in result if c.get("rating_2gis"))
    with_reviews = sum(1 for c in result if c.get("reviews_count_2gis"))

    logger.info(
        "[step4.5] Done in %.1fs — INN: %d/%d, FNS: %d/%d, social: %d/%d, 2GIS: %d/%d",
        elapsed, with_inn, total, with_fns, total, with_social, total, with_2gis, total,
    )

    if progress_callback:
        try:
            progress_callback(
                f"Обогащено: {with_fns}/{total} ФНС, {with_2gis}/{total} 2ГИС, {with_social}/{total} соцсети",
                "done",
            )
        except Exception:
            pass

    return result
