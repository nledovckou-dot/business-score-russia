"""2GIS Catalog API client: search organizations, get reviews/ratings, find competitors.

Endpoints used:
  - /3.0/items          — search organizations by name, rubric, geo
  - /3.0/items/byid     — get organization details by ID
  - /2.0/region/search  — resolve city name to region_id
  - /2.0/catalog/rubric/search — search rubric IDs by keyword
  - public-api.reviews.2gis.com — fetch individual review texts (requires separate access)

Auth: API key via `key` query parameter.
Rate limit: max 10 req/sec (enforced via threading lock + min interval).
Cache: in-memory dict to avoid duplicate API calls within a session.

Integration points (pipeline):
  - step4_competitors.py  -> search_competitors() to find nearby businesses by rubric
  - step5_deep_analysis.py -> enrich_company_data() for richer LLM context
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

# ── Configuration ──

_BASE_URL = "https://catalog.api.2gis.com"
_REVIEWS_BASE_URL = "https://public-api.reviews.2gis.com"

_DEFAULT_FIELDS = (
    "items.reviews,items.schedule,items.rubrics,"
    "items.point,items.org,items.address"
)

# ── Rate limiting: max 10 req/sec ──

_lock = threading.Lock()
_last_call: float = 0.0
_MIN_INTERVAL = 0.1  # 100ms between calls = max 10 req/sec
_MAX_RETRIES = 3

# ── In-memory cache ──

_cache: dict[str, Any] = {}

# ── Well-known region IDs for major Russian cities ──
# Avoids an extra API call for the most common cities.

_KNOWN_REGIONS: dict[str, int] = {
    "москва": 32,
    "санкт-петербург": 38,
    "петербург": 38,
    "спб": 38,
    "новосибирск": 42,
    "екатеринбург": 52,
    "казань": 61,
    "нижний новгород": 47,
    "челябинск": 48,
    "самара": 45,
    "омск": 44,
    "ростов-на-дону": 49,
    "уфа": 57,
    "красноярск": 54,
    "пермь": 53,
    "воронеж": 63,
    "волгоград": 62,
    "краснодар": 60,
    "тюмень": 55,
    "владивосток": 79,
    "сочи": 60,
}

# Day name mapping for schedule parsing
_DAYS_RU: dict[str, str] = {
    "Mon": "Пн",
    "Tue": "Вт",
    "Wed": "Ср",
    "Thu": "Чт",
    "Fri": "Пт",
    "Sat": "Сб",
    "Sun": "Вс",
}


# ═══════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════


def _api_key() -> str:
    """Get 2GIS API key from env or fallback to hardcoded default."""
    key = os.environ.get("TWOGIS_API_KEY", "")
    if not key:
        # Fallback to the project key (provided during integration)
        key = "30d59c90-ea14-4e1b-b8a8-7284c7646d12"
    return key


def _cache_key(endpoint: str, params: dict) -> str:
    """Build a deterministic cache key from endpoint + sorted params."""
    sorted_params = sorted(params.items())
    return f"{endpoint}|{'&'.join(f'{k}={v}' for k, v in sorted_params)}"


def _get(url: str, params: dict, timeout: int = 15) -> dict:
    """HTTP GET with rate limiting, retry, and caching.

    Rate limiting uses a global lock to ensure minimum interval between calls.
    Retries up to 3 times on 429/500/502/503 with exponential backoff.
    Results are cached in memory to avoid duplicate calls.
    """
    global _last_call

    # Add API key
    params["key"] = _api_key()

    # Check cache
    ck = _cache_key(url, {k: v for k, v in params.items() if k != "key"})
    if ck in _cache:
        logger.debug("[2gis] Cache hit: %s", ck[:80])
        return _cache[ck]

    # Build full URL
    qs = urllib.parse.urlencode(params, encoding="utf-8")
    full_url = f"{url}?{qs}"

    for attempt in range(_MAX_RETRIES):
        # Rate limit
        with _lock:
            now = time.monotonic()
            wait = _MIN_INTERVAL - (now - _last_call)
            if wait > 0:
                time.sleep(wait)
            _last_call = time.monotonic()

        req = urllib.request.Request(
            full_url,
            headers={
                "Accept": "application/json",
                "User-Agent": "BSR-Pipeline/1.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                # Cache successful response
                _cache[ck] = data
                return data

        except urllib.error.HTTPError as e:
            error_body = ""
            try:
                error_body = e.read().decode("utf-8")[:300] if e.fp else ""
            except Exception:
                pass

            # Retryable errors
            if e.code in (429, 500, 502, 503) and attempt < _MAX_RETRIES - 1:
                backoff = (attempt + 1) * 2  # 2s, 4s
                logger.warning(
                    "[2gis] HTTP %d (attempt %d/%d), retrying in %ds: %s",
                    e.code, attempt + 1, _MAX_RETRIES, backoff,
                    error_body[:100],
                )
                time.sleep(backoff)
                continue

            logger.error(
                "[2gis] HTTP %d on %s: %s",
                e.code, url, error_body[:200],
            )
            raise RuntimeError(
                f"2GIS API error {e.code}: {error_body[:200]}"
            ) from e

        except Exception as e:
            if attempt < _MAX_RETRIES - 1:
                time.sleep(1)
                continue
            logger.error("[2gis] Request failed: %s", e)
            raise RuntimeError(f"2GIS API request failed: {e}") from e

    raise RuntimeError("2GIS API: all retries exhausted")


def _parse_schedule(schedule: dict | None) -> dict[str, str]:
    """Parse 2GIS schedule dict into human-readable format.

    Input: {"Mon": {"working_hours": [{"from": "09:00", "to": "22:00"}]}, ...}
    Output: {"Пн": "09:00-22:00", "Вт": "09:00-22:00", ...}
    """
    if not schedule:
        return {}

    result: dict[str, str] = {}
    for eng_day, ru_day in _DAYS_RU.items():
        day_data = schedule.get(eng_day)
        if not day_data:
            result[ru_day] = "Выходной"
            continue

        hours = day_data.get("working_hours", [])
        if not hours:
            result[ru_day] = "Выходной"
            continue

        periods = []
        for period in hours:
            fr = period.get("from", "")
            to = period.get("to", "")
            if fr and to:
                periods.append(f"{fr}-{to}")
        result[ru_day] = ", ".join(periods) if periods else "Выходной"

    return result


def _parse_item(item: dict) -> dict:
    """Parse a single 2GIS item (branch/org) into a flat dict.

    Extracts: name, address, rating, reviews_count, working_hours,
    rubrics, coordinates, org info, branch_id.
    """
    # Reviews data
    reviews = item.get("reviews") or {}
    general_rating = reviews.get("general_rating")
    org_rating = reviews.get("org_rating")

    # Use branch-level rating if available, fall back to org-level
    rating = general_rating if general_rating is not None else org_rating

    general_review_count = reviews.get("general_review_count", 0)
    general_review_count_with_stars = reviews.get(
        "general_review_count_with_stars", 0
    )
    org_review_count = reviews.get("org_review_count", 0)

    # Point (coordinates)
    point = item.get("point") or {}

    # Rubrics
    rubrics_raw = item.get("rubrics") or []
    rubrics = [
        {
            "id": r.get("id", ""),
            "name": r.get("name", ""),
            "kind": r.get("kind", ""),
            "alias": r.get("alias", ""),
            "parent_id": r.get("parent_id", ""),
        }
        for r in rubrics_raw
    ]

    # Org info
    org = item.get("org") or {}

    # Schedule
    schedule = _parse_schedule(item.get("schedule"))

    # Address: prefer address_name, add address_comment if present
    address = item.get("address_name", "")
    comment = item.get("address_comment", "")
    if comment:
        address = f"{address} ({comment})"

    # Full name may include building_name
    full_name = item.get("full_name", item.get("name", ""))

    return {
        "branch_id": item.get("id", ""),
        "name": item.get("name", ""),
        "full_name": full_name,
        "address": address,
        "type": item.get("type", ""),
        "rating": rating,
        "reviews_count": general_review_count,
        "reviews_count_with_stars": general_review_count_with_stars,
        "org_rating": org_rating,
        "org_review_count": org_review_count,
        "working_hours": schedule,
        "rubrics": rubrics,
        "coordinates": {
            "lat": point.get("lat"),
            "lon": point.get("lon"),
        },
        "org_id": org.get("id", ""),
        "org_name": org.get("name", ""),
        "org_primary_name": org.get("primary", ""),
        "branch_count": org.get("branch_count", 0),
    }


# ═══════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════


def resolve_region_id(city: str) -> Optional[int]:
    """Resolve a city name to a 2GIS region_id.

    Checks the built-in lookup table first, then falls back to the API.
    Returns None if city not found.

    Args:
        city: City name in Russian (e.g., "Москва", "Санкт-Петербург")
    """
    if not city:
        return None

    # Normalize
    city_lower = city.lower().strip()

    # Check known regions first (no API call needed)
    if city_lower in _KNOWN_REGIONS:
        return _KNOWN_REGIONS[city_lower]

    # Fallback: API call
    try:
        data = _get(
            f"{_BASE_URL}/2.0/region/search",
            {"q": city},
        )
        items = data.get("result", {}).get("items", [])
        if items:
            region_id = int(items[0]["id"])
            # Cache for future use
            _KNOWN_REGIONS[city_lower] = region_id
            return region_id
    except Exception as e:
        logger.warning("[2gis] Failed to resolve region for '%s': %s", city, e)

    return None


def search_rubric(query: str, region_id: int = 32) -> list[dict]:
    """Search for rubric (category) IDs by keyword.

    Useful for finding the rubric_id to pass to search_competitors().

    Args:
        query: Search keyword (e.g., "ресторан", "салон красоты")
        region_id: 2GIS region ID (default: 32 = Moscow)

    Returns:
        List of dicts: {id, name, alias, branch_count, org_count}
    """
    try:
        data = _get(
            f"{_BASE_URL}/2.0/catalog/rubric/search",
            {"q": query, "region_id": str(region_id)},
        )
    except RuntimeError as e:
        logger.error("[2gis] Rubric search failed for '%s': %s", query, e)
        return []

    items = data.get("result", {}).get("items", [])
    return [
        {
            "id": item.get("id", ""),
            "name": item.get("name", ""),
            "alias": item.get("alias", ""),
            "branch_count": item.get("branch_count", 0),
            "org_count": item.get("org_count", 0),
        }
        for item in items
    ]


def search_organization(
    name: str,
    city: str | None = None,
    page_size: int = 5,
) -> list[dict]:
    """Search for organizations by name in 2GIS.

    Args:
        name: Organization name or search query
        city: Optional city name to narrow results (e.g., "Москва")
        page_size: Max results to return (1-50, default 5)

    Returns:
        List of parsed organization dicts with keys:
        branch_id, name, full_name, address, rating, reviews_count,
        reviews_count_with_stars, org_rating, org_review_count,
        working_hours, rubrics, coordinates, org_id, org_name,
        org_primary_name, branch_count
    """
    params: dict[str, str] = {
        "q": name,
        "type": "branch",
        "fields": _DEFAULT_FIELDS,
        "page_size": str(min(page_size, 50)),
    }

    # Resolve city to region_id
    if city:
        region_id = resolve_region_id(city)
        if region_id:
            params["region_id"] = str(region_id)

    try:
        data = _get(f"{_BASE_URL}/3.0/items", params)
    except RuntimeError as e:
        logger.error(
            "[2gis] Organization search failed for '%s': %s", name, e
        )
        return []

    items = data.get("result", {}).get("items", [])
    results = [_parse_item(item) for item in items]

    logger.info(
        "[2gis] search_organization('%s', city=%s): found %d results (total: %d)",
        name,
        city,
        len(results),
        data.get("result", {}).get("total", 0),
    )

    return results


def get_organization_by_id(org_id: str) -> dict | None:
    """Get organization details by 2GIS branch/item ID.

    Args:
        org_id: 2GIS item ID (e.g., "70000001054707077")

    Returns:
        Parsed organization dict, or None if not found.
    """
    try:
        data = _get(
            f"{_BASE_URL}/3.0/items/byid",
            {"id": org_id, "fields": _DEFAULT_FIELDS},
        )
    except RuntimeError as e:
        logger.error("[2gis] Get by ID failed for '%s': %s", org_id, e)
        return None

    items = data.get("result", {}).get("items", [])
    if not items:
        return None

    return _parse_item(items[0])


def get_organization_reviews(
    org_id: str,
    limit: int = 20,
) -> list[dict]:
    """Get individual reviews for an organization.

    Uses the 2GIS public reviews API (public-api.reviews.2gis.com).
    Note: this endpoint may require separate API access. If unavailable
    (403), returns an empty list and logs a warning.

    Args:
        org_id: 2GIS branch ID
        limit: Max reviews to fetch (default 20)

    Returns:
        List of dicts: {text, rating, date, author}
        Returns empty list if reviews API is not accessible.
    """
    params: dict[str, str] = {
        "limit": str(min(limit, 50)),
        "is_advertiser": "false",
    }

    try:
        data = _get(
            f"{_REVIEWS_BASE_URL}/2.0/branches/{org_id}/reviews",
            params,
            timeout=10,
        )
    except RuntimeError as e:
        error_str = str(e)
        if "403" in error_str or "FORBIDDEN" in error_str:
            # Expected: reviews API requires separate access/subscription.
            # The catalog API still returns review summaries (rating, count).
            logger.info(
                "[2gis] Reviews text API not accessible for branch %s "
                "(requires separate API access). "
                "Review summaries (rating/count) are still available "
                "via search_organization().",
                org_id,
            )
        else:
            logger.warning(
                "[2gis] Reviews fetch failed for branch %s: %s",
                org_id, e,
            )
        return []

    items = data.get("result", {}).get("items", [])
    reviews = []
    for item in items:
        user = item.get("user") or {}
        reviews.append({
            "text": item.get("text", ""),
            "rating": item.get("rating"),
            "date": item.get("date_created", ""),
            "author": user.get("name", ""),
        })

    logger.info(
        "[2gis] get_organization_reviews(%s): fetched %d reviews",
        org_id, len(reviews),
    )
    return reviews


def search_competitors(
    rubric_id: str,
    lat: float,
    lon: float,
    radius: int = 5000,
    limit: int = 10,
    sort: str = "rating",
) -> list[dict]:
    """Find nearby competitors by rubric within a given radius.

    Searches for organizations in the same rubric (category) around
    the specified coordinates. Useful for finding direct competitors
    of a B2C business (restaurant, salon, clinic, etc.).

    Args:
        rubric_id: 2GIS rubric ID (e.g., "164" for restaurants).
                   Use search_rubric() to find the right ID.
        lat: Latitude of the center point
        lon: Longitude of the center point
        radius: Search radius in meters (default 5000 = 5km)
        limit: Max results (default 10, max 50)
        sort: Sort order — "rating" (default) or "distance"

    Returns:
        List of parsed organization dicts, sorted by rating or distance.
    """
    params: dict[str, str] = {
        "rubric_id": rubric_id,
        "point": f"{lon},{lat}",
        "radius": str(radius),
        "type": "branch",
        "fields": _DEFAULT_FIELDS,
        "page_size": str(min(limit, 50)),
        "sort": sort,
    }

    try:
        data = _get(f"{_BASE_URL}/3.0/items", params)
    except RuntimeError as e:
        logger.error(
            "[2gis] Competitor search failed for rubric %s at (%s, %s): %s",
            rubric_id, lat, lon, e,
        )
        return []

    items = data.get("result", {}).get("items", [])
    results = [_parse_item(item) for item in items]
    total = data.get("result", {}).get("total", 0)

    logger.info(
        "[2gis] search_competitors(rubric=%s, radius=%dm): "
        "found %d results (total: %d)",
        rubric_id, radius, len(results), total,
    )

    return results


def enrich_company_data(
    name: str,
    city: str | None = None,
) -> dict:
    """High-level function: search company in 2GIS and enrich with reviews.

    Combines search_organization + get_organization_reviews into a single
    call. Returns enriched data suitable for LLM context in the pipeline.

    Args:
        name: Company name to search
        city: Optional city for narrowing results

    Returns:
        Dict with keys:
        - source: "2gis"
        - found: bool
        - name, full_name, address
        - rating, reviews_count, org_rating, org_review_count
        - reviews_sample: list of review dicts (if API accessible)
        - working_hours: dict of day -> hours
        - coordinates: {lat, lon}
        - rubrics: list of rubric dicts
        - org_id, branch_id, branch_count
        - all_branches: list of other branches found (if multiple matches)
    """
    result: dict[str, Any] = {
        "source": "2gis",
        "found": False,
        "name": name,
        "city": city,
    }

    # Search for the organization
    orgs = search_organization(name, city=city, page_size=10)
    if not orgs:
        logger.info(
            "[2gis] enrich_company_data: no results for '%s' (city=%s)",
            name, city,
        )
        return result

    # Pick the best match: prefer exact name match, then highest review count
    best = _pick_best_match(orgs, name)
    result["found"] = True

    # Copy all fields from the best match
    result.update({
        "name": best["name"],
        "full_name": best["full_name"],
        "address": best["address"],
        "rating": best["rating"],
        "reviews_count": best["reviews_count"],
        "reviews_count_with_stars": best["reviews_count_with_stars"],
        "org_rating": best["org_rating"],
        "org_review_count": best["org_review_count"],
        "working_hours": best["working_hours"],
        "coordinates": best["coordinates"],
        "rubrics": best["rubrics"],
        "org_id": best["org_id"],
        "branch_id": best["branch_id"],
        "branch_count": best["branch_count"],
        "org_name": best["org_name"],
        "org_primary_name": best["org_primary_name"],
    })

    # Try to fetch individual reviews
    if best["branch_id"]:
        reviews = get_organization_reviews(best["branch_id"], limit=20)
        result["reviews_sample"] = reviews
    else:
        result["reviews_sample"] = []

    # Include other branches as context (different locations of same org)
    other_branches = [
        {
            "branch_id": o["branch_id"],
            "name": o["name"],
            "address": o["address"],
            "rating": o["rating"],
            "reviews_count": o["reviews_count"],
        }
        for o in orgs
        if o["branch_id"] != best["branch_id"]
    ]
    if other_branches:
        result["other_branches"] = other_branches[:5]

    logger.info(
        "[2gis] enrich_company_data('%s'): found '%s', rating=%.1f, "
        "reviews=%d, branches=%d",
        name,
        best["name"],
        best.get("rating") or 0,
        best.get("reviews_count") or 0,
        best.get("branch_count") or 0,
    )

    return result


def _pick_best_match(orgs: list[dict], query: str) -> dict:
    """Pick the best matching organization from search results.

    Priority:
    1. Exact name match (case-insensitive)
    2. Name starts with query
    3. Highest review count (as a proxy for relevance)
    """
    query_lower = query.lower().strip()

    # Exact match
    for org in orgs:
        org_name = (org.get("org_primary_name") or org.get("name") or "").lower()
        if org_name == query_lower:
            return org

    # Starts with query
    for org in orgs:
        org_name = (org.get("org_primary_name") or org.get("name") or "").lower()
        if org_name.startswith(query_lower):
            return org

    # Contains query
    for org in orgs:
        org_name = (org.get("name") or "").lower()
        if query_lower in org_name:
            return org

    # Fallback: most reviewed
    return max(orgs, key=lambda o: o.get("reviews_count") or 0)


def clear_cache() -> int:
    """Clear the in-memory cache. Returns number of entries cleared."""
    count = len(_cache)
    _cache.clear()
    return count
