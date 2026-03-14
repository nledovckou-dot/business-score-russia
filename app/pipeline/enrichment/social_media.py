"""Social media enrichment: get real follower/subscriber counts.

Sources:
  - VK: API (public, no token for basic group info)
  - Telegram: Telemetr.me scraping (subscriber count)
  - Instagram: web scraping (follower count from profile page)

Each function returns None on failure (never raises).
"""

from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

logger = logging.getLogger(__name__)

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def get_vk_group(handle: str) -> dict | None:
    """Get VK group/page info by scraping the public page.

    VK API requires a token even for public groups, so we scrape
    the group page HTML to extract member count.

    Args:
        handle: VK group screen_name (e.g., "dodopizza" from vk.com/dodopizza)

    Returns:
        {"members": int, "name": str, "url": str} or None
    """
    if not handle:
        return None

    # Clean handle
    handle = handle.strip().lstrip("@").split("?")[0].split("/")[-1]
    if not handle or handle in ("share", "wall", "away.php"):
        return None

    url = f"https://vk.com/{urllib.parse.quote(handle)}"

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": _UA,
            "Accept": "text/html",
            "Accept-Language": "ru-RU,ru;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=12) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Extract member/subscriber count from page HTML
        # VK shows: "123 456 подписчиков" or "12,3 тыс. подписчиков"
        patterns = [
            # JSON in page: "memberCount":12345
            re.compile(r'"memberCount"\s*:\s*(\d+)'),
            re.compile(r'"members_count"\s*:\s*(\d+)'),
            # HTML: "12 345 подписчиков" or "12 345 участников"
            re.compile(r'([\d\s]+\d)\s*(?:подписчик|участник|member)', re.IGNORECASE),
            # "12,3 тыс." format
            re.compile(r'([\d,]+)\s*тыс\.\s*(?:подписчик|участник)', re.IGNORECASE),
        ]

        for pattern in patterns:
            match = pattern.search(html)
            if match:
                count_str = match.group(1).replace(" ", "").replace("\xa0", "")
                try:
                    if "тыс" in (match.group(0) if match.group(0) else ""):
                        count = int(float(count_str.replace(",", ".")) * 1000)
                    else:
                        count = int(count_str)
                    if count > 0:
                        # Extract group name
                        name_match = re.search(r'<title>([^<|]+)', html)
                        name = name_match.group(1).strip() if name_match else handle

                        logger.info("[social] VK '%s': %d members", handle, count)
                        return {
                            "members": count,
                            "name": name,
                            "url": url,
                        }
                except ValueError:
                    continue

        logger.info("[social] VK: no member count found for '%s'", handle)
        return None

    except Exception as e:
        logger.warning("[social] VK failed for '%s': %s", handle, str(e)[:200])
        return None


def get_telegram_subscribers(handle: str) -> dict | None:
    """Get Telegram channel/group subscriber count via Telemetr.me scraping.

    Args:
        handle: Telegram handle (e.g., "dodopizza" from t.me/dodopizza)

    Returns:
        {"subscribers": int, "name": str, "url": str} or None
    """
    if not handle:
        return None

    # Clean handle
    handle = handle.strip().lstrip("@").split("?")[0].split("/")[-1]
    if not handle or handle in ("share", "joinchat", "addstickers", "+"):
        return None

    url = f"https://telemetr.me/content/{urllib.parse.quote(handle)}/"

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": _UA,
            "Accept": "text/html",
            "Accept-Language": "ru-RU,ru;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Look for subscriber count patterns
        # Telemetr.me shows: "123 456 подписчиков" or "123K subscribers"
        patterns = [
            re.compile(r'"subscribers_count":\s*(\d+)'),
            re.compile(r'подписчик\w*[:\s]+(\d[\d\s]*\d|\d+)', re.IGNORECASE),
            re.compile(r'subscribers?[:\s]+(\d[\d\s]*\d|\d+)', re.IGNORECASE),
            re.compile(r'(\d[\d\s]*\d)\s*подписчик', re.IGNORECASE),
        ]

        for pattern in patterns:
            match = pattern.search(html)
            if match:
                count_str = match.group(1).replace(" ", "").replace("\xa0", "")
                try:
                    count = int(count_str)
                    if count > 0:
                        # Try to extract channel name
                        name_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html)
                        name = name_match.group(1).strip() if name_match else handle

                        logger.info("[social] TG '%s': %d subscribers", handle, count)
                        return {
                            "subscribers": count,
                            "name": name,
                            "url": f"https://t.me/{handle}",
                        }
                except ValueError:
                    continue

        logger.info("[social] TG: no subscriber count found for '%s'", handle)
        return None

    except Exception as e:
        logger.warning("[social] TG failed for '%s': %s", handle, str(e)[:200])
        return None


def get_instagram_followers(handle: str) -> dict | None:
    """Get Instagram follower count via web scraping.

    Note: Instagram aggressively blocks scraping. This is a best-effort
    attempt that works ~50% of the time without authentication.

    Args:
        handle: Instagram username (e.g., "dodopizza")

    Returns:
        {"followers": int, "name": str, "url": str} or None
    """
    if not handle:
        return None

    # Clean handle
    handle = handle.strip().lstrip("@").split("?")[0].split("/")[-1]
    if not handle:
        return None

    url = f"https://www.instagram.com/{urllib.parse.quote(handle)}/"

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": _UA,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Instagram embeds follower count in meta tags or JSON-LD
        patterns = [
            # Meta tag: <meta content="123 Followers" ...>
            re.compile(r'content="([\d,.\s]+)\s*Followers', re.IGNORECASE),
            # JSON: "edge_followed_by":{"count":12345}
            re.compile(r'"edge_followed_by"\s*:\s*\{\s*"count"\s*:\s*(\d+)'),
            # Meta description: "123K Followers"
            re.compile(r'([\d,.]+[KkMm]?)\s*Followers', re.IGNORECASE),
        ]

        for pattern in patterns:
            match = pattern.search(html)
            if match:
                count_str = match.group(1).replace(",", "").replace(" ", "").strip()
                try:
                    # Handle K/M suffixes
                    if count_str.upper().endswith("K"):
                        count = int(float(count_str[:-1]) * 1000)
                    elif count_str.upper().endswith("M"):
                        count = int(float(count_str[:-1]) * 1_000_000)
                    else:
                        count = int(float(count_str))

                    if count > 0:
                        logger.info("[social] IG '%s': %d followers", handle, count)
                        return {
                            "followers": count,
                            "name": handle,
                            "url": f"https://instagram.com/{handle}",
                        }
                except ValueError:
                    continue

        logger.info("[social] IG: no follower count found for '%s' (likely blocked)", handle)
        return None

    except urllib.error.HTTPError as e:
        if e.code == 302:
            logger.info("[social] IG: login redirect for '%s' (blocked)", handle)
        else:
            logger.warning("[social] IG HTTP %d for '%s'", e.code, handle)
        return None
    except Exception as e:
        logger.warning("[social] IG failed for '%s': %s", handle, str(e)[:200])
        return None


def enrich_social_links(social_links: list[dict]) -> list[dict]:
    """Enrich a list of social links with real follower/subscriber counts.

    Args:
        social_links: list of {"platform": str, "handle": str, "url": str}

    Returns:
        Same list with added "followers"/"subscribers"/"members" fields
    """
    for link in social_links:
        platform = link.get("platform", "")
        handle = link.get("handle", "")

        if not handle:
            continue

        try:
            if platform == "vk":
                data = get_vk_group(handle)
                if data:
                    link["members"] = data["members"]
                    link["verified_name"] = data["name"]
            elif platform == "telegram":
                data = get_telegram_subscribers(handle)
                if data:
                    link["subscribers"] = data["subscribers"]
                    link["verified_name"] = data["name"]
            elif platform == "instagram":
                data = get_instagram_followers(handle)
                if data:
                    link["followers"] = data["followers"]
        except Exception as e:
            logger.warning("[social] Enrichment failed for %s/%s: %s",
                          platform, handle, str(e)[:200])

    return social_links
