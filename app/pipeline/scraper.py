"""Website scraper: fetch URL → extract structured text content.

Two-level fetching: requests.get() first, Scrapling StealthyFetcher fallback
on Cloudflare/403/503/captcha blocks.
"""

from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Scrapling — optional dependency
try:
    from scrapling.fetchers import StealthyFetcher
    SCRAPLING_AVAILABLE = True
except ImportError:
    SCRAPLING_AVAILABLE = False

logger = logging.getLogger(__name__)

_BLOCK_SIGNATURES = (
    "just a moment",
    "checking your browser",
    "cf-browser-verification",
    "cf-challenge",
    "enable javascript and cookies",
    "attention required",
    "ddos protection by",
)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}


def _is_blocked(status_code: int, body: str) -> bool:
    """Detect Cloudflare / WAF / captcha blocks."""
    if status_code in (403, 503):
        return True
    if not body or len(body) < 500:
        lower = body.lower() if body else ""
        if any(sig in lower for sig in ("challenge", "captcha", "cf-")):
            return True
    lower = body.lower() if body else ""
    return any(sig in lower for sig in _BLOCK_SIGNATURES)


def _fetch_html_scrapling(url: str) -> str:
    """Fetch HTML via Scrapling StealthyFetcher (headless browser)."""
    page = StealthyFetcher.fetch(
        url,
        headless=True,
        solve_cloudflare=True,
        block_webrtc=True,
        hide_canvas=True,
        network_idle=True,
        disable_resources=True,
    )
    return page.html_content


def _fetch_html(url: str, timeout: int = 15) -> tuple[str, str, list[str]]:
    """Two-level fetch: requests first, Scrapling fallback on block.

    Returns (html, method, warnings) where method is "requests" or "scrapling".
    """
    warnings: list[str] = []

    # Attempt 1: requests
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
        resp.encoding = resp.apparent_encoding or "utf-8"
        body = resp.text

        if not _is_blocked(resp.status_code, body):
            resp.raise_for_status()
            return body, "requests", warnings

        # Blocked — try fallback
        block_reason = f"HTTP {resp.status_code}" if resp.status_code in (403, 503) else "Cloudflare/WAF"
        warnings.append(f"requests заблокирован ({block_reason}), переключаюсь на Scrapling")
        logger.info("Blocked by %s on %s, trying Scrapling fallback", block_reason, url)

    except requests.exceptions.RequestException as e:
        warnings.append(f"requests ошибка ({e}), переключаюсь на Scrapling")
        logger.info("Request failed for %s: %s, trying Scrapling fallback", url, e)

    # Attempt 2: Scrapling
    if not SCRAPLING_AVAILABLE:
        logger.warning("Scrapling not installed — cannot bypass block for %s", url)
        warnings.append("Scrapling не установлен — fallback недоступен (pip install scrapling)")
        raise RuntimeError(
            f"Сайт заблокировал запрос, а Scrapling не установлен. "
            f"Установите: pip install scrapling && scrapling install"
        )

    try:
        html = _fetch_html_scrapling(url)
        warnings.append("Загружено через Scrapling StealthyFetcher")
        return html, "scrapling", warnings
    except Exception as e:
        warnings.append(f"Scrapling тоже не смог загрузить: {e}")
        raise RuntimeError(
            f"Оба метода не смогли загрузить {url}. "
            f"requests: см. предупреждения; Scrapling: {e}"
        )


def scrape_website(url: str, timeout: int = 15) -> dict:
    """Scrape a website and return structured content.

    Returns dict with keys: url, domain, title, description, headings, text,
    contacts, social_links, pages_text, scrape_method, scrape_warnings.
    """
    if not url.startswith("http"):
        url = "https://" + url

    result = {
        "url": url,
        "domain": urlparse(url).netloc,
        "title": "",
        "description": "",
        "headings": [],
        "text": "",
        "contacts": {},
        "social_links": [],
        "pages_text": {},
        "scrape_method": "requests",
        "scrape_warnings": [],
    }

    try:
        html, method, warnings = _fetch_html(url, timeout=timeout)
        result["scrape_method"] = method
        result["scrape_warnings"] = warnings
    except Exception as e:
        result["error"] = str(e)
        return result

    soup = BeautifulSoup(html, "lxml")

    # Remove noise
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()

    # Title
    if soup.title:
        result["title"] = soup.title.get_text(strip=True)

    # Meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        result["description"] = meta_desc["content"].strip()

    # OG description fallback
    if not result["description"]:
        og_desc = soup.find("meta", attrs={"property": "og:description"})
        if og_desc and og_desc.get("content"):
            result["description"] = og_desc["content"].strip()

    # Headings
    for tag in soup.find_all(["h1", "h2", "h3"], limit=30):
        txt = tag.get_text(strip=True)
        if txt and len(txt) < 200:
            result["headings"].append(txt)

    # Main text (first 8000 chars)
    body = soup.find("body")
    if body:
        text = body.get_text(separator="\n", strip=True)
        # Collapse whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        result["text"] = text[:8000]

    # Contacts: emails, phones
    text_full = body.get_text() if body else ""
    emails = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text_full))
    phones = set(re.findall(r"(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}", text_full))
    if emails:
        result["contacts"]["emails"] = list(emails)[:5]
    if phones:
        result["contacts"]["phones"] = list(phones)[:5]

    # Address (look for common patterns)
    addr_el = soup.find(string=re.compile(r"(?:г\.|ул\.|пр\.|наб\.|пл\.)"))
    if addr_el:
        result["contacts"]["address_hint"] = addr_el.strip()[:200]

    # Social links
    social_patterns = {
        "instagram": r"instagram\.com/([^/?\s\"']+)",
        "telegram": r"t\.me/([^/?\s\"']+)",
        "vk": r"vk\.com/([^/?\s\"']+)",
        "youtube": r"youtube\.com/(?:c/|channel/|@)([^/?\s\"']+)",
        "tiktok": r"tiktok\.com/@([^/?\s\"']+)",
    }
    for link in soup.find_all("a", href=True):
        href = link["href"]
        for platform, pattern in social_patterns.items():
            m = re.search(pattern, href, re.I)
            if m:
                result["social_links"].append({"platform": platform, "handle": m.group(1), "url": href})

    # Try to fetch /about, /contacts pages for more context
    for subpage in ["/about", "/o-nas", "/contacts", "/kontakty", "/company"]:
        sub_url = urljoin(url, subpage)
        try:
            sub_html, sub_method, sub_warnings = _fetch_html(sub_url, timeout=8)
            result["scrape_warnings"].extend(sub_warnings)
            if sub_method != "requests":
                result["scrape_method"] = sub_method  # escalate to strongest method used
            s2 = BeautifulSoup(sub_html, "lxml")
            for tag in s2.find_all(["script", "style", "noscript"]):
                tag.decompose()
            body2 = s2.find("body")
            if body2:
                t2 = body2.get_text(separator="\n", strip=True)
                t2 = re.sub(r"\n{3,}", "\n\n", t2)
                if len(t2) > 100:
                    result["pages_text"][subpage] = t2[:4000]
        except Exception:
            pass

    return result
