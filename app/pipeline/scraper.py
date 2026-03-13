"""Website scraper: fetch URL -> extract structured text content.

Three-level cascade fetching:
  1. requests.get()               (fast, 15s timeout)
  2. Playwright headless Chromium  (30s timeout, JS rendering)
  3. Minimal fallback             (title + meta description from whatever HTML we got)
"""

from __future__ import annotations

import logging
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Playwright -- optional dependency (ARM64-compatible)
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

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

# Timeouts
TIMEOUT_REQUESTS = 15   # seconds
TIMEOUT_SCRAPLING = 30  # seconds
TIMEOUT_SUBPAGE = 8     # seconds for /about, /contacts, etc.


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


def _fetch_requests(url: str, timeout: int = TIMEOUT_REQUESTS) -> tuple[str, int]:
    """Fetch HTML via requests. Returns (html, status_code)."""
    resp = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text, resp.status_code


def _fetch_playwright(url: str, timeout: int = TIMEOUT_SCRAPLING) -> str:
    """Fetch HTML via Playwright headless Chromium (JS rendering).

    Uses timeout parameter (seconds) for page load wait.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=timeout * 1000)
            html = page.content()
        finally:
            browser.close()
    return html


def _extract_minimal(html: str | None) -> dict:
    """Extract title + meta description from partial/blocked HTML.

    Used as last-resort fallback when full parsing failed.
    Returns dict with title, description (may be empty strings).
    """
    result = {"title": "", "description": ""}
    if not html:
        return result

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        # Even lxml failed -- try html.parser
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return result

    if soup.title:
        result["title"] = soup.title.get_text(strip=True)

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        result["description"] = meta_desc["content"].strip()

    if not result["description"]:
        og_desc = soup.find("meta", attrs={"property": "og:description"})
        if og_desc and og_desc.get("content"):
            result["description"] = og_desc["content"].strip()

    return result


def _fetch_html(url: str, timeout: int = TIMEOUT_REQUESTS) -> tuple[str, str, list[str]]:
    """Three-level cascade fetch: requests -> Scrapling -> minimal.

    Returns (html, method, warnings) where method is
    "requests", "playwright", or "minimal".
    """
    warnings: list[str] = []
    last_html: str | None = None  # keep whatever HTML we got for minimal fallback

    # --- Attempt 1: requests ---
    t0 = time.monotonic()
    try:
        body, status_code = _fetch_requests(url, timeout=timeout)
        elapsed = time.monotonic() - t0
        last_html = body

        if not _is_blocked(status_code, body):
            if status_code < 400:
                logger.info(
                    "[scrape] OK url=%s method=requests status=%d time=%.2fs",
                    url, status_code, elapsed,
                )
                return body, "requests", warnings

        # Blocked
        block_reason = (
            f"HTTP {status_code}" if status_code in (403, 503)
            else "Cloudflare/WAF"
        )
        warnings.append(
            f"requests заблокирован ({block_reason}, {elapsed:.2f}s), "
            f"переключаюсь на Playwright"
        )
        logger.info(
            "[scrape] BLOCKED url=%s method=requests reason=%s status=%d time=%.2fs",
            url, block_reason, status_code, elapsed,
        )

    except requests.exceptions.RequestException as exc:
        elapsed = time.monotonic() - t0
        warnings.append(f"requests ошибка ({exc}, {elapsed:.2f}s), переключаюсь на Playwright")
        logger.info(
            "[scrape] FAIL url=%s method=requests error=%s time=%.2fs",
            url, exc, elapsed,
        )

    # --- Attempt 2: Playwright headless browser ---
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning(
            "[scrape] Playwright not installed -- fallback unavailable for %s", url
        )
        warnings.append(
            "Playwright не установлен -- fallback недоступен "
            "(pip install playwright && playwright install chromium)"
        )
    else:
        t1 = time.monotonic()
        try:
            html = _fetch_playwright(url, timeout=TIMEOUT_SCRAPLING)
            elapsed = time.monotonic() - t1
            logger.info(
                "[scrape] OK url=%s method=playwright time=%.2fs", url, elapsed,
            )
            warnings.append(f"Загружено через Playwright ({elapsed:.2f}s)")
            return html, "playwright", warnings

        except Exception as exc:
            elapsed = time.monotonic() - t1
            warnings.append(f"Playwright тоже не смог загрузить ({exc}, {elapsed:.2f}s)")
            logger.warning(
                "[scrape] FAIL url=%s method=playwright error=%s time=%.2fs",
                url, exc, elapsed,
            )

    # --- Attempt 3: minimal fallback ---
    # Extract whatever we can from the last HTML we received (even if blocked)
    minimal = _extract_minimal(last_html)
    if minimal["title"] or minimal["description"]:
        logger.info(
            "[scrape] MINIMAL url=%s title=%r desc_len=%d",
            url, minimal["title"][:60], len(minimal["description"]),
        )
        warnings.append(
            "Полный скрапинг не удался. Извлечены только title и meta description "
            "(метод minimal)."
        )
        # Build a synthetic HTML with just the extracted data so that the caller
        # can parse it uniformly.
        synthetic = (
            f"<html><head><title>{minimal['title']}</title>"
            f'<meta name="description" content="{minimal["description"]}">'
            f"</head><body><p>{minimal['description']}</p></body></html>"
        )
        return synthetic, "minimal", warnings

    # Total failure -- nothing useful at all
    logger.error(
        "[scrape] TOTAL FAIL url=%s -- all 3 methods exhausted", url,
    )
    raise RuntimeError(
        f"Все 3 метода скрапинга не смогли загрузить {url}: "
        f"requests, Scrapling, minimal. "
        f"Подробности: {'; '.join(warnings)}"
    )


def _parse_html(html: str, url: str) -> dict:
    """Parse HTML into structured data (title, text, contacts, social links)."""
    result: dict = {
        "title": "",
        "description": "",
        "headings": [],
        "text": "",
        "contacts": {},
        "social_links": [],
    }

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
        text = re.sub(r"\n{3,}", "\n\n", text)
        result["text"] = text[:8000]

    # Contacts: emails, phones
    text_full = body.get_text() if body else ""
    emails = set(
        re.findall(
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text_full
        )
    )
    phones = set(
        re.findall(
            r"(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}",
            text_full,
        )
    )
    if emails:
        result["contacts"]["emails"] = list(emails)[:5]
    if phones:
        result["contacts"]["phones"] = list(phones)[:5]

    # Address
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
                result["social_links"].append(
                    {"platform": platform, "handle": m.group(1), "url": href}
                )

    return result


def scrape_website(url: str, timeout: int = TIMEOUT_REQUESTS) -> dict:
    """Scrape a website and return structured content.

    Three-level cascade:
      1. requests (fast, 15s) — also retries via Playwright if SPA/JS-only
      2. Playwright headless Chromium (30s, JS rendering)
      3. Minimal fallback (title + meta description only)

    Returns dict with keys: url, domain, title, description, headings, text,
    contacts, social_links, pages_text, scrape_method, scrape_warnings.
    """
    if not url.startswith("http"):
        url = "https://" + url

    result: dict = {
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

    total_start = time.monotonic()

    # --- Main page fetch (3-level cascade) ---
    try:
        html, method, warnings = _fetch_html(url, timeout=timeout)
        result["scrape_method"] = method
        result["scrape_warnings"] = warnings
    except Exception as exc:
        result["error"] = str(exc)
        return result

    # --- Parse the fetched HTML ---
    parsed = _parse_html(html, url)
    result.update({
        "title": parsed["title"],
        "description": parsed["description"],
        "headings": parsed["headings"],
        "text": parsed["text"],
        "contacts": parsed["contacts"],
        "social_links": parsed["social_links"],
    })

    # --- JS-rendered SPA fallback: if requests got <50 chars, retry with Playwright ---
    if method == "requests" and len(parsed["text"]) < 50 and PLAYWRIGHT_AVAILABLE:
        logger.info(
            "[scrape] SPA detected (text_len=%d), retrying with Playwright: %s",
            len(parsed["text"]), url,
        )
        result["scrape_warnings"].append(
            f"SPA/JS-only сайт (текст {len(parsed['text'])} симв.), "
            f"переключаюсь на Playwright"
        )
        try:
            pw_html = _fetch_playwright(url, timeout=TIMEOUT_SCRAPLING)
            pw_parsed = _parse_html(pw_html, url)
            if len(pw_parsed["text"]) > len(parsed["text"]):
                parsed = pw_parsed
                result.update({
                    "title": parsed["title"] or result["title"],
                    "description": parsed["description"] or result["description"],
                    "headings": parsed["headings"] or result["headings"],
                    "text": parsed["text"],
                    "contacts": parsed["contacts"] or result["contacts"],
                    "social_links": parsed["social_links"] or result["social_links"],
                })
                result["scrape_method"] = "playwright"
                result["scrape_warnings"].append(
                    f"Playwright загрузил {len(parsed['text'])} символов"
                )
                html = pw_html  # use Playwright HTML for subpages too
                method = "playwright"
        except Exception as exc:
            logger.warning("[scrape] Playwright SPA retry failed: %s", exc)
            result["scrape_warnings"].append(f"Playwright retry не помог: {exc}")

    # --- Subpages (only if main method is not "minimal") ---
    if result["scrape_method"] != "minimal":
        subpages = [
            "/about", "/o-nas", "/contacts", "/kontakty", "/company",
            "/privacy", "/oferta", "/legal", "/requisites", "/rekvizity",
        ]
        for subpage in subpages:
            sub_url = urljoin(url, subpage)
            try:
                sub_html, sub_method, sub_warnings = _fetch_html(
                    sub_url, timeout=TIMEOUT_SUBPAGE
                )
                result["scrape_warnings"].extend(sub_warnings)
                if sub_method != "requests":
                    # Escalate to strongest method used
                    result["scrape_method"] = sub_method
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
                pass  # subpage failures are non-critical

    total_elapsed = time.monotonic() - total_start
    logger.info(
        "[scrape] DONE url=%s method=%s total_time=%.2fs text_len=%d",
        url, result["scrape_method"], total_elapsed, len(result.get("text", "")),
    )

    return result
