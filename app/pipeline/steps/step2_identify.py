"""Step 2: Identify company from scraped data using Gemini Flash (fast + cheap).

If scraping returned minimal data (JS-heavy site), falls back to web search
to find INN and legal entity name via DuckDuckGo.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
from app.pipeline.llm_client import call_llm_json
from app.pipeline.web_search import _search_duckduckgo

logger = logging.getLogger(__name__)


def _search_inn_web(domain: str, company_name: str) -> dict:
    """Search for INN and legal name via DuckDuckGo when scraping gave minimal data.

    Tries two queries:
      1. "{domain} ИНН"
      2. "{company_name} ИНН юрлицо"

    Returns dict with found inn, legal_name (or None).
    """
    found_inn = None
    found_legal = None

    queries = [
        f"{domain} ИНН",
        f'"{company_name}" ИНН юрлицо',
    ]

    for query in queries:
        if found_inn:
            break
        try:
            results = _search_duckduckgo(query)
        except Exception as e:
            logger.warning("Web search failed for '%s': %s", query, str(e)[:200])
            continue

        for r in results[:10]:
            text = f"{r.get('title', '')} {r.get('snippet', '')}"

            # Search for INN pattern
            if not found_inn:
                inn_match = re.search(r"ИНН\s*:?\s*(\d{10,12})", text)
                if inn_match:
                    found_inn = inn_match.group(1)
                    logger.info("Found INN %s via web search: %s", found_inn, query)

            # Search for legal name
            if not found_legal:
                legal_match = re.search(
                    r"(ООО|ЗАО|АО|ПАО|ИП)\s*[«\"«»]?\s*([\w\s\-\.]+?)[\s»\"»,\.]",
                    text,
                )
                if legal_match:
                    found_legal = f'{legal_match.group(1)} «{legal_match.group(2).strip()}»'
                    logger.info("Found legal name '%s' via web search", found_legal)

            if found_inn and found_legal:
                break

    return {"inn": found_inn, "legal_name": found_legal}


def _whois_lookup(domain: str) -> dict:
    """WHOIS lookup for .ru/.su/.рф domains via whois.tcinet.ru.

    Returns dict with inn, org (or None for each).
    """
    result = {"inn": None, "org": None}

    # Strip to root domain (business.auto.ru → auto.ru)
    domain = domain.lower().strip()
    parts = domain.split(".")
    # Find root: keep last 2 parts for .ru/.su, last 2 for .xn--p1ai/.рф
    if len(parts) > 2:
        # e.g. business.auto.ru → auto.ru, www.shop.example.ru → example.ru
        domain = ".".join(parts[-2:])

    # Only .ru, .su, .рф (xn--p1ai)
    tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
    is_rf = tld in ("ru", "su") or domain.endswith(".xn--p1ai") or tld == "рф"
    if not is_rf:
        return result

    # Convert .рф to punycode
    try:
        parts = domain.split(".")
        domain = ".".join(
            part.encode("idna").decode("ascii") for part in parts
        )
    except Exception:
        pass  # already ASCII

    try:
        proc = subprocess.run(
            ["whois", "-h", "whois.tcinet.ru", domain],
            capture_output=True, text=True, timeout=10,
        )
        text = proc.stdout

        inn_match = re.search(r"^taxpayer-id:\s*(\d{10,12})", text, re.MULTILINE)
        if inn_match:
            result["inn"] = inn_match.group(1)

        org_match = re.search(r"^org:\s*(.+)", text, re.MULTILINE)
        if org_match:
            result["org"] = org_match.group(1).strip()

    except FileNotFoundError:
        logger.warning("whois binary not found, skipping WHOIS lookup")
    except subprocess.TimeoutExpired:
        logger.warning("WHOIS timeout for %s", domain)
    except Exception as e:
        logger.warning("WHOIS failed for %s: %s", domain, str(e)[:200])

    return result


def run(scraped: dict) -> dict:
    """Identify company name, possible INN, legal entity from website text.

    Uses Gemini 2.5 Flash for speed.
    Falls back to web search if scraping returned minimal data and no INN found.
    Returns dict with: name, legal_name, inn, address, description, business_type_guess.
    """
    site_text = scraped.get("text", "")[:4000]
    contacts = json.dumps(scraped.get("contacts", {}), ensure_ascii=False)

    prompt = f"""Проанализируй данные с сайта и определи компанию.

URL: {scraped.get('url', '')}
Домен: {scraped.get('domain', '')}
Title: {scraped.get('title', '')}
Description: {scraped.get('description', '')}

Контакты: {contacts}

Текст сайта (первые 4000 символов):
{site_text}

Верни JSON:
{{
    "name": "Торговое название компании",
    "legal_name": "ООО «...» / ИП ... / null если не найдено",
    "inn": "ИНН если найден на сайте или null",
    "address": "Адрес если найден или null",
    "description": "Что делает компания, 1-2 предложения",
    "business_type_guess": "B2C_SERVICE / B2C_PRODUCT / B2B_SERVICE / B2B_PRODUCT / PLATFORM",
    "search_query": "Запрос для поиска юрлица в ФНС (название компании + город)"
}}

Правила:
- Если ИНН есть на сайте (обычно в подвале или на странице контактов) — обязательно укажи
- legal_name — ищи формы: ООО, ЗАО, АО, ПАО, ИП
- search_query — для поиска в ФНС, включи город если есть
- ТОЛЬКО JSON, без пояснений"""

    result = call_llm_json(prompt, provider="gemini", temperature=0.2, max_tokens=2000)

    # ── Regex extraction from FULL text (not truncated) ──
    # INN is often in the footer which gets cut by the 4000-char limit for LLM.
    # Search the complete main text + all sub-pages for INN and legal name.
    all_texts = [scraped.get("text", "")]
    for page_text in scraped.get("pages_text", {}).values():
        all_texts.append(page_text)
    full_text = "\n".join(all_texts)

    if not result.get("inn"):
        inn_match = re.search(r"ИНН\s*:?\s*(\d{10,12})", full_text)
        if inn_match:
            result["inn"] = inn_match.group(1)
            logger.info("Found INN %s via regex in scraped text", result["inn"])

    if not result.get("legal_name"):
        legal_match = re.search(
            r"(ООО|ЗАО|АО|ПАО|ИП)\s*[«\"](.*?)[»\"]",
            full_text,
        )
        if legal_match:
            result["legal_name"] = f"{legal_match.group(1)} «{legal_match.group(2)}»"
            logger.info("Found legal name '%s' via regex", result["legal_name"])

    # ── WHOIS lookup for .ru/.su/.рф domains ──
    if not result.get("inn"):
        domain = scraped.get("domain", "")
        whois_data = _whois_lookup(domain)
        if whois_data["inn"]:
            result["inn"] = whois_data["inn"]
            result["inn_source"] = "whois"
            logger.info("Found INN %s via WHOIS for %s", result["inn"], domain)
        if whois_data["org"]:
            result["whois_org"] = whois_data["org"]
            logger.info("Found org '%s' via WHOIS for %s", whois_data["org"], domain)

    # ── Web search fallback: if no INN found, search DuckDuckGo ──
    if not result.get("inn"):
        domain = scraped.get("domain", "")
        company_name = result.get("name", domain)
        scrape_method = scraped.get("scrape_method", "unknown")
        logger.info(
            "No INN from scraping (method=%s), trying web search for '%s'",
            scrape_method, company_name,
        )
        web_found = _search_inn_web(domain, company_name)
        if web_found["inn"]:
            result["inn"] = web_found["inn"]
            result["inn_source"] = "web_search"
        if web_found["legal_name"] and not result.get("legal_name"):
            result["legal_name"] = web_found["legal_name"]

    return result
