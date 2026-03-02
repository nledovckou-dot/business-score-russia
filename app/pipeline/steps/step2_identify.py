"""Step 2: Identify company from scraped data using Gemini Flash (fast + cheap)."""

from __future__ import annotations

import json
from app.pipeline.llm_client import call_llm_json


def run(scraped: dict) -> dict:
    """Identify company name, possible INN, legal entity from website text.

    Uses Gemini 2.5 Flash for speed.
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

    # Also check sub-pages for INN/legal info
    for page_name, page_text in scraped.get("pages_text", {}).items():
        if result.get("inn"):
            break
        # Quick scan for INN pattern
        import re
        inn_match = re.search(r"ИНН\s*:?\s*(\d{10,12})", page_text)
        if inn_match:
            result["inn"] = inn_match.group(1)

        # Quick scan for legal name
        if not result.get("legal_name"):
            legal_match = re.search(
                r"(ООО|ЗАО|АО|ПАО|ИП)\s*[«\"](.*?)[»\"]",
                page_text
            )
            if legal_match:
                result["legal_name"] = f"{legal_match.group(1)} «{legal_match.group(2)}»"

    return result
