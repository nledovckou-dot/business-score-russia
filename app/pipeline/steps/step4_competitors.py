"""Step 4: Find competitors using GPT-5.2 Pro + verify via web search."""

from __future__ import annotations

import logging
from app.pipeline.llm_client import call_llm_json

logger = logging.getLogger(__name__)

SYSTEM = """Ты — аналитик российского рынка. Твоя задача — определить прямых конкурентов компании.
Отвечай ТОЛЬКО валидным JSON. Все данные на русском языке."""


def run(scraped: dict, company_info: dict, fns_data: dict) -> dict:
    """Find 10 competitors using GPT-5.2 Pro, then verify each via web search.

    Uses real company data + website content to find relevant competitors.
    Returns dict with: competitors list (with verification status),
    market_name, axes for perceptual map.
    """
    okved = fns_data.get("fns_company", {}).get("okved", "")
    okved_name = fns_data.get("fns_company", {}).get("okved_name", "")
    address = fns_data.get("fns_company", {}).get("address", "")
    revenue = ""
    if fns_data.get("financials"):
        last = fns_data["financials"][-1]
        rev = last.get("revenue")
        if rev:
            revenue = f"{rev:,.0f} тыс. руб."

    prompt = f"""Определи 10 прямых конкурентов для этой компании.

## Данные компании

Название: {company_info.get('name', '')}
Юрлицо: {company_info.get('legal_name', '')}
ИНН: {fns_data.get('fns_company', {}).get('inn', '')}
ОКВЭД: {okved} — {okved_name}
Адрес: {address}
Выручка: {revenue}
Описание: {company_info.get('description', '')}
Тип бизнеса: {company_info.get('business_type_guess', '')}

Сайт: {scraped.get('url', '')}
Title: {scraped.get('title', '')}
Заголовки: {', '.join(scraped.get('headings', [])[:15])}

Текст сайта (краткий):
{scraped.get('text', '')[:3000]}

## Задание

Верни JSON:
{{
    "market_name": "Название рынка/ниши на русском",
    "market_description": "Краткое описание рынка, 2-3 предложения",
    "axis_x": "Название оси X для перцептуальной карты (например: Цена)",
    "axis_y": "Название оси Y (например: Качество / Функциональность / Уникальность)",
    "competitors": [
        {{
            "name": "Название компании",
            "legal_name": "ООО «...» если знаешь или null",
            "inn": "ИНН если знаешь или null",
            "website": "https://...",
            "city": "Город",
            "description": "Чем занимается, 1 предложение",
            "why_competitor": "Почему это конкурент, 1 предложение",
            "estimated_size": "small / medium / large",
            "threat_level": "high / med / low"
        }}
    ]
}}

## Правила
1. Только РЕАЛЬНЫЕ российские компании, которые действительно работают на этом рынке
2. Конкуренты должны быть ПРЯМЫМИ — тот же продукт/услуга, тот же сегмент
3. Приоритет: компании из того же города/региона
4. Включи разные масштабы: крупных лидеров рынка + компании сопоставимого размера
5. Website — реальные домены, которые существуют
6. Оси карты должны быть релевантны типу бизнеса"""

    result = call_llm_json(
        prompt, provider="openai", system=SYSTEM,
        temperature=0.5, max_tokens=8000,
    )

    # ── Verify competitors via web search ──
    competitors = result.get("competitors", [])
    if competitors:
        logger.info(
            "Верификация %d конкурентов через веб-поиск...", len(competitors)
        )
        try:
            from app.pipeline.web_search import verify_competitors_batch
            competitors = verify_competitors_batch(competitors)

            # Log summary
            verified_count = sum(1 for c in competitors if c.get("verified"))
            unverified = [
                c.get("name", "?") for c in competitors if not c.get("verified")
            ]
            logger.info(
                "Верификация завершена: %d/%d подтверждены",
                verified_count, len(competitors),
            )
            if unverified:
                logger.warning(
                    "Не подтверждены: %s", ", ".join(unverified)
                )

        except Exception as e:
            logger.error("Ошибка верификации конкурентов: %s", e)
            # Graceful degradation: mark all as unverified
            for comp in competitors:
                comp.setdefault("verified", False)
                comp.setdefault("verification_confidence", "unverified")
                comp.setdefault("verification_url", None)
                comp.setdefault("verification_sources", [])
                comp.setdefault("verification_notes", "Верификация недоступна")

        result["competitors"] = competitors

    return result
