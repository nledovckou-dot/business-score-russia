"""Step 1b: Marketplace data analysis (v2.0).

Runs for B2C_PRODUCT / PLATFORM / B2B_B2C_HYBRID.
One GPT call: analyze marketplace presence (per-unit pricing, seller_id, SKU).
"""

from __future__ import annotations

from typing import Optional

from app.pipeline.llm_client import call_llm_json


SYSTEM = """Ты — аналитик маркетплейсов. Тебе предоставлены данные о компании.
Проанализируй присутствие на маркетплейсах и верни структурированный JSON.

Правила:
- Per-unit pricing: цена набора ÷ кол-во единиц. Показать ОБА значения
- Средний чек = total_revenue / total_orders, НЕ среднее цен каталога
- Бестселлеры = по продажам в руб. (mpstats), НЕ по отзывам/рейтингам
- SKU count с датой сбора
- Фильтр по seller_id, НЕ brand name (избежать контаминации)
- Ответ — ТОЛЬКО валидный JSON"""


def run(
    company_info: dict,
    scraped: dict,
    competitors: Optional[list[dict]] = None,
) -> dict:
    """Analyze marketplace presence for a company and its competitors.

    Returns marketplace_data dict with pricing, SKU, channels.
    """
    comp_names = []
    if competitors:
        comp_names = [c.get("name", "") for c in competitors[:8]]

    prompt = f"""Проанализируй маркетплейс-присутствие компании и конкурентов.

## Компания
Название: {company_info.get('name', '')}
Сайт: {scraped.get('url', '')}
Тип: {company_info.get('business_type_guess', '')}

## Конкуренты
{', '.join(comp_names)}

## Задание

Верни JSON:
{{
  "company_marketplace": {{
    "wb": {{
      "present": true/false,
      "sku_count": число_или_null,
      "avg_price": число_или_null,
      "top_products": ["продукт1", "продукт2"]
    }},
    "ozon": {{
      "present": true/false,
      "sku_count": число_или_null,
      "avg_price": число_или_null,
      "top_products": ["продукт1", "продукт2"]
    }},
    "yandex_market": {{
      "present": true/false,
      "sku_count": число_или_null
    }}
  }},
  "competitor_marketplace": [
    {{
      "name": "Конкурент",
      "wb_present": true/false,
      "ozon_present": true/false,
      "wb_sku": число_или_null,
      "ozon_sku": число_или_null,
      "avg_price": число_или_null
    }}
  ],
  "pricing_notes": ["заметка о ценообразовании"],
  "data_quality": "Описание качества и ограничений данных"
}}

Если нет данных — верни пустые значения, не выдумывай."""

    try:
        result = call_llm_json(
            prompt, provider="openai", system=SYSTEM,
            temperature=0.3, max_tokens=4000,
        )
    except Exception:
        result = {
            "company_marketplace": {},
            "competitor_marketplace": [],
            "pricing_notes": [],
            "data_quality": "Данные не получены (ошибка LLM)",
        }

    return result
