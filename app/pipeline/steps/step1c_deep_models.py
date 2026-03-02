"""Step 1c: Deep business models analysis (v2.0).

One GPT call: lifecycle stage + sales channel map + perceptual map methodology.
For each competitor: stage + evidence + 12 channels (exists/not).
"""

from __future__ import annotations

from app.pipeline.llm_client import call_llm_json


SYSTEM = """Ты — бизнес-аналитик. Определи стадию жизненного цикла и каналы продаж
для каждого конкурента.

## Стадии жизненного цикла
- startup: убытки, рост >50%, фандрейзинг. Убыток нормален.
- growth: рост >20%, активные инвестиции в развитие.
- investment: стройка заводов, M&A, крупные CAPEX. ЗАПРЕТ на вывод о «неэффективности» — убыток объясняется инвестициями.
- mature: стабильная маржа, low CAPEX. Benchmark-ready.

## Каналы продаж — проверь для КАЖДОГО конкурента:
Сайт D2C, WB, Ozon, Яндекс.Маркет, Собственная розница, Мультибренд сети,
B2B/опт, HoReCa, Франшиза, Экспорт/СНГ, Lamoda, СберМегаМаркет.

Правило: «канал отсутствует» = проверено 3+ источника.
Заявка «стать партнёром» на сайте = канал ЕСТЬ.

- Ответ — ТОЛЬКО валидный JSON"""


# Standard channels to check
STANDARD_CHANNELS = [
    "Сайт D2C",
    "WB",
    "Ozon",
    "Яндекс.Маркет",
    "Собственная розница",
    "Мультибренд сети",
    "B2B/опт",
    "HoReCa",
    "Франшиза",
    "Экспорт/СНГ",
    "Lamoda",
    "СберМегаМаркет",
]


def run(
    company_info: dict,
    competitors: list[dict],
    fns_data: dict,
    market_info: dict,
) -> dict:
    """Analyze lifecycle stages and sales channels for all competitors.

    Returns dict with 'lifecycles', 'channels', 'perceptual_methodology'.
    """
    # Build competitor context
    comp_lines = []
    for i, c in enumerate(competitors, 1):
        comp_lines.append(
            f"{i}. {c.get('name','')} — {c.get('description','')}"
            f" (сайт: {c.get('website','')})"
            f" [угроза: {c.get('threat_level','med')}]"
        )
    comp_text = "\n".join(comp_lines)

    channels_list = ", ".join(STANDARD_CHANNELS)

    bt = company_info.get("business_type_guess", "")

    prompt = f"""Проанализируй жизненный цикл и каналы продаж конкурентов.

## Компания
Название: {company_info.get('name', '')}
Тип бизнеса: {bt}
Рынок: {market_info.get('market_name', '')}

## Конкуренты
{comp_text}

## Задание

Верни JSON:
{{
  "lifecycles": {{
    "Название конкурента": {{
      "stage": "startup/growth/investment/mature",
      "evidence": ["причина 1", "причина 2", "причина 3"],
      "year_founded": "ГГГГ или null"
    }}
  }},
  "channels": {{
    "Название конкурента": [
      {{"channel_name": "Сайт D2C", "exists": true/false/null, "source": "источник"}},
      {{"channel_name": "WB", "exists": true/false/null, "source": "источник"}}
    ]
  }},
  "perceptual_methodology": {{
    "x_axis": "название оси X",
    "y_axis": "название оси Y",
    "scale": "1-100",
    "criteria": {{
      "x_low": "что означает 0-20 по X",
      "x_mid": "что означает 40-60 по X",
      "x_high": "что означает 80-100 по X",
      "y_low": "что означает 0-20 по Y",
      "y_mid": "что означает 40-60 по Y",
      "y_high": "что означает 80-100 по Y"
    }}
  }}
}}

Каналы для проверки: {channels_list}

Для каждого конкурента:
1. Определи стадию жизненного цикла с обоснованием
2. Проверь все каналы продаж (exists: true если найдено, false если проверено и нет, null если неизвестно)
3. Предложи методологию перцептуальной карты для типа "{bt}" """

    try:
        result = call_llm_json(
            prompt, provider="openai", system=SYSTEM,
            temperature=0.3, max_tokens=8000,
        )
    except Exception:
        # Fallback: empty structure
        result = {
            "lifecycles": {},
            "channels": {},
            "perceptual_methodology": {},
        }

    # Validate and clean up
    if not isinstance(result.get("lifecycles"), dict):
        result["lifecycles"] = {}
    if not isinstance(result.get("channels"), dict):
        result["channels"] = {}

    valid_stages = {"startup", "growth", "investment", "mature"}
    for name, lc in result.get("lifecycles", {}).items():
        if isinstance(lc, dict):
            stage = str(lc.get("stage", "mature")).lower()
            if stage not in valid_stages:
                lc["stage"] = "mature"
            if not isinstance(lc.get("evidence"), list):
                lc["evidence"] = []

    for name, channels in result.get("channels", {}).items():
        if isinstance(channels, list):
            result["channels"][name] = [
                ch for ch in channels
                if isinstance(ch, dict) and ch.get("channel_name")
            ]

    return result
