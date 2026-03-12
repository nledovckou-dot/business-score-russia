"""Step 5: Deep analysis — разбит на секции (T3).

Вместо одного гигантского запроса к LLM, анализ разделён на 7 независимых
функций, каждая со своим промптом и JSON-схемой ответа. Это даёт:
- Надёжность: короткие ответы не ломают JSON
- Прозрачность: видно, в какой секции LLM ошибся
- Параллельное выполнение через ThreadPoolExecutor (T18)

v3.1: параллельный секционный пайплайн
"""

from __future__ import annotations

import json
import logging
import time
import concurrent.futures
from typing import Any, Callable, Optional

from app.pipeline.llm_client import call_llm_json

logger = logging.getLogger(__name__)


# ── Общий системный контекст для всех секций ──

_BASE_SYSTEM = """Ты — ведущий бизнес-аналитик (pipeline v3.0). Тебе предоставлены РЕАЛЬНЫЕ данные.

ВАЖНО:
- Используй РЕАЛЬНЫЕ данные, не выдумывай
- Если данных нет — верни null / пустой список, не фантазируй
- Все суммы в тысячах рублей если не указано иное
- ВСЁ на русском языке
- Ответ — ТОЛЬКО валидный JSON (без markdown, без ```)"""


# ── Вспомогательные функции ──


def _prepare_context(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
    competitors: list[dict] | None = None,
    market_info: dict | None = None,
) -> dict[str, str]:
    """Подготовить текстовые блоки контекста из сырых данных.

    Возвращает dict с ключами: company_text, fin_text, founders_text,
    director_text, comp_text, social_info, aff_text.
    """
    egrul = fns_data.get("egrul", {})

    # Финансы
    fin_text = "Нет данных из ФНС"
    if fns_data.get("financials"):
        fin_lines = []
        for f in fns_data["financials"]:
            fin_lines.append(
                f"  {f['year']}: выручка={f.get('revenue', '?')} тыс., "
                f"прибыль={f.get('net_profit', '?')} тыс., "
                f"активы={f.get('assets', '?')} тыс."
            )
        fin_text = "\n".join(fin_lines)

    # Учредители
    founders_text = "Нет данных"
    if egrul.get("founders"):
        f_lines = []
        for f in egrul["founders"]:
            f_lines.append(f"  {f.get('name', '')} — доля: {f.get('share_percent', '')}%")
        founders_text = "\n".join(f_lines)

    director = egrul.get("director", {})
    director_text = director.get("name", "Нет данных")

    # Конкуренты
    comp_text = ""
    if competitors:
        for i, c in enumerate(competitors, 1):
            comp_text += f"\n{i}. {c.get('name', '')} — {c.get('description', '')}"
            if c.get("website"):
                comp_text += f" ({c['website']})"
            comp_text += f" [угроза: {c.get('threat_level', 'med')}]"

    # Соцсети
    social_info = ""
    for s in scraped.get("social_links", []):
        social_info += f"  {s['platform']}: {s.get('handle', '')} ({s.get('url', '')})\n"

    # Аффилированные лица
    aff_text = "Нет данных"
    if fns_data.get("affiliates"):
        a_lines = []
        for a in fns_data["affiliates"][:10]:
            a_lines.append(
                f"  {a.get('name', '')} (ИНН: {a.get('inn', '')}) — {a.get('connection', '')}"
            )
        aff_text = "\n".join(a_lines)

    # Текст о компании (компактный блок для промптов)
    company_text = (
        f"Название: {company_info.get('name', '')}\n"
        f"Юрлицо: {egrul.get('full_name', company_info.get('legal_name', ''))}\n"
        f"ИНН: {egrul.get('inn', '')}\n"
        f"ОГРН: {egrul.get('ogrn', '')}\n"
        f"ОКВЭД: {egrul.get('okved', '')} — {egrul.get('okved_name', '')}\n"
        f"Дата регистрации: {egrul.get('reg_date', '')}\n"
        f"Уставный капитал: {egrul.get('capital', '')}\n"
        f"Директор: {director_text}\n"
        f"Тип бизнеса: {company_info.get('business_type_guess', '')}\n"
        f"Описание: {company_info.get('description', '')}\n"
        f"Сайт: {scraped.get('url', '')}"
    )

    return {
        "company_text": company_text,
        "fin_text": fin_text,
        "founders_text": founders_text,
        "director_text": director_text,
        "comp_text": comp_text,
        "social_info": social_info,
        "aff_text": aff_text,
    }


def _safe_llm_call(
    prompt: str,
    section_name: str,
    system: str = _BASE_SYSTEM,
    provider: str = "openai",
    temperature: float = 0.4,
    max_tokens: int = 6000,
) -> dict:
    """Вызвать LLM с retry при ошибке JSON.

    При первой ошибке — retry с указанием ошибки в промпте.
    При второй ошибке — вернуть пустой dict и залогировать.
    """
    try:
        result = call_llm_json(
            prompt, provider=provider, system=system,
            temperature=temperature, max_tokens=max_tokens,
        )
        logger.info(f"[step5:{section_name}] LLM вернул JSON, {len(json.dumps(result, ensure_ascii=False))} символов")
        return result
    except RuntimeError as e:
        logger.warning(f"[step5:{section_name}] Первая попытка: {e}")

    # Retry: добавляем в промпт указание на ошибку
    retry_prompt = (
        prompt + "\n\n## ВНИМАНИЕ: предыдущий ответ содержал невалидный JSON. "
        "Верни ТОЛЬКО валидный JSON без markdown-форматирования, без ``` блоков."
    )
    try:
        result = call_llm_json(
            retry_prompt, provider=provider, system=system,
            temperature=0.2, max_tokens=max_tokens,
        )
        logger.info(f"[step5:{section_name}] Retry успешен")
        return result
    except RuntimeError as e:
        logger.error(f"[step5:{section_name}] Retry провален: {e}")
        return {}


# ════════════════════════════════════════════════════════
# Секция 1: Рынок (market, regulatory_trends, tech_trends)
# ════════════════════════════════════════════════════════

def analyze_market(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
    market_info: dict,
) -> dict:
    """Анализ рынка: размер, тренды, регулирование, технологии.

    Возвращает dict с ключами: market, regulatory_trends, tech_trends.
    """
    ctx = _prepare_context(scraped, company_info, fns_data)
    market_name = market_info.get("market_name", "")
    market_desc = market_info.get("market_description", "")

    prompt = f"""Проведи анализ рынка на основе данных.

## Компания
{ctx['company_text']}

## Рынок
Название: {market_name}
Описание: {market_desc}

## Текст сайта (фрагмент)
{scraped.get('text', '')[:3000]}

## Задание

Верни JSON:

{{
  "market": {{
    "market_name": "{market_name}",
    "market_size": "XX млрд руб.",
    "growth_rate": "+X% CAGR",
    "data_points": [
      {{"year": 2021, "value": число, "label": "млрд руб."}},
      {{"year": 2022, "value": число, "label": "млрд руб."}},
      {{"year": 2023, "value": число, "label": "млрд руб."}},
      {{"year": 2024, "value": число, "label": "млрд руб."}}
    ],
    "trends": ["тренд 1", "тренд 2", "тренд 3", "тренд 4"],
    "sources": ["источник 1", "источник 2"]
  }},
  "regulatory_trends": [
    {{"date": "2024", "title": "Название", "description": "Описание", "color": "gold"}},
    {{"date": "2025", "title": "Название", "description": "Описание", "color": "blue"}}
  ],
  "tech_trends": ["технологический тренд 1", "тренд 2", "тренд 3"]
}}

## Правила
1. Размер рынка и темп роста — реалистичные для {market_name} в России
2. Data points — 4 года, данные рынка в млрд руб.
3. Тренды — 4 штуки, конкретные, актуальные
4. Regulatory_trends — 2-4 законодательных/регуляторных изменения
5. Tech_trends — 3-4 технологических тренда отрасли
6. Если данных нет — используй отраслевые знания, но источники пометь как оценочные"""

    result = _safe_llm_call(prompt, "market", max_tokens=4000)
    return result


# ════════════════════════════════════════════════════════
# Секция 2: Конкуренты (обогащение)
# ════════════════════════════════════════════════════════

def analyze_competitors_deep(
    scraped: dict,
    company_info: dict,
    competitors: list[dict],
    market_info: dict,
    deep_models: Optional[dict] = None,
    marketplace_data: Optional[dict] = None,
) -> dict:
    """Глубокий анализ конкурентов: radar, lifecycle, каналы продаж.

    Возвращает dict с ключами: competitors, radar_dimensions.
    """
    axis_x = market_info.get("axis_x", "Цена")
    axis_y = market_info.get("axis_y", "Качество")

    # Подготовка текста конкурентов
    comp_text = ""
    for i, c in enumerate(competitors, 1):
        comp_text += f"\n{i}. {c.get('name', '')} — {c.get('description', '')}"
        if c.get("website"):
            comp_text += f" ({c['website']})"
        comp_text += f" [угроза: {c.get('threat_level', 'med')}]"

    # Контекст deep_models (lifecycle, channels из step1c)
    deep_models_text = ""
    if deep_models:
        if deep_models.get("lifecycles"):
            deep_models_text += "\n## Предварительные данные жизненного цикла\n"
            for name, lc in deep_models["lifecycles"].items():
                deep_models_text += (
                    f"  {name}: стадия={lc.get('stage', '?')}, "
                    f"основание={', '.join(lc.get('evidence', []))}\n"
                )
        if deep_models.get("channels"):
            deep_models_text += "\n## Предварительные данные каналов продаж\n"
            for name, channels in deep_models["channels"].items():
                ch_list = [
                    f"{ch['channel_name']}="
                    f"{'да' if ch.get('exists') else 'нет' if ch.get('exists') is False else '?'}"
                    for ch in channels
                ]
                deep_models_text += f"  {name}: {', '.join(ch_list)}\n"

    # Контекст маркетплейсов (WB, Ozon и т.д.)
    marketplace_text = ""
    if marketplace_data:
        marketplace_text = (
            f"\n## Данные маркетплейсов\n"
            f"{json.dumps(marketplace_data, ensure_ascii=False, indent=2)[:3000]}\n"
        )

    prompt = f"""Обогати данные конкурентов для бизнес-анализа.

## Компания-объект анализа
Название: {company_info.get('name', '')}
Тип бизнеса: {company_info.get('business_type_guess', '')}

## Подтверждённые конкуренты
{comp_text}
{deep_models_text}
{marketplace_text}

## Задание

Верни JSON:

{{
  "competitors": [
    {{
      "name": "Название конкурента",
      "description": "Краткое описание (2-3 предложения)",
      "website": "https://...",
      "address": "адрес или null",
      "x": 0-100,
      "y": 0-100,
      "radar_scores": {{"Param1": 1-10, "Param2": 1-10, "Param3": 1-10, "Param4": 1-10, "Param5": 1-10, "Param6": 1-10}},
      "metrics": {{"Выручка": "значение или null", "Сотрудники": "значение или null", "Год основания": "ГГГГ или null"}},
      "threat_level": "high/med/low",
      "lifecycle": {{
        "stage": "startup/growth/investment/mature",
        "evidence": ["причина 1", "причина 2"],
        "year_founded": "ГГГГ или null"
      }},
      "sales_channels": [
        {{"channel_name": "Сайт D2C", "exists": true/false/null, "source": "источник"}},
        {{"channel_name": "WB", "exists": true/false/null, "source": "источник"}},
        {{"channel_name": "Ozon", "exists": true/false/null, "source": "источник"}},
        {{"channel_name": "Собственные точки", "exists": true/false/null, "source": "источник"}},
        {{"channel_name": "B2B/опт", "exists": true/false/null, "source": "источник"}},
        {{"channel_name": "HoReCa", "exists": true/false/null, "source": "источник"}},
        {{"channel_name": "Lamoda", "exists": true/false/null, "source": "источник"}}
      ]
    }}
  ],
  "radar_dimensions": ["Param1", "Param2", "Param3", "Param4", "Param5", "Param6"]
}}

## Правила
1. ИСПОЛЬЗУЙ ВСЕХ конкурентов из списка выше — не пропускай и не добавляй новых
2. x, y — координаты на перцептуальной карте (ось X = {axis_x}, ось Y = {axis_y})
3. radar_scores — 6 параметров, одинаковые для ВСЕХ конкурентов
4. radar_dimensions — те же 6 параметров (названия)
5. lifecycle — стадия + обоснование. Если CAPEX/стройка — stage=investment
6. sales_channels — 7 каналов минимум. exists=null если неизвестно
7. Если есть предварительные данные lifecycle/channels — используй их, но можешь уточнить"""

    result = _safe_llm_call(prompt, "competitors", max_tokens=8000)
    return result


# ════════════════════════════════════════════════════════
# Секция 3: Компания (swot, digital, company, market_share)
# ════════════════════════════════════════════════════════

def analyze_company(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
    competitors: list[dict],
) -> dict:
    """Анализ компании: SWOT, digital-аудит, описание, доля рынка.

    Возвращает dict с ключами: company, swot, digital, market_share.
    """
    ctx = _prepare_context(scraped, company_info, fns_data, competitors)
    egrul = fns_data.get("egrul", {})

    # Имена конкурентов для market_share
    comp_names = [c.get("name", f"Конкурент {i+1}") for i, c in enumerate(competitors)]

    prompt = f"""Проведи анализ компании: SWOT, digital-аудит, описание.

## Компания
{ctx['company_text']}

## Соцсети
{ctx['social_info']}

## Финансы из ФНС (тыс. руб.)
{ctx['fin_text']}

## Конкуренты (для контекста market_share)
{ctx['comp_text']}

## Текст сайта (фрагмент)
{scraped.get('text', '')[:4000]}

## Задание

Верни JSON:

{{
  "company": {{
    "name": "{company_info.get('name', '')}",
    "legal_name": "{egrul.get('full_name', '')}",
    "inn": "{egrul.get('inn', '')}",
    "okved": "{egrul.get('okved', '')}",
    "business_type": "{company_info.get('business_type_guess', 'B2B_SERVICE')}",
    "address": "адрес из данных",
    "website": "{scraped.get('url', '')}",
    "description": "Что делает компания, 2-3 предложения на основе данных сайта",
    "badges": ["badge1", "badge2", "badge3"]
  }},
  "swot": {{
    "strengths": ["сила 1 (с цифрами)", "сила 2", "сила 3", "сила 4"],
    "weaknesses": ["слабость 1", "слабость 2", "слабость 3", "слабость 4"],
    "opportunities": ["возм. 1", "возм. 2", "возм. 3", "возм. 4"],
    "threats": ["угроза 1", "угроза 2", "угроза 3", "угроза 4"]
  }},
  "digital": {{
    "social_accounts": [
      {{"platform": "...", "handle": "@...", "followers": число_или_null, "engagement_rate": число_или_null, "avg_likes": число_или_null, "avg_comments": число_или_null, "avg_views": число_или_null}}
    ],
    "seo_score": 0-100,
    "monthly_traffic": число
  }},
  "market_share": {{
    "{company_info.get('name', 'Компания')}": процент,
    {', '.join(f'"{n}": процент' for n in comp_names[:5])},
    "Другие": процент
  }}
}}

## Правила
1. company.description — на основе РЕАЛЬНОГО текста сайта
2. badges — 3 коротких тега (уникальные характеристики компании)
3. SWOT — по 4 пункта, конкретные, с цифрами из ФНС где возможно
4. digital — social_accounts: ОБЯЗАТЕЛЬНО заполни минимум 2-3 платформы
   - Для КАЖДОЙ компании в России проверь: VK, Telegram, Instagram
   - handle: используй формат @company_name если не знаешь точный хендл
   - followers: число подписчиков или null если неизвестно
   - engagement_rate: ER в процентах = (лайки + комменты + репосты) / подписчики * 100. Оцени по типичным для отрасли. Обычно: 1-3% хорошо, 3-6% отлично, >6% вирусный контент. null если неизвестно
   - avg_likes: среднее лайков на пост (оценка). null если неизвестно
   - avg_comments: среднее комментариев на пост (оценка). null если неизвестно
   - avg_views: среднее просмотров (для TG/VK). null если неизвестно
   - seo_score: 50-80 по умолчанию если нет данных
   - monthly_traffic: оценочное число посещений
   - ВАЖНО: engagement_rate — ГЛАВНАЯ метрика, важнее чем followers. Подписчики можно накрутить, ER нельзя
5. market_share — оценочный, в процентах, сумма = 100
6. Если данных social_accounts нет — всё равно верни массив с platform/handle/null для VK/TG/Instagram
7. НЕ возвращай ПУСТОЙ список social_accounts — это ломает отчёт"""

    result = _safe_llm_call(prompt, "company", max_tokens=5000)
    return result


# ════════════════════════════════════════════════════════
# Секция 4: Стратегия (recommendations, kpi, scenarios, timeline)
# ════════════════════════════════════════════════════════

def analyze_strategy(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
    competitors: list[dict],
) -> dict:
    """Стратегический анализ: рекомендации, KPI, сценарии.

    Возвращает dict с ключами: recommendations, kpi_benchmarks, scenarios,
    implementation_timeline.
    """
    ctx = _prepare_context(scraped, company_info, fns_data, competitors)
    bt = company_info.get("business_type_guess", "B2B_SERVICE")

    # Финансы для сценариев и KPI current values
    fin_json = "null"
    fns_current_hint = ""
    if fns_data.get("financials"):
        fin_json = json.dumps(fns_data["financials"], ensure_ascii=False)
        latest = fns_data["financials"][-1]
        rev = latest.get("revenue")
        profit = latest.get("net_profit")
        emp = latest.get("employees")
        yr = latest.get("year", "?")
        hints = [f"Год: {yr}"]
        if rev is not None:
            hints.append(f"Выручка: {rev} тыс. ₽")
            if profit is not None:
                margin = round(profit / rev * 100, 1) if rev else 0
                hints.append(f"Рентабельность: {margin}%")
        if profit is not None:
            hints.append(f"Чистая прибыль: {profit} тыс. ₽")
        if emp:
            hints.append(f"Сотрудников: {emp}")
            if rev:
                hints.append(f"Выручка/сотрудник: {round(rev / emp)} тыс. ₽")
        fns_current_hint = "\n".join(hints)

    prompt = f"""Разработай стратегию для компании.

## Компания
{ctx['company_text']}

## Финансы из ФНС (тыс. руб.)
{ctx['fin_text']}

## Конкуренты
{ctx['comp_text']}

## Тип бизнеса
{bt}

## Текущие показатели (ФНС) — используй как current в KPI
{fns_current_hint if fns_current_hint else "Нет данных ФНС"}

## Задание

Верни JSON:

{{
  "recommendations": [
    {{
      "title": "Рекомендация",
      "description": "Подробное описание (2-4 предложения)",
      "priority": "high/medium/low",
      "timeline": "Q1-Q2 2026",
      "expected_impact": "+X% метрика"
    }}
  ],
  "kpi_benchmarks": [
    {{"name": "KPI название", "current": число_или_null, "benchmark": число, "unit": "ед."}}
  ],
  "scenarios": [
    {{"name": "optimistic", "label": "Оптимистичный", "metrics": {{"Выручка, тыс. руб.": число, "Прибыль, тыс. руб.": число, "Сотрудники": число}}}},
    {{"name": "base", "label": "Базовый", "metrics": {{"Выручка, тыс. руб.": число, "Прибыль, тыс. руб.": число, "Сотрудники": число}}}},
    {{"name": "pessimistic", "label": "Пессимистичный", "metrics": {{"Выручка, тыс. руб.": число, "Прибыль, тыс. руб.": число, "Сотрудники": число}}}}
  ],
  "implementation_timeline": [
    {{"date": "Q1 2026", "title": "Шаг 1", "description": "Описание", "color": "gold"}},
    {{"date": "Q2 2026", "title": "Шаг 2", "description": "Описание", "color": "blue"}}
  ]
}}

## Правила
1. Рекомендации — 5-6 штук, приоритизированные (минимум 2 high)
2. KPI — 6-8, релевантных типу бизнеса ({bt})
3. ВАЖНО для kpi_benchmarks: поле "current" заполняй РЕАЛЬНЫМИ данными из ФНС выше:
   - Выручка = последний год из финансов
   - Рентабельность = чистая_прибыль / выручка × 100
   - Сотрудники = из данных ФНС
   - Если данных нет — ставь null (не выдумывай)
4. Сценарии — 3, с 3-5 метриками, основанных на реальной выручке из ФНС
5. Реальные финансы: {fin_json}
6. implementation_timeline — 4-6 шагов по кварталам
7. Если тип HYBRID — метрики B2C и B2B РАЗДЕЛЬНО в kpi_benchmarks"""

    result = _safe_llm_call(prompt, "strategy", max_tokens=5000)
    return result


# ════════════════════════════════════════════════════════
# Секция 5: Приложения (glossary, methodology, calc_traces, open_questions)
# ════════════════════════════════════════════════════════

def analyze_appendix(
    company_info: dict,
    fns_data: dict,
    market_info: dict,
) -> dict:
    """Приложения: глоссарий, методология, calc-traces, открытые вопросы.

    Возвращает dict с ключами: glossary, methodology, calc_traces, open_questions.
    """
    bt = company_info.get("business_type_guess", "B2B_SERVICE")
    market_name = market_info.get("market_name", "")

    # Подготовка данных ФНС для calc_traces
    fin_json = "null"
    if fns_data.get("financials"):
        fin_json = json.dumps(fns_data["financials"], ensure_ascii=False)

    prompt = f"""Сформируй приложения к бизнес-анализу.

## Контекст
Компания: {company_info.get('name', '')}
Тип бизнеса: {bt}
Рынок: {market_name}
Финансы ФНС: {fin_json}

## Задание

Верни JSON:

{{
  "glossary": {{
    "Термин1": "Определение и формула (если есть)",
    "Термин2": "Определение",
    "EBITDA": "...",
    "ROE": "...",
    "LTV": "...",
    "CAC": "...",
    "CAGR": "...",
    "RevPASH": "..."
  }},
  "methodology": {{
    "Источники данных": "ФНС (бухгалтерская отчётность), ЕГРЮЛ, Rusprofile, сайт компании, Яндекс Карты, HH.ru",
    "Период анализа": "Данные за 2021-2024 гг., анализ проведён в марте 2026",
    "Допущения": "описание ключевых допущений"
  }},
  "calc_traces": [
    {{
      "metric_name": "Название показателя",
      "value": "итоговое значение",
      "formula": "формула расчёта",
      "inputs": {{"вход1": значение}},
      "sources": ["источник"],
      "confidence": "FACT/CALC/ESTIMATE"
    }}
  ],
  "open_questions": ["вопрос 1 (что нужно уточнить)", "вопрос 2"]
}}

## Правила
1. glossary — 8-12 терминов, релевантных типу бизнеса ({bt})
2. methodology — все 3 поля обязательны
3. calc_traces — минимум 5 записей для ключевых показателей
4. calc_traces confidence: FACT (данные ФНС), CALC (вычислено из фактов), ESTIMATE (с допущениями)
5. Правило: если вход = ESTIMATE, результат = ESTIMATE
6. open_questions — 3-5 вопросов, которые нужно уточнить у компании"""

    result = _safe_llm_call(prompt, "appendix", max_tokens=4000)
    return result


# ════════════════════════════════════════════════════════
# Секция 6: Фаундеры и мнения
# ════════════════════════════════════════════════════════

def analyze_opinions(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
    market_info: dict,
) -> dict:
    """Фаундеры компании и мнения лидеров отрасли.

    Возвращает dict с ключами: founders, opinions.
    """
    egrul = fns_data.get("egrul", {})
    market_name = market_info.get("market_name", "")

    # Данные учредителей из ЕГРЮЛ
    founders_text = "Нет данных"
    if egrul.get("founders"):
        f_lines = []
        for f in egrul["founders"]:
            f_lines.append(f"  {f.get('name', '')} — доля: {f.get('share_percent', '')}%")
        founders_text = "\n".join(f_lines)

    director = egrul.get("director", {})
    director_text = director.get("name", "Нет данных")

    # Аффилированные лица
    aff_text = "Нет данных"
    if fns_data.get("affiliates"):
        a_lines = []
        for a in fns_data["affiliates"][:10]:
            a_lines.append(
                f"  {a.get('name', '')} (ИНН: {a.get('inn', '')}) — {a.get('connection', '')}"
            )
        aff_text = "\n".join(a_lines)

    prompt = f"""Сформируй данные о фаундерах и мнения лидеров отрасли.

## Компания
Название: {company_info.get('name', '')}
Юрлицо: {egrul.get('full_name', '')}
Рынок: {market_name}

## Учредители (ЕГРЮЛ)
{founders_text}

## Директор
{director_text}

## Аффилированные лица
{aff_text}

## Задание

Верни JSON:

{{
  "founders": [
    {{
      "name": "ФИО",
      "role": "Должность (Учредитель / Генеральный директор / ...)",
      "share": "X%" или null,
      "company": "{egrul.get('full_name', '')}",
      "social": {{}}
    }}
  ],
  "opinions": [
    {{
      "author": "Имя Фамилия",
      "role": "Должность, Компания",
      "quote": "Цитата о рынке/отрасли",
      "date": "Месяц Год",
      "source": "Название СМИ"
    }}
  ]
}}

## Правила
1. founders — используй данные из ЕГРЮЛ выше, директора добавь отдельно
2. Если доля не указана — share = null
3. opinions — 3-5 цитат РЕАЛЬНЫХ лидеров рынка «{market_name}»
4. Цитаты должны быть от реальных людей (руководители крупных компаний отрасли, аналитики)
5. Если точные цитаты неизвестны — НЕ выдумывай, лучше меньше, но реальные"""

    result = _safe_llm_call(prompt, "opinions", max_tokens=3000)
    return result


# ════════════════════════════════════════════════════════
# Секция 8: Продукты и услуги
# ════════════════════════════════════════════════════════

def analyze_products(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
) -> dict:
    """Анализ продуктов/услуг компании.

    Возвращает dict с ключом: products.
    """
    ctx = _prepare_context(scraped, company_info, fns_data)
    bt = company_info.get("business_type_guess", "B2B_SERVICE")

    prompt = f"""Проанализируй продукты и услуги компании.

## Компания
{ctx['company_text']}

## Текст сайта (фрагмент)
{scraped.get('text', '')[:5000]}

## Тип бизнеса
{bt}

## Задание

Верни JSON:

{{
  "products": [
    {{
      "name": "Название продукта/услуги/тарифа",
      "price": "Цена или ценовой диапазон (если известна)",
      "description": "Краткое описание (1-2 предложения)",
      "features": ["Ключевая особенность 1", "Ключевая особенность 2"]
    }}
  ]
}}

## Правила
1. Извлеки 3-6 основных продуктов/услуг/тарифов из текста сайта
2. Если компания — платформа (маркетплейс), перечисли основные сервисы
3. Если SaaS — перечисли тарифы
4. Если ритейл — перечисли категории товаров
5. Если ресторан — перечисли направления (доставка, банкеты, кейтеринг)
6. Цены — только если найдены на сайте, иначе null
7. НЕ выдумывай продукты — только из текста сайта"""

    result = _safe_llm_call(prompt, "products", max_tokens=3000)
    return result


# ════════════════════════════════════════════════════════
# Секция 7: HR-анализ
# ════════════════════════════════════════════════════════

def analyze_hr(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
) -> dict:
    """HR-анализ: вакансии, зарплаты, структура.

    Возвращает dict с ключом: hr_data.
    """
    ctx = _prepare_context(scraped, company_info, fns_data)
    bt = company_info.get("business_type_guess", "B2B_SERVICE")

    # Количество сотрудников из ФНС
    employees = None
    if fns_data.get("financials"):
        for f in reversed(fns_data["financials"]):
            if f.get("employees"):
                employees = f["employees"]
                break

    prompt = f"""Проведи HR-анализ компании.

## Компания
{ctx['company_text']}

## Количество сотрудников (ФНС)
{employees if employees else 'Нет данных'}

## Тип бизнеса
{bt}

## Задание

Верни JSON:

{{
  "hr_data": {{
    "employees_count": {employees if employees else 'null'},
    "avg_salary_market": "средняя зарплата в отрасли (gross)",
    "key_positions": [
      {{"title": "Должность", "salary_range": "от X до Y тыс. руб. gross", "demand": "высокий/средний/низкий"}}
    ],
    "hiring_channels": ["HH.ru", "Telegram-каналы", "Рекомендации"],
    "turnover_estimate": "оценка текучести в отрасли",
    "notes": "0 вакансий на HH != 0 найма — возможен найм через агентства, аффилированные юрлица, TG-каналы"
  }}
}}

## Правила
1. key_positions — 4-6 ключевых должностей для типа бизнеса {bt}
2. Зарплаты: gross + KPI 15-20% для позиций с переменной частью
3. 0 вакансий на HH ≠ 0 найма — упомянуть это
4. Если данных нет — оценка по отрасли, пометить как оценочные"""

    result = _safe_llm_call(prompt, "hr", max_tokens=3000)
    return result


# ════════════════════════════════════════════════════════
# Главная точка входа: run()
# ════════════════════════════════════════════════════════

def _run_section(
    name: str,
    index: int,
    total: int,
    fn: Callable[..., dict],
    *args: Any,
    progress_callback: Optional[Callable[[str, str], None]] = None,
    metrics_collector: Any = None,
    **kwargs: Any,
) -> tuple[str, dict, float]:
    """Обёртка для запуска одной секции с тайминг-логом и progress callback.

    Возвращает (name, result_dict, elapsed_seconds).
    При исключении внутри секции — логирует и возвращает пустой dict,
    чтобы остальные секции не упали.

    progress_callback(section_name, status) вызывается со статусами:
      "started", "done", "error"

    metrics_collector: T7 — propagated MetricsCollector for this child thread.
    """
    # T7: Propagate MetricsCollector to this child thread
    if metrics_collector is not None:
        from app.pipeline.llm_client import set_metrics_collector
        set_metrics_collector(metrics_collector)

    def _notify(status: str):
        if progress_callback:
            try:
                progress_callback(name, status)
            except Exception:
                pass  # не ломать пайплайн из-за callback

    logger.info(f"[step5] Секция {index}/{total}: {name} — старт")
    _notify("started")
    t0 = time.monotonic()
    try:
        result = fn(*args, **kwargs)
    except Exception:
        logger.exception(f"[step5] Секция {name} — ОШИБКА")
        result = {}
        elapsed = time.monotonic() - t0
        logger.info(f"[step5] Секция {name} — ошибка за {elapsed:.1f}s")
        _notify("error")
        return name, result, elapsed
    elapsed = time.monotonic() - t0
    logger.info(f"[step5] Секция {name} — завершена за {elapsed:.1f}s")
    _notify("done")
    return name, result, elapsed


def run(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
    competitors: list[dict],
    market_info: dict,
    deep_models: Optional[dict] = None,
    marketplace_data: Optional[dict] = None,
    progress_callback: Optional[Callable[[str, str], None]] = None,
    hh_data: Optional[dict] = None,
) -> dict:
    """Запуск секционного анализа (v3.1 — параллельный).

    Все 7 секций запускаются параллельно через ThreadPoolExecutor
    (max_workers=5, чтобы не перегружать LLM API).
    Каждая секция независима — принимает сырые входные данные и
    возвращает свою часть результата.
    Если одна секция упала — остальные всё равно завершатся.

    Args:
        progress_callback: optional (section_name, status) callback для SSE-событий.
            status: "started" | "done" | "error"

    Собирает результаты в единый dict, совместимый с ReportData.
    """
    total = 8
    logger.info(f"[step5] Запуск секционного анализа v3.2 — {total} секций параллельно (ThreadPoolExecutor, max_workers=5)")
    t_total = time.monotonic()

    # Описание секций: (name, function, args)
    sections: list[tuple[str, Callable[..., dict], tuple]] = [
        ("Анализ рынка", analyze_market, (scraped, company_info, fns_data, market_info)),
        ("Глубокий анализ конкурентов", analyze_competitors_deep, (scraped, company_info, competitors, market_info, deep_models, marketplace_data)),
        ("Анализ компании", analyze_company, (scraped, company_info, fns_data, competitors)),
        ("Стратегический анализ", analyze_strategy, (scraped, company_info, fns_data, competitors)),
        ("Приложения", analyze_appendix, (company_info, fns_data, market_info)),
        ("Фаундеры и мнения", analyze_opinions, (scraped, company_info, fns_data, market_info)),
        ("HR-анализ", analyze_hr, (scraped, company_info, fns_data)),
        ("Продукты и услуги", analyze_products, (scraped, company_info, fns_data)),
    ]

    # T7: Capture parent thread's metrics collector for propagation to child threads
    from app.pipeline.llm_client import _get_metrics_collector
    _parent_mc = _get_metrics_collector()

    # Запуск параллельно
    results_map: dict[str, dict] = {}
    timing_map: dict[str, float] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                _run_section, name, idx, total, fn, *args,
                progress_callback=progress_callback,
                metrics_collector=_parent_mc,
            ): name
            for idx, (name, fn, args) in enumerate(sections, 1)
        }

        for future in concurrent.futures.as_completed(futures):
            section_name = futures[future]
            try:
                name, result, elapsed = future.result()
                results_map[name] = result
                timing_map[name] = elapsed
            except Exception:
                logger.exception(f"[step5] Неожиданная ошибка в future секции '{section_name}'")
                results_map[section_name] = {}
                timing_map[section_name] = 0.0

    # Логирование тайминга
    elapsed_total = time.monotonic() - t_total
    logger.info("[step5] ── Тайминг секций ──")
    for name, fn, _ in sections:
        t = timing_map.get(name, 0.0)
        logger.info(f"[step5]   {name}: {t:.1f}s")
    logger.info(f"[step5] ── ИТОГО (параллельно): {elapsed_total:.1f}s ──")

    # Распаковка результатов по позиции в sections
    market_result = results_map.get("Анализ рынка", {})
    competitors_result = results_map.get("Глубокий анализ конкурентов", {})
    company_result = results_map.get("Анализ компании", {})
    strategy_result = results_map.get("Стратегический анализ", {})
    appendix_result = results_map.get("Приложения", {})
    opinions_result = results_map.get("Фаундеры и мнения", {})
    hr_result = results_map.get("HR-анализ", {})
    products_result = results_map.get("Продукты и услуги", {})

    # ── Сборка результата ──
    logger.info("[step5] Сборка результата из 8 секций...")
    result = _assemble_report(
        market_result=market_result,
        competitors_result=competitors_result,
        company_result=company_result,
        strategy_result=strategy_result,
        appendix_result=appendix_result,
        opinions_result=opinions_result,
        hr_result=hr_result,
        products_result=products_result,
        fns_data=fns_data,
        company_info=company_info,
        scraped=scraped,
        hh_data=hh_data,
    )

    logger.info(f"[step5] Секционный анализ завершён за {elapsed_total:.1f}s")
    return result


def _extract_salary_value(salary_range: str) -> int:
    """Extract numeric salary value from range string like 'от 80 до 120 тыс. руб. gross'."""
    import re
    if not salary_range:
        return 0
    # Find all numbers
    numbers = re.findall(r'[\d]+(?:\s*\d+)*', salary_range.replace(' ', ''))
    if not numbers:
        return 0
    values = [int(n) for n in numbers if n]
    if not values:
        return 0
    # If values look like thousands (< 1000), multiply by 1000
    avg = sum(values) // len(values)
    if avg < 1000:
        avg *= 1000
    return avg


def _transform_hr_data(hr_raw: dict, hh_data: dict | None = None) -> dict:
    """Transform LLM hr_data to template-compatible format.

    Template expects: metrics (list), salaries (list), notes (list), sources (str).
    LLM returns: employees_count, avg_salary_market, key_positions, hiring_channels, etc.
    """
    if not hr_raw and not hh_data:
        return {}

    result: dict[str, Any] = {}

    # ── Metrics cards ──
    metrics = []
    if hr_raw.get("employees_count"):
        metrics.append({"value": str(hr_raw["employees_count"]), "label": "Сотрудников (ФНС)", "color": "gold"})
    if hr_raw.get("avg_salary_market"):
        metrics.append({"value": str(hr_raw["avg_salary_market"]), "label": "Средняя зарплата (рынок)", "color": "blue"})
    if hr_raw.get("turnover_estimate"):
        metrics.append({"value": str(hr_raw["turnover_estimate"]), "label": "Текучесть (оценка)", "color": "red"})

    # HH.ru real data enrichment
    if hh_data:
        vcount = hh_data.get("vacancies_count", hh_data.get("open_vacancies_count", 0))
        if vcount:
            metrics.append({"value": str(vcount), "label": "Вакансий на HH.ru", "color": "green"})
        ind_sal = hh_data.get("industry_salaries", {})
        if ind_sal.get("median_salary_from"):
            metrics.append({
                "value": f'{ind_sal["median_salary_from"]:,} ₽'.replace(",", " "),
                "label": "Медиана зарплат (HH.ru)",
                "color": "blue",
            })

    result["metrics"] = metrics

    # ── Salaries chart ──
    salaries = []

    # Prefer HH.ru real vacancies for chart
    if hh_data and hh_data.get("salaries"):
        salaries = hh_data["salaries"]
    else:
        # Fallback: build from LLM key_positions
        for pos in (hr_raw.get("key_positions") or []):
            if isinstance(pos, dict):
                title = pos.get("title", "")
                value = _extract_salary_value(pos.get("salary_range", ""))
                if value and title:
                    salaries.append({"label": title, "value": value, "color": "#4A8FE0"})
        if salaries:
            salaries = sorted(salaries, key=lambda x: x["value"], reverse=True)

    if salaries:
        result["salaries"] = salaries

    # ── Notes ──
    notes = []
    notes_raw = hr_raw.get("notes", "")
    if isinstance(notes_raw, str) and notes_raw:
        notes.append(notes_raw)
    elif isinstance(notes_raw, list):
        notes.extend(notes_raw)

    channels = hr_raw.get("hiring_channels", [])
    if isinstance(channels, list) and channels:
        notes.append(f"Каналы найма: {', '.join(str(c) for c in channels)}")

    # Key positions detail
    for pos in (hr_raw.get("key_positions") or []):
        if isinstance(pos, dict):
            title = pos.get("title", "")
            salary = pos.get("salary_range", "")
            demand = pos.get("demand", "")
            if title:
                parts = [title]
                if salary:
                    parts.append(salary)
                if demand:
                    parts.append(f"спрос: {demand}")
                notes.append(" — ".join(parts))

    # HH.ru vacancies detail
    if hh_data and hh_data.get("vacancies"):
        vac_titles = [v.get("title", "") for v in hh_data["vacancies"][:5] if v.get("title")]
        if vac_titles:
            notes.append(f"Актуальные вакансии (HH.ru): {', '.join(vac_titles)}")

    result["notes"] = notes

    # ── Sources ──
    sources = []
    if hh_data:
        sources.append("HH.ru API (реальные данные)")
    sources.append("Оценка по данным рынка")
    result["sources"] = " + ".join(sources)

    return result


def _assemble_report(
    *,
    market_result: dict,
    competitors_result: dict,
    company_result: dict,
    strategy_result: dict,
    appendix_result: dict,
    opinions_result: dict,
    hr_result: dict,
    products_result: dict,
    fns_data: dict,
    company_info: dict,
    scraped: dict,
    hh_data: dict | None = None,
) -> dict:
    """Собрать единый dict из результатов всех секций.

    Гарантирует наличие всех обязательных полей для ReportData.
    """
    egrul = fns_data.get("egrul", {})

    # Базовый company из секции 3, с fallback на входные данные
    company = company_result.get("company", {})
    if not company.get("name"):
        company["name"] = company_info.get("name", "Компания")
    if not company.get("business_type"):
        company["business_type"] = company_info.get("business_type_guess", "B2B_SERVICE")
    if not company.get("website"):
        company["website"] = scraped.get("url", "")
    if not company.get("inn"):
        company["inn"] = egrul.get("inn", "")
    if not company.get("legal_name"):
        company["legal_name"] = egrul.get("full_name", "")
    if not company.get("okved"):
        company["okved"] = egrul.get("okved", "")

    # Финансы — всегда из ФНС (ground truth)
    financials = fns_data.get("financials", [])

    # Calc traces — fallback из ФНС если LLM не вернул
    calc_traces = appendix_result.get("calc_traces", [])
    if not calc_traces and financials:
        calc_traces = _generate_basic_calc_traces(financials)

    # Methodology — fallback
    methodology = appendix_result.get("methodology", {})
    if not methodology:
        methodology = {
            "Источники данных": "ФНС, Rusprofile, Яндекс Карты, HH.ru",
            "Период анализа": "Автоматический анализ",
            "Допущения": "Данные из открытых источников",
        }

    result: dict[str, Any] = {
        # Секция 3: Компания
        "company": company,
        "swot": company_result.get("swot"),
        "digital": company_result.get("digital"),
        "market_share": company_result.get("market_share", {}),

        # Секция 1: Рынок
        "market": market_result.get("market"),
        "regulatory_trends": market_result.get("regulatory_trends", []),
        "tech_trends": market_result.get("tech_trends", []),

        # Секция 2: Конкуренты
        "competitors": competitors_result.get("competitors", []),
        "radar_dimensions": competitors_result.get("radar_dimensions", []),

        # Финансы — из ФНС
        "financials": financials,

        # Секция 4: Стратегия
        "recommendations": strategy_result.get("recommendations", []),
        "kpi_benchmarks": strategy_result.get("kpi_benchmarks", []),
        "scenarios": strategy_result.get("scenarios", []),
        "implementation_timeline": strategy_result.get("implementation_timeline", []),

        # Секция 5: Приложения
        "open_questions": appendix_result.get("open_questions", []),
        "glossary": appendix_result.get("glossary", {}),
        "calc_traces": calc_traces,
        "methodology": methodology,

        # Секция 6: Фаундеры и мнения
        "founders": opinions_result.get("founders", []),
        "opinions": opinions_result.get("opinions", []),

        # Секция 7: HR (transform LLM format → template format, enrich with HH.ru)
        "hr_data": _transform_hr_data(hr_result.get("hr_data", {}), hh_data),

        # Секция 8: Продукты
        "products": products_result.get("products", []),

        # Pipeline metadata
        "pipeline_version": "3.2",
    }

    return result


# ── Fallback: calc traces из ФНС (без LLM) ──

def _generate_basic_calc_traces(financials: list[dict]) -> list[dict]:
    """Генерация базовых calc_traces из данных ФНС (fallback без LLM)."""
    traces = []
    latest = financials[-1] if financials else {}
    rev = latest.get("revenue")
    profit = latest.get("net_profit")
    assets = latest.get("assets")
    employees = latest.get("employees")
    year = latest.get("year", "?")

    if rev is not None:
        traces.append({
            "metric_name": f"Выручка ({year})",
            "value": f"{rev:,.0f} тыс. ₽",
            "formula": "Данные бухгалтерской отчётности",
            "inputs": {},
            "sources": ["ФНС"],
            "confidence": "FACT",
        })
    if profit is not None:
        traces.append({
            "metric_name": f"Чистая прибыль ({year})",
            "value": f"{profit:,.0f} тыс. ₽",
            "formula": "Данные бухгалтерской отчётности",
            "inputs": {},
            "sources": ["ФНС"],
            "confidence": "FACT",
        })
    if rev and profit:
        margin = round(profit / rev * 100, 1) if rev != 0 else 0
        traces.append({
            "metric_name": "Рентабельность по чистой прибыли",
            "value": f"{margin}%",
            "formula": "чистая_прибыль / выручка * 100",
            "inputs": {"чистая_прибыль": profit, "выручка": rev},
            "sources": ["ФНС"],
            "confidence": "CALC",
        })
    if rev and employees:
        per_emp = round(rev / employees)
        traces.append({
            "metric_name": "Выручка на сотрудника",
            "value": f"{per_emp:,.0f} тыс. ₽",
            "formula": "выручка / кол-во_сотрудников",
            "inputs": {"выручка": rev, "сотрудники": employees},
            "sources": ["ФНС"],
            "confidence": "CALC",
        })
    if assets and rev:
        turnover = round(rev / assets, 2) if assets != 0 else 0
        traces.append({
            "metric_name": "Оборачиваемость активов",
            "value": f"{turnover}",
            "formula": "выручка / активы",
            "inputs": {"выручка": rev, "активы": assets},
            "sources": ["ФНС"],
            "confidence": "CALC",
        })
    return traces
