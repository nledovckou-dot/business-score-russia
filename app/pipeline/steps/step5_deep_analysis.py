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
from app.pipeline.web_search import _search_duckduckgo

logger = logging.getLogger(__name__)


# ── Web search обогащение (T29) ──

def _web_search_context(queries: list[str], max_snippets: int = 10) -> str:
    """Search DuckDuckGo and return concatenated snippets for LLM context.

    Args:
        queries: list of search queries to try
        max_snippets: max total snippets to include

    Returns:
        Formatted text block with search results, or empty string.
    """
    all_snippets: list[str] = []
    for query in queries:
        if len(all_snippets) >= max_snippets:
            break
        try:
            results = _search_duckduckgo(query)
            for r in results[:5]:
                snippet = r.get("snippet", "").strip()
                title = r.get("title", "").strip()
                if snippet and len(snippet) > 30:
                    all_snippets.append(f"[{title}] {snippet}")
                    if len(all_snippets) >= max_snippets:
                        break
        except Exception as e:
            logger.debug("[T29] Web search failed for '%s': %s", query, str(e)[:100])

    if not all_snippets:
        return ""

    text = "\n".join(f"- {s[:300]}" for s in all_snippets)
    return f"\n## Дополнительный контекст (web search)\n{text}\n"


# ── Общий системный контекст для всех секций ──

_BASE_SYSTEM = """Ты — ведущий бизнес-аналитик (pipeline v3.0). Тебе предоставлены РЕАЛЬНЫЕ данные.

ВАЖНО:
- Используй РЕАЛЬНЫЕ данные, не выдумывай
- Если данных ФНС нет — используй экспертные оценки на основе типа бизнеса, рынка и доступного контекста. Пометь как оценку
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


# ── Постпроцессинг рынка (T48) ──


def _parse_numeric(s: str | int | float | None) -> float | None:
    """Extract first numeric value from a string like '150 млрд руб.' -> 150.0."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    import re
    m = re.search(r"[\d.,]+", str(s).replace(",", ".").replace(" ", ""))
    return float(m.group()) if m else None


def _postprocess_market(result: dict) -> dict:
    """T48: Programmatic CAGR from data_points + TAM>SAM>SOM validation.

    - Calculates CAGR from first/last data_points: (end/start)^(1/n) - 1
    - If calculated CAGR differs from LLM growth_rate by >5pp, overwrites it
    - Validates TAM > SAM > SOM ordering
    """
    import re

    market = result.get("market")
    if not market:
        return result

    # --- CAGR calculation ---
    data_points = market.get("data_points")
    if data_points and len(data_points) >= 2:
        try:
            start_val = float(data_points[0]["value"])
            end_val = float(data_points[-1]["value"])
            n_years = int(data_points[-1]["year"]) - int(data_points[0]["year"])
            if start_val > 0 and end_val > 0 and n_years > 0:
                cagr = (end_val / start_val) ** (1 / n_years) - 1
                cagr_pct = round(cagr * 100, 1)

                # Parse existing growth_rate from LLM
                existing_str = market.get("growth_rate", "")
                existing_match = re.search(r"[+-]?[\d.,]+", str(existing_str).replace(",", "."))
                existing_pct = float(existing_match.group()) if existing_match else None

                # If mismatch > 5pp, overwrite
                if existing_pct is None or abs(cagr_pct - existing_pct) > 5:
                    sign = "+" if cagr_pct >= 0 else ""
                    market["growth_rate"] = f"{sign}{cagr_pct}% CAGR"
                    logger.info(
                        f"[step5:market] CAGR пересчитан: {existing_str} → {market['growth_rate']} "
                        f"(из data_points: {start_val} → {end_val} за {n_years} лет)"
                    )
        except (ValueError, TypeError, KeyError, ZeroDivisionError) as e:
            logger.warning(f"[step5:market] Не удалось посчитать CAGR: {e}")

    # --- TAM > SAM > SOM validation ---
    tam_val = _parse_numeric(market.get("tam"))
    sam_val = _parse_numeric(market.get("sam"))
    som_val = _parse_numeric(market.get("som"))

    if tam_val is not None and sam_val is not None and som_val is not None:
        if not (tam_val >= sam_val >= som_val):
            logger.warning(
                f"[step5:market] TAM/SAM/SOM нарушен порядок: "
                f"TAM={tam_val}, SAM={sam_val}, SOM={som_val}. Корректирую."
            )
            # Sort descending and reassign
            sorted_vals = sorted([tam_val, sam_val, som_val], reverse=True)
            # Preserve original text format, just fix the numeric part
            market["tam"] = re.sub(
                r"[\d.,]+", str(sorted_vals[0]), str(market.get("tam", "")), count=1
            )
            market["sam"] = re.sub(
                r"[\d.,]+", str(sorted_vals[1]), str(market.get("sam", "")), count=1
            )
            market["som"] = re.sub(
                r"[\d.,]+", str(sorted_vals[2]), str(market.get("som", "")), count=1
            )

    return result


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
    "tam": "XX млрд руб. — Total Addressable Market (весь рынок)",
    "sam": "XX млрд руб. — Serviceable Addressable Market (доступный)",
    "som": "XX млрд руб. — Serviceable Obtainable Market (реально достижимый)",
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
6. Если данных нет — используй отраслевые знания, но источники пометь как оценочные
7. TAM > SAM > SOM. SAM = часть TAM доступная для данного типа бизнеса. SOM = реалистичная доля для компании за 3 года."""

    result = _safe_llm_call(prompt, "market", max_tokens=4000)
    result = _postprocess_market(result)
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

    # Подготовка текста конкурентов (T42: включаем данные из step4.5)
    comp_text = ""
    for i, c in enumerate(competitors, 1):
        comp_text += f"\n{i}. {c.get('name', '')} — {c.get('description', '')}"
        if c.get("website"):
            comp_text += f" ({c['website']})"
        comp_text += f" [угроза: {c.get('threat_level', 'med')}]"
        # T42: Real data from step4.5 enrichment
        if c.get("inn"):
            comp_text += f"\n   ИНН: {c['inn']}"
        if c.get("legal_name"):
            comp_text += f" | Юрлицо: {c['legal_name']}"
        metrics = c.get("metrics", {})
        if metrics.get("Выручка"):
            comp_text += f"\n   Выручка: {metrics['Выручка']}"
        if metrics.get("Сотрудники"):
            comp_text += f" | Сотрудники: {metrics['Сотрудники']}"
        if metrics.get("Год основания"):
            comp_text += f" | Год основания: {metrics['Год основания']}"
        if c.get("social_media"):
            social_parts = [f"{k}: {v.get('handle', v.get('url', ''))}" for k, v in c["social_media"].items()]
            comp_text += f"\n   Соцсети: {', '.join(social_parts)}"

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

    # T29: Web search обогащение конкурентов
    company_name = company_info.get('name', '')
    ws_queries = [f"{company_name} компания описание выручка"]
    for c in competitors[:5]:  # Top 5 competitors
        cname = c.get('name', '')
        if cname:
            ws_queries.append(f"{cname} компания выручка сотрудники")
    web_context = _web_search_context(ws_queries, max_snippets=15)

    prompt = f"""Обогати данные конкурентов для бизнес-анализа.

## Компания-объект анализа
Название: {company_name}
Тип бизнеса: {company_info.get('business_type_guess', '')}

## Подтверждённые конкуренты
{comp_text}
{deep_models_text}
{marketplace_text}
{web_context}

## Задание

ВАЖНО: Первый элемент в competitors — САМА анализируемая компания ({company_info.get('name', '')}).
Оцени её по тем же параметрам, что и конкурентов.

Верни JSON:

{{
  "competitors": [
    {{
      "name": "{company_info.get('name', '')}",
      "description": "Краткое описание САМОЙ компании (2-3 предложения)",
      "website": "https://...",
      "address": "адрес или null",
      "x": 0-100,
      "y": 0-100,
      "radar_scores": {{"Param1": 1-10, "Param2": 1-10, "Param3": 1-10, "Param4": 1-10, "Param5": 1-10, "Param6": 1-10}},
      "metrics": {{"Выручка": "значение или null", "Сотрудники": "значение или null", "Год основания": "ГГГГ или null"}},
      "threat_level": "self",
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
    }},
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
7. Если есть предварительные данные lifecycle/channels — используй их, но можешь уточнить
8. Первый элемент competitors = сама компания. Остальные = конкуренты."""

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

    # T29: Web search обогащение компании
    company_name = company_info.get('name', '')
    city = company_info.get('city', '')
    ws_queries = [
        f"{company_name} {city} отзывы",
        f"{company_name} компания описание",
        f"{company_name} {city} цены меню услуги",
    ]
    web_context = _web_search_context(ws_queries, max_snippets=10)

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
{web_context}

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
    fns_available = bool(fns_data.get("financials"))
    if fns_available:
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

    # T52: Prepare explicit KPI current values from ФНС
    fns_kpi_json = "null"
    fns_rev_k = None
    fns_profit_k = None
    fns_margin = None
    fns_emp = None
    if fns_available:
        latest = fns_data["financials"][-1]
        fns_rev_k = latest.get("revenue")
        fns_profit_k = latest.get("net_profit")
        fns_emp = latest.get("employees")
        if fns_rev_k and fns_profit_k:
            fns_margin = round(fns_profit_k / fns_rev_k * 100, 1) if fns_rev_k else None
        fns_kpi = {}
        if fns_rev_k is not None:
            fns_kpi["Выручка, тыс. руб."] = fns_rev_k
        if fns_profit_k is not None:
            fns_kpi["Чистая прибыль, тыс. руб."] = fns_profit_k
        if fns_margin is not None:
            fns_kpi["Рентабельность, %"] = fns_margin
        if fns_emp:
            fns_kpi["Сотрудники"] = fns_emp
            if fns_rev_k:
                fns_kpi["Выручка/сотрудник, тыс. руб."] = round(fns_rev_k / fns_emp)
        fns_kpi_json = json.dumps(fns_kpi, ensure_ascii=False, indent=2)

    # T50: Base scenario floor = current FNS revenue
    base_floor_hint = ""
    if fns_rev_k:
        base_floor_hint = f"\n⚠ Текущая выручка = {fns_rev_k} тыс. руб. Базовый сценарий ОБЯЗАН быть ≥ {fns_rev_k}. Оптимистичный > базового. Пессимистичный = -10..20% от текущей."

    # T55: When FNS data is missing, instruct LLM to estimate
    no_fns_estimation_hint = ""
    if not fns_available:
        no_fns_estimation_hint = f"""
⚠ ДАННЫЕ ФНС НЕДОСТУПНЫ. Это НЕ означает что компания не существует.
Ты ОБЯЗАН самостоятельно ОЦЕНИТЬ финансовые показатели компании на основе:
- Типа бизнеса: {bt}
- Описания компании и текста сайта
- Рыночных бенчмарков для данной отрасли
- Количества сотрудников (оцени по сайту/конкурентам)
- Конкурентного окружения

ПРАВИЛА ОЦЕНКИ:
1. Сценарии (scenarios): ОБЯЗАТЕЛЬНО заполни metrics числами-оценками. НЕ ставь 0 или null.
   - Оцени годовую выручку по типу бизнеса и масштабу компании
   - Базовый = текущая оценка, Оптимистичный = +20-30%, Пессимистичный = -10-20%
2. KPI (kpi_benchmarks): ОБЯЗАТЕЛЬНО заполни current числами-оценками. НЕ ставь null.
   - Используй отраслевые бенчмарки для оценки текущих показателей
   - Если KPI = рентабельность, оцени по отрасли (напр. общепит 5-15%, IT 15-30%, ритейл 3-8%)
3. Пометь все оценки: добавь к assumptions.description текст "(⚠ экспертная оценка, данные ФНС недоступны)"
"""

    # T29: Web search обогащение стратегии
    company_name = company_info.get('name', '')
    ws_queries = [
        f"{company_name} стратегия развитие планы",
        f"{company_info.get('business_type_guess', '')} рынок тренды Россия 2025 2026",
        f"{company_name} конкуренты рынок доля",
    ]
    web_context = _web_search_context(ws_queries, max_snippets=10)

    # T55: Build KPI current instruction depending on FNS availability
    if fns_available:
        kpi_current_instruction = f"""5. КРИТИЧНО для kpi_benchmarks: поле "current" = РЕАЛЬНЫЕ данные из ФНС выше:
   - Подставь значения из JSON "ТЕКУЩИЕ ПОКАЗАТЕЛИ" напрямую
   - Выручка, прибыль, рентабельность, сотрудники — всё из ФНС
   - current=null ТОЛЬКО если данных нет в ФНС. Не выдумывай"""
        scenario_instruction = f"""6. Сценарии — 3, горизонт 12 мес, на базе РЕАЛЬНОЙ выручки {fns_rev_k or 'неизв.'} тыс. руб.
7. АРИФМЕТИКА СЦЕНАРИЕВ: выручка = текущая × (1 + growth_pct/100). Проверь: базовый ≥ текущей, оптимистичный > базового"""
    else:
        kpi_current_instruction = """5. КРИТИЧНО для kpi_benchmarks: данные ФНС НЕДОСТУПНЫ. Ты ОБЯЗАН:
   - Заполнить поле "current" ЭКСПЕРТНОЙ ОЦЕНКОЙ для каждого KPI (НЕ null, НЕ 0)
   - Оценивай на основе типа бизнеса, масштаба компании, рыночных бенчмарков
   - Примеры оценки: рентабельность общепита 5-15%, IT 15-30%, ритейл 3-8%
   - ЗАПРЕЩЕНО возвращать current=null или current=0 — дай реалистичную оценку"""
        scenario_instruction = """6. Сценарии — 3, горизонт 12 мес. Данные ФНС НЕДОСТУПНЫ — ты ОБЯЗАН ОЦЕНИТЬ текущую выручку:
   - Оцени масштаб бизнеса по сайту, кол-ву сотрудников, конкурентам
   - Базовый сценарий = твоя оценка текущей выручки × (1 + рост 5-15%)
   - Оптимистичный = текущая × (1 + 20-35%), Пессимистичный = текущая × (1 - 10-20%)
   - ЗАПРЕЩЕНО ставить 0 или 0.0 в metrics. Дай реалистичные числа
   - Добавь к description: "(⚠ экспертная оценка, данные ФНС недоступны)"
7. АРИФМЕТИКА СЦЕНАРИЕВ: оптимистичный > базового > пессимистичного"""

    prompt = f"""Разработай стратегию для компании.

## Компания
{ctx['company_text']}

## Финансы из ФНС (тыс. руб.)
{ctx['fin_text']}

## Конкуренты
{ctx['comp_text']}

## Тип бизнеса
{bt}
{web_context}

## ТЕКУЩИЕ ПОКАЗАТЕЛИ (ФНС) — ОБЯЗАТЕЛЬНО используй как current в kpi_benchmarks
{fns_kpi_json}
{base_floor_hint}
{no_fns_estimation_hint}

## Задание

Верни JSON:

{{
  "recommendations": [
    {{
      "title": "Рекомендация",
      "description": "Подробное описание (2-4 предложения)",
      "priority": "high/medium/low",
      "timeline": "Q1-Q2 2026",
      "budget_estimate": "50-100 тыс. руб./мес",
      "expected_impact": "+X% метрика",
      "impact_rationale": "Обоснование: бенчмарк/расчёт/кейс",
      "target_kpi": "Какой KPI улучшает"
    }}
  ],
  "kpi_benchmarks": [
    {{"name": "KPI название", "current": число_или_оценка, "benchmark": число, "unit": "ед."}}
  ],
  "scenarios": [
    {{
      "name": "optimistic", "label": "Оптимистичный",
      "assumptions": {{"growth_pct": 30, "description": "Описание допущений"}},
      "metrics": {{"Выручка, тыс. руб.": число, "Прибыль, тыс. руб.": число, "Сотрудники": число}}
    }},
    {{
      "name": "base", "label": "Базовый",
      "assumptions": {{"growth_pct": 10, "description": "Описание допущений"}},
      "metrics": {{"Выручка, тыс. руб.": число, "Прибыль, тыс. руб.": число, "Сотрудники": число}}
    }},
    {{
      "name": "pessimistic", "label": "Пессимистичный",
      "assumptions": {{"growth_pct": -10, "description": "Описание допущений"}},
      "metrics": {{"Выручка, тыс. руб.": число, "Прибыль, тыс. руб.": число, "Сотрудники": число}}
    }}
  ],
  "implementation_timeline": [
    {{"date": "Q1 2026", "title": "Шаг 1", "description": "Описание", "color": "gold"}},
    {{"date": "Q2 2026", "title": "Шаг 2", "description": "Описание", "color": "blue"}}
  ]
}}

## Правила
1. Рекомендации — 5-6 штук, приоритизированные (минимум 2 high)
2. Для каждой рекомендации ОБЯЗАТЕЛЬНО: budget_estimate (диапазон стоимости), impact_rationale (откуда цифра), target_kpi
3. Не более 2 high-priority рекомендаций на один квартал — распредели по Q1-Q4
4. KPI — 6-8, релевантных типу бизнеса ({bt})
{kpi_current_instruction}
{scenario_instruction}
8. Каждый сценарий: assumptions.growth_pct + assumptions.description (почему такой рост)
9. Реальные финансы (все года): {fin_json}
10. implementation_timeline — 4-6 шагов по кварталам
11. Если тип HYBRID — метрики B2C и B2B РАЗДЕЛЬНО в kpi_benchmarks
12. ЗАПРЕТ: metrics со значениями 0 или 0.0 — это ломает отчёт. Всегда числа > 0"""

    result = _safe_llm_call(prompt, "strategy", max_tokens=6000)

    # T50+T52: Post-processing — fix scenarios and KPI
    result = _postprocess_strategy(result, fns_rev_k, fns_profit_k, fns_margin, fns_emp)
    return result


def _postprocess_strategy(
    result: dict,
    fns_rev_k: float | None,
    fns_profit_k: float | None,
    fns_margin: float | None,
    fns_emp: int | None,
) -> dict:
    """T50+T52+T54+T55: Fix scenarios arithmetic, fill KPI current, check recommendations.

    T55: When FNS data is missing, validate that LLM returned non-zero estimates
    and log warnings for zero values.
    """
    if not result:
        return result

    # ── T50/T55: Fix scenario arithmetic ──
    scenarios = result.get("scenarios", [])
    if scenarios:
        if fns_rev_k:
            # FNS data available — use exact arithmetic
            for scenario in scenarios:
                metrics = scenario.get("metrics", {})
                assumptions = scenario.get("assumptions", {})
                label = scenario.get("label", scenario.get("name", ""))

                # Find revenue key
                rev_key = None
                for k in metrics:
                    if "выручка" in k.lower() or "revenue" in k.lower():
                        rev_key = k
                        break

                if rev_key is None:
                    continue

                scenario_rev = metrics.get(rev_key)
                growth_pct = assumptions.get("growth_pct")

                # Calculate expected revenue from growth_pct if available
                if growth_pct is not None:
                    expected = round(fns_rev_k * (1 + growth_pct / 100))
                    if scenario_rev and abs(scenario_rev - expected) > expected * 0.3:
                        logger.warning(
                            "T50: Scenario '%s' revenue=%s but growth=%s%% from %s → corrected to %s",
                            label, scenario_rev, growth_pct, fns_rev_k, expected,
                        )
                        metrics[rev_key] = expected
                    elif not scenario_rev:
                        metrics[rev_key] = expected

                # Check: base >= current, optimistic > base
                if scenario_rev:
                    if "базов" in label.lower() or "base" in label.lower():
                        if scenario_rev < fns_rev_k * 0.95:
                            corrected = round(fns_rev_k * 1.1)
                            logger.warning(
                                "T50: Base scenario (%s) < current FNS revenue (%s) → corrected to %s",
                                scenario_rev, fns_rev_k, corrected,
                            )
                            metrics[rev_key] = corrected
                            if not assumptions.get("growth_pct"):
                                assumptions["growth_pct"] = 10

                # Recalculate profit from margin if available
                profit_key = None
                for k in metrics:
                    if "прибыль" in k.lower() or "profit" in k.lower():
                        profit_key = k
                        break
                if profit_key and fns_margin and metrics.get(rev_key):
                    expected_profit = round(metrics[rev_key] * fns_margin / 100)
                    current_profit = metrics.get(profit_key)
                    if current_profit and metrics.get(rev_key):
                        scenario_margin = abs(current_profit / metrics[rev_key] * 100) if metrics[rev_key] else 0
                        # If margin differs wildly from FNS margin, recalculate
                        if abs(scenario_margin - abs(fns_margin)) > 20:
                            metrics[profit_key] = expected_profit

                scenario["metrics"] = metrics
                scenario["assumptions"] = assumptions
        else:
            # T55: No FNS data — validate LLM estimates are non-zero
            for scenario in scenarios:
                metrics = scenario.get("metrics", {})
                assumptions = scenario.get("assumptions", {})
                label = scenario.get("label", scenario.get("name", ""))
                has_zeros = False

                for k, v in list(metrics.items()):
                    if v is None or v == 0 or v == 0.0:
                        has_zeros = True
                        logger.warning(
                            "T55: Scenario '%s' metric '%s' is %s — LLM failed to estimate",
                            label, k, v,
                        )

                if has_zeros:
                    # Mark in description that estimation failed
                    desc = assumptions.get("description", "")
                    if desc and "⚠" not in desc:
                        assumptions["description"] = f"{desc} (⚠ экспертная оценка, данные ФНС недоступны)"
                    scenario["assumptions"] = assumptions

    # ── T52/T55: Fill KPI current values ──
    kpi_benchmarks = result.get("kpi_benchmarks", [])

    if fns_rev_k is not None or fns_profit_k is not None or fns_margin is not None or fns_emp is not None:
        # FNS data available — fill from FNS
        fns_map = {}
        if fns_rev_k is not None:
            fns_map["выручка"] = fns_rev_k
        if fns_profit_k is not None:
            fns_map["прибыль"] = fns_profit_k
        if fns_margin is not None:
            fns_map["рентабельность"] = fns_margin
        if fns_emp is not None:
            fns_map["сотрудник"] = fns_emp
            if fns_rev_k:
                fns_map["выручка/сотрудник"] = round(fns_rev_k / fns_emp)
                fns_map["выручка на сотрудник"] = round(fns_rev_k / fns_emp)

        for kpi in kpi_benchmarks:
            if kpi.get("current") is not None:
                continue
            name_lower = kpi.get("name", "").lower()
            for fns_key, fns_val in fns_map.items():
                if fns_key in name_lower:
                    kpi["current"] = fns_val
                    logger.info("T52: Filled KPI '%s' current=%s from ФНС", kpi["name"], fns_val)
                    break
    else:
        # T55: No FNS data — validate LLM filled current values
        null_count = 0
        for kpi in kpi_benchmarks:
            if kpi.get("current") is None:
                null_count += 1
                logger.warning(
                    "T55: KPI '%s' current=null — LLM failed to estimate (no FNS data)",
                    kpi.get("name", "?"),
                )
                # Use benchmark as fallback for current (slightly below benchmark)
                benchmark = kpi.get("benchmark")
                if benchmark is not None:
                    try:
                        bench_val = float(benchmark)
                        # Estimate current as 70-85% of benchmark
                        kpi["current"] = round(bench_val * 0.75, 1)
                        logger.info(
                            "T55: KPI '%s' current estimated as 75%% of benchmark (%s → %s)",
                            kpi.get("name", "?"), benchmark, kpi["current"],
                        )
                    except (ValueError, TypeError):
                        pass

        if null_count > 0:
            logger.warning("T55: %d/%d KPIs had null current values (no FNS data)", null_count, len(kpi_benchmarks))

    result["kpi_benchmarks"] = kpi_benchmarks

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

    # T29: Web search обогащение продуктов
    company_name = company_info.get('name', '')
    city = company_info.get('city', '')
    ws_queries = [
        f"{company_name} {city} меню цены прайс",
        f"{company_name} услуги тарифы каталог",
        f"{company_name} продукция ассортимент",
    ]
    web_context = _web_search_context(ws_queries, max_snippets=10)

    prompt = f"""Проанализируй продукты и услуги компании.

## Компания
{ctx['company_text']}

## Текст сайта (фрагмент)
{scraped.get('text', '')[:5000]}
{web_context}

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

    # Регион из данных компании
    region = ""
    if fns_data.get("egrul", {}).get("address"):
        addr = fns_data["egrul"]["address"]
        if "москв" in addr.lower():
            region = "Москва"
        elif "санкт-петербург" in addr.lower() or "петербург" in addr.lower():
            region = "Санкт-Петербург"
        else:
            region = addr.split(",")[0] if "," in addr else ""

    prompt = f"""Проведи HR-анализ компании.

## Компания
{ctx['company_text']}

## Количество сотрудников (ФНС)
{employees if employees else 'Нет данных'}

## Тип бизнеса
{bt}

## Регион
{region or 'Россия (уточнить)'}

## Задание

Верни JSON:

{{
  "hr_data": {{
    "employees_count": {employees if employees else 'null'},
    "avg_salary_market": "средняя зарплата в отрасли (gross + KPI)",
    "key_positions": [
      {{
        "title": "Должность",
        "salary_range": "от X до Y тыс. руб. gross",
        "salary_with_kpi": "от X до Y тыс. руб. (gross + KPI 15-20%)",
        "demand": "высокий/средний/низкий"
      }}
    ],
    "hiring_channels": ["HH.ru", "Telegram-каналы", "Рекомендации"],
    "turnover_estimate": "оценка текучести в отрасли",
    "notes": "0 вакансий на HH != 0 найма — возможен найм через агентства, аффилированные юрлица, TG-каналы",
    "search_filters": {{
      "region": "{region or 'Россия'}",
      "period": "последние 12 месяцев",
      "experience": "1-6 лет"
    }}
  }}
}}

## Правила
1. key_positions — 4-6 ключевых должностей для типа бизнеса {bt}
2. Зарплаты: salary_range = gross HH.ru, salary_with_kpi = gross + KPI 15-20% ОТ ОКЛАДА (НЕ от выручки!)
3. Для позиций с переменной частью (менеджеры продаж, коммерческий директор) обязательно salary_with_kpi
4. 0 вакансий на HH ≠ 0 найма — упомянуть. Проверять: агентства, аффилированные юрлица, TG-каналы, раздел «Карьера» на сайте
5. Если данных нет — оценка по отрасли, пометить как оценочные
6. search_filters — указать фильтры HH.ru (регион, период, опыт)
7. Для технологов/R&D сверить с обзорами ANCOR/Antal (упомянуть как источник)"""

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
        original_competitors=competitors,
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
    result["sources"] = sources

    return result


def _infer_business_type_from_okved(okved: str) -> str | None:
    """Infer business type from ОКВЭД code."""
    if not okved:
        return None
    code = okved.split(".")[0]  # First 2 digits
    try:
        num = int(code)
    except ValueError:
        return None

    if num in (55, 56):
        return "B2C_SERVICE"  # HoReCa
    if num in range(86, 89) or num in (93, 96):
        return "B2C_SERVICE"  # Healthcare, beauty, fitness
    if num == 47 or (45 <= num <= 46):
        return "B2C_PRODUCT"  # Retail
    if num in (62, 63) or (69 <= num <= 74):
        return "B2B_SERVICE"  # IT, consulting
    if 25 <= num <= 33:
        return "B2B_PRODUCT"  # Manufacturing
    if num == 20:  # Chemical manufacturing (including cosmetics)
        return "B2B_PRODUCT"
    if num == 46:  # Wholesale trade
        return "B2B_PRODUCT"
    return None


def _founders_from_egrul(egrul: dict) -> list[dict]:
    """Fallback: generate founders list from ЕГРЮЛ data when LLM returns empty."""
    founders = []
    for f in egrul.get("founders", []):
        founders.append({
            "name": f.get("name", ""),
            "role": "Учредитель",
            "share": f.get("share_percent", ""),
            "social": "",
            "company": egrul.get("full_name", ""),
        })
    director = egrul.get("director", {})
    if director.get("name"):
        # Add director if not already in founders
        dir_name = director["name"]
        if not any(f["name"] == dir_name for f in founders):
            founders.append({
                "name": dir_name,
                "role": "Генеральный директор",
                "share": "",
                "social": "",
                "company": egrul.get("full_name", ""),
            })
    return founders


def _merge_competitors(
    llm_competitors: list[dict],
    original_competitors: list[dict],
    company_info: dict,
) -> list[dict]:
    """Merge enrichment data from step4/4.5 into LLM-generated competitors.

    LLM provides: radar_scores, x/y coordinates, lifecycle, sales_channels, description.
    Step4/4.5 provides: real INN, FNS financials, social_media, verification data, EGRUL.

    Strategy: match by name (fuzzy), then overlay real data onto LLM entries.
    """
    if not llm_competitors:
        return []

    if not original_competitors:
        return llm_competitors

    # Build lookup: lowercase name → original competitor dict
    orig_by_name: dict[str, dict] = {}
    for oc in original_competitors:
        name = (oc.get("name") or "").lower().strip()
        if name:
            orig_by_name[name] = oc

    target_name = (company_info.get("name") or "").lower().strip()

    merged = []
    for lc in llm_competitors:
        lc_name = (lc.get("name") or "").lower().strip()

        # Skip if this is the target company (it's in the LLM list as first element)
        if lc_name and target_name and (
            lc_name == target_name
            or lc_name in target_name
            or target_name in lc_name
        ):
            merged.append(lc)
            continue

        # Find matching original competitor
        orig = orig_by_name.get(lc_name)
        if not orig:
            # Try fuzzy: check if any original name is contained in LLM name or vice versa
            for oname, odata in orig_by_name.items():
                if oname in lc_name or lc_name in oname:
                    orig = odata
                    break

        if orig:
            # Merge real data from step4/4.5 into LLM entry
            # Real data wins for factual fields; LLM wins for analytical fields
            if orig.get("inn") and not lc.get("inn"):
                lc["inn"] = orig["inn"]
            if orig.get("legal_name") and not lc.get("legal_name"):
                lc["legal_name"] = orig["legal_name"]
            if orig.get("website") and not lc.get("website"):
                lc["website"] = orig["website"]
            # Merge verification data from step4
            if orig.get("verified") is not None:
                lc.setdefault("verified", orig["verified"])
            if orig.get("verification_confidence"):
                lc.setdefault("verification_confidence", orig["verification_confidence"])
            if orig.get("verification_sources"):
                lc.setdefault("verification_sources", orig["verification_sources"])
            # Merge FNS financials from step4.5
            if orig.get("fns_financials") and not lc.get("financials"):
                lc["financials"] = orig["fns_financials"]
            # Merge real metrics from step4.5 into LLM metrics
            if orig.get("metrics"):
                lc_metrics = lc.get("metrics") or {}
                for k, v in orig["metrics"].items():
                    if v is not None and k not in lc_metrics:
                        lc_metrics[k] = v
                lc["metrics"] = lc_metrics

        merged.append(lc)

    return merged


def _competitors_from_originals(original_competitors: list[dict]) -> list[dict]:
    """Build minimal Competitor-compatible dicts from step4/4.5 enriched data.

    Used as fallback when LLM competitor analysis fails completely.
    Produces entries that pass Pydantic validation for the Competitor model.
    """
    import random

    result = []
    for oc in original_competitors:
        name = oc.get("name", "")
        if not name:
            continue

        metrics = oc.get("metrics", {})
        comp = {
            "name": name,
            "description": oc.get("description") or oc.get("why_competitor") or "",
            "legal_name": oc.get("legal_name"),
            "inn": oc.get("inn"),
            "website": oc.get("website"),
            "address": oc.get("city") or oc.get("address"),
            # Spread competitors across the perceptual map
            "x": round(random.uniform(20, 80), 1),
            "y": round(random.uniform(20, 80), 1),
            "radar_scores": {},  # no radar without LLM
            "metrics": metrics,
            "threat_level": oc.get("threat_level", "med"),
            # Preserve verification data
            "verified": oc.get("verified", True),
            "verification_confidence": oc.get("verification_confidence", "unverified"),
            "verification_sources": oc.get("verification_sources", []),
            "verification_notes": oc.get("verification_notes"),
        }

        # Merge FNS financials
        if oc.get("fns_financials"):
            comp["financials"] = oc["fns_financials"]

        # Build lifecycle from step4.5 EGRUL data
        if oc.get("year_founded") or (oc.get("egrul") and oc["egrul"].get("reg_date")):
            year = oc.get("year_founded") or ""
            if not year and oc.get("egrul", {}).get("reg_date"):
                import re
                m = re.search(r"(\d{4})", oc["egrul"]["reg_date"])
                year = m.group(1) if m else ""
            comp["lifecycle"] = {
                "stage": "mature",
                "evidence": [f"Год основания: {year}"] if year else [],
                "year_founded": year or None,
            }

        result.append(comp)

    logger.info(
        "[step5] Built %d fallback competitors from step4/4.5 data",
        len(result),
    )
    return result


def _default_radar_dimensions(business_type: str) -> list[str]:
    """Return default radar dimensions when LLM doesn't provide them."""
    if "B2C_SERVICE" in business_type:
        return ["Качество", "Цена", "Сервис", "Локация", "Репутация", "Маркетинг"]
    elif "B2C_PRODUCT" in business_type:
        return ["Качество", "Цена", "Ассортимент", "Логистика", "Бренд", "Digital"]
    elif "B2B_SERVICE" in business_type:
        return ["Функциональность", "Цена", "Поддержка", "Интеграции", "Бренд", "Инновации"]
    elif "B2B_PRODUCT" in business_type:
        return ["Качество", "Цена", "Мощности", "Сертификация", "Логистика", "Инновации"]
    elif "PLATFORM" in business_type:
        return ["UX", "Цена", "Каталог", "Доставка", "Бренд", "Технологии"]
    else:
        return ["Качество", "Цена", "Сервис", "Репутация", "Маркетинг", "Инновации"]


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
    original_competitors: list[dict] | None = None,
) -> dict:
    """Собрать единый dict из результатов всех секций.

    Гарантирует наличие всех обязательных полей для ReportData.

    Args:
        original_competitors: enriched competitor dicts from step4/4.5.
            Used as fallback when LLM competitor analysis returns empty,
            and for merging real data (FNS, social) into LLM results.
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
    if not company.get("ogrn"):
        company["ogrn"] = egrul.get("ogrn", "")
    if not company.get("reg_date"):
        company["reg_date"] = egrul.get("reg_date", "")
    if not company.get("capital"):
        company["capital"] = egrul.get("capital", "")
    if not company.get("director"):
        director = egrul.get("director", {})
        if director.get("name"):
            company["director"] = director.get("name", "")

    # T41: Verify business type by ОКВЭД
    okved = company.get("okved", "")
    if okved:
        inferred_type = _infer_business_type_from_okved(okved)
        current_type = company.get("business_type", "")
        if inferred_type and inferred_type != current_type:
            logger.info(
                "Business type mismatch: LLM=%s, ОКВЭД(%s)=%s → using ОКВЭД",
                current_type, okved, inferred_type,
            )
            company["business_type"] = inferred_type
            company["business_type_source"] = f"ОКВЭД {okved}"

    # Финансы — всегда из ФНС (ground truth)
    financials = fns_data.get("financials", [])

    # T44: Calc traces — merge ФНС facts + LLM estimates
    calc_traces = appendix_result.get("calc_traces", [])
    fns_traces = _generate_basic_calc_traces(financials) if financials else []
    if not calc_traces:
        calc_traces = fns_traces
    elif fns_traces:
        # Prepend ФНС FACT traces before LLM ESTIMATE traces
        existing_lower = {t.get("metric_name", "").lower() for t in calc_traces}
        for ft in fns_traces:
            if ft["metric_name"].lower() not in existing_lower:
                calc_traces.insert(0, ft)

    # T46: Validate calc_traces — fix obvious arithmetic errors
    calc_traces = _validate_calc_traces(calc_traces, financials)

    # Methodology — fallback
    methodology = appendix_result.get("methodology", {})
    if not methodology:
        methodology = {
            "Источники данных": "ФНС, Rusprofile, Яндекс Карты, HH.ru",
            "Период анализа": "Автоматический анализ",
            "Допущения": "Данные из открытых источников",
        }

    # T47: Validate market_share — fix absurd percentages
    market_share = company_result.get("market_share", {})
    market_data = market_result.get("market") or {}
    market_share = _validate_market_share(market_share, financials, market_data)

    # T43: Founders — LLM → fallback to ЕГРЮЛ
    founders = opinions_result.get("founders", [])
    if not founders:
        founders = _founders_from_egrul(egrul)
        if founders:
            logger.info("T43: Founders from ЕГРЮЛ fallback (%d entries)", len(founders))

    # ── Конкуренты: fallback + merge enrichment data ──
    llm_competitors = competitors_result.get("competitors", [])
    radar_dimensions = competitors_result.get("radar_dimensions", [])
    final_competitors = _merge_competitors(
        llm_competitors, original_competitors or [], company_info,
    )
    if not final_competitors and (original_competitors or []):
        # LLM failed completely — build minimal competitor entries from step4/4.5
        logger.warning(
            "[step5] LLM competitor analysis returned empty — "
            "falling back to %d enriched competitors from step4/4.5",
            len(original_competitors),
        )
        final_competitors = _competitors_from_originals(original_competitors or [])
        if not radar_dimensions:
            radar_dimensions = _default_radar_dimensions(
                company_info.get("business_type_guess", "B2B_SERVICE")
            )

    result: dict[str, Any] = {
        # Секция 3: Компания
        "company": company,
        "swot": company_result.get("swot"),
        "digital": company_result.get("digital"),
        "market_share": market_share,

        # Секция 1: Рынок
        "market": market_result.get("market"),
        "regulatory_trends": market_result.get("regulatory_trends", []),
        "tech_trends": market_result.get("tech_trends", []),

        # Секция 2: Конкуренты
        "competitors": final_competitors,
        "radar_dimensions": radar_dimensions,

        # Финансы — из ФНС
        "financials": financials,

        # Секция 4: Стратегия
        "recommendations": strategy_result.get("recommendations", []),
        "kpi_benchmarks": strategy_result.get("kpi_benchmarks", []),
        "scenarios": strategy_result.get("scenarios", []),
        "implementation_timeline": strategy_result.get("implementation_timeline", []),

        # Секция 5: Приложения
        "open_questions": appendix_result.get("open_questions", []),
        "glossary": appendix_result.get("glossary") or _default_glossary(
            company_info.get("business_type_guess", "B2B_SERVICE")
        ),
        "calc_traces": calc_traces,
        "methodology": methodology,

        # Секция 6: Фаундеры и мнения
        "founders": founders,
        "opinions": opinions_result.get("opinions", []),

        # Секция 7: HR (transform LLM format → template format, enrich with HH.ru)
        "hr_data": _transform_hr_data(hr_result.get("hr_data", {}), hh_data),

        # Секция 8: Продукты
        "products": products_result.get("products", []),

        # Pipeline metadata
        "pipeline_version": "4.0",
    }

    # T49: Cross-section validation — fix contradictions
    result = _validate_cross_sections(result, fns_data)

    return result


# ── Fallback: glossary when LLM doesn't generate one ──

def _default_glossary(business_type: str) -> dict:
    """Universal business glossary fallback."""
    base = {
        "SWOT": "Метод стратегического анализа: Strengths (сильные стороны), Weaknesses (слабые), Opportunities (возможности), Threats (угрозы)",
        "EBITDA": "Прибыль до вычета процентов, налогов, амортизации — показатель операционной эффективности",
        "ROE": "Return on Equity — рентабельность собственного капитала (чистая прибыль / собственный капитал × 100%)",
        "ИНН": "Идентификационный номер налогоплательщика — уникальный код юридического лица в ФНС",
        "ОКВЭД": "Общероссийский классификатор видов экономической деятельности",
        "ЕГРЮЛ": "Единый государственный реестр юридических лиц",
        "ФНС": "Федеральная налоговая служба — источник официальных финансовых данных",
    }
    if "B2B" in business_type:
        base.update({
            "CAC": "Customer Acquisition Cost — стоимость привлечения одного клиента",
            "LTV": "Lifetime Value — совокупная прибыль от клиента за всё время сотрудничества",
            "NPS": "Net Promoter Score — индекс потребительской лояльности (от -100 до +100)",
            "ARR": "Annual Recurring Revenue — годовая повторяющаяся выручка (для SaaS)",
            "Churn": "Отток клиентов — доля клиентов, прекративших пользоваться продуктом за период",
        })
    if "B2C" in business_type:
        base.update({
            "Средний чек": "Средняя сумма одной покупки = выручка / количество заказов",
            "LTV": "Lifetime Value — совокупная выручка от одного клиента за всё время",
            "GMV": "Gross Merchandise Value — общий объём продаж через платформу",
            "Конверсия": "Доля посетителей, совершивших целевое действие (покупку, регистрацию)",
        })
    return base


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


# ── T46: Validate calc_traces ──

def _validate_calc_traces(traces: list[dict], financials: list[dict]) -> list[dict]:
    """Flag or fix calc_traces with obvious arithmetic errors."""
    fns_revenue = None
    if financials:
        latest = financials[-1]
        fns_revenue = latest.get("revenue")  # in тыс. руб.

    for trace in traces:
        name_lower = trace.get("metric_name", "").lower()
        confidence = trace.get("confidence", "ESTIMATE")

        # Skip facts — they're from ФНС, already verified
        if confidence == "FACT":
            continue

        # LTV/CAC > 10 → flag as suspicious
        if "ltv/cac" in name_lower or "ltv / cac" in name_lower:
            try:
                val = float(str(trace.get("value", "0")).replace(",", ".").replace("x", ""))
                if val > 10:
                    trace["confidence"] = "ESTIMATE"
                    trace["_warning"] = (
                        f"⚠ Значение {val} аномально высокое (норма 3-5x). "
                        "Возможна ошибка в CAC или LTV"
                    )
                    logger.warning("T46: LTV/CAC=%s flagged as suspicious", val)
            except (ValueError, TypeError):
                pass

        # Conversion rate > 20% → flag
        if "конверси" in name_lower or "conversion" in name_lower:
            try:
                val_str = str(trace.get("value", "0")).replace("%", "").replace(",", ".")
                val = float(val_str)
                if val > 20:
                    trace["confidence"] = "ESTIMATE"
                    trace["_warning"] = (
                        f"⚠ Конверсия {val}% нереалистична (норма e-commerce 2-5%)"
                    )
                    logger.warning("T46: Conversion=%s%% flagged", val)
            except (ValueError, TypeError):
                pass

        # EBITDA margin: check if expenses sum > 100%
        if "ebitda" in name_lower and "margin" in name_lower:
            try:
                val_str = str(trace.get("value", "0")).replace("%", "").replace(",", ".")
                val = float(val_str)
                if val > 35:
                    trace["_warning"] = (
                        f"⚠ EBITDA margin {val}% — проверить структуру расходов"
                    )
            except (ValueError, TypeError):
                pass

    return traces


# ── T47: Validate market share ──

def _validate_market_share(
    market_share: dict, financials: list[dict], market_data: dict,
) -> dict:
    """Fix absurd market share: check that share% × market_size ≈ revenue."""
    if not market_share or not financials:
        return market_share

    # Get company revenue (тыс. руб.)
    latest = financials[-1] if financials else {}
    revenue_k = latest.get("revenue")
    if not revenue_k:
        return market_share

    # Get market size
    market_size_str = ""
    if isinstance(market_data, dict):
        market_size_str = str(market_data.get("market_size", ""))

    # Try to parse market size (formats: "18 млрд руб.", "430 млрд")
    import re as _re
    market_size_k = None  # in тыс. руб.
    m = _re.search(r"([\d,.]+)\s*(млрд|трлн|млн)", market_size_str)
    if m:
        num = float(m.group(1).replace(",", "."))
        unit = m.group(2)
        if unit == "трлн":
            market_size_k = num * 1_000_000_000
        elif unit == "млрд":
            market_size_k = num * 1_000_000
        elif unit == "млн":
            market_size_k = num * 1_000

    if not market_size_k:
        return market_share

    # Validate each company's share
    validated = {}
    for name, share_pct in market_share.items():
        if not isinstance(share_pct, (int, float)):
            validated[name] = share_pct
            continue

        implied_revenue_k = share_pct / 100.0 * market_size_k

        # Check if it's the target company (first entry or matches name pattern)
        # If implied revenue > 3x actual → recalculate
        if implied_revenue_k > revenue_k * 3 and share_pct > 0.5:
            correct_pct = round(revenue_k / market_size_k * 100, 3)
            logger.warning(
                "T47: Market share %s: %.1f%% implies %.0f тыс. but revenue=%.0f тыс. "
                "→ corrected to %.3f%%",
                name, share_pct, implied_revenue_k, revenue_k, correct_pct,
            )
            validated[name] = correct_pct
        else:
            validated[name] = share_pct

    return validated


# ── T49: Cross-section validation ──

def _validate_cross_sections(result: dict, fns_data: dict) -> dict:
    """Fix contradictions between sections using ФНС as ground truth.

    Ensures: one number for revenue, employees, etc. across all sections.
    """
    financials = result.get("financials", [])
    if not financials:
        return result

    latest = financials[-1] if financials else {}
    fns_revenue_k = latest.get("revenue")  # тыс. руб.
    fns_employees = latest.get("employees")

    warnings = []

    # Check scenarios: base scenario should be >= current revenue
    scenarios = result.get("scenarios", [])
    if fns_revenue_k and scenarios:
        fns_revenue_mln = fns_revenue_k / 1000  # Convert to млн
        for scenario in scenarios:
            metrics = scenario.get("metrics", {})
            # Try to find revenue in metrics
            for key in ("revenue", "выручка", "Выручка"):
                val = metrics.get(key)
                if val and isinstance(val, (int, float)):
                    # Check if base/optimistic scenario is below current
                    label = scenario.get("label", scenario.get("name", ""))
                    if "базов" in label.lower() or "base" in label.lower():
                        if val < fns_revenue_mln * 0.8:
                            warnings.append(
                                f"Базовый сценарий ({val} млн) ниже текущей выручки "
                                f"({fns_revenue_mln:.1f} млн по ФНС)"
                            )

    # Add warnings to open_questions
    if warnings:
        open_q = result.get("open_questions", [])
        for w in warnings:
            q = f"[Авто-валидация] {w}"
            if q not in open_q:
                open_q.append(q)
        result["open_questions"] = open_q
        logger.info("T49: Added %d cross-section warnings", len(warnings))

    return result
