"""Step 5: Deep analysis with GPT-5.2 Pro — the main brain.

Takes all collected real data and produces the full ReportData JSON.
v2.0: adds calc_traces, lifecycle, sales_channels, methodology.
"""

from __future__ import annotations

import json
from typing import Optional

from app.pipeline.llm_client import call_llm_json


SYSTEM = """Ты — ведущий бизнес-аналитик (pipeline v2.0). Тебе предоставлены РЕАЛЬНЫЕ данные из ФНС,
с сайта компании и результаты исследования рынка. Твоя задача — провести глубокий
анализ и сформировать структурированный отчёт.

ВАЖНО:
- Используй РЕАЛЬНЫЕ финансовые данные из ФНС, не выдумывай
- Если данных нет — пометь в open_questions, не фантазируй
- Конкуренты уже определены и подтверждены пользователем
- Все суммы в тысячах рублей если не указано иное
- ВСЁ на русском языке
- Ответ — ТОЛЬКО валидный JSON

## Правила v2.0

### Calc-trace (прозрачность вычислений)
Для КАЖДОГО расчётного показателя добавь запись в calc_traces:
- metric_name: название
- value: итоговое значение
- formula: формула расчёта
- inputs: входные данные (dict)
- sources: список источников
- confidence: "FACT" (прямые данные), "CALC" (вычислено из фактов), "ESTIMATE" (с допущениями)
Правило: если хотя бы один вход = ESTIMATE, весь результат = ESTIMATE.

### Жизненный цикл конкурентов
Для каждого конкурента определи стадию:
- startup: убытки, рост >50%, фандрейзинг
- growth: рост >20%, CAPEX
- investment: стройка заводов, M&A, крупные CAPEX → ЗАПРЕТ на вывод о «неэффективности»
- mature: стабильная маржа, low CAPEX

### Каналы продаж
Для каждого конкурента проверь 7-12 каналов:
Сайт D2C, WB, Ozon, Собственные точки, B2B/опт, HoReCa, Lamoda и др.
"канал отсутствует" = проверено 3+ источника. "стать партнёром" на сайте = канал ЕСТЬ.

### HR-протокол
- 0 вакансий на HH ≠ 0 найма (агентства, аффилированные юрлица, TG-каналы)
- Зарплаты: gross + KPI 15-20% для позиций с переменной частью

### B2B_B2C_HYBRID
Если тип = HYBRID: метрики B2C и B2B считать РАЗДЕЛЬНО.
Средний чек, LTV, CAC — отдельно для каждого канала."""


def run(
    scraped: dict,
    company_info: dict,
    fns_data: dict,
    competitors: list[dict],
    market_info: dict,
    deep_models: Optional[dict] = None,
    marketplace_data: Optional[dict] = None,
) -> dict:
    """Run deep analysis with GPT-5.2 Pro.

    Combines all real data into a comprehensive report structure.
    v2.0: accepts deep_models (lifecycle, channels) and marketplace_data.
    """
    # Prepare financial summary
    fin_text = "Нет данных из ФНС"
    if fns_data.get("financials"):
        fin_lines = []
        for f in fns_data["financials"]:
            fin_lines.append(
                f"  {f['year']}: выручка={f.get('revenue','?')} тыс., "
                f"прибыль={f.get('net_profit','?')} тыс., "
                f"активы={f.get('assets','?')} тыс."
            )
        fin_text = "\n".join(fin_lines)

    # Prepare founders summary
    founders_text = "Нет данных"
    egrul = fns_data.get("egrul", {})
    if egrul.get("founders"):
        f_lines = []
        for f in egrul["founders"]:
            f_lines.append(f"  {f.get('name','')} — доля: {f.get('share_percent','')}%")
        founders_text = "\n".join(f_lines)

    director = egrul.get("director", {})
    director_text = director.get("name", "Нет данных")

    # Prepare competitors summary
    comp_text = ""
    for i, c in enumerate(competitors, 1):
        comp_text += f"\n{i}. {c.get('name','')} — {c.get('description','')}"
        if c.get("website"):
            comp_text += f" ({c['website']})"
        comp_text += f" [угроза: {c.get('threat_level','med')}]"

    # Prepare affiliates
    aff_text = "Нет данных"
    if fns_data.get("affiliates"):
        a_lines = []
        for a in fns_data["affiliates"][:10]:
            a_lines.append(f"  {a.get('name','')} (ИНН: {a.get('inn','')}) — {a.get('connection','')}")
        aff_text = "\n".join(a_lines)

    social_info = ""
    for s in scraped.get("social_links", []):
        social_info += f"  {s['platform']}: {s.get('handle', '')} ({s.get('url', '')})\n"

    axis_x = market_info.get("axis_x", "Цена")
    axis_y = market_info.get("axis_y", "Качество")

    # v2.0: deep models context
    deep_models_text = ""
    if deep_models:
        if deep_models.get("lifecycles"):
            deep_models_text += "\n## Предварительные данные жизненного цикла\n"
            for name, lc in deep_models["lifecycles"].items():
                deep_models_text += f"  {name}: стадия={lc.get('stage','?')}, основание={', '.join(lc.get('evidence',[]))}\n"
        if deep_models.get("channels"):
            deep_models_text += "\n## Предварительные данные каналов продаж\n"
            for name, channels in deep_models["channels"].items():
                ch_list = [f"{ch['channel_name']}={'да' if ch.get('exists') else 'нет' if ch.get('exists') is False else '?'}" for ch in channels]
                deep_models_text += f"  {name}: {', '.join(ch_list)}\n"

    # v2.0: marketplace data context
    marketplace_text = ""
    if marketplace_data:
        marketplace_text = f"\n## Данные маркетплейсов\n{json.dumps(marketplace_data, ensure_ascii=False, indent=2)[:3000]}\n"

    prompt = f"""Проведи полный бизнес-анализ на основе РЕАЛЬНЫХ данных.

## Компания

Название: {company_info.get('name', '')}
Юрлицо: {egrul.get('full_name', company_info.get('legal_name', ''))}
ИНН: {egrul.get('inn', '')}
ОГРН: {egrul.get('ogrn', '')}
ОКВЭД: {egrul.get('okved', '')} — {egrul.get('okved_name', '')}
Дата регистрации: {egrul.get('reg_date', '')}
Уставный капитал: {egrul.get('capital', '')}
Директор: {director_text}
Тип бизнеса: {company_info.get('business_type_guess', '')}
Описание: {company_info.get('description', '')}

Сайт: {scraped.get('url', '')}
Соцсети:
{social_info}

## Учредители (ЕГРЮЛ)
{founders_text}

## Аффилированные лица
{aff_text}

## Финансы из ФНС (тыс. руб.)
{fin_text}

## Рынок
Рынок: {market_info.get('market_name', '')}
Описание: {market_info.get('market_description', '')}

## Подтверждённые конкуренты
{comp_text}
{deep_models_text}
{marketplace_text}

## Текст сайта
{scraped.get('text', '')[:5000]}

## Задание

Верни JSON-объект со ВСЕМИ полями ниже.

{{
  "company": {{
    "name": "{company_info.get('name', '')}",
    "legal_name": "{egrul.get('full_name', '')}",
    "inn": "{egrul.get('inn', '')}",
    "okved": "{egrul.get('okved', '')}",
    "business_type": "{company_info.get('business_type_guess', 'B2B_SERVICE')}",
    "address": "адрес",
    "website": "{scraped.get('url', '')}",
    "description": "Что делает компания, 2-3 предложения",
    "badges": ["badge1", "badge2", "badge3"]
  }},
  "market": {{
    "market_name": "{market_info.get('market_name', '')}",
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
  "competitors": [
    {{
      "name": "Название",
      "description": "Описание",
      "website": "https://...",
      "address": "адрес или null",
      "x": 0-100,
      "y": 0-100,
      "radar_scores": {{"Param1": 1-10, "Param2": 1-10}},
      "metrics": {{"Метрика1": "значение"}},
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
  "radar_dimensions": ["Param1", "Param2", "Param3", "Param4", "Param5", "Param6"],
  "financials": {json.dumps(fns_data.get('financials', []), ensure_ascii=False) if fns_data.get('financials') else '[{{"year": 2024, "revenue": null, "net_profit": null, "assets": null, "equity": null, "employees": null}}]'},
  "swot": {{
    "strengths": ["сила 1", "сила 2", "сила 3", "сила 4"],
    "weaknesses": ["слабость 1", "слабость 2", "слабость 3", "слабость 4"],
    "opportunities": ["возм. 1", "возм. 2", "возм. 3", "возм. 4"],
    "threats": ["угроза 1", "угроза 2", "угроза 3", "угроза 4"]
  }},
  "digital": {{
    "social_accounts": [
      {{"platform": "...", "handle": "@...", "followers": число_или_null}}
    ],
    "seo_score": 0-100,
    "monthly_traffic": число
  }},
  "market_share": {{
    "Компания": процент,
    "Конкурент1": процент,
    "Другие": процент
  }},
  "recommendations": [
    {{
      "title": "Рекомендация",
      "description": "Подробное описание",
      "priority": "high/medium/low",
      "timeline": "Q1-Q2 2026",
      "expected_impact": "+X% метрика"
    }}
  ],
  "kpi_benchmarks": [
    {{"name": "KPI", "current": число_или_null, "benchmark": число, "unit": "ед."}}
  ],
  "scenarios": [
    {{"name": "optimistic", "label": "Оптимистичный", "metrics": {{"Выручка, тыс.": число}}}},
    {{"name": "base", "label": "Базовый", "metrics": {{"Выручка, тыс.": число}}}},
    {{"name": "pessimistic", "label": "Пессимистичный", "metrics": {{"Выручка, тыс.": число}}}}
  ],
  "open_questions": ["вопрос 1", "вопрос 2"],
  "glossary": {{"Термин": "Определение"}},
  "founders": [
    {{"name": "ФИО", "role": "Должность", "share": "X%", "company": "ООО «...»", "social": {{}}}}
  ],
  "opinions": [
    {{"author": "Имя", "role": "Должность", "quote": "Цитата", "date": "Месяц Год", "source": "Источник"}}
  ],
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
  "methodology": {{
    "Источники данных": "перечисление",
    "Период анализа": "даты",
    "Допущения": "описание"
  }}
}}

## Правила

1. Конкуренты — ИСПОЛЬЗУЙ тех что даны выше, для каждого добавь:
   - x, y координаты на перцептуальной карте (ось X = {axis_x}, ось Y = {axis_y})
   - radar_scores по 6 параметрам
   - metrics (выручка если известна, сотрудники, год основания)
   - lifecycle (стадия + обоснование + год основания)
   - sales_channels (7-12 каналов: exists true/false/null)
2. Финансы — ИСПОЛЬЗУЙ РЕАЛЬНЫЕ данные из ФНС выше, не выдумывай
3. SWOT — конкретный, с цифрами, основанный на реальных данных
4. Рекомендации — 5-6 штук, приоритизированные
5. KPI — 6-8, релевантных типу бизнеса
6. Сценарии — 3, с 3-5 метриками каждый, основанных на реальной выручке
7. Founders — используй данные из ЕГРЮЛ выше
8. Opinions — 3-5 цитат реальных лидеров этой отрасли
9. Если данных нет — добавь в open_questions, НЕ выдумывай
10. calc_traces — минимум 5 записей для ключевых показателей (RevPASH, средний чек, LTV, EBITDA margin и т.д.)
11. methodology — заполни все 3 поля
12. Если тип HYBRID — метрики B2C и B2B РАЗДЕЛЬНО в kpi_benchmarks и scenarios
13. Lifecycle: если компания в инвестиционной фазе (CAPEX, стройка) — НЕ критикуй убытки"""

    result = call_llm_json(
        prompt, provider="openai", system=SYSTEM,
        temperature=0.4, max_tokens=20000,
    )

    # Inject real financials if LLM ignored them
    if fns_data.get("financials") and not result.get("financials"):
        result["financials"] = fns_data["financials"]

    # v2.0: Fallback — generate basic calc_traces from FNS data if LLM didn't
    if not result.get("calc_traces") and fns_data.get("financials"):
        result["calc_traces"] = _generate_basic_calc_traces(fns_data["financials"])

    # v2.0: Ensure methodology exists
    if not result.get("methodology"):
        result["methodology"] = {
            "Источники данных": "ФНС, Rusprofile, Яндекс Карты, HH.ru",
            "Период анализа": "Автоматический анализ",
            "Допущения": "Данные из открытых источников",
        }

    return result


def _generate_basic_calc_traces(financials: list[dict]) -> list[dict]:
    """Generate basic calc traces from FNS financial data."""
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
            "formula": "чистая_прибыль / выручка × 100",
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
