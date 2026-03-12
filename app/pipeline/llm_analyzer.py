"""LLM analyzer: sends scraped website data to Gemini → gets structured ReportData JSON."""

from __future__ import annotations

import json
import os
import re
import urllib.request
import urllib.error
from typing import Any


GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"


def analyze_with_llm(scraped: dict, progress_cb=None) -> dict:
    """Send scraped data to Gemini and get structured report data back.

    Returns a dict matching the ReportData schema.
    progress_cb: optional callable(step_name, step_num, total_steps)
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    # Build the prompt
    site_text = scraped.get("text", "")[:6000]
    pages_text = ""
    for page, text in scraped.get("pages_text", {}).items():
        pages_text += f"\n--- Страница {page} ---\n{text[:2000]}\n"

    social_info = ""
    for s in scraped.get("social_links", []):
        social_info += f"  {s['platform']}: {s.get('handle', '')} ({s.get('url', '')})\n"

    contacts_info = json.dumps(scraped.get("contacts", {}), ensure_ascii=False)

    prompt = f"""Ты — аналитик бизнеса. Я дам тебе содержимое сайта компании. Твоя задача — собрать полный аналитический отчёт на основе этих данных и твоих знаний о рынке.

## Данные сайта

URL: {scraped.get('url', '')}
Домен: {scraped.get('domain', '')}
Title: {scraped.get('title', '')}
Description: {scraped.get('description', '')}
Заголовки: {', '.join(scraped.get('headings', [])[:20])}

Контакты: {contacts_info}
Соцсети:
{social_info}

Текст главной страницы:
{site_text}

Дополнительные страницы:
{pages_text}

## Задание

На основе этих данных и своих знаний о рынке, верни JSON-объект со ВСЕМИ полями ниже. Каждое поле обязательно — если точных данных нет, сделай обоснованную оценку на основе типа бизнеса и рынка.

Формат ответа — ТОЛЬКО валидный JSON (без markdown, без ```):

{{
  "company": {{
    "name": "Название компании",
    "legal_name": "ООО «...»" или null (если найдёшь на сайте),
    "inn": "ИНН" или null (если найдёшь),
    "okved": "XX.XX" или null,
    "business_type": "B2C_SERVICE" или "B2C_PRODUCT" или "B2B_SERVICE" или "B2B_PRODUCT" или "PLATFORM",
    "address": "адрес" или null,
    "website": "{scraped.get('url', '')}",
    "description": "Что делает компания, 2-3 предложения",
    "badges": ["badge1", "badge2", "badge3"]
  }},
  "market": {{
    "market_name": "Название рынка (по-русски)",
    "market_size": "XX млрд ₽",
    "growth_rate": "+X% CAGR",
    "data_points": [
      {{"year": 2021, "value": число, "label": "млрд ₽"}},
      {{"year": 2022, "value": число, "label": "млрд ₽"}},
      {{"year": 2023, "value": число, "label": "млрд ₽"}},
      {{"year": 2024, "value": число, "label": "млрд ₽"}}
    ],
    "trends": ["тренд 1", "тренд 2", "тренд 3", "тренд 4"],
    "sources": ["источник 1", "источник 2"]
  }},
  "competitors": [
    {{
      "name": "Конкурент 1",
      "description": "Описание",
      "website": "https://...",
      "address": "адрес" или null,
      "x": 0-100 (позиция на карте по оси Цена),
      "y": 0-100 (позиция по оси Качество/Уникальность),
      "radar_scores": {{"Параметр1": 1-10, "Параметр2": 1-10, ...}},
      "metrics": {{"Метрика1": "значение", "Метрика2": "значение"}},
      "threat_level": "high" или "med" или "low"
    }}
  ],
  "radar_dimensions": ["Параметр1", "Параметр2", "Параметр3", "Параметр4", "Параметр5", "Параметр6"],
  "financials": [
    {{"year": 2022, "revenue": число_тыс_руб, "net_profit": число_тыс_руб, "assets": число_тыс_руб, "equity": число_тыс_руб, "employees": число}},
    {{"year": 2023, "revenue": число, "net_profit": число, "assets": число, "equity": число, "employees": число}},
    {{"year": 2024, "revenue": число, "net_profit": число, "assets": число, "equity": число, "employees": число}}
  ],
  "swot": {{
    "strengths": ["сила 1", "сила 2", "сила 3", "сила 4"],
    "weaknesses": ["слабость 1", "слабость 2", "слабость 3", "слабость 4"],
    "opportunities": ["возм. 1", "возм. 2", "возм. 3", "возм. 4"],
    "threats": ["угроза 1", "угроза 2", "угроза 3", "угроза 4"]
  }},
  "digital": {{
    "social_accounts": [
      {{"platform": "Instagram", "handle": "@...", "followers": число_или_null, "engagement_rate": число_или_null, "avg_likes": число_или_null, "avg_comments": число_или_null, "avg_views": число_или_null}},
      {{"platform": "Telegram", "handle": "@...", "followers": число_или_null, "engagement_rate": число_или_null, "avg_likes": число_или_null, "avg_comments": число_или_null, "avg_views": число_или_null}}
    ],
    "seo_score": 0-100 (оценка),
    "monthly_traffic": число_оценка
  }},
  "market_share": {{
    "Компания": процент,
    "Конкурент1": процент,
    "Конкурент2": процент,
    "Другие": процент
  }},
  "recommendations": [
    {{
      "title": "Рекомендация 1",
      "description": "Подробное описание",
      "priority": "high" или "medium" или "low",
      "timeline": "Q1-Q2 2026",
      "expected_impact": "+X% метрика"
    }}
  ],
  "kpi_benchmarks": [
    {{"name": "KPI название", "current": число_или_null, "benchmark": число, "unit": "ед. изм."}}
  ],
  "scenarios": [
    {{"name": "optimistic", "label": "Оптимистичный", "metrics": {{"Метрика1": число, "Метрика2": число}}}},
    {{"name": "base", "label": "Базовый", "metrics": {{"Метрика1": число, "Метрика2": число}}}},
    {{"name": "pessimistic", "label": "Пессимистичный", "metrics": {{"Метрика1": число, "Метрика2": число}}}}
  ],
  "open_questions": ["вопрос 1", "вопрос 2", "вопрос 3"],
  "glossary": {{
    "Термин1": "Определение и формула",
    "Термин2": "Определение и формула"
  }},
  "founders": [
    {{"name": "ФИО", "role": "Должность", "share": "X%", "company": "ООО «...»", "social": {{}}}}
  ],
  "opinions": [
    {{"author": "Имя", "role": "Должность", "quote": "Цитата о рынке", "date": "Месяц Год", "source": "Источник"}}
  ]
}}

## Правила
1. Конкурентов минимум 5-8 штук, реальные компании на этом рынке
2. Radar dimensions — 6 параметров, релевантных типу бизнеса
3. Radar scores у КАЖДОГО конкурента по ВСЕМ 6 параметрам
4. Финансы — если не знаешь точно, оцени по размеру компании (но пометь в open_questions)
5. SWOT — по 4 пункта в каждом квадранте, конкретные, с цифрами где возможно
6. Рекомендации — 4-6 штук, приоритизированные, с таймлайном
7. KPI — 5-8 штук, релевантных типу бизнеса
8. Сценарии — 3 штуки, с 3-5 метриками в каждом
9. Glossary — 6-10 терминов с формулами
10. Opinions — 3-5 реальных цитат лидеров этого рынка (если не знаешь точные — создай правдоподобные)
11. ВСЁ на русском языке
12. Ответ — ТОЛЬКО JSON, без пояснений"""

    if progress_cb:
        progress_cb("Анализ данных через AI...", 2, 3)

    # Call Gemini
    url = f"{GEMINI_API_URL}?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 16000,
            "responseMimeType": "application/json",
        },
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        raise RuntimeError(f"Gemini API error {e.code}: {error_body[:500]}")

    # Extract text from Gemini response
    try:
        text = body["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected Gemini response: {json.dumps(body)[:500]}")

    # Parse JSON — strip markdown fences if present
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        result = json.loads(text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse LLM JSON: {e}\nRaw: {text[:1000]}")

    return result
