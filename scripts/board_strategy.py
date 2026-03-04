#!/usr/bin/env python3
"""Board of Directors — concrete strategy to get from 2/10 to 10/10."""

import json
import os
import sys
import time
import urllib.request
import urllib.error

API_KEY = os.environ.get("OPENAI_API_KEY") or os.environ.get("FALLBACK_LLM_API_KEY", "")


def call_openai(prompt: str, system: str, max_tokens: int = 8000) -> str:
    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens": max_tokens,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    usage = body.get("usage", {})
    print(f"Tokens: in={usage.get('prompt_tokens', 0)}, out={usage.get('completion_tokens', 0)}")
    return body["choices"][0]["message"]["content"]


DIAGNOSTIC = """
# ДИАГНОСТИКА: почему отчёты BSR 360 получили 2/10

## Текущие оценки (из 10)
- Aviasales: 5.2, Ozon: 5.6, Skillbox: 4.0, Selectel: 4.6, Dodo: 5.6
- Оценка CEO проекта: 2/10 — "ужасно что есть пустые места без данных"

## КОНКРЕТНЫЕ ПРОБЛЕМЫ (найдены в коде и отчётах)

### ПРОБЛЕМА 1: Пустые секции с заглушками
Каждый отчёт содержит 4-8 "📋 Данные по этой секции будут доступны после ручной проверки":
- **Digital-аудит (P4)** — пусто. LLM возвращает digital объект, но social_accounts часто пустой
- **Продукты и услуги (P5)** — пусто. Pipeline ВООБЩЕ не генерирует поле `products`
- **Фактчек (F1)** — пусто. step2a_verify.py существует, но НЕ заполняет поля `factcheck` и `digital_verification` в формате ReportData
- **Верификация digital (F2)** — пусто по той же причине
- **Совет директоров (B1)** — пусто. TPM лимит OpenAI (30K tokens/min) блокирует параллельные вызовы

### ПРОБЛЕМА 2: HR секция сломана (m4_hr_market.html)
- Шаблон ожидает: `hr_data.metrics` (список dict с value/label/color) и `hr_data.salaries` (для графика)
- LLM возвращает: `hr_data.key_positions`, `hr_data.employees_count`, `hr_data.avg_salary_market`
- Результат: шаблон рендерит ПУСТОЙ div, который выглядит как сломанная секция
- В Ozon: "HR-рынок и кадровый дефицит" показывает "0 в а к а н" — побуквенно!

### ПРОБЛЕМА 3: KPI "Нет данных"
- KPIBenchmark.current = null для реальных метрик
- Шаблон показывает "Нет данных" вместо "--" или скрытия
- Ozon: "Количество заказов — Нет данных", "Средний чек — Нет данных"

### ПРОБЛЕМА 4: HH.ru API не подключен
- Файл `app/pipeline/sources/hh_api.py` СУЩЕСТВУЕТ, но НЕ вызывается в pipeline
- App token есть: APPLGI4E9FIPPNIFD87G81NABJD2DO503RDSB6MEQDPLNMLCHT7OFA5AQOIGEK99
- Мог бы давать: реальные вакансии, зарплаты, количество открытых позиций

### ПРОБЛЕМА 5: Скрапинг слабый
- С VPS в Хельсинки многие .ru сайты блокируют
- Scrapling (StealthyFetcher) НЕ установлен на VPS — только requests + BS4
- Результат: text сайта = пустой или минимальный → LLM генерит из головы

### ПРОБЛЕМА 6: ФНС данные не всегда подтягиваются
- Для некоторых компаний (Ozon — иностранная юрисдикция) ФНС не возвращает данные
- Нет fallback на Rusprofile/SBIS

### ПРОБЛЕМА 7: Борд не работает
- call_board_llm_parallel() запускает 4 эксперта ПАРАЛЛЕЛЬНО → 4 × ~12K tokens = 48K → > 30K TPM лимит
- Нужно: последовательный вызов с задержками ИЛИ повысить TPM tier

## АРХИТЕКТУРА ПАЙПЛАЙНА (для понимания)
```
step1_scrape → step2_identify → step3_fns → step4_competitors → step5_deep_analysis (7 секций параллельно) → step6_board → build_report
```

step5 генерирует 7 параллельных секций через ThreadPoolExecutor:
1. analyze_market → market, regulatory_trends, tech_trends
2. analyze_competitors_deep → competitors, radar_dimensions
3. analyze_company → company, swot, digital, market_share
4. analyze_strategy → recommendations, kpi_benchmarks, scenarios, timeline
5. analyze_appendix → glossary, methodology, calc_traces, open_questions
6. analyze_opinions → founders, opinions
7. analyze_hr → hr_data

НЕ ГЕНЕРИРУЮТСЯ (пустые в ReportData): products, menu, tenders, reviews, factcheck, digital_verification, correlations, board_review

## СТЕК
- Python 3.11 / FastAPI / uvicorn / Jinja2 / Pydantic v2
- LLM: gpt-4o (основной), gemini-2.5-flash (быстрые), o3-mini (reasoning)
- VPS: Nikvps (Хельсинки), nginx :8090 → uvicorn :8083
- Deploy: git push → GitHub Actions → SSH → systemd restart
"""

SYSTEM = """Ты — совет директоров AI-стартапа BSR 360 (Бизнес-анализ 360°).

Ваш продукт: автоматическая генерация бизнес-аналитических отчётов по URL компании.
Текущее качество: 2/10 (оценка CEO). Цель: 10/10.

В совете:
1. CTO (техдир) — архитектура, инфра, надёжность
2. CDO (Chief Data Officer) — качество данных, верификация, источники
3. CPO (продукт) — UX, визуал, ценность для пользователя
4. CGO (рост) — бизнес-модель, монетизация

Напиши КОНКРЕТНЫЙ план действий от текущего 2/10 до 10/10.

ФОРМАТ:
- Каждое действие = конкретная задача с файлом и строкой кода
- Сгруппировать по спринтам (1-2 недели каждый)
- Указать: что починить, в каком файле, какой результат ожидается
- Приоритет: сначала то, что убирает ПУСТЫЕ СЕКЦИИ (главная боль CEO)

НЕ ПИШИ абстракций типа "улучшить качество данных". Только конкретика:
"В файле X, строка Y, заменить Z на W, потому что..."

Ответ на русском языке."""

def main():
    if not API_KEY:
        print("Set OPENAI_API_KEY or FALLBACK_LLM_API_KEY")
        sys.exit(1)

    print("=" * 70)
    print("BSR Совет Директоров — Стратегия 2/10 → 10/10")
    print("=" * 70)

    result = call_openai(DIAGNOSTIC, SYSTEM)
    print(result)

    with open("scripts/board_strategy.md", "w", encoding="utf-8") as f:
        f.write(result)
    print(f"\n\nСтратегия сохранена в scripts/board_strategy.md")


if __name__ == "__main__":
    main()
