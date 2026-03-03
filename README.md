# Анализ бизнеса 360

Полный отчёт о любой компании за 2 минуты. Вставьте ссылку на сайт — система найдёт юрлицо, конкурентов и соберёт отчёт с реальными данными.

**Live:** http://89.167.19.68:8090

## Как работает

```
URL сайта → скрапинг → идентификация компании → ФНС → конкуренты → глубокий анализ → HTML-отчёт
```

Интерактивный пайплайн с двумя паузами: пользователь подтверждает компанию и редактирует список конкурентов.

### Шаги

| # | Шаг | Что делает |
|---|-----|-----------|
| 1 | Скрапинг | requests + Scrapling fallback при блокировке |
| 2 | Идентификация | LLM определяет название, тип бизнеса |
| 3 | ФНС | Поиск юрлица по ИНН (api-fns.ru) |
| — | **Пауза** | Пользователь подтверждает/правит компанию |
| 4 | Конкуренты | GPT-5.2 Pro находит 8-12 конкурентов |
| — | **Пауза** | Пользователь убирает нерелевантных |
| 1b | Маркетплейсы | Анализ WB/Ozon (для B2C/HYBRID) |
| 1c | Deep models | Жизненный цикл + каналы продаж |
| 5 | Анализ | GPT-5.2 Pro: SWOT, финансы, рекомендации |
| 2a | Верификация | Python: проверка расчётов |
| 2b | Gate | Фильтрация пустых/нерелевантных секций |
| 6 | Сборка | Jinja2 → HTML + inline SVG графики |

### Типы бизнеса

Автоопределение по ОКВЭД. Каждый тип — свои KPI, оси перцептуальной карты, набор блоков:

- B2C_SERVICE (ресторан, салон, клиника)
- B2C_PRODUCT (ритейл, e-com)
- B2B_SERVICE (SaaS, IT, консалтинг)
- B2B_PRODUCT (производство)
- PLATFORM (маркетплейс)
- B2B_B2C_HYBRID (B2B+B2C)

## Стек

- **Backend:** Python 3.11, FastAPI, uvicorn
- **LLM:** GPT-5.2 Pro (основной) + Gemini 2.5 Flash (быстрые задачи) + o3 (reasoning)
- **Данные:** ФНС API, web scraping (requests + BeautifulSoup + Scrapling)
- **Отчёт:** Jinja2 templates → один HTML файл, тёмная тема, inline SVG (0 зависимостей)
- **Модели:** Pydantic v2 (20+ моделей)
- **Деплой:** GitHub Actions → SSH → systemd + nginx (Nikvps)

## Отчёт включает

- Обзор рынка и тренды
- Перцептуальная карта (scatter SVG)
- Радар конкурентов
- Финансы из ФНС (grouped bars)
- SWOT-анализ
- Digital-аудит (соцсети, трафик)
- Доля рынка (donut)
- KPI-бенчмарки
- Сценарии (оптимистичный/базовый/пессимистичный)
- Рекомендации с таймлайном
- Фактчек + calc-traces (ФАКТ/РАСЧЁТ/ОЦЕНКА)
- Фаундеры и мнения лидеров отрасли

## Запуск локально

```bash
cp .env.example .env
# Заполнить GEMINI_API_KEY, FALLBACK_LLM_API_KEY (OpenAI)

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8083
```

## Roadmap

Задачи ведутся в [GitHub Issues](https://github.com/nledovckou-dot/business-score-russia/issues) и [Project Board](https://github.com/users/nledovckou-dot/projects/1).
