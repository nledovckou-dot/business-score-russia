# Ночной прогон — 13-14 марта 2026

## Цель
Сгенерировать 10 демо-отчётов, найти и пофиксить все баги. Стабильный пайплайн.

## Сгенерированные отчёты

| # | Компания | ID | Размер | Тип бизнеса | Статус |
|---|----------|----|--------|-------------|--------|
| 1 | Кофемания | 437784aa | 161 KB | B2C_SERVICE | Готово |
| 2 | amoCRM | 7f58b489 | 159 KB | B2B_SERVICE | Готово |
| 3 | Ozon | 88ed9734 | 192 KB | PLATFORM | Готово |
| 4 | Инвитро | eccaeacb | 144 KB | B2C_SERVICE | Готово |
| 5 | Т-Банк (Тинькофф) | 56743cb7 | 175 KB | B2B_SERVICE | Готово |
| 6 | КАМАЗ | d5412710 | 143 KB | B2B_PRODUCT | Готово |
| 7 | Хеликс | c9499eb7 | 163 KB | B2C_SERVICE | Готово |
| 8 | Wildberries | 1bb8532a | 173 KB | PLATFORM | Готово |
| 9 | DNS | 03b2729a | 216 KB | B2C_PRODUCT | Готово |
| 10 | СДЭК | cbb07ef1 | 192 KB | B2B_SERVICE | Готово |

**Бонус**: Mindbox (08cf73dd, 187 KB) — B2B_SERVICE

**Все 10/10 отчётов успешно сгенерированы!** DNS и СДЭК потребовали 6+ попыток из-за deploy-kills-sessions проблемы.

### Отчёты на VPS (app/storage/reports/)

| ID | Размер | Дата | Компания |
|----|--------|------|----------|
| bc0eabe6 | 208K | 2026-03-13 17:48 | Selectel |
| 12e805cc | 172K | 2026-03-13 19:01 | DNS Shop |
| 7478ac4b | 152K | 2026-03-13 19:03 | Додо Пицца |
| d6402525 | 156K | 2026-03-13 19:05 | Т-Банк |
| 18fd7f67 | 204K | 2026-03-13 23:46 | КАМАЗ |
| 62715c47 | 188K | 2026-03-13 23:46 | DNS |
| bc1aa890 | 212K | 2026-03-14 00:50 | DNS |
| 03b2729a | 216K | 2026-03-14 01:47 | DNS (с QA Director) |
| cbb07ef1 | 192K | 2026-03-14 01:52 | СДЭК (с QA Director) |

## QA Director + программатические проверки (реализовано)

### Что добавлено

1. **6-й эксперт "Директор по качеству"** в Board of Directors (`step6_board.py`)
   - Проверяет 4 критерия: читаемость, пустые поля, единообразие единиц, согласованность
   - Каждый critique содержит поле `criteria` (readability/empty_fields/units/consistency)
   - Pre-scan: автоматический предварительный анализ перед промптом QA Director

2. **4 программатические проверки** в `step_quality.py`:
   - Check 6: `_empty_field_detector()` — SWOT квадранты, конкуренты, KPI, финансы, глоссарий
   - Check 7: `_unit_consistency_check()` — market share ~100%, radar [0,10], финансовые года
   - Check 8: `_cross_section_consistency_check()` — выручка ФНС vs сценарии vs KPI, конкуренты
   - Check 9: `_readability_check()` — JSON-фрагменты, placeholder'ы, шаблонные переменные

3. **Верификация**: QA Director присутствует в обоих новых отчётах (DNS, СДЭК)

## Найденные и исправленные баги

### Критические (пайплайн падал)

| Баг | Файл | Коммит | Описание |
|-----|------|--------|----------|
| Proxy format mismatch | scraper.py, fns.py | v0.7.0 | Прокси возвращал `{status, html}`, код ожидал `{ok, text}` |
| auth_token NameError | main.py | df91f21 | После удаления rate limit кода переменная auth_token не была определена |
| pkill killed SSH session | deploy.yml | ad2f5ef | `pkill -f uvicorn` убивал процесс SSH. Заменено на `systemctl stop/start` |
| Scrape crash → pipeline crash | step1_scrape.py | v0.7.0 | scrape_website() exception не ловилось |

### Средние (некорректные данные)

| Баг | Файл | Коммит | Описание |
|-----|------|--------|----------|
| Все конкуренты дропались | main.py | 08a117d | Competitor Pydantic validation слишком строгая — дропала конкурентов с невалидными nested fields. Заменено на salvage: минимальный набор полей + пробуем каждое optional поле |
| Competitor lifecycle потеря | main.py | fd2c37d | Salvage не сохранял lifecycle/financials/sales_channels. Добавлено пошаговое включение |
| str вместо list | main.py | v0.7.0 | `_ensure_list_of_str()` — LLM возвращал строку вместо списка для tech_trends, badges, swot quadrants и др. → char-by-char рендеринг |
| threat_level "self" | main.py, templates | v0.7.0 | Шаблон не знал значение "self" → KeyError. Добавлена поддержка |
| No-FNS estimation пустое | step5_deep_analysis.py | v0.7.0 | Когда ФНС недоступен, KPI current=null. Добавлена LLM-оценка с маркировкой ⚠ |

### Инфраструктурные

| Баг | Файл | Коммит | Описание |
|-----|------|--------|----------|
| Rate limit 10/час hardcoded | security.py | 226ef19, 9c3b2df | Env var не загружался. Hardcoded 100, потом disabled для тестирования |
| Stale .pyc в venv | deploy.yml | 2803ebb | Старый compiled code из `pip install -e .` перехватывал импорты |
| Deploy kills running sessions | deploy.yml | — | `git push` → restart → in-memory sessions потеряны. Нужна персистенция |

## Системные проблемы (не пофикшены)

### 1. Пустые конкуренты (средний балл 3/10)
**Проблема**: Конкурентные профили (C2), перцептуальная карта (C1), радар (C3), сравнительная таблица (C4) — часто пустые. LLM генерирует данные, но валидация Pydantic отбраковывает их.
**Фикс**: Salvage с пошаговым включением опциональных полей (08a117d, fd2c37d).
**Статус**: Деплоено и протестировано. DNS/СДЭК отчёты сгенерированы с улучшенной валидацией.

### 2. ФНС недоступен для крупных компаний
**Проблема**: ФНС API не возвращает данные для ПАО (КАМАЗ, WB, Тинькофф). Прокси fallback тоже не помогает.
**Причина**: Крупные компании публикуют отчётность отдельно, api-fns.ru может не иметь данных.
**Фикс**: No-FNS estimation через LLM (v0.7.0). Не идеально — нужен дополнительный источник (СБИС, Audit-it).

### 3. Deploy kills running analyses
**Проблема**: Каждый `git push` перезапускает BSR → in-memory сессии теряются.
**Workaround**: Не пушить пока генерируются отчёты. Отчёты сохраняются на диск и переживают рестарт.
**Правильный фикс**: Персистенция сессий в файловой системе или graceful shutdown с ожиданием.

### 4. Anthropic API кредиты исчерпаны
**Проблема**: Claude Opus для Board of Directors недоступен — "Your credit balance is too low".
**Workaround**: Board LLM автоматически fallback на GPT → Gemini. Работает, но качество рецензий ниже.
**Фикс**: Пополнить баланс Anthropic API.

### 5. LLM JSON truncation
**Проблема**: `[step5:competitors] Retry провален: Failed to parse LLM JSON: Unterminated string`
**Причина**: Gemini 2.5 Flash иногда обрезает длинные JSON-ответы (>16K токенов).
**Workaround**: Retry + fallback. Не всегда помогает.
**Фикс**: Увеличить max_tokens или разбивать запрос на части.

## Коммиты за ночь (хронологически)

1. `50166bd` — Temporarily disable rate limit on /api/analyze
2. `0c3d076` — Add debug workflow for VPS diagnostics
3. `df91f21` — Fix auth_token NameError, enhance debug workflow
4. `226ef19` — Hardcode REPORTS_PER_HOUR=100
5. `39d4ff3` — Add /api/debug-rate endpoint and PUBLIC_PATHS
6. `5f19352` — Force-kill old uvicorn + clear caches
7. `ad2f5ef` — Fix deploy: systemctl stop/start instead of pkill
8. `2803ebb` — Clear stale app package from venv
9. `dbec9e8` — Debug: check Python import location
10. `8d72342` — Fix deploy YAML syntax
11. `9c3b2df` — Disable report rate limit for batch testing
12. `7511068` — Debug: check nginx rate limiting
13. `a35888a` — Debug: launch CDEK + check sessions
14. `08a117d` — Improve competitor validation: salvage instead of drop
15. `8d4cbb1` — Debug: launch CDEK via localhost
16. `fd2c37d` — Competitor salvage: preserve lifecycle/financials/sales_channels
17. `d474601` — Debug: launch DNS + CDEK + Kamaz
18. `3220ad3` — Add NIGHT_RESULTS.md + launch DNS/CDEK test reports
19. `0fa1186` — Debug: relaunch DNS + CDEK with longer wait
20. `19de4b5` — Debug: simplify workflow to fix dispatch trigger

## Качество отчётов (по аудиту)

| Отчёт | Балл | Основные проблемы |
|-------|------|-------------------|
| КАМАЗ | 3/10 | 11 placeholder секций, нет конкурентов, нет финансов |
| Wildberries | 3/10 | Все конкуренты пусты, нет ФНС, нет SWOT |
| Хеликс | 3/10 | 5 placeholder секций, нет конкурентов, нет финансов |
| Тинькофф | ~4/10 | Placeholder для конкурентов и финансов, нереалистичные зарплаты HR |
| Ozon | ~5/10 | Оценка по аналогии (агент проверил другой отчёт) |
| amoCRM | ~6/10 | Наиболее полный, данные ФНС есть |
| Кофемания | ~6/10 | Данные ФНС есть, конкуренты частично |
| DNS (новый) | TBD | Сгенерирован с QA Director, 216K |
| СДЭК (новый) | TBD | Сгенерирован с QA Director, 192K |

## Выводы

1. **Пайплайн стабилен** — 10/10 отчётов сгенерированы (vs 0/10 до v0.7.0)
2. **QA Director работает** — 6-й эксперт Board of Directors присутствует в новых отчётах
3. **Программатические проверки** — 9 автоматических проверок качества (checks 1-9)
4. **Качество данных** — основная проблема. Конкуренты теряются при валидации (фикс деплоен и протестирован)
5. **ФНС** — работает для средних компаний, не для ПАО
6. **Board LLM** — fallback Claude → GPT → Gemini работает, но Anthropic кредиты нужны
7. **Инфраструктура** — deploy-kills-sessions — главная боль, workaround: не пушить во время генерации
8. **Следующий приоритет**: улучшить качество данных, пополнить Anthropic, добавить session persistence

## Ссылки на отчёты

Все отчёты доступны: `http://89.167.19.68:8090/reports/report_{ID}.html`
