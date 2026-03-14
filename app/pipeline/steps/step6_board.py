"""Step 6: Board of Directors — AI-экспертная панель рецензирует отчёт (T24).

После генерации отчёта 6 AI-экспертов (CFO, CMO, Industry Expert, Skeptic,
QA Director, CEO) независимо рецензируют его. Первые 5 работают параллельно,
затем CEO получает их результаты и формирует финальный вердикт.

Используется GPT-5.3 Codex через call_board_llm / call_board_llm_parallel (T28).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from app.pipeline.llm_client import call_board_llm, call_board_llm_parallel
from app.pipeline.release import add_blocking_issue, set_report_status

logger = logging.getLogger(__name__)

# ── Максимальный размер отчёта для промпта (символов) ──
_MAX_REPORT_CHARS = 80_000


# ── Определения экспертов ──

_EXPERT_CFO = {
    "role": "CFO",
    "name": "Финансовый директор",
    "system": (
        "Ты — опытный финансовый директор (CFO) с 20-летним стажем в аудиторских "
        "компаниях Big Four и корпоративных финансах. Твоя задача — рецензировать "
        "бизнес-аналитический отчёт с точки зрения финансовой достоверности.\n\n"
        "ФОКУС ПРОВЕРКИ:\n"
        "1. Финансовые показатели: выручка, прибыль, рентабельность, долговая нагрузка — "
        "соответствуют ли данным ФНС и ЕГРЮЛ?\n"
        "2. Расчёты (calc_trace): правильность формул, корректность входных данных, "
        "уровни достоверности (ФАКТ/РАСЧЁТ/ОЦЕНКА)\n"
        "3. Финансовые коэффициенты: ROE, EBITDA margin, Debt/Equity — посчитаны верно?\n"
        "4. Сравнение с конкурентами: корректна ли база сравнения? Учтена ли стадия "
        "жизненного цикла (инвестиционная фаза ≠ неэффективность)?\n"
        "5. KPI и бенчмарки: реалистичны ли целевые значения?\n"
        "6. Сценарии: адекватны ли допущения в оптимистичном/пессимистичном сценариях?\n\n"
        "ФОРМАТ ОТВЕТА — строго JSON:\n"
        "{\n"
        '  "approved": true/false,\n'
        '  "critiques": [\n'
        '    {"section": "название секции", "issue": "описание проблемы", '
        '"severity": "high/medium/low", "suggestion": "как исправить"}\n'
        "  ],\n"
        '  "summary": "краткий итог рецензии (2-3 предложения)"\n'
        "}\n\n"
        "Не одобряй отчёт (approved=false), если есть хотя бы одна ошибка severity=high.\n"
        "Отвечай ТОЛЬКО валидным JSON. Без markdown, без пояснений вне JSON."
    ),
    "focus_areas": [
        "financials", "calc_traces", "kpi_benchmarks",
        "scenarios", "market_share",
    ],
}

_EXPERT_CMO = {
    "role": "CMO",
    "name": "Директор по маркетингу",
    "system": (
        "Ты — директор по маркетингу (CMO) с опытом в digital-маркетинге, "
        "конкурентном анализе и позиционировании брендов на российском рынке. "
        "Твоя задача — рецензировать бизнес-аналитический отчёт с точки зрения "
        "маркетинга и конкурентной среды.\n\n"
        "ФОКУС ПРОВЕРКИ:\n"
        "1. Конкурентный анализ: найдены ли ВСЕ значимые конкуренты? Нет ли пропусков?\n"
        "2. Перцептуальная карта: обоснованы ли позиции? Есть ли критерии оценок?\n"
        "3. Digital-аудит: проверены ли соцсети (Instagram, Telegram, VK)? "
        "Актуальны ли данные о подписчиках?\n"
        "4. SWOT: реалистичны ли сильные/слабые стороны? Не упущены ли возможности?\n"
        "5. Каналы продаж: полная ли карта каналов для каждого конкурента?\n"
        "6. Рекомендации по маркетингу: конкретны ли они? Есть ли actionable шаги?\n"
        "7. Рынок: корректна ли оценка TAM/SAM/SOM? Актуальны ли тренды?\n\n"
        "ФОРМАТ ОТВЕТА — строго JSON:\n"
        "{\n"
        '  "approved": true/false,\n'
        '  "critiques": [\n'
        '    {"section": "название секции", "issue": "описание проблемы", '
        '"severity": "high/medium/low", "suggestion": "как исправить"}\n'
        "  ],\n"
        '  "summary": "краткий итог рецензии (2-3 предложения)"\n'
        "}\n\n"
        "Не одобряй отчёт (approved=false), если есть хотя бы одна ошибка severity=high.\n"
        "Отвечай ТОЛЬКО валидным JSON. Без markdown, без пояснений вне JSON."
    ),
    "focus_areas": [
        "competitors", "swot", "digital", "market",
        "recommendations", "radar_dimensions",
    ],
}

_EXPERT_INDUSTRY = {
    "role": "Industry Expert",
    "name": "Отраслевой эксперт",
    "system": (
        "Ты — ведущий отраслевой эксперт с глубоким знанием российского рынка. "
        "Ты знаешь специфику каждой отрасли: HoReCa, IT, ритейл, производство, "
        "косметика, медицина. Твоя задача — рецензировать отчёт с точки зрения "
        "отраслевой экспертизы.\n\n"
        "ФОКУС ПРОВЕРКИ:\n"
        "1. Тип бизнеса: правильно ли определён? Подходят ли метрики?\n"
        "2. Стадия жизненного цикла конкурентов: корректна ли оценка? "
        "Стартап/рост/инвестиционная фаза/зрелость — подтверждается данными?\n"
        "3. Отраслевые метрики: используются ли правильные KPI для данного типа? "
        "(RevPASH для ресторанов, ARR/MRR для SaaS, GMV для ритейла)\n"
        "4. Рыночные тренды: актуальны ли? Не устарели?\n"
        "5. Регуляторные риски: учтены ли отраслевые лицензии, сертификации?\n"
        "6. HR-данные: реалистичны ли зарплаты для отрасли? "
        "Учтён ли KPI (+15-20% к gross)?\n"
        "7. Фаундеры и владельцы: корректна ли информация по ЕГРЮЛ?\n\n"
        "ФОРМАТ ОТВЕТА — строго JSON:\n"
        "{\n"
        '  "approved": true/false,\n'
        '  "critiques": [\n'
        '    {"section": "название секции", "issue": "описание проблемы", '
        '"severity": "high/medium/low", "suggestion": "как исправить"}\n'
        "  ],\n"
        '  "summary": "краткий итог рецензии (2-3 предложения)"\n'
        "}\n\n"
        "Не одобряй отчёт (approved=false), если есть хотя бы одна ошибка severity=high.\n"
        "Отвечай ТОЛЬКО валидным JSON. Без markdown, без пояснений вне JSON."
    ),
    "focus_areas": [
        "market", "hr_data", "founders", "opinions",
        "regulatory_trends", "tech_trends",
    ],
}

_EXPERT_SKEPTIC = {
    "role": "Skeptic",
    "name": "Скептик",
    "system": (
        "Ты — профессиональный скептик и фактчекер. Ты НЕ доверяешь ничему на слово. "
        "Твоя задача — найти в отчёте:\n\n"
        "ФОКУС ПРОВЕРКИ:\n"
        "1. Галлюцинации: есть ли факты, которые выглядят выдуманными? "
        "Подозрительно точные числа без источника?\n"
        "2. Логические противоречия: один раздел утверждает X, другой — не-X?\n"
        "3. Необоснованные выводы: есть ли корреляция, выданная за причинность?\n"
        "4. Пропущенные допущения: какие неявные допущения сделаны в расчётах?\n"
        "5. Уровни достоверности: есть ли ОЦЕНКИ (⚠), выданные за ФАКТЫ (🔒)?\n"
        "6. Источники: все ли факты подтверждены минимум 2 источниками? "
        "Есть ли опора только на самоотчёт (уровень E)?\n"
        "7. Устаревшие данные: не старше ли данные 2 лет? "
        "Актуальны ли ссылки на конкурентов (не закрылись ли они)?\n"
        "8. Чёрный список: нет ли запрещённых секций "
        "(Price vs Rating, корреляция при N<15, динамика без сравнения)?\n\n"
        "БУДЬ МАКСИМАЛЬНО ПРИДИРЧИВ. Лучше перебдеть, чем недобдеть.\n\n"
        "ФОРМАТ ОТВЕТА — строго JSON:\n"
        "{\n"
        '  "approved": true/false,\n'
        '  "critiques": [\n'
        '    {"section": "название секции", "issue": "описание проблемы", '
        '"severity": "high/medium/low", "suggestion": "как исправить"}\n'
        "  ],\n"
        '  "summary": "краткий итог рецензии (2-3 предложения)"\n'
        "}\n\n"
        "Ставь approved=true ТОЛЬКО если отчёт безупречен. В 90% случаев — false.\n"
        "Отвечай ТОЛЬКО валидным JSON. Без markdown, без пояснений вне JSON."
    ),
    "focus_areas": [
        "factcheck", "calc_traces", "sources",
        "methodology", "section_gates",
    ],
}

_EXPERT_QA_DIRECTOR = {
    "role": "QA Director",
    "name": "Директор по качеству",
    "system": (
        "Ты — директор по качеству данных (QA Director) с опытом в data governance, "
        "контроле качества аналитических отчётов и аудите бизнес-документации. "
        "Твоя задача — проверить отчёт по 4 критериям качества.\n\n"
        "ФОКУС ПРОВЕРКИ — 4 КРИТЕРИЯ:\n\n"
        "1. ЧЕЛОВЕКОЧИТАЕМОСТЬ (readability):\n"
        "   - Числа форматированы с разделителями тысяч (1 234 567, не 1234567)\n"
        "   - Нет JSON-дампов или технических артефактов в текстовых полях\n"
        "   - Весь контент на русском языке (кроме названий брендов)\n"
        "   - Нет placeholder'ов (TODO, FIXME, Lorem ipsum, undefined, {{шаблон}})\n"
        "   - Нет обрезанных строк (заканчиваются на '...' или < 20 символов)\n\n"
        "2. ОТСУТСТВИЕ ПУСТЫХ ПОЛЕЙ (empty_fields):\n"
        "   - SWOT: все 4 квадранта (strengths, weaknesses, opportunities, threats) заполнены\n"
        "   - У каждого конкурента есть description и ключевые metrics\n"
        "   - KPI: и current, и benchmark заполнены (не пустые)\n"
        "   - Финансы: нет строк где все значения null\n"
        "   - Market: market_size заполнен\n"
        "   - Рекомендации: каждая имеет description\n"
        "   - Глоссарий: минимум 3 термина\n\n"
        "3. ЕДИНООБРАЗИЕ ЕДИНИЦ (units):\n"
        "   - Выручка во всех секциях в одних единицах (тыс. руб. или млн руб. — не mix)\n"
        "   - Проценты — числовые значения (не '~10%' или 'около 15')\n"
        "   - Radar scores все в диапазоне [0, 10]\n"
        "   - При сравнении конкурентов — одинаковые периоды/года\n"
        "   - Market share в сумме ~100% (допустимо 90-110%)\n\n"
        "4. ВНУТРЕННЯЯ СОГЛАСОВАННОСТЬ (consistency):\n"
        "   - Размер рынка одинаковый в market и в scenarios и в kpi\n"
        "   - Выручка из financials совпадает (±5%) с выручкой в scenarios и kpi_benchmarks\n"
        "   - Количество сотрудников: financials = hr_data\n"
        "   - Названия конкурентов одинаковы в competitors[], market_share, factcheck\n"
        "   - Нет противоречий между секциями\n\n"
        "ФОРМАТ ОТВЕТА — строго JSON:\n"
        "{\n"
        '  "approved": true/false,\n'
        '  "critiques": [\n'
        '    {"section": "название секции", "issue": "описание проблемы", '
        '"severity": "high/medium/low", "suggestion": "как исправить", '
        '"criteria": "readability|empty_fields|units|consistency"}\n'
        "  ],\n"
        '  "summary": "краткий итог рецензии (2-3 предложения)"\n'
        "}\n\n"
        "ВАЖНО: в каждом critique ОБЯЗАТЕЛЬНО указывай поле 'criteria' — "
        "одно из: readability, empty_fields, units, consistency.\n"
        "Не одобряй отчёт (approved=false), если есть хотя бы одна ошибка severity=high.\n"
        "Отвечай ТОЛЬКО валидным JSON. Без markdown, без пояснений вне JSON."
    ),
    "focus_areas": [
        "readability", "empty_fields", "units", "consistency",
        "formatting", "completeness",
    ],
}

_EXPERT_CEO = {
    "role": "CEO",
    "name": "Генеральный директор",
    "system": (
        "Ты — генеральный директор (CEO), который принимает решения на основе данных. "
        "Перед тобой бизнес-аналитический отчёт И рецензии пяти других экспертов:\n"
        "- CFO (финансы)\n"
        "- CMO (маркетинг и конкуренты)\n"
        "- Отраслевой эксперт\n"
        "- Скептик (фактчек)\n"
        "- QA Director (качество: читаемость, пустые поля, единообразие, согласованность)\n\n"
        "Твоя задача — СИНТЕЗИРОВАТЬ их рецензии и принять решение:\n\n"
        "1. Какие замечания ПРИНЯТЬ (обоснованные, критичные)?\n"
        "2. Какие ОТКЛОНИТЬ (необоснованные, мелочные, субъективные)?\n"
        "3. Готов ли отчёт к публикации или нужны доработки?\n\n"
        "КРИТЕРИИ РЕШЕНИЯ:\n"
        "- Все severity=high замечания, подтверждённые 2+ экспертами → ПРИНЯТЬ\n"
        "- severity=high от одного эксперта → ПРОВЕРИТЬ, принять если обоснованно\n"
        "- severity=medium → ПРИНЯТЬ если actionable и улучшает качество\n"
        "- severity=low → НА УСМОТРЕНИЕ, не блокировать публикацию\n\n"
        "ФОРМАТ ОТВЕТА — строго JSON:\n"
        "{\n"
        '  "approved": true/false,\n'
        '  "critiques": [\n'
        '    {"section": "название секции", "issue": "описание проблемы", '
        '"severity": "high/medium/low", "suggestion": "как исправить", '
        '"source_experts": ["CFO", "Skeptic"]}\n'
        "  ],\n"
        '  "accepted_critiques": [номера принятых замечаний из рецензий экспертов],\n'
        '  "rejected_critiques": [\n'
        '    {"expert": "CMO", "issue_summary": "...", "reason": "почему отклонено"}\n'
        "  ],\n"
        '  "summary": "финальный вердикт CEO (3-5 предложений)"\n'
        "}\n\n"
        "Не одобряй отчёт, если есть неисправленные замечания severity=high.\n"
        "Отвечай ТОЛЬКО валидным JSON. Без markdown, без пояснений вне JSON."
    ),
    "focus_areas": [
        "recommendations", "scenarios", "implementation_timeline",
        "overall_quality",
    ],
}


# ── Утилиты ──


def _truncate_report(report_data: dict, max_chars: int = _MAX_REPORT_CHARS) -> str:
    """Сериализовать отчёт в JSON и обрезать если слишком длинный.

    Приоритет: сначала убираем opinions, потом founders, потом factcheck,
    потом полный текст конкурентов — чтобы сохранить финансы и метрики.
    """
    full_json = json.dumps(report_data, ensure_ascii=False, default=str)

    if len(full_json) <= max_chars:
        return full_json

    # Постепенно убираем тяжёлые секции
    trimmed = dict(report_data)
    heavy_keys = [
        "opinions", "founders", "factcheck", "digital_verification",
        "glossary", "methodology", "products", "menu", "tenders",
    ]

    for key in heavy_keys:
        if key in trimmed:
            items = trimmed[key]
            if isinstance(items, list) and len(items) > 5:
                trimmed[key] = items[:3]
                trimmed[f"_{key}_note"] = (
                    f"Обрезано для рецензии: показано 3 из {len(items)} записей"
                )
            elif isinstance(items, dict) and len(str(items)) > 2000:
                trimmed[key] = {"_note": "Секция сокращена для рецензии"}

        full_json = json.dumps(trimmed, ensure_ascii=False, default=str)
        if len(full_json) <= max_chars:
            return full_json

    # Если всё ещё слишком большой — обрезаем жёстко
    if len(full_json) > max_chars:
        logger.warning(
            "Отчёт слишком большой даже после обрезки секций: %d > %d символов. "
            "Обрезаем до лимита.",
            len(full_json), max_chars,
        )
        return full_json[:max_chars] + '\n... [отчёт обрезан для рецензии]"}'

    return full_json


def _parse_expert_response(raw: str, expert_role: str) -> dict:
    """Парсим JSON-ответ эксперта. Устойчив к markdown-обёрткам."""
    import re

    text = raw.strip()

    # Убираем markdown-обёртки если есть
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(
            "Не удалось разобрать JSON от эксперта %s: %s\nRaw: %s",
            expert_role, e, text[:500],
        )
        # Возвращаем аварийную структуру
        return {
            "approved": False,
            "critiques": [{
                "section": "parse_error",
                "issue": f"Эксперт {expert_role} вернул невалидный JSON",
                "severity": "low",
                "suggestion": "Повторить запрос к эксперту",
            }],
            "summary": f"Ошибка парсинга ответа эксперта {expert_role}: {str(e)[:200]}",
            "_parse_error": True,
        }

    # Валидация структуры
    if "approved" not in parsed:
        parsed["approved"] = False
    if "critiques" not in parsed:
        parsed["critiques"] = []
    if "summary" not in parsed:
        parsed["summary"] = f"Эксперт {expert_role} не предоставил итог."

    # Нормализация severity
    for critique in parsed.get("critiques", []):
        sev = critique.get("severity", "medium").lower()
        if sev not in ("high", "medium", "low"):
            critique["severity"] = "medium"
        else:
            critique["severity"] = sev

    return parsed


# ── Основные функции ──


def _pre_scan_quality(report_data: dict) -> str:
    """Лёгкая версия программатических проверок для QA Director.

    Проверяет пустые поля и кросс-секционную согласованность.
    Результат добавляется в промпт QA Director для усиления его фокуса.
    """
    issues: list[str] = []

    # ── Пустые поля ──
    swot = report_data.get("swot") or {}
    for quad in ("strengths", "weaknesses", "opportunities", "threats"):
        items = swot.get(quad) or []
        if not items:
            issues.append(f"SWOT: квадрант '{quad}' пуст")

    competitors = report_data.get("competitors") or []
    for i, c in enumerate(competitors):
        if not isinstance(c, dict):
            continue
        if not c.get("description"):
            issues.append(f"Конкурент #{i+1} ({c.get('name', '?')}): нет description")

    kpi = report_data.get("kpi_benchmarks") or []
    for k in kpi:
        if not isinstance(k, dict):
            continue
        if k.get("current") is None and k.get("benchmark") is None:
            issues.append(f"KPI '{k.get('name', '?')}': нет current и benchmark")

    glossary = report_data.get("glossary") or []
    if len(glossary) < 3:
        issues.append(f"Глоссарий: {len(glossary)} терминов (минимум 3)")

    market = report_data.get("market") or {}
    if not market.get("market_size"):
        issues.append("Market: market_size не заполнен")

    recommendations = report_data.get("recommendations") or []
    for i, rec in enumerate(recommendations):
        if isinstance(rec, dict) and not rec.get("description"):
            issues.append(f"Рекомендация #{i+1}: нет description")

    # ── Кросс-секционная согласованность ──
    financials = report_data.get("financials") or []
    fns_revenue = None
    for fy in reversed(financials):
        if isinstance(fy, dict) and fy.get("revenue") is not None:
            try:
                fns_revenue = float(fy["revenue"])
            except (ValueError, TypeError):
                pass
            break

    # Проверка названий конкурентов в market_share
    comp_names = {c.get("name", "").strip().lower() for c in competitors if isinstance(c, dict) and c.get("name")}
    market_share = report_data.get("market_share") or {}
    share_names = {n.strip().lower() for n in market_share.keys() if n}
    if comp_names and share_names:
        missing_in_share = comp_names - share_names
        company_name = (report_data.get("company") or {}).get("name", "").strip().lower()
        missing_in_share.discard(company_name)
        if missing_in_share and len(missing_in_share) > len(comp_names) * 0.5:
            issues.append(
                f"Несогласованность: {len(missing_in_share)} конкурентов из competitors[] "
                "отсутствуют в market_share"
            )

    # Radar scores вне [0, 10]
    radar_dims = report_data.get("radar_dimensions") or []
    if radar_dims:
        for c in competitors:
            if not isinstance(c, dict):
                continue
            scores = c.get("radar_scores") or {}
            for dim, val in scores.items():
                try:
                    v = float(val)
                    if v < 0 or v > 10:
                        issues.append(
                            f"Radar score вне [0,10]: {c.get('name','?')}.{dim}={v}"
                        )
                except (ValueError, TypeError):
                    pass

    if not issues:
        return ""

    lines = [f"- {iss}" for iss in issues[:15]]
    return (
        "\n\n=== ПРЕДВАРИТЕЛЬНОЕ СКАНИРОВАНИЕ (автоматика) ===\n"
        f"Найдено проблем: {len(issues)}\n"
        + "\n".join(lines) +
        "\n=== КОНЕЦ СКАНИРОВАНИЯ ===\n"
        "Проверь эти проблемы и найди то, что автоматика могла пропустить."
    )


def form_panel(report_data: dict, company_info: dict) -> list[dict]:
    """Формирует панель из 5 AI-экспертов для рецензирования отчёта.

    Args:
        report_data: собранный отчёт (dict или ReportData.model_dump())
        company_info: базовая информация о компании (name, business_type, inn)

    Returns:
        Список из 5 экспертов: [{"role", "name", "system", "focus_areas"}, ...]
    """
    business_type = company_info.get("business_type", "B2C_SERVICE")
    company_name = company_info.get("name", "Компания")

    # Дополнительный контекст для каждого эксперта
    context_suffix = (
        f"\n\nКОНТЕКСТ: анализируемая компания — «{company_name}», "
        f"тип бизнеса: {business_type}. "
        "Учитывай специфику этого типа при рецензировании."
    )

    panel = []
    for expert_template in [
        _EXPERT_CFO, _EXPERT_CMO, _EXPERT_INDUSTRY, _EXPERT_SKEPTIC,
        _EXPERT_QA_DIRECTOR, _EXPERT_CEO,
    ]:
        expert = dict(expert_template)
        expert["system"] = expert["system"] + context_suffix
        panel.append(expert)

    logger.info(
        "Панель экспертов сформирована: %s",
        ", ".join(e["role"] for e in panel),
    )
    return panel


def run_review(report_data: dict, panel: list[dict]) -> dict:
    """Запускает рецензирование отчёта панелью экспертов.

    Порядок:
    1. CFO, CMO, Industry Expert, Skeptic — параллельно
    2. CEO — отдельно, получает результаты первых четырёх

    Args:
        report_data: собранный отчёт (dict)
        panel: список экспертов от form_panel()

    Returns:
        {
            "reviews": [{"role": ..., "name": ..., "response": {...}}, ...],
            "consensus": {
                "approved": bool,
                "critical_issues": int,
                "total_critiques": int
            },
            "needs_revision": bool
        }
    """
    t0 = time.monotonic()

    # Подготовка текста отчёта
    report_json = _truncate_report(report_data)
    logger.info(
        "Отчёт подготовлен для рецензии: %d символов", len(report_json),
    )

    # Разделяем экспертов: первые 4 (параллельно) и CEO (после)
    parallel_experts = [e for e in panel if e["role"] != "CEO"]
    ceo_expert = next((e for e in panel if e["role"] == "CEO"), None)

    if not ceo_expert:
        logger.error("CEO не найден в панели экспертов!")
        raise ValueError("Панель экспертов должна содержать CEO")

    # ── Pre-scan для QA Director ──
    pre_scan_text = _pre_scan_quality(report_data)
    if pre_scan_text:
        logger.info("Pre-scan нашёл проблемы для QA Director")

    # ── Шаг 1: параллельный запуск 5 экспертов ──
    logger.info("Запуск параллельной рецензии: %d экспертов", len(parallel_experts))

    parallel_prompts = []
    for expert in parallel_experts:
        # Для QA Director добавляем результат pre-scan
        extra_context = ""
        if expert["role"] == "QA Director" and pre_scan_text:
            extra_context = pre_scan_text

        prompt = (
            f"Ты — {expert['name']} ({expert['role']}). "
            f"Рецензируй бизнес-аналитический отчёт.\n\n"
            f"Твои области фокуса: {', '.join(expert['focus_areas'])}\n\n"
            f"=== ОТЧЁТ ===\n{report_json}\n=== КОНЕЦ ОТЧЁТА ==={extra_context}\n\n"
            "Дай структурированную рецензию в формате JSON."
        )
        parallel_prompts.append({
            "prompt": prompt,
            "system": expert["system"],
        })

    parallel_responses = call_board_llm_parallel(parallel_prompts)

    # Парсим ответы первых 5 экспертов
    reviews: list[dict] = []
    for expert, raw_response in zip(parallel_experts, parallel_responses):
        is_error = raw_response.startswith("[Board LLM Error]")
        if is_error:
            logger.error(
                "Эксперт %s вернул ошибку: %s", expert["role"], raw_response[:200],
            )
            parsed = {
                "approved": False,
                "critiques": [{
                    "section": "llm_error",
                    "issue": f"Эксперт {expert['role']} недоступен: {raw_response[:300]}",
                    "severity": "low",
                    "suggestion": "Повторить запрос позже",
                }],
                "summary": f"Эксперт {expert['role']} не смог завершить рецензию.",
                "_llm_error": True,
            }
        else:
            parsed = _parse_expert_response(raw_response, expert["role"])

        reviews.append({
            "role": expert["role"],
            "name": expert["name"],
            "response": parsed,
        })

    elapsed_parallel = round(time.monotonic() - t0, 2)
    logger.info(
        "Параллельная рецензия завершена за %.2fs. Результаты: %s",
        elapsed_parallel,
        {r["role"]: r["response"].get("approved", "?") for r in reviews},
    )

    # ── Шаг 2: CEO получает результаты первых 5 экспертов ──
    logger.info("Запуск рецензии CEO с результатами экспертов")

    expert_summaries = []
    for review in reviews:
        resp = review["response"]
        critiques_text = ""
        for i, c in enumerate(resp.get("critiques", []), 1):
            critiques_text += (
                f"  {i}. [{c.get('severity', '?').upper()}] "
                f"Секция: {c.get('section', '?')} — {c.get('issue', '?')}\n"
                f"     Рекомендация: {c.get('suggestion', 'нет')}\n"
            )
        expert_summaries.append(
            f"### {review['name']} ({review['role']})\n"
            f"Вердикт: {'ОДОБРЕНО' if resp.get('approved') else 'НЕ ОДОБРЕНО'}\n"
            f"Итог: {resp.get('summary', 'нет итога')}\n"
            f"Замечания:\n{critiques_text or '  (нет замечаний)'}\n"
        )

    ceo_prompt = (
        f"Ты — {ceo_expert['name']} ({ceo_expert['role']}). "
        "Перед тобой бизнес-аналитический отчёт и рецензии пяти экспертов.\n\n"
        f"=== ОТЧЁТ (сокращённо) ===\n{report_json[:40000]}\n"
        "=== КОНЕЦ ОТЧЁТА ===\n\n"
        "=== РЕЦЕНЗИИ ЭКСПЕРТОВ ===\n"
        + "\n".join(expert_summaries) +
        "\n=== КОНЕЦ РЕЦЕНЗИЙ ===\n\n"
        "Синтезируй рецензии. Реши, какие замечания принять, "
        "какие отклонить. Дай финальный вердикт в формате JSON."
    )

    t1 = time.monotonic()
    ceo_raw = call_board_llm(prompt=ceo_prompt, system=ceo_expert["system"])
    elapsed_ceo = round(time.monotonic() - t1, 2)

    if ceo_raw.startswith("[Board LLM Error]"):
        logger.error("CEO вернул ошибку: %s", ceo_raw[:200])
        ceo_parsed = {
            "approved": False,
            "critiques": [],
            "accepted_critiques": [],
            "rejected_critiques": [],
            "summary": f"CEO недоступен: {ceo_raw[:300]}",
            "_llm_error": True,
        }
    else:
        ceo_parsed = _parse_expert_response(ceo_raw, "CEO")

    reviews.append({
        "role": ceo_expert["role"],
        "name": ceo_expert["name"],
        "response": ceo_parsed,
    })

    logger.info("Рецензия CEO завершена за %.2fs. Вердикт: %s",
                elapsed_ceo, ceo_parsed.get("approved", "?"))

    # ── Агрегация результатов ──
    all_critiques = []
    for review in reviews:
        for critique in review["response"].get("critiques", []):
            critique["from_expert"] = review["role"]
            all_critiques.append(critique)

    critical_issues = sum(
        1 for c in all_critiques if c.get("severity") == "high"
    )
    total_critiques = len(all_critiques)
    # Одобрение: CEO одобрил И нет critical issues
    ceo_approved = ceo_parsed.get("approved", False)
    consensus_approved = ceo_approved and critical_issues == 0

    elapsed_total = round(time.monotonic() - t0, 2)
    logger.info(
        "Рецензирование завершено за %.2fs. Консенсус: approved=%s, "
        "critical=%d, total_critiques=%d",
        elapsed_total, consensus_approved, critical_issues, total_critiques,
    )

    return {
        "reviews": reviews,
        "consensus": {
            "approved": consensus_approved,
            "critical_issues": critical_issues,
            "total_critiques": total_critiques,
        },
        "needs_revision": not consensus_approved,
        "timing": {
            "parallel_sec": elapsed_parallel,
            "ceo_sec": elapsed_ceo,
            "total_sec": elapsed_total,
        },
    }


def apply_revisions(report_data: dict, reviews: dict) -> dict:
    """Применяет результаты рецензирования к отчёту (T26).

    1. Форматирует board_review для шаблона b1_board_conclusion.html
    2. Применяет авто-правки по high-severity замечаниям:
       - Обновляет section_gates для отключения ненадёжных секций
       - Добавляет blocking issues и open questions
       - Переводит отчёт в draft, если совет не одобрил публикацию

    Args:
        report_data: собранный отчёт
        reviews: результат run_review()

    Returns:
        Обновлённый report_data с ключом "board_review" и применёнными правками
    """
    consensus = reviews.get("consensus", {})

    # ── 1. Форматируем reviews для шаблона ──
    # Шаблон ожидает: review.role, review.name, review.approved, review.summary, review.critiques
    template_reviews = []
    for review in reviews.get("reviews", []):
        resp = review.get("response", {})
        template_reviews.append({
            "role": review.get("role", "Unknown"),
            "name": review.get("name", review.get("role", "Unknown")),
            "approved": resp.get("approved", False),
            "summary": resp.get("summary", ""),
            "critiques": resp.get("critiques", []),
        })

    board_review = {
        "reviews": template_reviews,
        "consensus": {
            "approved": consensus.get("approved", False),
            "critical_issues": consensus.get("critical_issues", 0),
            "total_critiques": consensus.get("total_critiques", 0),
        },
        "timing": reviews.get("timing", {}),
    }

    report_data["board_review"] = board_review

    # ── 2. Собираем все замечания для авто-правок ──
    all_critiques = []
    for review in template_reviews:
        for critique in review.get("critiques", []):
            critique_with_expert = dict(critique)
            critique_with_expert["from_expert"] = review["role"]
            all_critiques.append(critique_with_expert)

    high_critiques = [c for c in all_critiques if c.get("severity") == "high"]

    if not consensus.get("approved", False):
        add_blocking_issue(
            report_data,
            f"Совет директоров не одобрил отчёт ({consensus.get('critical_issues', 0)} критических замечаний)",
        )
        set_report_status(report_data, "draft")
    else:
        set_report_status(report_data, "publishable")

    # ── 3. Авто-правки по high-severity замечаниям ──
    section_gates = report_data.get("section_gates", {})
    board_warnings = []

    # Маппинг секций из замечаний к block IDs
    _SECTION_TO_BLOCKS = {
        "финанс": ["P2"], "financial": ["P2"],
        "swot": ["P3"], "digital": ["P4"],
        "конкурент": ["C1", "C2", "C3", "C4"], "competitor": ["C1", "C2", "C3", "C4"],
        "рынок": ["M1"], "market": ["M1"],
        "рекоменда": ["S1"], "recommend": ["S1"],
        "kpi": ["S2"], "бенчмарк": ["S2"], "benchmark": ["S2"],
        "сценар": ["S3"], "scenario": ["S3"],
        "корреляц": ["S4"], "correlation": ["S4"],
        "opinion": ["O2"], "мнени": ["O2"],
        "founder": ["O1"], "фаундер": ["O1"], "владел": ["O1"],
        "hr": ["M4"], "кадр": ["M4"],
        "доля рынка": ["P10"], "market_share": ["P10"],
    }

    for critique in high_critiques:
        section_name = (critique.get("section", "") or "").lower()
        issue = (critique.get("issue", "") or "").lower()

        # High-severity замечания должны убирать сомнительные секции из публичного отчёта.
        is_untrusted = any(kw in issue for kw in [
            "галлюцина", "выдуман", "фейк", "не существ", "hallucin",
            "fabricat", "несуществующ", "вымышлен",
            "unverified", "невериф", "без источ", "без подтвержд",
        ])

        matched_blocks = []
        for key, blocks in _SECTION_TO_BLOCKS.items():
            if key in section_name or key in issue:
                matched_blocks.extend(blocks)

        if matched_blocks:
            for block_id in matched_blocks:
                section_gates[block_id] = False
                logger.warning(
                    "Board: гейтим блок %s по замечанию '%s' от %s",
                    block_id, critique.get("issue", "")[:80], critique.get("from_expert", "?"),
                )

        if is_untrusted:
            add_blocking_issue(
                report_data,
                f"{critique.get('from_expert', 'Board')}: {critique.get('issue', '')}".strip(),
            )

        # Добавляем warning
        board_warnings.append({
            "section": critique.get("section", ""),
            "issue": critique.get("issue", ""),
            "expert": critique.get("from_expert", ""),
            "suggestion": critique.get("suggestion", ""),
        })

    report_data["section_gates"] = section_gates
    report_data["failed_gates"] = sorted({k for k, v in section_gates.items() if not v})

    # ── 4. Добавляем board warnings к open_questions ──
    if board_warnings:
        open_questions = report_data.get("open_questions", [])
        for w in board_warnings[:5]:  # Максимум 5 вопросов от борда
            question = (
                f"[Совет директоров / {w['expert']}] {w['issue']}"
            )
            if question not in open_questions:
                open_questions.append(question)
            suggestion = (w.get("suggestion") or "").strip()
            if suggestion:
                issue_text = f"Нужно действие: {suggestion}"
                if issue_text not in open_questions:
                    open_questions.append(issue_text)
        report_data["open_questions"] = open_questions

    logger.info(
        "Board review применён: approved=%s, critiques=%d (high=%d), "
        "gated_blocks=%d, warnings_added=%d",
        consensus.get("approved", False),
        len(all_critiques),
        len(high_critiques),
        sum(1 for v in section_gates.values() if not v),
        len(board_warnings),
    )

    return report_data
