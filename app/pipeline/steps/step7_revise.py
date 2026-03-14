"""Step 7: Revision — применяет замечания совета директоров к данным отчёта.

Feedback loop:
1. Собирает high/medium замечания от борда
2. Группирует по секциям report_data
3. Для каждой затронутой секции: отправляет данные + критику в LLM
4. LLM возвращает исправленные данные
5. Заменяет секцию в report_data
6. Применяет программатические фиксы (форматирование, нормализация)
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from app.pipeline.llm_client import call_board_llm

logger = logging.getLogger(__name__)

# ── Маппинг секций из критик борда → ключи report_data ──

_CRITIQUE_TO_KEYS: dict[str, list[str]] = {
    # Финансы
    "финанс": ["financials"],
    "financial": ["financials"],
    "выручк": ["financials"],
    "revenue": ["financials"],
    "прибыл": ["financials"],
    "рентаб": ["financials"],
    # SWOT
    "swot": ["swot"],
    # Конкуренты
    "конкурент": ["competitors"],
    "competitor": ["competitors"],
    "перцепту": ["competitors"],
    "радар": ["competitors", "radar_dimensions"],
    # Рынок
    "рынок": ["market"],
    "market": ["market"],
    "tam": ["market"],
    "sam": ["market"],
    # Digital
    "digital": ["digital"],
    "цифров": ["digital"],
    "соцсет": ["digital"],
    "instagram": ["digital"],
    "telegram": ["digital"],
    # HR
    "hr": ["hr_data"],
    "кадр": ["hr_data"],
    "зарплат": ["hr_data"],
    "вакан": ["hr_data"],
    # KPI
    "kpi": ["kpi_benchmarks"],
    "бенчмарк": ["kpi_benchmarks"],
    "benchmark": ["kpi_benchmarks"],
    # Рекомендации
    "рекоменда": ["recommendations"],
    "recommend": ["recommendations"],
    # Сценарии
    "сценар": ["scenarios"],
    "scenario": ["scenarios"],
    # Глоссарий
    "глоссарий": ["glossary"],
    "glossary": ["glossary"],
    # Фаундеры/мнения
    "фаундер": ["founders"],
    "founder": ["founders"],
    "владел": ["founders"],
    "мнени": ["opinions"],
    "opinion": ["opinions"],
    "цитат": ["opinions"],
    # Доля рынка
    "доля рынка": ["market_share"],
    "market_share": ["market_share"],
    # Методология
    "методолог": ["methodology"],
    # Фактчек
    "фактчек": ["factcheck"],
    "factcheck": ["factcheck"],
    # Продукты
    "продукт": ["products"],
    "услуг": ["products"],
}


def _map_critique_to_keys(critique: dict) -> set[str]:
    """Определяет какие ключи report_data затронуты критикой."""
    section = (critique.get("section") or "").lower()
    issue = (critique.get("issue") or "").lower()
    suggestion = (critique.get("suggestion") or "").lower()
    combined = f"{section} {issue} {suggestion}"

    keys: set[str] = set()
    for keyword, data_keys in _CRITIQUE_TO_KEYS.items():
        if keyword in combined:
            keys.update(data_keys)

    return keys


def _collect_actionable_critiques(board_review: dict) -> list[dict]:
    """Собирает high и medium замечания от всех экспертов."""
    critiques = []
    for review in board_review.get("reviews", []):
        resp = review.get("response", review)
        for critique in resp.get("critiques", []):
            sev = critique.get("severity", "low").lower()
            if sev in ("high", "medium"):
                critique_copy = dict(critique)
                critique_copy["from_expert"] = review.get("role", "Unknown")
                critiques.append(critique_copy)
    return critiques


def _build_fix_prompt(
    key: str,
    section_data: Any,
    critiques: list[dict],
    company_info: dict,
) -> str:
    """Строит промпт для LLM: исправь секцию по замечаниям."""
    critiques_text = "\n".join(
        f"- [{c.get('severity', '?').upper()}] {c.get('from_expert', '?')}: "
        f"{c.get('issue', '?')} → {c.get('suggestion', 'нет рекомендации')}"
        for c in critiques
    )

    section_json = json.dumps(section_data, ensure_ascii=False, default=str)
    # Ограничиваем размер секции
    if len(section_json) > 30000:
        section_json = section_json[:30000] + "\n... [обрезано]"

    company_name = company_info.get("name", "Компания")
    business_type = company_info.get("business_type", "B2C_SERVICE")

    return (
        f"Ты — редактор данных бизнес-аналитического отчёта.\n"
        f"Компания: {company_name}, тип: {business_type}\n\n"
        f"Секция отчёта: {key}\n"
        f"Текущие данные (JSON):\n{section_json}\n\n"
        f"Замечания экспертов совета директоров:\n{critiques_text}\n\n"
        f"ЗАДАЧА: исправь ВСЕ замечания и верни ОБНОВЛЁННЫЕ данные.\n\n"
        f"ПРАВИЛА:\n"
        f"- Верни ТОЛЬКО валидный JSON — ту же структуру что на входе, но исправленную\n"
        f"- Если данные отсутствуют — добавь реалистичную оценку с пометкой '⚠ Оценка'\n"
        f"- Числа должны быть реалистичными для отрасли ({business_type})\n"
        f"- Текст на русском языке\n"
        f"- Нет placeholder'ов (TODO, N/A, undefined, Lorem)\n"
        f"- Нет JSON-фрагментов в текстовых полях\n"
        f"- Числа с разделителями тысяч (1 234 567)\n"
        f"- Если замечание о пустых полях — заполни их\n"
        f"- Если замечание о несогласованности — приведи к единой версии\n"
        f"- Сохрани ВСЮ существующую корректную информацию\n\n"
        f"Отвечай ТОЛЬКО JSON. Без markdown, без ```json, без пояснений."
    )


def _parse_fix_response(raw: str, key: str) -> Any | None:
    """Парсит JSON-ответ LLM с исправленными данными."""
    text = raw.strip()

    # Убираем markdown-обёртки
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(
            "Не удалось разобрать исправления для секции %s: %s\nRaw: %s",
            key, e, text[:300],
        )
        return None


# ── Программатические фиксы ──


def _fix_readability(report_data: dict) -> int:
    """Убирает JSON-фрагменты и placeholder'ы из текстовых полей."""
    fixes = 0

    # Паттерны проблем
    json_re = re.compile(r'\{"[^"]+":')
    placeholder_re = re.compile(
        r'\b(TODO|FIXME|Lorem ipsum|placeholder|undefined|N/A)\b',
        re.IGNORECASE,
    )
    template_re = re.compile(r'\{\{[^}]+\}\}')

    def clean_text(val: Any) -> tuple[Any, bool]:
        if not isinstance(val, str):
            return val, False
        changed = False
        # JSON fragments
        if json_re.search(val):
            # Try to extract meaningful text
            cleaned = re.sub(r'\{[^}]*\}', '', val).strip()
            if cleaned and len(cleaned) > 10:
                val = cleaned
                changed = True
        # Placeholders
        if placeholder_re.search(val):
            val = placeholder_re.sub('', val).strip()
            changed = True
        # Template variables
        if template_re.search(val):
            val = template_re.sub('', val).strip()
            changed = True
        return val, changed

    def walk_and_clean(obj: Any) -> Any:
        nonlocal fixes
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, str):
                    cleaned, changed = clean_text(v)
                    if changed:
                        obj[k] = cleaned
                        fixes += 1
                elif isinstance(v, (dict, list)):
                    walk_and_clean(v)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, str):
                    cleaned, changed = clean_text(item)
                    if changed:
                        obj[i] = cleaned
                        fixes += 1
                elif isinstance(item, (dict, list)):
                    walk_and_clean(item)
        return obj

    walk_and_clean(report_data)
    return fixes


def _fix_radar_scores(report_data: dict) -> int:
    """Нормализует radar scores в [0, 10]."""
    fixes = 0
    for comp in report_data.get("competitors") or []:
        if not isinstance(comp, dict):
            continue
        scores = comp.get("radar_scores") or {}
        for dim, val in list(scores.items()):
            try:
                v = float(val)
                if v < 0:
                    scores[dim] = 0.0
                    fixes += 1
                elif v > 10:
                    scores[dim] = 10.0
                    fixes += 1
            except (ValueError, TypeError):
                scores[dim] = 5.0
                fixes += 1
    return fixes


def _fix_empty_swot(report_data: dict) -> int:
    """Заполняет пустые квадранты SWOT дефолтными значениями."""
    swot = report_data.get("swot")
    if not swot or not isinstance(swot, dict):
        return 0

    fixes = 0
    company_name = (report_data.get("company") or {}).get("name", "Компания")
    defaults = {
        "strengths": [f"Присутствие {company_name} на рынке"],
        "weaknesses": ["Требуется дополнительный анализ слабых сторон"],
        "opportunities": ["Потенциал роста в цифровых каналах"],
        "threats": ["Конкурентное давление на рынке"],
    }
    for quad, default in defaults.items():
        items = swot.get(quad)
        if not items or (isinstance(items, list) and len(items) == 0):
            swot[quad] = default
            fixes += 1

    return fixes


def _fix_market_share_sum(report_data: dict) -> int:
    """Нормализует market_share чтобы сумма была ~100%."""
    ms = report_data.get("market_share")
    if not ms or not isinstance(ms, dict):
        return 0

    total = sum(v for v in ms.values() if isinstance(v, (int, float)))
    if total <= 0:
        return 0

    # Допустимый диапазон 90-110%
    if 90 <= total <= 110:
        return 0

    # Нормализуем
    factor = 100.0 / total
    for key in ms:
        if isinstance(ms[key], (int, float)):
            ms[key] = round(ms[key] * factor, 1)

    logger.info("Market share нормализован: %.1f%% → 100%%", total)
    return 1


# ── Главная функция ──


def revise_report(
    report_data: dict,
    board_review: dict,
    company_info: dict,
    max_sections: int = 5,
) -> dict:
    """Применяет замечания совета директоров к данным отчёта.

    Args:
        report_data: текущие данные отчёта
        board_review: результат run_review() (reviews + consensus)
        company_info: информация о компании (name, business_type)
        max_sections: максимум секций для LLM-ревизии (для экономии API)

    Returns:
        Обновлённый report_data с исправленными данными
    """
    t0 = __import__("time").monotonic()

    # 1. Собираем actionable замечания
    critiques = _collect_actionable_critiques(board_review)
    if not critiques:
        logger.info("Нет actionable замечаний для ревизии")
        return report_data

    logger.info(
        "Ревизия: %d actionable замечаний (high+medium) от борда",
        len(critiques),
    )

    # 2. Группируем по ключам report_data
    critiques_by_key: dict[str, list[dict]] = {}
    for critique in critiques:
        keys = _map_critique_to_keys(critique)
        for key in keys:
            if key not in critiques_by_key:
                critiques_by_key[key] = []
            critiques_by_key[key].append(critique)

    # Если не удалось замапить ни одну критику, пробуем общий подход
    unmapped = [c for c in critiques if not _map_critique_to_keys(c)]
    if unmapped:
        logger.info(
            "Не удалось замапить %d замечаний на секции: %s",
            len(unmapped),
            [c.get("section", "?") for c in unmapped],
        )

    # 3. Приоритизация: секции с наибольшим числом high-severity замечаний
    def priority(key: str) -> int:
        crits = critiques_by_key.get(key, [])
        return sum(2 if c.get("severity") == "high" else 1 for c in crits)

    sorted_keys = sorted(critiques_by_key.keys(), key=priority, reverse=True)
    keys_to_fix = sorted_keys[:max_sections]

    logger.info(
        "Секции для LLM-ревизии (%d/%d): %s",
        len(keys_to_fix), len(sorted_keys), keys_to_fix,
    )

    # 4. LLM-ревизия каждой секции
    llm_fixes = 0
    for key in keys_to_fix:
        section_data = report_data.get(key)
        if section_data is None:
            logger.warning("Секция %s отсутствует в report_data, пропускаем", key)
            continue

        section_critiques = critiques_by_key[key]
        logger.info(
            "Ревизия секции '%s': %d замечаний", key, len(section_critiques),
        )

        prompt = _build_fix_prompt(key, section_data, section_critiques, company_info)

        system = (
            "Ты — редактор данных бизнес-аналитического отчёта. "
            "Исправляешь данные по замечаниям экспертов. "
            "Отвечай ТОЛЬКО валидным JSON."
        )

        try:
            raw = call_board_llm(prompt=prompt, system=system)
            if raw.startswith("[Board LLM Error]"):
                logger.error("LLM error при ревизии %s: %s", key, raw[:200])
                continue

            fixed = _parse_fix_response(raw, key)
            if fixed is not None:
                # Валидация: фиксы должны быть того же типа
                if type(fixed) == type(section_data):
                    report_data[key] = fixed
                    llm_fixes += 1
                    logger.info("Секция '%s' исправлена через LLM", key)
                elif isinstance(section_data, dict) and isinstance(fixed, dict):
                    # Мержим — LLM мог вернуть только часть полей
                    section_data.update(fixed)
                    llm_fixes += 1
                    logger.info("Секция '%s' обновлена (merge) через LLM", key)
                else:
                    logger.warning(
                        "Тип данных не совпадает для %s: ожидался %s, получен %s",
                        key, type(section_data).__name__, type(fixed).__name__,
                    )
        except Exception as e:
            logger.error("Ошибка при LLM-ревизии секции %s: %s", key, e)

    # 5. Программатические фиксы (всегда)
    prog_fixes = 0
    prog_fixes += _fix_readability(report_data)
    prog_fixes += _fix_radar_scores(report_data)
    prog_fixes += _fix_empty_swot(report_data)
    prog_fixes += _fix_market_share_sum(report_data)

    elapsed = round(__import__("time").monotonic() - t0, 2)
    logger.info(
        "Ревизия завершена за %.2fs: %d LLM-фиксов, %d программатических фиксов",
        elapsed, llm_fixes, prog_fixes,
    )

    return report_data
