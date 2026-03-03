"""Block profiles: which blocks render for each business type.

Each block ID maps to a Jinja2 template path under blocks/.
Order in the list = order in the report.
"""

from __future__ import annotations

from typing import Optional

from app.config import BusinessType

# Block ID → template path (relative to blocks/)
BLOCK_TEMPLATES: dict[str, str] = {
    # Part I — Macro (market)
    "M1": "macro/m1_market_overview.html",
    "M2": "macro/m2_regulatory.html",
    "M3": "macro/m3_tech_trends.html",
    "M4": "macro/m4_hr_market.html",
    # Part II — Meso (competitors)
    "C1": "meso/c1_perceptual_map.html",
    "C2": "meso/c2_competitor_profiles.html",
    "C3": "meso/c3_radar.html",
    "C4": "meso/c4_comparison_table.html",
    "C5": "meso/c5_coverage_heatmap.html",
    "C6": "meso/c6_geography.html",
    "C7": "meso/c7_lifecycle.html",
    "C8": "meso/c8_sales_channels.html",
    # Part III — Micro (company)
    "P1": "micro/p1_company_profile.html",
    "P2": "micro/p2_financials.html",
    "P3": "micro/p3_swot.html",
    "P4": "micro/p4_digital_audit.html",
    "P5": "micro/p5_product_analysis.html",
    "P6": "micro/p6_menu_pricing.html",
    "P7": "micro/p7_reviews.html",
    "P8": "micro/p8_tenders.html",
    "P9": "micro/p9_assortment.html",
    "P10": "micro/p10_market_share.html",
    # Part IV — Strategy
    "S1": "strategy/s1_recommendations.html",
    "S2": "strategy/s2_kpi_benchmarks.html",
    "S3": "strategy/s3_scenarios.html",
    "S4": "strategy/s4_correlations.html",
    "S5": "strategy/s5_timeline.html",
    # Part V — Appendix
    "A1": "appendix/a1_open_questions.html",
    "A2": "appendix/a2_glossary.html",
    "A3": "appendix/a3_methodology.html",
    "A4": "appendix/a4_calc_traces.html",
    # Part VI — Factcheck
    "F1": "factcheck/f1_fact_verification.html",
    "F2": "factcheck/f2_digital_verification.html",
    # Part VII — Founders & Opinions (optional)
    "O1": "factcheck/o1_founders.html",
    "O2": "factcheck/o2_opinions.html",
    # Part VIII — Board of Directors
    "B1": "board/b1_board_conclusion.html",
}


# Sections: logical grouping of blocks with titles
SECTIONS = [
    {"id": "macro",    "num": "I",   "title": "Макро-анализ рынка",        "subtitle": "Объём, динамика, тренды отрасли"},
    {"id": "meso",     "num": "II",  "title": "Конкурентный ландшафт",     "subtitle": "Перцептуальная карта, профили, сравнение"},
    {"id": "micro",    "num": "III", "title": "Анализ компании",           "subtitle": "Профиль, финансы, SWOT, digital"},
    {"id": "strategy", "num": "IV",  "title": "Стратегия и рекомендации",  "subtitle": "KPI, сценарии, таймлайн внедрения"},
    {"id": "appendix", "num": "V",   "title": "Приложения",               "subtitle": "Открытые вопросы, справка по терминам, методология"},
    {"id": "factcheck","num": "VI",  "title": "Верификация",              "subtitle": "Фактчек, digital-аудит, источники"},
    {"id": "founders", "num": "VII", "title": "Фаундеры и мнения",        "subtitle": "Карта владельцев, цитаты лидеров отрасли"},
    {"id": "lifecycle","num": "VIII","title": "Жизненный цикл",           "subtitle": "Стадия каждого конкурента и обоснование"},
    {"id": "channels", "num": "IX",  "title": "Карта каналов продаж",     "subtitle": "Полная матрица каналов по всем конкурентам"},
    {"id": "board",    "num": "X",   "title": "Заключение совета директоров", "subtitle": "AI-эксперты: рецензия и рекомендации"},
]


# Which blocks belong to which section
SECTION_BLOCKS: dict[str, list[str]] = {
    "macro":    ["M1", "M2", "M3", "M4"],
    "meso":     ["C1", "C2", "C3", "C4", "C5", "C6"],
    "micro":    ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"],
    "strategy": ["S1", "S2", "S3", "S4", "S5"],
    "appendix": ["A1", "A2", "A3", "A4"],
    "factcheck":["F1", "F2"],
    "founders": ["O1", "O2"],
    "lifecycle":["C7"],
    "channels": ["C8"],
    "board":    ["B1"],
}


# Block availability per business type
PROFILES: dict[BusinessType, list[str]] = {
    BusinessType.B2C_SERVICE: [
        "M1", "M2", "M3", "M4",
        "C1", "C2", "C3", "C4", "C6",
        "P1", "P2", "P3", "P4", "P6", "P7", "P10",
        "S1", "S2", "S3", "S4", "S5",
        "A1", "A2", "A3", "A4", "F1", "F2",
        "O1", "O2", "C7", "C8", "B1",
    ],
    BusinessType.B2C_PRODUCT: [
        "M1", "M2", "M3", "M4",
        "C1", "C2", "C3", "C4", "C6",
        "P1", "P2", "P3", "P4", "P9", "P10",
        "S1", "S2", "S3", "S4", "S5",
        "A1", "A2", "A3", "A4", "F1", "F2",
        "O1", "O2", "C7", "C8", "B1",
    ],
    BusinessType.B2B_SERVICE: [
        "M1", "M2", "M3", "M4",
        "C1", "C2", "C3", "C4", "C5",
        "P1", "P2", "P3", "P4", "P5", "P8", "P10",
        "S1", "S2", "S3", "S4", "S5",
        "A1", "A2", "A3", "A4", "F1", "F2",
        "O1", "O2", "C7", "C8", "B1",
    ],
    BusinessType.B2B_PRODUCT: [
        "M1", "M2", "M3", "M4",
        "C1", "C2", "C3", "C4", "C5",
        "P1", "P2", "P3", "P4", "P5", "P8", "P9", "P10",
        "S1", "S2", "S3", "S4", "S5",
        "A1", "A2", "A3", "A4", "F1", "F2",
        "O1", "O2", "C7", "C8", "B1",
    ],
    BusinessType.PLATFORM: [
        "M1", "M2", "M3", "M4",
        "C1", "C2", "C3", "C4",
        "P1", "P2", "P3", "P4", "P5", "P10",
        "S1", "S2", "S3", "S4", "S5",
        "A1", "A2", "A3", "A4", "F1", "F2",
        "O1", "O2", "C7", "C8", "B1",
    ],
    BusinessType.B2B_B2C_HYBRID: [
        "M1", "M2", "M3", "M4",
        "C1", "C2", "C3", "C4", "C5", "C6",
        "P1", "P2", "P3", "P4", "P5", "P8", "P9", "P10",
        "S1", "S2", "S3", "S4", "S5",
        "A1", "A2", "A3", "A4", "F1", "F2",
        "O1", "O2", "C7", "C8", "B1",
    ],
}


def get_blocks_for_type(btype: BusinessType) -> list[str]:
    """Return ordered list of block IDs for a business type."""
    return PROFILES.get(btype, PROFILES[BusinessType.B2C_SERVICE])


def get_active_sections(
    block_ids: list[str],
    section_gates: dict[str, bool] | None = None,
) -> list[dict]:
    """Return only sections that have at least one active block.

    If section_gates is provided, blocks gated as False are excluded.
    """
    gates = section_gates or {}
    active = []
    for section in SECTIONS:
        sec_blocks = [
            b for b in SECTION_BLOCKS[section["id"]]
            if b in block_ids and gates.get(b, True)
        ]
        if sec_blocks:
            active.append({**section, "blocks": sec_blocks})
    return active
