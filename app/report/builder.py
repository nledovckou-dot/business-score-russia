"""Report builder: assembles HTML from blocks, profiles, charts, and data."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from app.config import (
    PERCEPTUAL_AXES,
    REPORTS_DIR,
    TEMPLATES_DIR,
    THEME_DEFAULT,
    BusinessType,
)
from app.models import ReportData
from app.report.charts.bars import render_grouped_bars_svg, render_horizontal_bars_svg
from app.report.charts.donut import render_donut_svg
from app.report.charts.heatmap import render_heatmap_svg
from app.report.charts.radar import render_radar_svg
from app.report.charts.scatter import render_scatter_svg
from app.report.profiles import (
    BLOCK_TEMPLATES,
    get_active_sections,
    get_blocks_for_type,
)


# Human-readable names for placeholder fallback (T9/T14)
BLOCK_NAMES: dict[str, str] = {
    "M1": "Обзор рынка", "M2": "Регуляторика", "M3": "Технологические тренды", "M4": "Рынок труда",
    "C1": "Перцептуальная карта", "C2": "Профили конкурентов", "C3": "Радар компетенций",
    "C4": "Сравнительная таблица", "C5": "Охват рынка", "C6": "География конкурентов",
    "C7": "Жизненный цикл", "C8": "Каналы продаж",
    "P1": "Профиль компании", "P2": "Финансы", "P3": "SWOT-анализ",
    "P4": "Digital-аудит", "P5": "Продукты и услуги", "P6": "Меню и ценообразование",
    "P7": "Отзывы", "P8": "Тендеры", "P9": "Ассортимент", "P10": "Доля рынка",
    "S1": "Рекомендации", "S2": "KPI и бенчмарки", "S3": "Сценарии",
    "S4": "Корреляции", "S5": "Таймлайн внедрения",
    "A1": "Открытые вопросы", "A2": "Глоссарий", "A3": "Методология", "A4": "Прозрачность расчётов",
    "F1": "Фактчек", "F2": "Верификация digital", "O1": "Фаундеры", "O2": "Мнения экспертов",
    "B1": "Заключение совета директоров",
}


def _render_placeholder(block_id: str) -> str:
    """Render a placeholder card for an empty block (T9/T14)."""
    name = BLOCK_NAMES.get(block_id, block_id)
    return (
        '<div class="block-placeholder">'
        '<div class="ph-icon">&#128203;</div>'
        f'<div class="ph-title">{name}</div>'
        '<div class="ph-text">Данные по этой секции будут доступны после ручной проверки.</div>'
        '</div>'
    )


def build_report(data: ReportData, theme: dict[str, str] | None = None) -> str:
    """Build a complete HTML report from ReportData.

    Returns the full HTML string (single file, no external dependencies).
    """
    if theme is None:
        theme = THEME_DEFAULT

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )

    # Determine active blocks and sections
    block_ids = get_blocks_for_type(data.company.business_type)
    sections = get_active_sections(block_ids, data.section_gates or None)

    # Pre-render all charts
    charts = _render_all_charts(data)

    # Build context shared across all blocks
    base_ctx = _build_base_context(data, charts, theme)

    # Render each block (T9/T14: show placeholder instead of hiding empty blocks)
    for section in sections:
        rendered = []
        for block_id in section["blocks"]:
            template_path = BLOCK_TEMPLATES.get(block_id)
            if not template_path:
                continue
            try:
                tmpl = env.get_template(template_path)
                html = tmpl.render(**base_ctx)
                if html.strip():
                    rendered.append(html)
                else:
                    rendered.append(_render_placeholder(block_id))
            except Exception as e:
                rendered.append(
                    f'<div class="callout callout-red"><h4>Ошибка блока {BLOCK_NAMES.get(block_id, block_id)}</h4>'
                    f'<p>{e}</p></div>'
                )
        section["rendered_blocks"] = rendered

    # Filter out empty sections
    sections = [s for s in sections if s["rendered_blocks"]]

    # Render base template
    base_tmpl = env.get_template("base.html")
    html = base_tmpl.render(
        company=data.company,
        report_date=data.report_date,
        sections=sections,
        block_count=sum(len(s["rendered_blocks"]) for s in sections),
        theme=theme,
    )

    return html


def save_report(data: ReportData, filename: str | None = None, theme: dict | None = None) -> Path:
    """Build and save the report to disk. Returns the file path."""
    html = build_report(data, theme)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if filename is None:
        safe_name = data.company.name.replace(" ", "_").replace("/", "_")
        filename = f"{safe_name}_{data.report_date.isoformat()}.html"

    path = REPORTS_DIR / filename
    path.write_text(html, encoding="utf-8")
    return path


def _build_base_context(data: ReportData, charts: dict[str, str], theme: dict) -> dict[str, Any]:
    """Build the template context dict with all data and pre-rendered charts."""
    axes = PERCEPTUAL_AXES.get(data.company.business_type, ("Цена", "Качество"))

    # Comparison columns for C4
    comparison_columns: list[str] = []
    if data.competitors:
        all_keys: set[str] = set()
        for c in data.competitors:
            all_keys.update(c.metrics.keys())
        comparison_columns = sorted(all_keys)

    # Company comparison data for C4
    company_comparison: dict[str, Any] = {}

    # Legend items for scatter
    scatter_legend = []
    if data.competitors:
        scatter_legend.append({"label": data.company.name, "color": "#C9A44C"})
        colors = ["#4A8FE0", "#3DB86A", "#D44040", "#9060D0", "#E08040", "#40B0A0"]
        for i, c in enumerate(data.competitors):
            scatter_legend.append({"label": c.name, "color": colors[i % len(colors)]})

    # Legend items for radar
    radar_legend = []
    if data.competitors and data.radar_dimensions:
        radar_legend.append({"label": data.company.name, "color": "#C9A44C"})
        colors = ["#4A8FE0", "#3DB86A", "#D44040", "#9060D0"]
        for i, c in enumerate(data.competitors[:4]):
            radar_legend.append({"label": c.name, "color": colors[i % len(colors)]})

    # Donut legend
    donut_legend = []
    donut_colors = ["#C9A44C", "#4A8FE0", "#3DB86A", "#D44040", "#9060D0", "#E08040", "#40B0A0"]
    for i, (name, _) in enumerate(data.market_share.items()):
        donut_legend.append({"label": name, "color": donut_colors[i % len(donut_colors)]})

    # Financials legend
    financials_legend = [
        {"label": "Выручка", "color": "#C9A44C"},
        {"label": "Чистая прибыль", "color": "#3DB86A"},
    ]

    return {
        # Core
        "company": data.company,
        "report_date": data.report_date,
        "theme": theme,
        # Macro
        "market": data.market,
        "market_chart_svg": charts.get("market_bars", ""),
        "regulatory_trends": data.regulatory_trends,
        "tech_trends": data.tech_trends,
        "hr_data": data.hr_data,
        "hr_chart_svg": charts.get("hr_bars", ""),
        # Meso
        "competitors": data.competitors,
        "scatter_svg": charts.get("scatter", ""),
        "x_axis": axes[0],
        "y_axis": axes[1],
        "legend_items": scatter_legend,
        "radar_svg": charts.get("radar", ""),
        "radar_dimensions": data.radar_dimensions,
        "radar_legend": radar_legend,
        "comparison_columns": comparison_columns,
        "company_comparison": company_comparison,
        "coverage_heatmap_svg": charts.get("coverage_heatmap", ""),
        # Micro
        "financials": data.financials,
        "financials_chart_svg": charts.get("financials_bars", ""),
        "financials_legend": financials_legend,
        "swot": data.swot,
        "digital": data.digital,
        "digital_chart_svg": charts.get("digital_bars", ""),
        "products": data.products,
        "menu": data.menu,
        "menu_chart_svg": charts.get("menu_donut", ""),
        "reviews": data.reviews,
        "reviews_chart_svg": charts.get("reviews_bars", ""),
        "tenders": data.tenders,
        "donut_svg": charts.get("market_share_donut", ""),
        "donut_legend": donut_legend,
        # Strategy
        "recommendations": data.recommendations,
        "kpi_benchmarks": data.kpi_benchmarks,
        "kpi_chart_svg": charts.get("kpi_bars", ""),
        "scenarios": data.scenarios,
        "scenarios_chart_svg": charts.get("scenarios_bars", ""),
        "scenarios_legend": [
            {"label": "Оптимистичный", "color": "#3DB86A"},
            {"label": "Базовый", "color": "#C9A44C"},
            {"label": "Пессимистичный", "color": "#D44040"},
        ],
        "correlations_svg": charts.get("correlations_heatmap", ""),
        "implementation_timeline": data.implementation_timeline,
        # Appendix
        "open_questions": data.open_questions,
        "glossary": data.glossary,
        # Factcheck
        "factcheck": data.factcheck,
        "digital_verification": data.digital_verification,
        "digital_verification_chart_svg": charts.get("digital_verification_bars", ""),
        # Founders
        "founders": data.founders,
        "opinions": data.opinions,
        # v2.0
        "calc_traces": data.calc_traces,
        "methodology": data.methodology,
        "section_gates": data.section_gates,
        "pipeline_version": data.pipeline_version,
        # Board of Directors (T27)
        "board_review": data.board_review,
    }


def _render_all_charts(data: ReportData) -> dict[str, str]:
    """Pre-render all SVG charts needed by blocks."""
    charts: dict[str, str] = {}

    # M1: Market overview grouped bars
    if data.market and data.market.data_points:
        charts["market_bars"] = render_grouped_bars_svg(
            categories=[str(dp.year) for dp in data.market.data_points],
            series=[{
                "label": "Объём рынка",
                "values": [dp.value for dp in data.market.data_points],
                "color": "#C9A44C",
            }],
        )

    # M4: HR salary bars
    if data.hr_data.get("salaries"):
        charts["hr_bars"] = render_horizontal_bars_svg(
            items=data.hr_data["salaries"],
        )

    # C1: Perceptual map
    if data.competitors:
        axes = PERCEPTUAL_AXES.get(data.company.business_type, ("Цена", "Качество"))
        points = [{"name": data.company.name, "x": 50, "y": 50, "color": "#C9A44C", "size": 9}]
        colors = ["#4A8FE0", "#3DB86A", "#D44040", "#9060D0", "#E08040", "#40B0A0"]
        for i, c in enumerate(data.competitors):
            points.append({
                "name": c.name,
                "x": c.x,
                "y": c.y,
                "color": colors[i % len(colors)],
                "size": 7,
            })
        charts["scatter"] = render_scatter_svg(
            points=points,
            x_label=axes[0],
            y_label=axes[1],
            highlight_name=data.company.name,
        )

    # C3: Radar
    if data.radar_dimensions and data.competitors:
        datasets = []
        # Company itself (values from first competitor's radar_scores as placeholder)
        company_scores = [5.0] * len(data.radar_dimensions)  # default
        datasets.append({
            "label": data.company.name,
            "values": company_scores,
            "color": "#C9A44C",
            "highlight": True,
        })
        colors = ["#4A8FE0", "#3DB86A", "#D44040", "#9060D0"]
        for i, c in enumerate(data.competitors[:4]):
            vals = [c.radar_scores.get(dim, 5.0) for dim in data.radar_dimensions]
            datasets.append({
                "label": c.name,
                "values": vals,
                "color": colors[i % len(colors)],
                "highlight": False,
            })
        charts["radar"] = render_radar_svg(
            dimensions=data.radar_dimensions,
            datasets=datasets,
        )

    # P2: Financial bars
    if data.financials:
        years = [str(f.year) for f in data.financials]
        revenue_vals = [f.revenue or 0 for f in data.financials]
        profit_vals = [f.net_profit or 0 for f in data.financials]
        charts["financials_bars"] = render_grouped_bars_svg(
            categories=years,
            series=[
                {"label": "Выручка", "values": revenue_vals, "color": "#C9A44C"},
                {"label": "Чистая прибыль", "values": profit_vals, "color": "#3DB86A"},
            ],
        )

    # P4: Digital followers bars
    if data.digital and data.digital.social_accounts:
        charts["digital_bars"] = render_horizontal_bars_svg(
            items=[
                {"label": acc.platform + " " + acc.handle, "value": acc.followers or 0, "color": "#4A8FE0"}
                for acc in data.digital.social_accounts
            ],
        )

    # P10: Market share donut
    if data.market_share:
        segments = [
            {"label": name, "value": val}
            for name, val in data.market_share.items()
        ]
        charts["market_share_donut"] = render_donut_svg(
            segments=segments,
            center_label="Доля рынка",
        )

    # S2: KPI benchmarks bars
    if data.kpi_benchmarks:
        items = []
        for kpi in data.kpi_benchmarks:
            if kpi.current is not None:
                items.append({"label": kpi.name, "value": kpi.current, "color": "#C9A44C"})
        if items:
            charts["kpi_bars"] = render_horizontal_bars_svg(items=items)

    # S3: Scenarios grouped bars
    if data.scenarios:
        # Use first metric key across all scenarios
        all_keys: set[str] = set()
        for sc in data.scenarios:
            all_keys.update(sc.metrics.keys())
        metric_keys = sorted(all_keys)[:5]  # limit to 5 metrics

        if metric_keys:
            sc_colors = {"optimistic": "#3DB86A", "base": "#C9A44C", "pessimistic": "#D44040"}
            series = []
            for sc in data.scenarios:
                series.append({
                    "label": sc.label,
                    "values": [sc.metrics.get(k, 0) for k in metric_keys],
                    "color": sc_colors.get(sc.name, "#C9A44C"),
                })
            charts["scenarios_bars"] = render_grouped_bars_svg(
                categories=metric_keys,
                series=series,
            )

    # S4: Correlations heatmap
    if data.correlations:
        metrics_set: set[str] = set()
        for c in data.correlations:
            metrics_set.add(c.metric_a)
            metrics_set.add(c.metric_b)
        metrics_list = sorted(metrics_set)
        n = len(metrics_list)
        idx_map = {m: i for i, m in enumerate(metrics_list)}
        values = [[0.0] * n for _ in range(n)]
        for c in data.correlations:
            i, j = idx_map[c.metric_a], idx_map[c.metric_b]
            values[i][j] = c.value
            values[j][i] = c.value
        for i in range(n):
            values[i][i] = 1.0
        charts["correlations_heatmap"] = render_heatmap_svg(
            rows=metrics_list,
            cols=metrics_list,
            values=values,
        )

    # F2: Digital verification bars
    if data.digital_verification:
        items = [
            {"label": v.get("company", "?"), "value": v.get("total_followers", 0), "color": "#4A8FE0"}
            for v in data.digital_verification
        ]
        if items:
            charts["digital_verification_bars"] = render_horizontal_bars_svg(items=items)

    return charts
