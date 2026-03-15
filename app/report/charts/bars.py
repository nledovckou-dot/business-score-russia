"""SVG bar chart generators: horizontal, vertical, and grouped bars."""

from __future__ import annotations

from typing import Any, List, Optional


def render_horizontal_bars_svg(
    items: list[dict[str, Any]],
    width: int = 700,
    bar_height: int = 28,
    gap: int = 12,
    max_val: float | None = None,
    color: str = "#C9A44C",
) -> str:
    """Horizontal bar chart.

    items: [{"label": "Label", "value": 123, "color": "#C9A44C"}, ...]
    """
    if not items:
        return ""

    # Ensure all values are numeric (LLM can return strings)
    for item in items:
        v = item.get("value", 0)
        if not isinstance(v, (int, float)):
            try:
                item["value"] = float(str(v).replace(" ", "").replace(",", ""))
            except (ValueError, TypeError):
                item["value"] = 0

    label_w = 160
    val_w = 80
    chart_w = width - label_w - val_w - 20

    if max_val is None:
        max_val = max(item["value"] for item in items) or 1

    total_h = len(items) * (bar_height + gap) + 20
    lines = [f'<svg viewBox="0 0 {width} {total_h}" class="chart-svg" xmlns="http://www.w3.org/2000/svg">']

    for idx, item in enumerate(items):
        y = idx * (bar_height + gap) + 10
        lbl = item["label"]
        val = item["value"]
        c = item.get("color", color)
        bar_w = max((val / max_val) * chart_w, 4) if max_val > 0 else 4
        display_val = item.get("display", f"{val:,.0f}")

        # Label
        lines.append(f'  <text x="{label_w - 8}" y="{y + bar_height / 2 + 4}" text-anchor="end" fill="#5a6880" font-size="12" font-family="Inter, system-ui, sans-serif">{lbl}</text>')
        # Track
        lines.append(f'  <rect x="{label_w}" y="{y}" width="{chart_w}" height="{bar_height}" rx="4" fill="#f0f2f5"/>')
        # Fill
        delay = idx * 0.1
        lines.append(f'  <rect x="{label_w}" y="{y}" width="{bar_w:.1f}" height="{bar_height}" rx="4" fill="{c}" class="bar-animated" style="animation-delay:{delay:.1f}s"/>')
        # Value
        lines.append(f'  <text x="{label_w + chart_w + 10}" y="{y + bar_height / 2 + 4}" fill="#1a2b4a" font-size="12" font-weight="600" font-family="Inter, system-ui, sans-serif">{display_val}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


def render_grouped_bars_svg(
    categories: list[str],
    series: list[dict[str, Any]],
    width: int = 700,
    height: int = 350,
    max_val: float | None = None,
) -> str:
    """Vertical grouped bar chart.

    categories: ["2021", "2022", "2023"]
    series: [{"label": "Выручка", "values": [100, 120, 150], "color": "#C9A44C"}, ...]
    """
    if not categories or not series:
        return ""

    # Ensure all values are numeric
    for s in series:
        s["values"] = [
            float(v) if isinstance(v, (int, float)) else
            (float(str(v).replace(" ", "").replace(",", "")) if v is not None else 0.0)
            for v in s.get("values", [])
        ]

    margin = {"top": 30, "right": 20, "bottom": 50, "left": 70}
    chart_w = width - margin["left"] - margin["right"]
    chart_h = height - margin["top"] - margin["bottom"]

    all_vals = [v for s in series for v in s["values"]]
    if max_val is None:
        max_val = max(all_vals) if all_vals else 1
    if max_val == 0:
        max_val = 1

    n_cats = len(categories)
    n_series = len(series)
    group_w = chart_w / n_cats
    bar_w = min(group_w / (n_series + 1), 40)
    bar_gap = 4

    lines = [f'<svg viewBox="0 0 {width} {height}" class="chart-svg" xmlns="http://www.w3.org/2000/svg">']

    # Y-axis grid
    n_ticks = 5
    for i in range(n_ticks + 1):
        y_val = max_val * i / n_ticks
        y_pos = margin["top"] + chart_h - (chart_h * i / n_ticks)
        lines.append(f'  <line x1="{margin["left"]}" y1="{y_pos:.1f}" x2="{width - margin["right"]}" y2="{y_pos:.1f}" stroke="#e2e6ed" stroke-width="1"/>')
        display = f"{y_val:,.0f}" if y_val >= 1 else f"{y_val:.2f}"
        lines.append(f'  <text x="{margin["left"] - 8}" y="{y_pos + 4:.1f}" text-anchor="end" fill="#8a96a8" font-size="10" font-family="Inter, system-ui, sans-serif">{display}</text>')

    # Bars
    for cat_idx, cat in enumerate(categories):
        group_x = margin["left"] + cat_idx * group_w
        # Category label
        label_x = group_x + group_w / 2
        lines.append(f'  <text x="{label_x:.1f}" y="{height - 15}" text-anchor="middle" fill="#5a6880" font-size="11" font-family="Inter, system-ui, sans-serif">{cat}</text>')

        for s_idx, s in enumerate(series):
            val = s["values"][cat_idx] if cat_idx < len(s["values"]) else 0
            color = s.get("color", "#C9A44C")
            bar_h = (val / max_val) * chart_h if max_val > 0 else 0
            x = group_x + (group_w - n_series * (bar_w + bar_gap)) / 2 + s_idx * (bar_w + bar_gap)
            y = margin["top"] + chart_h - bar_h

            delay = (cat_idx * n_series + s_idx) * 0.08
            lines.append(f'  <rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" rx="3" fill="{color}" class="vbar-animated" style="animation-delay:{delay:.1f}s"/>')

    # Legend
    legend_y = height - 2
    for s_idx, s in enumerate(series):
        lx = margin["left"] + s_idx * 140
        lines.append(f'  <rect x="{lx}" y="{legend_y - 8}" width="12" height="12" rx="2" fill="{s.get("color", "#C9A44C")}"/>')
        lines.append(f'  <text x="{lx + 18}" y="{legend_y}" fill="#5a6880" font-size="10" font-family="Inter, system-ui, sans-serif">{s["label"]}</text>')

    lines.append("</svg>")
    return "\n".join(lines)
