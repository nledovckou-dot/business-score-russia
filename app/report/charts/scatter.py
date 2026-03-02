"""SVG scatter (perceptual map) chart generator."""

from __future__ import annotations

from typing import Any, List, Optional


def render_scatter_svg(
    points: list[dict[str, Any]],
    x_label: str = "Цена",
    y_label: str = "Качество",
    width: int = 700,
    height: int = 500,
    highlight_name: str | None = None,
) -> str:
    """Perceptual map / scatter plot.

    points: [{"name": "Company", "x": 65, "y": 80, "color": "#C9A44C", "size": 8}, ...]
    x, y values are 0–100 scale.
    """
    if not points:
        return ""

    margin = {"top": 30, "right": 30, "bottom": 60, "left": 60}
    chart_w = width - margin["left"] - margin["right"]
    chart_h = height - margin["top"] - margin["bottom"]

    def to_px(x_val: float, y_val: float) -> tuple[float, float]:
        px_x = margin["left"] + (x_val / 100) * chart_w
        px_y = margin["top"] + chart_h - (y_val / 100) * chart_h
        return px_x, px_y

    lines = [f'<svg viewBox="0 0 {width} {height}" class="chart-svg" xmlns="http://www.w3.org/2000/svg">']

    # Grid
    for i in range(0, 101, 25):
        x, y = to_px(i, 0)
        x2, y2 = to_px(i, 100)
        lines.append(f'  <line x1="{x:.1f}" y1="{y:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="#2E2838" stroke-width="1"/>')
        lines.append(f'  <text x="{x:.1f}" y="{height - 25}" text-anchor="middle" fill="#706880" font-size="10" font-family="Segoe UI, system-ui, sans-serif">{i}</text>')

    for i in range(0, 101, 25):
        x, y = to_px(0, i)
        x2, y2 = to_px(100, i)
        lines.append(f'  <line x1="{x:.1f}" y1="{y:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="#2E2838" stroke-width="1"/>')
        lines.append(f'  <text x="{margin["left"] - 10}" y="{y + 4:.1f}" text-anchor="end" fill="#706880" font-size="10" font-family="Segoe UI, system-ui, sans-serif">{i}</text>')

    # Axis labels
    lines.append(f'  <text x="{margin["left"] + chart_w / 2}" y="{height - 5}" text-anchor="middle" fill="#A8A0B0" font-size="12" font-weight="600" font-family="Segoe UI, system-ui, sans-serif">{x_label} &rarr;</text>')
    lines.append(f'  <text x="14" y="{margin["top"] + chart_h / 2}" text-anchor="middle" fill="#A8A0B0" font-size="12" font-weight="600" font-family="Segoe UI, system-ui, sans-serif" transform="rotate(-90 14 {margin["top"] + chart_h / 2})">{y_label} &rarr;</text>')

    # Quadrant labels (faint)
    mid_x_left, mid_y_top = to_px(20, 85)
    mid_x_right, mid_y_bottom = to_px(80, 15)
    lines.append(f'  <text x="{mid_x_left:.1f}" y="{mid_y_top:.1f}" fill="#2E2838" font-size="14" font-weight="700" font-family="Segoe UI, system-ui, sans-serif">Нишевые</text>')
    right_x, bottom_y = to_px(75, 85)
    lines.append(f'  <text x="{right_x:.1f}" y="{bottom_y:.1f}" fill="#2E2838" font-size="14" font-weight="700" font-family="Segoe UI, system-ui, sans-serif">Лидеры</text>')

    # Points
    for idx, pt in enumerate(points):
        px, py = to_px(pt["x"], pt["y"])
        color = pt.get("color", "#A8A0B0")
        size = pt.get("size", 7)
        name = pt["name"]
        is_highlight = highlight_name and name == highlight_name

        dot_class = "scatter-dot-main" if is_highlight else "scatter-dot"
        delay = idx * 0.1
        r = size * 1.5 if is_highlight else size

        if is_highlight:
            # Glow
            lines.append(f'  <circle cx="{px:.1f}" cy="{py:.1f}" r="{r + 6}" fill="{color}" opacity="0.15"/>')

        lines.append(f'  <circle cx="{px:.1f}" cy="{py:.1f}" r="{r}" fill="{color}" class="{dot_class}" style="animation-delay:{delay:.1f}s"/>')

        # Name label
        label_y = py - r - 6
        lines.append(f'  <text x="{px:.1f}" y="{label_y:.1f}" text-anchor="middle" fill="{color}" font-size="10" font-weight="{"700" if is_highlight else "400"}" font-family="Segoe UI, system-ui, sans-serif">{name}</text>')

    lines.append("</svg>")
    return "\n".join(lines)
