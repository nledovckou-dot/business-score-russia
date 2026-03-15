"""SVG radar chart generator."""

from __future__ import annotations

import math
from typing import Any, List


def render_radar_svg(
    dimensions: list[str],
    datasets: list[dict[str, Any]],
    width: int = 500,
    height: int = 500,
    max_val: float = 10.0,
    levels: int = 5,
) -> str:
    """Generate inline SVG radar chart.

    datasets: [{"label": "Company", "values": [8,6,7,...], "color": "#C9A44C", "highlight": True}, ...]
    """
    cx, cy = width / 2, height / 2
    radius = min(cx, cy) - 60
    n = len(dimensions)
    if n < 3:
        return ""

    angle_step = 2 * math.pi / n

    def polar(i: int, val: float) -> tuple[float, float]:
        angle = -math.pi / 2 + i * angle_step
        r = (val / max_val) * radius
        return cx + r * math.cos(angle), cy + r * math.sin(angle)

    lines: list[str] = []
    lines.append(f'<svg viewBox="0 0 {width} {height}" class="chart-svg" xmlns="http://www.w3.org/2000/svg">')

    # Grid levels
    for lvl in range(1, levels + 1):
        r = radius * lvl / levels
        pts = []
        for i in range(n):
            angle = -math.pi / 2 + i * angle_step
            pts.append(f"{cx + r * math.cos(angle):.1f},{cy + r * math.sin(angle):.1f}")
        lines.append(f'  <polygon points="{" ".join(pts)}" fill="none" stroke="#e2e6ed" stroke-width="1" opacity="0.6"/>')

    # Axis lines + labels
    for i, dim in enumerate(dimensions):
        x, y = polar(i, max_val)
        lines.append(f'  <line x1="{cx}" y1="{cy}" x2="{x:.1f}" y2="{y:.1f}" stroke="#e2e6ed" stroke-width="1" opacity="0.4"/>')
        # Label position (slightly beyond max)
        lx, ly = polar(i, max_val * 1.18)
        anchor = "middle"
        if lx < cx - 10:
            anchor = "end"
        elif lx > cx + 10:
            anchor = "start"
        lines.append(f'  <text x="{lx:.1f}" y="{ly:.1f}" text-anchor="{anchor}" fill="#5a6880" font-size="11" font-family="Inter, system-ui, sans-serif">{dim}</text>')

    # Data polygons
    for ds_idx, ds in enumerate(datasets):
        values = ds.get("values", [])
        color = ds.get("color", "#C9A44C")
        highlight = ds.get("highlight", False)
        label = ds.get("label", "")

        if len(values) != n:
            continue

        pts = []
        for i, v in enumerate(values):
            x, y = polar(i, v)
            pts.append(f"{x:.1f},{y:.1f}")

        opacity = "0.25" if highlight else "0.12"
        stroke_w = "2.5" if highlight else "1.5"
        anim_class = "radar-area-main" if highlight else "radar-area-comp"

        lines.append(f'  <polygon points="{" ".join(pts)}" fill="{color}" fill-opacity="{opacity}" stroke="{color}" stroke-width="{stroke_w}" class="{anim_class}"/>')

        # Dots
        for i, v in enumerate(values):
            x, y = polar(i, v)
            r = "5" if highlight else "3.5"
            lines.append(f'  <circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{color}" class="radar-dot"/>')

    lines.append("</svg>")
    return "\n".join(lines)
