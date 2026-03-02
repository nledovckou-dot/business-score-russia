"""SVG donut chart generator."""

from __future__ import annotations

import math
from typing import Any, List

COLORS = ["#C9A44C", "#4A8FE0", "#3DB86A", "#D44040", "#9060D0", "#E08040", "#40B0A0", "#E8C46A"]


def render_donut_svg(
    segments: list[dict[str, Any]],
    width: int = 400,
    height: int = 400,
    inner_radius: float = 0.55,
    center_label: str = "",
    center_value: str = "",
) -> str:
    """Donut chart.

    segments: [{"label": "Company A", "value": 35, "color": "#C9A44C"}, ...]
    """
    if not segments:
        return ""

    cx, cy = width / 2, height / 2
    outer_r = min(cx, cy) - 50
    inner_r = outer_r * inner_radius

    total = sum(s["value"] for s in segments) or 1

    lines = [f'<svg viewBox="0 0 {width} {height}" class="chart-svg" xmlns="http://www.w3.org/2000/svg">']

    angle = -90  # start from top

    for idx, seg in enumerate(segments):
        pct = seg["value"] / total
        sweep = pct * 360
        color = seg.get("color", COLORS[idx % len(COLORS)])

        start_rad = math.radians(angle)
        end_rad = math.radians(angle + sweep)

        # Outer arc
        x1_o = cx + outer_r * math.cos(start_rad)
        y1_o = cy + outer_r * math.sin(start_rad)
        x2_o = cx + outer_r * math.cos(end_rad)
        y2_o = cy + outer_r * math.sin(end_rad)

        # Inner arc
        x1_i = cx + inner_r * math.cos(end_rad)
        y1_i = cy + inner_r * math.sin(end_rad)
        x2_i = cx + inner_r * math.cos(start_rad)
        y2_i = cy + inner_r * math.sin(start_rad)

        large_arc = 1 if sweep > 180 else 0

        path = (
            f"M {x1_o:.1f} {y1_o:.1f} "
            f"A {outer_r} {outer_r} 0 {large_arc} 1 {x2_o:.1f} {y2_o:.1f} "
            f"L {x1_i:.1f} {y1_i:.1f} "
            f"A {inner_r} {inner_r} 0 {large_arc} 0 {x2_i:.1f} {y2_i:.1f} Z"
        )

        delay = idx * 0.12
        lines.append(f'  <path d="{path}" fill="{color}" class="donut-segment" style="animation-delay:{delay:.1f}s" opacity="0.85"/>')

        # Label line for segments > 5%
        if pct > 0.05:
            mid_angle = math.radians(angle + sweep / 2)
            label_r = outer_r + 20
            lx = cx + label_r * math.cos(mid_angle)
            ly = cy + label_r * math.sin(mid_angle)
            anchor = "start" if lx > cx else "end"
            lines.append(f'  <text x="{lx:.1f}" y="{ly:.1f}" text-anchor="{anchor}" fill="#A8A0B0" font-size="10" font-family="Segoe UI, system-ui, sans-serif">{seg["label"]} ({pct:.0%})</text>')

        angle += sweep

    # Center text
    if center_value:
        lines.append(f'  <text x="{cx}" y="{cy - 6}" text-anchor="middle" fill="#C9A44C" font-size="22" font-weight="700" font-family="Segoe UI, system-ui, sans-serif">{center_value}</text>')
    if center_label:
        lines.append(f'  <text x="{cx}" y="{cy + 16}" text-anchor="middle" fill="#706880" font-size="11" font-family="Segoe UI, system-ui, sans-serif">{center_label}</text>')

    lines.append("</svg>")
    return "\n".join(lines)
