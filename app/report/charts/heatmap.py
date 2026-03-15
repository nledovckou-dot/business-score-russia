"""SVG heatmap generator for correlation matrices and coverage maps."""

from __future__ import annotations

from typing import Any, List, Optional, Tuple


def render_heatmap_svg(
    rows: list[str],
    cols: list[str],
    values: list[list[float]],
    width: int = 700,
    height: int | None = None,
    min_val: float = -1.0,
    max_val: float = 1.0,
    show_values: bool = True,
    color_scheme: str = "diverging",  # "diverging" (red-gold-green) or "sequential" (gold)
) -> str:
    """Render a heatmap as inline SVG.

    values: 2D list [row][col], values between min_val and max_val.
    """
    if not rows or not cols or not values:
        return ""

    cell_h = 36
    label_w = 140
    col_header_h = 80
    cell_w = min((width - label_w) / len(cols), 80)
    actual_w = label_w + cell_w * len(cols) + 10

    if height is None:
        height = int(col_header_h + cell_h * len(rows) + 20)

    lines = [f'<svg viewBox="0 0 {actual_w:.0f} {height}" class="chart-svg" xmlns="http://www.w3.org/2000/svg">']

    # Column headers (rotated)
    for j, col in enumerate(cols):
        x = label_w + j * cell_w + cell_w / 2
        lines.append(f'  <text x="{x:.1f}" y="{col_header_h - 10}" text-anchor="end" fill="#5a6880" font-size="10" font-family="Inter, system-ui, sans-serif" transform="rotate(-45 {x:.1f} {col_header_h - 10})">{col}</text>')

    # Cells
    for i, row in enumerate(rows):
        y = col_header_h + i * cell_h
        # Row label
        lines.append(f'  <text x="{label_w - 8}" y="{y + cell_h / 2 + 4}" text-anchor="end" fill="#5a6880" font-size="11" font-family="Inter, system-ui, sans-serif">{row}</text>')

        for j in range(len(cols)):
            val = values[i][j] if j < len(values[i]) else 0
            x = label_w + j * cell_w

            # Color mapping
            if color_scheme == "diverging":
                color, text_color = _diverging_color(val, min_val, max_val)
            else:
                color, text_color = _sequential_color(val, min_val, max_val)

            lines.append(f'  <rect x="{x:.1f}" y="{y}" width="{cell_w:.1f}" height="{cell_h}" fill="{color}" stroke="#e2e6ed" stroke-width="1"/>')

            if show_values:
                display = f"{val:.2f}" if abs(val) < 10 else f"{val:.0f}"
                lines.append(f'  <text x="{x + cell_w / 2:.1f}" y="{y + cell_h / 2 + 4}" text-anchor="middle" fill="{text_color}" font-size="10" font-weight="600" font-family="Inter, system-ui, sans-serif">{display}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


def _diverging_color(val: float, min_val: float, max_val: float) -> tuple[str, str]:
    """Red → Gold → Green color scale (light theme)."""
    if val >= 0.7:
        return "rgba(61,184,106,0.22)", "#1a7a3a"
    elif val >= 0.4:
        return "rgba(61,184,106,0.12)", "#2a8a4a"
    elif val >= 0.1:
        return "rgba(201,164,76,0.14)", "#8a7030"
    elif val >= -0.1:
        return "rgba(180,180,200,0.12)", "#5a6880"
    elif val >= -0.4:
        return "rgba(212,64,64,0.10)", "#b03030"
    else:
        return "rgba(212,64,64,0.18)", "#a02020"


def _sequential_color(val: float, min_val: float, max_val: float) -> tuple[str, str]:
    """Single-hue gold scale (light theme)."""
    rng = max_val - min_val or 1
    norm = (val - min_val) / rng
    if norm >= 0.8:
        return "rgba(201,164,76,0.28)", "#6a5520"
    elif norm >= 0.6:
        return "rgba(201,164,76,0.20)", "#7a6530"
    elif norm >= 0.4:
        return "rgba(201,164,76,0.13)", "#8a7030"
    elif norm >= 0.2:
        return "rgba(201,164,76,0.07)", "#5a6880"
    else:
        return "rgba(220,225,235,0.5)", "#8a96a8"
