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
        lines.append(f'  <text x="{x:.1f}" y="{col_header_h - 10}" text-anchor="end" fill="#A8A0B0" font-size="10" font-family="Segoe UI, system-ui, sans-serif" transform="rotate(-45 {x:.1f} {col_header_h - 10})">{col}</text>')

    # Cells
    for i, row in enumerate(rows):
        y = col_header_h + i * cell_h
        # Row label
        lines.append(f'  <text x="{label_w - 8}" y="{y + cell_h / 2 + 4}" text-anchor="end" fill="#A8A0B0" font-size="11" font-family="Segoe UI, system-ui, sans-serif">{row}</text>')

        for j in range(len(cols)):
            val = values[i][j] if j < len(values[i]) else 0
            x = label_w + j * cell_w

            # Color mapping
            if color_scheme == "diverging":
                color, text_color = _diverging_color(val, min_val, max_val)
            else:
                color, text_color = _sequential_color(val, min_val, max_val)

            lines.append(f'  <rect x="{x:.1f}" y="{y}" width="{cell_w:.1f}" height="{cell_h}" fill="{color}" stroke="#0D0B0E" stroke-width="1"/>')

            if show_values:
                display = f"{val:.2f}" if abs(val) < 10 else f"{val:.0f}"
                lines.append(f'  <text x="{x + cell_w / 2:.1f}" y="{y + cell_h / 2 + 4}" text-anchor="middle" fill="{text_color}" font-size="10" font-weight="600" font-family="Segoe UI, system-ui, sans-serif">{display}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


def _diverging_color(val: float, min_val: float, max_val: float) -> tuple[str, str]:
    """Red → Gold → Green color scale."""
    if val >= 0.7:
        return "rgba(61,184,106,0.30)", "#3DB86A"
    elif val >= 0.4:
        return "rgba(61,184,106,0.15)", "#3DB86A"
    elif val >= 0.1:
        return "rgba(201,164,76,0.15)", "#E8C46A"
    elif val >= -0.1:
        return "rgba(201,164,76,0.08)", "#706880"
    elif val >= -0.4:
        return "rgba(212,64,64,0.12)", "#D44040"
    else:
        return "rgba(212,64,64,0.25)", "#D44040"


def _sequential_color(val: float, min_val: float, max_val: float) -> tuple[str, str]:
    """Single-hue gold scale."""
    rng = max_val - min_val or 1
    norm = (val - min_val) / rng
    if norm >= 0.8:
        return "rgba(201,164,76,0.35)", "#E8C46A"
    elif norm >= 0.6:
        return "rgba(201,164,76,0.25)", "#C9A44C"
    elif norm >= 0.4:
        return "rgba(201,164,76,0.15)", "#C9A44C"
    elif norm >= 0.2:
        return "rgba(201,164,76,0.08)", "#A8A0B0"
    else:
        return "rgba(28,24,32,0.5)", "#706880"
