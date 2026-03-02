"""Test script: builds reports from test data and opens them in browser."""

import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.models import ReportData
from app.report.builder import save_report


def load_and_build(json_path: str, output_name: str) -> Path:
    """Load test data JSON and build a report."""
    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data = ReportData(**raw)
    path = save_report(data, filename=output_name)
    print(f"  Built: {path} ({path.stat().st_size / 1024:.0f} KB)")
    return path


def main():
    base = Path(__file__).parent
    test_dir = base / "test_data"

    print("Building test reports...\n")

    # B2B SaaS
    p1 = load_and_build(str(test_dir / "b2b_saas.json"), "test_b2b_saas.html")

    # B2C Restaurant
    p2 = load_and_build(str(test_dir / "b2c_restaurant.json"), "test_b2c_restaurant.html")

    # B2B+B2C Hybrid
    p3 = load_and_build(str(test_dir / "b2b_b2c_hybrid.json"), "test_b2b_b2c_hybrid.html")

    print(f"\nDone! Reports saved to:")
    print(f"  B2B: {p1}")
    print(f"  B2C: {p2}")
    print(f"  HYBRID: {p3}")
    print(f"\nOpen in browser:")
    print(f"  open '{p1}'")
    print(f"  open '{p2}'")
    print(f"  open '{p3}'")


if __name__ == "__main__":
    main()
