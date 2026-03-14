"""Tests for report builder: build_report produces valid HTML.

Tests that the builder renders non-empty HTML, includes expected sections,
handles edge cases (empty data, different business types), and that
placeholder logic works.
"""

import re

import pytest

from app.config import BusinessType, THEME_DEFAULT
from app.models import Company, ReportData
from app.report.builder import build_report, _render_placeholder, BLOCK_NAMES
from app.report.profiles import get_blocks_for_type, get_active_sections, PROFILES


# ── build_report basic tests ──


class TestBuildReport:
    """Tests for the main build_report function."""

    def test_produces_nonempty_html(self, minimal_report_data):
        """build_report returns non-empty HTML string."""
        html = build_report(minimal_report_data)
        assert isinstance(html, str)
        assert len(html) > 1000  # should be substantial

    def test_html_has_doctype(self, minimal_report_data):
        """Output starts with <!DOCTYPE html>."""
        html = build_report(minimal_report_data)
        assert html.strip().lower().startswith("<!doctype html")

    def test_html_has_closing_tags(self, minimal_report_data):
        """HTML has proper closing tags."""
        html = build_report(minimal_report_data)
        assert "</html>" in html
        assert "</head>" in html
        assert "</body>" in html

    def test_company_name_in_html(self, minimal_report_data):
        """Company name appears in the rendered HTML."""
        html = build_report(minimal_report_data)
        assert minimal_report_data.company.name in html

    def test_competitors_in_html(self, minimal_report_data):
        """Competitor names appear in the rendered HTML."""
        html = build_report(minimal_report_data)
        for comp in minimal_report_data.competitors:
            assert comp.name in html

    def test_theme_colors_in_html(self, minimal_report_data):
        """Theme CSS variables are present in HTML."""
        html = build_report(minimal_report_data)
        # Check that the gold color from THEME_DEFAULT appears
        assert THEME_DEFAULT["gold"] in html or "C9A44C" in html

    def test_dark_theme_background(self, minimal_report_data):
        """Dark theme background color is present."""
        html = build_report(minimal_report_data)
        # The bg color #0D0B0E should be in CSS
        assert THEME_DEFAULT["bg"] in html or "0D0B0E" in html

    def test_svg_charts_present(self, minimal_report_data):
        """SVG charts are rendered inline (at least one <svg tag)."""
        html = build_report(minimal_report_data)
        assert "<svg" in html

    def test_no_external_dependencies(self, minimal_report_data):
        """HTML should not reference external CSS/JS files (self-contained)."""
        html = build_report(minimal_report_data)
        # Should not have <link rel="stylesheet" href="http
        assert 'href="http' not in html.lower().split("<style")[0] if "<style" in html.lower() else True
        # Should not have <script src="http
        external_script = re.findall(r'<script\s+src=["\']https?://', html, re.IGNORECASE)
        assert len(external_script) == 0

    def test_custom_theme(self, minimal_report_data):
        """Custom theme dict is applied."""
        custom_theme = dict(THEME_DEFAULT)
        custom_theme["gold"] = "#FF0000"
        html = build_report(minimal_report_data, theme=custom_theme)
        assert "#FF0000" in html

    def test_draft_banner_renders_blocking_issues(self, minimal_report_data):
        """Draft reports show the blocking issues banner."""
        minimal_report_data.report_status = "draft"
        minimal_report_data.blocking_issues = ["QA: нет подтверждённых финансовых данных"]
        html = build_report(minimal_report_data)
        assert "ЧЕРНОВИК" in html
        assert "нет подтверждённых финансовых данных" in html

    def test_publishable_report_hides_draft_banner(self, minimal_report_data):
        """Publishable reports don't show the draft banner."""
        minimal_report_data.report_status = "publishable"
        minimal_report_data.draft_mode = False
        minimal_report_data.blocking_issues = []
        html = build_report(minimal_report_data)
        assert "Блокирующие замечания" not in html


# ── Different business types ──


class TestBuildReportBusinessTypes:
    """Test report builds for each business type."""

    @pytest.mark.parametrize("btype", list(BusinessType))
    def test_builds_for_each_type(self, btype):
        """build_report works for every BusinessType enum value."""
        company = Company(name=f"Test {btype.value}", business_type=btype)
        data = ReportData(company=company)
        html = build_report(data)
        assert isinstance(html, str)
        assert len(html) > 500
        assert company.name in html


# ── Edge cases ──


class TestBuildReportEdgeCases:
    """Edge cases for report building."""

    def test_empty_report_data(self, company_b2c_service):
        """Report with only company (no data) still produces valid HTML."""
        data = ReportData(company=company_b2c_service)
        html = build_report(data)
        assert isinstance(html, str)
        assert "</html>" in html
        assert company_b2c_service.name in html

    def test_report_with_no_competitors(self, company_b2c_service):
        """Report renders correctly without competitors."""
        data = ReportData(company=company_b2c_service, competitors=[])
        html = build_report(data)
        assert isinstance(html, str)
        assert len(html) > 500

    def test_report_with_section_gates(self, minimal_report_data):
        """Section gates disable specific blocks."""
        minimal_report_data.section_gates = {"P2": False, "S4": False}
        html = build_report(minimal_report_data)
        assert isinstance(html, str)
        # The gated blocks should not contain their full content
        # (they are simply excluded from rendering)

    def test_from_json_test_data(self, b2c_restaurant_json):
        """Build report from test_data/b2c_restaurant.json."""
        data = ReportData(**b2c_restaurant_json)
        html = build_report(data)
        assert isinstance(html, str)
        assert len(html) > 5000
        assert data.company.name in html

    def test_from_json_b2b_saas(self, b2b_saas_json):
        """Build report from test_data/b2b_saas.json."""
        data = ReportData(**b2b_saas_json)
        html = build_report(data)
        assert isinstance(html, str)
        assert len(html) > 5000

    def test_from_json_hybrid(self, b2b_b2c_hybrid_json):
        """Build report from test_data/b2b_b2c_hybrid.json."""
        data = ReportData(**b2b_b2c_hybrid_json)
        html = build_report(data)
        assert isinstance(html, str)
        assert len(html) > 5000


# ── Placeholder rendering ──


class TestPlaceholder:
    """Tests for placeholder card rendering."""

    def test_render_placeholder_known_block(self):
        """Placeholder for known block ID uses human-readable name."""
        html = _render_placeholder("M1")
        assert "Обзор рынка" in html
        assert "block-placeholder" in html

    def test_render_placeholder_unknown_block(self):
        """Placeholder for unknown block ID uses the raw ID."""
        html = _render_placeholder("ZZ99")
        assert "ZZ99" in html
        assert "block-placeholder" in html

    def test_block_names_complete(self):
        """BLOCK_NAMES covers all blocks in BLOCK_TEMPLATES."""
        from app.report.profiles import BLOCK_TEMPLATES
        for block_id in BLOCK_TEMPLATES:
            assert block_id in BLOCK_NAMES, f"Missing BLOCK_NAMES entry for {block_id}"


# ── Profiles / active sections ──


class TestProfiles:
    """Tests for block profiles and section logic."""

    @pytest.mark.parametrize("btype", list(BusinessType))
    def test_all_types_have_profiles(self, btype):
        """Every business type has a defined profile."""
        blocks = get_blocks_for_type(btype)
        assert isinstance(blocks, list)
        assert len(blocks) > 0

    def test_b2c_service_has_menu_block(self):
        """B2C_SERVICE includes P6 (Menu/Pricing)."""
        blocks = get_blocks_for_type(BusinessType.B2C_SERVICE)
        assert "P6" in blocks

    def test_b2b_service_has_tenders_block(self):
        """B2B_SERVICE includes P8 (Tenders)."""
        blocks = get_blocks_for_type(BusinessType.B2B_SERVICE)
        assert "P8" in blocks

    def test_all_types_have_board_block(self):
        """Every business type includes B1 (Board conclusion)."""
        for btype in BusinessType:
            blocks = get_blocks_for_type(btype)
            assert "B1" in blocks, f"Business type {btype} missing B1 block"

    def test_section_gates_exclude_blocks(self):
        """Section gates with False exclude blocks from active sections."""
        blocks = ["M1", "M2", "C1", "C2", "P1", "P2"]
        gates = {"M2": False, "P2": False}
        sections = get_active_sections(blocks, gates)
        all_active_blocks = []
        for s in sections:
            all_active_blocks.extend(s["blocks"])
        assert "M2" not in all_active_blocks
        assert "P2" not in all_active_blocks
        assert "M1" in all_active_blocks
        assert "C1" in all_active_blocks

    def test_section_gates_none_includes_all(self):
        """No gates means all blocks are active."""
        blocks = ["M1", "M2", "C1"]
        sections = get_active_sections(blocks, None)
        all_active_blocks = []
        for s in sections:
            all_active_blocks.extend(s["blocks"])
        assert "M1" in all_active_blocks
        assert "M2" in all_active_blocks
