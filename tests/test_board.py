"""Tests for Board of Directors review: form_panel, apply_revisions, parse helpers.

Tests pure Python logic only — NO LLM calls (run_review requires API keys).
"""

import json

import pytest

from app.pipeline.steps.step6_board import (
    _parse_expert_response,
    _truncate_report,
    apply_revisions,
    form_panel,
)


# ── form_panel ──


class TestFormPanel:
    """Tests for panel formation."""

    def test_panel_has_6_experts(self):
        """Panel always contains exactly 6 experts."""
        panel = form_panel(
            report_data={"company": {"name": "Test"}},
            company_info={"name": "Test Co", "business_type": "B2C_SERVICE"},
        )
        assert len(panel) == 6

    def test_panel_roles(self):
        """Panel contains the expected expert roles."""
        panel = form_panel(
            report_data={},
            company_info={"name": "Test", "business_type": "B2B_SERVICE"},
        )
        roles = {e["role"] for e in panel}
        assert roles == {"CFO", "CMO", "Industry Expert", "Skeptic", "QA Director", "CEO"}

    def test_panel_expert_has_required_keys(self):
        """Each expert dict has role, name, system, focus_areas."""
        panel = form_panel(
            report_data={},
            company_info={"name": "Test"},
        )
        for expert in panel:
            assert "role" in expert
            assert "name" in expert
            assert "system" in expert
            assert "focus_areas" in expert
            assert isinstance(expert["focus_areas"], list)
            assert len(expert["focus_areas"]) > 0

    def test_panel_includes_company_context(self):
        """Each expert's system prompt contains company name."""
        panel = form_panel(
            report_data={},
            company_info={"name": "Maison Rouge", "business_type": "B2C_SERVICE"},
        )
        for expert in panel:
            assert "Maison Rouge" in expert["system"]
            assert "B2C_SERVICE" in expert["system"]

    def test_panel_default_business_type(self):
        """Default business type is used when not specified."""
        panel = form_panel(
            report_data={},
            company_info={"name": "Unknown"},
        )
        for expert in panel:
            assert "B2C_SERVICE" in expert["system"]

    def test_panel_default_company_name(self):
        """Default company name 'Kompaniya' is used when not specified."""
        panel = form_panel(
            report_data={},
            company_info={},
        )
        for expert in panel:
            assert "Компания" in expert["system"]

    def test_cfo_focus_areas(self):
        """CFO focuses on financials, calc_traces, kpi_benchmarks."""
        panel = form_panel(report_data={}, company_info={"name": "X"})
        cfo = next(e for e in panel if e["role"] == "CFO")
        assert "financials" in cfo["focus_areas"]
        assert "calc_traces" in cfo["focus_areas"]

    def test_skeptic_focus_areas(self):
        """Skeptic focuses on factcheck, sources, methodology."""
        panel = form_panel(report_data={}, company_info={"name": "X"})
        skeptic = next(e for e in panel if e["role"] == "Skeptic")
        assert "factcheck" in skeptic["focus_areas"]
        assert "methodology" in skeptic["focus_areas"]

    def test_ceo_is_last(self):
        """CEO should be the last expert in the panel."""
        panel = form_panel(report_data={}, company_info={"name": "X"})
        assert panel[-1]["role"] == "CEO"


# ── _parse_expert_response ──


class TestParseExpertResponse:
    """Tests for JSON parsing of expert responses."""

    def test_valid_json(self):
        """Valid JSON response is parsed correctly."""
        raw = json.dumps({
            "approved": True,
            "critiques": [
                {"section": "financials", "issue": "Minor rounding", "severity": "low", "suggestion": "Fix"}
            ],
            "summary": "Looks good overall.",
        })
        result = _parse_expert_response(raw, "CFO")
        assert result["approved"] is True
        assert len(result["critiques"]) == 1
        assert result["critiques"][0]["severity"] == "low"

    def test_markdown_wrapped_json(self):
        """JSON wrapped in ```json ... ``` is parsed correctly."""
        raw = '```json\n{"approved": false, "critiques": [], "summary": "Issues found"}\n```'
        result = _parse_expert_response(raw, "CMO")
        assert result["approved"] is False
        assert result["summary"] == "Issues found"

    def test_markdown_without_lang(self):
        """JSON wrapped in ``` ... ``` (no language) is parsed."""
        raw = '```\n{"approved": true, "critiques": [], "summary": "OK"}\n```'
        result = _parse_expert_response(raw, "Skeptic")
        assert result["approved"] is True

    def test_invalid_json_returns_fallback(self):
        """Invalid JSON returns a fallback structure with parse_error."""
        raw = "This is not JSON at all"
        result = _parse_expert_response(raw, "Industry Expert")
        assert result["approved"] is False
        assert result.get("_parse_error") is True
        assert len(result["critiques"]) == 1
        assert result["critiques"][0]["section"] == "parse_error"

    def test_missing_approved_defaults_false(self):
        """Response without 'approved' field defaults to False."""
        raw = json.dumps({"critiques": [], "summary": "No verdict"})
        result = _parse_expert_response(raw, "CEO")
        assert result["approved"] is False

    def test_missing_critiques_defaults_empty(self):
        """Response without 'critiques' field defaults to empty list."""
        raw = json.dumps({"approved": True, "summary": "All good"})
        result = _parse_expert_response(raw, "CFO")
        assert result["critiques"] == []

    def test_missing_summary_gets_default(self):
        """Response without 'summary' gets a default message."""
        raw = json.dumps({"approved": True, "critiques": []})
        result = _parse_expert_response(raw, "CMO")
        assert "CMO" in result["summary"]

    def test_severity_normalization(self):
        """Invalid severity values are normalized to 'medium'."""
        raw = json.dumps({
            "approved": False,
            "critiques": [
                {"section": "X", "issue": "Y", "severity": "CRITICAL", "suggestion": "Z"},
                {"section": "A", "issue": "B", "severity": "High", "suggestion": "C"},
                {"section": "D", "issue": "E", "severity": "low", "suggestion": "F"},
            ],
            "summary": "Test",
        })
        result = _parse_expert_response(raw, "Skeptic")
        assert result["critiques"][0]["severity"] == "medium"  # CRITICAL -> medium
        assert result["critiques"][1]["severity"] == "high"    # High -> high (lowercased)
        assert result["critiques"][2]["severity"] == "low"     # low stays low


# ── _truncate_report ──


class TestTruncateReport:
    """Tests for report truncation before sending to LLM."""

    def test_small_report_unchanged(self):
        """Small report passes through without truncation."""
        data = {"company": {"name": "Test"}, "financials": []}
        result = _truncate_report(data)
        parsed = json.loads(result)
        assert parsed["company"]["name"] == "Test"

    def test_large_report_truncated(self):
        """Large report is truncated to fit within limit."""
        # Create a large report with many opinions
        data = {
            "company": {"name": "Test"},
            "opinions": [{"author": f"Expert {i}", "quote": "x" * 500} for i in range(100)],
            "founders": [{"name": f"Founder {i}", "role": "CEO"} for i in range(50)],
            "factcheck": [{"fact": f"Fact {i}" * 100} for i in range(50)],
        }
        result = _truncate_report(data, max_chars=5000)
        assert len(result) <= 5500  # some tolerance for the truncation note

    def test_preserves_company_info(self):
        """Even when truncating, company info is preserved."""
        data = {
            "company": {"name": "Important Corp", "inn": "1234567890"},
            "opinions": [{"author": f"E{i}", "quote": "x" * 1000} for i in range(100)],
        }
        result = _truncate_report(data, max_chars=3000)
        assert "Important Corp" in result

    def test_truncation_order(self):
        """Heavy keys are truncated in priority order."""
        data = {
            "company": {"name": "Test"},
            "opinions": [{"author": f"E{i}", "quote": "q" * 300} for i in range(20)],
            "founders": [{"name": f"F{i}"} for i in range(20)],
            "financials": [{"year": 2024, "revenue": 100}],  # should be preserved
        }
        result = _truncate_report(data, max_chars=3000)
        parsed = json.loads(result)
        # Opinions and founders should be trimmed
        if "opinions" in parsed and isinstance(parsed["opinions"], list):
            assert len(parsed["opinions"]) <= 3
        # Financials should be preserved
        assert "financials" in parsed


# ── apply_revisions ──


class TestApplyRevisions:
    """Tests for applying board review results to report data."""

    def test_adds_board_review_key(self, mock_board_reviews):
        """apply_revisions adds board_review to report_data."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews)
        assert "board_review" in result
        assert "reviews" in result["board_review"]
        assert "consensus" in result["board_review"]

    def test_board_review_structure(self, mock_board_reviews):
        """Board review has expected structure for template."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews)
        br = result["board_review"]

        # Check reviews structure
        assert len(br["reviews"]) == 6
        for review in br["reviews"]:
            assert "role" in review
            assert "name" in review
            assert "approved" in review
            assert "summary" in review
            assert "critiques" in review

    def test_consensus_propagated(self, mock_board_reviews):
        """Consensus is correctly propagated."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews)
        consensus = result["board_review"]["consensus"]
        assert consensus["approved"] is False
        assert consensus["critical_issues"] == 2

    def test_approved_review(self, mock_board_reviews_approved):
        """Approved review has correct consensus."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews_approved)
        assert result["board_review"]["consensus"]["approved"] is True
        assert result["board_review"]["consensus"]["critical_issues"] == 0
        assert result.get("report_status", "draft") != "draft"

    def test_hallucination_gates_blocks(self, mock_board_reviews):
        """High-severity hallucination critiques gate the related blocks."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews)
        gates = result["section_gates"]
        # The mock has "hallucinated" competitor data -> C1, C2, C3, C4 should be gated
        # The mock critique says "fabricated/hallucinated" in the issue field
        # and "competitors" in section, matching "конкурент" key
        gated_blocks = [k for k, v in gates.items() if not v]
        # The competitor blocks should be gated due to hallucination keyword
        assert any(b in gated_blocks for b in ["C1", "C2", "C3", "C4"])

    def test_no_hallucination_no_gating(self, mock_board_reviews_approved):
        """Approved review doesn't gate any blocks."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews_approved)
        gates = result["section_gates"]
        gated = [k for k, v in gates.items() if not v]
        assert len(gated) == 0

    def test_warnings_added_to_open_questions(self, mock_board_reviews):
        """High-severity warnings are added to open_questions."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": ["Existing question"]}
        result = apply_revisions(report, mock_board_reviews)
        oq = result["open_questions"]
        assert len(oq) > 1  # original + board warnings
        assert oq[0] == "Existing question"
        # Board warnings contain expert role
        board_questions = [q for q in oq if "[Совет директоров" in q]
        assert len(board_questions) > 0

    def test_failed_review_adds_blocking_issue(self, mock_board_reviews):
        """Rejected board review becomes a blocking issue."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews)
        assert result["report_status"] == "draft"
        assert any("Совет директоров не одобрил отчёт" in issue for issue in result["blocking_issues"])

    def test_max_5_board_warnings(self):
        """At most 5 board warnings are added to open_questions."""
        # Create reviews with many high-severity critiques
        reviews = {
            "reviews": [
                {
                    "role": "Skeptic",
                    "name": "Skeptic",
                    "response": {
                        "approved": False,
                        "critiques": [
                            {"section": f"section_{i}", "issue": f"Issue {i}", "severity": "high", "suggestion": f"Fix {i}"}
                            for i in range(10)
                        ],
                        "summary": "Many problems.",
                    },
                },
            ],
            "consensus": {"approved": False, "critical_issues": 10, "total_critiques": 10},
            "needs_revision": True,
        }
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, reviews)
        board_questions = [q for q in result["open_questions"] if "[Совет директоров" in q]
        assert len(board_questions) <= 5

    def test_preserves_existing_section_gates(self):
        """Existing section gates are preserved, new ones added."""
        reviews = {
            "reviews": [
                {
                    "role": "Skeptic",
                    "name": "Skeptic",
                    "response": {
                        "approved": False,
                        "critiques": [
                            {
                                "section": "market",
                                "issue": "Data looks fabricated/hallucinated",
                                "severity": "high",
                                "suggestion": "Verify",
                            },
                        ],
                        "summary": "Hallucination detected.",
                    },
                },
            ],
            "consensus": {"approved": False, "critical_issues": 1, "total_critiques": 1},
        }
        report = {
            "company": {"name": "Test"},
            "section_gates": {"P2": False},  # pre-existing gate
            "open_questions": [],
        }
        result = apply_revisions(report, reviews)
        gates = result["section_gates"]
        assert gates["P2"] is False  # preserved
        # M1 should now be gated because "market" matches the section mapping
        assert gates.get("M1") is False

    def test_timing_preserved(self, mock_board_reviews):
        """Timing info is preserved in board_review."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews)
        assert "timing" in result["board_review"]
        assert result["board_review"]["timing"]["total_sec"] == 5.6

    def test_no_critiques_no_warnings(self, mock_board_reviews_approved):
        """No critiques means no warnings added."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, mock_board_reviews_approved)
        board_questions = [q for q in result["open_questions"] if "[Совет директоров" in q]
        assert len(board_questions) == 0

    def test_duplicate_questions_not_added(self, mock_board_reviews):
        """Same warning is not added twice to open_questions."""
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result1 = apply_revisions(report, mock_board_reviews)
        # Apply again
        result2 = apply_revisions(result1, mock_board_reviews)
        board_questions = [q for q in result2["open_questions"] if "[Совет директоров" in q]
        # Each unique question should appear only once
        assert len(board_questions) == len(set(board_questions))

    def test_empty_reviews(self):
        """apply_revisions handles empty reviews gracefully."""
        reviews = {
            "reviews": [],
            "consensus": {"approved": True, "critical_issues": 0, "total_critiques": 0},
        }
        report = {"company": {"name": "Test"}, "section_gates": {}, "open_questions": []}
        result = apply_revisions(report, reviews)
        assert result["board_review"]["consensus"]["approved"] is True
        assert len(result["board_review"]["reviews"]) == 0
