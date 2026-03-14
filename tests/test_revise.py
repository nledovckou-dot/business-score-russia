"""Tests for deterministic post-board revision."""

from app.pipeline.steps.step7_revise import revise_report


def test_revise_gates_untrusted_competitors():
    report = {
        "company": {"name": "Test Co"},
        "section_gates": {},
        "failed_gates": [],
        "blocking_issues": [],
        "open_questions": [],
        "competitors": [
            {"name": "A", "description": "desc", "website": "https://a.test", "verification_sources": ["2GIS"]},
            {"name": "B", "description": "desc", "website": "https://b.test", "verification_sources": ["site"]},
            {"name": "C", "description": "desc", "website": "https://c.test"},
        ],
        "financials": [{"year": 2024, "revenue": 100}],
        "market_share": {"A": 40.0, "B": 30.0, "C": 30.0},
    }
    board_review = {
        "reviews": [
            {
                "role": "Skeptic",
                "response": {
                    "critiques": [
                        {
                            "section": "competitors",
                            "issue": "Competitor C data looks fabricated/hallucinated",
                            "severity": "high",
                            "suggestion": "Remove or verify",
                        },
                    ]
                },
            }
        ]
    }

    result = revise_report(report, board_review, {"name": "Test Co"})
    assert result["report_status"] == "draft"
    assert result["section_gates"]["C1"] is False
    assert any("fabricated" in issue or "hallucinated" in issue for issue in result["blocking_issues"])


def test_revise_drops_unsourced_quotes_and_empty_financials():
    report = {
        "company": {"name": "Test Co"},
        "section_gates": {},
        "failed_gates": [],
        "blocking_issues": [],
        "open_questions": [],
        "financials": [
            {"year": 2024, "revenue": None, "net_profit": None},
            {"year": 2023, "revenue": 1000, "net_profit": 10},
        ],
        "opinions": [
            {"author": "A", "quote": "Unsourced"},
            {"author": "B", "quote": "Sourced", "source": "РБК"},
        ],
        "competitors": [
            {"name": "A", "description": "desc", "website": "https://a.test", "verification_sources": ["2GIS"]},
            {"name": "B", "description": "desc", "website": "https://b.test", "verification_sources": ["site"]},
            {"name": "C", "description": "desc", "website": "https://c.test", "verification_sources": ["site"]},
        ],
        "market_share": {"A": 40.0, "B": 30.0, "C": 30.0},
    }
    board_review = {"reviews": []}

    result = revise_report(report, board_review, {"name": "Test Co"})
    assert len(result["financials"]) == 1
    assert len(result["opinions"]) == 1
    assert result["opinions"][0]["author"] == "B"


def test_revise_gates_invalid_market_share_without_renormalizing():
    report = {
        "company": {"name": "Test Co"},
        "section_gates": {},
        "failed_gates": [],
        "blocking_issues": [],
        "open_questions": [],
        "financials": [{"year": 2024, "revenue": 100}],
        "competitors": [
            {"name": "A", "description": "desc", "website": "https://a.test", "verification_sources": ["2GIS"]},
            {"name": "B", "description": "desc", "website": "https://b.test", "verification_sources": ["site"]},
            {"name": "C", "description": "desc", "website": "https://c.test", "verification_sources": ["site"]},
        ],
        "market_share": {"A": 80.0, "B": 40.0, "C": "unknown"},
    }
    board_review = {
        "reviews": [
            {
                "role": "QA Director",
                "response": {
                    "critiques": [
                        {
                            "section": "market_share",
                            "issue": "Market share unverified and inconsistent",
                            "severity": "medium",
                            "suggestion": "Hide section until verified",
                        },
                    ]
                },
            }
        ]
    }

    result = revise_report(report, board_review, {"name": "Test Co"})
    assert "C" not in result["market_share"]
    assert result["section_gates"]["P10"] is False
    assert any("Доли рынка несогласованы" in issue for issue in result["blocking_issues"])
