"""Shared fixtures for BSR test suite.

Provides sample ReportData objects for different business types,
minimal company dicts, and board review mock data.
"""

import json
from datetime import date
from pathlib import Path

import pytest

from app.config import BusinessType, ConfidenceLevel, LifecycleStage
from app.models import (
    CalcTrace,
    Company,
    Competitor,
    DigitalAudit,
    FactItem,
    FinancialYear,
    Founder,
    KPIBenchmark,
    LifecycleInfo,
    MarketDataPoint,
    MarketOverview,
    Opinion,
    Recommendation,
    ReportData,
    SalesChannel,
    Scenario,
    SocialAccount,
    SWOT,
    TimelineItem,
)


# ── Paths ──

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_DATA_DIR = PROJECT_ROOT / "test_data"


# ── Minimal company fixtures ──


@pytest.fixture
def company_b2c_service() -> Company:
    """Minimal B2C_SERVICE company (restaurant)."""
    return Company(
        name="Test Restaurant",
        legal_name='OOO "Test"',
        inn="7841000001",
        okved="56.10",
        business_type=BusinessType.B2C_SERVICE,
        address="Test Street 1, Moscow",
        website="https://test-restaurant.ru",
        description="A test restaurant.",
        badges=["Test badge"],
    )


@pytest.fixture
def company_b2b_service() -> Company:
    """Minimal B2B_SERVICE company (SaaS)."""
    return Company(
        name="Test SaaS",
        legal_name='OOO "TestSoft"',
        inn="7700000002",
        okved="62.01",
        business_type=BusinessType.B2B_SERVICE,
        website="https://test-saas.ru",
    )


@pytest.fixture
def company_hybrid() -> Company:
    """Minimal B2B_B2C_HYBRID company."""
    return Company(
        name="Test Hybrid",
        legal_name='OOO "HybridCo"',
        inn="7700000003",
        okved="20.42",
        business_type=BusinessType.B2B_B2C_HYBRID,
    )


# ── Sample competitors ──


@pytest.fixture
def sample_competitors() -> list[Competitor]:
    """3 sample competitors with radar scores, lifecycle, and sales channels."""
    return [
        Competitor(
            name="Competitor A",
            description="Strong competitor A",
            x=60,
            y=80,
            radar_scores={"Quality": 8, "Price": 5, "Service": 7},
            metrics={"Rating": "4.5", "Reviews": "1200"},
            threat_level="high",
            lifecycle=LifecycleInfo(
                stage=LifecycleStage.MATURE,
                evidence=["Stable revenue", "10+ years"],
                year_founded="2012",
            ),
            sales_channels=[
                SalesChannel(channel_name="Website", exists=True, source="2GIS"),
                SalesChannel(channel_name="Delivery", exists=False, source="Check"),
            ],
        ),
        Competitor(
            name="Competitor B",
            description="Growing competitor B",
            x=40,
            y=60,
            radar_scores={"Quality": 6, "Price": 7, "Service": 5},
            metrics={"Rating": "4.2", "Reviews": "500"},
            threat_level="med",
            lifecycle=LifecycleInfo(
                stage=LifecycleStage.GROWTH,
                evidence=["Revenue growing 30% YoY"],
                year_founded="2020",
            ),
            sales_channels=[
                SalesChannel(channel_name="Website", exists=True, source="Site"),
            ],
        ),
        Competitor(
            name="Competitor C",
            description="Startup competitor C",
            x=30,
            y=30,
            radar_scores={"Quality": 5, "Price": 9, "Service": 4},
            metrics={"Rating": "3.8"},
            threat_level="low",
            lifecycle=LifecycleInfo(
                stage=LifecycleStage.STARTUP,
                evidence=["Founded last year", "Fundraising"],
            ),
        ),
    ]


# ── Full ReportData (minimal but complete) ──


@pytest.fixture
def minimal_report_data(company_b2c_service, sample_competitors) -> ReportData:
    """Minimal ReportData with all key sections populated."""
    return ReportData(
        company=company_b2c_service,
        report_date=date(2026, 3, 1),
        market=MarketOverview(
            market_name="Test Market",
            market_size="100 bln RUB",
            growth_rate="+5%",
            data_points=[
                MarketDataPoint(year=2023, value=90),
                MarketDataPoint(year=2024, value=95),
                MarketDataPoint(year=2025, value=100),
            ],
            trends=["Trend A", "Trend B"],
            sources=["Source A"],
        ),
        competitors=sample_competitors,
        radar_dimensions=["Quality", "Price", "Service"],
        financials=[
            FinancialYear(year=2023, revenue=50_000_000, net_profit=5_000_000),
            FinancialYear(year=2024, revenue=60_000_000, net_profit=7_000_000),
        ],
        swot=SWOT(
            strengths=["Good location"],
            weaknesses=["Small team"],
            opportunities=["Growing market"],
            threats=["New competitors"],
        ),
        digital=DigitalAudit(
            social_accounts=[
                SocialAccount(platform="Instagram", handle="@test", followers=5000),
                SocialAccount(platform="Telegram", handle="@test_tg", followers=2000),
            ],
        ),
        market_share={"Test Restaurant": 15.0, "Competitor A": 25.0, "Others": 60.0},
        recommendations=[
            Recommendation(
                title="Expand delivery",
                description="Launch delivery to capture more orders",
                priority="high",
                timeline="Q2 2026",
            ),
        ],
        kpi_benchmarks=[
            KPIBenchmark(name="Average check", current=2500, benchmark=3000, unit="RUB"),
        ],
        scenarios=[
            Scenario(name="optimistic", label="Optimistic", metrics={"Revenue": 80}),
            Scenario(name="base", label="Base", metrics={"Revenue": 65}),
            Scenario(name="pessimistic", label="Pessimistic", metrics={"Revenue": 50}),
        ],
        glossary={"RevPASH": "Revenue Per Available Seat Hour"},
        factcheck=[
            FactItem(fact="Revenue 60M in 2024", sources_count=2, verified=True, sources=["FNS", "SBIS"]),
        ],
        founders=[
            Founder(name="Ivan Ivanov", role="CEO", share="100%", company="Test Restaurant"),
        ],
        opinions=[
            Opinion(author="Expert A", quote="Market is growing", date="2025-12", source="RBC"),
        ],
        calc_traces=[
            CalcTrace(
                metric_name="Average check",
                value=2500,
                formula="revenue / orders",
                inputs={"revenue": 60_000_000, "orders": 24_000},
                sources=["FNS", "Internal"],
                confidence=ConfidenceLevel.CALC,
            ),
        ],
        methodology={"data_collection": "Web scraping + FNS API", "period": "2023-2024"},
    )


# ── JSON test data loader ──


@pytest.fixture
def b2c_restaurant_json() -> dict:
    """Load b2c_restaurant.json test data as dict."""
    path = TEST_DATA_DIR / "b2c_restaurant.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def b2b_saas_json() -> dict:
    """Load b2b_saas.json test data as dict."""
    path = TEST_DATA_DIR / "b2b_saas.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def b2b_b2c_hybrid_json() -> dict:
    """Load b2b_b2c_hybrid.json test data as dict."""
    path = TEST_DATA_DIR / "b2b_b2c_hybrid.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Board review mock data ──


@pytest.fixture
def mock_board_reviews() -> dict:
    """Mock result of run_review() for testing apply_revisions."""
    return {
        "reviews": [
            {
                "role": "CFO",
                "name": "Financial Director",
                "response": {
                    "approved": True,
                    "critiques": [
                        {
                            "section": "financials",
                            "issue": "ROE not calculated",
                            "severity": "medium",
                            "suggestion": "Add ROE calculation",
                        },
                    ],
                    "summary": "Financials look solid, minor improvements needed.",
                },
            },
            {
                "role": "CMO",
                "name": "Marketing Director",
                "response": {
                    "approved": True,
                    "critiques": [],
                    "summary": "Marketing analysis is comprehensive.",
                },
            },
            {
                "role": "Industry Expert",
                "name": "Industry Expert",
                "response": {
                    "approved": True,
                    "critiques": [
                        {
                            "section": "market",
                            "issue": "Market size estimate seems outdated",
                            "severity": "low",
                            "suggestion": "Update to 2025 data",
                        },
                    ],
                    "summary": "Good industry coverage.",
                },
            },
            {
                "role": "Skeptic",
                "name": "Skeptic",
                "response": {
                    "approved": False,
                    "critiques": [
                        {
                            "section": "competitors",
                            "issue": "Competitor C data looks fabricated/hallucinated",
                            "severity": "high",
                            "suggestion": "Verify competitor C existence via 2GIS",
                        },
                    ],
                    "summary": "Possible hallucination in competitor data.",
                },
            },
            {
                "role": "QA Director",
                "name": "QA Director",
                "response": {
                    "approved": False,
                    "critiques": [
                        {
                            "section": "competitors",
                            "issue": "Нет подтверждения источников по конкуренту C",
                            "severity": "medium",
                            "suggestion": "Оставить только verified competitors",
                            "criteria": "empty_fields",
                        },
                    ],
                    "summary": "Качество данных по конкурентам недостаточно подтверждено.",
                },
            },
            {
                "role": "CEO",
                "name": "CEO",
                "response": {
                    "approved": False,
                    "critiques": [
                        {
                            "section": "competitors",
                            "issue": "Competitor C data unverified",
                            "severity": "high",
                            "suggestion": "Remove or verify",
                            "source_experts": ["Skeptic"],
                        },
                    ],
                    "accepted_critiques": [1],
                    "rejected_critiques": [],
                    "summary": "Fix the hallucination issue before publishing.",
                },
            },
        ],
        "consensus": {
            "approved": False,
            "critical_issues": 2,
            "total_critiques": 5,
        },
        "needs_revision": True,
        "timing": {
            "parallel_sec": 3.5,
            "ceo_sec": 2.1,
            "total_sec": 5.6,
        },
    }


@pytest.fixture
def mock_board_reviews_approved() -> dict:
    """Mock board review result where everything is approved."""
    return {
        "reviews": [
            {
                "role": "CFO",
                "name": "Financial Director",
                "response": {
                    "approved": True,
                    "critiques": [],
                    "summary": "All good.",
                },
            },
            {
                "role": "CMO",
                "name": "Marketing Director",
                "response": {
                    "approved": True,
                    "critiques": [],
                    "summary": "Marketing is solid.",
                },
            },
            {
                "role": "Industry Expert",
                "name": "Industry Expert",
                "response": {
                    "approved": True,
                    "critiques": [],
                    "summary": "Industry analysis is accurate.",
                },
            },
            {
                "role": "Skeptic",
                "name": "Skeptic",
                "response": {
                    "approved": True,
                    "critiques": [],
                    "summary": "No issues found.",
                },
            },
            {
                "role": "QA Director",
                "name": "QA Director",
                "response": {
                    "approved": True,
                    "critiques": [],
                    "summary": "Критических проблем качества не найдено.",
                },
            },
            {
                "role": "CEO",
                "name": "CEO",
                "response": {
                    "approved": True,
                    "critiques": [],
                    "accepted_critiques": [],
                    "rejected_critiques": [],
                    "summary": "Report is ready for publication.",
                },
            },
        ],
        "consensus": {
            "approved": True,
            "critical_issues": 0,
            "total_critiques": 0,
        },
        "needs_revision": False,
        "timing": {
            "parallel_sec": 3.0,
            "ceo_sec": 1.5,
            "total_sec": 4.5,
        },
    }
