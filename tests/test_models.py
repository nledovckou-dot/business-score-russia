"""Tests for Pydantic models: Company, Competitor, ReportData, and sub-models.

Tests validation, defaults, serialization, and edge cases.
"""

import json
from datetime import date

import pytest
from pydantic import ValidationError

from app.config import BusinessType, ConfidenceLevel, LifecycleStage
from app.models import (
    CalcTrace,
    Company,
    Competitor,
    CorrelationPair,
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


# ── Company model ──


class TestCompany:
    """Tests for Company model validation."""

    def test_minimal_company(self):
        """Company with only required fields."""
        c = Company(name="Test", business_type=BusinessType.B2C_SERVICE)
        assert c.name == "Test"
        assert c.business_type == BusinessType.B2C_SERVICE
        assert c.inn is None
        assert c.badges == []

    def test_full_company(self, company_b2c_service):
        """Company with all fields populated."""
        c = company_b2c_service
        assert c.name == "Test Restaurant"
        assert c.inn == "7841000001"
        assert c.okved == "56.10"
        assert c.business_type == BusinessType.B2C_SERVICE
        assert len(c.badges) == 1

    def test_company_missing_name_raises(self):
        """Company without name should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            Company(business_type=BusinessType.B2C_SERVICE)
        assert "name" in str(exc_info.value)

    def test_company_missing_business_type_raises(self):
        """Company without business_type should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            Company(name="Test")
        assert "business_type" in str(exc_info.value)

    def test_company_invalid_business_type_raises(self):
        """Company with invalid business type should fail validation."""
        with pytest.raises(ValidationError):
            Company(name="Test", business_type="INVALID_TYPE")

    def test_all_business_types(self):
        """All BusinessType enum values are accepted."""
        for bt in BusinessType:
            c = Company(name=f"Test {bt.value}", business_type=bt)
            assert c.business_type == bt

    def test_company_serialization_roundtrip(self, company_b2c_service):
        """Company can be serialized to dict and back."""
        d = company_b2c_service.model_dump()
        c2 = Company(**d)
        assert c2.name == company_b2c_service.name
        assert c2.business_type == company_b2c_service.business_type
        assert c2.inn == company_b2c_service.inn

    def test_company_json_roundtrip(self, company_b2c_service):
        """Company serializes to JSON and deserializes correctly."""
        json_str = company_b2c_service.model_dump_json()
        c2 = Company.model_validate_json(json_str)
        assert c2 == company_b2c_service


# ── Competitor model ──


class TestCompetitor:
    """Tests for Competitor model."""

    def test_minimal_competitor(self):
        """Competitor with only required field (name)."""
        c = Competitor(name="Rival")
        assert c.name == "Rival"
        assert c.x == 50.0
        assert c.y == 50.0
        assert c.threat_level == "med"
        assert c.radar_scores == {}
        assert c.lifecycle is None
        assert c.sales_channels == []
        assert c.verified is True
        assert c.verification_confidence == "unverified"

    def test_competitor_with_lifecycle(self):
        """Competitor with lifecycle info."""
        li = LifecycleInfo(
            stage=LifecycleStage.INVESTMENT,
            evidence=["Building factory", "M&A activity"],
            year_founded="2015",
        )
        c = Competitor(name="InvestCo", lifecycle=li)
        assert c.lifecycle.stage == LifecycleStage.INVESTMENT
        assert len(c.lifecycle.evidence) == 2

    def test_competitor_with_sales_channels(self):
        """Competitor with sales channels."""
        channels = [
            SalesChannel(channel_name="WB", exists=True, source="mpstats"),
            SalesChannel(channel_name="Ozon", exists=False, source="check"),
            SalesChannel(channel_name="D2C", exists=None),
        ]
        c = Competitor(name="ChannelCo", sales_channels=channels)
        assert len(c.sales_channels) == 3
        assert c.sales_channels[0].exists is True
        assert c.sales_channels[1].exists is False
        assert c.sales_channels[2].exists is None  # unknown

    def test_competitor_radar_scores(self):
        """Competitor radar scores dict."""
        scores = {"Quality": 8.5, "Price": 3.0, "Speed": 7.0}
        c = Competitor(name="R", radar_scores=scores)
        assert c.radar_scores["Quality"] == 8.5
        assert len(c.radar_scores) == 3

    def test_competitor_verified_flags(self):
        """Competitor verification fields."""
        c = Competitor(
            name="Verified",
            verified=True,
            verification_confidence="high",
            verification_sources=["2GIS", "Yandex Maps"],
            verification_notes="Confirmed active",
        )
        assert c.verified is True
        assert c.verification_confidence == "high"
        assert len(c.verification_sources) == 2


# ── Financial models ──


class TestFinancialYear:
    """Tests for FinancialYear model."""

    def test_minimal(self):
        fy = FinancialYear(year=2024)
        assert fy.year == 2024
        assert fy.revenue is None
        assert fy.employees is None

    def test_full(self):
        fy = FinancialYear(
            year=2024,
            revenue=100_000_000,
            net_profit=10_000_000,
            assets=50_000_000,
            equity=30_000_000,
            liabilities=20_000_000,
            employees=50,
        )
        assert fy.revenue == 100_000_000
        assert fy.employees == 50


class TestCalcTrace:
    """Tests for CalcTrace (v2.0 transparency)."""

    def test_minimal(self):
        ct = CalcTrace(metric_name="Test", value=100)
        assert ct.confidence == ConfidenceLevel.ESTIMATE  # default

    def test_full(self):
        ct = CalcTrace(
            metric_name="LTV",
            value=85000,
            formula="avg_order * frequency * lifetime",
            inputs={"avg_order": 42500, "frequency": 4, "lifetime": 0.5},
            sources=["mpstats", "internal"],
            confidence=ConfidenceLevel.CALC,
        )
        assert ct.confidence == ConfidenceLevel.CALC
        assert ct.inputs["avg_order"] == 42500

    def test_confidence_levels(self):
        for level in ConfidenceLevel:
            ct = CalcTrace(metric_name="X", value=0, confidence=level)
            assert ct.confidence == level


# ── SWOT model ──


class TestSWOT:
    """Tests for SWOT model."""

    def test_empty(self):
        s = SWOT()
        assert s.strengths == []
        assert s.weaknesses == []
        assert s.opportunities == []
        assert s.threats == []

    def test_populated(self):
        s = SWOT(
            strengths=["A", "B"],
            weaknesses=["C"],
            opportunities=["D", "E", "F"],
            threats=["G"],
        )
        assert len(s.strengths) == 2
        assert len(s.opportunities) == 3


# ── Digital audit ──


class TestDigitalAudit:
    """Tests for DigitalAudit and SocialAccount."""

    def test_empty(self):
        d = DigitalAudit()
        assert d.social_accounts == []
        assert d.seo_score is None

    def test_with_accounts(self):
        accs = [
            SocialAccount(platform="Instagram", handle="@test", followers=5000, verified=True),
            SocialAccount(platform="Telegram", handle="@test_tg", followers=2000),
        ]
        d = DigitalAudit(social_accounts=accs, seo_score=75.0, monthly_traffic=10000)
        assert len(d.social_accounts) == 2
        assert d.social_accounts[0].verified is True
        assert d.seo_score == 75.0


# ── Other sub-models ──


class TestSubModels:
    """Quick tests for remaining sub-models."""

    def test_market_data_point(self):
        p = MarketDataPoint(year=2025, value=180.5, label="bln RUB")
        assert p.year == 2025

    def test_market_overview(self):
        m = MarketOverview(market_name="HoReCa SPB")
        assert m.market_name == "HoReCa SPB"
        assert m.data_points == []

    def test_recommendation(self):
        r = Recommendation(title="Do X", description="Because Y", priority="high")
        assert r.priority == "high"
        assert r.timeline is None

    def test_scenario(self):
        s = Scenario(name="base", label="Base", metrics={"revenue": 65.0})
        assert s.metrics["revenue"] == 65.0

    def test_kpi_benchmark(self):
        k = KPIBenchmark(name="Check", current=2500, benchmark=3000, unit="RUB")
        assert k.current == 2500

    def test_fact_item(self):
        f = FactItem(fact="Revenue is 60M", sources_count=2, verified=True, sources=["FNS", "SBIS"])
        assert f.verified is True
        assert f.sources_count == 2

    def test_founder(self):
        f = Founder(name="Ivan", role="CEO", share="100%", social={"tg": "@ivan"})
        assert f.social["tg"] == "@ivan"

    def test_opinion(self):
        o = Opinion(author="Expert", quote="Market is growing", date="2025-12")
        assert o.author == "Expert"

    def test_correlation_pair(self):
        cp = CorrelationPair(metric_a="Price", metric_b="Rating", value=0.85)
        assert cp.value == 0.85

    def test_timeline_item(self):
        t = TimelineItem(date="2025-Q1", title="Launch", color="green")
        assert t.color == "green"

    def test_lifecycle_info_defaults(self):
        li = LifecycleInfo()
        assert li.stage == LifecycleStage.MATURE  # default
        assert li.evidence == []

    def test_sales_channel(self):
        sc = SalesChannel(channel_name="WB", exists=True, source="mpstats", url="https://wb.ru/seller/123")
        assert sc.exists is True
        assert sc.url is not None


# ── Full ReportData ──


class TestReportData:
    """Tests for the top-level ReportData model."""

    def test_minimal_report(self, company_b2c_service):
        """ReportData with only company (required)."""
        rd = ReportData(company=company_b2c_service)
        assert rd.company.name == "Test Restaurant"
        assert rd.competitors == []
        assert rd.financials == []
        assert rd.pipeline_version == "2.0"
        assert rd.board_review == {}
        assert rd.report_status == "draft"
        assert rd.blocking_issues == []
        assert rd.quality_summary == {}

    def test_report_date_default(self, company_b2c_service):
        """Report date defaults to today."""
        rd = ReportData(company=company_b2c_service)
        assert rd.report_date == date.today()

    def test_full_report(self, minimal_report_data):
        """Full report with all sections validates correctly."""
        rd = minimal_report_data
        assert rd.company.name == "Test Restaurant"
        assert len(rd.competitors) == 3
        assert len(rd.financials) == 2
        assert rd.swot is not None
        assert rd.digital is not None
        assert len(rd.calc_traces) == 1
        assert len(rd.factcheck) == 1
        assert len(rd.founders) == 1

    def test_report_serialization_roundtrip(self, minimal_report_data):
        """ReportData dict roundtrip."""
        d = minimal_report_data.model_dump()
        rd2 = ReportData(**d)
        assert rd2.company.name == minimal_report_data.company.name
        assert len(rd2.competitors) == len(minimal_report_data.competitors)

    def test_report_json_roundtrip(self, minimal_report_data):
        """ReportData JSON roundtrip."""
        json_str = minimal_report_data.model_dump_json()
        rd2 = ReportData.model_validate_json(json_str)
        assert rd2.company == minimal_report_data.company

    def test_report_from_test_data_b2c(self, b2c_restaurant_json):
        """ReportData from test_data/b2c_restaurant.json."""
        rd = ReportData(**b2c_restaurant_json)
        assert rd.company.business_type == BusinessType.B2C_SERVICE
        assert len(rd.competitors) > 0

    def test_report_from_test_data_b2b(self, b2b_saas_json):
        """ReportData from test_data/b2b_saas.json."""
        rd = ReportData(**b2b_saas_json)
        assert rd.company.business_type == BusinessType.B2B_SERVICE

    def test_report_from_test_data_hybrid(self, b2b_b2c_hybrid_json):
        """ReportData from test_data/b2b_b2c_hybrid.json."""
        rd = ReportData(**b2b_b2c_hybrid_json)
        assert rd.company.business_type == BusinessType.B2B_B2C_HYBRID

    def test_section_gates(self, company_b2c_service):
        """Section gates dict is stored correctly."""
        rd = ReportData(
            company=company_b2c_service,
            section_gates={"P2": False, "C1": True, "S4": False},
        )
        assert rd.section_gates["P2"] is False
        assert rd.section_gates["C1"] is True

    def test_board_review_dict(self, company_b2c_service):
        """Board review dict is stored correctly."""
        rd = ReportData(
            company=company_b2c_service,
            board_review={"reviews": [], "consensus": {"approved": True}},
        )
        assert rd.board_review["consensus"]["approved"] is True


# ── Config helpers ──


class TestConfigDetection:
    """Tests for detect_business_type from config."""

    def test_restaurant_okved(self):
        from app.config import detect_business_type
        assert detect_business_type("56.10") == BusinessType.B2C_SERVICE

    def test_it_okved(self):
        from app.config import detect_business_type
        assert detect_business_type("62.01") == BusinessType.B2B_SERVICE

    def test_retail_okved(self):
        from app.config import detect_business_type
        assert detect_business_type("47") == BusinessType.B2C_PRODUCT

    def test_manufacturing_okved(self):
        from app.config import detect_business_type
        assert detect_business_type("28.99") == BusinessType.B2B_PRODUCT

    def test_unknown_okved(self):
        from app.config import detect_business_type
        assert detect_business_type("99.99") is None

    def test_auto_dealer_okved(self):
        from app.config import detect_business_type
        assert detect_business_type("45.1") == BusinessType.B2C_PRODUCT
