"""Pydantic models for company data, report blocks, and assembled report."""

from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.config import BusinessType, ConfidenceLevel, LifecycleStage


# ─── Company ───

class Company(BaseModel):
    name: str
    legal_name: Optional[str] = None
    inn: Optional[str] = None
    okved: Optional[str] = None
    business_type: BusinessType
    address: Optional[str] = None
    website: Optional[str] = None
    description: Optional[str] = None
    badges: List[str] = Field(default_factory=list)


# ─── Financial data ───

class FinancialYear(BaseModel):
    year: int
    revenue: Optional[float] = None
    net_profit: Optional[float] = None
    assets: Optional[float] = None
    equity: Optional[float] = None
    liabilities: Optional[float] = None
    employees: Optional[int] = None


# ─── Calc Trace (v2.0) ───

class CalcTrace(BaseModel):
    """Transparent calculation trace: formula + inputs + confidence."""
    metric_name: str
    value: Any
    formula: Optional[str] = None
    inputs: Dict[str, Any] = Field(default_factory=dict)
    sources: List[str] = Field(default_factory=list)
    confidence: ConfidenceLevel = ConfidenceLevel.ESTIMATE


# ─── Lifecycle (v2.0) ───

class LifecycleInfo(BaseModel):
    """Company lifecycle stage with evidence."""
    stage: LifecycleStage = LifecycleStage.MATURE
    evidence: List[str] = Field(default_factory=list)
    year_founded: Optional[str] = None


# ─── Sales Channel (v2.0) ───

class SalesChannel(BaseModel):
    """Single sales channel presence."""
    channel_name: str
    exists: Optional[bool] = None  # True/False/None=unknown
    source: Optional[str] = None
    url: Optional[str] = None


# ─── Competitor ───

class Competitor(BaseModel):
    name: str
    description: Optional[str] = None
    legal_name: Optional[str] = None
    inn: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    x: float = 50.0
    y: float = 50.0
    radar_scores: Dict[str, float] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    threat_level: str = "med"
    financials: List[FinancialYear] = Field(default_factory=list)
    # v2.0
    lifecycle: Optional[LifecycleInfo] = None
    sales_channels: List[SalesChannel] = Field(default_factory=list)
    # T11: верификация через web search
    verified: bool = True  # default True for backward compat
    verification_confidence: str = "unverified"  # high/medium/low/unverified
    verification_sources: List[str] = Field(default_factory=list)
    verification_notes: Optional[str] = None


# ─── SWOT ───

class SWOT(BaseModel):
    strengths: List[str] = Field(default_factory=list)
    weaknesses: List[str] = Field(default_factory=list)
    opportunities: List[str] = Field(default_factory=list)
    threats: List[str] = Field(default_factory=list)


# ─── Digital audit ───

class SocialAccount(BaseModel):
    platform: str
    handle: Optional[str] = None
    followers: Optional[int] = None
    verified: bool = False
    url: Optional[str] = None


class DigitalAudit(BaseModel):
    social_accounts: List[SocialAccount] = Field(default_factory=list)
    seo_score: Optional[float] = None
    monthly_traffic: Optional[int] = None
    traffic_source: Optional[str] = None


# ─── Market data ───

class MarketDataPoint(BaseModel):
    year: int
    value: float
    label: Optional[str] = None


class MarketOverview(BaseModel):
    market_name: str
    market_size: Optional[str] = None
    growth_rate: Optional[str] = None
    data_points: List[MarketDataPoint] = Field(default_factory=list)
    trends: List[str] = Field(default_factory=list)
    sources: List[str] = Field(default_factory=list)


# ─── Strategy ───

class Recommendation(BaseModel):
    title: str
    description: str
    priority: str = "medium"
    timeline: Optional[str] = None
    expected_impact: Optional[str] = None


class Scenario(BaseModel):
    name: str
    label: str
    metrics: Dict[str, float] = Field(default_factory=dict)


class KPIBenchmark(BaseModel):
    name: str
    current: Optional[float] = None
    benchmark: Optional[float] = None
    unit: str = ""
    color: str = "gold"


# ─── Factcheck ───

class FactItem(BaseModel):
    fact: str
    sources_count: int = 1
    verified: bool = False
    sources: List[str] = Field(default_factory=list)
    correction: Optional[str] = None


# ─── Founders & opinions ───

class Founder(BaseModel):
    name: str
    role: Optional[str] = None
    share: Optional[str] = None
    social: Dict[str, str] = Field(default_factory=dict)
    company: Optional[str] = None


class Opinion(BaseModel):
    author: str
    role: Optional[str] = None
    quote: str
    date: Optional[str] = None
    source: Optional[str] = None
    source_url: Optional[str] = None


# ─── Correlation matrix ───

class CorrelationPair(BaseModel):
    metric_a: str
    metric_b: str
    value: float
    label: str = ""


# ─── Timeline item ───

class TimelineItem(BaseModel):
    date: str
    title: str
    description: Optional[str] = None
    color: str = "gold"


# ─── Full report data ───

class ReportData(BaseModel):
    """All data needed to render a full report."""
    company: Company
    report_date: date = Field(default_factory=date.today)

    # Part I — Macro
    market: Optional[MarketOverview] = None
    regulatory_trends: List[TimelineItem] = Field(default_factory=list)
    tech_trends: List[str] = Field(default_factory=list)
    hr_data: Dict[str, Any] = Field(default_factory=dict)

    # Part II — Meso
    competitors: List[Competitor] = Field(default_factory=list)
    radar_dimensions: List[str] = Field(default_factory=list)

    # Part III — Micro
    financials: List[FinancialYear] = Field(default_factory=list)
    swot: Optional[SWOT] = None
    digital: Optional[DigitalAudit] = None
    reviews: Dict[str, Any] = Field(default_factory=dict)
    products: List[Dict[str, Any]] = Field(default_factory=list)
    menu: Dict[str, Any] = Field(default_factory=dict)
    tenders: List[Dict[str, Any]] = Field(default_factory=list)
    market_share: Dict[str, float] = Field(default_factory=dict)

    # Part IV — Strategy
    recommendations: List[Recommendation] = Field(default_factory=list)
    kpi_benchmarks: List[KPIBenchmark] = Field(default_factory=list)
    scenarios: List[Scenario] = Field(default_factory=list)
    correlations: List[CorrelationPair] = Field(default_factory=list)
    implementation_timeline: List[TimelineItem] = Field(default_factory=list)

    # Part V — Appendix + Factcheck
    open_questions: List[str] = Field(default_factory=list)
    glossary: Dict[str, str] = Field(default_factory=dict)
    factcheck: List[FactItem] = Field(default_factory=list)
    digital_verification: List[Dict[str, Any]] = Field(default_factory=list)

    # Part VI — Founders & Opinions
    founders: List[Founder] = Field(default_factory=list)
    opinions: List[Opinion] = Field(default_factory=list)

    # v2.0 — Pipeline extensions
    calc_traces: List[CalcTrace] = Field(default_factory=list)
    methodology: Dict[str, str] = Field(default_factory=dict)
    section_gates: Dict[str, bool] = Field(default_factory=dict)
    pipeline_version: str = "2.0"

    # v3.0 — Board of Directors review (T24-T27)
    board_review: Dict[str, Any] = Field(default_factory=dict)
