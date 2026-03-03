"""Metrics collector for BSR pipeline — per-report timing, LLM usage, cost estimates.

Usage:
    mc = MetricsCollector(session_id="abc123", company="ООО Рога")
    mc.start_timer("step1_scrape")
    ...
    mc.stop_timer("step1_scrape")
    mc.record_llm_call("gpt-5.2-pro", tokens_in=2000, tokens_out=800)
    mc.finalize()  # saves to data/metrics.jsonl

Aggregate stats:
    stats = get_aggregate_stats()  # returns dict for /api/stats
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("bsr.metrics")

# ── Paths ──

_BASE_DIR = Path(__file__).resolve().parent.parent
METRICS_DIR = _BASE_DIR / "data"
METRICS_FILE = METRICS_DIR / "metrics.jsonl"

# ── Cost per 1K tokens (USD estimates) ──

MODEL_PRICING: dict[str, dict[str, float]] = {
    # model_name -> {"input": $/1K, "output": $/1K}
    "gpt-5.2-pro": {"input": 0.005, "output": 0.015},
    "gpt-5.3-codex": {"input": 0.008, "output": 0.024},
    "gemini-2.5-flash": {"input": 0.0001, "output": 0.0001},
    "o3": {"input": 0.010, "output": 0.030},
}

# Fallback for unknown models
_DEFAULT_PRICING = {"input": 0.005, "output": 0.015}

# Thread lock for file writes
_write_lock = threading.Lock()


class MetricsCollector:
    """Collects per-report metrics: step timings, LLM token usage, cost.

    Thread-safe: record_llm_call() can be called from multiple threads
    (e.g., parallel LLM sections in step5, board parallel experts).
    """

    def __init__(self, session_id: str = "", company: str = ""):
        self.session_id = session_id
        self.company = company
        self.created_at = time.time()

        # Step timings: {step_name: {"start": float, "end": float, "elapsed": float}}
        self._timers: dict[str, dict[str, float]] = {}
        self._active_timers: dict[str, float] = {}  # step_name -> start_time

        # LLM calls: [{"model": str, "tokens_in": int, "tokens_out": int, "cost_usd": float}]
        self._llm_calls: list[dict[str, Any]] = []

        # Lock for thread-safe access (multiple LLM threads record simultaneously)
        self._lock = threading.Lock()

        self._finalized = False

    def start_timer(self, step_name: str) -> None:
        """Start timing a pipeline step."""
        self._active_timers[step_name] = time.monotonic()

    def stop_timer(self, step_name: str) -> float:
        """Stop timing a pipeline step. Returns elapsed seconds."""
        start = self._active_timers.pop(step_name, None)
        if start is None:
            logger.warning("stop_timer called for '%s' without matching start_timer", step_name)
            return 0.0
        elapsed = round(time.monotonic() - start, 3)
        self._timers[step_name] = {
            "elapsed": elapsed,
        }
        return elapsed

    def record_llm_call(
        self,
        model: str,
        tokens_in: int = 0,
        tokens_out: int = 0,
    ) -> None:
        """Record a single LLM API call with token counts. Thread-safe."""
        pricing = MODEL_PRICING.get(model, _DEFAULT_PRICING)
        cost_usd = round(
            (tokens_in / 1000) * pricing["input"] + (tokens_out / 1000) * pricing["output"],
            6,
        )
        with self._lock:
            self._llm_calls.append({
                "model": model,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "cost_usd": cost_usd,
            })

    def finalize(self) -> dict[str, Any]:
        """Compute totals, save to JSONL, return the metrics dict."""
        if self._finalized:
            logger.warning("MetricsCollector.finalize() called twice for session %s", self.session_id)
            return {}

        self._finalized = True

        # Stop any still-running timers
        for step_name in list(self._active_timers.keys()):
            self.stop_timer(step_name)

        # Compute totals
        total_time = round(sum(t["elapsed"] for t in self._timers.values()), 3)
        total_tokens_in = sum(c["tokens_in"] for c in self._llm_calls)
        total_tokens_out = sum(c["tokens_out"] for c in self._llm_calls)
        total_cost_usd = round(sum(c["cost_usd"] for c in self._llm_calls), 6)
        llm_call_count = len(self._llm_calls)

        # Per-model aggregation
        model_totals: dict[str, dict[str, Any]] = {}
        for call in self._llm_calls:
            m = call["model"]
            if m not in model_totals:
                model_totals[m] = {"calls": 0, "tokens_in": 0, "tokens_out": 0, "cost_usd": 0.0}
            model_totals[m]["calls"] += 1
            model_totals[m]["tokens_in"] += call["tokens_in"]
            model_totals[m]["tokens_out"] += call["tokens_out"]
            model_totals[m]["cost_usd"] = round(model_totals[m]["cost_usd"] + call["cost_usd"], 6)

        record = {
            "session_id": self.session_id,
            "company": self.company,
            "timestamp": self.created_at,
            "total_time_sec": total_time,
            "step_timings": {k: v["elapsed"] for k, v in self._timers.items()},
            "llm_calls": llm_call_count,
            "total_tokens_in": total_tokens_in,
            "total_tokens_out": total_tokens_out,
            "total_cost_usd": total_cost_usd,
            "model_totals": model_totals,
        }

        # Write to JSONL
        _append_metrics(record)

        return record


def _append_metrics(record: dict[str, Any]) -> None:
    """Append a single metrics record to the JSONL file (thread-safe)."""
    try:
        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False) + "\n"
        with _write_lock:
            with open(METRICS_FILE, "a", encoding="utf-8") as f:
                f.write(line)
        logger.info(
            "Metrics saved: session=%s, time=%.1fs, llm_calls=%d, cost=$%.4f",
            record.get("session_id", "?"),
            record.get("total_time_sec", 0),
            record.get("llm_calls", 0),
            record.get("total_cost_usd", 0),
        )
    except Exception:
        logger.exception("Failed to write metrics to %s", METRICS_FILE)


def get_aggregate_stats() -> dict[str, Any]:
    """Read all metrics from JSONL and compute aggregate statistics.

    Returns dict with:
        total_reports, avg_time_sec, total_cost_usd,
        avg_cost_per_report_usd, total_llm_calls,
        avg_tokens_per_report, model_usage, recent_reports
    """
    records = _read_all_records()

    if not records:
        return {
            "total_reports": 0,
            "avg_time_sec": 0,
            "total_cost_usd": 0,
            "avg_cost_per_report_usd": 0,
            "total_llm_calls": 0,
            "avg_tokens_per_report": 0,
            "model_usage": {},
            "recent_reports": [],
        }

    total = len(records)
    times = [r.get("total_time_sec", 0) for r in records]
    costs = [r.get("total_cost_usd", 0) for r in records]
    llm_calls = [r.get("llm_calls", 0) for r in records]
    tokens = [r.get("total_tokens_in", 0) + r.get("total_tokens_out", 0) for r in records]

    # Aggregate model usage
    model_usage: dict[str, dict[str, Any]] = {}
    for r in records:
        for model, data in (r.get("model_totals") or {}).items():
            if model not in model_usage:
                model_usage[model] = {"calls": 0, "tokens_in": 0, "tokens_out": 0, "cost_usd": 0.0}
            model_usage[model]["calls"] += data.get("calls", 0)
            model_usage[model]["tokens_in"] += data.get("tokens_in", 0)
            model_usage[model]["tokens_out"] += data.get("tokens_out", 0)
            model_usage[model]["cost_usd"] = round(
                model_usage[model]["cost_usd"] + data.get("cost_usd", 0), 6
            )

    # Last 10 reports (most recent first)
    recent = sorted(records, key=lambda r: r.get("timestamp", 0), reverse=True)[:10]
    recent_reports = [
        {
            "session_id": r.get("session_id", ""),
            "company": r.get("company", ""),
            "time_sec": r.get("total_time_sec", 0),
            "cost_usd": r.get("total_cost_usd", 0),
            "llm_calls": r.get("llm_calls", 0),
            "timestamp": r.get("timestamp", 0),
        }
        for r in recent
    ]

    # Average step timings across all reports
    step_counts: dict[str, list[float]] = {}
    for r in records:
        for step, elapsed in (r.get("step_timings") or {}).items():
            step_counts.setdefault(step, []).append(elapsed)
    avg_step_timings = {
        step: round(sum(vals) / len(vals), 2)
        for step, vals in step_counts.items()
    }

    return {
        "total_reports": total,
        "avg_time_sec": round(sum(times) / total, 1),
        "min_time_sec": round(min(times), 1),
        "max_time_sec": round(max(times), 1),
        "total_cost_usd": round(sum(costs), 4),
        "avg_cost_per_report_usd": round(sum(costs) / total, 4),
        "total_llm_calls": sum(llm_calls),
        "avg_tokens_per_report": round(sum(tokens) / total),
        "model_usage": model_usage,
        "avg_step_timings": avg_step_timings,
        "recent_reports": recent_reports,
    }


def _read_all_records() -> list[dict[str, Any]]:
    """Read all lines from metrics.jsonl, skip corrupted lines."""
    if not METRICS_FILE.exists():
        return []
    records = []
    try:
        with open(METRICS_FILE, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning("Skipping corrupted metrics line %d", line_num)
    except Exception:
        logger.exception("Failed to read metrics file %s", METRICS_FILE)
    return records
