"""Auto-select best available LLM model from each provider before report generation.

Before each report, probes providers and picks the best available model.
Falls back to hardcoded registry if probe fails.

Model registry is ranked: index 0 = best, last = fallback.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ── Model Registry (ranked best → worst) ──
# Updated: 2026-03-12

OPENAI_MODELS = [
    "gpt-5.4-pro",        # top tier: best reasoning, most demanding tasks (Mar 2026)
    "gpt-5.4",            # flagship: 1M ctx, best reasoning + coding (Mar 2026)
    "gpt-5.4-thinking",   # extended thinking for complex tasks
    "gpt-5.3-instant",    # fast + accurate (Mar 2026)
    "gpt-4o",             # previous gen, stable fallback
    "gpt-5-mini",         # budget
]

ANTHROPIC_MODELS = [
    "claude-opus-4-6",    # best: 1M ctx, adaptive thinking (Feb 2026)
    "claude-sonnet-4-6",  # balanced: near-Opus quality (Feb 2026)
    "claude-haiku-4-5-20251001",  # fast + cheap
]

GEMINI_MODELS = [
    "gemini-3.1-pro",     # best reasoning (Feb 2026)
    "gemini-3-flash",     # balanced: Pro-grade at Flash speed (Feb 2026)
    "gemini-3.1-flash-lite",  # cheapest ($0.25/1M in)
    "gemini-2.5-flash",   # previous gen fallback
]


@dataclass
class SelectedModels:
    """Models selected for current report generation."""
    main: str = ""           # основной мозг (SWOT, стратегия, конкуренты)
    main_provider: str = ""  # openai / anthropic / gemini
    board: str = ""          # Board of Directors (рецензирование)
    board_provider: str = ""
    fast: str = ""           # быстрые задачи (маркетплейсы, маппинг)
    fast_provider: str = ""
    timestamp: float = field(default_factory=time.time)
    probe_results: dict = field(default_factory=dict)


def _probe_openai(timeout: float = 10) -> Optional[str]:
    """Check which OpenAI models are available. Returns best available model ID."""
    key = os.environ.get("OPENAI_API_KEY") or os.environ.get("FALLBACK_LLM_API_KEY", "")
    if not key:
        return None
    try:
        req = urllib.request.Request(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {key}"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        available = {m["id"] for m in body.get("data", [])}
        for model in OPENAI_MODELS:
            if model in available:
                return model
        # If exact match not found, try prefix match
        for model in OPENAI_MODELS:
            for avail in available:
                if avail.startswith(model):
                    return avail
    except Exception as e:
        logger.warning("OpenAI probe failed: %s", str(e)[:200])
    return None


def _probe_anthropic(timeout: float = 10) -> Optional[str]:
    """Check Anthropic availability with a minimal request. Returns best model if available."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None
    # Anthropic doesn't have /models endpoint — check key validity with minimal call
    try:
        payload = json.dumps({
            "model": ANTHROPIC_MODELS[0],
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 5,
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return ANTHROPIC_MODELS[0]  # Best model works
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Model not found — try next
            logger.info("Anthropic %s not available, trying fallback", ANTHROPIC_MODELS[0])
            return ANTHROPIC_MODELS[1] if len(ANTHROPIC_MODELS) > 1 else None
        if e.code in (401, 403):
            logger.warning("Anthropic auth failed")
            return None
        # 429, 500 etc — model exists but rate limited, still usable
        return ANTHROPIC_MODELS[0]
    except Exception as e:
        logger.warning("Anthropic probe failed: %s", str(e)[:200])
    return None


def _probe_gemini(timeout: float = 10) -> Optional[str]:
    """Check which Gemini models are available. Returns best available model ID."""
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        return None
    try:
        req = urllib.request.Request(
            f"https://generativelanguage.googleapis.com/v1beta/models?key={key}",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        available = {m["name"].replace("models/", "") for m in body.get("models", [])}
        for model in GEMINI_MODELS:
            if model in available:
                return model
        # Prefix match
        for model in GEMINI_MODELS:
            for avail in available:
                if avail.startswith(model):
                    return avail
    except Exception as e:
        logger.warning("Gemini probe failed: %s", str(e)[:200])
    return None


def select_models(probe: bool = True) -> SelectedModels:
    """Select best available models for report generation.

    If probe=True, queries each provider API to check model availability.
    If probe fails, falls back to hardcoded defaults.

    Strategy:
    - MAIN: best OpenAI model (reasoning + large context)
    - BOARD: best Anthropic model (Claude Opus — no TPM limits, parallel calls)
    - FAST: best Gemini model (speed + cost efficiency)
    """
    result = SelectedModels()
    probe_results = {}

    if probe:
        t0 = time.monotonic()

        # Probe all 3 in sequence (total ~30s worst case)
        openai_best = _probe_openai()
        anthropic_best = _probe_anthropic()
        gemini_best = _probe_gemini()

        elapsed = round(time.monotonic() - t0, 2)
        probe_results = {
            "openai": openai_best or "UNAVAILABLE",
            "anthropic": anthropic_best or "UNAVAILABLE",
            "gemini": gemini_best or "UNAVAILABLE",
            "probe_time_s": elapsed,
        }
        logger.info("Model probe in %.2fs: %s", elapsed, probe_results)
    else:
        openai_best = None
        anthropic_best = None
        gemini_best = None

    # Assign roles with fallbacks
    # MAIN: OpenAI (best reasoning) → Anthropic → Gemini
    result.main = openai_best or os.environ.get("LLM_MODEL_MAIN", OPENAI_MODELS[0])
    result.main_provider = "openai" if openai_best else "openai"

    # BOARD: Anthropic (no TPM limits, parallel) → OpenAI → Gemini
    result.board = anthropic_best or os.environ.get("LLM_MODEL_OPUS", ANTHROPIC_MODELS[0])
    result.board_provider = "anthropic" if anthropic_best else "anthropic"

    # FAST: Gemini (speed + cost) → OpenAI mini
    result.fast = gemini_best or os.environ.get("LLM_MODEL_FAST", GEMINI_MODELS[-1])
    result.fast_provider = "gemini" if gemini_best else "gemini"

    result.probe_results = probe_results

    logger.info(
        "Selected models — MAIN: %s (%s), BOARD: %s (%s), FAST: %s (%s)",
        result.main, result.main_provider,
        result.board, result.board_provider,
        result.fast, result.fast_provider,
    )

    return result


# ── Cached selection (refreshes every 30 min) ──
_cached: Optional[SelectedModels] = None
_CACHE_TTL = 1800  # 30 minutes


def get_models(force_refresh: bool = False) -> SelectedModels:
    """Get selected models, with 30-min cache to avoid probing on every request."""
    global _cached
    if _cached and not force_refresh and (time.time() - _cached.timestamp) < _CACHE_TTL:
        return _cached
    _cached = select_models(probe=True)
    return _cached
