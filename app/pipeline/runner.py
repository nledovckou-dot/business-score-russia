"""Pipeline step runner with retries, timing, and state tracking (T2).

Provides `run_step()` — a wrapper that adds:
- Automatic retries with exponential backoff
- Per-step timing
- Error logging with step context
- State tracking (which steps ran, succeeded, failed)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class StepResult:
    """Result of a single pipeline step."""
    name: str
    status: str  # "success", "warning", "error"
    result: Any = None
    error: Optional[str] = None
    elapsed_sec: float = 0.0
    attempts: int = 1


@dataclass
class PipelineState:
    """Tracks overall pipeline execution state."""
    steps: list[StepResult] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)

    def add(self, result: StepResult):
        self.steps.append(result)

    @property
    def total_elapsed(self) -> float:
        return round(time.monotonic() - self.started_at, 2)

    @property
    def failed_steps(self) -> list[str]:
        return [s.name for s in self.steps if s.status == "error"]

    @property
    def summary(self) -> dict:
        return {
            "total_steps": len(self.steps),
            "success": sum(1 for s in self.steps if s.status == "success"),
            "warnings": sum(1 for s in self.steps if s.status == "warning"),
            "errors": sum(1 for s in self.steps if s.status == "error"),
            "total_sec": self.total_elapsed,
            "steps": [
                {"name": s.name, "status": s.status, "elapsed": s.elapsed_sec, "attempts": s.attempts}
                for s in self.steps
            ],
        }


def run_step(
    name: str,
    func: Callable[..., Any],
    *args,
    max_retries: int = 2,
    backoff_base: float = 3.0,
    fallback: Any = None,
    critical: bool = False,
    **kwargs,
) -> StepResult:
    """Run a pipeline step with retry logic.

    Args:
        name: Human-readable step name for logging
        func: The step function to call
        *args, **kwargs: Arguments passed to func
        max_retries: Number of retries on failure (0 = no retries)
        backoff_base: Base seconds for exponential backoff (3, 6, 12...)
        fallback: Value to return if all retries fail (None = re-raise)
        critical: If True and all retries fail, raises the exception

    Returns:
        StepResult with status, result, timing, attempt count
    """
    last_error = None

    for attempt in range(1, max_retries + 2):  # +2 because range is exclusive and attempt 1 = first try
        t0 = time.monotonic()
        try:
            result = func(*args, **kwargs)
            elapsed = round(time.monotonic() - t0, 2)

            if attempt > 1:
                logger.info(
                    "Step '%s' succeeded on attempt %d (%.2fs)",
                    name, attempt, elapsed,
                )

            return StepResult(
                name=name,
                status="success",
                result=result,
                elapsed_sec=elapsed,
                attempts=attempt,
            )

        except Exception as e:
            elapsed = round(time.monotonic() - t0, 2)
            last_error = e

            if attempt <= max_retries:
                wait = backoff_base * (2 ** (attempt - 1))
                logger.warning(
                    "Step '%s' failed (attempt %d/%d, %.2fs): %s. "
                    "Retrying in %.1fs...",
                    name, attempt, max_retries + 1, elapsed, str(e)[:200], wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "Step '%s' failed after %d attempts (%.2fs): %s",
                    name, attempt, elapsed, str(e)[:300],
                )

    # All retries exhausted
    if critical:
        raise last_error  # type: ignore

    if fallback is not None:
        logger.warning(
            "Step '%s' using fallback value after %d failed attempts",
            name, max_retries + 1,
        )
        return StepResult(
            name=name,
            status="warning",
            result=fallback,
            error=str(last_error)[:300] if last_error else None,
            elapsed_sec=round(time.monotonic() - t0, 2) if 't0' in dir() else 0,
            attempts=max_retries + 1,
        )

    return StepResult(
        name=name,
        status="error",
        error=str(last_error)[:300] if last_error else None,
        elapsed_sec=0,
        attempts=max_retries + 1,
    )
