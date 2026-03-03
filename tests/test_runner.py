"""Tests for pipeline step runner: retries, fallbacks, state tracking.

Tests pure Python logic only — no LLM calls, no network.
Uses mock functions that succeed or fail on demand.
"""

import time
from unittest.mock import MagicMock, patch

import pytest

from app.pipeline.runner import PipelineState, StepResult, run_step


# ── StepResult model ──


class TestStepResult:
    """Tests for StepResult dataclass."""

    def test_default_values(self):
        """StepResult has sensible defaults."""
        sr = StepResult(name="test_step", status="success")
        assert sr.name == "test_step"
        assert sr.status == "success"
        assert sr.result is None
        assert sr.error is None
        assert sr.elapsed_sec == 0.0
        assert sr.attempts == 1

    def test_with_all_fields(self):
        """StepResult stores all fields."""
        sr = StepResult(
            name="fetch_data",
            status="warning",
            result={"key": "value"},
            error="partial failure",
            elapsed_sec=2.5,
            attempts=3,
        )
        assert sr.result == {"key": "value"}
        assert sr.error == "partial failure"
        assert sr.elapsed_sec == 2.5
        assert sr.attempts == 3


# ── PipelineState ──


class TestPipelineState:
    """Tests for PipelineState tracking."""

    def test_empty_state(self):
        """New PipelineState has no steps."""
        state = PipelineState()
        assert len(state.steps) == 0
        assert state.failed_steps == []

    def test_add_step(self):
        """add() appends a StepResult."""
        state = PipelineState()
        sr = StepResult(name="step1", status="success")
        state.add(sr)
        assert len(state.steps) == 1
        assert state.steps[0].name == "step1"

    def test_add_multiple_steps(self):
        """Multiple steps are tracked in order."""
        state = PipelineState()
        state.add(StepResult(name="step1", status="success"))
        state.add(StepResult(name="step2", status="warning", error="partial"))
        state.add(StepResult(name="step3", status="error", error="failed"))
        assert len(state.steps) == 3
        assert state.steps[0].name == "step1"
        assert state.steps[2].name == "step3"

    def test_failed_steps(self):
        """failed_steps returns names of steps with status='error'."""
        state = PipelineState()
        state.add(StepResult(name="ok1", status="success"))
        state.add(StepResult(name="warn1", status="warning"))
        state.add(StepResult(name="fail1", status="error"))
        state.add(StepResult(name="fail2", status="error"))
        assert state.failed_steps == ["fail1", "fail2"]

    def test_total_elapsed(self):
        """total_elapsed reports time since creation."""
        state = PipelineState()
        # Just check it's a non-negative float
        assert state.total_elapsed >= 0.0

    def test_summary(self):
        """summary returns correct aggregate counts."""
        state = PipelineState()
        state.add(StepResult(name="s1", status="success", elapsed_sec=1.0, attempts=1))
        state.add(StepResult(name="s2", status="success", elapsed_sec=2.0, attempts=1))
        state.add(StepResult(name="s3", status="warning", elapsed_sec=0.5, attempts=2))
        state.add(StepResult(name="s4", status="error", elapsed_sec=0.0, attempts=3))

        summary = state.summary
        assert summary["total_steps"] == 4
        assert summary["success"] == 2
        assert summary["warnings"] == 1
        assert summary["errors"] == 1
        assert len(summary["steps"]) == 4
        assert summary["steps"][0]["name"] == "s1"
        assert summary["steps"][2]["attempts"] == 2


# ── run_step: success scenarios ──


class TestRunStepSuccess:
    """Tests for run_step when the function succeeds."""

    def test_success_first_try(self):
        """Function succeeds on first call."""
        func = MagicMock(return_value="data")
        result = run_step("fetch", func, max_retries=2)

        assert result.status == "success"
        assert result.result == "data"
        assert result.attempts == 1
        assert result.error is None
        func.assert_called_once()

    def test_passes_args_and_kwargs(self):
        """Arguments and keyword arguments are forwarded to func."""
        func = MagicMock(return_value="ok")
        run_step("test", func, "arg1", "arg2", max_retries=0, key="val")

        func.assert_called_once_with("arg1", "arg2", key="val")

    def test_elapsed_time_tracked(self):
        """elapsed_sec is positive for a non-trivial function."""
        def slow_func():
            time.sleep(0.05)
            return "done"

        result = run_step("slow", slow_func, max_retries=0)
        assert result.status == "success"
        assert result.elapsed_sec >= 0.04

    def test_returns_none_result(self):
        """Functions returning None still produce success."""
        func = MagicMock(return_value=None)
        result = run_step("none_step", func, max_retries=0)
        assert result.status == "success"
        assert result.result is None


# ── run_step: retry scenarios ──


class TestRunStepRetries:
    """Tests for run_step retry behavior."""

    def test_retries_on_failure_then_succeeds(self):
        """Function fails once, then succeeds on retry."""
        func = MagicMock(side_effect=[ValueError("boom"), "recovered"])
        result = run_step("retry_step", func, max_retries=2, backoff_base=0.01)

        assert result.status == "success"
        assert result.result == "recovered"
        assert result.attempts == 2
        assert func.call_count == 2

    def test_retries_twice_then_succeeds(self):
        """Function fails twice, succeeds on third try."""
        func = MagicMock(
            side_effect=[RuntimeError("fail1"), RuntimeError("fail2"), "ok"]
        )
        result = run_step("retry3", func, max_retries=2, backoff_base=0.01)

        assert result.status == "success"
        assert result.result == "ok"
        assert result.attempts == 3
        assert func.call_count == 3

    def test_no_retries_when_max_retries_zero(self):
        """With max_retries=0, function is called once."""
        func = MagicMock(side_effect=ValueError("fail"))
        result = run_step("no_retry", func, max_retries=0)

        assert result.status == "error"
        assert result.attempts == 1
        func.assert_called_once()


# ── run_step: fallback scenarios ──


class TestRunStepFallback:
    """Tests for run_step fallback behavior after exhausting retries."""

    def test_uses_fallback_after_exhausting_retries(self):
        """After all retries fail, fallback value is returned with status='warning'."""
        func = MagicMock(side_effect=RuntimeError("always fails"))
        result = run_step(
            "fallback_step", func,
            max_retries=1,
            backoff_base=0.01,
            fallback={"default": True},
        )

        assert result.status == "warning"
        assert result.result == {"default": True}
        assert result.error is not None
        assert "always fails" in result.error
        assert result.attempts == 2  # 1 initial + 1 retry

    def test_fallback_none_returns_error_status(self):
        """When fallback is None (default), returns error status instead of warning."""
        func = MagicMock(side_effect=RuntimeError("fail"))
        result = run_step("no_fallback", func, max_retries=0)

        assert result.status == "error"
        assert result.result is None
        assert result.error is not None

    def test_fallback_empty_list(self):
        """Fallback with empty list is used (not treated as None)."""
        func = MagicMock(side_effect=RuntimeError("fail"))
        result = run_step(
            "list_fallback", func,
            max_retries=0,
            fallback=[],
        )

        # Empty list is falsy but is not None, so it should still be used as fallback
        # However, the code checks `if fallback is not None`, so [] should work
        assert result.status == "warning"
        assert result.result == []

    def test_fallback_zero(self):
        """Fallback with 0 is used (not treated as None)."""
        func = MagicMock(side_effect=RuntimeError("fail"))
        result = run_step(
            "zero_fallback", func,
            max_retries=0,
            fallback=0,
        )
        # 0 is falsy but not None, so it won't trigger the fallback path
        # because `if fallback is not None` is True but `0` is falsy for the
        # actual check. Let's verify actual behavior:
        # The code says `if fallback is not None:` so 0 IS not None -> warning
        assert result.status == "warning"
        assert result.result == 0


# ── run_step: critical failure ──


class TestRunStepCritical:
    """Tests for run_step with critical=True."""

    def test_critical_raises_on_failure(self):
        """With critical=True, raises the last exception after exhausting retries."""
        func = MagicMock(side_effect=RuntimeError("critical failure"))

        with pytest.raises(RuntimeError, match="critical failure"):
            run_step(
                "critical_step", func,
                max_retries=1,
                backoff_base=0.01,
                critical=True,
            )

        assert func.call_count == 2  # 1 initial + 1 retry

    def test_critical_success_does_not_raise(self):
        """With critical=True, successful function does not raise."""
        func = MagicMock(return_value="ok")
        result = run_step("critical_ok", func, max_retries=1, critical=True)
        assert result.status == "success"

    def test_critical_overrides_fallback(self):
        """With critical=True, the exception is raised even if fallback is provided."""
        func = MagicMock(side_effect=ValueError("critical error"))

        with pytest.raises(ValueError, match="critical error"):
            run_step(
                "critical_with_fallback", func,
                max_retries=0,
                critical=True,
                fallback="unused",
            )


# ── run_step: integration with PipelineState ──


class TestRunStepWithPipelineState:
    """Tests for using run_step results with PipelineState."""

    def test_pipeline_tracks_mixed_results(self):
        """PipelineState correctly tracks a mix of success, warning, error steps."""
        state = PipelineState()

        # Step 1: success
        result1 = run_step("step1", lambda: "data", max_retries=0)
        state.add(result1)

        # Step 2: fallback (warning)
        failing = MagicMock(side_effect=RuntimeError("fail"))
        result2 = run_step("step2", failing, max_retries=0, fallback={})
        state.add(result2)

        # Step 3: error (no fallback)
        failing2 = MagicMock(side_effect=RuntimeError("fail2"))
        result3 = run_step("step3", failing2, max_retries=0)
        state.add(result3)

        summary = state.summary
        assert summary["total_steps"] == 3
        assert summary["success"] == 1
        assert summary["warnings"] == 1
        assert summary["errors"] == 1
        assert state.failed_steps == ["step3"]

    def test_pipeline_all_success(self):
        """Pipeline with all successful steps."""
        state = PipelineState()
        for i in range(5):
            result = run_step(f"step{i}", lambda x=i: x * 2, max_retries=0)
            state.add(result)

        summary = state.summary
        assert summary["success"] == 5
        assert summary["errors"] == 0
        assert summary["warnings"] == 0


# ── run_step: backoff behavior ──


class TestRunStepBackoff:
    """Tests verifying that backoff timing works (approximately)."""

    @patch("app.pipeline.runner.time.sleep")
    def test_backoff_called_on_retry(self, mock_sleep):
        """time.sleep is called with increasing backoff between retries."""
        func = MagicMock(side_effect=[RuntimeError("fail"), "ok"])
        result = run_step("backoff_test", func, max_retries=1, backoff_base=3.0)

        assert result.status == "success"
        # Sleep should have been called once (between attempt 1 and 2)
        mock_sleep.assert_called_once()
        # backoff_base * 2^(attempt-1) = 3.0 * 2^0 = 3.0
        args = mock_sleep.call_args[0]
        assert args[0] == 3.0

    @patch("app.pipeline.runner.time.sleep")
    def test_exponential_backoff_values(self, mock_sleep):
        """Backoff doubles with each retry: 3, 6, 12..."""
        func = MagicMock(
            side_effect=[RuntimeError("f1"), RuntimeError("f2"), RuntimeError("f3"), "ok"]
        )
        result = run_step("exp_backoff", func, max_retries=3, backoff_base=3.0)

        assert result.status == "success"
        assert result.attempts == 4
        # 3 sleep calls: 3.0, 6.0, 12.0
        sleep_values = [call[0][0] for call in mock_sleep.call_args_list]
        assert sleep_values == [3.0, 6.0, 12.0]

    @patch("app.pipeline.runner.time.sleep")
    def test_no_sleep_on_first_try_success(self, mock_sleep):
        """No sleep when function succeeds on first try."""
        func = MagicMock(return_value="ok")
        run_step("no_sleep", func, max_retries=2, backoff_base=3.0)
        mock_sleep.assert_not_called()


# ── run_step: error message truncation ──


class TestRunStepErrorTruncation:
    """Tests that error messages are truncated to 300 chars."""

    def test_long_error_truncated(self):
        """Error messages longer than 300 chars are truncated."""
        long_msg = "x" * 500
        func = MagicMock(side_effect=RuntimeError(long_msg))
        result = run_step("trunc_test", func, max_retries=0)

        assert result.status == "error"
        assert len(result.error) <= 300

    def test_short_error_preserved(self):
        """Short error messages are preserved as-is."""
        func = MagicMock(side_effect=RuntimeError("short error"))
        result = run_step("short_err", func, max_retries=0)
        assert result.error == "short error"
