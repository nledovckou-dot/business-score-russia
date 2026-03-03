"""Tests for security module: URL validation, rate limiting, sanitization.

Tests pure Python logic only — no network calls, no FastAPI dependencies.
"""

import time

import pytest

from app.security import (
    _cleanup_timestamps,
    _is_ip_address,
    _is_private_ip,
    _request_log,
    _report_log,
    _rate_lock,
    check_rate_limit_report,
    check_rate_limit_request,
    sanitize_dict,
    sanitize_error,
    sanitize_text,
    validate_url,
    REPORTS_PER_HOUR,
    REQUESTS_PER_MINUTE,
)


# ── URL Validation ──


class TestValidateUrl:
    """Tests for validate_url function."""

    def test_valid_https_url(self):
        ok, url, err = validate_url("https://example.com")
        assert ok is True
        assert url == "https://example.com"
        assert err == ""

    def test_valid_http_url(self):
        ok, url, err = validate_url("http://example.com")
        assert ok is True
        assert url == "http://example.com"

    def test_auto_adds_https(self):
        """URL without scheme gets https:// prepended."""
        ok, url, err = validate_url("example.com")
        assert ok is True
        assert url == "https://example.com"

    def test_url_with_path(self):
        ok, url, err = validate_url("https://example.com/path/to/page?q=1")
        assert ok is True
        assert "/path/to/page" in url

    def test_empty_url(self):
        ok, url, err = validate_url("")
        assert ok is False
        assert "URL не указан" in err

    def test_whitespace_only_url(self):
        ok, url, err = validate_url("   ")
        assert ok is False
        assert "URL не указан" in err

    def test_ftp_scheme_rejected(self):
        """Non-http(s) schemes are rejected."""
        ok, url, err = validate_url("ftp://files.example.com")
        assert ok is False
        assert "Недопустимая схема" in err

    def test_javascript_scheme_rejected(self):
        """javascript: scheme is rejected (caught as invalid domain after https:// prepend)."""
        ok, url, err = validate_url("javascript:alert(1)")
        assert ok is False
        # javascript: has no "://" so gets https:// prepended; then domain validation fails
        assert err != ""

    def test_data_scheme_rejected(self):
        """data: scheme is rejected (caught as invalid domain after https:// prepend)."""
        ok, url, err = validate_url("data:text/html,<h1>XSS</h1>")
        assert ok is False
        assert err != ""

    def test_file_scheme_rejected(self):
        """file:// scheme is rejected."""
        ok, url, err = validate_url("file:///etc/passwd")
        assert ok is False
        assert "Недопустимая схема" in err

    def test_localhost_blocked(self):
        """localhost is blocked (SSRF prevention)."""
        ok, url, err = validate_url("http://localhost:8080")
        assert ok is False
        assert "Локальные адреса" in err

    def test_127_0_0_1_blocked(self):
        ok, url, err = validate_url("http://127.0.0.1")
        assert ok is False
        assert "Локальные адреса" in err

    def test_0_0_0_0_blocked(self):
        ok, url, err = validate_url("http://0.0.0.0")
        assert ok is False
        assert "Локальные адреса" in err

    def test_private_10_x_blocked(self):
        """10.0.0.0/8 private range blocked."""
        ok, url, err = validate_url("http://10.0.0.1")
        assert ok is False
        assert "Приватные IP" in err

    def test_private_172_16_blocked(self):
        """172.16.0.0/12 private range blocked."""
        ok, url, err = validate_url("http://172.16.0.1")
        assert ok is False
        assert "Приватные IP" in err

    def test_private_192_168_blocked(self):
        """192.168.0.0/16 private range blocked."""
        ok, url, err = validate_url("http://192.168.1.1")
        assert ok is False
        assert "Приватные IP" in err

    def test_link_local_blocked(self):
        """169.254.0.0/16 link-local range blocked."""
        ok, url, err = validate_url("http://169.254.1.1")
        assert ok is False
        assert "Приватные IP" in err

    def test_public_ip_allowed(self):
        """Public IP addresses are allowed."""
        ok, url, err = validate_url("http://8.8.8.8")
        assert ok is True

    def test_too_long_url(self):
        """URLs over 2048 chars are rejected."""
        long_url = "https://example.com/" + "a" * 2048
        ok, url, err = validate_url(long_url)
        assert ok is False
        assert "слишком длинный" in err

    def test_cyrillic_domain(self):
        """Cyrillic domain names are allowed."""
        ok, url, err = validate_url("https://ресторан.рф")
        assert ok is True

    def test_strips_whitespace(self):
        """Leading/trailing whitespace is stripped."""
        ok, url, err = validate_url("  https://example.com  ")
        assert ok is True
        assert url == "https://example.com"

    def test_strips_control_chars(self):
        """Control characters are stripped from URL."""
        ok, url, err = validate_url("https://example\x00.com")
        assert ok is True
        assert "\x00" not in url


# ── IP address helpers ──


class TestIPHelpers:
    """Tests for _is_ip_address and _is_private_ip."""

    def test_is_ip_address_valid(self):
        assert _is_ip_address("192.168.1.1") is True
        assert _is_ip_address("8.8.8.8") is True
        assert _is_ip_address("0.0.0.0") is True
        assert _is_ip_address("255.255.255.255") is True

    def test_is_ip_address_invalid(self):
        assert _is_ip_address("example.com") is False
        assert _is_ip_address("256.1.1.1") is False
        assert _is_ip_address("1.2.3") is False
        assert _is_ip_address("abc.def.ghi.jkl") is False

    def test_is_private_ip(self):
        assert _is_private_ip("10.0.0.1") is True
        assert _is_private_ip("10.255.255.255") is True
        assert _is_private_ip("172.16.0.1") is True
        assert _is_private_ip("172.31.255.255") is True
        assert _is_private_ip("192.168.0.1") is True
        assert _is_private_ip("192.168.255.255") is True
        assert _is_private_ip("169.254.1.1") is True
        assert _is_private_ip("127.0.0.1") is True

    def test_is_not_private_ip(self):
        assert _is_private_ip("8.8.8.8") is False
        assert _is_private_ip("172.32.0.1") is False
        assert _is_private_ip("192.169.0.1") is False
        assert _is_private_ip("11.0.0.1") is False

    def test_is_private_ip_non_ip(self):
        """Non-IP strings return False."""
        assert _is_private_ip("example.com") is False
        assert _is_private_ip("not.an.ip.addr") is False


# ── Text Sanitization ──


class TestSanitizeText:
    """Tests for sanitize_text function."""

    def test_plain_text_passthrough(self):
        assert sanitize_text("hello world") == "hello world"

    def test_html_escaped(self):
        """HTML tags are escaped."""
        result = sanitize_text("<script>alert('xss')</script>")
        assert "<script>" not in result
        assert "&lt;script&gt;" in result

    def test_quotes_escaped(self):
        result = sanitize_text('He said "hello"')
        assert "&quot;" in result

    def test_ampersand_escaped(self):
        result = sanitize_text("Tom & Jerry")
        assert "&amp;" in result

    def test_control_chars_stripped(self):
        """Null bytes and control chars are removed."""
        result = sanitize_text("hello\x00world\x01test")
        assert "\x00" not in result
        assert "\x01" not in result
        assert "helloworld" in result

    def test_max_length(self):
        """Text is truncated to max_length."""
        long = "a" * 1000
        result = sanitize_text(long, max_length=100)
        assert len(result) == 100

    def test_default_max_length(self):
        """Default max_length is 500."""
        long = "b" * 600
        result = sanitize_text(long)
        assert len(result) == 500

    def test_non_string_returns_empty(self):
        """Non-string input returns empty string."""
        assert sanitize_text(123) == ""
        assert sanitize_text(None) == ""

    def test_unicode_preserved(self):
        """Cyrillic and other unicode preserved."""
        result = sanitize_text("Ресторан Maison Rouge")
        assert "Ресторан" in result


class TestSanitizeDict:
    """Tests for sanitize_dict function."""

    def test_sanitizes_specified_fields(self):
        d = {"name": "<b>Test</b>", "value": 123, "desc": "<script>x</script>"}
        result = sanitize_dict(d, ("name", "desc"))
        assert "&lt;b&gt;" in result["name"]
        assert "&lt;script&gt;" in result["desc"]
        assert result["value"] == 123  # non-text field untouched

    def test_ignores_missing_fields(self):
        d = {"name": "Test"}
        result = sanitize_dict(d, ("name", "nonexistent"))
        assert result["name"] == "Test"

    def test_non_string_fields_untouched(self):
        d = {"name": "Test", "count": 42}
        result = sanitize_dict(d, ("name", "count"))
        assert result["count"] == 42  # int not sanitized


# ── Rate Limiting ──


class TestRateLimiting:
    """Tests for rate limiting functions."""

    def _clear_rate_logs(self, ip: str):
        """Helper to clear rate logs for a specific IP."""
        with _rate_lock:
            _request_log.pop(ip, None)
            _report_log.pop(ip, None)

    def test_request_rate_limit_allows_normal(self):
        """Normal request volume is allowed."""
        test_ip = "test_normal_1.2.3.4"
        self._clear_rate_logs(test_ip)
        result = check_rate_limit_request(test_ip)
        assert result is None  # allowed

    def test_request_rate_limit_blocks_excess(self):
        """Exceeding request limit returns error message."""
        test_ip = "test_excess_1.2.3.5"
        self._clear_rate_logs(test_ip)

        # Fill up the limit
        for _ in range(REQUESTS_PER_MINUTE):
            result = check_rate_limit_request(test_ip)
            assert result is None

        # Next request should be blocked
        result = check_rate_limit_request(test_ip)
        assert result is not None
        assert "Слишком много" in result

        self._clear_rate_logs(test_ip)

    def test_report_rate_limit_allows_normal(self):
        """Normal report generation is allowed."""
        test_ip = "test_report_1.2.3.6"
        self._clear_rate_logs(test_ip)
        result = check_rate_limit_report(test_ip)
        assert result is None

    def test_report_rate_limit_blocks_excess(self):
        """Exceeding report limit returns error message."""
        test_ip = "test_report_excess_1.2.3.7"
        self._clear_rate_logs(test_ip)

        for _ in range(REPORTS_PER_HOUR):
            result = check_rate_limit_report(test_ip)
            assert result is None

        result = check_rate_limit_report(test_ip)
        assert result is not None
        assert "Лимит отчётов" in result

        self._clear_rate_logs(test_ip)

    def test_cleanup_timestamps(self):
        """Old timestamps are cleaned up."""
        now = time.time()
        timestamps = [now - 120, now - 90, now - 30, now - 10, now]
        cleaned = _cleanup_timestamps(timestamps, 60)
        assert len(cleaned) == 3  # only last 3 within 60s window

    def test_different_ips_independent(self):
        """Rate limits are per-IP."""
        ip_a = "test_ipa_1.2.3.8"
        ip_b = "test_ipb_1.2.3.9"
        self._clear_rate_logs(ip_a)
        self._clear_rate_logs(ip_b)

        # Fill up ip_a
        for _ in range(REQUESTS_PER_MINUTE):
            check_rate_limit_request(ip_a)

        # ip_b should still be fine
        result = check_rate_limit_request(ip_b)
        assert result is None

        self._clear_rate_logs(ip_a)
        self._clear_rate_logs(ip_b)


# ── Error Sanitization ──


class TestSanitizeError:
    """Tests for sanitize_error function."""

    def test_strips_file_paths_macos(self):
        """macOS file paths are stripped."""
        msg = "Error in /Users/john/project/app/main.py: something failed"
        result = sanitize_error(msg)
        assert "/Users/" not in result
        assert "[...]" in result

    def test_strips_file_paths_linux(self):
        """Linux file paths are stripped."""
        msg = "Error in /home/deploy/app/service.py: crash"
        result = sanitize_error(msg)
        assert "/home/" not in result

    def test_strips_opt_paths(self):
        """Deployment paths stripped."""
        msg = "Error at /opt/business-score-russia/app/main.py"
        result = sanitize_error(msg)
        assert "/opt/" not in result

    def test_strips_python_traceback(self):
        """Python traceback file references stripped."""
        msg = 'File "/opt/app/main.py", line 42, in handle_request'
        result = sanitize_error(msg)
        assert "main.py" not in result

    def test_include_details_mode(self):
        """With include_details=True, full message preserved."""
        msg = "Error in /Users/john/project/app/main.py: something failed"
        result = sanitize_error(msg, include_details=True)
        assert "/Users/john" in result

    def test_truncates_long_messages(self):
        """Messages over 300 chars are truncated."""
        msg = "x" * 500
        result = sanitize_error(msg)
        assert len(result) <= 300

    def test_exception_object(self):
        """Works with Exception objects too."""
        try:
            raise ValueError("test error at /home/user/app.py")
        except ValueError as e:
            result = sanitize_error(e)
            assert "/home/" not in result
            assert "test error" in result
