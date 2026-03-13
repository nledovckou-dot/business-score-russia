"""Security utilities: rate limiting, input sanitization, session cleanup.

Pure Python — no extra dependencies. Uses collections for rate limiting.
"""

from __future__ import annotations

import html
import logging
import os
import re
import time
import threading
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger("bsr.security")

# ── Rate Limiting (per-IP, sliding window) ──────────────────────────

# {ip: [timestamp, timestamp, ...]}
_request_log: dict[str, list[float]] = defaultdict(list)
_report_log: dict[str, list[float]] = defaultdict(list)
_rate_lock = threading.Lock()

# Limits
REQUESTS_PER_MINUTE = 30
REPORTS_PER_HOUR = int(os.environ.get("BSR_REPORTS_PER_HOUR", "30"))


def _cleanup_timestamps(timestamps: list[float], window_seconds: float) -> list[float]:
    """Remove timestamps older than the window."""
    cutoff = time.time() - window_seconds
    return [t for t in timestamps if t > cutoff]


def check_rate_limit_request(ip: str) -> str | None:
    """Check per-minute request rate limit.

    Returns None if OK, or an error message string if limit exceeded.
    """
    now = time.time()
    with _rate_lock:
        _request_log[ip] = _cleanup_timestamps(_request_log[ip], 60)
        if len(_request_log[ip]) >= REQUESTS_PER_MINUTE:
            return f"Слишком много запросов. Лимит: {REQUESTS_PER_MINUTE} запросов в минуту."
        _request_log[ip].append(now)
    return None


def check_rate_limit_report(ip: str) -> str | None:
    """Check per-hour report generation rate limit.

    Returns None if OK, or an error message string if limit exceeded.
    """
    now = time.time()
    with _rate_lock:
        _report_log[ip] = _cleanup_timestamps(_report_log[ip], 3600)
        if len(_report_log[ip]) >= REPORTS_PER_HOUR:
            return f"Лимит отчётов: {REPORTS_PER_HOUR} в час. Попробуйте позже."
        _report_log[ip].append(now)
    return None


# ── Input Sanitization ──────────────────────────────────────────────

# Allowed URL schemes
_ALLOWED_SCHEMES = {"http", "https"}

# Regex for basic domain validation (must contain at least one dot, no spaces)
_DOMAIN_RE = re.compile(
    r"^[a-zA-Z0-9\u0400-\u04FF]"              # starts with alnum or cyrillic
    r"[a-zA-Z0-9\u0400-\u04FF._-]{0,252}"     # middle chars
    r"\.[a-zA-Z\u0400-\u04FF]{2,63}$"         # TLD (2-63 chars)
)

# Characters that should never appear in user text inputs (control chars, null bytes)
_DANGEROUS_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def validate_url(raw: str) -> tuple[bool, str, str]:
    """Validate and sanitize a URL input.

    Returns (is_valid, cleaned_url, error_message).
    If valid: (True, "https://example.com", "")
    If invalid: (False, "", "Error description")
    """
    raw = raw.strip()

    if not raw:
        return False, "", "URL не указан"

    # Strip dangerous characters
    raw = _DANGEROUS_CHARS_RE.sub("", raw)

    # Reject non-http(s) schemes early
    if "://" in raw and not raw.startswith(("http://", "https://")):
        scheme = raw.split("://")[0]
        return False, "", f"Недопустимая схема URL: {scheme}. Используйте http или https."

    # Add scheme if missing
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw

    # Parse
    try:
        parsed = urlparse(raw)
    except Exception:
        return False, "", "Некорректный URL"

    # Scheme check
    if parsed.scheme not in _ALLOWED_SCHEMES:
        return False, "", f"Недопустимая схема URL: {parsed.scheme}. Используйте http или https."

    # Host check
    hostname = parsed.hostname or ""
    if not hostname:
        return False, "", "URL не содержит домена"

    # Block local/private addresses
    if hostname in ("localhost", "127.0.0.1", "0.0.0.0", "::1"):
        return False, "", "Локальные адреса не поддерживаются"

    # Block private IP ranges (basic SSRF protection)
    if _is_private_ip(hostname):
        return False, "", "Приватные IP-адреса не поддерживаются"

    # Domain format validation
    if not _DOMAIN_RE.match(hostname) and not _is_ip_address(hostname):
        return False, "", "Некорректный домен"

    # Length limit
    if len(raw) > 2048:
        return False, "", "URL слишком длинный (макс. 2048 символов)"

    return True, raw, ""


def _is_ip_address(host: str) -> bool:
    """Check if host is an IP address."""
    parts = host.split(".")
    if len(parts) == 4:
        try:
            return all(0 <= int(p) <= 255 for p in parts)
        except ValueError:
            pass
    return False


def _is_private_ip(host: str) -> bool:
    """Check if host is a private/reserved IP address."""
    parts = host.split(".")
    if len(parts) != 4:
        return False
    try:
        octets = [int(p) for p in parts]
    except ValueError:
        return False

    if not all(0 <= o <= 255 for o in octets):
        return False

    # 10.0.0.0/8
    if octets[0] == 10:
        return True
    # 172.16.0.0/12
    if octets[0] == 172 and 16 <= octets[1] <= 31:
        return True
    # 192.168.0.0/16
    if octets[0] == 192 and octets[1] == 168:
        return True
    # 169.254.0.0/16 (link-local)
    if octets[0] == 169 and octets[1] == 254:
        return True
    # 127.0.0.0/8
    if octets[0] == 127:
        return True

    return False


def sanitize_text(value: str, max_length: int = 500) -> str:
    """Sanitize a text input: escape HTML, strip control chars, limit length."""
    if not isinstance(value, str):
        return ""
    value = _DANGEROUS_CHARS_RE.sub("", value)
    value = html.escape(value, quote=True)
    return value[:max_length]


def sanitize_dict(d: dict[str, Any], text_fields: tuple[str, ...], max_length: int = 500) -> dict[str, Any]:
    """Sanitize specified text fields in a dict."""
    result = dict(d)
    for key in text_fields:
        if key in result and isinstance(result[key], str):
            result[key] = sanitize_text(result[key], max_length)
    return result


# ── Session Cleanup ─────────────────────────────────────────────────

SESSION_TTL_SECONDS = 2 * 3600  # 2 hours
_CLEANUP_INTERVAL = 300  # check every 5 minutes
_cleanup_thread: threading.Thread | None = None
_cleanup_stop = threading.Event()


def start_session_cleanup(sessions: dict[str, dict[str, Any]]):
    """Start background thread that auto-expires old sessions."""
    global _cleanup_thread

    def _cleanup_loop():
        while not _cleanup_stop.wait(_CLEANUP_INTERVAL):
            _expire_sessions(sessions)

    _cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True, name="session-cleanup")
    _cleanup_thread.start()
    logger.info("Session cleanup thread started (TTL=%ds, interval=%ds)", SESSION_TTL_SECONDS, _CLEANUP_INTERVAL)


def stop_session_cleanup():
    """Stop the cleanup thread (for graceful shutdown)."""
    _cleanup_stop.set()
    if _cleanup_thread and _cleanup_thread.is_alive():
        _cleanup_thread.join(timeout=5)


def _expire_sessions(sessions: dict[str, dict[str, Any]]):
    """Remove sessions older than SESSION_TTL_SECONDS."""
    now = time.time()
    expired = [
        sid for sid, data in sessions.items()
        if now - data.get("created_at", now) > SESSION_TTL_SECONDS
    ]
    for sid in expired:
        del sessions[sid]
    if expired:
        logger.info("Expired %d sessions, %d remaining", len(expired), len(sessions))


# ── Error Sanitization ──────────────────────────────────────────────

# Patterns to strip from error messages shown to users
_PATH_PATTERNS = [
    re.compile(r"/Users/[^\s:\"']+"),           # macOS paths
    re.compile(r"/home/[^\s:\"']+"),            # Linux paths
    re.compile(r"/opt/[^\s:\"']+"),             # deployment paths
    re.compile(r"/tmp/[^\s:\"']+"),             # temp paths
    re.compile(r"[A-Z]:\\[^\s:\"']+"),          # Windows paths
    re.compile(r"File \"[^\"]+\""),             # Python traceback file refs
    re.compile(r", line \d+, in \w+"),          # Python traceback line refs
]


def sanitize_error(error: str | Exception, include_details: bool = False) -> str:
    """Sanitize error message for user display.

    In production (include_details=False): strips paths, tracebacks, internal info.
    In dev (include_details=True): returns full message.
    """
    msg = str(error)

    if include_details:
        return msg

    # Strip internal paths and traceback details
    for pattern in _PATH_PATTERNS:
        msg = pattern.sub("[...]", msg)

    # Limit length
    if len(msg) > 300:
        msg = msg[:297] + "..."

    return msg


def get_client_ip(request: Any) -> str:
    """Extract client IP from request, respecting X-Forwarded-For (behind nginx)."""
    # Check X-Forwarded-For (set by nginx/reverse proxy)
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        # Take the first (client) IP
        return forwarded.split(",")[0].strip()
    # Check X-Real-IP (nginx)
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    # Direct connection
    if hasattr(request, "client") and request.client:
        return request.client.host
    return "unknown"
