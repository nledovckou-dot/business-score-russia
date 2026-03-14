"""Simple auth: cookie-based sessions, file-based user storage.

Supports email/password login AND Google Sign-In.
No external dependencies — uses stdlib hashlib, secrets, json, urllib.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger("bsr.auth")

USERS_DIR = Path("data/users")
TOKEN_INDEX_PATH = Path("data/users/_tokens.json")
FREE_REPORTS_LIMIT = 5
TOKEN_TTL_DAYS = 30
MAX_TOKENS_PER_USER = 5

# Google OAuth — whitelist of allowed emails (env: BSR_ALLOWED_EMAILS, comma-separated)
ALLOWED_EMAILS = set(
    e.strip().lower()
    for e in os.getenv("BSR_ALLOWED_EMAILS", "").split(",")
    if e.strip()
)
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

# Yandex OAuth
YANDEX_CLIENT_ID = os.getenv("YANDEX_CLIENT_ID", "")
YANDEX_CLIENT_SECRET = os.getenv("YANDEX_CLIENT_SECRET", "")

# Simple email regex (not RFC 5322 compliant, but good enough for MVP)
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

_lock = threading.Lock()


def _email_to_filename(email: str) -> str:
    """Deterministic filename from email (sha256 hex)."""
    return hashlib.sha256(email.lower().strip().encode()).hexdigest() + ".json"


def _hash_password(password: str, salt: bytes | None = None) -> tuple[bytes, bytes]:
    """Hash password with PBKDF2-HMAC-SHA256. Returns (salt, hash)."""
    if salt is None:
        salt = os.urandom(32)
    pw_hash = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
    return salt, pw_hash


def _load_token_index() -> dict[str, str]:
    """Load token -> email_hash index. Returns {token: email_hash_filename}."""
    if not TOKEN_INDEX_PATH.exists():
        return {}
    try:
        return json.loads(TOKEN_INDEX_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_token_index(index: dict[str, str]):
    """Save token index atomically."""
    TOKEN_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(TOKEN_INDEX_PATH) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False)
    os.replace(tmp, str(TOKEN_INDEX_PATH))


def _load_user(filename: str) -> dict | None:
    """Load user dict from file."""
    path = USERS_DIR / filename
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _save_user(filename: str, user: dict):
    """Save user dict atomically."""
    USERS_DIR.mkdir(parents=True, exist_ok=True)
    path = USERS_DIR / filename
    tmp = str(path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(user, f, ensure_ascii=False, indent=2)
    os.replace(tmp, str(path))


class AuthManager:
    """File-based user authentication and report quota management."""

    def __init__(self):
        USERS_DIR.mkdir(parents=True, exist_ok=True)

    def register(
        self,
        email: str,
        password: str,
        consent_data: bool = False,
        consent_marketing: bool = False,
        client_ip: str = "",
    ) -> dict:
        """Register new user. Returns user public dict or raises ValueError."""
        email = email.lower().strip()
        if not email or not _EMAIL_RE.match(email):
            raise ValueError("Некорректный email")
        if len(email) > 254:
            raise ValueError("Email слишком длинный")
        if not password or len(password) < 6:
            raise ValueError("Пароль должен быть не менее 6 символов")
        if len(password) > 128:
            raise ValueError("Пароль слишком длинный")
        if not consent_data:
            raise ValueError("Необходимо согласие на обработку персональных данных")

        filename = _email_to_filename(email)

        with _lock:
            # Check if user exists
            existing = _load_user(filename)
            if existing:
                raise ValueError("Пользователь с таким email уже зарегистрирован")

            # Hash password
            salt, pw_hash = _hash_password(password)

            # Generate session token
            token = secrets.token_urlsafe(32)
            now = time.time()

            user = {
                "email": email,
                "password_salt": salt.hex(),
                "password_hash": pw_hash.hex(),
                "created_at": now,
                "reports_used": 0,
                "report_ids": [],
                "consent_data_processing": True,
                "consent_email_marketing": bool(consent_marketing),
                "consent_timestamp": now,
                "consent_ip": client_ip,
                "active_tokens": [
                    {
                        "token": token,
                        "created_at": now,
                        "last_used": now,
                    }
                ],
            }

            _save_user(filename, user)

            # Update token index
            index = _load_token_index()
            index[token] = filename
            _save_token_index(index)

        logger.info("New user registered: %s", email)
        return {
            "email": email,
            "token": token,
            "reports_used": 0,
            "reports_remaining": FREE_REPORTS_LIMIT,
        }

    def login(self, email: str, password: str) -> dict | None:
        """Login. Returns user public dict with token, or None on failure."""
        email = email.lower().strip()
        if not email or not password:
            return None

        filename = _email_to_filename(email)

        with _lock:
            user = _load_user(filename)
            if not user:
                return None

            # Verify password
            salt = bytes.fromhex(user["password_salt"])
            _, pw_hash = _hash_password(password, salt)
            if pw_hash.hex() != user["password_hash"]:
                return None

            # Generate new token
            token = secrets.token_urlsafe(32)
            now = time.time()

            # Add token, evict old ones if over limit
            tokens = user.get("active_tokens", [])
            # Remove expired tokens
            tokens = [
                t for t in tokens
                if now - t.get("created_at", 0) < TOKEN_TTL_DAYS * 86400
            ]
            # If at limit, remove oldest
            if len(tokens) >= MAX_TOKENS_PER_USER:
                tokens.sort(key=lambda t: t.get("last_used", 0))
                removed = tokens[:len(tokens) - MAX_TOKENS_PER_USER + 1]
                tokens = tokens[len(tokens) - MAX_TOKENS_PER_USER + 1:]
                # Clean removed tokens from index
                index = _load_token_index()
                for rt in removed:
                    index.pop(rt.get("token", ""), None)
                _save_token_index(index)

            tokens.append({
                "token": token,
                "created_at": now,
                "last_used": now,
            })
            user["active_tokens"] = tokens
            _save_user(filename, user)

            # Update token index
            index = _load_token_index()
            index[token] = filename
            _save_token_index(index)

        reports_used = user.get("reports_used", 0)
        logger.info("User logged in: %s", email)
        return {
            "email": email,
            "token": token,
            "reports_used": reports_used,
            "reports_remaining": max(0, FREE_REPORTS_LIMIT - reports_used),
        }

    def check_token(self, token: str) -> dict | None:
        """Validate session token. Returns user public dict or None."""
        if not token:
            return None

        with _lock:
            index = _load_token_index()
            filename = index.get(token)
            if not filename:
                return None

            user = _load_user(filename)
            if not user:
                # Stale index entry
                index.pop(token, None)
                _save_token_index(index)
                return None

            # Find token in user's active tokens
            now = time.time()
            found = False
            for t in user.get("active_tokens", []):
                if t.get("token") == token:
                    # Check expiry
                    if now - t.get("created_at", 0) > TOKEN_TTL_DAYS * 86400:
                        # Expired — remove
                        user["active_tokens"] = [
                            x for x in user["active_tokens"]
                            if x.get("token") != token
                        ]
                        _save_user(filename, user)
                        index.pop(token, None)
                        _save_token_index(index)
                        return None
                    t["last_used"] = now
                    found = True
                    break

            if not found:
                index.pop(token, None)
                _save_token_index(index)
                return None

            _save_user(filename, user)

        reports_used = user.get("reports_used", 0)
        return {
            "email": user.get("email", ""),
            "reports_used": reports_used,
            "reports_remaining": max(0, FREE_REPORTS_LIMIT - reports_used),
            "report_ids": user.get("report_ids", []),
        }

    def can_generate_report(self, token: str | None) -> tuple[bool, str]:
        """Check if user can generate a report.

        Returns (allowed, reason).
        - No token -> allow (anonymous, no tracking)
        - Valid token + reports_used < limit -> allow
        - Valid token + reports_used >= limit -> deny
        - Invalid token -> allow (treat as anonymous)
        """
        if not token:
            return True, ""

        user = self.check_token(token)
        if user is None:
            # Invalid/expired token — treat as anonymous
            return True, ""

        if user["reports_used"] >= FREE_REPORTS_LIMIT:
            return False, (
                f"Лимит бесплатных отчётов исчерпан ({FREE_REPORTS_LIMIT}/{FREE_REPORTS_LIMIT}). "
                "Свяжитесь с нами для расширения доступа."
            )

        return True, ""

    def increment_report_count(self, token: str | None, report_id: str = ""):
        """Increment reports_used for the user. No-op if token is None/invalid."""
        if not token:
            return

        with _lock:
            index = _load_token_index()
            filename = index.get(token)
            if not filename:
                return

            user = _load_user(filename)
            if not user:
                return

            user["reports_used"] = user.get("reports_used", 0) + 1
            if report_id:
                ids = user.get("report_ids", [])
                ids.append(report_id)
                user["report_ids"] = ids
            _save_user(filename, user)

        logger.info("Report count incremented for user (token=%s...)", token[:8])

    def get_user_reports(self, token: str) -> list[str]:
        """Get list of user's generated report IDs."""
        user = self.check_token(token)
        if not user:
            return []
        return user.get("report_ids", [])

    def google_login(self, id_token: str) -> dict | None:
        """Verify Google ID token and login/register user.

        Returns user public dict with token, or None on failure.
        """
        if not GOOGLE_CLIENT_ID:
            logger.error("GOOGLE_CLIENT_ID not set")
            return None

        # Verify token with Google
        try:
            url = f"https://oauth2.googleapis.com/tokeninfo?id_token={id_token}"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("Google token verification failed: %s", str(e)[:200])
            return None

        # Check audience matches our client ID
        if payload.get("aud") != GOOGLE_CLIENT_ID:
            logger.warning("Google token aud mismatch: %s", payload.get("aud", ""))
            return None

        email = payload.get("email", "").lower().strip()
        if not email or not payload.get("email_verified", False):
            logger.warning("Google email not verified: %s", email)
            return None

        # Login or auto-register
        filename = _email_to_filename(email)
        with _lock:
            user = _load_user(filename)
            token = secrets.token_urlsafe(32)
            now = time.time()

            if not user:
                # Auto-register Google user (no password)
                user = {
                    "email": email,
                    "google_sub": payload.get("sub", ""),
                    "name": payload.get("name", ""),
                    "picture": payload.get("picture", ""),
                    "created_at": now,
                    "reports_used": 0,
                    "report_ids": [],
                    "active_tokens": [],
                }
                logger.info("Auto-registered Google user: %s", email)

            # Add token
            tokens = user.get("active_tokens", [])
            tokens = [
                t for t in tokens
                if now - t.get("created_at", 0) < TOKEN_TTL_DAYS * 86400
            ]
            if len(tokens) >= MAX_TOKENS_PER_USER:
                tokens.sort(key=lambda t: t.get("last_used", 0))
                removed = tokens[:len(tokens) - MAX_TOKENS_PER_USER + 1]
                tokens = tokens[len(tokens) - MAX_TOKENS_PER_USER + 1:]
                index = _load_token_index()
                for rt in removed:
                    index.pop(rt.get("token", ""), None)
                _save_token_index(index)

            tokens.append({"token": token, "created_at": now, "last_used": now})
            user["active_tokens"] = tokens
            # Update Google profile
            user["google_sub"] = payload.get("sub", user.get("google_sub", ""))
            user["name"] = payload.get("name", user.get("name", ""))
            user["picture"] = payload.get("picture", user.get("picture", ""))
            _save_user(filename, user)

            index = _load_token_index()
            index[token] = filename
            _save_token_index(index)

        reports_used = user.get("reports_used", 0)
        logger.info("Google login: %s", email)
        return {
            "email": email,
            "name": user.get("name", ""),
            "picture": user.get("picture", ""),
            "token": token,
            "reports_used": reports_used,
            "reports_remaining": max(0, FREE_REPORTS_LIMIT - reports_used),
        }

    def yandex_login(self, auth_code: str) -> dict | None:
        """Exchange Yandex auth code for token, login/register user.

        Returns user public dict with session token, or None on failure.
        """
        if not YANDEX_CLIENT_ID or not YANDEX_CLIENT_SECRET:
            logger.error("YANDEX_CLIENT_ID or YANDEX_CLIENT_SECRET not set")
            return None

        # Exchange code for access token
        try:
            token_data = urllib.parse.urlencode({
                "grant_type": "authorization_code",
                "code": auth_code,
                "client_id": YANDEX_CLIENT_ID,
                "client_secret": YANDEX_CLIENT_SECRET,
            }).encode()
            req = urllib.request.Request(
                "https://oauth.yandex.ru/token",
                data=token_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                token_resp = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("Yandex token exchange failed: %s", str(e)[:200])
            return None

        access_token = token_resp.get("access_token")
        if not access_token:
            logger.warning("Yandex: no access_token in response")
            return None

        # Get user info
        try:
            req = urllib.request.Request(
                "https://login.yandex.ru/info?format=json",
                headers={"Authorization": f"OAuth {access_token}"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                user_info = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("Yandex user info failed: %s", str(e)[:200])
            return None

        email = (user_info.get("default_email") or "").lower().strip()
        if not email:
            logger.warning("Yandex: no email in user info")
            return None

        # Login or auto-register
        filename = _email_to_filename(email)
        with _lock:
            user = _load_user(filename)
            session_token = secrets.token_urlsafe(32)
            now = time.time()

            if not user:
                # Auto-register Yandex user (no password)
                name = " ".join(filter(None, [
                    user_info.get("first_name", ""),
                    user_info.get("last_name", ""),
                ])).strip() or user_info.get("login", "")
                user = {
                    "email": email,
                    "yandex_id": str(user_info.get("id", "")),
                    "name": name,
                    "picture": f"https://avatars.yandex.net/get-yapic/{user_info.get('default_avatar_id', '0')}/islands-200",
                    "created_at": now,
                    "reports_used": 0,
                    "report_ids": [],
                    "consent_data_processing": True,
                    "consent_email_marketing": False,
                    "consent_timestamp": now,
                    "active_tokens": [],
                }
                logger.info("Auto-registered Yandex user: %s", email)

            # Add session token
            tokens = user.get("active_tokens", [])
            tokens = [
                t for t in tokens
                if now - t.get("created_at", 0) < TOKEN_TTL_DAYS * 86400
            ]
            if len(tokens) >= MAX_TOKENS_PER_USER:
                tokens.sort(key=lambda t: t.get("last_used", 0))
                removed = tokens[:len(tokens) - MAX_TOKENS_PER_USER + 1]
                tokens = tokens[len(tokens) - MAX_TOKENS_PER_USER + 1:]
                idx = _load_token_index()
                for rt in removed:
                    idx.pop(rt.get("token", ""), None)
                _save_token_index(idx)

            tokens.append({"token": session_token, "created_at": now, "last_used": now})
            user["active_tokens"] = tokens
            user["yandex_id"] = str(user_info.get("id", user.get("yandex_id", "")))
            _save_user(filename, user)

            idx = _load_token_index()
            idx[session_token] = filename
            _save_token_index(idx)

        reports_used = user.get("reports_used", 0)
        logger.info("Yandex login: %s", email)
        return {
            "email": email,
            "name": user.get("name", ""),
            "picture": user.get("picture", ""),
            "token": session_token,
            "reports_used": reports_used,
            "reports_remaining": max(0, FREE_REPORTS_LIMIT - reports_used),
        }

    def logout(self, token: str):
        """Remove a specific token (logout)."""
        if not token:
            return

        with _lock:
            index = _load_token_index()
            filename = index.pop(token, None)
            if filename:
                _save_token_index(index)
                user = _load_user(filename)
                if user:
                    user["active_tokens"] = [
                        t for t in user.get("active_tokens", [])
                        if t.get("token") != token
                    ]
                    _save_user(filename, user)

        logger.info("Token revoked: %s...", token[:8] if token else "none")
