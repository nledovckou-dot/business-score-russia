"""Session storage abstraction with pluggable backends (T1).

Provides MemoryStore (default, same as before) and FileStore (survives restart).
Use get_store() factory to obtain the configured backend.

Design notes:
- get() returns a mutable dict reference. For MemoryStore this is the live object.
  For FileStore this is loaded from disk into an in-memory cache.
- Background threads mutate the dict directly (push events, change status, add data).
- save(sid) persists the current state. For MemoryStore it's a no-op (data is already
  in memory). For FileStore it writes the cached dict to disk atomically.
- Call save() at key checkpoints (status transitions, end of pipeline steps).
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

logger = logging.getLogger("bsr.session_store")

DEFAULT_TTL_SEC = 2 * 3600  # 2 hours


# ── Abstract base ──────────────────────────────────────────────────────

class SessionStore(ABC):
    """Abstract session store."""

    @abstractmethod
    def get(self, session_id: str) -> dict | None:
        """Return session data dict (mutable reference), or None if missing/expired."""
        ...

    @abstractmethod
    def set(self, session_id: str, data: dict, ttl_sec: int = DEFAULT_TTL_SEC) -> None:
        """Create or overwrite a session."""
        ...

    @abstractmethod
    def save(self, session_id: str) -> None:
        """Persist current in-memory state to durable storage.

        For MemoryStore: no-op (data is already in memory).
        For FileStore: writes the cached dict to disk atomically.
        Call this at key checkpoints (status transitions, end of steps).
        """
        ...

    @abstractmethod
    def delete(self, session_id: str) -> None:
        """Delete a session (no error if missing)."""
        ...

    @abstractmethod
    def exists(self, session_id: str) -> bool:
        """Check existence (respects TTL)."""
        ...

    @abstractmethod
    def cleanup_expired(self) -> int:
        """Remove expired sessions. Returns count of removed sessions."""
        ...

    @abstractmethod
    def list_sessions(self) -> list[str]:
        """Return list of active (non-expired) session IDs."""
        ...


# ── In-memory backend ──────────────────────────────────────────────────

class MemoryStore(SessionStore):
    """In-memory store (current behavior, default).

    Uses dict + threading.Lock for thread safety.
    TTL enforcement on get() + periodic cleanup.
    get() returns the live dict reference -- mutations are reflected immediately.
    """

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}
        self._meta: dict[str, dict[str, float]] = {}  # {sid: {created_at, ttl}}
        self._lock = threading.Lock()

    def get(self, session_id: str) -> dict | None:
        with self._lock:
            meta = self._meta.get(session_id)
            if meta is None:
                return None
            # TTL check
            if time.time() - meta["created_at"] > meta["ttl"]:
                self._data.pop(session_id, None)
                self._meta.pop(session_id, None)
                return None
            return self._data.get(session_id)

    def set(self, session_id: str, data: dict, ttl_sec: int = DEFAULT_TTL_SEC) -> None:
        now = time.time()
        with self._lock:
            existing_meta = self._meta.get(session_id)
            self._data[session_id] = data
            self._meta[session_id] = {
                "created_at": existing_meta["created_at"] if existing_meta else now,
                "ttl": ttl_sec,
            }

    def save(self, session_id: str) -> None:
        """No-op for MemoryStore -- data is already in memory."""
        pass

    def delete(self, session_id: str) -> None:
        with self._lock:
            self._data.pop(session_id, None)
            self._meta.pop(session_id, None)

    def exists(self, session_id: str) -> bool:
        with self._lock:
            meta = self._meta.get(session_id)
            if meta is None:
                return False
            if time.time() - meta["created_at"] > meta["ttl"]:
                self._data.pop(session_id, None)
                self._meta.pop(session_id, None)
                return False
            return True

    def cleanup_expired(self) -> int:
        now = time.time()
        with self._lock:
            expired = [
                sid for sid, meta in self._meta.items()
                if now - meta["created_at"] > meta["ttl"]
            ]
            for sid in expired:
                self._data.pop(sid, None)
                self._meta.pop(sid, None)
        if expired:
            logger.info(
                "MemoryStore: expired %d sessions, %d remaining",
                len(expired), len(self._data),
            )
        return len(expired)

    def list_sessions(self) -> list[str]:
        now = time.time()
        with self._lock:
            return [
                sid for sid, meta in self._meta.items()
                if now - meta["created_at"] <= meta["ttl"]
            ]


# ── File-based backend ─────────────────────────────────────────────────

class FileStore(SessionStore):
    """File-based store -- survives server restart.

    Each session = JSON file in {base_dir}/{session_id}.json.
    In-memory cache for fast reads; save() flushes to disk atomically.

    Architecture:
    - set() puts data into _cache AND writes to disk.
    - get() reads from _cache (fast path) or loads from disk (cold start).
    - save() flushes _cache[sid] to disk (atomic: tempfile + os.replace).
    - Background threads mutate the cached dict directly; save() persists.
    - On restart, get() re-loads from disk files.

    Uses fcntl file locking to prevent race conditions.
    """

    def __init__(self, base_dir: str | Path = "data/sessions") -> None:
        self._dir = Path(base_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # In-memory cache: {sid: session_data_dict}
        self._cache: dict[str, dict[str, Any]] = {}
        # Metadata cache: {sid: {created_at, ttl}}
        self._meta: dict[str, dict[str, float]] = {}
        # Load existing sessions from disk on startup
        self._load_all()
        logger.info("FileStore: using directory %s", self._dir.resolve())

    def _load_all(self) -> None:
        """Load all non-expired session files into cache on startup."""
        now = time.time()
        loaded = 0
        for fpath in self._dir.glob("*.json"):
            envelope = self._read_file(fpath)
            if envelope is None:
                continue
            meta = envelope.get("_meta", {})
            created_at = meta.get("created_at", 0)
            ttl = meta.get("ttl", DEFAULT_TTL_SEC)
            if now - created_at > ttl:
                # Expired -- clean up
                try:
                    fpath.unlink(missing_ok=True)
                except OSError:
                    pass
                continue
            sid = fpath.stem
            self._cache[sid] = envelope.get("data", {})
            self._meta[sid] = {"created_at": created_at, "ttl": ttl}
            loaded += 1
        if loaded:
            logger.info("FileStore: loaded %d sessions from disk", loaded)

    def _path(self, session_id: str) -> Path:
        # Sanitize session_id to prevent path traversal
        safe_id = "".join(c for c in session_id if c.isalnum() or c in "-_")
        return self._dir / f"{safe_id}.json"

    def _read_file(self, path: Path) -> dict | None:
        """Read and parse a session file. Returns None if missing or invalid."""
        if not path.exists():
            return None
        try:
            fd = os.open(str(path), os.O_RDONLY)
            try:
                fcntl.flock(fd, fcntl.LOCK_SH)
                raw = os.read(fd, 50 * 1024 * 1024)  # 50MB max
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
            return json.loads(raw.decode("utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("FileStore: failed to read %s: %s", path, exc)
            return None

    def _write_file(self, path: Path, envelope: dict) -> None:
        """Atomic write: tempfile in same dir + os.replace."""
        raw = json.dumps(envelope, ensure_ascii=False, default=str).encode("utf-8")
        fd, tmp_path = tempfile.mkstemp(dir=str(self._dir), suffix=".tmp")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            os.write(fd, raw)
            os.fsync(fd)
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            os.replace(tmp_path, str(path))
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _flush(self, session_id: str) -> None:
        """Write session from cache to disk (must hold self._lock)."""
        data = self._cache.get(session_id)
        meta = self._meta.get(session_id)
        if data is None or meta is None:
            return
        envelope = {
            "_meta": {
                "created_at": meta["created_at"],
                "updated_at": time.time(),
                "ttl": meta["ttl"],
            },
            "data": data,
        }
        self._write_file(self._path(session_id), envelope)

    def get(self, session_id: str) -> dict | None:
        with self._lock:
            # Fast path: check cache
            if session_id in self._cache:
                meta = self._meta.get(session_id, {})
                if time.time() - meta.get("created_at", 0) > meta.get("ttl", DEFAULT_TTL_SEC):
                    # Expired
                    self._cache.pop(session_id, None)
                    self._meta.pop(session_id, None)
                    try:
                        self._path(session_id).unlink(missing_ok=True)
                    except OSError:
                        pass
                    return None
                return self._cache[session_id]

            # Cold path: try loading from disk (e.g., after code reload without full restart)
            path = self._path(session_id)
            envelope = self._read_file(path)
            if envelope is None:
                return None
            meta = envelope.get("_meta", {})
            created_at = meta.get("created_at", 0)
            ttl = meta.get("ttl", DEFAULT_TTL_SEC)
            if time.time() - created_at > ttl:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
                return None
            # Populate cache
            data = envelope.get("data", {})
            self._cache[session_id] = data
            self._meta[session_id] = {"created_at": created_at, "ttl": ttl}
            return data

    def set(self, session_id: str, data: dict, ttl_sec: int = DEFAULT_TTL_SEC) -> None:
        now = time.time()
        with self._lock:
            existing_meta = self._meta.get(session_id)
            self._cache[session_id] = data
            self._meta[session_id] = {
                "created_at": existing_meta["created_at"] if existing_meta else now,
                "ttl": ttl_sec,
            }
            self._flush(session_id)

    def save(self, session_id: str) -> None:
        """Persist current in-memory state to disk."""
        with self._lock:
            if session_id in self._cache:
                self._flush(session_id)

    def delete(self, session_id: str) -> None:
        with self._lock:
            self._cache.pop(session_id, None)
            self._meta.pop(session_id, None)
            try:
                self._path(session_id).unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("FileStore: failed to delete %s: %s", self._path(session_id), exc)

    def exists(self, session_id: str) -> bool:
        # Delegate to get() which handles TTL + cache + disk
        return self.get(session_id) is not None

    def cleanup_expired(self) -> int:
        now = time.time()
        count = 0
        with self._lock:
            # Clean cache
            expired_sids = [
                sid for sid, meta in self._meta.items()
                if now - meta.get("created_at", 0) > meta.get("ttl", DEFAULT_TTL_SEC)
            ]
            for sid in expired_sids:
                self._cache.pop(sid, None)
                self._meta.pop(sid, None)
                try:
                    self._path(sid).unlink(missing_ok=True)
                except OSError:
                    pass
                count += 1
            # Also scan disk for files not in cache (orphaned)
            for fpath in self._dir.glob("*.json"):
                sid = fpath.stem
                if sid in self._meta:
                    continue  # already handled above (or still valid)
                envelope = self._read_file(fpath)
                if envelope is None:
                    try:
                        fpath.unlink(missing_ok=True)
                        count += 1
                    except OSError:
                        pass
                    continue
                meta = envelope.get("_meta", {})
                created_at = meta.get("created_at", 0)
                ttl = meta.get("ttl", DEFAULT_TTL_SEC)
                if now - created_at > ttl:
                    try:
                        fpath.unlink(missing_ok=True)
                        count += 1
                    except OSError:
                        pass

        if count:
            logger.info(
                "FileStore: expired %d sessions, %d remaining",
                count, len(self._cache),
            )
        return count

    def list_sessions(self) -> list[str]:
        now = time.time()
        with self._lock:
            return [
                sid for sid, meta in self._meta.items()
                if now - meta.get("created_at", 0) <= meta.get("ttl", DEFAULT_TTL_SEC)
            ]


# ── Factory ────────────────────────────────────────────────────────────

_store_instance: SessionStore | None = None
_store_lock = threading.Lock()


def get_store() -> SessionStore:
    """Get configured session store (singleton).

    Reads STORE_BACKEND env var:
    - "memory" (default) -> MemoryStore
    - "file" -> FileStore(path from STORE_FILE_DIR or "data/sessions")
    """
    global _store_instance
    if _store_instance is not None:
        return _store_instance

    with _store_lock:
        # Double-check after acquiring lock
        if _store_instance is not None:
            return _store_instance

        backend = os.getenv("STORE_BACKEND", "memory").lower().strip()

        if backend == "file":
            base_dir = os.getenv("STORE_FILE_DIR", "data/sessions")
            _store_instance = FileStore(base_dir=base_dir)
            logger.info("Session store: FileStore (dir=%s)", base_dir)
        else:
            _store_instance = MemoryStore()
            logger.info("Session store: MemoryStore (in-memory)")

    return _store_instance
