"""Tests for app.session_store — MemoryStore and FileStore backends (T1)."""

from __future__ import annotations

import os
import tempfile
import time

import pytest

from app.session_store import MemoryStore, FileStore, get_store, DEFAULT_TTL_SEC


# ── MemoryStore tests ──────────────────────────────────────────────────


class TestMemoryStore:

    def test_set_and_get(self):
        s = MemoryStore()
        data = {"status": "created", "events": [], "data": {}}
        s.set("abc", data)
        result = s.get("abc")
        assert result is data  # same object reference for MemoryStore
        assert result["status"] == "created"

    def test_get_nonexistent_returns_none(self):
        s = MemoryStore()
        assert s.get("nonexistent") is None

    def test_exists(self):
        s = MemoryStore()
        assert not s.exists("abc")
        s.set("abc", {"status": "ok"})
        assert s.exists("abc")

    def test_delete(self):
        s = MemoryStore()
        s.set("abc", {"status": "ok"})
        assert s.exists("abc")
        s.delete("abc")
        assert not s.exists("abc")
        assert s.get("abc") is None

    def test_delete_nonexistent_no_error(self):
        s = MemoryStore()
        s.delete("nonexistent")  # should not raise

    def test_list_sessions(self):
        s = MemoryStore()
        s.set("a", {"x": 1})
        s.set("b", {"x": 2})
        s.set("c", {"x": 3})
        result = s.list_sessions()
        assert sorted(result) == ["a", "b", "c"]

    def test_ttl_expired_on_get(self):
        s = MemoryStore()
        s.set("abc", {"status": "ok"}, ttl_sec=1)
        assert s.get("abc") is not None
        time.sleep(1.1)
        assert s.get("abc") is None

    def test_ttl_expired_on_exists(self):
        s = MemoryStore()
        s.set("abc", {"status": "ok"}, ttl_sec=1)
        assert s.exists("abc")
        time.sleep(1.1)
        assert not s.exists("abc")

    def test_cleanup_expired(self):
        s = MemoryStore()
        s.set("expired", {"x": 1}, ttl_sec=0)  # already expired
        s.set("alive", {"x": 2}, ttl_sec=3600)
        time.sleep(0.1)
        count = s.cleanup_expired()
        assert count == 1
        assert not s.exists("expired")
        assert s.exists("alive")

    def test_save_is_noop(self):
        s = MemoryStore()
        s.set("abc", {"status": "ok"})
        s.save("abc")  # should not raise
        assert s.get("abc")["status"] == "ok"

    def test_mutable_reference(self):
        """MemoryStore.get() returns a live reference -- mutations are visible."""
        s = MemoryStore()
        s.set("abc", {"events": []})
        ref = s.get("abc")
        ref["events"].append({"event": "test"})
        # The mutation should be visible via another get()
        assert len(s.get("abc")["events"]) == 1

    def test_set_preserves_created_at(self):
        s = MemoryStore()
        s.set("abc", {"v": 1})
        time.sleep(0.05)
        s.set("abc", {"v": 2})  # update
        # created_at should be preserved (from first set)
        meta = s._meta["abc"]
        # The created_at should be older than "now"
        assert time.time() - meta["created_at"] >= 0.05


# ── FileStore tests ────────────────────────────────────────────────────


class TestFileStore:

    def _make_store(self, tmp_path):
        return FileStore(base_dir=str(tmp_path / "sessions"))

    def test_set_and_get(self, tmp_path):
        s = self._make_store(tmp_path)
        data = {"status": "created", "events": [], "data": {}}
        s.set("abc", data)
        result = s.get("abc")
        assert result is not None
        assert result["status"] == "created"

    def test_get_nonexistent_returns_none(self, tmp_path):
        s = self._make_store(tmp_path)
        assert s.get("nonexistent") is None

    def test_exists(self, tmp_path):
        s = self._make_store(tmp_path)
        assert not s.exists("abc")
        s.set("abc", {"status": "ok"})
        assert s.exists("abc")

    def test_delete(self, tmp_path):
        s = self._make_store(tmp_path)
        s.set("abc", {"status": "ok"})
        s.delete("abc")
        assert not s.exists("abc")
        assert s.get("abc") is None

    def test_list_sessions(self, tmp_path):
        s = self._make_store(tmp_path)
        s.set("a", {"x": 1})
        s.set("b", {"x": 2})
        s.set("c", {"x": 3})
        result = s.list_sessions()
        assert sorted(result) == ["a", "b", "c"]

    def test_ttl_expired(self, tmp_path):
        s = self._make_store(tmp_path)
        s.set("abc", {"status": "ok"}, ttl_sec=1)
        assert s.get("abc") is not None
        time.sleep(1.1)
        assert s.get("abc") is None

    def test_cleanup_expired(self, tmp_path):
        s = self._make_store(tmp_path)
        s.set("expired", {"x": 1}, ttl_sec=0)
        s.set("alive", {"x": 2}, ttl_sec=3600)
        time.sleep(0.1)
        count = s.cleanup_expired()
        assert count >= 1
        assert not s.exists("expired")
        assert s.exists("alive")

    def test_survives_restart(self, tmp_path):
        """FileStore persists data -- a new instance reads from disk."""
        base = str(tmp_path / "sessions")
        s1 = FileStore(base_dir=base)
        s1.set("abc", {"status": "analyzing", "events": [{"e": 1}]})

        # Simulate restart: create a new FileStore instance
        s2 = FileStore(base_dir=base)
        result = s2.get("abc")
        assert result is not None
        assert result["status"] == "analyzing"
        assert len(result["events"]) == 1

    def test_mutable_reference_with_save(self, tmp_path):
        """FileStore.get() returns a cached reference. save() persists."""
        s = self._make_store(tmp_path)
        s.set("abc", {"events": []})

        ref = s.get("abc")
        ref["events"].append({"event": "test"})
        s.save("abc")  # persist to disk

        # Re-read from disk (simulate restart)
        s2 = FileStore(base_dir=s._dir)
        result = s2.get("abc")
        assert len(result["events"]) == 1
        assert result["events"][0]["event"] == "test"

    def test_atomic_write(self, tmp_path):
        """Verify file is written atomically (no partial files)."""
        s = self._make_store(tmp_path)
        s.set("abc", {"big": "x" * 10000})
        # File should exist and be valid JSON
        fpath = s._path("abc")
        assert fpath.exists()
        import json
        with open(fpath) as f:
            data = json.load(f)
        assert data["data"]["big"] == "x" * 10000

    def test_session_id_sanitization(self, tmp_path):
        """Path traversal in session_id should be sanitized."""
        s = self._make_store(tmp_path)
        s.set("../../../etc/passwd", {"evil": True})
        # Should NOT create a file outside the sessions directory
        evil_path = tmp_path / "sessions" / "..%2F..%2F..%2Fetc%2Fpasswd.json"
        assert not evil_path.exists()
        # Should create a safe file
        safe_path = s._path("../../../etc/passwd")
        assert "etcpasswd" in safe_path.name  # sanitized name

    def test_set_preserves_created_at(self, tmp_path):
        s = self._make_store(tmp_path)
        s.set("abc", {"v": 1})
        time.sleep(0.05)
        s.set("abc", {"v": 2})
        # Read from disk to verify created_at is preserved
        import json
        with open(s._path("abc")) as f:
            envelope = json.load(f)
        # created_at should be from the first set
        assert time.time() - envelope["_meta"]["created_at"] >= 0.05


# ── Factory tests ──────────────────────────────────────────────────────


class TestGetStore:

    def test_default_is_memory(self, monkeypatch):
        """Default backend is MemoryStore."""
        import app.session_store as mod
        # Reset singleton
        monkeypatch.setattr(mod, "_store_instance", None)
        monkeypatch.delenv("STORE_BACKEND", raising=False)
        s = mod.get_store()
        assert isinstance(s, MemoryStore)
        # Reset after test
        monkeypatch.setattr(mod, "_store_instance", None)

    def test_file_backend(self, monkeypatch, tmp_path):
        """STORE_BACKEND=file creates FileStore."""
        import app.session_store as mod
        monkeypatch.setattr(mod, "_store_instance", None)
        monkeypatch.setenv("STORE_BACKEND", "file")
        monkeypatch.setenv("STORE_FILE_DIR", str(tmp_path / "sessions"))
        s = mod.get_store()
        assert isinstance(s, FileStore)
        monkeypatch.setattr(mod, "_store_instance", None)

    def test_singleton(self, monkeypatch):
        """get_store() returns the same instance."""
        import app.session_store as mod
        monkeypatch.setattr(mod, "_store_instance", None)
        monkeypatch.delenv("STORE_BACKEND", raising=False)
        s1 = mod.get_store()
        s2 = mod.get_store()
        assert s1 is s2
        monkeypatch.setattr(mod, "_store_instance", None)
