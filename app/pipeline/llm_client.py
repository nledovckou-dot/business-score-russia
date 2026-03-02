"""Multi-provider LLM client: GPT-5.2 Pro (main brain) + Gemini 2.5 Flash (fast tasks)."""

from __future__ import annotations

import json
import os
import urllib.request
import urllib.error
import re
import time
from typing import Any, Optional


# ── Provider configs ──

OPENAI_URL = "https://api.openai.com/v1/chat/completions"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

MODEL_MAIN = "gpt-5.2-pro"       # основной мозг
MODEL_FAST = "gemini-2.5-flash"   # быстрые задачи
MODEL_REASON = "o3"               # reasoning


def _openai_key() -> str:
    key = os.environ.get("OPENAI_API_KEY") or os.environ.get("FALLBACK_LLM_API_KEY", "")
    if not key:
        raise RuntimeError("OPENAI_API_KEY / FALLBACK_LLM_API_KEY not set")
    return key


def _gemini_key() -> str:
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set")
    return key


def call_openai(
    prompt: str,
    model: str = MODEL_MAIN,
    system: str = "",
    temperature: float = 0.4,
    max_tokens: int = 16000,
    json_mode: bool = False,
) -> str:
    """Call OpenAI-compatible API. Returns text response."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        OPENAI_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {_openai_key()}",
        },
        method="POST",
    )

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            if e.code in (429, 500, 502, 503) and attempt < 2:
                wait = (attempt + 1) * 5
                time.sleep(wait)
                continue
            raise RuntimeError(f"OpenAI API error {e.code}: {error_body[:500]}")
        except Exception as e:
            if attempt < 2:
                time.sleep(3)
                continue
            raise


def call_gemini(
    prompt: str,
    model: str = MODEL_FAST,
    temperature: float = 0.4,
    max_tokens: int = 8000,
    json_mode: bool = False,
) -> str:
    """Call Gemini API. Returns text response."""
    url = GEMINI_URL.format(model=model) + f"?key={_gemini_key()}"

    gen_config: dict[str, Any] = {
        "temperature": temperature,
        "maxOutputTokens": max_tokens,
    }
    if json_mode:
        gen_config["responseMimeType"] = "application/json"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": gen_config,
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            return body["candidates"][0]["content"]["parts"][0]["text"]
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            if e.code in (429, 500, 502, 503) and attempt < 2:
                time.sleep((attempt + 1) * 3)
                continue
            raise RuntimeError(f"Gemini API error {e.code}: {error_body[:500]}")
        except Exception as e:
            if attempt < 2:
                time.sleep(3)
                continue
            raise


def call_llm_json(
    prompt: str,
    provider: str = "openai",
    model: Optional[str] = None,
    system: str = "",
    temperature: float = 0.4,
    max_tokens: int = 16000,
) -> dict:
    """Call LLM and parse JSON response. Strips markdown fences if present."""
    if provider == "openai":
        text = call_openai(
            prompt, model=model or MODEL_MAIN, system=system,
            temperature=temperature, max_tokens=max_tokens, json_mode=True,
        )
    else:
        text = call_gemini(
            prompt, model=model or MODEL_FAST,
            temperature=temperature, max_tokens=max_tokens, json_mode=True,
        )

    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse LLM JSON: {e}\nRaw: {text[:1000]}")
