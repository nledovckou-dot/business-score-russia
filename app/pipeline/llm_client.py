"""Multi-provider LLM client: GPT-5.2 Pro (main) + Gemini 2.5 Flash (fast) + GPT-5.3 Codex (board)."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
import urllib.error
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

logger = logging.getLogger(__name__)


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


# ── Board of Directors LLM (T28) ──


def call_board_llm(prompt: str, system: str | None = None) -> str:
    """Вызов LLM для совета директоров (GPT-5.3 Codex).

    Используется для AI-экспертов, которые критикуют и рецензируют отчёт.
    Низкая temperature (0.3) для точных, выверенных ответов.

    Fallback: если GPT-5.3 Codex недоступен, используется основной LLM (GPT-5.2 Pro).
    Retry: 2 попытки с backoff перед fallback.
    """
    from app.config import BOARD_LLM_MODEL, BOARD_LLM_TEMPERATURE, BOARD_LLM_MAX_TOKENS

    model = BOARD_LLM_MODEL
    temperature = BOARD_LLM_TEMPERATURE
    max_tokens = BOARD_LLM_MAX_TOKENS

    messages: list[dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    data = json.dumps(payload).encode("utf-8")

    # --- Попытка 1-2: GPT-5.3 Codex ---
    for attempt in range(2):
        t0 = time.monotonic()
        try:
            req = urllib.request.Request(
                OPENAI_URL,
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {_openai_key()}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=180) as resp:
                body = json.loads(resp.read().decode("utf-8"))

            elapsed = round(time.monotonic() - t0, 2)
            usage = body.get("usage", {})
            tokens_in = usage.get("prompt_tokens", "?")
            tokens_out = usage.get("completion_tokens", "?")
            logger.info(
                "Board LLM OK: model=%s, tokens_in=%s, tokens_out=%s, time=%.2fs",
                model, tokens_in, tokens_out, elapsed,
            )
            return body["choices"][0]["message"]["content"]

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            elapsed = round(time.monotonic() - t0, 2)
            logger.warning(
                "Board LLM attempt %d failed: model=%s, status=%d, time=%.2fs, error=%s",
                attempt + 1, model, e.code, elapsed, error_body[:200],
            )
            if e.code in (429, 500, 502, 503) and attempt < 1:
                time.sleep((attempt + 1) * 5)
                continue
            # Не retryable или исчерпаны попытки — идём в fallback
            break

        except Exception as exc:
            elapsed = round(time.monotonic() - t0, 2)
            logger.warning(
                "Board LLM attempt %d exception: model=%s, time=%.2fs, error=%s",
                attempt + 1, model, elapsed, str(exc)[:200],
            )
            if attempt < 1:
                time.sleep(3)
                continue
            break

    # --- Fallback: основной LLM (GPT-5.2 Pro) ---
    logger.info("Board LLM fallback: %s -> %s", model, MODEL_MAIN)
    return call_openai(
        prompt=prompt,
        model=MODEL_MAIN,
        system=system or "",
        temperature=temperature,
        max_tokens=max_tokens,
    )


def call_board_llm_parallel(prompts: list[dict]) -> list[str]:
    """Параллельные вызовы для экспертов совета директоров.

    Args:
        prompts: список словарей [{"prompt": "...", "system": "..."}, ...]
            - prompt (str): обязательный текст запроса
            - system (str, optional): системный промпт для эксперта

    Returns:
        list[str]: ответы в том же порядке, что и prompts.
        Если вызов для конкретного эксперта упал — возвращается строка с описанием ошибки
        (начинается с "[Board LLM Error]"), чтобы не ломать весь пайплайн.
    """
    results: list[str | None] = [None] * len(prompts)

    def _call_single(idx: int, item: dict) -> tuple[int, str]:
        t0 = time.monotonic()
        try:
            response = call_board_llm(
                prompt=item["prompt"],
                system=item.get("system"),
            )
            elapsed = round(time.monotonic() - t0, 2)
            logger.info("Board parallel #%d done in %.2fs", idx, elapsed)
            return idx, response
        except Exception as exc:
            elapsed = round(time.monotonic() - t0, 2)
            logger.error(
                "Board parallel #%d failed in %.2fs: %s", idx, elapsed, str(exc)[:300],
            )
            return idx, f"[Board LLM Error] Expert #{idx}: {str(exc)[:500]}"

    with ThreadPoolExecutor(max_workers=min(5, len(prompts))) as executor:
        futures = {
            executor.submit(_call_single, i, p): i
            for i, p in enumerate(prompts)
        }
        for future in as_completed(futures):
            idx, response = future.result()
            results[idx] = response

    # Гарантируем что нет None (на случай если futures не вернули результат)
    return [r if r is not None else "[Board LLM Error] No response received" for r in results]
