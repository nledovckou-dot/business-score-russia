"""Multi-provider LLM client with auto-selection of best available models.

Before each report, model_selector probes OpenAI/Anthropic/Gemini APIs
and picks the best available model. Fallback chain: GPT → Opus → Gemini.
"""

from __future__ import annotations

import json
import logging
import os
import threading
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
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

# Defaults — will be overridden by model_selector.get_models() at runtime
MODEL_MAIN = os.environ.get("LLM_MODEL_MAIN", "gpt-5.4")            # основной мозг
MODEL_FAST = os.environ.get("LLM_MODEL_FAST", "gemini-3-flash")     # быстрые задачи
MODEL_REASON = os.environ.get("LLM_MODEL_REASON", "gpt-5.4-thinking")  # reasoning
MODEL_OPUS = os.environ.get("LLM_MODEL_OPUS", "claude-opus-4-6")    # Claude Opus — board + fallback


def refresh_models() -> dict:
    """Probe all providers and update MODEL_* globals with best available models.
    Call this before each report generation. Returns probe results dict.
    """
    global MODEL_MAIN, MODEL_FAST, MODEL_OPUS, MODEL_REASON
    from app.pipeline.model_selector import get_models
    selected = get_models(force_refresh=True)
    MODEL_MAIN = selected.main
    MODEL_FAST = selected.fast
    MODEL_OPUS = selected.board
    logger.info("Models refreshed — MAIN: %s, BOARD: %s, FAST: %s", MODEL_MAIN, MODEL_OPUS, MODEL_FAST)
    return selected.probe_results


# ── Metrics hook (T7) ──
# Thread-local storage for the active MetricsCollector.
# Pipeline code sets this via set_metrics_collector() before running steps.
_metrics_local = threading.local()


def set_metrics_collector(collector) -> None:
    """Bind a MetricsCollector to the current thread. LLM calls will auto-record usage."""
    _metrics_local.collector = collector


def _get_metrics_collector():
    """Get the MetricsCollector for the current thread, or None."""
    return getattr(_metrics_local, "collector", None)


def _record_usage(model: str, tokens_in: int, tokens_out: int) -> None:
    """Record LLM usage if a MetricsCollector is active on this thread."""
    mc = _get_metrics_collector()
    if mc is not None:
        mc.record_llm_call(model, tokens_in=tokens_in, tokens_out=tokens_out)


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


def _anthropic_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
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
            # Record token usage for metrics (T7)
            usage = body.get("usage", {})
            _record_usage(
                model,
                tokens_in=usage.get("prompt_tokens", 0),
                tokens_out=usage.get("completion_tokens", 0),
            )
            return body["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            # Quota exhausted → fallback: Claude Opus → Gemini
            if "insufficient_quota" in error_body:
                logger.warning("OpenAI quota exhausted, trying Claude Opus fallback")
                try:
                    return call_anthropic(
                        prompt, model=MODEL_OPUS, system=system,
                        temperature=temperature, max_tokens=max_tokens,
                        json_mode=json_mode,
                    )
                except Exception as opus_err:
                    logger.warning("Claude Opus fallback failed: %s, trying Gemini", str(opus_err)[:200])
                    full_prompt = prompt
                    if system:
                        full_prompt = f"[System]: {system}\n\n{prompt}"
                    return call_gemini(
                        full_prompt, model=MODEL_FAST,
                        temperature=temperature, max_tokens=max_tokens,
                        json_mode=json_mode,
                    )
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
            # Record token usage for metrics (T7)
            usage_meta = body.get("usageMetadata", {})
            _record_usage(
                model,
                tokens_in=usage_meta.get("promptTokenCount", 0),
                tokens_out=usage_meta.get("candidatesTokenCount", 0),
            )
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


def call_anthropic(
    prompt: str,
    model: str = MODEL_OPUS,
    system: str = "",
    temperature: float = 0.4,
    max_tokens: int = 16000,
    json_mode: bool = False,
) -> str:
    """Call Anthropic Claude API. Returns text response."""
    messages = [{"role": "user", "content": prompt}]

    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if system:
        payload["system"] = system

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        ANTHROPIC_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "x-api-key": _anthropic_key(),
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            # Record token usage
            usage = body.get("usage", {})
            _record_usage(
                model,
                tokens_in=usage.get("input_tokens", 0),
                tokens_out=usage.get("output_tokens", 0),
            )
            # Extract text from content blocks
            content = body.get("content", [])
            text_parts = [b["text"] for b in content if b.get("type") == "text"]
            return "\n".join(text_parts)
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            if e.code in (429, 500, 502, 503, 529) and attempt < 2:
                wait = (attempt + 1) * 5
                time.sleep(wait)
                continue
            raise RuntimeError(f"Anthropic API error {e.code}: {error_body[:500]}")
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
    if provider == "anthropic":
        # Claude: просим JSON через system prompt
        json_system = (system + "\n\n" if system else "") + "Ответ — ТОЛЬКО валидный JSON (без markdown, без ```)."
        text = call_anthropic(
            prompt, model=model or MODEL_OPUS, system=json_system,
            temperature=temperature, max_tokens=max_tokens, json_mode=True,
        )
    elif provider == "openai":
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
    """Вызов LLM для совета директоров — Claude Opus (основной) + GPT (fallback).

    Используется для AI-экспертов, которые критикуют и рецензируют отчёт.
    Claude Opus выбран для board т.к. нет TPM лимитов OpenAI на длинные промпты.
    Низкая temperature (0.3) для точных, выверенных ответов.

    Chain: Claude Opus → GPT → Gemini.
    """
    from app.config import BOARD_LLM_TEMPERATURE, BOARD_LLM_MAX_TOKENS

    temperature = BOARD_LLM_TEMPERATURE
    max_tokens = BOARD_LLM_MAX_TOKENS

    # --- Попытка 1: Claude Opus (основной для board) ---
    t0 = time.monotonic()
    try:
        result = call_anthropic(
            prompt=prompt,
            model=MODEL_OPUS,
            system=system or "",
            temperature=temperature,
            max_tokens=max_tokens,
        )
        elapsed = round(time.monotonic() - t0, 2)
        logger.info("Board LLM OK: model=%s, time=%.2fs", MODEL_OPUS, elapsed)
        return result
    except Exception as exc:
        elapsed = round(time.monotonic() - t0, 2)
        logger.warning(
            "Board LLM Claude Opus failed in %.2fs: %s", elapsed, str(exc)[:200],
        )

    # --- Попытка 2: GPT fallback ---
    t0 = time.monotonic()
    try:
        result = call_openai(
            prompt=prompt,
            model=MODEL_MAIN,
            system=system or "",
            temperature=temperature,
            max_tokens=max_tokens,
        )
        elapsed = round(time.monotonic() - t0, 2)
        logger.info("Board LLM fallback GPT OK: model=%s, time=%.2fs", MODEL_MAIN, elapsed)
        return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        elapsed = round(time.monotonic() - t0, 2)
        logger.warning(
            "Board LLM GPT fallback failed: status=%d, time=%.2fs, error=%s",
            e.code, elapsed, error_body[:200],
        )
    except Exception as exc:
        elapsed = round(time.monotonic() - t0, 2)
        logger.warning("Board LLM GPT fallback exception: %.2fs: %s", elapsed, str(exc)[:200])

    # --- Попытка 3: Gemini (последний fallback) ---
    logger.info("Board LLM final fallback → Gemini")
    full_prompt = prompt
    if system:
        full_prompt = f"[System]: {system}\n\n{prompt}"
    return call_gemini(full_prompt, temperature=temperature, max_tokens=max_tokens)


def call_board_llm_parallel(prompts: list[dict]) -> list[str]:
    """Параллельные вызовы для экспертов совета директоров.

    С Claude Opus не нужны паузы между вызовами (нет TPM лимита OpenAI).
    Используем ThreadPoolExecutor для параллельного выполнения.

    Args:
        prompts: список словарей [{"prompt": "...", "system": "..."}, ...]

    Returns:
        list[str]: ответы в том же порядке, что и prompts.
    """
    results: list[str | None] = [None] * len(prompts)

    def _call_one(idx: int, item: dict) -> tuple[int, str]:
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
            logger.error("Board parallel #%d failed in %.2fs: %s", idx, elapsed, str(exc)[:300])
            return idx, f"[Board LLM Error] Expert #{idx}: {str(exc)[:500]}"

    # Параллельно — Claude Opus не имеет TPM лимитов OpenAI
    with ThreadPoolExecutor(max_workers=min(len(prompts), 4)) as pool:
        futures = [pool.submit(_call_one, i, item) for i, item in enumerate(prompts)]
        for future in as_completed(futures):
            idx, response = future.result()
            results[idx] = response

    return [r or "[Board LLM Error] No response" for r in results]
