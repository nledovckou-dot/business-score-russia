#!/usr/bin/env python3
"""Board of Directors evaluation — оценка 5 демо-отчётов по 10-балльной шкале.

Последовательно (НЕ параллельно) вызывает GPT-4o для каждого отчёта,
чтобы не упираться в TPM лимит (30K tokens/min).
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from html.parser import HTMLParser

# ── Config ──
API_KEY = os.environ.get("OPENAI_API_KEY") or os.environ.get("FALLBACK_LLM_API_KEY", "")
MODEL = "gpt-4o"
VPS_BASE = "http://89.167.19.68:8090"

REPORTS = [
    {"url": f"{VPS_BASE}/reports/report_bdd19006.html", "name": "Авиасейлс", "type": "PLATFORM"},
    {"url": f"{VPS_BASE}/reports/report_7ad63700.html", "name": "Ozon", "type": "PLATFORM"},
    {"url": f"{VPS_BASE}/reports/report_ae51e27e.html", "name": "Skillbox", "type": "B2B_SERVICE"},
    {"url": f"{VPS_BASE}/reports/report_06e90799.html", "name": "Selectel", "type": "B2B_SERVICE"},
    {"url": f"{VPS_BASE}/reports/report_3132c7a3.html", "name": "Dodo Engineering", "type": "B2C_SERVICE"},
]

DELAY_BETWEEN_REPORTS = 35  # seconds — stay under 30K TPM


class HTMLTextExtractor(HTMLParser):
    """Extract visible text from HTML, skip scripts/styles."""

    def __init__(self):
        super().__init__()
        self.result = []
        self._skip = False
        self._skip_tags = {"script", "style", "svg", "noscript"}

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self._skip_tags:
            self._skip = True

    def handle_endtag(self, tag):
        if tag.lower() in self._skip_tags:
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            text = data.strip()
            if text:
                self.result.append(text)

    def get_text(self):
        return "\n".join(self.result)


def fetch_report(url: str) -> str:
    """Download report HTML and extract text."""
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8")
    extractor = HTMLTextExtractor()
    extractor.feed(html)
    return extractor.get_text()


def call_openai(prompt: str, system: str, max_tokens: int = 4000) -> str:
    """Call OpenAI API."""
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",
        },
        method="POST",
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            usage = body.get("usage", {})
            print(f"    Tokens: in={usage.get('prompt_tokens', 0)}, out={usage.get('completion_tokens', 0)}")
            return body["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8") if e.fp else ""
            print(f"    API error {e.code}: {err[:200]}")
            if e.code in (429, 500, 502, 503) and attempt < 2:
                wait = (attempt + 1) * 15
                print(f"    Retry in {wait}s...")
                time.sleep(wait)
                continue
            raise
    return ""


SYSTEM_PROMPT = """Ты — совет директоров AI-платформы бизнес-анализа «BSR 360».
Тебе представлен текст автоматически сгенерированного бизнес-аналитического отчёта.

Оцени отчёт от имени 5 экспертов. Каждый ставит оценку от 1 до 10.

ЭКСПЕРТЫ:
1. CFO (Финансовый директор) — точность финансовых данных, корректность расчётов, адекватность KPI
2. CMO (Директор по маркетингу) — конкурентный анализ, SWOT, digital-аудит, рекомендации
3. Отраслевой эксперт — правильность типа бизнеса, релевантность метрик, рыночные тренды
4. Скептик — галлюцинации, логические противоречия, необоснованные выводы, источники
5. CEO (Генеральный директор) — общее качество, полезность для бизнеса, actionable выводы

КРИТЕРИИ ОЦЕНКИ (для каждого эксперта):
- 1-3: Серьёзные ошибки, галлюцинации, бесполезно
- 4-5: Много проблем, но есть полезные элементы
- 6-7: Хорошо, есть замечания, но в целом полезно
- 8-9: Отлично, незначительные замечания
- 10: Безупречно

ФОРМАТ ОТВЕТА — строго JSON:
{
  "company": "название компании",
  "experts": [
    {
      "role": "CFO",
      "score": 7,
      "strengths": ["что хорошо (1-2 пункта)"],
      "weaknesses": ["что плохо (1-2 пункта)"],
      "comment": "краткий вердикт (1 предложение)"
    },
    ... (для всех 5 экспертов)
  ],
  "average_score": 7.2,
  "overall_verdict": "общий вердикт совета директоров (2-3 предложения)",
  "top_issues": ["главная проблема 1", "главная проблема 2", "главная проблема 3"]
}

ВАЖНО:
- Будь ЧЕСТЕН и ПРИДИРЧИВ. Это MVP, галлюцинации ожидаемы — не завышай оценки.
- Проверяй: есть ли выдуманные цифры? Есть ли источники? Логичен ли анализ?
- Сравнивай с тем, что ожидаешь от профессионального бизнес-отчёта.
- Отвечай ТОЛЬКО валидным JSON."""


def evaluate_report(report_text: str, company_name: str, business_type: str) -> dict:
    """Evaluate a single report by the board."""
    # Truncate to fit in context window (max ~15K tokens input)
    max_chars = 50000
    if len(report_text) > max_chars:
        report_text = report_text[:max_chars] + "\n\n... [отчёт обрезан для оценки]"

    prompt = (
        f"Компания: {company_name}\n"
        f"Тип бизнеса: {business_type}\n\n"
        f"=== ТЕКСТ ОТЧЁТА ===\n{report_text}\n=== КОНЕЦ ОТЧЁТА ===\n\n"
        "Оцени этот отчёт по 10-балльной шкале от имени каждого из 5 экспертов."
    )

    raw = call_openai(prompt, SYSTEM_PROMPT)
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"    JSON parse error: {e}")
        return {"error": str(e), "raw": text[:500]}


def main():
    if not API_KEY:
        print("ERROR: Set OPENAI_API_KEY or FALLBACK_LLM_API_KEY")
        sys.exit(1)

    print("=" * 70)
    print("BSR Совет Директоров — оценка 5 демо-отчётов")
    print("=" * 70)

    results = []

    for i, report in enumerate(REPORTS, 1):
        print(f"\n{'─' * 70}")
        print(f"[{i}/5] {report['name']} ({report['type']})")
        print(f"  URL: {report['url']}")

        # 1. Fetch report
        print(f"  Загрузка отчёта...")
        try:
            text = fetch_report(report["url"])
            print(f"  Текст: {len(text)} символов")
        except Exception as e:
            print(f"  ❌ Ошибка загрузки: {e}")
            results.append({"company": report["name"], "error": str(e)})
            continue

        # 2. Evaluate
        print(f"  Оценка совета директоров...")
        t0 = time.time()
        try:
            evaluation = evaluate_report(text, report["name"], report["type"])
            elapsed = int(time.time() - t0)
            print(f"  ✅ Оценка получена за {elapsed}s")
        except Exception as e:
            print(f"  ❌ Ошибка оценки: {e}")
            results.append({"company": report["name"], "error": str(e)})
            continue

        evaluation["report_url"] = report["url"]
        results.append(evaluation)

        # Print scores
        if "experts" in evaluation:
            print(f"\n  📊 Оценки:")
            for expert in evaluation["experts"]:
                score = expert.get("score", "?")
                role = expert.get("role", "?")
                comment = expert.get("comment", "")
                print(f"    {role}: {score}/10 — {comment}")
            avg = evaluation.get("average_score", "?")
            print(f"\n  ⭐ Средняя оценка: {avg}/10")
            verdict = evaluation.get("overall_verdict", "")
            print(f"  💬 Вердикт: {verdict}")

        # Delay before next report (avoid TPM limit)
        if i < len(REPORTS):
            print(f"\n  ⏳ Пауза {DELAY_BETWEEN_REPORTS}s (TPM лимит)...")
            time.sleep(DELAY_BETWEEN_REPORTS)

    # ── Summary ──
    print(f"\n{'=' * 70}")
    print("ИТОГОВАЯ ТАБЛИЦА")
    print(f"{'=' * 70}")
    print(f"{'Компания':<25} {'CFO':>4} {'CMO':>4} {'Отр':>4} {'Скеп':>4} {'CEO':>4} {'Сред':>5}")
    print(f"{'─' * 25} {'─'*4} {'─'*4} {'─'*4} {'─'*4} {'─'*4} {'─'*5}")

    for r in results:
        if "error" in r:
            print(f"  {r.get('company', '?'):<23} ❌ {r['error'][:40]}")
            continue
        name = r.get("company", "?")[:24]
        experts = r.get("experts", [])
        scores = {e.get("role", ""): e.get("score", 0) for e in experts}
        cfo = scores.get("CFO", "?")
        cmo = scores.get("CMO", "?")
        ind = scores.get("Отраслевой эксперт", scores.get("Industry Expert", "?"))
        skp = scores.get("Скептик", scores.get("Skeptic", "?"))
        ceo = scores.get("CEO", "?")
        avg = r.get("average_score", "?")
        print(f"  {name:<23} {cfo:>4} {cmo:>4} {ind:>4} {skp:>4} {ceo:>4} {avg:>5}")

    # Top issues across all reports
    print(f"\n{'─' * 70}")
    print("КЛЮЧЕВЫЕ ПРОБЛЕМЫ:")
    for r in results:
        if "error" in r:
            continue
        name = r.get("company", "?")
        issues = r.get("top_issues", [])
        if issues:
            print(f"\n  {name}:")
            for issue in issues[:3]:
                print(f"    • {issue}")

    # Save
    output_file = "scripts/board_evaluations.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n\nРезультаты сохранены в {output_file}")


if __name__ == "__main__":
    main()
