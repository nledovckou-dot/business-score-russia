#!/usr/bin/env python3
"""Sprint 1 validation: generate 5 NEW demo reports + Board evaluation.

Runs end-to-end: API start → poll → fetch report → GPT-4o evaluation.
Sequential to stay under TPM limits.
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from html.parser import HTMLParser

API_BASE = "http://89.167.19.68:8090"
API_KEY = os.environ.get("OPENAI_API_KEY") or os.environ.get("FALLBACK_LLM_API_KEY", "")

# 5 companies — mix of proven accessible + new ones
COMPANIES = [
    {"url": "https://selectel.ru", "name": "Selectel (B2B_SERVICE — облачный хостинг)"},
    {"url": "https://skillbox.ru", "name": "Skillbox (B2B_SERVICE — EdTech)"},
    {"url": "https://aviasales.ru", "name": "Aviasales (PLATFORM — поиск билетов)"},
    {"url": "https://dodo.dev", "name": "Dodo Engineering (B2C_SERVICE — пиццерия)"},
    {"url": "https://ozon.ru", "name": "Ozon (PLATFORM — маркетплейс)"},
]

DELAY_BETWEEN_EVALS = 30  # seconds between GPT evaluations


class HTMLTextExtractor(HTMLParser):
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


def api_request(method, path, data=None, timeout=30):
    url = API_BASE + path
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("Content-Type", "application/json")
    else:
        req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8") if e.fp else ""
        if e.code != 429:  # suppress noisy rate-limit logs
            print(f"  HTTP {e.code}: {body[:200]}")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


def poll_session(sid, timeout_sec=600):
    start = time.time()
    last_status = ""
    while time.time() - start < timeout_sec:
        result = api_request("GET", f"/api/session/{sid}")
        if result is None:
            time.sleep(3)
            continue

        status = result.get("status", "")
        if status != last_status:
            print(f"  [{int(time.time()-start):3d}s] Status: {status}")
            last_status = status

        if status == "waiting_company":
            company_data = result.get("data", {}).get("company_info", {})
            print(f"  Компания: {company_data.get('name', '?')} | ИНН: {company_data.get('inn', '?')}")
            api_request("POST", f"/api/confirm-company/{sid}", company_data)
            time.sleep(5)
            continue

        if status == "waiting_competitors":
            competitors = result.get("data", {}).get("competitors", [])
            print(f"  Конкуренты: {len(competitors)} шт")
            api_request("POST", f"/api/confirm-competitors/{sid}", {"competitors": competitors})
            time.sleep(5)
            continue

        if status == "done":
            report_data = result.get("data", {})
            report_url = report_data.get("report", {}).get("url", "")
            company = report_data.get("report", {}).get("company", "")
            size_kb = report_data.get("report", {}).get("size_kb", 0)
            elapsed = int(time.time() - start)
            print(f"  ✅ Готово за {elapsed}s: {company} ({size_kb} KB)")
            return {
                "url": API_BASE + report_url if report_url else None,
                "company": company,
                "size_kb": size_kb,
                "elapsed_sec": elapsed,
            }

        if status == "error":
            error_msg = result.get("data", {}).get("error", "Unknown error")
            print(f"  ❌ Ошибка: {error_msg}")
            return {"error": error_msg}

        time.sleep(10)

    return {"error": "timeout"}


def fetch_report_text(url):
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8")
    extractor = HTMLTextExtractor()
    extractor.feed(html)
    return extractor.get_text()


def call_openai(prompt, system, max_tokens=4000):
    payload = {
        "model": "gpt-4o",
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
                wait = (attempt + 1) * 20
                print(f"    Retry in {wait}s...")
                time.sleep(wait)
                continue
            raise
    return ""


EVAL_SYSTEM = """Ты — совет директоров AI-платформы бизнес-анализа «BSR 360».
Тебе представлен текст автоматически сгенерированного бизнес-аналитического отчёта.

Оцени отчёт от имени 5 экспертов. Каждый ставит оценку от 1 до 10.

ЭКСПЕРТЫ:
1. CFO (Финансовый директор) — точность финансовых данных, корректность расчётов, адекватность KPI
2. CMO (Директор по маркетингу) — конкурентный анализ, SWOT, digital-аудит, рекомендации
3. Отраслевой эксперт — правильность типа бизнеса, релевантность метрик, рыночные тренды
4. Скептик — галлюцинации, логические противоречия, необоснованные выводы, источники
5. CEO (Генеральный директор) — общее качество, полезность для бизнеса, actionable выводы

КРИТЕРИИ ОЦЕНКИ:
- 1-3: Серьёзные ошибки, галлюцинации, бесполезно
- 4-5: Много проблем, но есть полезные элементы
- 6-7: Хорошо, есть замечания, но в целом полезно
- 8-9: Отлично, незначительные замечания
- 10: Безупречно

ФОРМАТ — строго JSON:
{
  "company": "название",
  "experts": [
    {"role": "CFO", "score": 7, "strengths": ["..."], "weaknesses": ["..."], "comment": "краткий вердикт"}
  ],
  "average_score": 7.2,
  "overall_verdict": "общий вердикт (2-3 предложения)",
  "top_issues": ["проблема 1", "проблема 2", "проблема 3"],
  "improvements_vs_before": ["что стало лучше по сравнению с типичным MVP"]
}

ВАЖНО:
- Будь ЧЕСТЕН и ПРИДИРЧИВ
- Проверяй: есть ли источники у цифр? Есть ли пустые секции? Есть ли маркировка достоверности?
- ОТДЕЛЬНО отметь: есть ли секции с заглушками/пустотой
- Ответ ТОЛЬКО JSON"""


def evaluate_report(report_text, company_name):
    max_chars = 50000
    if len(report_text) > max_chars:
        report_text = report_text[:max_chars] + "\n\n... [отчёт обрезан]"

    prompt = (
        f"Компания: {company_name}\n\n"
        f"=== ТЕКСТ ОТЧЁТА ===\n{report_text}\n=== КОНЕЦ ===\n\n"
        "Оцени по 10-балльной шкале от имени 5 экспертов."
    )

    raw = call_openai(prompt, EVAL_SYSTEM)
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        return {"error": str(e), "raw": text[:500]}


def main():
    if not API_KEY:
        print("ERROR: Set OPENAI_API_KEY or FALLBACK_LLM_API_KEY")
        sys.exit(1)

    print("=" * 70)
    print("Sprint 1 Validation — 5 новых отчётов + оценка")
    print("=" * 70)

    results = []

    for i, company in enumerate(COMPANIES, 1):
        print(f"\n{'─' * 70}")
        print(f"[{i}/5] {company['name']}")
        print(f"  URL: {company['url']}")

        # 1. Generate report
        resp = api_request("POST", "/api/start", {"url": company["url"]})
        if not resp or not resp.get("ok"):
            print(f"  ❌ Не удалось начать: {resp}")
            results.append({"company": company["name"], "error": "start_failed"})
            continue

        sid = resp["session_id"]
        print(f"  Session: {sid}")

        gen_result = poll_session(sid, timeout_sec=600)
        if gen_result.get("error"):
            results.append({"company": company["name"], "error": gen_result["error"]})
            continue

        report_url = gen_result["url"]

        # 2. Fetch report text
        print(f"  Загрузка отчёта для оценки...")
        try:
            text = fetch_report_text(report_url)
            print(f"  Текст: {len(text)} символов")
        except Exception as e:
            print(f"  ❌ Ошибка загрузки: {e}")
            results.append({"company": company["name"], "report_url": report_url, "error": str(e)})
            continue

        # 3. Evaluate
        print(f"  Оценка совета директоров...")
        t0 = time.time()
        try:
            evaluation = evaluate_report(text, company["name"])
            elapsed = int(time.time() - t0)
            print(f"  ✅ Оценка за {elapsed}s")
        except Exception as e:
            print(f"  ❌ Ошибка оценки: {e}")
            results.append({"company": company["name"], "report_url": report_url, "error": str(e)})
            continue

        evaluation["report_url"] = report_url
        evaluation["generation"] = gen_result
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
            print(f"\n  ⭐ Средняя: {avg}/10")
            verdict = evaluation.get("overall_verdict", "")
            print(f"  💬 {verdict}")

        # Delay before next
        if i < len(COMPANIES):
            print(f"\n  ⏳ Пауза {DELAY_BETWEEN_EVALS}s (TPM)...")
            time.sleep(DELAY_BETWEEN_EVALS)

    # ── Summary ──
    print(f"\n{'=' * 70}")
    print("ИТОГОВАЯ ТАБЛИЦА SPRINT 1")
    print(f"{'=' * 70}")
    print(f"{'Компания':<25} {'CFO':>4} {'CMO':>4} {'Отр':>4} {'Скеп':>4} {'CEO':>4} {'Сред':>5} {'Время':>6}")
    print(f"{'─' * 25} {'─'*4} {'─'*4} {'─'*4} {'─'*4} {'─'*4} {'─'*5} {'─'*6}")

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
        gen = r.get("generation", {})
        elapsed = gen.get("elapsed_sec", "?")
        print(f"  {name:<23} {cfo:>4} {cmo:>4} {ind:>4} {skp:>4} {ceo:>4} {avg:>5} {elapsed:>5}s")

    # Issues
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

    # Improvements
    print(f"\n{'─' * 70}")
    print("ЧТО УЛУЧШИЛОСЬ:")
    for r in results:
        if "error" in r:
            continue
        improvements = r.get("improvements_vs_before", [])
        if improvements:
            name = r.get("company", "?")
            print(f"\n  {name}:")
            for imp in improvements[:3]:
                print(f"    ✓ {imp}")

    # Save
    with open("scripts/sprint1_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n\nРезультаты: scripts/sprint1_results.json")


if __name__ == "__main__":
    main()
