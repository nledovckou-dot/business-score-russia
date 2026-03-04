#!/usr/bin/env python3
"""Generate 5 demo reports via BSR API and collect Board of Directors scores."""

import json
import sys
import time
import urllib.request
import urllib.parse
import urllib.error

API_BASE = "http://89.167.19.68:8090"
PUBLIC_BASE = "http://89.167.19.68:8090"

# 5 real small/medium businesses — tested accessible from Helsinki VPS
COMPANIES = [
    {
        "url": "https://aviasales.ru",
        "name": "Aviasales (PLATFORM — поиск авиабилетов)",
    },
    {
        "url": "https://ozon.ru",
        "name": "Ozon (PLATFORM — маркетплейс)",
    },
    {
        "url": "https://skillbox.ru",
        "name": "Skillbox (B2B_SERVICE — EdTech, онлайн-образование)",
    },
    {
        "url": "https://selectel.ru",
        "name": "Selectel (B2B_SERVICE — облачный хостинг)",
    },
    {
        "url": "https://dodo.dev",
        "name": "Dodo Engineering / Додо Пицца (B2C_SERVICE — пиццерия)",
    },
]


def api_request(method, path, data=None, timeout=30):
    """Make API request, return parsed JSON."""
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
        print(f"  HTTP {e.code}: {body[:200]}")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


def poll_session(sid, timeout_sec=600):
    """Poll session status until terminal state."""
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
            # Auto-confirm company
            company_data = result.get("data", {}).get("company_info", {})
            print(f"  Компания: {company_data.get('name', '?')} | ИНН: {company_data.get('inn', '?')}")
            confirm = api_request("POST", f"/api/confirm-company/{sid}", company_data)
            if confirm:
                print(f"  ✓ Компания подтверждена")
            time.sleep(2)
            continue

        if status == "waiting_competitors":
            # Auto-confirm competitors
            competitors = result.get("data", {}).get("competitors", [])
            print(f"  Конкуренты: {len(competitors)} шт")
            for c in competitors[:5]:
                v = "✓" if c.get("verified") else "?"
                print(f"    {v} {c.get('name', '?')}")
            confirm = api_request("POST", f"/api/confirm-competitors/{sid}", {
                "competitors": competitors,
            })
            if confirm:
                print(f"  ✓ Конкуренты подтверждены")
            time.sleep(2)
            continue

        if status == "done":
            report_data = result.get("data", {})
            report_url = report_data.get("report", {}).get("url", "")
            company = report_data.get("report", {}).get("company", "")
            size_kb = report_data.get("report", {}).get("size_kb", 0)
            elapsed = int(time.time() - start)
            print(f"  ✅ Готово за {elapsed}s: {company} ({size_kb} KB)")
            return {
                "url": PUBLIC_BASE + report_url if report_url else None,
                "company": company,
                "size_kb": size_kb,
                "elapsed_sec": elapsed,
                "session_id": sid,
            }

        if status == "error":
            error_msg = result.get("data", {}).get("error", "Unknown error")
            print(f"  ❌ Ошибка: {error_msg}")
            return {"error": error_msg, "session_id": sid}

        time.sleep(3)

    print(f"  ⏰ Таймаут ({timeout_sec}s)")
    return {"error": "timeout", "session_id": sid}


def main():
    print("=" * 60)
    print("BSR Demo Report Generator — 5 отчётов")
    print("=" * 60)

    results = []

    for i, company in enumerate(COMPANIES, 1):
        print(f"\n{'─' * 60}")
        print(f"[{i}/5] {company['name']}")
        print(f"  URL: {company['url']}")

        # Start analysis
        resp = api_request("POST", "/api/start", {"url": company["url"]})
        if not resp or not resp.get("ok"):
            print(f"  ❌ Не удалось начать: {resp}")
            results.append({"company": company["name"], "error": "start_failed"})
            continue

        sid = resp["session_id"]
        print(f"  Session: {sid}")

        # Poll until done
        result = poll_session(sid, timeout_sec=600)
        result["input_name"] = company["name"]
        result["input_url"] = company["url"]
        results.append(result)

        # Small delay between reports
        if i < len(COMPANIES):
            print(f"  Пауза 5 сек перед следующим отчётом...")
            time.sleep(5)

    # Summary
    print(f"\n{'=' * 60}")
    print("ИТОГИ")
    print(f"{'=' * 60}")

    for i, r in enumerate(results, 1):
        name = r.get("input_name", r.get("company", "?"))
        if r.get("error"):
            print(f"  {i}. ❌ {name}: {r['error']}")
        else:
            url = r.get("url", "?")
            elapsed = r.get("elapsed_sec", 0)
            size = r.get("size_kb", 0)
            print(f"  {i}. ✅ {name}")
            print(f"     URL: {url}")
            print(f"     Время: {elapsed}s | Размер: {size} KB")

    # Save results
    with open("scripts/demo_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nРезультаты сохранены в scripts/demo_results.json")


if __name__ == "__main__":
    main()
