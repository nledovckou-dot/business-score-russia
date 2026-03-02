"""Step 3: FNS lookup — real financial data, founders, affiliates."""

from __future__ import annotations

from typing import Optional
from app.pipeline.fns import search_company, get_egrul, get_financials, get_affiliates


def run(company_info: dict, confirmed_inn: Optional[str] = None) -> dict:
    """Fetch real data from FNS.

    If confirmed_inn is provided (from user verification), use it directly.
    Otherwise search by name/inn from step 2.

    Returns dict with: fns_company, egrul, financials, affiliates, fns_candidates.
    """
    inn = confirmed_inn or company_info.get("inn")
    search_query = company_info.get("search_query", company_info.get("name", ""))

    result = {
        "fns_candidates": [],
        "fns_company": {},
        "egrul": {},
        "financials": [],
        "affiliates": [],
    }

    # If we have INN, go directly
    if inn:
        try:
            candidates = search_company(inn, limit=3)
            result["fns_candidates"] = candidates
            if candidates:
                result["fns_company"] = candidates[0]
        except Exception as e:
            result["fns_error"] = str(e)

    # Also search by name
    if not result["fns_company"] and search_query:
        try:
            candidates = search_company(search_query, limit=5)
            result["fns_candidates"] = candidates
            if candidates:
                result["fns_company"] = candidates[0]
        except Exception as e:
            result["fns_error"] = str(e)

    # If we found a company, get detailed data
    fns_inn = result["fns_company"].get("inn", "")
    if fns_inn:
        # EGRUL — founders, director, capital
        try:
            result["egrul"] = get_egrul(fns_inn)
        except Exception as e:
            result["egrul_error"] = str(e)

        # Financial statements
        try:
            result["financials"] = get_financials(fns_inn)
        except Exception as e:
            result["financials_error"] = str(e)

        # Affiliated companies through founders
        founders = result.get("egrul", {}).get("founders", [])
        if founders:
            try:
                result["affiliates"] = get_affiliates(fns_inn, founders)
            except Exception as e:
                result["affiliates_error"] = str(e)

    return result
