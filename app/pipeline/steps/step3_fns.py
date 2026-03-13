"""Step 3: FNS lookup — real financial data, founders, affiliates."""

from __future__ import annotations

import logging
import re
from typing import Optional
from app.pipeline.fns import search_company, get_egrul, get_financials, get_affiliates

logger = logging.getLogger(__name__)


def _normalize_name(name: str) -> set[str]:
    """Normalize company name to a set of lowercase tokens for comparison."""
    name = name.lower()
    # Remove org forms
    name = re.sub(r"\b(ооо|зао|ао|пао|ип|ooo|oao)\b", "", name)
    # Remove quotes, punctuation
    name = re.sub(r"[«»\"'.,\-\(\)]+", " ", name)
    tokens = {t for t in name.split() if len(t) > 1}
    return tokens


def _detect_entity_mismatch(company_info: dict, fns_company: dict) -> dict | None:
    """Compare brand name (from site) with FNS legal entity name.

    Returns mismatch info if names diverge significantly, or None.
    """
    brand_name = company_info.get("name", "").strip()
    legal_name = fns_company.get("name", "") or fns_company.get("full_name", "")
    legal_name = legal_name.strip()

    if not brand_name or not legal_name:
        return None

    brand_tokens = _normalize_name(brand_name)
    legal_tokens = _normalize_name(legal_name)

    if not brand_tokens or not legal_tokens:
        return None

    # Jaccard similarity
    intersection = brand_tokens & legal_tokens
    union = brand_tokens | legal_tokens
    similarity = len(intersection) / len(union) if union else 0.0

    # Also check if domain name appears in legal name
    domain = company_info.get("domain", "").replace(".ru", "").replace(".рф", "").replace("www.", "")
    domain_in_legal = domain.lower() in legal_name.lower() if domain else False

    if similarity >= 0.3 or domain_in_legal:
        return None  # Names match well enough

    return {
        "has_mismatch": True,
        "brand_name": brand_name,
        "legal_name": legal_name,
        "similarity": round(similarity, 2),
        "whois_org": company_info.get("whois_org", ""),
        "note": "Торговое название сайта не совпадает с юрлицом в ФНС. Возможно холдинг/дочерняя компания. Финансы ФНС могут включать другие продукты/сервисы.",
    }


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

    # Also search by name / search_query
    if not result["fns_company"] and search_query:
        try:
            candidates = search_company(search_query, limit=5)
            result["fns_candidates"] = candidates
            if candidates:
                result["fns_company"] = candidates[0]
        except Exception as e:
            result["fns_error"] = str(e)

    # Try legal_name (e.g. "ООО «ИМПЕРИЯ КОСМЕТИКИ»" → "ИМПЕРИЯ КОСМЕТИКИ")
    if not result["fns_company"]:
        import re
        legal_name = company_info.get("legal_name", "")
        if legal_name:
            # Strip org form prefix and quotes for cleaner FNS search
            clean = re.sub(r"^(ООО|ЗАО|АО|ПАО|ИП)\s*", "", legal_name)
            clean = re.sub(r"[«»\"']+", "", clean).strip()
            if clean and clean != search_query:
                try:
                    candidates = search_company(clean, limit=5)
                    if candidates:
                        result["fns_candidates"] = candidates
                        result["fns_company"] = candidates[0]
                except Exception as e:
                    result["fns_error"] = str(e)

    # Entity mismatch detection
    if result["fns_company"]:
        mismatch = _detect_entity_mismatch(company_info, result["fns_company"])
        if mismatch:
            result["entity_mismatch"] = mismatch
            logger.warning(
                "Entity mismatch: brand='%s' vs legal='%s' (similarity=%.2f)",
                mismatch["brand_name"], mismatch["legal_name"], mismatch["similarity"],
            )

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
