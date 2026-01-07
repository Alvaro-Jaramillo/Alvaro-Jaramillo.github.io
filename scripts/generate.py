#!/usr/bin/env python3
"""Generate the data file for the GitHub Pages dashboard.

This repository is deployed via GitHub Actions to GitHub Pages.

V2 approach:
- Query the web via GDELT 2.1 Doc API using a set of high-signal keyword packs.
- Heuristically keep only USA/Canada-relevant hits.
- Enrich each hit with topic tags + simple numeric "signals" (investment, sqft, jobs).
- Write `data/items.json` consumed by `index.html`.

Notes:
- We intentionally avoid scraping Google results in the workflow (fragile and ToS-sensitive).
- If you later want Google Programmable Search API, it can be added as a secondary source.
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
from jinja2 import Template

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
TPL_DIR = os.path.join(ROOT, "templates")


GDELT_DOC_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"


US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
    "District of Columbia",
]

CA_PROVINCES = [
    "Alberta", "British Columbia", "Manitoba", "New Brunswick", "Newfoundland",
    "Newfoundland and Labrador", "Nova Scotia", "Northwest Territories", "Nunavut",
    "Ontario", "Prince Edward Island", "Quebec", "Saskatchewan", "Yukon",
]


def _dt_to_gdelt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


@dataclass(frozen=True)
class Topic:
    key: str
    label: str
    query: str


TOPICS: List[Topic] = [
    Topic(
        key="facility_new",
        label="New facility / warehouse / DC",
        query='("new warehouse" OR "new distribution center" OR "new distribution centre" OR "new DC" OR "new fulfillment center" OR "new fulfilment centre" OR "opens new" OR "opening a new")',
    ),
    Topic(
        key="facility_expansion",
        label="Expansion / modernization",
        query='("expansion" OR "expanded" OR "expanding" OR "modernization" OR "modernisation" OR "upgrade" OR "capacity increase" OR "adds capacity" OR "adding capacity")',
    ),
    Topic(
        key="manufacturing_investment",
        label="Manufacturing investment",
        query='("manufacturing investment" OR "plant investment" OR "production expansion" OR "new plant" OR "new factory" OR "manufacturing facility")',
    ),
    Topic(
        key="warehouse_investment",
        label="Warehouse / logistics investment",
        query='("warehouse investment" OR "distribution investment" OR "logistics investment" OR "capital investment" OR capex OR "investing" OR "invests")',
    ),
    Topic(
        key="real_estate_signal",
        label="Industrial real estate / build-to-suit",
        query='("build-to-suit" OR "industrial lease" OR "leased" OR "site selection" OR "selects site" OR "planning commission" OR rezoning OR permit OR permitting OR "zoning")',
    ),
    Topic(
        key="automation_signal",
        label="Automation project signal",
        query='("AS/RS" OR ASRS OR "automated storage" OR shuttle OR "goods-to-person" OR GTP OR AMR OR "autonomous mobile" OR robotics OR "robotic" OR "palletizing" OR palletizing OR sortation OR conveyor OR "WMS" OR "WES" OR "WCS" OR "warehouse automation" OR "distribution automation")',
    ),
    Topic(
        key="leadership_change",
        label="Leadership change (CEO/VP/Automation)",
        query='("appointed" OR "names" OR "named" OR "joins" OR "hired" OR "promoted" OR "resigns" OR "steps down") AND (CEO OR COO OR CFO OR "Chief Executive" OR "VP" OR "Vice President" OR "Head of" OR Director) AND ("supply chain" OR operations OR logistics OR automation OR engineering)',
    ),
    Topic(
        key="revenue_update",
        label="Revenue / earnings update",
        query='("revenue" OR "net sales" OR "earnings" OR "guidance" OR "quarter" OR "Q1" OR "Q2" OR "Q3" OR "Q4") AND ("supply chain" OR "distribution" OR "capacity" OR "capex" OR "investment")',
    ),
    Topic(
        key="risk_urgency",
        label="Risk / urgency (closure, labor, disruption)",
        query='("layoffs" OR "closure" OR "shutting down" OR "consolidation" OR strike OR union OR "labor shortage" OR "labour shortage" OR fire OR recall)',
    ),
]


def build_geo_query() -> str:
    # Prefer explicit geography mentions; this also helps reduce global noise.
    geo_terms = [
        '"United States"', '"U.S."', 'USA', '"United States of America"',
        'Canada', '"Canadian"',
    ] + [f'"{s}"' for s in US_STATES] + [f'"{p}"' for p in CA_PROVINCES]
    return "(" + " OR ".join(geo_terms) + ")"


GEO_QUERY = build_geo_query()
SOURCE_COUNTRY_QUERY = "(sourceCountry:US OR sourceCountry:CA)"


def gdelt_fetch(query: str, start: datetime, end: datetime, max_records: int = 250) -> List[dict]:
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "sort": "HybridRel",
        "maxrecords": str(max_records),
        "startdatetime": _dt_to_gdelt(start),
        "enddatetime": _dt_to_gdelt(end),
    }
    r = requests.get(GDELT_DOC_ENDPOINT, params=params, timeout=30)
    r.raise_for_status()
    data = r.json() if r.content else {}
    return (data.get("articles") or [])


_MONEY_RE = re.compile(r"\$\s?([0-9]+(?:\.[0-9]+)?)\s?(B|M|K|billion|million|thousand)?", re.IGNORECASE)
_SQFT_RE = re.compile(r"([0-9][0-9,]{2,})\s?(sq\.?\s?ft\.?|square\s+feet|sf)", re.IGNORECASE)
_JOBS_RE = re.compile(r"([0-9][0-9,]{1,})\s+(jobs|job)", re.IGNORECASE)


def _normalize_money(m: re.Match) -> Optional[float]:
    try:
        v = float(m.group(1))
    except Exception:
        return None
    mult = (m.group(2) or "").lower()
    if mult in {"b", "billion"}:
        v *= 1_000_000_000
    elif mult in {"m", "million"}:
        v *= 1_000_000
    elif mult in {"k", "thousand"}:
        v *= 1_000
    return v


def extract_signals(text: str) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {"investment_usd": None, "sqft": None, "jobs": None}
    if not text:
        return out
    mm = _MONEY_RE.search(text)
    if mm:
        out["investment_usd"] = _normalize_money(mm)
    sm = _SQFT_RE.search(text)
    if sm:
        out["sqft"] = float(sm.group(1).replace(",", ""))
    jm = _JOBS_RE.search(text)
    if jm:
        out["jobs"] = float(jm.group(1).replace(",", ""))
    return out


def detect_region_and_country(title: str, source_country: str | None) -> Tuple[Optional[str], Optional[str]]:
    t = (title or "")
    for p in CA_PROVINCES:
        if re.search(r"\b" + re.escape(p) + r"\b", t, re.IGNORECASE):
            return p, "CA"
    for s in US_STATES:
        if re.search(r"\b" + re.escape(s) + r"\b", t, re.IGNORECASE):
            return s, "US"
    # fallback by source country
    if (source_country or "").upper() in {"US", "CA"}:
        return None, (source_country or "").upper()
    # heuristic keywords
    if re.search(r"\bcanada\b|\bontario\b|\bquebec\b", t, re.IGNORECASE):
        return None, "CA"
    if re.search(r"\bunited states\b|\busa\b|\bu\.s\.\b", t, re.IGNORECASE):
        return None, "US"
    return None, None


def is_us_or_ca(item: dict) -> bool:
    title = item.get("title") or ""
    region, country = detect_region_and_country(title, item.get("sourceCountry"))
    return country in {"US", "CA"} or region is not None


def _parse_published(published: Optional[str]) -> Optional[datetime]:
    """Parse GDELT seendate strings into datetimes.

    Observed formats can vary; we handle the common ones:
    - '2026-01-07 13:02:00'
    - ISO 8601
    """
    if not published:
        return None
    s = published.strip()
    # GDELT commonly returns "YYYY-MM-DD HH:MM:SS".
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _load_existing_items() -> List[dict]:
    path = os.path.join(DATA_DIR, "items.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload.get("items") or []
    except Exception:
        return []


def build_items(now_utc: datetime) -> Tuple[List[dict], dict]:
    # Query window: look back far enough that the dashboard is usually populated
    # even during slow news periods.
    end = now_utc
    start = now_utc - timedelta(hours=48)

    existing_items = _load_existing_items()

    seen_urls: Set[str] = set()
    # Seed seen URLs/IDs from existing items so we don't keep re-adding the same stories.
    seen_ids: Set[str] = set()
    for it in existing_items:
        if isinstance(it, dict):
            if it.get("url"):
                seen_urls.add(it["url"])
            if it.get("id"):
                seen_ids.add(it["id"])

    items: List[dict] = []
    meta = {
        "generated_at": now_utc.isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "source": "GDELT 2.1 Doc API",
    }

    for topic in TOPICS:
        q = f"({topic.query}) AND {GEO_QUERY} AND {SOURCE_COUNTRY_QUERY}"
        try:
            arts = gdelt_fetch(q, start=start, end=end, max_records=250)
        except Exception as e:
            print(f"[warn] GDELT fetch failed for topic {topic.key}: {e}", file=sys.stderr)
            continue

        for a in arts:
            url = (a.get("url") or "").strip()
            title = (a.get("title") or "").strip()
            if not url or not title:
                continue
            if url in seen_urls:
                continue
            if not is_us_or_ca(a):
                continue

            seen_urls.add(url)
            domain = (a.get("domain") or "").strip()
            source_country = (a.get("sourceCountry") or "").strip().upper() or None
            published = (a.get("seendate") or "").strip() or None

            region, country = detect_region_and_country(title, source_country)
            signals = extract_signals(title)

            items.append(
                {
                    "id": _stable_id(url),
                    "title": title,
                    "url": url,
                    "source": domain or a.get("sourceCollection") or "(unknown)",
                    "source_country": source_country,
                    "published": published,
                    "topics": [topic.key],
                    "topic_labels": [topic.label],
                    "region": region,
                    "country": country,
                    "signals": signals,
                }
            )

    # Merge same URL across topics (rare but possible if query overlaps)
    merged: Dict[str, dict] = {}
    for it in items:
        mid = it["id"]
        if mid not in merged:
            merged[mid] = it
        else:
            merged[mid]["topics"] = sorted(set(merged[mid]["topics"]) | set(it["topics"]))
            merged[mid]["topic_labels"] = sorted(set(merged[mid]["topic_labels"]) | set(it["topic_labels"]))

    # Now merge newly fetched items into the existing store, de-duping by ID.
    store: Dict[str, dict] = {}
    for it in existing_items:
        if not isinstance(it, dict) or not it.get("id"):
            continue
        store[it["id"]] = it

    for mid, it in merged.items():
        if mid in store:
            # Merge topic tags if we already have this story.
            store[mid]["topics"] = sorted(set(store[mid].get("topics") or []) | set(it.get("topics") or []))
            store[mid]["topic_labels"] = sorted(set(store[mid].get("topic_labels") or []) | set(it.get("topic_labels") or []))
            # Prefer newer/filled fields where possible.
            for k in ["title", "source", "source_country", "published", "region", "country", "signals"]:
                if not store[mid].get(k) and it.get(k):
                    store[mid][k] = it[k]
        else:
            store[mid] = it

    # Trim store to keep the dashboard fast and relevant.
    # Keep items published in the last 30 days (fallback: keep if published missing).
    cutoff = now_utc - timedelta(days=30)
    trimmed: List[dict] = []
    for it in store.values():
        dt = _parse_published(it.get("published"))
        if dt is None or dt >= cutoff:
            trimmed.append(it)

    # Sort newest-first by parsed publish time.
    trimmed.sort(key=lambda x: (_parse_published(x.get("published")) or datetime(1970, 1, 1, tzinfo=timezone.utc)), reverse=True)
    return trimmed, meta


def _stable_id(url: str) -> str:
    import hashlib

    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def write_json(items: List[dict], meta: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    payload = {
        "meta": meta,
        "topics": [{"key": t.key, "label": t.label} for t in TOPICS],
        "items": items,
    }
    with open(os.path.join(DATA_DIR, "items.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def render_index(updated_human: str) -> None:
    with open(os.path.join(TPL_DIR, "base.html"), "r", encoding="utf-8") as f:
        tpl = Template(f.read())
    html = tpl.render(title="Industry Signals Dashboard", updated_human=updated_human)
    with open(os.path.join(ROOT, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)


def main() -> None:
    now = datetime.now(timezone.utc)
    items, meta = build_items(now)
    write_json(items, meta)
    updated_human = now.strftime("%Y-%m-%d %H:%M UTC")
    render_index(updated_human)
    print(f"Rendered {len(items)} items.")


if __name__ == "__main__":
    main()
