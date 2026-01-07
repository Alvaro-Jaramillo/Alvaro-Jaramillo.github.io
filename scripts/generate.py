#!/usr/bin/env python3
"""Generate the data file for the GitHub Pages dashboard.

This script runs in GitHub Actions and produces `data/items.json` consumed by
`index.html`.

Key behavior (tuned for "broad monitoring"):
- Query GDELT Doc API with high-signal keyword packs.
- DO NOT require geo terms in the query (too many headlines omit them).
- Filter to USA/Canada heuristically after fetching (title+snippet+sourceCountry+URL).
- Merge into existing `data/items.json` and keep last 30 days.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import requests
from jinja2 import Template

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
TPL_DIR = os.path.join(ROOT, "templates")

GDELT_DOC_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"

# --------- Geo dictionaries (used only for post-filter + region detection) ---------
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


# High-signal keyword packs. We keep these fairly broad and rely on post-filtering.
TOPICS: List[Topic] = [
    Topic(
        key="facility_new",
        label="New facility / warehouse / DC",
        query='("new warehouse" OR "new distribution center" OR "new distribution centre" OR "new DC" OR "new fulfillment center" OR "new fulfilment centre" OR "opens new" OR "opening a new" OR groundbreaking OR "ribbon cutting")',
    ),
    Topic(
        key="facility_expansion",
        label="Expansion / modernization",
        query='("warehouse expansion" OR "distribution center expansion" OR "plant expansion" OR expansion OR expanded OR expanding OR modernization OR modernisation OR upgrade OR renovation OR "adds capacity" OR "adding capacity")',
    ),
    Topic(
        key="manufacturing_investment",
        label="Manufacturing investment",
        query='("manufacturing investment" OR "plant investment" OR "capital investment" OR capex OR "production expansion" OR "new plant" OR "new factory" OR "manufacturing facility")',
    ),
    Topic(
        key="warehouse_investment",
        label="Warehouse / logistics investment",
        query='("warehouse investment" OR "distribution investment" OR "logistics investment" OR "supply chain investment" OR "capital investment" OR capex OR "investing" OR "invests")',
    ),
    Topic(
        key="real_estate_signal",
        label="Industrial real estate / build-to-suit",
        query='("build-to-suit" OR "industrial lease" OR "leased" OR "site selection" OR "selects site" OR "planning commission" OR rezoning OR permit OR permitting OR zoning)',
    ),
    Topic(
        key="automation_signal",
        label="Automation project signal",
        query='("AS/RS" OR ASRS OR "automated storage" OR shuttle OR "goods-to-person" OR GTP OR AMR OR "autonomous mobile" OR robotics OR robotic OR palletizing OR sortation OR conveyor OR "WMS" OR "WES" OR "WCS" OR "warehouse automation" OR "distribution automation")',
    ),
    Topic(
        key="leadership_change",
        label="Leadership change (CEO/VP/Automation)",
        query='((appointed OR names OR named OR joins OR hired OR promoted OR resigns OR "steps down") AND (CEO OR COO OR CFO OR "Chief Executive" OR VP OR "Vice President" OR "Head of" OR Director) AND ("supply chain" OR operations OR logistics OR automation OR engineering))',
    ),
    Topic(
        key="revenue_update",
        label="Revenue / earnings update",
        query='((revenue OR "net sales" OR earnings OR guidance OR quarter OR Q1 OR Q2 OR Q3 OR Q4) AND ("supply chain" OR distribution OR logistics OR capex OR investment))',
    ),
    Topic(
        key="risk_urgency",
        label="Risk / urgency (closure, labor, disruption)",
        query='((layoffs OR closure OR "shutting down" OR consolidation OR strike OR union OR "labor shortage" OR "labour shortage" OR fire OR recall) AND (plant OR warehouse OR "distribution center" OR "distribution centre" OR facility))',
    ),
]


# --------- Fetch ---------

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
    r = requests.get(GDELT_DOC_ENDPOINT, params=params, timeout=45)
    r.raise_for_status()
    data = r.json() if r.content else {}
    return data.get("articles") or []


# --------- Enrichment ---------

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


def detect_region_and_country(text: str, source_country: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    t = text or ""
    for p in CA_PROVINCES:
        if re.search(r"\b" + re.escape(p) + r"\b", t, re.IGNORECASE):
            return p, "CA"
    for s in US_STATES:
        if re.search(r"\b" + re.escape(s) + r"\b", t, re.IGNORECASE):
            return s, "US"

    sc = (source_country or "").upper()
    if sc in {"US", "CA"}:
        return None, sc

    if re.search(r"\bcanada\b|\bontario\b|\bquebec\b", t, re.IGNORECASE):
        return None, "CA"
    if re.search(r"\bunited states\b|\busa\b|\bu\.s\.\b", t, re.IGNORECASE):
        return None, "US"

    return None, None


def is_us_or_ca(article: dict) -> bool:
    title = article.get("title") or ""
    snippet = article.get("snippet") or article.get("description") or ""
    blob = f"{title} {snippet}"

    region, country = detect_region_and_country(blob, article.get("sourceCountry"))

    url = (article.get("url") or "").lower()
    if url.endswith(".ca") or ".ca/" in url:
        return True

    return country in {"US", "CA"} or region is not None


def _parse_published(published: Optional[str]) -> Optional[datetime]:
    if not published:
        return None
    s = published.strip()
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


def _stable_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


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
    end = now_utc
    # Seed window: 7 days to ensure initial population, then history is retained/trimmed.
    start = now_utc - timedelta(days=7)

    existing_items = _load_existing_items()

    seen_urls: Set[str] = set()
    store: Dict[str, dict] = {}

    for it in existing_items:
        if not isinstance(it, dict) or not it.get("id"):
            continue
        store[it["id"]] = it
        if it.get("url"):
            seen_urls.add(it["url"])

    meta = {
        "generated_at": now_utc.isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "source": "GDELT 2.1 Doc API",
    }

    total_fetched = 0
    total_kept = 0

    for topic in TOPICS:
        q = f"({topic.query})"  # intentionally no GEO constraint
        try:
            arts = gdelt_fetch(q, start=start, end=end, max_records=250)
        except Exception as e:
            print(f"[warn] GDELT fetch failed for topic {topic.key}: {e}", file=sys.stderr)
            continue

        total_fetched += len(arts)
        kept_this_topic = 0

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

            snippet = (a.get("snippet") or a.get("description") or "").strip()
            blob = f"{title} {snippet}".strip()

            region, country = detect_region_and_country(blob, source_country)
            signals = extract_signals(blob)

            item_id = _stable_id(url)
            new_item = {
                "id": item_id,
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

            if item_id in store:
                # Merge topic tags.
                store[item_id]["topics"] = sorted(set(store[item_id].get("topics") or []) | {topic.key})
                store[item_id]["topic_labels"] = sorted(set(store[item_id].get("topic_labels") or []) | {topic.label})
            else:
                store[item_id] = new_item

            kept_this_topic += 1

        total_kept += kept_this_topic
        print(f"[topic] {topic.key}: fetched={len(arts)} kept={kept_this_topic}")

    # Keep last 30 days.
    cutoff = now_utc - timedelta(days=30)
    trimmed: List[dict] = []
    for it in store.values():
        dt = _parse_published(it.get("published"))
        if dt is None or dt >= cutoff:
            trimmed.append(it)

    trimmed.sort(
        key=lambda x: (_parse_published(x.get("published")) or datetime(1970, 1, 1, tzinfo=timezone.utc)),
        reverse=True,
    )

    print(f"[summary] total_fetched={total_fetched} total_kept={total_kept} stored={len(trimmed)}")
    return trimmed, meta


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
