#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

import requests

ITEMS_PATH = "data/items.json"

LOOKBACK_HOURS = 24 * 7
RETENTION_DAYS = 30
REQUEST_DELAY_SECONDS = 1.0
MAX_ITEMS_PER_FEED = 60
TIMEOUT_SECONDS = 25

GN_RSS_URL = "https://news.google.com/rss/search"

TOPICS = [
    {"key": "facility_new", "label": "New facility / warehouse / DC"},
    {"key": "facility_expansion", "label": "Expansion / modernization"},
    {"key": "manufacturing_investment", "label": "Manufacturing investment"},
    {"key": "warehouse_investment", "label": "Warehouse / logistics investment"},
    {"key": "real_estate_signal", "label": "Industrial real estate / build-to-suit"},
    {"key": "automation_signal", "label": "Automation project signal"},
    {"key": "leadership_change", "label": "Leadership change (CEO/VP/Automation)"},
    {"key": "revenue_update", "label": "Revenue / earnings update"},
    {"key": "risk_urgency", "label": "Risk / urgency (closure, labor, disruption)"},
]

TOPIC_QUERIES: Dict[str, str] = {
    "facility_new": '"new warehouse" OR "new distribution center" OR "new fulfillment center" OR groundbreaking OR "ribbon cutting" OR "new logistics facility"',
    "facility_expansion": '"warehouse expansion" OR "distribution center expansion" OR modernization OR upgrade OR renovation OR "adds capacity" OR "expanding operations"',
    "manufacturing_investment": '"manufacturing investment" OR "plant expansion" OR "new plant" OR "new factory" OR "production expansion" OR "adds production capacity"',
    "warehouse_investment": '("warehouse investment" OR "logistics investment" OR "supply chain investment" OR capex OR "capital expenditure") AND (warehouse OR logistics OR "distribution center")',
    "real_estate_signal": '"build-to-suit" OR "industrial lease" OR "site selection" OR rezoning OR permitting OR zoning OR "economic development" OR "tax incentive" OR "land purchase"',
    "automation_signal": '"warehouse automation" OR "material handling automation" OR ASRS OR "AS/RS" OR AGV OR AMR OR robotics OR sortation OR "goods-to-person" OR WMS OR WCS OR WES',
    "leadership_change": '(appointed OR named OR "joins as" OR "hired as" OR promoted OR resigns OR "steps down") AND (CEO OR COO OR CFO OR "Chief" OR VP OR "Vice President" OR Director) AND (supply chain OR logistics OR operations OR manufacturing OR automation)',
    "revenue_update": '(revenue OR earnings OR guidance OR "annual report" OR "net sales") AND (capex OR logistics OR warehouse OR supply chain)',
    "risk_urgency": '(closure OR closing OR layoffs OR strike OR union OR "labor dispute" OR recall OR outage OR disruption OR fire) AND (warehouse OR "distribution center" OR plant OR facility)',
}

NON_NA_COUNTRY_HINTS = re.compile(
    r"\b(UK|United Kingdom|England|Scotland|Wales|Ireland|EU|European Union|Germany|France|Spain|Italy|Netherlands|Sweden|Norway|Finland|Denmark|Poland|India|China|Japan|Korea|Australia|New Zealand|Brazil|Mexico|Nigeria|South Africa)\b",
    re.IGNORECASE,
)

def _hash_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]

def _safe_pubdate_to_iso(pub: str) -> str:
    if not pub:
        return ""
    try:
        d = parsedate_to_datetime(pub)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc).isoformat()
    except Exception:
        return ""

def _parse_google_rss(xml_text: str) -> List[dict]:
    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    if channel is None:
        for child in root:
            if child.tag.endswith("channel"):
                channel = child
                break
    if channel is None:
        return []

    items = []
    for it in channel.findall("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()

        source_el = it.find("source")
        source_name = (source_el.text.strip() if source_el is not None and source_el.text else "")

        items.append({
            "title": title,
            "url": link,
            "published_at": _safe_pubdate_to_iso(pub),
            "source_name": source_name,
        })
    return items

def _fetch_rss(query: str, hl: str, gl: str, ceid: str) -> Tuple[List[dict], Optional[str]]:
    params = {"q": query, "hl": hl, "gl": gl, "ceid": ceid}
    headers = {"User-Agent": "AJ-IndustrySignals/1.0 (+github-pages)"}
    r = requests.get(GN_RSS_URL, params=params, headers=headers, timeout=TIMEOUT_SECONDS)
    if r.status_code != 200:
        return [], f"HTTP {r.status_code}"
    try:
        return _parse_google_rss(r.text), None
    except Exception as e:
        return [], f"parse_error: {e}"

def _prune(items: List[dict], now: dt.datetime) -> List[dict]:
    cutoff = now - dt.timedelta(days=RETENTION_DAYS)
    out = []
    for it in items:
        iso = it.get("published_at") or ""
        keep = True
        if iso:
            try:
                d = dt.datetime.fromisoformat(iso.replace("Z", "+00:00"))
                if d.tzinfo is None:
                    d = d.replace(tzinfo=dt.timezone.utc)
                d = d.astimezone(dt.timezone.utc)
                if d < cutoff:
                    keep = False
            except Exception:
                pass
        if keep:
            out.append(it)
    return out

def main() -> int:
    now = dt.datetime.now(tz=dt.timezone.utc)
    window_start = now - dt.timedelta(hours=LOOKBACK_HOURS)

    existing = {}
    if os.path.exists(ITEMS_PATH):
        try:
            with open(ITEMS_PATH, "r", encoding="utf-8") as f:
                existing_doc = json.load(f) or {}
            for it in (existing_doc.get("items") or []):
                if it.get("id"):
                    existing[it["id"]] = it
        except Exception:
            existing = {}

    fetched_total = 0
    new_total = 0
    errors = []

    editions = [
        ("en-US", "US", "US:en"),
        ("en-CA", "CA", "CA:en"),
    ]

    for topic in TOPICS:
        key = topic["key"]
        query = TOPIC_QUERIES.get(key, "")
        if not query:
            continue

        for hl, gl, ceid in editions:
            feed_items, err = _fetch_rss(query=query, hl=hl, gl=gl, ceid=ceid)
            if err:
                errors.append(f"{key} {gl}: {err}")
            fetched_total += len(feed_items)

            for a in feed_items[:MAX_ITEMS_PER_FEED]:
                title = a.get("title") or ""
                url = a.get("url") or ""
                if not url:
                    continue
                if NON_NA_COUNTRY_HINTS.search(title):
                    continue

                iid = _hash_id(url)
                if iid in existing:
                    tset = set(existing[iid].get("topics") or [])
                    tset.add(key)
                    existing[iid]["topics"] = sorted(tset)
                    if not existing[iid].get("source_name") and a.get("source_name"):
                        existing[iid]["source_name"] = a.get("source_name") or ""
                    if not existing[iid].get("published_at") and a.get("published_at"):
                        existing[iid]["published_at"] = a.get("published_at") or ""
                        existing[iid]["seendate"] = a.get("published_at") or ""
                else:
                    existing[iid] = {
                        "id": iid,
                        "title": title,
                        "url": url,
                        "source_name": a.get("source_name") or "",
                        "published_at": a.get("published_at") or "",
                        "seendate": a.get("published_at") or "",
                        "topics": [key],
                        "snippet": "",
                        "signals": {},
                    }
                    new_total += 1

            time.sleep(REQUEST_DELAY_SECONDS)

    items = list(existing.values())
    items = _prune(items, now)
    items.sort(key=lambda x: x.get("published_at") or x.get("seendate") or "", reverse=True)

    out = {
        "meta": {
            "generated_at": now.isoformat(),
            "window_start": window_start.isoformat(),
            "window_end": now.isoformat(),
            "source": "Google News RSS (search-based)",
            "fetched": fetched_total,
            "new_items": new_total,
            "errors": errors[:50],
        },
        "topics": TOPICS,
        "items": items,
    }

    os.makedirs(os.path.dirname(ITEMS_PATH), exist_ok=True)
    with open(ITEMS_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"[summary] fetched={fetched_total} new_items={new_total} stored={len(items)} errors={len(errors)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
