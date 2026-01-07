#!/usr/bin/env python3
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import os
import random
import re
import sys
import time
from typing import List

import requests

GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"
MODE = "ArtList"
SORT = "Date"

LOOKBACK_HOURS = 24 * 7
RETENTION_DAYS = 30

MAX_RECORDS = 75
TOPICS_PER_RUN = 3

MIN_DELAY_SECONDS = 2.0
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2.0

ITEMS_PATH = "data/items.json"
STATE_PATH = "data/state.json"

@dataclasses.dataclass(frozen=True)
class Topic:
    key: str
    label: str
    query: str

TOPICS: List[Topic] = [
    Topic("facility_new","New facility / warehouse / DC",
          '("new warehouse" OR "new distribution center" OR "new fulfillment center" '
          'OR groundbreaking OR "ribbon cutting" OR "new logistics facility")'),
    Topic("facility_expansion","Expansion / modernization",
          '("warehouse expansion" OR "distribution center expansion" OR modernization '
          'OR upgrade OR renovation OR "adds capacity")'),
    Topic("manufacturing_investment","Manufacturing investment",
          '("manufacturing investment" OR "manufacturing expansion" OR "new plant" OR "new factory")'),
    Topic("warehouse_investment","Warehouse / logistics investment",
          '(("warehouse investment" OR "logistics investment" OR capex) '
          'AND (warehouse OR "distribution center" OR logistics))'),
    Topic("real_estate_signal","Industrial real estate / build-to-suit",
          '("build-to-suit" OR "industrial lease" OR "site selection" OR rezoning OR permitting '
          'OR zoning OR "economic development" OR "tax incentive")'),
    Topic("automation_signal","Automation project signal",
          '("AS/RS" OR ASRS OR "warehouse automation" OR AGV OR AMR OR robotics '
          'OR palletizing OR sortation OR WMS OR WCS OR WES)'),
    Topic("leadership_change","Leadership change",
          '((appointed OR named OR "joins as" OR "hired as" OR resigns) '
          'AND (CEO OR COO OR CFO OR VP OR Director))'),
    Topic("revenue_update","Revenue / earnings update",
          '(("revenue" OR earnings OR guidance) AND (capex OR logistics OR warehouse))'),
    Topic("risk_urgency","Risk / urgency",
          '((closure OR layoffs OR strike OR recall OR disruption) '
          'AND (warehouse OR plant OR facility))'),
]

US_HINTS = [r"\bUSA\b", r"\bUnited States\b", r"\bCalifornia\b", r"\bTexas\b", r"\bNew York\b"]
CA_HINTS = [r"\bCanada\b", r"\bOntario\b", r"\bQuebec\b", r"\bBritish Columbia\b"]

_last_call_ts = 0.0

def throttle():
    global _last_call_ts
    now = time.time()
    if now - _last_call_ts < MIN_DELAY_SECONDS:
        time.sleep(MIN_DELAY_SECONDS - (now - _last_call_ts))
    _last_call_ts = time.time()

def gdelt_dt(d: dt.datetime) -> str:
    return d.astimezone(dt.timezone.utc).strftime("%Y%m%d%H%M%S")

def url_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]

def is_us_or_ca(a: dict) -> bool:
    blob = f"{a.get('title','')} {a.get('snippet','')}"
    if a.get("sourceCountry") in {"US","CA"}:
        return True
    for pat in US_HINTS + CA_HINTS:
        if re.search(pat, blob, re.IGNORECASE):
            return True
    if ".ca/" in (a.get("url") or ""):
        return True
    return False

def fetch_gdelt(query: str, start: dt.datetime, end: dt.datetime):
    params = {
        "query": query,
        "mode": MODE,
        "format": "json",
        "sort": SORT,
        "maxrecords": str(MAX_RECORDS),
        "startdatetime": gdelt_dt(start),
        "enddatetime": gdelt_dt(end),
    }

    for attempt in range(1, MAX_RETRIES + 1):
        throttle()
        r = requests.get(GDELT_ENDPOINT, params=params, timeout=25)
        if r.status_code == 200:
            return r.json().get("articles", [])
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            try:
                wait = int(retry_after) if retry_after else BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            except ValueError:
                wait = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            wait *= random.uniform(0.8, 1.3)
            print(f"[429] Retry-After={retry_after} sleeping={wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
            continue
        r.raise_for_status()
    return []

def load_json(path):
    if os.path.exists(path):
        with open(path,"r",encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,"w",encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def pick_topics():
    state = load_json(STATE_PATH)
    cur = state.get("topic_cursor", 0)
    chosen = [TOPICS[(cur+i)%len(TOPICS)] for i in range(TOPICS_PER_RUN)]
    state["topic_cursor"] = (cur + TOPICS_PER_RUN) % len(TOPICS)
    save_json(STATE_PATH, state)
    return chosen

def main():
    now = dt.datetime.now(tz=dt.timezone.utc)
    start = now - dt.timedelta(hours=LOOKBACK_HOURS)

    existing = load_json(ITEMS_PATH).get("items", [])
    by_id = {i["id"]: i for i in existing if "id" in i}

    for t in pick_topics():
        arts = fetch_gdelt(t.query, start, now)
        kept = [a for a in arts if is_us_or_ca(a)]
        print(f"[topic] {t.key}: fetched={len(arts)} kept={len(kept)}")
        for a in kept:
            url = a.get("url")
            if not url:
                continue
            by_id.setdefault(url_id(url), {
                "id": url_id(url),
                "title": a.get("title",""),
                "url": url,
                "source_name": a.get("domain",""),
                "seendate": a.get("seendate",""),
                "topics": [t.key],
                "snippet": a.get("snippet",""),
            })

    out = {
        "meta": {
            "generated_at": now.isoformat(),
            "window_start": start.isoformat(),
            "window_end": now.isoformat(),
            "source": "GDELT 2.1 Doc API",
        },
        "topics": [{"key": t.key, "label": t.label} for t in TOPICS],
        "items": sorted(by_id.values(), key=lambda x: x.get("seendate",""), reverse=True)
    }

    save_json(ITEMS_PATH, out)
    print(f"[summary] stored={len(out['items'])}")

if __name__ == "__main__":
    main()
