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
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Optional

import requests

# -----------------------------
# Strategy
# -----------------------------
# 1) ONE broad GDELT request per run (minimizes 429 risk).
# 2) Respect Retry-After + retry/backoff.
# 3) If GDELT fails (429 or non-JSON/HTML response), fall back to Google News RSS.
# 4) Never overwrite items.json with empty results if upstream is failing.
# -----------------------------

LOOKBACK_HOURS = 24 * 7
RETENTION_DAYS = 30
MAX_RECORDS = 250

MIN_DELAY_SECONDS = 2.0
MAX_RETRIES = 6
BACKOFF_BASE_SECONDS = 2.0

ITEMS_PATH = "data/items.json"

GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_MODE = "ArtList"
GDELT_SORT = "Date"

BROAD_QUERY = (
    '(warehouse OR "distribution center" OR "fulfillment center" OR logistics OR "supply chain" OR manufacturing OR plant OR automation) '
    'AND (investment OR invest OR expansion OR expand OR "new facility" OR "new warehouse" OR "new plant" OR groundbreaking OR "ribbon cutting" '
    'OR modernization OR upgrade OR capex OR "capital expenditure" OR earnings OR revenue OR guidance OR appointed OR named OR resigns OR layoffs OR strike)'
)

GN_RSS_URL = "https://news.google.com/rss/search"
GN_RSS_PARAMS = {
    "q": "new warehouse OR new distribution center OR warehouse expansion OR manufacturing investment OR warehouse automation OR AS/RS OR AGV OR AMR OR capex OR logistics investment OR plant expansion OR appointed CEO logistics",
    "hl": "en-US",
    "gl": "US",
    "ceid": "US:en",
}

@dataclasses.dataclass(frozen=True)
class Topic:
    key: str
    label: str

TOPICS: List[Topic] = [
    Topic("facility_new", "New facility / warehouse / DC"),
    Topic("facility_expansion", "Expansion / modernization"),
    Topic("manufacturing_investment", "Manufacturing investment"),
    Topic("warehouse_investment", "Warehouse / logistics investment"),
    Topic("real_estate_signal", "Industrial real estate / build-to-suit"),
    Topic("automation_signal", "Automation project signal"),
    Topic("leadership_change", "Leadership change (CEO/VP/Automation)"),
    Topic("revenue_update", "Revenue / earnings update"),
    Topic("risk_urgency", "Risk / urgency (closure, labor, disruption)"),
]

TOPIC_PATTERNS: Dict[str, List[str]] = {
    "facility_new": [
        r"\bnew\s+(warehouse|distribution\s+center|fulfillment\s+center|dc)\b",
        r"\bopens?\b.*\bwarehouse\b",
        r"\bgroundbreaking\b", r"\bribbon\s+cutting\b",
        r"\bnew\s+logistics\s+facility\b",
    ],
    "facility_expansion": [
        r"\bwarehouse\s+expansion\b", r"\bexpanding\b.*\bwarehouse\b",
        r"\bdistribution\s+center\s+expansion\b",
        r"\bmoderni[sz]ation\b", r"\bupgrade\b", r"\brenovation\b",
        r"\badds?\s+capacity\b", r"\badding\s+capacity\b",
    ],
    "manufacturing_investment": [
        r"\bmanufacturing\s+investment\b", r"\bplant\s+investment\b",
        r"\bnew\s+(plant|factory)\b", r"\bproduction\s+expansion\b",
        r"\badds?\s+production\s+capacity\b",
    ],
    "warehouse_investment": [
        r"\bwarehouse\s+investment\b", r"\blogistics\s+investment\b",
        r"\bsupply\s+chain\s+investment\b", r"\bcapex\b",
        r"\bcapital\s+expenditure\b", r"\bdistribution\s+investment\b",
    ],
    "real_estate_signal": [
        r"\bbuild-?to-?suit\b", r"\bindustrial\s+lease\b",
        r"\bsite\s+selection\b", r"\brezoning\b", r"\bpermitting\b",
        r"\bzoning\b", r"\beconomic\s+development\b", r"\btax\s+incentive\b",
        r"\bland\s+purchase\b",
    ],
    "automation_signal": [
        r"\bAS/RS\b", r"\bASRS\b", r"\bwarehouse\s+automation\b",
        r"\bmaterial\s+handling\s+automation\b",
        r"\bAGV\b", r"\bAMR\b", r"\brobotics?\b", r"\bpalleti[sz]ing\b",
        r"\bsortation\b", r"\bWMS\b", r"\bWCS\b", r"\bWES\b",
    ],
    "leadership_change": [
        r"\bappointed\b.*\b(CEO|COO|CFO|VP|Vice\s+President|Director)\b",
        r"\bnamed\b.*\b(CEO|COO|CFO|VP|Vice\s+President|Director)\b",
        r"\bjoins\s+as\b", r"\bhired\s+as\b", r"\bpromoted\s+to\b",
        r"\bsteps\s+down\b", r"\bresigns?\b", r"\bresignation\b",
    ],
    "revenue_update": [
        r"\brevenue\b", r"\bearnings\b", r"\bguidance\b",
        r"\bannual\s+report\b", r"\bnet\s+sales\b",
    ],
    "risk_urgency": [
        r"\bclosure\b", r"\bclosing\b", r"\blayoffs\b",
        r"\bstrike\b", r"\bunion\b", r"\blabor\s+dispute\b",
        r"\brecall\b", r"\boutage\b", r"\bdisruption\b",
        r"\bfire\b.*\b(warehouse|plant|facility)\b",
    ],
}

US_HINTS = [
    r"\bUSA\b", r"\bU\.S\.\b", r"\bUnited States\b",
    r"\bCalifornia\b", r"\bTexas\b", r"\bFlorida\b", r"\bNew York\b",
    r"\bGeorgia\b", r"\bIllinois\b", r"\bOhio\b",
]
CA_HINTS = [r"\bCanada\b", r"\bOntario\b", r"\bQuebec\b", r"\bQuébec\b", r"\bAlberta\b", r"\bBritish Columbia\b"]

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
    return hashlib.sha256((url or "").encode("utf-8")).hexdigest()[:16]

def load_json(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            return {}
    return {}

def save_json(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def is_us_or_ca_blob(blob: str, url: str, source_country: Optional[str]) -> bool:
    sc = (source_country or "").upper()
    if sc in {"US", "CA"}:
        return True
    u = (url or "").lower()
    if u.endswith(".ca") or ".ca/" in u:
        return True
    for pat in CA_HINTS:
        if re.search(pat, blob, re.IGNORECASE):
            return True
    for pat in US_HINTS:
        if re.search(pat, blob, re.IGNORECASE):
            return True
    return False

def classify_topics(blob: str) -> List[str]:
    hits = []
    for key, patterns in TOPIC_PATTERNS.items():
        for p in patterns:
            if re.search(p, blob, re.IGNORECASE):
                hits.append(key)
                break
    return sorted(set(hits))

def prune(items: List[dict], now: dt.datetime) -> List[dict]:
    cutoff = now - dt.timedelta(days=RETENTION_DAYS)
    out = []
    for it in items:
        ts = it.get("seendate") or it.get("published_at") or ""
        keep = True
        try:
            if ts and ts[0].isdigit() and len(ts) >= 14:
                d = dt.datetime.strptime(ts[:14], "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
            else:
                d = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if d < cutoff:
                keep = False
        except Exception:
            pass
        if keep:
            out.append(it)
    return out

def merge(existing: List[dict], new_items: List[dict]) -> List[dict]:
    by_id = {it.get("id"): it for it in existing if it.get("id")}
    for it in new_items:
        iid = it.get("id")
        if not iid:
            continue
        if iid in by_id:
            cur = by_id[iid]
            cur["topics"] = sorted(set(cur.get("topics") or []) | set(it.get("topics") or []))
        else:
            by_id[iid] = it
    merged = list(by_id.values())
    merged.sort(key=lambda x: x.get("seendate") or x.get("published_at") or "", reverse=True)
    return merged

def _sleep_for_429(r: requests.Response, attempt: int) -> None:
    retry_after = r.headers.get("Retry-After")
    try:
        wait = int(retry_after) if retry_after else BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
    except ValueError:
        wait = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
    wait *= random.uniform(0.8, 1.3)
    print(f"[warn] GDELT 429. Retry-After={retry_after} attempt={attempt}/{MAX_RETRIES} sleeping={wait:.1f}s", file=sys.stderr)
    time.sleep(wait)

def fetch_gdelt_broad(start: dt.datetime, end: dt.datetime) -> Tuple[List[dict], str]:
    params = {
        "query": BROAD_QUERY,
        "mode": GDELT_MODE,
        "format": "json",
        "sort": GDELT_SORT,
        "maxrecords": str(MAX_RECORDS),
        "startdatetime": gdelt_dt(start),
        "enddatetime": gdelt_dt(end),
    }

    last_status = None
    for attempt in range(1, MAX_RETRIES + 1):
        throttle()
        r = requests.get(GDELT_ENDPOINT, params=params, timeout=25, headers={"User-Agent": "AJ-IndustrySignals/1.0"})
        last_status = r.status_code

        if r.status_code == 200:
            # Sometimes upstream returns HTML/empty even with 200. Guard JSON parsing.
            try:
                data = r.json()
            except Exception as e:
                # Treat as transient failure and retry.
                backoff = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
                print(f"[warn] GDELT 200 but non-JSON response. attempt={attempt}/{MAX_RETRIES} sleeping={backoff:.1f}s err={e}", file=sys.stderr)
                time.sleep(backoff)
                continue
            return (data.get("articles") or []), "gdelt"

        if r.status_code == 429:
            _sleep_for_429(r, attempt)
            continue

        # For other 5xx, retry; otherwise break and fall back
        if 500 <= r.status_code < 600:
            backoff = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
            print(f"[warn] GDELT {r.status_code}. attempt={attempt}/{MAX_RETRIES} sleeping={backoff:.1f}s", file=sys.stderr)
            time.sleep(backoff)
            continue

        # Non-retryable
        try:
            r.raise_for_status()
        except Exception as e:
            print(f"[warn] GDELT request failed: {e}", file=sys.stderr)
        break

    return [], f"gdelt_failed_{last_status}"

def fetch_google_news_rss() -> List[dict]:
    r = requests.get(GN_RSS_URL, params=GN_RSS_PARAMS, timeout=25, headers={"User-Agent": "AJ-IndustrySignals/1.0"})
    r.raise_for_status()

    # Google RSS sometimes includes namespaces; handle both.
    root = ET.fromstring(r.text)
    channel = root.find("channel")
    if channel is None:
        # Namespace fallback
        channel = root.find("{http://purl.org/rss/1.0/}channel")
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
            "published_at": pub,
            "source_name": source_name,
            "snippet": "",
            "seendate": "",
            "sourceCountry": None,
            "domain": "",
        })
    return items

def main() -> int:
    now = dt.datetime.now(tz=dt.timezone.utc)
    window_start = now - dt.timedelta(hours=LOOKBACK_HOURS)
    window_end = now

    existing_doc = load_json(ITEMS_PATH)
    existing_items = existing_doc.get("items") or []

    articles, source = fetch_gdelt_broad(window_start, window_end)

    if not articles:
        print(f"[warn] GDELT returned 0 articles (source={source}). Falling back to Google News RSS.", file=sys.stderr)
        try:
            articles = fetch_google_news_rss()
            source = "google_news_rss"
        except Exception as e:
            print(f"[warn] Google News RSS fetch failed: {e}", file=sys.stderr)
            # Do not overwrite existing file
            print("[summary] No upstream data. Preserving existing items.json.", file=sys.stderr)
            return 0

    fetched = len(articles)
    kept = 0
    collected: List[dict] = []

    for a in articles:
        title = a.get("title") or ""
        snippet = a.get("snippet") or a.get("description") or ""
        url = a.get("url") or ""
        if not url:
            continue

        blob = f"{title} {snippet}"
        if not is_us_or_ca_blob(blob, url, a.get("sourceCountry")):
            continue

        topics = classify_topics(blob)
        if not topics:
            continue

        kept += 1
        collected.append({
            "id": url_id(url),
            "title": title,
            "url": url,
            "source_name": a.get("domain") or a.get("source_name") or "",
            "published_at": a.get("published_at") or "",
            "seendate": a.get("seendate") or "",
            "topics": topics,
            "snippet": snippet,
            "signals": {},
        })

    merged = merge(existing_items, collected)
    merged = prune(merged, now)

    out = {
        "meta": {
            "generated_at": now.isoformat(),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "source": source,
            "fetched": fetched,
            "kept": kept,
        },
        "topics": [{"key": t.key, "label": t.label} for t in TOPICS],
        "items": merged,
    }

    # Never wipe the feed if upstream returned nothing useful
    if kept == 0 and len(existing_items) > 0:
        print("[summary] Kept=0; preserving existing items.json.", file=sys.stderr)
        return 0

    save_json(ITEMS_PATH, out)
    print(f"[summary] source={source} fetched={fetched} kept={kept} stored={len(merged)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
