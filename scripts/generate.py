#!/usr/bin/env python3
import os, sys, json, time, hashlib
import yaml
import feedparser
from jinja2 import Template
from datetime import datetime, timezone
from dateutil import parser as dateparser

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
TPL_DIR = os.path.join(ROOT, "templates")

def load_feeds():
    with open(os.path.join(ROOT, "feeds.yml"), "r") as f:
        cfg = yaml.safe_load(f) or {}
    sites = cfg.get("sites", [])
    # Clean items
    out = []
    for s in sites:
        name = s.get("name")
        url = s.get("url")
        max_items = s.get("max_items", 10)
        if name and url:
            out.append({"name": name.strip(), "url": url.strip(), "max_items": int(max_items)})
    return out

def to_iso(dt):
    if not dt:
        return None
    if isinstance(dt, datetime):
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    try:
        d = dateparser.parse(dt)
        if not d.tzinfo:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def fetch_articles(sites):
    articles = []
    for site in sites:
        try:
            parsed = feedparser.parse(site["url"])
            count = 0
            for entry in parsed.entries:
                if count >= site["max_items"]:
                    break
                title = entry.get("title") or ""
                link = entry.get("link") or ""
                desc = entry.get("summary") or entry.get("description") or ""
                published = to_iso(entry.get("published") or entry.get("updated"))
                if not title or not link:
                    continue
                articles.append({
                    "source": site["name"],
                    "title": title.strip(),
                    "link": link.strip(),
                    "description": strip_html(desc).strip()[:600],
                    "published": published
                })
                count += 1
        except Exception as e:
            print(f"[warn] Failed to parse {site['name']} - {site['url']}: {e}", file=sys.stderr)
    return articles

def strip_html(html):
    # very light HTML stripper
    import re
    return re.sub("<[^<]+?>", "", html or "")

def write_json(articles):
    os.makedirs(DATA_DIR, exist_ok=True)
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "articles": articles
    }
    with open(os.path.join(DATA_DIR, "articles.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def render_index(updated_human):
    with open(os.path.join(TPL_DIR, "base.html"), "r", encoding="utf-8") as f:
        tpl = Template(f.read())
    html = tpl.render(title="Industry News Dashboard", updated_human=updated_human)
    with open(os.path.join(ROOT, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

def main():
    sites = load_feeds()
    articles = fetch_articles(sites)
    # Sort newest first
    articles.sort(key=lambda a: a.get("published") or "", reverse=True)
    write_json(articles)
    updated_human = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    render_index(updated_human)
    print(f"Rendered {len(articles)} articles.")

if __name__ == "__main__":
    main()
