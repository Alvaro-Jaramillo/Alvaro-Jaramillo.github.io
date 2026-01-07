# Industry Signals Dashboard (USA + Canada)

This repo builds a clean, single-page dashboard that surfaces **facility / investment / automation / leadership** signals across the **USA and Canada**.

It runs on **GitHub Pages** and refreshes via **GitHub Actions every 30 minutes**.

## How it works

1. A GitHub Actions workflow (see `.github/workflows/update.yml`) runs on a schedule.
2. `scripts/generate.py` queries the web using the **GDELT 2.1 Doc API** (news aggregation) with several topic query packs.
3. The script writes `data/items.json`.
4. `index.html` loads `data/items.json` and renders a filterable feed.

## What you can filter in the UI

- Topic (facility new, expansion, manufacturing investment, warehouse investment, real estate signals, automation, leadership changes, revenue updates, risk/urgency)
- Country (USA / Canada)
- Region (state / province when detected)
- Signals (contains **$ investment**, **sqft**, **jobs** in the headline)
- Source (publisher domain)
- Time window (last 24h / 7d / 30d)

## Customize the topics

Edit `TOPICS` inside `scripts/generate.py`.

Each topic has:
- `key`: stable identifier (used by the UI)
- `label`: what shows up as a filter chip
- `query`: GDELT query string

After you change topics, the dashboard will reflect it on the next scheduled run.

## Run locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r scripts/requirements.txt
python scripts/generate.py
# open index.html in your browser
```

> Note: Running locally requires internet access (to reach the GDELT API).

## Notes / next upgrades

- If you want Google-based results too, you can add **Google Programmable Search API** as a secondary source (requires an API key stored in GitHub Actions secrets).
- If you want better geo-precision, we can add a follow-up step that extracts locations from article content (requires fetching pages or using a richer API).
