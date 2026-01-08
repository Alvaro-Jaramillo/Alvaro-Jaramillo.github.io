# Automation Links 2.0

A GitHub Pages site that shows automation/intralogistics links.

## How it works
- `keywords.json` contains search queries (keywords/phrases).
- GitHub Actions runs hourly and executes `scripts/fetch-feeds.mjs`.
- The script pulls Google News RSS search feeds for each query, dedupes, scores signals, auto-tags, and writes `data/links.json`.
- The website reads `data/links.json` and displays the list with filters.

## Local test
```bash
npm install
npm run fetch
python -m http.server 8000
```
Open http://localhost:8000

## Add keywords
Edit `keywords.json` and push. Then run the workflow or wait for the next scheduled run.
