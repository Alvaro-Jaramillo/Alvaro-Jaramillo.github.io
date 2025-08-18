# News Aggregator for GitHub Pages

This repo builds a clean, single-page site that shows the latest top articles from websites you choose. It runs on GitHub Pages with a GitHub Actions workflow that fetches RSS/Atom feeds, writes a JSON data file, and renders `index.html` with a nice layout.

## How it works
- You list sites and their RSS/Atom URLs in `feeds.yml`.
- A GitHub Action runs on a schedule or on demand.
- The Python script in `scripts/generate.py` pulls the feeds and produces `data/articles.json` and a fresh `index.html` using templates in `templates/`.
- GitHub Pages serves the site from the default branch or the `gh-pages` branch, depending on your repo settings.

## Quick start
1. **Add your sites**: Edit `feeds.yml` and place your target websites and feed URLs.
2. **Commit and push** the repo to GitHub.
3. **Enable Pages**: Settings → Pages → Build from **GitHub Actions** or from your default branch.
4. The provided **workflow** commits changes after each run. It uses the repo's `GITHUB_TOKEN` by default.

## Run locally
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r scripts/requirements.txt
python scripts/generate.py
# open index.html in your browser
```

## Manual run in GitHub
Go to **Actions → Update news → Run workflow**.

## Customize look and feel
- Edit `templates/base.html` and `assets/style.css`.
- The site uses a simple card grid with a filter box, sort control, and source toggles.
