import fs from "node:fs";
import path from "node:path";
import Parser from "rss-parser";

const root = process.cwd();
const cfgPath = path.join(root, "keywords.json");
const outDir = path.join(root, "data");
const outPath = path.join(outDir, "links.json");

const parser = new Parser({
  timeout: 20000,
  headers: { "User-Agent": "AutomationLinks2.0 (GitHub Actions RSS fetcher)" }
});

function stripHtml(s = "") {
  return s
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<\/?[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function pickSummary(item) {
  const candidates = [item.contentSnippet, item.summary, item.content, item["content:encoded"], item.description].filter(Boolean);
  const text = stripHtml(candidates[0] || "");
  return text.length > 260 ? text.slice(0, 260) + "…" : text;
}

function toIso(d) {
  if (!d) return null;
  const dt = new Date(d);
  return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
}

function buildGoogleNewsRssUrl(query, lang="en", region="US") {
  // Google News RSS search endpoint (query in URL). We rely on standard RSS parsing.
  const q = encodeURIComponent(query);
  // hl (UI language), gl (country), ceid (country:language)
  const ceid = `${region}:${lang}`;
  return `https://news.google.com/rss/search?q=${q}&hl=${lang}&gl=${region}&ceid=${ceid}`;
}

const TAG_RULES = [
  { tag: "expansion", patterns: ["expansion","new distribution center","new warehouse","capacity expansion","facility expansion","greenfield","groundbreaking"] },
  { tag: "labor", patterns: ["labor shortage","labor costs","hiring","staffing","turnover"] },
  { tag: "ecommerce", patterns: ["e-commerce","ecommerce","omnichannel","same-day","next-day","direct-to-consumer","d2c"] },
  { tag: "asrs", patterns: ["as/rs","automated storage","shuttle system","miniload","high bay","pallet shuttle"] },
  { tag: "robotics", patterns: ["robotic","robotics","amr","agv","autonomous mobile","goods-to-person","g2p"] },
  { tag: "material-handling", patterns: ["conveyor","sortation","material handling","intralogistics"] },
  { tag: "modernization", patterns: ["modernization","upgrade","retrofit","brownfield","digital supply chain"] }
];

const SIGNAL_WEIGHTS = [
  { name: "direct_automation", patterns: ["warehouse automation","automated warehouse","distribution center automation","intralogistics automation","automated material handling"], w: 3 },
  { name: "core_tech", patterns: ["as/rs","goods-to-person","shuttle system","robotic picking","amr","agv","high bay","miniload"], w: 2 },
  { name: "expansion", patterns: ["new distribution center","distribution center expansion","warehouse expansion","capacity expansion","new warehouse","greenfield","groundbreaking"], w: 2 },
  { name: "pain", patterns: ["labor shortage","capacity constraints","bottleneck","backlog","fulfillment delays","rising labor costs"], w: 1 }
];

function scoreSignal(text) {
  const t = (text || "").toLowerCase();
  let score = 0;
  const hits = [];
  for (const g of SIGNAL_WEIGHTS) {
    for (const p of g.patterns) {
      if (t.includes(p)) {
        score += g.w;
        hits.push(p);
        break;
      }
    }
  }
  let bucket = "low";
  if (score >= 5) bucket = "hot";
  else if (score >= 3) bucket = "medium";
  return { score, bucket, hits };
}

// Very lightweight company guess: take first "X - Y" pattern often used by Google News RSS (Title - Publisher),
// or first capitalized token sequence (fallback).
function guessCompany(title="") {
  // If title includes ':' or ' - ', take left side (often company/event).
  const split1 = title.split(" - ")[0]?.trim();
  if (split1 && split1.length >= 3 && split1.length <= 80) return split1;
  const split2 = title.split(":")[0]?.trim();
  if (split2 && split2.length >= 3 && split2.length <= 80) return split2;

  const m = title.match(/\b([A-Z][A-Za-z&\.]+(?:\s+[A-Z][A-Za-z&\.]+){0,3})\b/);
  return m ? m[1].trim() : null;
}

function autoTags(text) {
  const t = (text || "").toLowerCase();
  const tags = new Set();
  for (const r of TAG_RULES) {
    if (r.patterns.some(p => t.includes(p))) tags.add(r.tag);
  }
  return [...tags];
}

async function main() {
  if (!fs.existsSync(cfgPath)) throw new Error("keywords.json not found");

  const cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
  const queries = cfg.queries || [];
  const maxItemsPerQuery = Number(cfg.maxItemsPerQuery ?? 50);
  const maxTotalItems = Number(cfg.maxTotalItems ?? 600);
  const lang = cfg.language || "en";
  const region = cfg.region || "US";

  const items = [];
  const errors = [];

  for (const q of queries) {
    const url = buildGoogleNewsRssUrl(q, lang, region);
    try {
      const feed = await parser.parseURL(url);
      const feedItems = (feed.items || []).slice(0, maxItemsPerQuery);
      for (const it of feedItems) {
        const link = it.link || it.guid || "";
        if (!link) continue;

        const title = (it.title || "").trim();
        const summary = pickSummary(it);
        const textForMatch = `${title} ${summary}`;

        const sig = scoreSignal(textForMatch);
        const tags = new Set([...(autoTags(textForMatch)), ...(sig.hits.length ? ["signal"] : [])]);

        items.push({
          title,
          url: link,
          source: "Google News (RSS Search)",
          company_guess: guessCompany(title),
          matched_queries: [q],
          tags: [...tags],
          signal_score: sig.score,
          signal_bucket: sig.bucket,
          published_at: toIso(it.isoDate || it.pubDate || it.date),
          summary
        });
      }
    } catch (e) {
      errors.push({ query: q, url, error: String(e?.message || e) });
    }
  }

  // Deduplicate by URL, merge matched_queries and tags
  const byUrl = new Map();
  for (const it of items) {
    const key = (it.url || "").trim();
    if (!key) continue;
    if (!byUrl.has(key)) {
      byUrl.set(key, it);
    } else {
      const prev = byUrl.get(key);
      const mq = new Set([...(prev.matched_queries || []), ...(it.matched_queries || [])]);
      const tg = new Set([...(prev.tags || []), ...(it.tags || [])]);
      prev.matched_queries = [...mq];
      prev.tags = [...tg];
      // Keep the higher signal score
      if ((it.signal_score ?? 0) > (prev.signal_score ?? 0)) {
        prev.signal_score = it.signal_score;
        prev.signal_bucket = it.signal_bucket;
      }
    }
  }

  const deduped = [...byUrl.values()];
  deduped.sort((a, b) => {
    const da = new Date(a.published_at || 0).getTime();
    const db = new Date(b.published_at || 0).getTime();
    return db - da;
  });

  const finalItems = deduped.slice(0, maxTotalItems);

  fs.mkdirSync(outDir, { recursive: true });
  const out = {
    generated_at: new Date().toISOString(),
    mode: "google_news_rss_search",
    query_count: queries.length,
    item_count: finalItems.length,
    errors,
    items: finalItems
  };

  fs.writeFileSync(outPath, JSON.stringify(out, null, 2), "utf8");

  console.log(`Wrote ${finalItems.length} items to ${outPath}`);
  if (errors.length) console.log(`Errors: ${errors.length}`);

  // Fail only if everything failed
  if (finalItems.length === 0 && queries.length > 0) process.exitCode = 2;
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
