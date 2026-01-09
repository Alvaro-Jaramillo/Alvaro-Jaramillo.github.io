const els = {
  list: document.getElementById("list"),
  empty: document.getElementById("empty"),
  error: document.getElementById("error"),
  count: document.getElementById("count"),
  updatedAt: document.getElementById("updatedAt"),
  statusPill: document.getElementById("statusPill"),
  search: document.getElementById("search"),
  sourceFilter: document.getElementById("sourceFilter"),
  tagFilter: document.getElementById("tagFilter"),
  signalFilter: document.getElementById("signalFilter"),
  clearBtn: document.getElementById("clearBtn"),
  countryFilter: document.getElementById("countryFilter"),
  companyPanel: document.getElementById("companyPanel"),
  companyList: document.getElementById("companyList"),
  companyCount: document.getElementById("companyCount"),
};

let allItems = [];
let filtered = [];
let companyFilter = "";

const companyStopWords = new Set([
  "news",
  "how",
  "analysis",
  "report",
  "review",
  "guide",
  "overview",
  "latest",
  "today",
  "update",
  "updates"
]);

const canadaHints = [
  "canada",
  "canadian",
  "ontario",
  "quebec",
  "british columbia",
  "alberta",
  "manitoba",
  "saskatchewan",
  "nova scotia",
  "new brunswick",
  "newfoundland",
  "labrador",
  "prince edward island",
  "pei",
  "yukon",
  "nunavut",
  "northwest territories"
];

const usaHints = [
  "usa",
  "u.s.",
  "u.s.a.",
  "united states",
  "american"
];

const otherHints = [
  "united kingdom",
  "uk",
  "england",
  "scotland",
  "wales",
  "ireland",
  "germany",
  "france",
  "spain",
  "italy",
  "netherlands",
  "sweden",
  "norway",
  "denmark",
  "finland",
  "australia",
  "new zealand",
  "india",
  "china",
  "japan",
  "korea",
  "mexico",
  "brazil",
  "singapore"
];

function norm(s) {
  return (s || "").toString().toLowerCase().trim();
}

function getCompanyKey(raw) {
  return norm(raw);
}

function isGeneralCompany(raw) {
  const cleaned = (raw || "").toString().trim();
  if (!cleaned) return true;
  const key = getCompanyKey(cleaned);
  if (!key) return true;
  if (companyStopWords.has(key)) return true;
  const words = cleaned.split(/\s+/);
  if (words.length > 6) return true;
  if (cleaned.length > 60) return true;
  return false;
}

function deriveCountry(item) {
  const text = norm([item.title, item.summary, item.company_guess].filter(Boolean).join(" "));
  if (!text) return "USA";
  if (canadaHints.some(h => text.includes(h))) return "Canada";
  if (otherHints.some(h => text.includes(h))) return "Other";
  if (usaHints.some(h => text.includes(h))) return "USA";
  return "USA";
}

function getSelectedCountries() {
  if (!els.countryFilter) return [];
  return [...els.countryFilter.querySelectorAll("input[type=\"checkbox\"]:checked")].map(input => input.value);
}

function resetCountries() {
  if (!els.countryFilter) return;
  const inputs = els.countryFilter.querySelectorAll("input[type=\"checkbox\"]");
  inputs.forEach(input => {
    input.checked = input.value === "USA" || input.value === "Canada";
  });
}

function formatDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString(undefined, { year: "numeric", month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function buildOptions(selectEl, values, placeholder) {
  const current = selectEl.value;
  selectEl.innerHTML = "";
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = placeholder;
  selectEl.appendChild(opt0);

  values.forEach(v => {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    selectEl.appendChild(opt);
  });

  if ([...selectEl.options].some(o => o.value === current)) selectEl.value = current;
}

function applyFilters() {
  const q = norm(els.search.value);
  const source = els.sourceFilter.value;
  const tag = els.tagFilter.value;
  const sig = els.signalFilter.value;
  const countries = getSelectedCountries();

  filtered = allItems.filter(item => {
    if (source && item.source !== source) return false;
    if (tag && !(item.tags || []).includes(tag)) return false;
    if (sig) {
      const bucket = (item.signal_bucket || "").toLowerCase();
      if (bucket !== sig) return false;
    }
    if (countries.length && !countries.includes(deriveCountry(item))) return false;

    if (!q) return true;
    const hay = norm([
      item.title,
      item.summary,
      item.source,
      item.company_guess,
      (item.tags || []).join(" "),
      (item.matched_queries || []).join(" ")
    ].join(" "));
    return hay.includes(q);
  });

  if (companyFilter) {
    if (companyFilter === "__general__") {
      filtered = filtered.filter(item => isGeneralCompany(item.company_guess));
    } else {
      filtered = filtered.filter(item => getCompanyKey(item.company_guess) === companyFilter);
    }
  }

  render();
}

function render() {
  els.list.innerHTML = "";
  els.count.textContent = filtered.length.toLocaleString();
  els.empty.classList.toggle("hidden", filtered.length !== 0);
  els.companyList.innerHTML = "";

  for (const item of filtered) {
    const card = document.createElement("div");
    card.className = "card";

    const top = document.createElement("div");
    top.className = "card-top";

    const title = document.createElement("h3");
    const link = document.createElement("a");
    link.href = item.url;
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = item.title || item.url;
    title.appendChild(link);

    const dateChip = document.createElement("div");
    dateChip.className = "chip";
    dateChip.innerHTML = `<strong>${formatDate(item.published_at)}</strong>`;

    top.appendChild(title);
    top.appendChild(dateChip);

    const sub = document.createElement("div");
    sub.className = "sub";
    sub.textContent = item.summary || "";

    const chips = document.createElement("div");
    chips.className = "chips";

    // Signal badge
    const sig = (item.signal_bucket || "low").toLowerCase();
    const sigChip = document.createElement("div");
    sigChip.className = "chip " + (sig === "hot" ? "badge-hot" : sig === "medium" ? "badge-med" : "badge-low");
    sigChip.innerHTML = `Signal: <strong>${sig}</strong> (${item.signal_score ?? 0})`;
    chips.appendChild(sigChip);

    const sourceChip = document.createElement("div");
    sourceChip.className = "chip";
    sourceChip.innerHTML = `Source: <strong>${item.source || "—"}</strong>`;
    chips.appendChild(sourceChip);

    if (item.company_guess) {
      const c = document.createElement("div");
      c.className = "chip";
      c.innerHTML = `Company: <strong>${item.company_guess}</strong>`;
      chips.appendChild(c);
    }

    if (item.tags && item.tags.length) {
      for (const t of item.tags.slice(0, 6)) {
        const c = document.createElement("div");
        c.className = "chip";
        c.textContent = t;
        chips.appendChild(c);
      }
    }

    card.appendChild(top);
    if (item.summary) card.appendChild(sub);
    card.appendChild(chips);
    els.list.appendChild(card);
  }

  const companyMap = new Map();
  let generalCount = 0;
  for (const item of filtered) {
    const raw = (item.company_guess || "").toString().trim();
    if (isGeneralCompany(raw)) {
      generalCount += 1;
      continue;
    }
    const key = getCompanyKey(raw);
    const entry = companyMap.get(key) || { name: raw, count: 0 };
    entry.count += 1;
    companyMap.set(key, entry);
  }

  const companyStats = [...companyMap.values()].sort((a, b) => {
    if (b.count !== a.count) return b.count - a.count;
    return a.name.localeCompare(b.name);
  });

  els.companyCount.textContent = companyStats.length.toLocaleString();
  const allBtn = document.createElement("button");
  allBtn.type = "button";
  allBtn.className = "company-item" + (companyFilter === "" ? " is-active" : "");
  allBtn.innerHTML = `<span>All results</span><span class="company-count">${filtered.length.toLocaleString()}</span>`;
  allBtn.addEventListener("click", () => {
    companyFilter = "";
    applyFilters();
  });
  els.companyList.appendChild(allBtn);

  const generalBtn = document.createElement("button");
  generalBtn.type = "button";
  generalBtn.className = "company-item" + (companyFilter === "__general__" ? " is-active" : "");
  generalBtn.innerHTML = `<span>General news</span><span class="company-count">${generalCount.toLocaleString()}</span>`;
  if (generalCount === 0) {
    generalBtn.disabled = true;
  } else {
    generalBtn.addEventListener("click", () => {
      companyFilter = "__general__";
      applyFilters();
    });
  }
  els.companyList.appendChild(generalBtn);

  if (companyStats.length === 0) {
    const empty = document.createElement("div");
    empty.className = "company-empty";
    empty.textContent = "No companies yet.";
    els.companyList.appendChild(empty);
  } else {
    for (const company of companyStats) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "company-item" + (companyFilter === getCompanyKey(company.name) ? " is-active" : "");
      btn.innerHTML = `<span>${company.name}</span><span class="company-count">${company.count.toLocaleString()}</span>`;
      btn.addEventListener("click", () => {
        companyFilter = getCompanyKey(company.name);
        applyFilters();
      });
      els.companyList.appendChild(btn);
    }
  }
}

async function load() {
  els.statusPill.textContent = "Loading…";
  try {
    const res = await fetch(`data/links.json?ts=${Date.now()}`, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    allItems = (data.items || []).sort((a, b) => {
      const da = new Date(a.published_at || 0).getTime();
      const db = new Date(b.published_at || 0).getTime();
      return db - da;
    });

    const sources = [...new Set(allItems.map(i => i.source).filter(Boolean))].sort();
    const tags = [...new Set(allItems.flatMap(i => i.tags || []).filter(Boolean))].sort();

    buildOptions(els.sourceFilter, sources, "All sources");
    buildOptions(els.tagFilter, tags, "All tags");

    els.updatedAt.textContent = formatDate(data.generated_at);
    els.statusPill.textContent = `Loaded ${allItems.length.toLocaleString()}`;

    els.error.classList.add("hidden");
    applyFilters();
  } catch (e) {
    console.error(e);
    els.statusPill.textContent = "Failed to load";
    els.error.classList.remove("hidden");
    els.updatedAt.textContent = "—";
    allItems = [];
    filtered = [];
    render();
  }
}

els.search.addEventListener("input", applyFilters);
els.sourceFilter.addEventListener("change", applyFilters);
els.tagFilter.addEventListener("change", applyFilters);
els.signalFilter.addEventListener("change", applyFilters);
els.countryFilter?.addEventListener("change", applyFilters);
els.clearBtn.addEventListener("click", () => {
  els.search.value = "";
  els.sourceFilter.value = "";
  els.tagFilter.value = "";
  els.signalFilter.value = "";
  companyFilter = "";
  resetCountries();
  applyFilters();
});

load();
