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
};

let allItems = [];
let filtered = [];

function norm(s) {
  return (s || "").toString().toLowerCase().trim();
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

  filtered = allItems.filter(item => {
    if (source && item.source !== source) return false;
    if (tag && !(item.tags || []).includes(tag)) return false;
    if (sig) {
      const bucket = (item.signal_bucket || "").toLowerCase();
      if (bucket !== sig) return false;
    }

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

  render();
}

function render() {
  els.list.innerHTML = "";
  els.count.textContent = filtered.length.toLocaleString();
  els.empty.classList.toggle("hidden", filtered.length !== 0);

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
els.clearBtn.addEventListener("click", () => {
  els.search.value = "";
  els.sourceFilter.value = "";
  els.tagFilter.value = "";
  els.signalFilter.value = "";
  applyFilters();
});

load();
