/* app.js – Gold-Kauf-Signal
 * Korrekte Spot-Einheit (USD/oz) + Portfolio-Wert = gramm * usd_per_gram
 */

const OZ_IN_GRAM = 31.1034768;

const state = {
  spot: null,            // aus spot.json
  history: null,         // aus history.json
  diagnostics: [],
  portfolio: {
    grams: Number(localStorage.getItem("portfolio_grams") || "0"),
    valueUSD: 0
  }
};

// ----------------------- Utilities -----------------------

function fmt(n, digits = 2) {
  if (n === null || n === undefined || isNaN(n)) return "—";
  return new Intl.NumberFormat("de-DE", { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(n);
}

function diag(msg) {
  const t = new Date().toTimeString().slice(0,8);
  state.diagnostics.push(`[${t}] ${msg}`);
  const el = document.getElementById("diag");
  if (el) el.textContent = state.diagnostics.join("\n");
}

// ----------------------- Data Load -----------------------

async function fetchJSON(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  return await res.json();
}

async function loadData() {
  diag("Preflight startet …");
  try {
    const [spot, history] = await Promise.all([
      fetchJSON("data/spot.json"),
      fetchJSON("data/history.json").catch(() => ({ history: [] }))
    ]);
    state.spot = spot;
    state.history = history;
    diag("fetching json…");
    diag(`loaded: rows=${(history.history||[]).length}, goldPoints=${(history.history||[]).length}`);
  } catch (e) {
    diag("fetch error: " + e.message);
  }
}

// ----------------------- Spot & Portfolio -----------------------

function currentUsdPerOunce() {
  if (!state.spot) return null;
  // Priorität: neues Feld, fallback Legacy XAUUSD
  const val = state.spot.usd_per_ounce ?? state.spot.XAUUSD ?? null;
  return (typeof val === "number" && !isNaN(val)) ? val : null;
}
function currentUsdPerGram() {
  if (!state.spot) return null;
  if (typeof state.spot.usd_per_gram === "number") return state.spot.usd_per_gram;
  const oz = currentUsdPerOunce();
  return oz ? (oz / OZ_IN_GRAM) : null;
}
function currentUsdPerKg() {
  const g = currentUsdPerGram();
  return g ? g * 1000.0 : null;
}

function renderSpotHeader() {
  const el = document.getElementById("spotHeader");
  if (!el) return;
  const oz = currentUsdPerOunce();
  const perKg = currentUsdPerKg();
  const date = state.spot?.spot_date || "—";
  const src = state.spot?.source || "stooq";
  el.innerHTML = `
    <div>Spot: <strong>${fmt(oz, 2)} USD/oz</strong> <span class="muted">(≈ ${fmt(perKg,0)} USD/kg)</span></div>
    <div class="muted">Quelle: ${src}, Datum: ${date || "—"}</div>
  `;
}

function bindPortfolio() {
  const input = document.getElementById("portfolioGrams");
  const out = document.getElementById("portfolioValue");
  if (!input || !out) return;

  const recompute = () => {
    const grams = Number(input.value || "0");
    state.portfolio.grams = isNaN(grams) ? 0 : grams;
    localStorage.setItem("portfolio_grams", String(state.portfolio.grams));
    const usd_g = currentUsdPerGram();
    const val = (usd_g ? usd_g * state.portfolio.grams : null);
    state.portfolio.valueUSD = val || 0;
    out.textContent = (val === null) ? "—" : `${fmt(val, 2)} USD`;
  };

  input.value = String(state.portfolio.grams || 0);
  input.addEventListener("input", recompute);
  recompute();
}

// ----------------------- Render (minimal) -----------------------

function renderSystemCard() {
  const el = document.getElementById("systemStatus");
  if (!el) return;
  const len = (state.history?.history || []).length;
  const spotOZ = currentUsdPerOunce();
  el.innerHTML = `
    <div><strong>Preflight OK</strong></div>
    <div>history.len=${len} • spot(USD/oz)=${fmt(spotOZ,2)}</div>
  `;
}

async function main() {
  await loadData();
  renderSpotHeader();
  bindPortfolio();
  renderSystemCard();
  diag("render complete.");
}

document.addEventListener("DOMContentLoaded", main);
