// js/spot_patch_v1.js
// Nicht-invasives Patch für v1.0: korrekter Spot (USD/oz) & Portfolio (Gramm×USD/g)

(function () {
  const OZ_IN_GRAM = 31.1034768;

  function fmt(n, d = 2) {
    if (n === null || n === undefined || isNaN(n)) return "—";
    return new Intl.NumberFormat("de-DE", {
      minimumFractionDigits: d,
      maximumFractionDigits: d
    }).format(n);
  }

  async function getJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error(`${url} ${r.status}`);
    return r.json();
  }

  function currentUsdPerOunce(spot) {
    const v = spot.usd_per_ounce ?? spot.XAUUSD ?? null;
    return (typeof v === "number" && !isNaN(v)) ? v : null;
  }
  function usdPerGram(spot) {
    if (typeof spot.usd_per_gram === "number") return spot.usd_per_gram;
    const oz = currentUsdPerOunce(spot);
    return oz ? (oz / OZ_IN_GRAM) : null;
  }
  function usdPerKg(spot) {
    const g = usdPerGram(spot);
    return g ? g * 1000.0 : null;
  }

  function enhanceSpotHeader(spot) {
    // Falls deine v1.0 einen Spot-Platzhalter hat (z. B. #spotHeader), schreiben wir dort rein.
    // Sonst bleiben wir still.
    const el = document.querySelector("#spotHeader");
    if (!el) return;
    const oz = currentUsdPerOunce(spot);
    const perKg = usdPerKg(spot);
    el.innerHTML = `
      <div>Spot: <strong>${fmt(oz,2)} USD/oz</strong> <span class="muted">(≈ ${fmt(perKg,0)} USD/kg)</span></div>
      <div class="muted">Quelle: ${spot.source || "stooq"}, Datum: ${spot.spot_date || "—"}</div>
    `;
  }

  function enhancePortfolio(spot) {
    // Erwartete IDs aus v1.0: #portfolioGrams (input number) und #portfolioValue (span)
    const inGrams = document.querySelector("#portfolioGrams");
    const outVal  = document.querySelector("#portfolioValue");
    if (!inGrams || !outVal) return;

    const recompute = () => {
      const grams = Number(inGrams.value || "0");
      localStorage.setItem("portfolio_grams", String(grams));
      const gPrice = usdPerGram(spot);
      const val = (gPrice ? grams * gPrice : null);
      outVal.textContent = (val === null) ? "—" : `${fmt(val, 2)} USD`;
    };

    const saved = Number(localStorage.getItem("portfolio_grams") || "0");
    if (!inGrams.value) inGrams.value = String(isNaN(saved) ? 0 : saved);
    inGrams.addEventListener("input", recompute);
    recompute();
  }

  function note(msg) {
    const d = document.querySelector("#diag");
    if (d) d.textContent += `\n[spot_patch] ${msg}`;
  }

  async function run() {
    try {
      const spot = await getJSON("data/spot.json");
      enhanceSpotHeader(spot);
      enhancePortfolio(spot);
      note(`spot(USD/oz)=${fmt(currentUsdPerOunce(spot),2)} ok`);
    } catch (e) {
      note(`failed: ${e.message}`);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();
