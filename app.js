/* =========================================================================
   Gold-Kauf-Signal – App-Logik (Client-seitig)
   ========================================================================= */

/* ----------------------------- Utilities -------------------------------- */

function uiLog(msg) {
  const el = document.getElementById('diag');
  if (!el) return;
  const now = new Date().toISOString().slice(11, 19);
  el.textContent = (el.textContent ? el.textContent + "\n" : "") + `[${now}] ${msg}`;
}

const nowBust = () => '?t=' + Date.now();
const fmtUSD = (n) =>
  new Intl.NumberFormat('de-DE', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(n);
const fmtEUR = (n) =>
  new Intl.NumberFormat('de-DE', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(n);

const DRIVER_KEYS = [
  "DFII10", "DTWEXBGS", "VIXCLS", "DCOILBRENTEU", "T10YIE",
  "BAMLH0A0HYM2", "NAPM", "RECPROUSM156N", "T10Y2Y"
];

const OZ_PER_KG = 32.1507465686;

// requestIdleCallback Polyfill (für iOS/Safari)
window.requestIdleCallback = window.requestIdleCallback || function (cb) {
  return setTimeout(() => cb({ timeRemaining: () => 50, didTimeout: false }), 1);
};
window.cancelIdleCallback = window.cancelIdleCallback || function (id) { clearTimeout(id); };

// Fetch mit Timeout
async function fetchJSON(url, ms = 7000) {
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, { signal: ctrl.signal });
    return await r.json();
  } finally {
    clearTimeout(to);
  }
}

/* ----------------------------- Konfiguration ----------------------------- */

const FREQ = {
  DFII10: { type: "daily", steps: 10, label: "/10T" },
  DTWEXBGS: { type: "daily", steps: 10, label: "/10T" },
  VIXCLS: { type: "daily", steps: 10, label: "/10T" },
  DCOILBRENTEU: { type: "daily", steps: 10, label: "/10T" },
  T10YIE: { type: "daily", steps: 10, label: "/10T" },
  BAMLH0A0HYM2: { type: "daily", steps: 10, label: "/10T" },
  NAPM: { type: "monthly", steps: 1, label: "/1M" },
  RECPROUSM156N: { type: "monthly", steps: 1, label: "/1M" },
  T10Y2Y: { type: "daily", steps: 10, label: "/10T" },
};

const GROUPS = {
  rates: ["DFII10", "T10YIE", "T10Y2Y"],
  risk: ["DTWEXBGS", "VIXCLS", "BAMLH0A0HYM2", "RECPROUSM156N"],
  real: ["DCOILBRENTEU", "NAPM"]
};

const LABELS = {
  DFII10: "Realzinsen (Zinskosten)",
  DTWEXBGS: "US-Dollar (Dollar-Stärke)",
  VIXCLS: "VIX (Marktstress)",
  DCOILBRENTEU: "Ölpreis (Inflationstreiber)",
  T10YIE: "Inflationserwartung",
  BAMLH0A0HYM2: "HY-Spreads",
  NAPM: "PMI",
  RECPROUSM156N: "Rezessionsrisiko",
  T10Y2Y: "Zinskurve 10y–2y"
};

/* ----------------------------- Bewertung -------------------------------- */

function assessDrivers(t) {
  const def = (val, betterLow) => {
    if (val == null || !Number.isFinite(val)) return { status: "neutral", msg: "Neutral (keine Daten)" };
    if (betterLow) {
      if (val <= 0) return { status: "green", msg: "Gut für deinen Goldpreis" };
      if (val < 1) return { status: "yellow", msg: "Eher neutral" };
      return { status: "red", msg: "Schlecht für deinen Goldpreis" };
    } else {
      if (val >= 0) return { status: "green", msg: "Gut für deinen Goldpreis" };
      if (val > -1) return { status: "yellow", msg: "Eher neutral" };
      return { status: "red", msg: "Schlecht für deinen Goldpreis" };
    }
  };
  return {
    DFII10: def(t.DFII10, true),
    DTWEXBGS: def(t.DTWEXBGS, true),
    VIXCLS: def(t.VIXCLS, false),
    DCOILBRENTEU: def(-t.DCOILBRENTEU, true),
    T10YIE: def(t.T10YIE, false),
    BAMLH0A0HYM2: def(t.BAMLH0A0HYM2, false),
    NAPM: def(-t.NAPM, true),
    RECPROUSM156N: def(t.RECPROUSM156N, false),
    T10Y2Y: def(-t.T10Y2Y, true)
  };
}

function summarize(drvs) {
  const map = { green: 2, yellow: 1, red: -2, neutral: 0 };
  let s = 0, n = 0;
  for (const k in drvs) { s += map[drvs[k].status]; n++; }
  const avg = s / Math.max(1, n);
  if (avg >= 1) return { overall: "green", text: "In Summe eher positiv." };
  if (avg <= -1) return { overall: "red", text: "In Summe eher negativ." };
  return { overall: "yellow", text: "In Summe eher neutral." };
}

function recommendation(overall, momentum) {
  const score = (overall === "green" ? 2 : overall === "red" ? -2 : 0) + (momentum >= 0 ? 1 : -1);
  if (score >= 2) return { status: "green", text: "Kaufen" };
  if (score <= -2) return { status: "red", text: "Nicht kaufen" };
  return { status: "yellow", text: "Abwarten" };
}

/* ----------------------------- Forecast ---------------------------------- */

function forecast(series, horizonDays) {
  if (!series || series.length < 90) return { median: null, lo: null, hi: null };
  const sorted = [...series].sort((a, b) => new Date(a.date) - new Date(b.date));
  const px = sorted.map(d => d.price).filter(v => Number.isFinite(v) && v > 0);
  if (px.length < 90) return { median: null, lo: null, hi: null };
  const logR = [];
  for (let i = 1; i < px.length; i++) logR.push(Math.log(px[i] / px[i - 1]));
  const w = Math.min(60, logR.length), tail = logR.slice(-w);
  const mu = tail.reduce((a, b) => a + b, 0) / Math.max(1, tail.length);
  const sigma = Math.sqrt(tail.reduce((a, b) => a + (b - mu) * (b - mu), 0) / Math.max(1, tail.length));
  const last = px[px.length - 1];
  const steps = Math.max(1, Math.round(horizonDays));
  const med = last * Math.exp(mu * steps);
  const lo = last * Math.exp((mu - 1.64 * sigma) * steps);
  const hi = last * Math.exp((mu + 1.64 * sigma) * steps);
  return { median: med, lo, hi };
}

/* -------------------- Historischer Vergleich (Similarity) ---------------- */

function cosineOverlap(a, b) {
  let num = 0, na = 0, nb = 0, c = 0;
  for (const k of DRIVER_KEYS) {
    const va = a[k], vb = b[k];
    if (va == null || !Number.isFinite(va) || vb == null || !Number.isFinite(vb)) continue;
    num += va * vb; na += va * va; nb += vb * vb; c++;
  }
  if (c === 0) return -1;
  const den = Math.sqrt(na) * Math.sqrt(nb);
  return den > 0 ? num / den : -1;
}

function zscoreVector(vec, stats) {
  const out = {};
  for (const k of DRIVER_KEYS) {
    const v = vec[k];
    const m = stats[k]?.mean ?? 0;
    const s = stats[k]?.std ?? 1e-9;
    out[k] = (v == null || !Number.isFinite(v)) ? null : (v - m) / (s || 1e-9);
  }
  return out;
}

/* -------------------- Zeitreihen + Deltas (stabil) ----------------------- */

function buildSeriesMap(rows) {
  const map = {};
  for (const k of DRIVER_KEYS) {
    const arr = rows
      .map(r => ({ date: r.date, value: r[k] }))
      .filter(o => Number.isFinite(o.value))
      .sort((a, b) => new Date(a.date) - new Date(b.date));
    map[k] = arr;
  }
  return map;
}

function currentDeltaFromSeries(seriesArr, steps) {
  if (!seriesArr || seriesArr.length < steps + 1) return null;
  const cur = seriesArr[seriesArr.length - 1].value;
  const past = seriesArr[seriesArr.length - 1 - steps].value;
  const tiny = 1e-9;
  if (Math.abs(cur) < tiny || Math.abs(past) < tiny) return null;
  return (cur - past) / Math.abs(past);
}

function currentDeltaMonthly(seriesArr) {
  if (!seriesArr || seriesArr.length < 2) return null;
  const cur = seriesArr[seriesArr.length - 1].value;
  const past = seriesArr[seriesArr.length - 2].value;
  const tiny = 1e-9;
  if (Math.abs(cur) < tiny || Math.abs(past) < tiny) return null;
  return (cur - past) / Math.abs(past);
}

function dateToSeriesIndex(seriesArr) {
  const dates = seriesArr.map(x => x.date);
  return function (dateStr) {
    if (!dates.length) return -1;
    let lo = 0, hi = dates.length - 1, ans = -1;
    const target = new Date(dateStr).getTime();
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const t = new Date(dates[mid]).getTime();
      if (t <= target) { ans = mid; lo = mid + 1; } else { hi = mid - 1; }
    }
    return ans;
  };
}

function historicalDeltaAtDate(seriesArr, idxFinder, steps, dateStr, isMonthly) {
  if (!seriesArr || !seriesArr.length) return null;
  const j = idxFinder(dateStr);
  if (j < 0) return null;
  const jPast = j - (isMonthly ? 1 : steps);
  if (jPast < 0) return null;
  const cur = seriesArr[j].value, past = seriesArr[jPast].value;
  const tiny = 1e-9;
  if (Math.abs(cur) < tiny || Math.abs(past) < tiny) return null;
  return (cur - past) / Math.abs(past);
}

/* ----------------------------- Systemstatus ------------------------------ */

function renderSystemFromPreflight() {
  const el = document.getElementById('system-info');
  if (!el) return;
  const pf = window.PREFLIGHT;
  if (!pf) { el.textContent = "Datenquelle: FRED, stooq"; return; }
  const hasErr = Array.isArray(pf.errors) && pf.errors.length > 0;
  const hasWarn = Array.isArray(pf.warnings) && pf.warnings.length > 0;
  const badgeClass = hasErr ? "red" : hasWarn ? "yellow" : "green";
  const head = hasErr ? "Preflight FAIL" : hasWarn ? "Preflight WARN" : "Preflight OK";
  const h = pf.stats?.history || {}; const s = pf.stats?.spot || {}; const v = pf.stats?.vendors || {};
  let parts = [];
  if (h.start && h.end) parts.push(`Zeitraum: ${h.start} – ${h.end}`);
  if (h.end) {
    const ageD = Math.floor((Date.now() - Date.parse(h.end + "T00:00:00Z")) / 86400000);
    parts.push(`Letzter History-Tag: ${h.end} (${ageD}T alt)`);
  }
  if (s.hasValue) parts.push(`Spot: vorhanden`);
  else parts.push(`Spot: —`);
  if (v.domains != null) parts.push(`Vendors: D=${v.domains} P=${v.pages ?? 0} Prod=${v.products ?? 0} Items=${v.items ?? 0}`);
  el.innerHTML = `<span class="badge ${badgeClass}">${head}</span> • ${parts.join(" • ")}`;
}

/* ----------------------------- Gruppendot -------------------------------- */

function setGroupDot(id, status) {
  const el = document.getElementById(id);
  if (!el) return;
  el.className = `dot ${status}`;
  el.setAttribute('aria-label', `Gruppenstatus: ${status}`);
}
function computeGroupStatus(keys, assess) {
  const map = { green: 2, yellow: 1, red: -2, neutral: 0 };
  let s = 0, n = 0;
  keys.forEach(k => { const st = assess[k]?.status || 'neutral'; s += map[st]; n++; });
  const avg = s / Math.max(1, n);
  if (avg >= 1) return 'green';
  if (avg <= -1) return 'red';
  return 'yellow';
}

/* ----------------------------- Tooltips ---------------------------------- */

let openTip = null;

function ensureTooltipEl() {
  let el = document.getElementById('tooltip');
  if (!el) {
    el = document.createElement('div');
    el.id = 'tooltip';
    el.className = 'tooltip';
    el.setAttribute('role', 'dialog');
    el.setAttribute('aria-live', 'polite');
    document.body.appendChild(el);
  }
  return el;
}
function closeTooltip() {
  if (!openTip) return;
  const { btn, el } = openTip;
  btn.setAttribute('aria-expanded', 'false');
  el.style.display = 'none';
  openTip = null;
}
function openTooltip(btn, text) {
  const el = ensureTooltipEl();
  el.textContent = text;
  el.style.display = 'block';

  const r = btn.getBoundingClientRect();
  const margin = 8;
  const vw = Math.max(document.documentElement.clientWidth, window.innerWidth || 0);

  el.style.left = '0px'; el.style.top = '0px';
  const ew = el.offsetWidth, eh = el.offsetHeight;

  let x = Math.max(8, Math.min(vw - ew - 8, r.left));
  let y = r.top - eh - margin;
  let pos = 'top';
  if (y < 8) { y = r.bottom + margin; pos = 'bottom'; }
  el.style.left = `${x}px`;
  el.style.top = `${y}px`;
  el.setAttribute('data-pos', pos);

  btn.setAttribute('aria-expanded', 'true');
  openTip = { btn, el };
}

document.addEventListener('click', (ev) => {
  const target = ev.target.closest('.info');
  if (target) {
    ev.stopPropagation();
    const txt = target.getAttribute('data-tip') || '';
    if (openTip && openTip.btn === target) { closeTooltip(); return; }
    closeTooltip();
    openTooltip(target, txt);
  } else {
    closeTooltip();
  }
});
document.addEventListener('keydown', (ev) => { if (ev.key === 'Escape') closeTooltip(); });
document.addEventListener('scroll', closeTooltip, { passive: true });

function tooltipFor(key, assess, freqCfg) {
  const a = assess[key];
  const status = a?.status || 'neutral';
  const msg = a?.msg || '—';
  const rule =
    key === 'DFII10' || key === 'DTWEXBGS' || key === 'DCOILBRENTEU' || key === 'T10Y2Y' || key === 'NAPM'
      ? 'Regel: Niedriger ist besser.'
      : 'Regel: Höher ist besser.';
  const win = freqCfg?.label ? `Fenster: Änderung ${freqCfg.label}.` : '';
  const statusTxt =
    status === 'green' ? 'Status: Gut (Grün).' :
      status === 'yellow' ? 'Status: Eher neutral (Gelb).' :
        status === 'red' ? 'Status: Schlecht (Rot).' :
          'Status: Neutral.';
  return `${rule} ${win} ${statusTxt} ${msg}`;
}

/* ----------------------------- Portfolio --------------------------------- */

let SPOT_USD_PER_KG = null; // aus spot.json
let FIX_USD_PER_OZ = null;  // Fallback LBMA-Fix (aus history)

function resolveUsdPerKg() {
  if (Number.isFinite(SPOT_USD_PER_KG)) return { value: SPOT_USD_PER_KG, source: 'Spot' };
  if (Number.isFinite(FIX_USD_PER_OZ)) return { value: FIX_USD_PER_OZ * OZ_PER_KG, source: 'LBMA-Fix' };
  return { value: null, source: '—' };
}

function updateHoldingsValue() {
  const input = document.getElementById('holdings-grams');
  const out = document.getElementById('holdings-value');
  const note = document.getElementById('holdings-note');
  if (!input || !out) return;
  const grams = Number(input.value);
  const { value: usdPerKg, source } = resolveUsdPerKg();
  if (!Number.isFinite(grams) || grams <= 0 || !Number.isFinite(usdPerKg)) {
    out.className = 'badge neutral'; out.textContent = '—';
    if (note) note.textContent = `Preisquelle: ${source === '—' ? '—' : source}.`;
    return;
  }
  const usd = usdPerKg * (grams / 1000);
  out.className = 'badge green'; out.textContent = fmtUSD(usd);
  if (note) note.textContent = `Preisquelle: ${source}.`;
}

/* ----------------------------- Vendors (Seriöse Händler) ----------------- */

function premiumBadgeClass(prem) {
  if (!Number.isFinite(prem)) return 'neutral';
  if (prem <= 0.03) return 'green';
  if (prem <= 0.06) return 'yellow';
  return 'red';
}

async function renderVendorsCard() {
  const list = document.getElementById('vendors-list');
  const note = document.getElementById('vendors-note');
  const sel = document.getElementById('vendor-product');
  if (!list || !note || !sel) return;

  async function loadAndRender() {
    list.innerHTML = '<div class="driver skeleton"><div class="skeleton-bar"></div></div>';
    try {
      const vend = await fetch('data/vendors_auto.json?t=' + Date.now()).then(r => r.json());
      const product = sel.value;
      const items = [];

      (vend.vendors || []).forEach(v => {
        (v.items || []).forEach(it => {
          if (it.product === product) {
            items.push({
              domain: v.domain,
              trust: v.trust,
              name: it.name,
              price: it.price?.value ?? null,
              currency: it.price?.currency ?? 'EUR',
              prem: it.premium,
              url: it.url,
              checked_at: it.checked_at,
              shipping_included: it.price?.shipping_included ?? null
            });
          }
        });
      });

      // sort: primär Premium (niedrig), sekundär Trust (hoch)
      items.sort((a, b) => {
        const pa = Number.isFinite(a.prem) ? a.prem : 99;
        const pb = Number.isFinite(b.prem) ? b.prem : 99;
        if (pa !== pb) return pa - pb;
        return (b.trust || 0) - (a.trust || 0);
      });

      list.innerHTML = '';
      if (!items.length) {
        const div = document.createElement('div');
        div.className = 'vendor-card';
        div.innerHTML = `<div><strong>Keine Angebote gefunden</strong><div class="info-line">Wir konnten für dieses Produkt aktuell keinen Preis aus strukturierten Shop-Daten lesen.</div></div>`;
        list.appendChild(div);
      } else {
        items.forEach(it => {
          const div = document.createElement('div');
          const cls = premiumBadgeClass(it.prem);
          const premTxt = Number.isFinite(it.prem) ? ((it.prem * 100).toFixed(1) + '%') : '—';
          const ship = it.shipping_included === true ? 'inkl. Versand' : it.shipping_included === false ? 'zzgl. Versand' : '';
          div.className = 'vendor-card';
          div.innerHTML = `
            <div>
              <strong>${it.domain}</strong>
              <div class="info-line">${it.name || ''}</div>
              <div class="info-line">Preis: ${Number.isFinite(it.price) ? fmtEUR(it.price) : '—'} ${ship ? '• ' + ship : ''}</div>
            </div>
            <div class="vendor-actions">
              <span class="badge prem ${cls}" title="Aufschlag ggü. Spot">${premTxt}</span>
              <a class="btn" href="${it.url}" target="_blank" rel="noopener">Zum Händler</a>
            </div>`;
          list.appendChild(div);
        });
      }

      // Fußnote mit Diagnostics (Fix: keine Mischung von ?? und || ohne Klammern)
      const totals = vend.diagnostics?.totals || {};
      const gen = vend.generated ? new Date(vend.generated).toLocaleString('de-DE') : '—';
      let domains = totals.domains;
      if (!domains) {
        domains = ((vend.vendors && vend.vendors.length) ? vend.vendors.length : 0);
      }
      const statsLine = `Domains=${domains} • Seiten=${totals.pages ?? 0} • Produkte=${totals.products ?? 0} • Items=${totals.items ?? 0}`;
      note.textContent = `Stand: ${gen} • ${statsLine} • Quellen: strukturierte Shop-Daten (JSON-LD/Microdata/RDFa)`;
    } catch (e) {
      list.innerHTML = '';
      const div = document.createElement('div');
      div.className = 'vendor-card';
      div.innerHTML = `<div><strong>Händlerdaten nicht verfügbar</strong><div class="info-line">vendors_auto.json konnte nicht geladen werden.</div></div>`;
      list.appendChild(div);
      note.textContent = '—';
    }
  }

  sel.addEventListener('change', loadAndRender);
  await loadAndRender();
}

/* ----------------------------- App-Start --------------------------------- */

(async function () {
  try {
    uiLog('fetching json…');

    // Systemstatus aus Preflight
    renderSystemFromPreflight();
    setTimeout(renderSystemFromPreflight, 300);

    // Daten laden
    const [hist, spot] = await Promise.all([
      fetchJSON('data/history.json' + nowBust()).catch(() => ({ history: [] })),
      fetchJSON('data/spot.json' + nowBust()).catch(() => ({ XAUUSD: null, timestamp: null }))
    ]);

    const spotVal = (spot && spot.XAUUSD != null) ? Number(spot.XAUUSD) : null;
    SPOT_USD_PER_KG = Number.isFinite(spotVal) ? spotVal : null;
    const spotline = document.getElementById('spotline');
    if (spotline) spotline.textContent = `Spot: ${Number.isFinite(spotVal) ? spotVal.toFixed(2) + ' USD/kg' : '—'}`;

    // History aufbereiten
    const rows = (hist.history || []).map(r => ({
      date: r.timestamp,
      GOLD: (r.GOLDAMGBD228NLBM ?? null),
      DFII10: r.DFII10 ?? null,
      DTWEXBGS: r.DTWEXBGS ?? null,
      VIXCLS: r.VIXCLS ?? null,
      DCOILBRENTEU: r.DCOILBRENTEU ?? null,
      T10YIE: r.T10YIE ?? null,
      BAMLH0A0HYM2: r.BAMLH0A0HYM2 ?? null,
      NAPM: r.NAPM ?? null,
      RECPROUSM156N: r.RECPROUSM156N ?? null,
      T10Y2Y: r.T10Y2Y ?? null
    })).sort((a, b) => new Date(a.date) - new Date(b.date));

    // LBMA Fix (USD/oz) als Fallback
    const lastFix = [...rows].reverse().find(r => Number.isFinite(r.GOLD));
    FIX_USD_PER_OZ = lastFix ? Number(lastFix.GOLD) : null;

    document.body.classList.remove('is-loading');

    const seriesMap = buildSeriesMap(rows);
    const goldSeries = rows.filter(r => Number.isFinite(r.GOLD)).map(r => ({ date: r.date, price: r.GOLD }));
    uiLog(`loaded: rows=${rows.length}, goldPoints=${goldSeries.length}`);

    // Momentum (10 Tage, log)
    let momentum = 0;
    if (goldSeries.length > 10) {
      const last = goldSeries[goldSeries.length - 1].price;
      const prev = goldSeries[goldSeries.length - 11].price;
      if (Number.isFinite(last) && Number.isFinite(prev) && prev > 0) momentum = Math.log(last / prev) / 10;
    }

    // Aktuelle Deltas
    const latestDelta = {};
    for (const k of DRIVER_KEYS) {
      const cfg = FREQ[k];
      const arr = seriesMap[k];
      if (!cfg || !arr || !arr.length) { latestDelta[k] = null; continue; }
      latestDelta[k] = (cfg.type === "monthly") ? currentDeltaMonthly(arr) : currentDeltaFromSeries(arr, cfg.steps);
    }

    // Bewertung
    const assess = assessDrivers(latestDelta);

    // Treiber-Listenelement
    function driverItem(key) {
      const a = assess[key] || { status: "neutral", msg: "Neutral" };
      const cfg = FREQ[key] || { label: "" };
      const d = latestDelta[key];
      const dval = Number.isFinite(d) ? ((d >= 0 ? '+' : '') + (100 * d).toFixed(1) + '%' + cfg.label) : '—';
      const tip = tooltipFor(key, assess, cfg);

      const el = document.createElement('div');
      el.className = 'driver';
      el.innerHTML = `
        <div class="ampel">
          <div class="dot ${a.status}"></div>
          <div class="msg">
            <strong>${LABELS[key]}</strong>
            <span class="muted">${a.msg}</span>
          </div>
          <button type="button" class="info" aria-label="Info zu ${LABELS[key]}" aria-expanded="false" data-tip="${tip.replace(/"/g, '&quot;')}">i</button>
        </div>
        <div class="pill">${dval}</div>`;
      return el;
    }

    // Treiber rendern
    const $rates = document.getElementById('grp-rates');
    const $risk = document.getElementById('grp-risk');
    const $real = document.getElementById('grp-real');
    [$rates, $risk, $real].forEach(el => { if (el) el.innerHTML = ''; });

    ["DFII10", "T10YIE", "T10Y2Y"].forEach(k => $rates && $rates.appendChild(driverItem(k)));
    ["DTWEXBGS", "VIXCLS", "BAMLH0A0HYM2", "RECPROUSM156N"].forEach(k => $risk && $risk.appendChild(driverItem(k)));
    ["DCOILBRENTEU", "NAPM"].forEach(k => $real && $real.appendChild(driverItem(k)));

    const sum = summarize(assess);
    const sumEl = document.getElementById('drivers-sum');
    if (sumEl) sumEl.textContent = `In Summe: ${sum.text}`;

    setGroupDot('gs-rates', computeGroupStatus(GROUPS.rates, assess));
    setGroupDot('gs-risk', computeGroupStatus(GROUPS.risk, assess));
    setGroupDot('gs-real', computeGroupStatus(GROUPS.real, assess));

    const rec = recommendation(sum.overall, momentum);
    const sigDot = document.getElementById('sig-dot');
    const sigText = document.getElementById('sig-text');
    if (sigDot) sigDot.className = `dot ${rec.status}`;
    if (sigText) sigText.textContent = rec.text;

    // Forecasts
    const horizons = [30, 90, 180];
    const $fc = document.getElementById('forecast'); if ($fc) $fc.innerHTML = '';
    horizons.forEach(h => {
      const f = forecast(goldSeries, h);
      const last = goldSeries.length ? goldSeries[goldSeries.length - 1].price : null;
      const medPct = (Number.isFinite(f.median) && Number.isFinite(last) && last > 0) ? (f.median / last - 1) : null;
      const status = (!Number.isFinite(medPct) ? 'neutral' : medPct > 0.02 ? 'green' : medPct < -0.02 ? 'red' : 'yellow');
      const bandLo = (Number.isFinite(f.lo) && Number.isFinite(f.median) && f.median > 0) ? (100 * (f.lo / f.median - 1)).toFixed(1) + '%' : '—';
      const bandHi = (Number.isFinite(f.hi) && Number.isFinite(f.median) && f.median > 0) ? (100 * (f.hi / f.median - 1)).toFixed(1) + '%' : '—';
      const wrap = document.createElement('div');
      wrap.className = 'kpi';
      wrap.innerHTML = `
        <span>${h} Tage</span>
        <span><span class="badge ${status}">${Number.isFinite(medPct) ? (medPct >= 0 ? '+' : '') + (100 * medPct).toFixed(1) + '%' : '—'}</span>
        <span class="band">[${bandLo}, ${bandHi}]</span></span>`;
      $fc && $fc.appendChild(wrap);
    });

    // Historischer Vergleich (idle)
    const $an = document.getElementById('analogs');
    requestIdleCallback(() => {
      try {
        const indexFinders = {}; for (const k of DRIVER_KEYS) { indexFinders[k] = dateToSeriesIndex(seriesMap[k]); }
        const deltaRows = rows.map(r => {
          const d = { timestamp: r.date };
          for (const k of DRIVER_KEYS) {
            const cfg = FREQ[k], arr = seriesMap[k], idxF = indexFinders[k];
            d[k] = (!cfg || !arr || !arr.length) ? null : historicalDeltaAtDate(arr, idxF, cfg.steps, r.date, cfg.type === "monthly");
          }
          return d;
        });

        const refStats = {};
        for (const k of DRIVER_KEYS) {
          const vals = deltaRows.map(d => d[k]).filter(v => Number.isFinite(v));
          const m = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
          const s = vals.length ? Math.sqrt(vals.reduce((a, b) => a + (b - m) * (b - m), 0) / vals.length) : 1;
          refStats[k] = { mean: m, std: s || 1e-9 };
        }
        const currentZ = zscoreVector(latestDelta, refStats);

        const cutoff = rows.length - 60;
        const scored = [];
        for (let i = 20; i < cutoff; i++) {
          const vecZ = zscoreVector(deltaRows[i], refStats);
          const sim = cosineOverlap(currentZ, vecZ);
          if (sim > -1) scored.push({ i, sim, date: rows[i].date });
        }
        scored.sort((a, b) => b.sim - a.sim);

        function perf90d(dateStr) {
          let baseIdx = goldSeries.findIndex(g => g.date >= dateStr);
          if (baseIdx >= 0) {
            const fwd = baseIdx + 90;
            if (fwd < goldSeries.length) {
              const r = goldSeries[baseIdx], f = goldSeries[fwd];
              return ((f.price / r.price - 1) * 100);
            }
          }
          return null;
        }

        if ($an) $an.innerHTML = '';
        const threshold = 0.60;
        let shown = 0;
        const top = scored.filter(s => s.sim >= threshold).slice(0, 3);
        top.forEach(hit => {
          const p90 = perf90d(hit.date);
          const div = document.createElement('div'); div.className = 'driver';
          const mo = new Date(hit.date).toLocaleDateString('de-DE', { year: 'numeric', month: 'long' });
          div.innerHTML = `<div><strong>Ähnlich zu ${mo}</strong><div class="muted">Ähnlichkeit: ${(100 * hit.sim).toFixed(0)}%</div></div><div class="pill">90-Tage: ${Number.isFinite(p90) ? p90.toFixed(1) + '%' : '—'}</div>`;
          $an && $an.appendChild(div);
          shown++;
        });
        if (shown === 0) {
          const best = scored.length ? scored[0] : null;
          const div = document.createElement('div'); div.className = 'driver';
          if (best) {
            const mo = new Date(best.date).toLocaleDateString('de-DE', { year: 'numeric', month: 'long' });
            const p90 = perf90d(best.date);
            div.innerHTML = `<div><strong>Nächster Treffer: ${mo}</strong><div class="muted">Ähnlichkeit: ${(100 * (Math.max(0, best.sim))).toFixed(0)}% (keine klare Analogie)</div></div><div class="pill">90-Tage: ${Number.isFinite(p90) ? p90.toFixed(1) + '%' : '—'}</div>`;
          } else {
            div.innerHTML = `<div><strong>Keine Historie vergleichbar</strong><div class="muted">Zu wenig verwertbare Treiberdaten</div></div><div class="pill">—</div>`;
          }
          $an && $an.appendChild(div);
        }
        uiLog('analogies rendered (idle).');
      } catch (err) {
        uiLog('analog error: ' + String(err?.message || err));
      }
    });

    // Portfolio LS + Live
    const input = document.getElementById('holdings-grams');
    if (input) {
      const saved = localStorage.getItem('holdings_grams');
      if (saved != null && saved !== '') input.value = saved;
      updateHoldingsValue();
      input.addEventListener('input', () => {
        localStorage.setItem('holdings_grams', input.value || '');
        updateHoldingsValue();
      });
    }

    // Vendors-Karte laden (asynchron)
    setTimeout(renderVendorsCard, 0);

    // Systemstatus ggf. aktualisieren
    renderSystemFromPreflight();
    setTimeout(renderSystemFromPreflight, 1200);

    uiLog('render complete.');
  } catch (e) {
    console.error("App-Fehler:", e);
    const st = document.getElementById('sig-text');
    if (st) st.textContent = 'Fehler beim Laden';
    uiLog('ERROR: ' + String(e?.message || e));
    document.body.classList.remove('is-loading');
  }
})();
