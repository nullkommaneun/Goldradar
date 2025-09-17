(function(){
  const diagEl = () => document.getElementById('diag');
  const log = (m) => { const t = new Date().toISOString().slice(11,19); const el=diagEl(); if(el){ el.textContent += `\n[${t}] ${m}`; } };

  async function json(url){ try{ const r = await fetch(url + '?t=' + Date.now()); return await r.json(); } catch(_){ return null; } }

  async function run(){
    window.PREFLIGHT = window.PREFLIGHT || {};
    const pf = window.PREFLIGHT;
    pf.errors = pf.errors || [];
    pf.warnings = pf.warnings || [];
    pf.stats = pf.stats || {};

    log("Preflight startet …");
    // History
    const hist = await json('data/history.json');
    if (hist && Array.isArray(hist.history) && hist.history.length){
      const rows = hist.history;
      pf.stats.history = {
        start: rows[0].timestamp,
        end: rows[rows.length-1].timestamp,
        rows: rows.length
      };
    } else {
      pf.errors.push("history.json fehlt/leer");
    }

    // Spot
    const spot = await json('data/spot.json');
    if (spot && typeof spot.XAUUSD !== 'undefined' && spot.XAUUSD !== null){
      pf.stats.spot = { hasValue: true, timestamp: spot.timestamp || null };
    } else {
      pf.warnings.push("Kein aktueller Spotpreis (spot.json)");
      pf.stats.spot = { hasValue: false };
    }

    // Vendors + Diagnostics
    const vend = await json('data/vendors_auto.json');
    if (vend && Array.isArray(vend.vendors)){
      const totals = vend.diagnostics?.totals || {};
      const domains = (vend.diagnostics?.domains || []).length || (vend.vendors || []).length || 0;

      pf.stats.vendors = {
        domains: domains,
        pages: totals.pages ?? null,
        products: totals.products ?? null,
        offers: totals.offers ?? null,
        items: totals.items ?? null,
        generated: vend.generated || null
      };

      const msg = `vendors: domains=${domains} pages=${totals.pages||0} products=${totals.products||0} offers=${totals.offers||0} items=${totals.items||0}`;
      log(msg);

      if (!totals.items) pf.warnings.push("Händlerdaten gefunden, aber keine verwertbaren Items.");
    } else {
      pf.warnings.push("vendors_auto.json nicht verfügbar.");
    }

    log("Preflight OK");
  }

  run().catch(e => { (window.PREFLIGHT.errors = window.PREFLIGHT.errors || []).push(String(e)); });
})();
