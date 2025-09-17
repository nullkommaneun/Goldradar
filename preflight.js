(async function(){
  const q = '?t=' + Date.now(); // Cache-Bust
  const el = document.getElementById('diag');
  try {
    const [hResp, sResp, dResp] = await Promise.all([
      fetch('data/history.json'+q),
      fetch('data/spot.json'+q),
      fetch('data/diag.json'+q).catch(()=>null)
    ]);

    const [hist, spot, diag] = await Promise.all([
      hResp.ok ? hResp.json() : Promise.resolve({history:[]}),
      sResp.ok ? sResp.json() : Promise.resolve({XAUUSD:null,timestamp:null}),
      (dResp && dResp.ok) ? dResp.json() : Promise.resolve({})
    ]);

    const len = Array.isArray(hist.history) ? hist.history.length : 0;
    el.textContent = "Preflight OK\n"
      + "history.len=" + len + "\n"
      + "spot=" + (spot && spot.XAUUSD!=null ? Number(spot.XAUUSD).toFixed(2) : "—") + "\n"
      + "diag=" + JSON.stringify(diag);

    // Falls history leer ist, App neutral lassen – app.js kümmert sich ums Rendering.
  } catch (e) {
    el.textContent = "Preflight-Error: " + String(e);
  }
})();
