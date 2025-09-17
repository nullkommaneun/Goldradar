// ---------- Mini-Logger in die UI ----------
function uiLog(msg) {
  const el = document.getElementById('diag');
  if (!el) return;
  const now = new Date().toISOString().slice(11,19);
  el.textContent = (el.textContent ? el.textContent + "\n" : "") + `[${now}] ${msg}`;
}

// ---------- Helpers ----------
const nowBust = () => '?t=' + Date.now();
const pct = v => (isFinite(v) ? (v>=0?'+':'') + (100*v).toFixed(1) + '%' : '—');

const SERIES = ["GOLDAMGBD228NLBM","DFII10","DTWEXBGS","VIXCLS","DCOILBRENTEU","T10YIE","BAMLH0A0HYM2","NAPM","RECPROUSM156N","T10Y2Y"];
const DRIVER_KEYS = ["DFII10","DTWEXBGS","VIXCLS","DCOILBRENTEU","T10YIE","BAMLH0A0HYM2","NAPM","RECPROUSM156N","T10Y2Y"];

// ---------- Driver assessment ----------
function assessDrivers(t){
  const def = (val, betterLow) => {
    if(val==null || !isFinite(val)) return {status:"neutral", msg:"Neutral (keine Daten)"};
    if(betterLow){ if(val<=0) return {status:"green", msg:"Gut für deinen Goldpreis"};
      if(val<1) return {status:"yellow", msg:"Eher neutral"}; return {status:"red", msg:"Schlecht für deinen Goldpreis"}; }
    else { if(val>=0) return {status:"green", msg:"Gut für deinen Goldpreis"};
      if(val>-1) return {status:"yellow", msg:"Eher neutral"}; return {status:"red", msg:"Schlecht für deinen Goldpreis"}; }
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
function summarize(drvs){
  const scoreMap={green:2,yellow:1,red:-2,neutral:0};
  let s=0,n=0; for(const k in drvs){ s+=scoreMap[drvs[k].status]; n++; }
  const avg=s/Math.max(1,n);
  if(avg>=1) return {overall:"green", text:"In Summe eher positiv."};
  if(avg<=-1) return {overall:"red", text:"In Summe eher negativ."};
  return {overall:"yellow", text:"In Summe eher neutral."};
}
function recommendation(overall, momentum){
  const score = (overall==="green"?2:overall==="red"?-2:0) + (momentum>=0?1:-1);
  if(score>=2) return {status:"green", text:"Kaufen"};
  if(score<=-2) return {status:"red", text:"Nicht kaufen"};
  return {status:"yellow", text:"Abwarten"};
}

// ---------- Forecast ----------
function forecast(series, horizonDays){
  if(!series || series.length<90) return {median:null, lo:null, hi:null};
  const sorted=[...series].sort((a,b)=>new Date(a.date)-new Date(b.date));
  const px = sorted.map(d=>d.price).filter(v=>isFinite(v)&&v>0);
  if(px.length<90) return {median:null, lo:null, hi:null};
  const logR=[]; for(let i=1;i<px.length;i++) logR.push(Math.log(px[i]/px[i-1]));
  const w = Math.min(60, logR.length), tail = logR.slice(-w);
  const mu = tail.reduce((a,b)=>a+b,0)/Math.max(1,tail.length);
  const sigma = Math.sqrt(tail.reduce((a,b)=>a+(b-mu)*(b-mu),0)/Math.max(1,tail.length));
  const last = px[px.length-1];
  const steps = Math.max(1, Math.round(horizonDays));
  const med = last * Math.exp(mu*steps);
  const lo  = last * Math.exp((mu-1.64*sigma)*steps);
  const hi  = last * Math.exp((mu+1.64*sigma)*steps);
  return {median:med, lo, hi};
}

// ---------- Similarity helpers ----------
function cosineOverlap(a,b){
  // Cosine nur über Keys, die bei beiden vorhanden sind
  let num=0,na=0,nb=0,c=0;
  for(const k of DRIVER_KEYS){
    const va=a[k], vb=b[k];
    if(va==null || !isFinite(va) || vb==null || !isFinite(vb)) continue;
    num += va*vb; na += va*va; nb += vb*vb; c++;
  }
  if(c===0) return -1;
  const den = Math.sqrt(na)*Math.sqrt(nb);
  return den>0 ? num/den : -1;
}
function zscoreVector(vec, stats){
  const out={};
  for(const k of DRIVER_KEYS){
    const v = vec[k];
    const m = stats[k]?.mean ?? 0;
    const s = stats[k]?.std  ?? 1e-9;
    out[k] = (v==null || !isFinite(v)) ? null : (v - m) / (s || 1e-9);
  }
  return out;
}

// ---------- Delta-Bausteine ----------
function nearestValidValue(rows, key, startIndex, lookback=5){
  for(let i=startIndex; i>=0 && i>=startIndex - lookback; i--){
    const v = rows[i][key];
    if(isFinite(v)) return {idx:i, v};
  }
  return null;
}
function deltaAt(rows, key, index, lag=10, lookback=5){
  // Nutzt Kalenderindex (t und t-10), sucht jeweils bis zu 'lookback' Tage nach gültigen Werten zurück.
  const tiny=1e-6;
  const cur = nearestValidValue(rows, key, index, lookback);
  const past = nearestValidValue(rows, key, Math.max(0, index - lag), lookback);
  if(!cur || !past) return null;
  if(Math.abs(past.v) < tiny || Math.abs(cur.v) < tiny) return null;
  return (cur.v - past.v) / Math.abs(past.v);
}

// ---------- App ----------
(async function(){
  try{
    uiLog('fetching json…');
    const [hist, spot] = await Promise.all([
      fetch('data/history.json'+nowBust()).then(r=>r.json()).catch(()=>({history:[]})),
      fetch('data/spot.json'+nowBust()).then(r=>r.json()).catch(()=>({XAUUSD:null,timestamp:null}))
    ]);

    // Spot (EU-Maß: USD/kg)
    const spotVal = (spot && spot.XAUUSD!=null) ? Number(spot.XAUUSD) : null;
    document.getElementById('spotline').textContent = `Spot: ${spotVal!=null ? spotVal.toFixed(2)+' USD/kg' : '—'}`;

    // Rows
    const rows = (hist.history||[]).map(r=>({
      date: r.timestamp,
      GOLD:r.GOLDAMGBD228NLBM??null,
      DFII10:r.DFII10??null, DTWEXBGS:r.DTWEXBGS??null, VIXCLS:r.VIXCLS??null, DCOILBRENTEU:r.DCOILBRENTEU??null,
      T10YIE:r.T10YIE??null, BAMLH0A0HYM2:r.BAMLH0A0HYM2??null, NAPM:r.NAPM??null, RECPROUSM156N:r.RECPROUSM156N??null, T10Y2Y:r.T10Y2Y??null
    })).sort((a,b)=> new Date(a.date)-new Date(b.date));

    const goldSeries = rows.filter(r=>isFinite(r.GOLD)).map(r=>({date:r.date, price:r.GOLD}));
    uiLog(`loaded: rows=${rows.length}, goldPoints=${goldSeries.length}`);

    // Momentum (10d)
    let momentum=0;
    if(goldSeries.length>10){
      const last=goldSeries[goldSeries.length-1].price;
      const prev=goldSeries[goldSeries.length-11].price;
      if(isFinite(last) && isFinite(prev) && prev>0) momentum = Math.log(last/prev)/10;
    }

    // --- Deltas (10 Kalendertage, tolerant) ---
    const lastIndex = rows.length - 1;
    const latestDelta = {};
    for(const k of DRIVER_KEYS){
      latestDelta[k] = deltaAt(rows, k, lastIndex, 10, 5);
    }

    // Treiber rendern (— bei null)
    const labels = {
      DFII10:"Realzinsen (Zinskosten)", DTWEXBGS:"US-Dollar (Dollar-Stärke)", VIXCLS:"VIX (Marktstress)",
      DCOILBRENTEU:"Ölpreis (Inflationstreiber)", T10YIE:"Inflationserwartung", BAMLH0A0HYM2:"HY-Spreads",
      NAPM:"PMI", RECPROUSM156N:"Rezessionsrisiko", T10Y2Y:"Zinskurve 10y–2y"
    };
    const assess = assessDrivers(latestDelta);
    const $drv = document.getElementById('drivers');
    $drv.innerHTML = '';
    Object.keys(labels).forEach(k=>{
      const a = assess[k]||{status:"neutral",msg:"Neutral"};
      const dval = isFinite(latestDelta[k]) ? ((latestDelta[k]>=0?'+':'')+(100*latestDelta[k]).toFixed(1)+'%/10T') : '—';
      const el=document.createElement('div');
      el.className='driver';
      el.innerHTML = `
        <div class="ampel"><div class="dot ${a.status}"></div><div class="msg"><strong>${labels[k]}</strong><br><span class="muted">${a.msg}</span></div></div>
        <div class="pill">${dval}</div>`;
      $drv.appendChild(el);
    });

    const sum = summarize(assess);
    document.getElementById('drivers-sum').textContent = `In Summe: ${sum.text}`;

    // Top-Empfehlung
    const rec = recommendation(sum.overall, momentum);
    document.getElementById('sig-dot').className = `dot ${rec.status}`;
    document.getElementById('sig-text').textContent = rec.text;

    // Forecasts
    const horizons=[30,90,180];
    const $fc = document.getElementById('forecast');
    $fc.innerHTML = '';
    horizons.forEach(h=>{
      const f = forecast(goldSeries, h);
      const last = goldSeries.length ? goldSeries[goldSeries.length-1].price : null;
      const medPct = (isFinite(f.median) && isFinite(last) && last>0) ? (f.median/last - 1) : null;
      const status = (!isFinite(medPct) ? 'neutral' : medPct>0.02 ? 'green' : medPct<-0.02 ? 'red' : 'yellow');
      const bandLo = (isFinite(f.lo) && isFinite(f.median) && f.median>0) ? (100*(f.lo/f.median - 1)).toFixed(1)+'%' : '—';
      const bandHi = (isFinite(f.hi) && isFinite(f.median) && f.median>0) ? (100*(f.hi/f.median - 1)).toFixed(1)+'%' : '—';
      const wrap = document.createElement('div');
      wrap.className='kpi';
      wrap.innerHTML = `
        <span>${h} Tage</span>
        <span><span class="badge ${status}">${isFinite(medPct)? (medPct>=0?'+':'')+(100*medPct).toFixed(1)+'%':'—'}</span>
        <span class="band">[${bandLo}, ${bandHi}]</span></span>`;
      $fc.appendChild(wrap);
    });

    // ---------- Historischer Vergleich ----------
    const $an = document.getElementById('analogs');
    $an.innerHTML = '';

    // 10T-Relative für alle t (tolerant)
    const deltaRows = rows.map(r=>{
      const d = {timestamp:r.date};
      for(const k of DRIVER_KEYS) d[k]=null;
      return d;
    });
    for(let i=0;i<rows.length;i++){
      for(const k of DRIVER_KEYS){
        deltaRows[i][k] = deltaAt(rows, k, i, 10, 5);
      }
    }

    // Referenz-Stats (Z-Scores)
    const refStats={};
    for(const k of DRIVER_KEYS){
      const vals = deltaRows.map(d=>d[k]).filter(v=>isFinite(v));
      const m = vals.length? vals.reduce((a,b)=>a+b,0)/vals.length : 0;
      const s = vals.length? Math.sqrt(vals.reduce((a,b)=>a+(b-m)*(b-m),0)/vals.length) : 1;
      refStats[k]={mean:m,std:s||1e-9};
    }
    const currentZ = zscoreVector(latestDelta, refStats);

    // Scoring
    const cutoff = rows.length-60; // genug Zukunft für 90T-Perf
    const scored=[];
    for(let i=20;i<cutoff;i++){
      const vecZ = zscoreVector(deltaRows[i], refStats);
      const sim = cosineOverlap(currentZ, vecZ);
      if(sim>-1) scored.push({i, sim, date: rows[i].date});
    }
    scored.sort((a,b)=>b.sim - a.sim);

    // Helper: 90T-Performance
    function perf90d(dateStr){
      let baseIdx = goldSeries.findIndex(g=>g.date>=dateStr);
      if(baseIdx>=0){
        const fwd = baseIdx+90;
        if(fwd < goldSeries.length){
          const r = goldSeries[baseIdx], f = goldSeries[fwd];
          return ((f.price/r.price - 1)*100);
        }
      }
      return null;
    }

    // Anzeige: Top über Schwelle ODER mindestens bester Treffer
    const threshold = 0.60;
    let shown = 0;

    const top = scored.filter(s=>s.sim>=threshold).slice(0,3);
    top.forEach(hit=>{
      const p90 = perf90d(hit.date);
      const div=document.createElement('div'); div.className='driver';
      const mo = new Date(hit.date).toLocaleDateString('de-DE',{year:'numeric',month:'long'});
      div.innerHTML=`<div><strong>Ähnlich zu ${mo}</strong><div class="muted">Ähnlichkeit: ${(100*hit.sim).toFixed(0)}%</div></div><div class="pill">90-Tage: ${p90!=null? p90.toFixed(1)+'%':'—'}</div>`;
      $an.appendChild(div);
      shown++;
    });

    if(shown===0){
      const best = scored.length? scored[0] : null;
      const div=document.createElement('div'); div.className='driver';
      if(best){
        const mo = new Date(best.date).toLocaleDateString('de-DE',{year:'numeric',month:'long'});
        const p90 = perf90d(best.date);
        div.innerHTML=`<div><strong>Nächster Treffer: ${mo}</strong><div class="muted">Ähnlichkeit: ${(100*(Math.max(0,best.sim))).toFixed(0)}% (keine klare Analogie)</div></div><div class="pill">90-Tage: ${p90!=null? p90.toFixed(1)+'%':'—'}</div>`;
      } else {
        div.innerHTML=`<div><strong>Keine Historie vergleichbar</strong><div class="muted">Zu wenig verwertbare Treiberdaten</div></div><div class="pill">—</div>`;
      }
      $an.appendChild(div);
    }

    uiLog('render complete.');
  }catch(e){
    console.error("App-Fehler:", e);
    document.getElementById('sig-text').textContent = 'Fehler beim Laden';
    uiLog('ERROR: ' + String(e?.message || e));
  }
})();
