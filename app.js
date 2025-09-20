// ---------- UI-Logger ----------
function uiLog(msg) {
  const el = document.getElementById('diag');
  if (!el) return;
  const now = new Date().toISOString().slice(11,19);
  el.textContent = (el.textContent ? el.textContent + "\n" : "") + `[${now}] ${msg}`;
}

// ---------- Helpers ----------
const nowBust = () => '?t=' + Date.now();
const pct = v => (Number.isFinite(v) ? (v>=0?'+':'') + (100*v).toFixed(1) + '%' : '—');

const DRIVER_KEYS = ["DFII10","DTWEXBGS","VIXCLS","DCOILBRENTEU","T10YIE","BAMLH0A0HYM2","NAPM","RECPROUSM156N","T10Y2Y"];

const FREQ = {
  DFII10:   {type:"daily",   steps:10, label:"/10T"},
  DTWEXBGS: {type:"daily",   steps:10, label:"/10T"},
  VIXCLS:   {type:"daily",   steps:10, label:"/10T"},
  DCOILBRENTEU:{type:"daily",steps:10, label:"/10T"},
  T10YIE:   {type:"daily",   steps:10, label:"/10T"},
  BAMLH0A0HYM2:{type:"daily",steps:10, label:"/10T"},
  NAPM:     {type:"monthly", steps:1,  label:"/1M"},
  RECPROUSM156N:{type:"monthly",steps:1,label:"/1M"},
  T10Y2Y:   {type:"daily",   steps:10, label:"/10T"},
};

// Gruppierung
const GROUPS = {
  rates:   ["DFII10","T10YIE","T10Y2Y"],
  risk:    ["DTWEXBGS","VIXCLS","BAMLH0A0HYM2","RECPROUSM156N"],
  real:    ["DCOILBRENTEU","NAPM"]
};

// Labels & Bewertung
function assessDrivers(t){
  const def = (val, betterLow) => {
    if(val==null || !Number.isFinite(val)) return {status:"neutral", msg:"Neutral (keine Daten)"};
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

// Forecast
function forecast(series, horizonDays){
  if(!series || series.length<90) return {median:null, lo:null, hi:null};
  const sorted=[...series].sort((a,b)=>new Date(a.date)-new Date(b.date));
  const px = sorted.map(d=>d.price).filter(v=>Number.isFinite(v)&&v>0);
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

// Similarity
function cosineOverlap(a,b){
  let num=0,na=0,nb=0,c=0;
  for(const k of DRIVER_KEYS){
    const va=a[k], vb=b[k];
    if(va==null || !Number.isFinite(va) || vb==null || !Number.isFinite(vb)) continue;
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
    out[k] = (v==null || !Number.isFinite(v)) ? null : (v - m) / (s || 1e-9);
  }
  return out;
}

// Serienaufbereitung + Deltas
function buildSeriesMap(rows){
  const map = {};
  for(const k of DRIVER_KEYS){
    const arr = rows
      .map(r => ({date:r.date, value: r[k]}))
      .filter(o => Number.isFinite(o.value))
      .sort((a,b)=> new Date(a.date)-new Date(b.date));
    map[k] = arr;
  }
  return map;
}
function currentDeltaFromSeries(seriesArr, steps){
  if(!seriesArr || seriesArr.length < steps+1) return null;
  const cur = seriesArr[seriesArr.length-1].value;
  const past = seriesArr[seriesArr.length-1-steps].value;
  const tiny=1e-9;
  if(Math.abs(cur)<tiny || Math.abs(past)<tiny) return null;
  return (cur - past) / Math.abs(past);
}
function currentDeltaMonthly(seriesArr){
  if(!seriesArr || seriesArr.length < 2) return null;
  const cur = seriesArr[seriesArr.length-1].value;
  const past = seriesArr[seriesArr.length-2].value;
  const tiny=1e-9;
  if(Math.abs(cur)<tiny || Math.abs(past)<tiny) return null;
  return (cur - past) / Math.abs(past);
}
function dateToSeriesIndex(seriesArr){
  const dates = seriesArr.map(x=>x.date);
  return function(dateStr){
    if(!dates.length) return -1;
    let lo=0, hi=dates.length-1, ans=-1;
    const target = new Date(dateStr).getTime();
    while(lo<=hi){
      const mid=(lo+hi)>>1;
      const t = new Date(dates[mid]).getTime();
      if(t<=target){ ans=mid; lo=mid+1; } else { hi=mid-1; }
    }
    return ans;
  };
}
function historicalDeltaAtDate(seriesArr, idxFinder, steps, dateStr, isMonthly){
  if(!seriesArr || !seriesArr.length) return null;
  const j = idxFinder(dateStr);
  if(j<0) return null;
  const jPast = j - (isMonthly? 1 : steps);
  if(jPast < 0) return null;
  const cur = seriesArr[j].value, past = seriesArr[jPast].value;
  const tiny=1e-9;
  if(Math.abs(cur)<tiny || Math.abs(past)<tiny) return null;
  return (cur - past) / Math.abs(past);
}

// ---------- System-Status aus Preflight ----------
function renderSystemFromPreflight() {
  const el = document.getElementById('system-info');
  if (!el) return;

  const pf = window.PREFLIGHT;
  if (!pf) { el.textContent = "Datenquelle: FRED, stooq"; return; }

  const hasErr = Array.isArray(pf.errors) && pf.errors.length>0;
  const hasWarn = Array.isArray(pf.warnings) && pf.warnings.length>0;
  const badgeClass = hasErr ? "red" : hasWarn ? "yellow" : "green";
  const head = hasErr ? "Preflight FAIL" : hasWarn ? "Preflight WARN" : "Preflight OK";

  const h = pf.stats?.history || {};
  const s = pf.stats?.spot || {};

  let parts = [];
  if (h.start && h.end) parts.push(`Zeitraum: ${h.start} – ${h.end}`);

  if (h.end) {
    const ageD = Math.floor((Date.now() - Date.parse(h.end+"T00:00:00Z"))/86400000);
    parts.push(`Letzter History-Tag: ${h.end} (${ageD}T alt)`);
  }

  if (s.hasValue) {
    if (s.timestamp) {
      const ageH = Math.floor((Date.now() - Date.parse(s.timestamp))/3600000);
      parts.push(`Spot: vorhanden (${ageH}h alt)`);
    } else {
      parts.push(`Spot: vorhanden`);
    }
  } else {
    parts.push(`Spot: —`);
  }

  const warnTail = (!hasErr && hasWarn && pf.warnings[0]) ? ` • Hinweis: ${pf.warnings[0]}` : "";
  el.innerHTML = `<span class="badge ${badgeClass}">${head}</span> • ${parts.join(" • ")}${warnTail}`;
}

// ---------- App ----------
(async function(){
  try{
    uiLog('fetching json…');
    const [hist, spot] = await Promise.all([
      fetch('data/history.json'+nowBust()).then(r=>r.json()).catch(()=>({history:[]})),
      fetch('data/spot.json'+nowBust()).then(r=>r.json()).catch(()=>({XAUUSD:null,timestamp:null}))
    ]);

    // Spot
    const spotVal = (spot && spot.XAUUSD!=null) ? Number(spot.XAUUSD) : null;
    document.getElementById('spotline').textContent = `Spot: ${Number.isFinite(spotVal) ? spotVal.toFixed(2)+' USD/kg' : '—'}`;

    // Rows
    const rows = (hist.history||[]).map(r=>({
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
    })).sort((a,b)=> new Date(a.date)-new Date(b.date));

    const goldSeries = rows.filter(r=>Number.isFinite(r.GOLD)).map(r=>({date:r.date, price:r.GOLD}));
    uiLog(`loaded: rows=${rows.length}, goldPoints=${goldSeries.length}`);

    // Serien & Indexfinder
    const seriesMap = buildSeriesMap(rows);
    const indexFinders = {};
    for(const k of DRIVER_KEYS){ indexFinders[k] = dateToSeriesIndex(seriesMap[k]); }

    // Momentum
    let momentum=0;
    if(goldSeries.length>10){
      const last=goldSeries[goldSeries.length-1].price;
      const prev=goldSeries[goldSeries.length-11].price;
      if(Number.isFinite(last) && Number.isFinite(prev) && prev>0) momentum = Math.log(last/prev)/10;
    }

    // Aktuelle Deltas
    const latestDelta = {};
    for(const k of DRIVER_KEYS){
      const cfg = FREQ[k];
      const arr = seriesMap[k];
      if(!cfg || !arr || !arr.length){ latestDelta[k]=null; continue; }
      if(cfg.type==="monthly") latestDelta[k] = currentDeltaMonthly(arr);
      else latestDelta[k] = currentDeltaFromSeries(arr, cfg.steps);
    }

    // Labels
    const labels = {
      DFII10:"Realzinsen (Zinskosten)", DTWEXBGS:"US-Dollar (Dollar-Stärke)", VIXCLS:"VIX (Marktstress)",
      DCOILBRENTEU:"Ölpreis (Inflationstreiber)", T10YIE:"Inflationserwartung", BAMLH0A0HYM2:"HY-Spreads",
      NAPM:"PMI", RECPROUSM156N:"Rezessionsrisiko", T10Y2Y:"Zinskurve 10y–2y"
    };

    // Bewertung & Anzeige
    const assess = assessDrivers(latestDelta);

    function driverItem(key){
      const a = assess[key]||{status:"neutral",msg:"Neutral"};
      const cfg = FREQ[key] || {label:""};
      const d = latestDelta[key];
      const dval = Number.isFinite(d) ? ((d>=0?'+':'')+(100*d).toFixed(1)+'%'+cfg.label) : '—';
      const el=document.createElement('div');
      el.className='driver';
      el.innerHTML = `
        <div class="ampel">
          <div class="dot ${a.status}"></div>
          <div class="msg"><strong>${labels[key]}</strong><span class="muted">${a.msg}</span></div>
        </div>
        <div class="pill">${dval}</div>`;
      return el;
    }

    const $rates = document.getElementById('grp-rates');
    const $risk  = document.getElementById('grp-risk');
    const $real  = document.getElementById('grp-real');
    [$rates,$risk,$real].forEach(el=>{ if(el) el.innerHTML=''; });

    GROUPS.rates.forEach(k => $rates.appendChild(driverItem(k)));
    GROUPS.risk.forEach(k  => $risk.appendChild(driverItem(k)));
    GROUPS.real.forEach(k  => $real.appendChild(driverItem(k)));

    const sum = summarize(assess);
    document.getElementById('drivers-sum').textContent = `In Summe: ${sum.text}`;

    // Ampel-Empfehlung (Header)
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
      const medPct = (Number.isFinite(f.median) && Number.isFinite(last) && last>0) ? (f.median/last - 1) : null;
      const status = (!Number.isFinite(medPct) ? 'neutral' : medPct>0.02 ? 'green' : medPct<-0.02 ? 'red' : 'yellow');
      const bandLo = (Number.isFinite(f.lo) && Number.isFinite(f.median) && f.median>0) ? (100*(f.lo/f.median - 1)).toFixed(1)+'%' : '—';
      const bandHi = (Number.isFinite(f.hi) && Number.isFinite(f.median) && f.median>0) ? (100*(f.hi/f.median - 1)).toFixed(1)+'%' : '—';
      const wrap = document.createElement('div');
      wrap.className='kpi';
      wrap.innerHTML = `
        <span>${h} Tage</span>
        <span><span class="badge ${status}">${Number.isFinite(medPct)? (medPct>=0?'+':'')+(100*medPct).toFixed(1)+'%':'—'}</span>
        <span class="band">[${bandLo}, ${bandHi}]</span></span>`;
      $fc.appendChild(wrap);
    });

    // Historischer Vergleich (unverändert)
    const $an = document.getElementById('analogs');
    $an.innerHTML = '';

    const deltaRows = rows.map(r=>{
      const d = {timestamp:r.date};
      for(const k of DRIVER_KEYS){
        const cfg=FREQ[k], arr=seriesMap[k], idxF=dateToSeriesIndex(seriesMap[k]||[]);
        d[k] = (!cfg||!arr||!arr.length) ? null
              : historicalDeltaAtDate(arr, idxF, cfg.steps, r.date, cfg.type==="monthly");
      }
      return d;
    });

    const refStats={};
    for(const k of DRIVER_KEYS){
      const vals = deltaRows.map(d=>d[k]).filter(v=>Number.isFinite(v));
      const m = vals.length? vals.reduce((a,b)=>a+b,0)/vals.length : 0;
      const s = vals.length? Math.sqrt(vals.reduce((a,b)=>a+(b-m)*(b-m),0)/vals.length) : 1;
      refStats[k]={mean:m,std:s||1e-9};
    }
    const currentZ = zscoreVector(latestDelta, refStats);

    const cutoff = rows.length-60;
    const scored=[];
    for(let i=20;i<cutoff;i++){
      const vecZ = zscoreVector(deltaRows[i], refStats);
      const sim = cosineOverlap(currentZ, vecZ);
      if(sim>-1) scored.push({i, sim, date: rows[i].date});
    }
    scored.sort((a,b)=>b.sim - a.sim);

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

    const threshold = 0.60;
    let shown = 0;
    const top = scored.filter(s=>s.sim>=threshold).slice(0,3);
    top.forEach(hit=>{
      const p90 = perf90d(hit.date);
      const div=document.createElement('div'); div.className='driver';
      const mo = new Date(hit.date).toLocaleDateString('de-DE',{year:'numeric',month:'long'});
      div.innerHTML=`<div><strong>Ähnlich zu ${mo}</strong><div class="muted">Ähnlichkeit: ${(100*hit.sim).toFixed(0)}%</div></div><div class="pill">90-Tage: ${Number.isFinite(p90)? p90.toFixed(1)+'%':'—'}</div>`;
      $an.appendChild(div);
      shown++;
    });
    if(shown===0){
      const best = scored.length? scored[0] : null;
      const div=document.createElement('div'); div.className='driver';
      if(best){
        const mo = new Date(best.date).toLocaleDateString('de-DE',{year:'numeric',month:'long'});
        const p90 = perf90d(best.date);
        div.innerHTML=`<div><strong>Nächster Treffer: ${mo}</strong><div class="muted">Ähnlichkeit: ${(100*(Math.max(0,best.sim))).toFixed(0)}% (keine klare Analogie)</div></div><div class="pill">90-Tage: ${Number.isFinite(p90)? p90.toFixed(1)+'%':'—'}</div>`;
      } else {
        div.innerHTML=`<div><strong>Keine Historie vergleichbar</strong><div class="muted">Zu wenig verwertbare Treiberdaten</div></div><div class="pill">—</div>`;
      }
      $an.appendChild(div);
    }

    // System-Status aus Preflight darstellen (sofort + verzögert, falls Preflight noch läuft)
    renderSystemFromPreflight();
    setTimeout(renderSystemFromPreflight, 300);
    setTimeout(renderSystemFromPreflight, 1200);

    uiLog('render complete.');
  }catch(e){
    console.error("App-Fehler:", e);
    document.getElementById('sig-text').textContent = 'Fehler beim Laden';
    uiLog('ERROR: ' + String(e?.message || e));
  }
})();
