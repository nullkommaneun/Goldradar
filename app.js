// ---------- Helpers
const nowBust = () => '?t=' + Date.now();
const pct = v => (isFinite(v) ? (v>=0?'+':'') + (100*v).toFixed(1) + '%' : '—');
const SERIES = ["GOLDAMGBD228NLBM","DFII10","DTWEXBGS","VIXCLS","DCOILBRENTEU","T10YIE","BAMLH0A0HYM2","NAPM","RECPROUSM156N","T10Y2Y"];

// ---------- Driver assessment
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

// ---------- Forecast
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

// ---------- App
(async function(){
  try{
    // JSON laden (Cache-Bust)
    const [hist, spot] = await Promise.all([
      fetch('data/history.json'+nowBust()).then(r=>r.json()).catch(()=>({history:[]})),
      fetch('data/spot.json'+nowBust()).then(r=>r.json()).catch(()=>({XAUUSD:null,timestamp:null}))
    ]);

    // Rows normalisieren
    const rows = (hist.history||[]).map(r=>({
      date: r.timestamp,
      GOLD:r.GOLDAMGBD228NLBM??null,
      DFII10:r.DFII10??null, DTWEXBGS:r.DTWEXBGS??null, VIXCLS:r.VIXCLS??null, DCOILBRENTEU:r.DCOILBRENTEU??null,
      T10YIE:r.T10YIE??null, BAMLH0A0HYM2:r.BAMLH0A0HYM2??null, NAPM:r.NAPM??null, RECPROUSM156N:r.RECPROUSM156N??null, T10Y2Y:r.T10Y2Y??null
    })).sort((a,b)=> new Date(a.date)-new Date(b.date));

    const goldSeries = rows.filter(r=>isFinite(r.GOLD)).map(r=>({date:r.date, price:r.GOLD}));

    // Momentum (10d)
    let momentum=0;
    if(goldSeries.length>10){
      const last=goldSeries[goldSeries.length-1].price;
      const prev=goldSeries[goldSeries.length-11].price;
      if(isFinite(last) && isFinite(prev) && prev>0) momentum = Math.log(last/prev)/10;
    }

    // Deltas (10d relativ)
    function shortDelta(arr, key){
      const list = arr.map(r=> ({d:r.date, v:r[key]})).filter(x=>isFinite(x.v));
      if(list.length<11) return null;
      const vN=list[list.length-1].v, vP=list[list.length-11].v;
      if(!isFinite(vN)||!isFinite(vP)) return null;
      const base=Math.max(1e-9,Math.abs(vP));
      return (vN-vP)/base;
    }
    const latestDelta = {
      DFII10: shortDelta(rows,'DFII10'),
      DTWEXBGS: shortDelta(rows,'DTWEXBGS'),
      VIXCLS: shortDelta(rows,'VIXCLS'),
      DCOILBRENTEU: shortDelta(rows,'DCOILBRENTEU'),
      T10YIE: shortDelta(rows,'T10YIE'),
      BAMLH0A0HYM2: shortDelta(rows,'BAMLH0A0HYM2'),
      NAPM: shortDelta(rows,'NAPM'),
      RECPROUSM156N: shortDelta(rows,'RECPROUSM156N'),
      T10Y2Y: shortDelta(rows,'T10Y2Y')
    };

    // Treiber rendern
    const labels = {
      DFII10:"Realzinsen (Zinskosten)", DTWEXBGS:"US-Dollar (Dollar-Stärke)", VIXCLS:"VIX (Marktstress)",
      DCOILBRENTEU:"Ölpreis (Inflationstreiber)", T10YIE:"Inflationserwartung", BAMLH0A0HYM2:"HY-Spreads",
      NAPM:"PMI", RECPROUSM156N:"Rezessionsrisiko", T10Y2Y:"Zinskurve 10y–2y"
    };
    const assess = assessDrivers(latestDelta);
    const $drv = document.getElementById('drivers');
    Object.keys(labels).forEach(k=>{
      const a = assess[k]||{status:"neutral",msg:"Neutral"};
      const el=document.createElement('div');
      el.className='driver';
      el.innerHTML = `
        <div class="ampel"><div class="dot ${a.status}"></div><div class="msg"><strong>${labels[k]}</strong><br><span class="muted">${a.msg}</span></div></div>
        <div class="pill">${isFinite(latestDelta[k])? (latestDelta[k]>=0? '+' : '')+(100*latestDelta[k]).toFixed(1)+'%/10T' : '—'}</div>`;
      $drv.appendChild(el);
    });

    const sum = summarize(assess);
    document.getElementById('drivers-sum').textContent = `In Summe: ${sum.text}`;

    // Top-Empfehlung
    const rec = recommendation(sum.overall, momentum);
    document.getElementById('sig-dot').className = `dot ${rec.status}`;
    document.getElementById('sig-text').textContent = rec.text;

    // Spot/Status
    const spotTs = spot.timestamp? new Date(spot.timestamp):null;
    document.getElementById('spotline').textContent =
      `Spot: ${spot.XAUUSD!=null ? Number(spot.XAUUSD).toFixed(2)+' USD/oz' : '—'}`;

    const lastHist = rows.length? rows[rows.length-1].date : null;
    document.getElementById('sys-status').textContent =
      `Letztes History-Datum: ${lastHist||'—'} • Datenquelle: FRED, stooq`;

    // Forecasts (robust; bei <90 Punkten „—“)
    const horizons=[30,90,180];
    const $fc = document.getElementById('forecast');
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

    // Historische Analogien (optional, nur bei genug Daten)
    const $an = document.getElementById('analogs');
    if(rows.length>120){
      // 10T-Relative für alle Zeitpunkte
      const deltas = rows.map(r=>{
        const d = {timestamp:r.date};
        for(const k of Object.keys(latestDelta)) d[k]=null;
        return d;
      });
      for(let i=10;i<rows.length;i++){
        const win0 = rows[i-10], winN = rows[i], di = deltas[i];
        for(const k of Object.keys(latestDelta)){
          const vN = winN[k], v0 = win0[k];
          if(isFinite(vN)&&isFinite(v0)&&Math.abs(v0)>1e-9) di[k]=(vN-v0)/Math.abs(v0);
        }
      }
      // Z-Scores
      const refStats={};
      for(const k of Object.keys(latestDelt
