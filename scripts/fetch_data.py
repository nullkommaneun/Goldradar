#!/usr/bin/env python3
import os, sys, json, csv, io, math, time, datetime as dt
from urllib.request import urlopen, Request
from urllib.parse import urlencode

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
SERIES = [
    "GOLDAMGBD228NLBM",  # LBMA-Gold USD/oz
    "DFII10",            # 10Y Real Yield
    "DTWEXBGS",          # Dollar Index Broad
    "VIXCLS",            # VIX
    "DCOILBRENTEU",      # Brent
    "T10YIE",            # 10Y Breakeven
    "BAMLH0A0HYM2",      # HY Spreads
    "NAPM",              # PMI
    "RECPROUSM156N",     # Rezessionsrisiko
    "T10Y2Y"             # 10Y-2Y
]

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
HISTORY_PATH = os.path.join(OUT_DIR, "history.json")
SPOT_PATH = os.path.join(OUT_DIR, "spot.json")

def http_get(url, headers=None):
    req = Request(url, headers=headers or {"User-Agent":"gold-signal-bot/1.0"})
    with urlopen(req, timeout=30) as r:
        return r.read()

def fetch_fred_series(series_id, start_date):
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY nicht gesetzt.")
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date
    }
    url = FRED_BASE + "?" + urlencode(params)
    raw = http_get(url)
    j = json.loads(raw.decode("utf-8"))
    obs = j.get("observations", [])
    out = {}
    for o in obs:
        d = o.get("date")
        v = o.get("value")
        if not d: 
            continue
        # Monats- oder Tagesreihen – wir lassen ISO yyyy-mm-dd
        try:
            val = float(v)
        except:
            val = None
        out[d[:10]] = val
    return out

def fetch_stooq_spot():
    # stooq „last quote“ CSV: includes date,time,open,high,low,close,volume
    # f=sd2t2ohlcv (symbol,date,time,open,high,low,close,volume)
    url = "https://stooq.com/q/l/?s=xauusd&f=sd2t2ohlcv&h&e=csv"
    try:
        data = http_get(url).decode("utf-8", errors="ignore")
        rdr = csv.DictReader(io.StringIO(data))
        row = next(rdr, None)
        if not row:
            return {"timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "XAUUSD": None}
        date = row.get("Date") or row.get("date")
        time_ = row.get("Time") or row.get("time") or "00:00:00"
        close = row.get("Close") or row.get("close")
        try:
            px = float(close)
        except:
            px = None
        # build UTC timestamp; stooq times are exchange times; we record as-is in UTC for the app
        ts = f"{date}T{(time_ if len(time_)==8 else '00:00:00')}Z"
        return {"timestamp": ts, "XAUUSD": px}
    except Exception as e:
        return {"timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "XAUUSD": None}

def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    today = dt.date.today()
    start = today - dt.timedelta(days=365*20 + 30)  # ~20y + Puffer
    start_str = start.isoformat()

    # Pull all series
    frames = {}
    for sid in SERIES:
        print(f"FRED {sid} …", file=sys.stderr)
        frames[sid] = fetch_fred_series(sid, start_str)

    # Build unified history day-grid across all keys present
    all_dates = set()
    for sid, m in frames.items():
        all_dates.update(m.keys())
    if not all_dates:
        raise RuntimeError("Keine FRED-Daten empfangen.")
    dates = sorted([d for d in all_dates if d >= start_str])

    # Compose rows
    history = []
    for d in dates:
        row = {"timestamp": d}
        for sid in SERIES:
            v = frames.get(sid, {}).get(d, None)
            # Mark missing as None
            row[sid] = (None if v is None or (isinstance(v,float) and math.isnan(v)) else v)
        history.append(row)

    # Write history.json
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump({"history": history}, f, ensure_ascii=False)

    # Spot
    spot = fetch_stooq_spot()
    with open(SPOT_PATH, "w", encoding="utf-8") as f:
        json.dump(spot, f, ensure_ascii=False)

    print(f"Wrote {HISTORY_PATH} ({len(history)} Zeilen) und {SPOT_PATH}", file=sys.stderr)

if __name__ == "__main__":
    main()
