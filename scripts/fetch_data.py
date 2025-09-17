#!/usr/bin/env python3
import os, sys, json, csv, io, math, datetime as dt
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
        try:
            val = float(v)
        except:
            val = None
        out[d[:10]] = val
    return out

def fetch_stooq_spot():
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
        ts = f"{date}T{(time_ if len(time_)==8 else '00:00:00')}Z"
        return {"timestamp": ts, "XAUUSD": px}
    except Exception:
        return {"timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "XAUUSD": None}

def fetch_stooq_history():
    """
    Lädt tägliche Historie XAUUSD (close) von stooq.
    Rückgabe: dict[YYYY-MM-DD] = float|None
    """
    url = "https://stooq.com/q/d/l/?s=xauusd&i=d"
    try:
        data = http_get(url).decode("utf-8", errors="ignore")
        rdr = csv.reader(io.StringIO(data))
        header = next(rdr, None)  # ["Date","Open","High","Low","Close","Volume"]
        out = {}
        for row in rdr:
            if not row or len(row) < 5:
                continue
            d = row[0][:10]
            try:
                close = float(row[4])
            except:
                close = None
            out[d] = close
        return out
    except Exception:
        return {}

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    today = dt.date.today()
    start = today - dt.timedelta(days=365*20 + 30)
    start_str = start.isoformat()

    # FRED Serien
    frames = {}
    for sid in SERIES:
        print(f"FRED {sid} …", file=sys.stderr)
        frames[sid] = fetch_fred_series(sid, start_str)

    # stooq GOLD Historie (Backfill)
    stq_gold = fetch_stooq_history()
    print(f"stooq GOLD datapoints: {len(stq_gold)}", file=sys.stderr)

    # Alle Datums sammeln
    all_dates = set()
    for sid, m in frames.items():
        all_dates.update(m.keys())
    all_dates.update(stq_gold.keys())
    if not all_dates:
        raise RuntimeError("Keine Daten empfangen (FRED/stooq).")
    dates = sorted([d for d in all_dates if d >= start_str])

    # Compose rows + Backfill GOLD
    history = []
    valid_gold = 0
    for d in dates:
        row = {"timestamp": d}
        # GOLD von FRED oder stooq
        fred_gold = frames.get("GOLDAMGBD228NLBM", {}).get(d, None)
        if fred_gold is None:
            fred_gold = stq_gold.get(d, None)
        if isinstance(fred_gold, float) and not math.isnan(fred_gold):
            valid_gold += 1
        row["GOLDAMGBD228NLBM"] = (None if fred_gold is None or (isinstance(fred_gold,float) and math.isnan(fred_gold)) else fred_gold)
        # Rest
        for sid in SERIES:
            if sid == "GOLDAMGBD228NLBM":
                continue
            v = frames.get(sid, {}).get(d, None)
            row[sid] = (None if v is None or (isinstance(v,float) and math.isnan(v)) else v)
        history.append(row)

    # Schreiben
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump({"history": history}, f, ensure_ascii=False)

    spot = fetch_stooq_spot()
    with open(SPOT_PATH, "w", encoding="utf-8") as f:
        json.dump(spot, f, ensure_ascii=False)

    print(f"Wrote {HISTORY_PATH} ({len(history)} Zeilen, GOLD valid={valid_gold}) und {SPOT_PATH}", file=sys.stderr)

if __name__ == "__main__":
    main()
