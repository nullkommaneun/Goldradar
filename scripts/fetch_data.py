#!/usr/bin/env python3
import os, sys, json, csv, io, math, time, datetime as dt
from urllib.request import urlopen, Request
from urllib.parse import urlencode

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
SERIES = [
    "GOLDAMGBD228NLBM",  # LBMA-Gold USD/oz (FRED, ggf. Lücken)
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

ROOT = os.path.dirname(__file__)
OUT_DIR = os.path.join(ROOT, "..", "data")
HISTORY_PATH = os.path.join(OUT_DIR, "history.json")
SPOT_PATH = os.path.join(OUT_DIR, "spot.json")
DIAG_PATH = os.path.join(OUT_DIR, "diag.json")

# ------------- HTTP utils -------------
def http_get(url, headers=None, timeout=30):
    req = Request(url, headers=headers or {"User-Agent":"gold-signal-bot/1.0"})
    with urlopen(req, timeout=timeout) as r:
        return r.read()

def get_with_retries(urls, parse_fn, retries=3, sleep_s=1.2):
    last_exc = None
    for url in urls:
        for attempt in range(1, retries+1):
            try:
                raw = http_get(url)
                return parse_fn(raw)
            except Exception as e:
                last_exc = e
                time.sleep(sleep_s)
    if last_exc:
        raise last_exc

# ------------- FRED -------------
def fetch_fred_series(series_id, start_date):
    if not FRED_API_KEY:
        # Für dein Setup: wir verzichten aufs Abbrechen; liefern leere Map,
        # damit UI neutral bleibt. Im diag.json wird das sichtbar.
        return {}
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
        d = (o.get("date") or "")[:10]
        v = o.get("value")
        if not d:
            continue
        try:
            val = float(v)
        except:
            val = None
        out[d] = val
    return out

# ------------- stooq -------------
def parse_stooq_daily_csv(raw_bytes):
    text = raw_bytes.decode("utf-8", errors="ignore")
    rdr = csv.reader(io.StringIO(text))
    header = next(rdr, None)  # e.g. ["Date","Open","High","Low","Close","Volume"]
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

def fetch_stooq_history():
    """Hole XAUUSD Historie von mehreren stooq Endpunkten und merge."""
    urls = [
        "https://stooq.com/q/d/l/?s=xauusd&i=d",
        "https://stooq.pl/q/d/l/?s=xauusd&i=d",
        # Alternative (Adjustments/anderer Pfad), manchmal liefert diese länger zurück:
        "https://stooq.com/q/a/?s=xauusd&i=d",
        "https://stooq.pl/q/a/?s=xauusd&i=d",
    ]
    merged = {}
    for u in urls:
        try:
            part = get_with_retries([u], parse_stooq_daily_csv, retries=3, sleep_s=0.8)
            for d, v in part.items():
                if d not in merged or merged[d] is None:
                    merged[d] = v
        except Exception:
            # still try others
            pass
    return merged

def fetch_stooq_spot():
    # last quote; wenn das mal ausfällt, geben wir null zurück
    urls = [
        "https://stooq.com/q/l/?s=xauusd&f=sd2t2ohlcv&h&e=csv",
        "https://stooq.pl/q/l/?s=xauusd&f=sd2t2ohlcv&h&e=csv",
    ]
    def parse(raw):
        data = raw.decode("utf-8", errors="ignore")
        rdr = csv.DictReader(io.StringIO(data))
        row = next(rdr, None)
        if not row:
            raise RuntimeError("empty stooq spot")
        date = row.get("Date") or row.get("date")
        time_ = row.get("Time") or row.get("time") or "00:00:00"
        close = row.get("Close") or row.get("close")
        try:
            px = float(close)
        except:
            px = None
        ts = f"{date}T{(time_ if time_ and len(time_)==8 else '00:00:00')}Z"
        return {"timestamp": ts, "XAUUSD": px}
    try:
        return get_with_retries(urls, parse, retries=3, sleep_s=0.8)
    except Exception:
        return {"timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "XAUUSD": None}

# ------------- Main -------------
def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    today = dt.date.today()
    start = today - dt.timedelta(days=365*20 + 30)  # ~20y + Puffer
    start_str = start.isoformat()

    diag = {"start": start_str, "series_counts": {}, "gold_backfill": 0}

    # 1) FRED Serien
    frames = {}
    for sid in SERIES:
        try:
            frames[sid] = fetch_fred_series(sid, start_str)
        except Exception as e:
            frames[sid] = {}
        diag["series_counts"][sid] = len(frames[sid])

    # 2) stooq GOLD Historie (Backfill)
    stq_gold = fetch_stooq_history()
    diag["gold_backfill"] = len(stq_gold)

    # 3) Unified Date-Set
    all_dates = set(stq_gold.keys())
    for sid, m in frames.items():
        all_dates.update(m.keys())

    if not all_dates:
        # Schreibe leere Files, aber klarmachen was los ist
        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump({"history": []}, f, ensure_ascii=False)
        with open(SPOT_PATH, "w", encoding="utf-8") as f:
            json.dump(fetch_stooq_spot(), f, ensure_ascii=False)
        with open(DIAG_PATH, "w", encoding="utf-8") as f:
            json.dump(diag, f, ensure_ascii=False)
        print("Keine Daten empfangen (FRED/stooq).", file=sys.stderr)
        return

    dates = sorted([d for d in all_dates if d >= start_str])

    # 4) Compose rows + Backfill GOLD
    history = []
    valid_gold = 0
    for d in dates:
        row = {"timestamp": d}
        # GOLD aus FRED, ansonsten stooq
        v_gold = frames.get("GOLDAMGBD228NLBM", {}).get(d, None)
        if v_gold is None:
            v_gold = stq_gold.get(d, None)
        if isinstance(v_gold, float) and not math.isnan(v_gold):
            valid_gold += 1
        row["GOLDAMGBD228NLBM"] = (None if v_gold is None or (isinstance(v_gold,float) and math.isnan(v_gold)) else v_gold)
        # Rest
        for sid in SERIES:
            if sid == "GOLDAMGBD228NLBM":
                continue
            v = frames.get(sid, {}).get(d, None)
            row[sid] = (None if v is None or (isinstance(v,float) and math.isnan(v)) else v)
        history.append(row)

    # 5) Schreiben
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump({"history": history}, f, ensure_ascii=False)

    spot = fetch_stooq_spot()
    with open(SPOT_PATH, "w", encoding="utf-8") as f:
        json.dump(spot, f, ensure_ascii=False)

    diag["rows"] = len(history)
    diag["gold_valid"] = valid_gold
    with open(DIAG_PATH, "w", encoding="utf-8") as f:
        json.dump(diag, f, ensure_ascii=False)

    print(f"Wrote {HISTORY_PATH} ({len(history)} rows, GOLD valid={valid_gold}) • stooq_backfill={len(stq_gold)}", file=sys.stderr)

if __name__ == "__main__":
    main()
