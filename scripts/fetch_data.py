#!/usr/bin/env python3
"""
Robustes Datenskript für Gold-Kauf-Signal.

- Holt FRED-Serien (mit Retries), toleriert Ausfälle (liefert dann leere Spalten -> nulls).
- Holt GOLD-Historie redundant von stooq (mehrere Domains/Endpunkte) und merged sie.
- Schreibt:
    data/history.json  (vereinte tägliche Historie ~20 Jahre)
    data/spot.json     (aktueller XAUUSD-Spot)
    data/diag.json     (Diagnosezahlen & genutzte Quellen)

Hinweise:
- Keine künstliche Interpolation/Forward-Fill. Fehlende Werte bleiben null.
- Forecast im Frontend benötigt >=90 valide GOLD-Punkte. Das ist Ziel von gold_backfill.

Autor: Dein Projekt
"""

import os
import sys
import io
import csv
import json
import math
import time
import datetime as dt
from urllib.request import urlopen, Request
from urllib.parse import urlencode

# ------------------- Konfiguration -------------------
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
SERIES = [
    "GOLDAMGBD228NLBM",  # LBMA-Gold USD/oz (FRED; oft lückig)
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
OUT_DIR = os.path.normpath(os.path.join(ROOT, "..", "data"))
HISTORY_PATH = os.path.join(OUT_DIR, "history.json")
SPOT_PATH = os.path.join(OUT_DIR, "spot.json")
DIAG_PATH = os.path.join(OUT_DIR, "diag.json")

START_DAYS_BACK = 365 * 20 + 30  # ~20 Jahre + Puffer
USER_AGENT = "gold-kaufsignal/1.1 (+github pages bot)"

# ------------------- HTTP-Helfer -------------------
def http_get(url: str, timeout: int = 30) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as r:
        return r.read()

def try_urls(urls, parse_fn, retries=3, sleep_s=0.8, timeout=30):
    """
    Versucht nacheinander mehrere URLs. Für jede URL bis zu 'retries' Versuche.
    Gibt beim ersten erfolgreichen Parse das Ergebnis zurück.
    """
    last_err = None
    for url in urls:
        for attempt in range(1, retries + 1):
            try:
                raw = http_get(url, timeout=timeout)
                return parse_fn(raw), url, attempt
            except Exception as e:
                last_err = e
                time.sleep(sleep_s)
    if last_err:
        raise last_err
    raise RuntimeError("No URL attempted")

# ------------------- Parser -------------------
def parse_fred_json(raw: bytes):
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
        except Exception:
            val = None
        out[d] = val
    return out

def parse_stooq_daily_csv(raw: bytes):
    """
    Erwartet ein CSV mit Spalten: Date, Open, High, Low, Close, Volume
    Liefert dict[YYYY-MM-DD] = float|None (Close)
    """
    txt = raw.decode("utf-8", errors="ignore")
    rdr = csv.reader(io.StringIO(txt))
    # Header line (überspringen, falls vorhanden)
    header = next(rdr, None)
    out = {}
    for row in rdr:
        if not row or len(row) < 5:
            continue
        d = (row[0] or "")[:10]
        if not d:
            continue
        try:
            close = float(row[4])
        except Exception:
            close = None
        out[d] = close
    return out

def parse_stooq_spot_csv(raw: bytes):
    """
    Einzeiliges CSV: symbol,date,time,open,high,low,close,volume
    """
    txt = raw.decode("utf-8", errors="ignore")
    rdr = csv.DictReader(io.StringIO(txt))
    row = next(rdr, None)
    if not row:
        raise RuntimeError("stooq spot empty")
    date = row.get("Date") or row.get("date") or ""
    time_ = row.get("Time") or row.get("time") or "00:00:00"
    close = row.get("Close") or row.get("close")
    try:
        px = float(close)
    except Exception:
        px = None
    ts = f"{date[:10]}T{(time_ if len(time_)==8 else '00:00:00')}Z"
    return {"timestamp": ts, "XAUUSD": px}

# ------------------- Quellen-Clients -------------------
def fetch_fred_series(series_id: str, start_date: str, retries=3, sleep_s=0.8):
    """
    Holt eine FRED-Serie. Bricht NICHT hart ab, wenn FRED_API_KEY fehlt.
    Gibt dict[YYYY-MM-DD] = float|None zurück (evtl. leer).
    """
    if not FRED_API_KEY:
        return {}

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date
    }
    url = FRED_BASE + "?" + urlencode(params)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            raw = http_get(url, timeout=30)
            return parse_fred_json(raw)
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)
    # Tolerant: leeres Mapping bei Fehler
    return {}

def fetch_stooq_gold_history():
    """
    Holt GOLD-Historie von mehreren stooq-Domains/Endpunkten und mergen.
    """
    urls = [
        "https://stooq.com/q/d/l/?s=xauusd&i=d",
        "https://stooq.pl/q/d/l/?s=xauusd&i=d",
        # alternative "adjusted"-Pfadvarianten, liefern oft denselben Inhalt
        "https://stooq.com/q/a/?s=xauusd&i=d",
        "https://stooq.pl/q/a/?s=xauusd&i=d",
    ]
    used = []
    merged = {}

    for u in urls:
        try:
            part, used_url, _ = try_urls([u], parse_stooq_daily_csv, retries=3, sleep_s=0.6)
            used.append(used_url)
            # Merge bevorzugt vorhandene Werte
            for d, v in part.items():
                if d not in merged or merged[d] is None:
                    merged[d] = v
        except Exception:
            # weiter probieren – wir wollen robust sein
            continue

    return merged, used

def fetch_stooq_spot():
    urls = [
        "https://stooq.com/q/l/?s=xauusd&f=sd2t2ohlcv&h&e=csv",
        "https://stooq.pl/q/l/?s=xauusd&f=sd2t2ohlcv&h&e=csv",
    ]
    try:
        data, used_url, _ = try_urls(urls, parse_stooq_spot_csv, retries=3, sleep_s=0.6)
        data["_source"] = used_url
        return data
    except Exception:
        # Fallback: Null-Spot, aber gültiger Zeitstempel
        return {"timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "XAUUSD": None, "_source": None}

# ------------------- Main-Pipeline -------------------
def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    today = dt.date.today()
    start = today - dt.timedelta(days=START_DAYS_BACK)
    start_str = start.isoformat()

    diag = {
        "start": start_str,
        "series_counts": {},
        "gold_sources": [],
        "gold_backfill": 0,
        "gold_valid": 0,
        "rows": 0,
        "notes": []
    }

    # 1) FRED-Serien (tolerant)
    fred_frames = {}
    for sid in SERIES:
        fred_frames[sid] = fetch_fred_series(sid, start_str)
        diag["series_counts"][sid] = len(fred_frames[sid])

    # 2) GOLD-Historie (stooq) – redundanter Backfill
    stooq_gold, gold_used_sources = fetch_stooq_gold_history()
    diag["gold_sources"] = gold_used_sources
    diag["gold_backfill"] = len(stooq_gold)

    # 3) Datumssuperset
    all_dates = set(stooq_gold.keys())
    for sid, m in fred_frames.items():
        all_dates.update(m.keys())

    if not all_dates:
        # Schreibe leere Dateien + Diag, ohne hart zu scheitern
        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump({"history": []}, f, ensure_ascii=False)
        with open(SPOT_PATH, "w", encoding="utf-8") as f:
            json.dump(fetch_stooq_spot(), f, ensure_ascii=False)
        with open(DIAG_PATH, "w", encoding="utf-8") as f:
            json.dump(diag, f, ensure_ascii=False)
        print("WARN: Keine Daten empfangen (FRED+stooq beide leer).", file=sys.stderr)
        return

    # Relevanter Zeitraum
    dates = sorted(d for d in all_dates if d >= start_str)

    # 4) Zeilen komponieren (GOLD: FRED → stooq fallback)
    history = []
    gold_valid = 0
    for d in dates:
        row = {"timestamp": d}

        # GOLD
        g = fred_frames.get("GOLDAMGBD228NLBM", {}).get(d)
        if g is None:
            g = stooq_gold.get(d)
        if isinstance(g, float) and not math.isnan(g):
            gold_valid += 1
        row["GOLDAMGBD228NLBM"] = (None if g is None or (isinstance(g, float) and math.isnan(g)) else g)

        # Restliche Serien 1:1 (evtl. None)
        for sid in SERIES:
            if sid == "GOLDAMGBD228NLBM":
                continue
            v = fred_frames.get(sid, {}).get(d)
            row[sid] = (None if v is None or (isinstance(v, float) and math.isnan(v)) else v)

        history.append(row)

    # 5) Schreiben
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump({"history": history}, f, ensure_ascii=False)

    spot = fetch_stooq_spot()
    with open(SPOT_PATH, "w", encoding="utf-8") as f:
        json.dump(spot, f, ensure_ascii=False)

    diag["rows"] = len(history)
    diag["gold_valid"] = gold_valid
    with open(DIAG_PATH, "w", encoding="utf-8") as f:
        json.dump(diag, f, ensure_ascii=False)

    # Konsolenhinweise (für Actions-Logs)
    print(
        f"Wrote data: rows={len(history)}, gold_valid={gold_valid}, gold_backfill={len(stooq_gold)}; "
        f"sources={','.join(gold_used_sources) if gold_used_sources else '—'}",
        file=sys.stderr
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Failsafe: nie lautlos sterben – schreibe minimaldiagnose
        try:
            os.makedirs(OUT_DIR, exist_ok=True)
            if not os.path.exists(HISTORY_PATH):
                with open(HISTORY_PATH, "w", encoding="utf-8") as f:
                    json.dump({"history": []}, f, ensure_ascii=False)
            if not os.path.exists(SPOT_PATH):
                with open(SPOT_PATH, "w", encoding="utf-8") as f:
                    json.dump({"timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "XAUUSD": None}, f, ensure_ascii=False)
            with open(DIAG_PATH, "w", encoding="utf-8") as f:
                json.dump({"error": str(e)}, f, ensure_ascii=False)
        finally:
            raise
