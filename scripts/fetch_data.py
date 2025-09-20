#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, sys, time, math
from pathlib import Path
from datetime import datetime, timezone
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FRED_SERIES = [
    "DFII10","DTWEXBGS","VIXCLS","DCOILBRENTEU","T10YIE",
    "BAMLH0A0HYM2","NAPM","RECPROUSM156N","T10Y2Y"
]
FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}"

STOOQ_XAUUSD_D = "https://stooq.com/q/d/l/?s=xauusd&i=d"   # USD per ounce (daily)

OZ_IN_GRAM = 31.1034768

def _download(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")

def _parse_stooq_csv(csv_text: str):
    # stooq daily CSV: date,open,high,low,close,volume
    lines = [l.strip() for l in csv_text.splitlines() if l.strip()]
    if len(lines) < 2:
        return None
    last = lines[-1].split(",")
    if len(last) < 5:
        return None
    date_iso = last[0]
    close = last[4]
    try:
        price_oz = float(close)
    except Exception:
        return None
    return date_iso, price_oz

def write_spot_json(xau_usd_per_oz: float, spot_date: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    usd_per_g = xau_usd_per_oz / OZ_IN_GRAM
    usd_per_kg = usd_per_g * 1000.0

    payload = {
        # Abwärts-kompatibel (wie bisher genutzt – USD pro Unze!)
        "timestamp": ts,
        "XAUUSD": round(xau_usd_per_oz, 4),

        # neue Hilfsfelder (Frontend nutzt diese für korrekte Anzeige/Berechnung)
        "source": "stooq.com (XAUUSD daily close)",
        "spot_date": spot_date,
        "usd_per_ounce": round(xau_usd_per_oz, 4),
        "usd_per_gram": round(usd_per_g, 6),
        "usd_per_kg": round(usd_per_kg, 2)
    }
    (DATA_DIR / "spot.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def write_history_json():
    # Minimal: wir lassen vorhandenen FRED-Logikteil bestehen (angenommen in deinem Repo),
    # hier nur Skelett, da Fokus auf Spot-Korrektur liegt.
    # Falls deine bestehende Version bereits sauber läuft, diesen Stub ignorieren.
    pass

def main():
    # 1) Spot laden (stooq XAUUSD = USD/oz)
    try:
        csv_text = _download(STOOQ_XAUUSD_D)
        parsed = _parse_stooq_csv(csv_text)
        if not parsed:
            raise RuntimeError("stooq XAUUSD parse failed")
        spot_date, xau_oz = parsed
        write_spot_json(xau_oz, spot_date)
        print(f"[fetch_data] spot OK: {xau_oz:.2f} USD/oz (date={spot_date})")
    except Exception as e:
        print(f"[fetch_data] spot failed: {e}", file=sys.stderr)
        # Schreibe neutrale Struktur, falls nötig
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fallback = {"timestamp": ts, "XAUUSD": None, "source": "stooq.com", "spot_date": None,
                    "usd_per_ounce": None, "usd_per_gram": None, "usd_per_kg": None}
        (DATA_DIR / "spot.json").write_text(json.dumps(fallback, ensure_ascii=False, indent=2), encoding="utf-8")

    # 2) Historie (belasse deine bestehende Implementierung)
    try:
        write_history_json()
    except Exception as e:
        print(f"[fetch_data] history skip/err: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
