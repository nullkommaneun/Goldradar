#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, sys
from pathlib import Path
from datetime import datetime, timezone
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# stooq: XAUUSD = USD pro Unze (daily close)
STOOQ_XAUUSD_D = "https://stooq.com/q/d/l/?s=xauusd&i=d"
OZ_IN_GRAM = 31.1034768

def _download(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")

def _parse_stooq_csv(csv_text: str):
    # Format: date,open,high,low,close,volume
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
        # Legacy-Feld beibehalten (USD pro Unze):
        "timestamp": ts,
        "XAUUSD": round(xau_usd_per_oz, 4),

        # Neue, bequeme Felder für das Frontend:
        "source": "stooq.com (XAUUSD daily close)",
        "spot_date": spot_date,
        "usd_per_ounce": round(xau_usd_per_oz, 4),
        "usd_per_gram": round(usd_per_g, 6),
        "usd_per_kg": round(usd_per_kg, 2)
    }
    (DATA_DIR / "spot.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

def main():
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
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fallback = {
            "timestamp": ts, "XAUUSD": None, "source": "stooq.com", "spot_date": None,
            "usd_per_ounce": None, "usd_per_gram": None, "usd_per_kg": None
        }
        (DATA_DIR / "spot.json").write_text(
            json.dumps(fallback, ensure_ascii=False, indent=2), encoding="utf-8"
        )

if __name__ == "__main__":
    main()
