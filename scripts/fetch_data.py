#!/usr/bin/env python3
return None
last = rows[-1]
close = last.get("Close") or last.get("close")
try:
px = float(close)
except (TypeError, ValueError):
px = None
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
return {"timestamp": ts, "XAUUSD": px}




def main():
api_key = os.environ.get("FRED_API_KEY")
if not api_key:
print("ERROR: FRED_API_KEY env var missing", file=sys.stderr)
sys.exit(1)


# Sammle alle Reihen
timelines = {"date_set": set()}
for sid in FRED_SERIES:
try:
data = fetch_fred(sid, api_key)
except Exception as e:
print(f"WARN: failed {sid}: {e}")
data = {}
timelines[sid] = data
timelines["date_set"].update(data.keys())


dates = sorted(timelines["date_set"]) # ISO‑Strings YYYY‑MM‑DD
history = []
for d in dates:
row = {"timestamp": d}
for sid in FRED_SERIES:
v = timelines[sid].get(d, None)
row[sid] = v if (isinstance(v, float) or v is None) else None
history.append(row)


os.makedirs("data", exist_ok=True)
with open("data/history.json", "w", encoding="utf-8") as f:
json.dump({"history": history}, f, ensure_ascii=False)


spot = fetch_stooq_xauusd() or {"timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "XAUUSD": None}
with open("data/spot.json", "w", encoding="utf-8") as f:
json.dump(spot, f, ensure_ascii=False)


print(f"Wrote {len(history)} rows to data/history.json; spot={spot.get('XAUUSD')}")




if __name__ == "__main__":
main()
