#!/usr/bin/env python3
"""
Vollautomatischer Vendor-Fetch:
- Respektiert robots.txt (Standardbibliothek urllib.robotparser)
- Nutzt strukturierte Daten (JSON-LD Product/Offer)
- Extrahiert Preise, Verfügbarkeit, Versandhinweise
- Normalisiert Gewicht → Produktklassen (bar-100g, coin-1oz-maple, coin-1oz-krugerrand, coin-1oz)
- Berechnet Premium ggü. Spot (USD/kg) + ECB EURUSD
- Schreibt data/vendors_auto.json
"""

from __future__ import annotations
import json, re, time, math, sys
from pathlib import Path
from urllib.parse import urlparse, urljoin
import urllib.robotparser as robotparser

import httpx
from lxml import html
import extruct
from extruct.jsonld import JsonLdExtractor

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Whitelist seriöser Domains – erweiterbar
WHITELIST = [
    "proaurum.de",
    "degussa-goldhandel.de",
    "heubach-edelmetalle.de",
]

HEADERS = {
    "User-Agent": "GoldKaufSignalBot (+https://github.com/%s)" % (sys.argv[0] or "repo")
}

OZ_TO_G = 31.1034768
USD_PER_EUR_DEFAULT = 1.08  # Fallback, falls ECB nicht erreichbar
HTTP_TIMEOUT = 20.0
MAX_URLS_PER_DOMAIN = 30
REQ_DELAY = 1.0  # s

def fetch(client: httpx.Client, url: str) -> httpx.Response | None:
    try:
        return client.get(url, timeout=HTTP_TIMEOUT, headers=HEADERS, follow_redirects=True)
    except Exception:
        return None

def ecb_eurusd(client: httpx.Client) -> float:
    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
    r = fetch(client, url)
    if not r or r.status_code != 200:
        return USD_PER_EUR_DEFAULT
    try:
        doc = html.fromstring(r.content)
        rate = doc.xpath("//Cube[@currency='USD']/@rate")
        if rate:
            return float(rate[0])
    except Exception:
        pass
    return USD_PER_EUR_DEFAULT

def get_spot_usd_per_kg() -> float | None:
    p = DATA_DIR / "spot.json"
    if not p.exists():
        return None
    try:
        j = json.loads(p.read_text(encoding="utf-8"))
        v = j.get("XAUUSD")
        return float(v) if isinstance(v, (int, float)) else None
    except Exception:
        return None

# ----- Robots.txt (stdlib) -----
_robots_cache: dict[str, robotparser.RobotFileParser] = {}
def robots_ok(domain: str, url: str) -> bool:
    try:
        rp = _robots_cache.get(domain)
        if rp is None:
            rp = robotparser.RobotFileParser()
            rp.set_url(f"https://{domain}/robots.txt")
            try:
                rp.read()
            except Exception:
                # bei Fehler: konservativ erlauben nur „normale“ Pfade
                _robots_cache[domain] = rp
                return not any(seg in url for seg in ("/wp-admin", "/admin", "/cart"))
            _robots_cache[domain] = rp
        return rp.can_fetch(HEADERS["User-Agent"], url)
    except Exception:
        return True

def find_candidate_urls(client: httpx.Client, domain: str) -> list[str]:
    urls = set()
    # sitemap.xml
    for sm in (f"https://{domain}/sitemap.xml", f"https://{domain}/sitemap_index.xml"):
        if not robots_ok(domain, sm):
            continue
        r = fetch(client, sm); time.sleep(REQ_DELAY)
        if r and r.status_code == 200 and (b"<urlset" in r.content or b"<sitemapindex" in r.content):
            try:
                doc = html.fromstring(r.content)
                locs = doc.xpath("//loc/text()")
                for u in locs:
                    if isinstance(u, str):
                        if any(tok in u.lower() for tok in ("gold", "barren", "bar", "maple", "kruger", "krügerrand", "coin", "unze", "1oz", "100g", "100-g")):
                            urls.add(u.strip())
                            if len(urls) >= MAX_URLS_PER_DOMAIN: break
                if len(urls) >= MAX_URLS_PER_DOMAIN: break
            except Exception:
                pass

    # Fallback: Startseite scannen
    if not urls:
        home = f"https://{domain}/"
        if robots_ok(domain, home):
            r = fetch(client, home); time.sleep(REQ_DELAY)
            if r and r.status_code == 200:
                try:
                    doc = html.fromstring(r.content)
                    for a in doc.xpath("//a/@href"):
                        if not isinstance(a, str): continue
                        u = urljoin(home, a)
                        pu = urlparse(u)
                        if pu.netloc.endswith(domain):
                            path = pu.path.lower()
                            if any(tok in path for tok in ("gold", "barren", "maple", "kruger", "krügerrand", "1oz", "100g")):
                                urls.add(u)
                                if len(urls) >= MAX_URLS_PER_DOMAIN: break
                except Exception:
                    pass
    return list(urls)[:MAX_URLS_PER_DOMAIN]

def parse_jsonld_products(html_bytes: bytes, base_url: str) -> list[dict]:
    try:
        data = extruct.extract(
            html_bytes, base_url=base_url, syntaxes=["json-ld"], uniform=True
        ).get("json-ld", [])
    except Exception:
        try:
            data = JsonLdExtractor().extract(html_bytes.decode("utf-8", "ignore"))
        except Exception:
            data = []

    products = []
    for node in data:
        if not isinstance(node, dict): continue
        types = node.get("@type")
        if isinstance(types, list):
            is_product = any(t.lower() == "product" for t in types if isinstance(t, str))
        else:
            is_product = isinstance(types, str) and types.lower() == "product"
        if not is_product:
            continue
        products.append(node)
    return products

def as_float(x):
    try:
        if isinstance(x, str):
            x = x.replace(",", ".").strip()
        return float(x)
    except Exception:
        return None

RE_G = re.compile(r"(\d{1,4}[\,\.]?\d*)\s*g\b", re.I)
RE_OZ = re.compile(r"(\d{1,2}([\,\.]\d+)?)\s*(oz|unze)", re.I)

def extract_weight_g(prod: dict) -> float | None:
    w = prod.get("weight")
    if isinstance(w, dict):
        v = as_float(w.get("value"))
        unit = (w.get("unitCode") or w.get("unitText") or "").lower()
        if v and (unit.startswith("grm") or "gram" in unit):
            return v
        if v and ("oz" in unit or "ounce" in unit):
            return v * OZ_TO_G
    if isinstance(w, (int, float)):
        return float(w)

    name = " ".join([str(prod.get("name") or ""), str(prod.get("description") or "")])
    m = RE_G.search(name)
    if m:
        return as_float(m.group(1))
    m = RE_OZ.search(name)
    if m:
        return as_float(m.group(1)) * OZ_TO_G
    return None

def classify_product(name: str, weight_g: float | None) -> str | None:
    n = (name or "").lower()
    if weight_g:
        if 95 <= weight_g <= 105:
            if any(k in n for k in ("barren", "bar", "cast", "linge", "tafel")):
                return "bar-100g"
        if 30 <= weight_g <= 32.5:
            if "maple" in n:
                return "coin-1oz-maple"
            if "kruger" in n or "krügerrand" in n:
                return "coin-1oz-krugerrand"
            if any(k in n for k in ("unze", "oz", "coin", "münze")):
                return "coin-1oz"
    if "maple" in n: return "coin-1oz-maple"
    if "kruger" in n or "krügerrand" in n: return "coin-1oz-krugerrand"
    return None

def normalize_offer(offer) -> dict | None:
    if not isinstance(offer, dict):
        return None
    price = as_float(offer.get("price") or offer.get("lowPrice") or offer.get("highPrice"))
    cur = (offer.get("priceCurrency") or "").upper() or None
    avail = (offer.get("availability") or "").split("/")[-1] if offer.get("availability") else None
    shipping_included = None
    ship = offer.get("shippingDetails")
    if isinstance(ship, dict):
        shipping_included = True
    return {"price": price, "currency": cur, "availability": avail, "shipping_included": shipping_included}

def best_offer(prod: dict) -> dict | None:
    offers = prod.get("offers")
    if isinstance(offers, list):
        cands = [normalize_offer(o) for o in offers]
    elif isinstance(offers, dict):
        if offers.get("@type") == "AggregateOffer":
            low = as_float(offers.get("lowPrice"))
            cur = (offers.get("priceCurrency") or "").upper() or None
            cands = [{"price": low, "currency": cur, "availability": None, "shipping_included": None}]
        else:
            cands = [normalize_offer(offers)]
    else:
        cands = []
    cands = [c for c in cands if c and c["price"] and c["currency"]]
    if not cands: return None
    return sorted(cands, key=lambda x: x["price"])[0]

def main():
    out = {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "fx": {},
        "products": ["bar-100g","coin-1oz-maple","coin-1oz-krugerrand","coin-1oz"],
        "vendors": []
    }

    spot_usd_per_kg = get_spot_usd_per_kg()  # aus data/spot.json
    with httpx.Client(http2=True) as client:
        eurusd = ecb_eurusd(client)  # USD pro EUR
        out["fx"]["EURUSD"] = eurusd

        spot_eur_per_g = None
        if spot_usd_per_kg:
            usd_per_g = spot_usd_per_kg / 1000.0
            eur_per_g = usd_per_g / eurusd
            spot_eur_per_g = eur_per_g

        for domain in WHITELIST:
            vendor = {"domain": domain, "trust": 90, "items": []}
            urls = find_candidate_urls(client, domain)
            seen = set()
            for u in urls:
                pu = urlparse(u)
                if pu.netloc and not pu.netloc.endswith(domain): continue
                if u in seen: continue
                if not robots_ok(domain, u): continue
                r = fetch(client, u); time.sleep(REQ_DELAY)
                seen.add(u)
                if not r or r.status_code != 200 or not r.content:
                    continue

                prods = parse_jsonld_products(r.content, u)
                for prod in prods:
                    name = prod.get("name") or ""
                    w_g = extract_weight_g(prod)
                    cls = classify_product(name, w_g)
                    if not cls:
                        continue
                    offer = best_offer(prod)
                    if not offer:
                        continue
                    price = offer["price"]
                    cur = offer["currency"]
                    if cur == "USD":
                        price = price / eurusd
                        cur = "EUR"
                    if cur != "EUR":
                        continue

                    item = {
                        "product": cls,
                        "name": name,
                        "weight_g": round(w_g, 3) if w_g else None,
                        "price": {"value": round(price, 2), "currency": "EUR", "shipping_included": offer["shipping_included"]},
                        "availability": offer["availability"] or "Unknown",
                        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "source": "jsonld",
                        "url": u
                    }

                    if spot_eur_per_g and w_g and spot_eur_per_g > 0:
                        fair = spot_eur_per_g * w_g
                        prem = (price / fair) - 1.0
                        if -0.2 <= prem <= 2.0:
                            item["premium"] = round(prem, 4)
                        else:
                            item["premium"] = None
                    vendor["items"].append(item)

            best_per_product = {}
            for it in vendor["items"]:
                p = it["product"]
                if p not in best_per_product or (it.get("premium") is not None and (best_per_product[p].get("premium") is None or it["premium"] < best_per_product[p]["premium"])):
                    best_per_product[p] = it
            vendor["items"] = list(best_per_product.values())
            out["vendors"].append(vendor)

    (DATA_DIR / "vendors_auto.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote data/vendors_auto.json with", len(out["vendors"]), "vendors")

if __name__ == "__main__":
    main()
