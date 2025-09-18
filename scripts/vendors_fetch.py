#!/usr/bin/env python3
"""
Vendors-Autofetch für seriöse Händler (inkl. philoro.de)
- Discovery: Sitemaps + Category-Seeds + Domain-Heuristiken
- Strukturierte Daten: JSON-LD, Microdata, RDFa (extruct) + Fallback-JSON-LD
- Produkt-/Offer-Normalisierung inkl. priceSpecification & @graph
- Premium ggü. Spot (EUR/g) auf Basis von spot.json + ECB EURUSD
- Diagnostics in vendors_auto.json
"""

from __future__ import annotations
import json, re, time, sys
from pathlib import Path
from urllib.parse import urlparse
import urllib.robotparser as robotparser

import httpx
from lxml import html
import extruct
from extruct.jsonld import JsonLdExtractor

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------- Konfiguration / Domains -------------------------

WHITELIST = [
    "proaurum.de",
    "degussa-goldhandel.de",
    "heubach-edelmetalle.de",
    "philoro.de",  # neu
]

DOMAIN_SEEDS: dict[str, list[str]] = {
    "philoro.de": [
        "https://philoro.de/shop/goldbarren",
        "https://philoro.de/shop/goldmuenzen-krugerrand",
        "https://philoro.de/shop/goldbarren-100g",
    ],
}

HEADERS = {
    "User-Agent": "GoldKaufSignalBot/1.0 (+https://github.com/nullkommaneun/Goldradar)"
}

OZ_TO_G = 31.1034768
USD_PER_EUR_DEFAULT = 1.08
HTTP_TIMEOUT = 20.0

MAX_URLS_PER_DOMAIN = 200
MAX_SITEMAPS = 10
REQ_DELAY = 0.9  # höflicher Crawl

# ----------------------------- Helpers ------------------------------------

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

# robots.txt
_robots_cache: dict[str, robotparser.RobotFileParser] = {}
def robots_ok(domain: str, url: str) -> bool:
    try:
        rp = _robots_cache.get(domain)
        if rp is None:
            rp = robotparser.RobotFileParser()
            rp.set_url(f"https://{domain}/robots.txt")
            try: rp.read()
            except Exception:
                _robots_cache[domain] = rp
                return not any(seg in url for seg in ("/wp-admin", "/admin", "/cart"))
            _robots_cache[domain] = rp
        return rp.can_fetch(HEADERS["User-Agent"], url)
    except Exception:
        return True

# ----------------------------- Discovery ----------------------------------

KW_PATH = (
    "gold", "goldmuenzen", "goldbarren",  # erweitert
    "barren", "bar", "muenze", "münze",
    "maple", "kruger", "krügerrand", "kruegerrand",
    "coin", "unze", "1oz", "100g", "100-g", "1-oz"
)

def looks_product_path_generic(path: str) -> bool:
    p = path.lower()
    return any(k in p for k in KW_PATH)

def looks_product_path(domain: str, path: str) -> bool:
    """Domain-spezifische Erkennung von Produkt-Detailseiten."""
    p = path.lower().rstrip("/")
    if domain == "philoro.de":
        # philoro: Produktseiten liegen meist unter /shop/<slug...> mit Bindestrichen und ohne weitere Kategorie
        # Beispiel: /shop/goldbarren-100g/philoro-goldbarren-100g
        if not p.startswith("/shop"):
            return False
        # Kategorie-Roots ausschließen (genau die Seeds)
        if p in ("/shop/goldbarren", "/shop/goldmuenzen-krugerrand", "/shop/goldbarren-100g"):
            return False
        # Produkt-Detail hat typischerweise einen weiteren Segmentteil mit Bindestrich
        last = p.split("/")[-1]
        if "-" in last and len(last) >= 6:
            return True
        # sicherheitshalber: /shop/goldmuenzen-* /shop/goldbarren-* als Kandidaten
        if "/shop/goldmuenzen-" in p or "/shop/goldbarren-" in p:
            return True
        return looks_product_path_generic(p)
    # Default
    return looks_product_path_generic(p)

def discover_from_sitemaps(client: httpx.Client, domain: str) -> list[str]:
    urls=set()
    for sm in (f"https://{domain}/sitemap.xml", f"https://{domain}/sitemap_index.xml"):
        if not robots_ok(domain, sm): continue
        r = fetch(client, sm); time.sleep(REQ_DELAY)
        if not (r and r.status_code==200): continue
        try:
            doc = html.fromstring(r.content)
            locs = [l for l in doc.xpath("//loc/text()") if isinstance(l,str)]
        except Exception:
            locs = []
        submaps = [u for u in locs if u.endswith(".xml")]
        for u in submaps[:MAX_SITEMAPS]:
            if not robots_ok(domain, u): continue
            r2 = fetch(client, u); time.sleep(REQ_DELAY)
            if not (r2 and r2.status_code==200): continue
            try:
                doc2 = html.fromstring(r2.content)
                locs += [l for l in doc2.xpath("//loc/text()") if isinstance(l,str)]
            except Exception:
                pass
        for u in locs:
            pu = urlparse(u)
            if not pu.netloc.endswith(domain): continue
            if looks_product_path(domain, pu.path):
                urls.add(u)
                if len(urls) >= MAX_URLS_PER_DOMAIN: break
        if len(urls) >= MAX_URLS_PER_DOMAIN: break
    return list(urls)

def extract_links_from_page(base_url: str, domain: str, r: httpx.Response) -> list[str]:
    out=[]
    try:
        doc = html.fromstring(r.content)
        doc.make_links_absolute(base_url, resolve_base_href=True)
        for a in doc.xpath("//a[@href]/@href"):
            if not isinstance(a,str): continue
            pu = urlparse(a)
            if looks_product_path(domain, pu.path):
                out.append(a)
    except Exception:
        pass
    return out

def find_candidate_urls(client: httpx.Client, domain: str) -> list[str]:
    urls = set()

    # Seeds
    for seed in DOMAIN_SEEDS.get(domain, []):
        if len(urls) >= MAX_URLS_PER_DOMAIN: break
        if not robots_ok(domain, seed): continue
        r = fetch(client, seed); time.sleep(REQ_DELAY)
        if r and r.status_code==200:
            for u in extract_links_from_page(seed, domain, r):
                urls.add(u)
                if len(urls) >= MAX_URLS_PER_DOMAIN: break

    # Sitemaps
    if len(urls) < MAX_URLS_PER_DOMAIN:
        for u in discover_from_sitemaps(client, domain):
            urls.add(u)
            if len(urls) >= MAX_URLS_PER_DOMAIN: break

    # Home-Fallback
    if len(urls) < 20:
        home = f"https://{domain}/"
        if robots_ok(domain, home):
            r = fetch(client, home); time.sleep(REQ_DELAY)
            if r and r.status_code==200:
                for u in extract_links_from_page(home, domain, r):
                    urls.add(u)
                    if len(urls) >= MAX_URLS_PER_DOMAIN: break

    return list(urls)[:MAX_URLS_PER_DOMAIN]

# ----------------------- Structured Data Parsing --------------------------

def parse_structured(html_bytes: bytes, base_url: str) -> dict:
    """Sammelt Produkte aus JSON-LD/Microdata/RDFa. Erkennt auch @graph und Offer.itemOffered."""
    data = {"products":[]}
    try:
        ext = extruct.extract(
            html_bytes, base_url=base_url,
            syntaxes=["json-ld","microdata","rdfa"],
            uniform=True
        )
    except Exception:
        try:
            ext = {"json-ld": JsonLdExtractor().extract(html_bytes.decode("utf-8","ignore"))}
        except Exception:
            ext = {}

    def push_product(node: dict):
        if not isinstance(node, dict): return
        data["products"].append(node)

    # JSON-LD
    for node in ext.get("json-ld", []) or []:
        if not isinstance(node, (dict, list)): continue
        nodes = node if isinstance(node, list) else [node]
        for n in nodes:
            if not isinstance(n, dict): continue
            t = n.get("@type")
            # @graph kann mehrere Knoten enthalten
            if "@graph" in n and isinstance(n["@graph"], list):
                for g in n["@graph"]:
                    if isinstance(g, dict):
                        gt = g.get("@type")
                        if (isinstance(gt,str) and gt.lower()=="product") or (isinstance(gt,list) and any(isinstance(x,str) and x.lower()=="product" for x in gt)):
                            push_product(g)
                        # Offer mit itemOffered → Product
                        if (isinstance(gt,str) and gt.lower()=="offer") and isinstance(g.get("itemOffered"), dict):
                            prod = g["itemOffered"]; prod.setdefault("offers", g)
                            push_product(prod)
                continue

            # reines Product
            is_prod = (isinstance(t,str) and t.lower()=="product") or (isinstance(t,list) and any(isinstance(x,str) and x.lower()=="product" for x in t))
            if is_prod:
                push_product(n)
                continue

            # Offer → itemOffered als Product
            is_offer = (isinstance(t,str) and t.lower()=="offer") or (isinstance(t,list) and any(isinstance(x,str) and x.lower()=="offer" for x in t))
            if is_offer and isinstance(n.get("itemOffered"), dict):
                prod = n["itemOffered"]; prod.setdefault("offers", n)
                push_product(prod)

            # ItemList (Kategorie) → Links einsammeln
            if n.get("@type") == "ItemList" and isinstance(n.get("itemListElement"), list):
                for it in n["itemListElement"]:
                    u = it.get("url") or (isinstance(it.get("item"), dict) and it["item"].get("@id"))
                    if isinstance(u,str):
                        data.setdefault("links",[]).append(u)

    # Microdata / RDFa vereinheitlichen
    for syntax in ("microdata","rdfa"):
        for node in ext.get(syntax, []) or []:
            try:
                t = node.get("type") or node.get("@type")
                is_product = False
                if isinstance(t, list): is_product = any(isinstance(x,str) and x.lower().endswith("product") for x in t)
                elif isinstance(t,str): is_product = t.lower().endswith("product")
                if is_product:
                    props = node.get("properties") or {}
                    prod = {"@type":"Product",
                            "name": props.get("name"),
                            "description": props.get("description"),
                            "weight": props.get("weight"),
                            "offers": props.get("offers")}
                    push_product(prod)
            except Exception:
                continue

    # Fallback: rohe <script type="application/ld+json">
    try:
        doc = html.fromstring(html_bytes)
        for s in doc.xpath("//script[@type='application/ld+json']/text()"):
            try:
                j = json.loads(s)
            except Exception:
                continue
            nodes = j if isinstance(j, list) else [j]
            for n in nodes:
                if not isinstance(n, dict): continue
                if "@graph" in n and isinstance(n["@graph"], list):
                    for g in n["@graph"]:
                        if isinstance(g, dict):
                            gt = g.get("@type")
                            if (gt == "Product") or (isinstance(gt, list) and "Product" in gt):
                                push_product(g)
                            if gt == "Offer" and isinstance(g.get("itemOffered"), dict):
                                prod = g["itemOffered"]; prod.setdefault("offers", g); push_product(prod)
                else:
                    gt = n.get("@type")
                    if gt == "Product" or (isinstance(gt, list) and "Product" in gt):
                        push_product(n)
                    if gt == "Offer" and isinstance(n.get("itemOffered"), dict):
                        prod = n["itemOffered"]; prod.setdefault("offers", n); push_product(prod)
    except Exception:
        pass

    return data

# ---------------------- Normalisierung (Produkt/Preis) --------------------

def as_float(x):
    try:
        if isinstance(x, str):
            x = x.replace(",", ".").strip()
        return float(x)
    except Exception:
        return None

RE_G  = re.compile(r"(\d{1,4}[\,\.]?\d*)\s*g\b", re.I)
RE_OZ = re.compile(r"(\d{1,2}([\,\.]\d+)?)\s*(oz|unze)", re.I)

def extract_weight_g(prod: dict) -> float | None:
    w = prod.get("weight")
    if isinstance(w, dict):
        v = as_float(w.get("value"))
        unit = (w.get("unitCode") or w.get("unitText") or "").lower()
        if v and (unit.startswith("grm") or "gram" in unit): return v
        if v and ("oz" in unit or "ounce" in unit): return v * OZ_TO_G
    if isinstance(w, (int, float)): return float(w)

    name = " ".join([str(prod.get("name") or ""), str(prod.get("description") or "")])
    m = RE_G.search(name)
    if m: return as_float(m.group(1))
    m = RE_OZ.search(name)
    if m: 
        val = as_float(m.group(1))
        return val * OZ_TO_G if val is not None else None
    return None

def classify_product(name: str, weight_g: float | None) -> str | None:
    n = (name or "").lower()
    if weight_g:
        if 95 <= weight_g <= 105:
            if any(k in n for k in ("barren", "bar", "cast", "linge", "tafel")): return "bar-100g"
        if 30 <= weight_g <= 32.5:
            if "maple" in n: return "coin-1oz-maple"
            if "kruger" in n or "krügerrand" in n or "kruegerrand" in n: return "coin-1oz-krugerrand"
            if any(k in n for k in ("unze","oz","coin","münze","muenze")): return "coin-1oz"
    if "maple" in n: return "coin-1oz-maple"
    if "kruger" in n or "krügerrand" in n or "kruegerrand" in n: return "coin-1oz-krugerrand"
    return None

def normalize_offer(offer) -> dict | None:
    if not isinstance(offer, dict): return None

    # Preis direkt
    price = as_float(offer.get("price") or offer.get("lowPrice") or offer.get("highPrice"))

    # priceSpecification
    if price is None and isinstance(offer.get("priceSpecification"), dict):
        price = as_float(offer["priceSpecification"].get("price"))

    # Währung
    cur = (offer.get("priceCurrency") or "").upper() or None
    if not cur and isinstance(offer.get("priceSpecification"), dict):
        cur = (offer["priceSpecification"].get("priceCurrency") or "").upper() or None

    avail = (offer.get("availability") or "").split("/")[-1] if offer.get("availability") else None
    ship_incl = None
    ship = offer.get("shippingDetails")
    if isinstance(ship, dict): ship_incl = True

    if price is None or not cur:
        return None
    return {"price": price, "currency": cur, "availability": avail, "shipping_included": ship_incl}

def best_offer(prod: dict) -> dict | None:
    offers = prod.get("offers")
    cands = []
    if isinstance(offers, list):
        for o in offers:
            cand = normalize_offer(o)
            if cand: cands.append(cand)
    elif isinstance(offers, dict):
        if offers.get("@type") == "AggregateOffer":
            low = as_float(offers.get("lowPrice")) or as_float((offers.get("offers") or [{}])[0].get("price"))
            cur = (offers.get("priceCurrency") or "").upper() or None
            if not cur and isinstance(offers.get("priceSpecification"), dict):
                cur = (offers["priceSpecification"].get("priceCurrency") or "").upper() or None
            if low and cur:
                cands = [{"price": low, "currency": cur, "availability": None, "shipping_included": None}]
        else:
            cand = normalize_offer(offers)
            if cand: cands.append(cand)

    cands = [c for c in cands if c and c["price"] and c["currency"]]
    if not cands: return None
    return sorted(cands, key=lambda x: x["price"])[0]

# --------------------------------- Main -----------------------------------

def main():
    out = {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "fx": {},
        "products": ["bar-100g","coin-1oz-maple","coin-1oz-krugerrand","coin-1oz"],
        "vendors": [],
        "diagnostics": {
            "totals": {"domains": 0, "pages": 0, "products": 0, "offers": 0, "items": 0},
            "domains": []
        }
    }

    spot_usd_per_kg = get_spot_usd_per_kg()
    with httpx.Client(http2=True) as client:
        eurusd = ecb_eurusd(client)
        out["fx"]["EURUSD"] = eurusd

        spot_eur_per_g = None
        if spot_usd_per_kg:
            usd_per_g = spot_usd_per_kg / 1000.0
            eur_per_g = usd_per_g / eurusd
            spot_eur_per_g = eur_per_g

        for domain in WHITELIST:
            dstat = {"domain": domain, "pages": 0, "products": 0, "offers": 0, "items": 0, "notes": []}
            out["diagnostics"]["domains"].append(dstat)
            out["diagnostics"]["totals"]["domains"] += 1

            vendor = {"domain": domain, "trust": 95 if domain == "philoro.de" else 90, "items": []}
            urls = find_candidate_urls(client, domain)
            seen = set()

            for u in urls:
                pu = urlparse(u)
                if pu.netloc and not pu.netloc.endswith(domain): continue
                if u in seen: continue
                seen.add(u)
                if not robots_ok(domain, u):
                    dstat["notes"].append(f"blocked robots: {u}")
                    continue

                r = fetch(client, u); time.sleep(REQ_DELAY)
                if not r or r.status_code != 200 or not r.content:
                    dstat["notes"].append(f"bad status: {u} ({getattr(r,'status_code',None)})")
                    continue
                dstat["pages"] += 1

                data = parse_structured(r.content, u)
                products = data.get("products") or []
                dstat["products"] += len(products)

                # ItemList-Links: kurz auflösen (begrenzter Fanout)
                for link in (data.get("links") or [])[:8]:
                    if link in seen: continue
                    if not robots_ok(domain, link): continue
                    r2 = fetch(client, link); time.sleep(REQ_DELAY)
                    seen.add(link)
                    if r2 and r2.status_code==200 and r2.content:
                        more = parse_structured(r2.content, link)
                        ps = more.get("products") or []
                        dstat["pages"] += 1
                        dstat["products"] += len(ps)
                        products.extend(ps)

                for prod in products:
                    name = (prod.get("name") or "").strip()
                    if not name: continue
                    w_g = extract_weight_g(prod)
                    cls = classify_product(name, w_g)
                    if not cls: continue
                    offer = best_offer(prod)
                    if not offer: continue
                    dstat["offers"] += 1

                    price = offer["price"]; cur = offer["currency"]
                    if cur == "USD":
                        price = price / eurusd; cur = "EUR"
                    if cur != "EUR":  # wir werten aktuell nur EUR aus
                        continue

                    item = {
                        "product": cls,
                        "name": name,
                        "weight_g": round(w_g, 3) if w_g else None,
                        "price": {"value": round(price, 2), "currency": "EUR", "shipping_included": offer["shipping_included"]},
                        "availability": offer["availability"] or "Unknown",
                        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "source": "structured",
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

            # bestes Angebot je Produkt
            best = {}
            for it in vendor["items"]:
                p = it["product"]
                if p not in best or (it.get("premium") is not None and (best[p].get("premium") is None or it["premium"] < best[p]["premium"])):
                    best[p] = it
            vendor["items"] = list(best.values())

            dstat["items"] += len(vendor["items"])
            out["diagnostics"]["totals"]["pages"]    += dstat["pages"]
            out["diagnostics"]["totals"]["products"] += dstat["products"]
            out["diagnostics"]["totals"]["offers"]   += dstat["offers"]
            out["diagnostics"]["totals"]["items"]    += dstat["items"]

            out["vendors"].append(vendor)

    (DATA_DIR / "vendors_auto.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote data/vendors_auto.json with", len(out["vendors"]), "vendors")
    print("Diagnostics:", json.dumps(out["diagnostics"], ensure_ascii=False))

if __name__ == "__main__":
    main()
