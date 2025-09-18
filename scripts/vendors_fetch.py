#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Vendors-Autofetch – robuste Extraktion mit Bot-/Consent-Erkennung
Domains: philoro.de, degussa-goldhandel.de, heubach-edelmetalle.de, proaurum.de

Änderungen:
- Realistische Browser-Header + Accept-Language (de-DE)
- Erkennung von Consent-/Bot-Walls (Cloudflare, Consent, Captcha)
- Erweiterte Diagnostik: pages_blocked + blocked_examples
- Bestehende Parserpfade (JSON-LD/@graph, Offer.itemOffered, Micro/RDFa, OG, itemprop, JSON-Fallback) bleiben
"""

from __future__ import annotations
import json, re, time, sys
from pathlib import Path
from urllib.parse import urlparse
import urllib.robotparser as robotparser

import httpx
from lxml import html
try:
    import extruct
    from extruct.jsonld import JsonLdExtractor
    HAS_EXSTRUCT = True
except Exception:
    HAS_EXSTRUCT = False

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

WHITELIST = [
    "proaurum.de",
    "degussa-goldhandel.de",
    "heubach-edelmetalle.de",
    "philoro.de",
]

DOMAIN_SEEDS: dict[str, list[str]] = {
    "philoro.de": [
        "https://philoro.de/shop/goldbarren",
        "https://philoro.de/shop/goldbarren-100g",
        "https://philoro.de/shop/goldmuenzen-krugerrand",
    ],
}

# >>> realistische Headers
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

OZ_TO_G = 31.1034768
USD_PER_EUR_DEFAULT = 1.08
HTTP_TIMEOUT = 25.0

MAX_URLS_PER_DOMAIN = 220
MAX_SITEMAPS = 10
REQ_DELAY = 0.9  # höflich

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
        return rp.can_fetch(HEADERS.get("User-Agent","*"), url)
    except Exception:
        return True

# --------------------------- Blocker-Erkennung ----------------------------

BLOCK_PATTERNS = (
    r"consent", r"cookie", r"datenschutz", r"einwilligung",
    r"bot.?detect", r"access.?denied", r"captcha", r"cloudflare",
    r"just\sa\ssec(ond)?", r"verif(y|ikation)",
)

def looks_blocked(content_bytes: bytes) -> bool:
    try:
        # grob, aber schnell: nur Anfang der Seite prüfen
        text = content_bytes[:20000].decode("utf-8", "ignore").lower()
    except Exception:
        return False
    return any(re.search(p, text) for p in BLOCK_PATTERNS)

# ----------------------------- Discovery ----------------------------------

KW_PATH = (
    "produkt",      # philoro
    "gold", "goldmuenzen", "goldbarren",
    "barren", "bar", "muenze", "münze",
    "maple", "kruger", "krügerrand", "kruegerrand",
    "coin", "unze", "1oz", "100g", "100-g", "1-oz"
)

def looks_product_path_generic(path: str) -> bool:
    p = path.lower()
    return any(k in p for k in KW_PATH)

def looks_product_path(domain: str, path: str) -> bool:
    p = path.lower().rstrip("/")

    if domain == "philoro.de":
        if p.startswith("/produkt/") and len(p) > len("/produkt/") + 3:
            return True
        if p.startswith("/shop/") and "-" in p.split("/")[-1]:
            return True
        if p in ("/shop/goldbarren", "/shop/goldbarren-100g", "/shop/goldmuenzen-krugerrand"):
            return False
        return looks_product_path_generic(p)

    return looks_product_path_generic(p)

def discover_from_sitemaps(client: httpx.Client, domain: str) -> list[str]:
    urls=set()
    for sm in (f"https://{domain}/sitemap.xml", f"https://{domain}/sitemap_index.xml"):
        if not robots_ok(domain, sm): continue
        r = fetch(client, sm); time.sleep(REQ_DELAY)
        if not (r and r.status_code==200 and r.content): continue
        try:
            doc = html.fromstring(r.content)
            locs = [l for l in doc.xpath("//loc/text()") if isinstance(l,str)]
        except Exception:
            locs = []
        submaps = [u for u in locs if u.endswith(".xml")]
        for u in submaps[:MAX_SITEMAPS]:
            if not robots_ok(domain, u): continue
            r2 = fetch(client, u); time.sleep(REQ_DELAY)
            if not (r2 and r2.status_code==200 and r2.content): continue
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
        if r and r.status_code==200 and r.content:
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
            if r and r.status_code==200 and r.content:
                for u in extract_links_from_page(home, domain, r):
                    urls.add(u)
                    if len(urls) >= MAX_URLS_PER_DOMAIN: break

    return list(urls)[:MAX_URLS_PER_DOMAIN]

# ----------------------- Structured Data Parsing --------------------------

RE_PRICE_JSON = re.compile(r'"price"\s*:\s*"?(?P<price>[\d\.,]+)"?\s*[,}]', re.I)
RE_CURR_JSON  = re.compile(r'"priceCurrency"\s*:\s*"(?P<cur>[A-Z]{3})"', re.I)

def parse_structured(html_bytes: bytes, base_url: str) -> dict:
    data = {"products": [], "hints": {"jsonld":0, "micro_rdfa":0, "og":0, "itemprop":0, "json_fallback":0}}
    ext = {}
    if HAS_EXSTRUCT:
        try:
            ext = extruct.extract(html_bytes, base_url=base_url, syntaxes=["json-ld","microdata","rdfa"], uniform=True)
        except Exception:
            try:
                ext = {"json-ld": JsonLdExtractor().extract(html_bytes.decode("utf-8","ignore"))}
            except Exception:
                ext = {}

    def push_product(node: dict):
        if isinstance(node, dict):
            data["products"].append(node)

    # JSON-LD
    for node in ext.get("json-ld", []) or []:
        nodes = node if isinstance(node, list) else [node]
        for n in nodes:
            if not isinstance(n, dict): continue
            t = n.get("@type")
            if isinstance(n.get("@graph"), list):
                for g in n["@graph"]:
                    if not isinstance(g, dict): continue
                    gt = g.get("@type")
                    if gt == "Product" or (isinstance(gt, list) and "Product" in gt):
                        push_product(g); data["hints"]["jsonld"] += 1
                    if gt == "Offer" and isinstance(g.get("itemOffered"), dict):
                        prod = g["itemOffered"]; prod.setdefault("offers", g)
                        push_product(prod); data["hints"]["jsonld"] += 1
                continue
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                push_product(n); data["hints"]["jsonld"] += 1; continue
            if t == "Offer" or (isinstance(t, list) and "Offer" in t):
                if isinstance(n.get("itemOffered"), dict):
                    prod = n["itemOffered"]; prod.setdefault("offers", n)
                    push_product(prod); data["hints"]["jsonld"] += 1
            if n.get("@type") == "ItemList" and isinstance(n.get("itemListElement"), list):
                for it in n["itemListElement"]:
                    u = it.get("url") or (isinstance(it.get("item"), dict) and it["item"].get("@id"))
                    if isinstance(u, str):
                        data.setdefault("links", []).append(u)

    # Microdata/RDFa
    for syntax in ("microdata","rdfa"):
        for node in ext.get(syntax, []) or []:
            try:
                t = node.get("type") or node.get("@type")
                is_product = False
                if isinstance(t, list): is_product = any(isinstance(x,str) and x.lower().endswith("product") for x in t)
                elif isinstance(t, str): is_product = t.lower().endswith("product")
                if is_product:
                    props = node.get("properties") or {}
                    prod = {"@type":"Product",
                            "name": props.get("name"),
                            "description": props.get("description"),
                            "weight": props.get("weight"),
                            "offers": props.get("offers")}
                    push_product(prod); data["hints"]["micro_rdfa"] += 1
            except Exception:
                continue

    # OpenGraph-Product
    try:
        doc = html.fromstring(html_bytes)
        og_price = doc.xpath("//meta[@property='product:price:amount']/@content")
        og_curr  = doc.xpath("//meta[@property='product:price:currency']/@content")
        if og_price:
            prod = {"@type":"Product", "name": (doc.xpath('//meta[@property="og:title"]/@content') or [""])[0].strip()}
            prod["offers"] = {"@type":"Offer", "price": og_price[0], "priceCurrency": (og_curr[0].upper() if og_curr else "EUR")}
            push_product(prod); data["hints"]["og"] += 1
    except Exception:
        pass

    # itemprop-Fallback
    try:
        doc = html.fromstring(html_bytes)
        price_candidates = []
        price_candidates += doc.xpath('//*[@itemprop="price"]/@content')
        price_candidates += doc.xpath('string((//*[@itemprop="price"])[1])')
        cur_candidates = []
        cur_candidates += doc.xpath('//*[@itemprop="priceCurrency"]/@content')
        cur_candidates += doc.xpath('string((//*[@itemprop="priceCurrency"])[1])')
        price_candidates = [p.strip() for p in price_candidates if isinstance(p, str) and p.strip()]
        cur_candidates = [c.strip().upper() for c in cur_candidates if isinstance(c, str) and c.strip()]
        if price_candidates:
            prod = {"@type":"Product", "name": (doc.xpath("//h1/text()") or [""])[0].strip()}
            prod["offers"] = {"@type":"Offer", "price": price_candidates[0], "priceCurrency": (cur_candidates[0] if cur_candidates else "EUR")}
            push_product(prod); data["hints"]["itemprop"] += 1
    except Exception:
        pass

    # JSON-RegEx-Fallback
    try:
        doc = html.fromstring(html_bytes)
        for s in doc.xpath("//script/text()"):
            if "price" not in s: continue
            m1 = RE_PRICE_JSON.search(s)
            m2 = RE_CURR_JSON.search(s)
            if m1 and m2:
                prod = {"@type":"Product", "name": (doc.xpath("//h1/text()") or [""])[0].strip()}
                prod["offers"] = {"@type":"Offer", "price": m1.group("price"), "priceCurrency": m2.group("cur")}
                push_product(prod); data["hints"]["json_fallback"] += 1
                break
    except Exception:
        pass

    return data

# ---------------------- Normalisierung & Klassifikation --------------------

def as_float(x):
    try:
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".").strip()
        return float(x)
    except Exception:
        return None

RE_G  = re.compile(r"(\d{1,4}(?:[\,\.]\d+)?)\s*g\b", re.I)
RE_OZ = re.compile(r"(\d{1,2}(?:[\,\.]\d+)?)\s*(oz|unze)", re.I)

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
    if m:
        val = as_float(m.group(1))
        if val is not None: return val
    m = RE_OZ.search(name)
    if m:
        val = as_float(m.group(1))
        if val is not None: return val * OZ_TO_G
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

    price = as_float(offer.get("price") or offer.get("lowPrice") or offer.get("highPrice"))
    if price is None and isinstance(offer.get("priceSpecification"), dict):
        price = as_float(offer["priceSpecification"].get("price"))

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
            "totals": {"domains": 0, "pages": 0, "products": 0, "offers": 0, "items": 0, "pages_blocked": 0},
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
            dstat = {
                "domain": domain, "pages": 0, "products": 0, "offers": 0, "items": 0, "notes": [],
                "pages_with_jsonld": 0, "pages_with_micro": 0, "pages_with_og": 0, "pages_with_fallback": 0,
                "pages_blocked": 0,
                "examples": {"jsonld": [], "micro": [], "og": [], "fallback": [], "blocked": []}
            }
            out["diagnostics"]["domains"].append(dstat)
            out["diagnostics"]["totals"]["domains"] += 1

            vendor = {"domain": domain, "trust": 98 if domain == "philoro.de" else 90, "items": []}
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

                # Consent/Bot-Wall?
                if looks_blocked(r.content):
                    dstat["pages_blocked"] += 1
                    out["diagnostics"]["totals"]["pages_blocked"] += 1
                    if len(dstat["examples"]["blocked"]) < 3:
                        dstat["examples"]["blocked"].append(u)
                    # trotzdem zählen wir die Seite, aber ohne Parsing
                    dstat["pages"] += 1
                    continue

                dstat["pages"] += 1

                parsed = parse_structured(r.content, u)
                products = parsed.get("products") or []
                hints = parsed.get("hints") or {}
                if hints.get("jsonld"): 
                    dstat["pages_with_jsonld"] += 1
                    if len(dstat["examples"]["jsonld"]) < 3: dstat["examples"]["jsonld"].append(u)
                if hints.get("micro_rdfa"): 
                    dstat["pages_with_micro"] += 1
                    if len(dstat["examples"]["micro"]) < 3: dstat["examples"]["micro"].append(u)
                if hints.get("og"): 
                    dstat["pages_with_og"] += 1
                    if len(dstat["examples"]["og"]) < 3: dstat["examples"]["og"].append(u)
                if hints.get("itemprop") or hints.get("json_fallback"):
                    dstat["pages_with_fallback"] += 1
                    if len(dstat["examples"]["fallback"]) < 3: dstat["examples"]["fallback"].append(u)

                dstat["products"] += len(products)

                # ItemList-Fanout begrenzen
                for link in (parsed.get("links") or [])[:6]:
                    if link in seen: continue
                    if not robots_ok(domain, link): continue
                    r2 = fetch(client, link); time.sleep(REQ_DELAY)
                    seen.add(link)
                    if not (r2 and r2.status_code==200 and r2.content): 
                        continue
                    if looks_blocked(r2.content):
                        dstat["pages_blocked"] += 1
                        out["diagnostics"]["totals"]["pages_blocked"] += 1
                        if len(dstat["examples"]["blocked"]) < 3:
                            dstat["examples"]["blocked"].append(link)
                        dstat["pages"] += 1
                        continue

                    dstat["pages"] += 1
                    parsed2 = parse_structured(r2.content, link)
                    ps = parsed2.get("products") or []
                    hints2 = parsed2.get("hints") or {}
                    if hints2.get("jsonld"): 
                        dstat["pages_with_jsonld"] += 1
                        if len(dstat["examples"]["jsonld"]) < 3: dstat["examples"]["jsonld"].append(link)
                    if hints2.get("micro_rdfa"): 
                        dstat["pages_with_micro"] += 1
                        if len(dstat["examples"]["micro"]) < 3: dstat["examples"]["micro"].append(link)
                    if hints2.get("og"): 
                        dstat["pages_with_og"] += 1
                        if len(dstat["examples"]["og"]) < 3: dstat["examples"]["og"].append(link)
                    if hints2.get("itemprop") or hints2.get("json_fallback"):
                        dstat["pages_with_fallback"] += 1
                        if len(dstat["examples"]["fallback"]) < 3: dstat["examples"]["fallback"].append(link)

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
                    # USD → EUR
                    if cur == "USD":
                        price = price / eurusd; cur = "EUR"
                    if cur != "EUR":
                        continue

                    item = {
                        "product": cls,
                        "name": name,
                        "weight_g": round(w_g, 3) if w_g else None,
                        "price": {"value": round(price, 2), "currency": "EUR", "shipping_included": offer["shipping_included"]},
                        "availability": offer["availability"] or "Unknown",
                        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "source": "structured_or_fallback",
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
            out["diagnostics"]["totals"]["pages"]         += dstat["pages"]
            out["diagnostics"]["totals"]["products"]      += dstat["products"]
            out["diagnostics"]["totals"]["offers"]        += dstat["offers"]
            out["diagnostics"]["totals"]["items"]         += dstat["items"]

            out["vendors"].append(vendor)

    (DATA_DIR / "vendors_auto.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote data/vendors_auto.json with", len(out["vendors"]), "vendors")
    print("Diagnostics:", json.dumps(out["diagnostics"], ensure_ascii=False))

if __name__ == "__main__":
    main()
