# Gold-Kauf-Signal (Client-only)

Eine minimalistische Web-App, die **in Klartext** anzeigt, ob heute ein guter Zeitpunkt für den Goldkauf ist – inkl. 30/90/180-Tage-Prognose, Treiber-Ampeln und historischem Vergleich.

## Live-Betrieb
- Statisch via **GitHub Pages** (kein Backend).
- Daten werden von **GitHub Actions** alle 2 Stunden aktualisiert.

## Setup

1. **Repo-Struktur**

2. **FRED API Key**
- Kostenlos unter https://fred.stlouisfed.org/ (Account → API Keys).
- „Describe the application“: z. B. *„Clientseitige GitHub-Pages-App zur Anzeige goldrelevanter Makro-Indikatoren (FRED) für Bildungs-/Analysezwecke.“*
- Im Repository als Secret `FRED_API_KEY` hinterlegen (Settings → Secrets and variables → Actions).

3. **GitHub Actions**
- Workflow `.github/workflows/build-data.yml` ist enthalten.
- Läuft alle 2 h und bei manuellem Trigger.

## Datenquellen
- **FRED**: 
- `GOLDAMGBD228NLBM`, `DFII10`, `DTWEXBGS`, `VIXCLS`, `DCOILBRENTEU`, `T10YIE`, `BAMLH0A0HYM2`, `NAPM`, `RECPROUSM156N`, `T10Y2Y`.
- **Spotpreis**: **stooq** `XAUUSD`.

## JSON-Schemata

### `data/history.json`
```json
{
"history": [
 {
   "timestamp": "YYYY-MM-DD",
   "GOLDAMGBD228NLBM": 1895.54,
   "DFII10": 1.79,
   "DTWEXBGS": 126.2,
   "VIXCLS": 14.2,
   "DCOILBRENTEU": 81.4,
   "T10YIE": 2.23,
   "BAMLH0A0HYM2": 3.97,
   "NAPM": 49.8,
   "RECPROUSM156N": 1.2,
   "T10Y2Y": -0.45
 }
]
}
