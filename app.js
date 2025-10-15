// ===================================================================================
// ZENTRALE KONFIGURATION
// ===================================================================================
const CONFIG = {
    // NEU: API-Schlüssel für goldapi.io
    GOLDAPI_KEY: 'goldapi-10qhje19mgsbzljs-io',
    GOLDAPI_BASE_URL: 'https://www.goldapi.io/api/',

    // GEÄNDERT: Umbenannt für Klarheit, wird weiterhin für ökonomische Daten genutzt
    ALPHA_VANTAGE_API_KEY: '547GHY3CRL7BKWPC',
    ALPHA_VANTAGE_BASE_URL: 'https://www.alphavantage.co/query?',
    
    // Konfiguration für Alpha Vantage Fallbacks
    VXX_PROXY_SYMBOL: 'VXX',

    CACHE_DURATION_HOURS: {
        INFLATION: 24 * 7,
        INTEREST: 24 * 7,
        VOLATILITY: 4,
        EURUSD: 1,
        GOLD: 1, 
    },
    TROY_OUNCE_TO_GRAM: 31.1035,
};

// ===================================================================================
// DOM ELEMENT REFERENZEN (Unverändert)
// ===================================================================================
const UI = {
    statusMessage: document.getElementById('status-message'),
    priceSourceNote: document.getElementById('price-source-note'),
    goldPrice: document.getElementById('gold-price'),
    factorsContainer: document.getElementById('factors-container'),
    assessmentResult: document.getElementById('assessment-result'),
    diagnosticsPanel: document.getElementById('diagnostics-panel'),
    toggleDiagnosticsBtn: document.getElementById('toggle-diagnostics'),
    diagnosticsLog: document.getElementById('diagnostics-log'),
};

// ===================================================================================
// DIAGNOSE & LOGGING (Unverändert)
// ===================================================================================
function logDiagnostic(message, type = 'info', details = null) {
    console.log(`[${type.toUpperCase()}] ${message}`, details || '');
    if (!UI.diagnosticsLog) return;
    const entry = document.createElement('div');
    const logClass = (type === 'error' || type === 'warning') ? 'error' : type;
    entry.className = `log-entry log-${logClass}`;
    const timestamp = new Date().toLocaleTimeString('de-DE');
    let content = `[${timestamp}] ${message}`;
    if (details) {
        try { content += `\n--- Details ---\n${JSON.stringify(details, null, 2)}`; } 
        catch (e) { content += `\n--- Details (Roh) ---\n${details}`; }
    }
    entry.textContent = content;
    UI.diagnosticsLog.appendChild(entry);
    UI.diagnosticsLog.scrollTop = UI.diagnosticsLog.scrollHeight;
}

// ===================================================================================
// CACHING-HELFER (Gehärtet & Unverändert)
// ===================================================================================
function getCachedData(key) { /* ... bleibt unverändert ... */ }
function setCachedData(key, data) { /* ... bleibt unverändert ... */ }

// ===================================================================================
// API-DATENABRUF (Stark angepasst)
// ===================================================================================
async function fetchApi(name, url, apiProvider) {
    const headers = {};
    let finalUrl = url;

    // NEU: Passt die Anfrage an den jeweiligen API-Provider an
    if (apiProvider === 'goldapi') {
        headers['x-access-token'] = CONFIG.GOLDAPI_KEY;
    } else if (apiProvider === 'alphavantage') {
        finalUrl += `&apikey=${CONFIG.ALPHA_VANTAGE_API_KEY}`;
    }

    try {
        const response = await fetch(finalUrl, { headers });
        if (!response.ok) throw new Error(`HTTP Status ${response.status}`);
        const data = await response.json();
        // Alpha Vantage spezifische Fehlerprüfung
        if (apiProvider === 'alphavantage') {
            const errorKey = Object.keys(data).find(k => k.toLowerCase().includes('error') || k.toLowerCase().includes('note'));
            if (errorKey) throw new Error(`API meldet: ${data[errorKey]}`);
        }
        return data;
    } catch (error) {
        throw new Error(`[${name}] ${error.message}`);
    }
}

// ENTFERNT: Die komplexe Multi-Fallback-Funktion für Gold ist nicht mehr nötig.
// Die neue API ist spezialisiert und sollte direkt funktionieren.

async function getResilientData(name, cacheKey, fetchFunction) {
    const cached = getCachedData(cacheKey);
    if (cached && !cached.isExpired) {
        logDiagnostic(`[CACHE] Lade gültige '${name}' Daten aus dem Cache.`, 'success');
        return { value: cached.data, source: 'cache', timestamp: cached.timestamp };
    }

    try {
        const liveData = await fetchFunction();
        setCachedData(cacheKey, liveData);
        return { value: liveData, source: 'live', timestamp: Date.now() };
    } catch (error) {
        logDiagnostic(`[API] Live-Abruf für '${name}' fehlgeschlagen.`, 'warning', { message: error.message });
        if (cached) {
            logDiagnostic(`[FALLBACK] Nutze veraltete '${name}' Daten aus dem Cache.`, 'info');
            return { value: cached.data, source: 'cache', timestamp: cached.timestamp };
        }
        logDiagnostic(`[FEHLER] Keine Live- oder Cache-Daten für '${name}' verfügbar.`, 'error');
        return { value: null, source: 'none', timestamp: null };
    }
}

// ===================================================================================
// DATENVERARBEITUNG & ANALYSE (Unverändert in der Logik)
// ===================================================================================
function extractValue(dataObject, path) { /* ... bleibt unverändert ... */ }
function analyzeFactors(data) { /* ... bleibt unverändert ... */ }

// ===================================================================================
// UI-UPDATE FUNKTIONEN (Angepasst für die neue Datenquelle)
// ===================================================================================
function updatePriceDisplay(goldResult, eurUsdResult) {
    // HINWEIS: Die Extraktion des Preises ist jetzt viel einfacher.
    const goldPriceUsd = goldResult?.specificValue;
    const eurUsdRate = eurUsdResult?.specificValue;

    if (!goldPriceUsd || !eurUsdRate) {
        UI.goldPrice.textContent = '--.--';
        UI.priceSourceNote.style.display = 'none';
        return;
    }

    // Die Umrechnung bleibt logisch gleich: (Preis in USD pro Unze / Gramm pro Unze) / (USD pro EUR)
    // Da goldapi.io EUR/USD liefert, müssen wir den Kehrwert nehmen, um USD pro EUR zu erhalten.
    const usdPerGram = goldPriceUsd / CONFIG.TROY_OUNCE_TO_GRAM;
    const eurPerGram = usdPerGram / eurUsdRate;

    UI.goldPrice.textContent = eurPerGram.toFixed(2).replace('.', ',');
    
    // Der explizite Hinweis auf den ETF-Proxy ist nicht mehr nötig.
    UI.priceSourceNote.style.display = 'none';
}
function displayFactors(analysis) { /* ... bleibt unverändert ... */ }
function displayAssessment(score, missingData) { /* ... bleibt unverändert ... */ }


// ===================================================================================
// HAUPT-LOGIK & INITIALISIERUNG (Angepasst an die neuen APIs)
// ===================================================================================
async function main() {
    if (!CONFIG.GOLDAPI_KEY || !CONFIG.ALPHA_VANTAGE_API_KEY) {
        logDiagnostic("FEHLER: Ein oder mehrere API-Schlüssel nicht konfiguriert.", 'error');
        UI.statusMessage.textContent = 'Konfigurationsfehler.';
        return;
    }
    
    UI.statusMessage.textContent = 'Rufe Livedaten ab...';
    logDiagnostic("Starte robusten Datenabruf (Hybrid-Modus)...", 'info');

    const [goldResult, eurUsdResult, inflationResult, interestResult, volatilityResult] = await Promise.all([
        // NEU: Abruf von goldapi.io für Goldpreis (XAU in USD)
        getResilientData("GOLD", "gold", () => fetchApi("GOLD (XAU/USD)", `${CONFIG.GOLDAPI_BASE_URL}XAU/USD`, 'goldapi')),
        
        // NEU: Abruf von goldapi.io für Währungskurs
        getResilientData("EUR/USD", "eurusd", () => fetchApi("EUR/USD", `${CONFIG.GOLDAPI_BASE_URL}EUR/USD`, 'goldapi')),
        
        // BLEIBT: Abruf von Alpha Vantage für ökonomische Daten
        getResilientData("INFLATION", "inflation", () => fetchApi("INFLATION", `${CONFIG.ALPHA_VANTAGE_BASE_URL}function=INFLATION`, 'alphavantage')),
        getResilientData("ZINSEN", "interest", () => fetchApi("ZINSEN", `${CONFIG.ALPHA_VANTAGE_BASE_URL}function=FEDERAL_FUNDS_RATE&interval=monthly`, 'alphavantage')),
        getResilientData("VOLATILITÄT", "volatility", () => fetchApi("VOLATILITÄT", `${CONFIG.ALPHA_VANTAGE_BASE_URL}function=TIME_SERIES_DAILY&symbol=${CONFIG.VXX_PROXY_SYMBOL}`, 'alphavantage'))
    ]);

    logDiagnostic(`Datenquellen: GOLD (${goldResult.source}@goldapi), EUR/USD (${eurUsdResult.source}@goldapi), INFLATION (${inflationResult.source}@av), ZINSEN (${interestResult.source}@av), VOLATILITÄT (${volatilityResult.source}@av)`, 'info');

    const marketData = {
        // GEÄNDERT: Der Pfad zum Extrahieren des Preises ist bei goldapi.io viel einfacher: "price"
        gold:     { ...goldResult, specificValue: goldResult.value?.price },
        eurUsd:   { ...eurUsdResult, specificValue: eurUsdResult.value?.price },
        
        // Unverändert für Alpha Vantage Daten
        inflation: extractValue(inflationResult, 'data.0.value'),
        interest:  extractValue(interestResult, 'data.0.value'),
        vix:       extractValue(volatilityResult, `Time Series (Daily).${Object.keys(volatilityResult.value?.['Time Series (Daily)'] || {})[0]}.4. close`),
    };
    
    if (marketData.gold.specificValue === null || marketData.eurUsd.specificValue === null) {
        const reason = "Kritische Preis- oder Währungsdaten konnten nicht geladen werden. Eine Anzeige ist nicht möglich.";
        logDiagnostic(reason, 'error');
        UI.statusMessage.textContent = 'Fehler beim Laden kritischer Daten.';
        UI.assessmentResult.innerHTML = `<p>${reason}</p>`;
        UI.assessmentResult.className = 'assessment-caution';
        return;
    }

    logDiagnostic("Datenverarbeitung und Analyse beginnen.", 'info');
    const analysis = analyzeFactors(marketData);

    // UI Updates
    updatePriceDisplay(marketData.gold, marketData.eurUsd);
    displayFactors(analysis);
    displayAssessment(analysis.score, analysis.missingData);

    UI.statusMessage.textContent = `Analyse abgeschlossen | Letzte Aktualisierung: ${new Date().toLocaleTimeString('de-DE')}`;
    logDiagnostic("UI erfolgreich aktualisiert.", 'success');
}

// Initialisierung der App
document.addEventListener('DOMContentLoaded', () => { /* ... bleibt unverändert ... */ });
 
