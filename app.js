// ===================================================================================
// ZENTRALE KONFIGURATION
// ===================================================================================
const CONFIG = {
    API_KEY: '547GHY3CRL7BKWPC', // WICHTIG: Sollte in einem Backend/Proxy sein
    API_BASE_URL: 'https://www.alphavantage.co/query?',
    GOLD_PROXY_SYMBOL: 'GLD',
    OUNCES_PER_GLD_SHARE: 0.09194,
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
// DOM ELEMENT REFERENZEN
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
// DIAGNOSE & LOGGING
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
        try {
            content += `\n--- Details ---\n${JSON.stringify(details, null, 2)}`;
        } catch (e) {
            content += `\n--- Details (Roh) ---\n${details}`;
        }
    }
    entry.textContent = content;
    UI.diagnosticsLog.appendChild(entry);
    UI.diagnosticsLog.scrollTop = UI.diagnosticsLog.scrollHeight;
}

// ===================================================================================
// CACHING-HELFER (GEHÄRTET)
// ===================================================================================
function getCachedData(key) {
    const cached = localStorage.getItem(key);
    if (!cached) return null;
    try {
        const { timestamp, data } = JSON.parse(cached);
        const ageInHours = (Date.now() - timestamp) / (1000 * 60 * 60);
        const cacheDuration = CONFIG.CACHE_DURATION_HOURS[key.toUpperCase()] || 1;
        // WICHTIG: Prüfen ob die Daten im Cache gültig sind.
        if (data === null || data === undefined) {
            logDiagnostic(`[CACHE] Ungültige (null) Daten für '${key}' im Cache gefunden. Werden ignoriert.`, 'warning');
            return null;
        }
        return { data, timestamp, isExpired: ageInHours > cacheDuration };
    } catch (e) {
        logDiagnostic(`[CACHE] Fehler beim Parsen der Cache-Daten für '${key}'. Cache wird ignoriert.`, 'error');
        return null;
    }
}

function setCachedData(key, data) {
    // NEU: Dieser Schutz verhindert, dass fehlerhafte Daten jemals in den Cache gelangen.
    if (data === null || data === undefined) {
        logDiagnostic(`[CACHE] Verhindere das Speichern von 'null' für '${key}'.`, 'warning');
        return;
    }
    const item = { timestamp: Date.now(), data };
    localStorage.setItem(key, JSON.stringify(item));
    logDiagnostic(`[CACHE] Speichere '${key}' im Cache.`, 'info');
}

// ===================================================================================
// API-DATENABRUF
// ===================================================================================
async function fetchApi(name, url) {
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP Status ${response.status}`);
        const data = await response.json();
        const errorKey = Object.keys(data).find(k => k.toLowerCase().includes('error') || k.toLowerCase().includes('note'));
        if (errorKey) throw new Error(`API meldet: ${data[errorKey]}`);
        return data;
    } catch (error) {
        const enhancedError = new Error(`[${name}] ${error.message}`);
        enhancedError.originalError = error;
        throw enhancedError;
    }
}

async function fetchGoldPriceMultiFallback() {
    // Versuch 1: Spotpreis (XAU)
    try {
        logDiagnostic("[GOLD] Versuch 1: Spotpreis (XAU/USD) abrufen...", 'info');
        const url = `${CONFIG.API_BASE_URL}function=CURRENCY_EXCHANGE_RATE&from_currency=XAU&to_currency=USD&apikey=${CONFIG.API_KEY}`;
        const data = await fetchApi("GOLD (Spot)", url);
        const price = parseFloat(data['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        if (isNaN(price)) throw new Error("Spotpreis konnte nicht extrahiert werden.");
        return { source: 'SPOT', price, note: 'Preisquelle: Spot (XAU/USD)' };
    } catch (spotError) {
        logDiagnostic(`[GOLD] Versuch 1 (Spot) fehlgeschlagen: ${spotError.message}`, 'warning');
    }

    // Versuch 2: ETF Proxy (GLD) via GLOBAL_QUOTE
    try {
        logDiagnostic(`[GOLD] Versuch 2: ETF Proxy (${CONFIG.GOLD_PROXY_SYMBOL}) via GLOBAL_QUOTE...`, 'info');
        const url = `${CONFIG.API_BASE_URL}function=GLOBAL_QUOTE&symbol=${CONFIG.GOLD_PROXY_SYMBOL}&apikey=${CONFIG.API_KEY}`;
        const data = await fetchApi("GOLD (GLD Quote)", url);
        if (!data['Global Quote'] || Object.keys(data['Global Quote']).length === 0) throw new Error("Leere GLOBAL_QUOTE Antwort.");
        const gldPrice = parseFloat(data['Global Quote']['05. price']);
        if (isNaN(gldPrice)) throw new Error("GLD Quote-Preis konnte nicht extrahiert werden.");
        return { source: 'ETF_PROXY_QUOTE', price: gldPrice / CONFIG.OUNCES_PER_GLD_SHARE, note: 'Preisquelle: Gold-ETF Proxy (GLD, aktuell)' };
    } catch (quoteError) {
        logDiagnostic(`[GOLD] Versuch 2 (GLD Quote) fehlgeschlagen: ${quoteError.message}`, 'warning');
    }

    // Versuch 3: ETF Proxy (GLD) via TIME_SERIES_DAILY
    try {
        logDiagnostic(`[GOLD] Versuch 3: ETF Proxy (${CONFIG.GOLD_PROXY_SYMBOL}) via TIME_SERIES_DAILY...`, 'info');
        const url = `${CONFIG.API_BASE_URL}function=TIME_SERIES_DAILY&symbol=${CONFIG.GOLD_PROXY_SYMBOL}&apikey=${CONFIG.API_KEY}`;
        const data = await fetchApi("GOLD (GLD Daily)", url);
        const timeSeries = data['Time Series (Daily)'];
        if (!timeSeries) throw new Error("Keine Zeitreihendaten gefunden.");
        const latestDate = Object.keys(timeSeries)[0];
        const gldPrice = parseFloat(timeSeries[latestDate]['4. close']);
        if (isNaN(gldPrice)) throw new Error("GLD Daily-Preis konnte nicht extrahiert werden.");
        return { source: 'ETF_PROXY_DAILY', price: gldPrice / CONFIG.OUNCES_PER_GLD_SHARE, note: `Preisquelle: Gold-ETF Proxy (GLD, Schlusskurs v. ${latestDate})` };
    } catch (dailyError) {
        logDiagnostic(`[GOLD] Alle 3 Abrufversuche fehlgeschlagen: ${dailyError.message}`, 'error');
        throw new Error("Goldpreis konnte über keine Methode abgerufen werden.");
    }
}

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
// DATENVERARBEITUNG & ANALYSE
// ===================================================================================

function extractValue(dataObject, path) {
    if (!dataObject || !dataObject.value) return { ...dataObject, specificValue: null };
    try {
        const value = path.split('.').reduce((obj, key) => obj && obj[key], dataObject.value);
        const parsed = parseFloat(value);
        return { ...dataObject, specificValue: isNaN(parsed) ? null : parsed };
    } catch (e) {
        return { ...dataObject, specificValue: null };
    }
}

function analyzeFactors(data) {
    let score = 0;
    const analysis = { details: {}, missingData: false };

    const interestRate = data.interest?.specificValue;
    const inflationRate = data.inflation?.specificValue;
    const eurUsdRate = data.eurUsd?.specificValue;
    const vixValue = data.vix?.specificValue;

    // 1. Realzins
    if (interestRate !== null && inflationRate !== null) {
        const realInterestRate = interestRate - inflationRate;
        analysis.details.realInterestRate = { raw: realInterestRate, impact: 'neutral' };
        if (realInterestRate > 2.0) { score -= 3; analysis.details.realInterestRate.impact = 'negative'; }
        else if (realInterestRate > 0.5) { score -= 1; analysis.details.realInterestRate.impact = 'negative'; }
        else if (realInterestRate < -1.0) { score += 3; analysis.details.realInterestRate.impact = 'positive'; }
        else if (realInterestRate < 0) { score += 1; analysis.details.realInterestRate.impact = 'positive'; }
    } else {
        analysis.details.realInterestRate = { raw: null, impact: 'neutral' };
        if (interestRate === null || inflationRate === null) analysis.missingData = true;
    }

    // 2. Inflation
    if (inflationRate !== null) {
        analysis.details.inflation = { raw: inflationRate, impact: 'neutral' };
        if (inflationRate > 4.0) { score += 2; analysis.details.inflation.impact = 'positive'; }
        else if (inflationRate > 2.5) { score += 1; analysis.details.inflation.impact = 'positive'; }
        else if (inflationRate < 1.5) { score -= 1; analysis.details.inflation.impact = 'negative'; }
    } else {
        analysis.details.inflation = { raw: null, impact: 'neutral' };
    }

    // 3. USD Stärke
    if (eurUsdRate !== null) {
        analysis.details.eurUsd = { raw: eurUsdRate, impact: 'neutral' };
        if (eurUsdRate < 1.05) { score -= 1; analysis.details.eurUsd.impact = 'negative'; }
        else if (eurUsdRate > 1.15) { score += 1; analysis.details.eurUsd.impact = 'positive'; }
    } else {
        analysis.details.eurUsd = { raw: null, impact: 'neutral' };
    }

    // 4. Marktunsicherheit
    if (vixValue !== null) {
        analysis.details.vix = { raw: vixValue, name: "Marktunsicherheit (VXX Proxy)", impact: 'neutral' };
        if (vixValue > 45) { score += 3; analysis.details.vix.impact = 'positive'; }
        else if (vixValue > 30) { score += 1; analysis.details.vix.impact = 'positive'; }
        else if (vixValue < 20) { score -= 1; analysis.details.vix.impact = 'negative'; }
    } else {
        analysis.details.vix = { raw: null, name: "Marktunsicherheit (VXX Proxy)", impact: 'neutral' };
        analysis.missingData = true;
    }

    analysis.score = score;
    return analysis;
}

// ===================================================================================
// UI-UPDATE FUNKTIONEN
// ===================================================================================

function updatePriceDisplay(goldResult, eurUsdResult) {
    const goldData = goldResult?.value;
    const goldPrice = goldData?.price;
    const eurUsdRate = eurUsdResult?.specificValue;

    if (!goldPrice || !eurUsdRate) {
        UI.goldPrice.textContent = '--.--';
        UI.priceSourceNote.style.display = 'none';
        return;
    }

    const usdPerGram = goldPrice / CONFIG.TROY_OUNCE_TO_GRAM;
    const eurPerGram = usdPerGram / eurUsdRate;
    UI.goldPrice.textContent = eurPerGram.toFixed(2).replace('.', ',');
    
    UI.priceSourceNote.textContent = goldData.note || 'Preisquelle: Gold-ETF Proxy (GLD)';
    UI.priceSourceNote.style.display = goldData.source.startsWith('ETF_PROXY') ? 'block' : 'none';
}

function displayFactors(analysis) {
    UI.factorsContainer.innerHTML = '';
    const { details } = analysis;
    const formatValue = (raw, suffix = '', decimals = 2) => (raw === null || isNaN(raw)) ? 'N/A' : `${raw.toFixed(decimals).replace('.', ',')}${suffix}`;
    
    const factors = [
        { name: "Realzins (Zins - Inflation)", value: formatValue(details.realInterestRate.raw, '%'), impact: details.realInterestRate.impact },
        { name: "Inflation (USA)", value: formatValue(details.inflation.raw, '%', 1), impact: details.inflation.impact },
        { name: "EUR/USD Wechselkurs", value: formatValue(details.eurUsd.raw, '', 4), impact: details.eurUsd.impact },
        { name: "Marktunsicherheit (VXX Proxy)", value: formatValue(details.vix.raw), impact: details.vix.impact }
    ];

    factors.forEach(factor => {
        const item = document.createElement('div');
        item.className = 'factor-item';
        let impactText = 'N/A (Daten fehlen)';
        if (factor.impact === 'positive') impactText = 'Bullish (Gut für Gold)';
        if (factor.impact === 'negative') impactText = 'Bearish (Schlecht für Gold)';
        if (factor.impact === 'neutral' && factor.value !== 'N/A') impactText = 'Neutral';

        item.innerHTML = `
            <span class="factor-name">${factor.name}</span>
            <div class="factor-data">
                <span class="factor-value">${factor.value}</span>
                <span class="factor-impact ${factor.value === 'N/A' ? 'neutral' : factor.impact}">${impactText}</span>
            </div>`;
        UI.factorsContainer.appendChild(item);
    });
}

function displayAssessment(score, missingData) {
    UI.assessmentResult.className = '';
    let text, cssClass, explanation;

    if (score >= 4) { [text, cssClass, explanation] = ["Starkes Kaufsignal", "assessment-buy", "Die verfügbaren Indikatoren sprechen stark für einen Goldkauf."]; }
    else if (score > 0) { [text, cssClass, explanation] = ["Kaufsignal", "assessment-buy", "Die Marktlage ist tendenziell günstig für Goldinvestitionen."]; }
    else if (score >= -2) { [text, cssClass, explanation] = ["Neutral / Abwarten", "assessment-wait", "Die Indikatoren sind gemischt."]; }
    else { [text, cssClass, explanation] = ["Vorsicht / Nicht Kaufen", "assessment-caution", "Die Indikatoren sprechen derzeit gegen eine Goldinvestition."]; }

    if (missingData) {
        explanation += " ⚠️ ACHTUNG: Nicht alle Indikatoren konnten geladen werden. Die Bewertung ist unsicher.";
        if (cssClass !== "assessment-wait") cssClass = "assessment-wait";
    }

    UI.assessmentResult.innerHTML = `<h3>${text} (Score: ${score})</h3><p>${explanation}</p>`;
    UI.assessmentResult.classList.add(cssClass);
}

// ===================================================================================
// HAUPT-LOGIK & INITIALISIERUNG
// ===================================================================================
async function main() {
    if (!CONFIG.API_KEY || CONFIG.API_KEY === 'DEIN_API_SCHLUESSEL_HIER') {
        logDiagnostic("FEHLER: API-Schlüssel nicht konfiguriert.", 'error');
        UI.statusMessage.textContent = 'Konfigurationsfehler.';
        return;
    }
    
    UI.statusMessage.textContent = 'Rufe Livedaten ab...';
    logDiagnostic("Starte robusten Datenabruf...", 'info');

    const [eurUsdResult, goldResult, inflationResult, interestResult, volatilityResult] = await Promise.all([
        getResilientData("EUR/USD", "eurusd", () => fetchApi("EUR/USD", `${CONFIG.API_BASE_URL}function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD&apikey=${CONFIG.API_KEY}`)),
        getResilientData("GOLD", "gold", fetchGoldPriceMultiFallback),
        getResilientData("INFLATION", "inflation", () => fetchApi("INFLATION", `${CONFIG.API_BASE_URL}function=INFLATION&apikey=${CONFIG.API_KEY}`)),
        getResilientData("ZINSEN", "interest", () => fetchApi("ZINSEN", `${CONFIG.API_BASE_URL}function=FEDERAL_FUNDS_RATE&interval=monthly&apikey=${CONFIG.API_KEY}`)),
        getResilientData("VOLATILITÄT", "volatility", () => fetchApi("VOLATILITÄT", `${CONFIG.API_BASE_URL}function=TIME_SERIES_DAILY&symbol=VXX&apikey=${CONFIG.API_KEY}`))
    ]);

    logDiagnostic(`Datenquellen: EUR/USD (${eurUsdResult.source}), GOLD (${goldResult.source}), INFLATION (${inflationResult.source}), ZINSEN (${interestResult.source}), VOLATILITÄT (${volatilityResult.source})`, 'info');

    const marketData = {
        eurUsd: extractValue(eurUsdResult, 'Realtime Currency Exchange Rate.5. Exchange Rate'),
        gold: { ...goldResult, specificValue: goldResult.value?.price },
        inflation: extractValue(inflationResult, 'data.0.value'),
        interest: extractValue(interestResult, 'data.0.value'),
        vix: extractValue(volatilityResult, `Time Series (Daily).${Object.keys(volatilityResult.value?.['Time Series (Daily)'] || {})[0]}.4. close`),
    };
    
    if (marketData.gold.specificValue === null || marketData.eurUsd.specificValue === null) {
        const reason = "Kritische Preis- oder Währungsdaten konnten weder live noch aus dem Cache geladen werden. Eine Anzeige ist nicht möglich.";
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
document.addEventListener('DOMContentLoaded', () => {
    UI.toggleDiagnosticsBtn.addEventListener('click', () => {
        const isHidden = UI.diagnosticsPanel.classList.toggle('diagnostics-hidden');
        UI.toggleDiagnosticsBtn.textContent = isHidden ? 'Diagnosepanel anzeigen' : 'Diagnosepanel ausblenden';
        UI.toggleDiagnosticsBtn.setAttribute('aria-expanded', !isHidden);
    });
    
    logDiagnostic('System initialisiert.', 'info');
    main();
});
