// ===================================================================================
// ZENTRALE KONFIGURATION
// HINWEIS: Alle "magischen" Werte sind jetzt an einem Ort. Das macht
// die Anpassung und Wartung in Zukunft viel einfacher.
// ===================================================================================
const CONFIG = {
    // KRITISCH: Der API-Schlüssel sollte NICHT hier stehen. Er sollte in einer
    // Serverless Function (Backend) sicher gespeichert sein. Für die lokale Entwicklung
    // kann er hier temporär eingetragen werden.
    // SIEHE ERKLÄRUNG UNTEN, WIE MAN DAS SICHER MACHT.
    API_KEY: '547GHY3CRL7BKWPC', 
    API_BASE_URL: 'https://www.alphavantage.co/query?',
    
    // Konfiguration für den Goldpreis-Fallback
    GOLD_PROXY_SYMBOL: 'GLD',
    OUNCES_PER_GLD_SHARE: 0.09194, // Schätzung basierend auf ETF-Daten

    // Konfiguration für das Caching
    CACHE_DURATION_HOURS: {
        INFLATION: 24 * 7, // Inflationsdaten ändern sich monatlich, 1x pro Woche abrufen reicht
        INTEREST: 24 * 7,  // Zinsdaten ändern sich selten, 1x pro Woche abrufen reicht
        VOLATILITY: 4,     // Volatilitätsdaten ändern sich täglich, alle 4 Stunden abrufen reicht
    },
    
    // Konstanten für die Analyse
    TROY_OUNCE_TO_GRAM: 31.1035,
};

// ===================================================================================
// DOM ELEMENT REFERENZEN
// HINWEIS: Elemente einmalig abrufen und in einem Objekt speichern.
// Das ist performanter und übersichtlicher.
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
// (Deine Logging-Funktionen waren bereits sehr gut, fast keine Änderungen nötig)
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
// CACHING-HELFER
// HINWEIS: Diese neuen Funktionen helfen, API-Aufrufe zu reduzieren.
// Sie speichern Daten im `localStorage` des Browsers.
// ===================================================================================
function getCachedData(key) {
    const cached = localStorage.getItem(key);
    if (!cached) {
        logDiagnostic(`[CACHE] Kein Eintrag für '${key}' gefunden.`, 'info');
        return null;
    }

    const { timestamp, data } = JSON.parse(cached);
    const ageInHours = (Date.now() - timestamp) / (1000 * 60 * 60);

    const cacheDuration = CONFIG.CACHE_DURATION_HOURS[key.toUpperCase()];
    if (ageInHours > cacheDuration) {
        logDiagnostic(`[CACHE] Daten für '${key}' sind abgelaufen (${ageInHours.toFixed(1)}h > ${cacheDuration}h).`, 'info');
        localStorage.removeItem(key);
        return null;
    }
    
    logDiagnostic(`[CACHE] Lade '${key}' aus Cache (Alter: ${ageInHours.toFixed(1)}h).`, 'success');
    return data;
}

function setCachedData(key, data) {
    const item = {
        timestamp: Date.now(),
        data,
    };
    localStorage.setItem(key, JSON.stringify(item));
    logDiagnostic(`[CACHE] Speichere '${key}' im Cache.`, 'info');
}

// ===================================================================================
// API-DATENABRUF (Gehärtet & mit Caching)
// ===================================================================================

/**
 * Universelle und robuste Fetch-Funktion.
 * HINWEIS: Wurde erweitert, um Caching zu unterstützen.
 */
async function fetchData(name, url, options = {}) {
    const { cacheKey } = options;
    
    if (cacheKey) {
        const cachedData = getCachedData(cacheKey);
        if (cachedData) return cachedData;
    }
    
    logDiagnostic(`[API] Rufe '${name}' ab...`, 'info');

    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP Fehler! Status: ${response.status}`);
        }
        const data = await response.json();

        // Deine API-Fehlererkennung war schon sehr gut und wird beibehalten
        const errorKey = Object.keys(data).find(k => k.toLowerCase().includes('error') || k.toLowerCase().includes('note'));
        if (errorKey) {
            throw new Error(`API meldet: ${data[errorKey]}`);
        }
        if (url.includes('GLOBAL_QUOTE') && (!data['Global Quote'] || Object.keys(data['Global Quote']).length === 0)) {
            throw new Error("API Antwort für GLOBAL_QUOTE ist leer.");
        }
        
        if (cacheKey) {
            setCachedData(cacheKey, data);
        }
        return data;

    } catch (error) {
        // Wirft einen neuen, kontextbezogenen Fehler für besseres Debugging
        throw new Error(`[${name}] ${error.message}`);
    }
}

/**
 * Holt Goldpreis mit Fallback-Logik.
 * (Deine Logik war bereits exzellent, wurde nur an die neue fetchData-Struktur angepasst)
 */
async function fetchGoldPriceWithFallback() {
    const spotUrl = `${CONFIG.API_BASE_URL}function=CURRENCY_EXCHANGE_RATE&from_currency=XAU&to_currency=USD&apikey=${CONFIG.API_KEY}`;
    const proxyUrl = `${CONFIG.API_BASE_URL}function=GLOBAL_QUOTE&symbol=${CONFIG.GOLD_PROXY_SYMBOL}&apikey=${CONFIG.API_KEY}`;

    try {
        logDiagnostic("[GOLD] Versuch 1: Spotpreis (XAU/USD) abrufen...", 'info');
        const data = await fetchData("GOLD (XAU/USD)", spotUrl);
        const price = parseFloat(data['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        if (isNaN(price)) throw new Error("Spotpreis konnte nicht extrahiert werden.");
        return { source: 'SPOT', price };
    } catch (spotError) {
        logDiagnostic(`[GOLD] Spotpreis fehlgeschlagen. Starte Fallback. Grund: ${spotError.message}`, 'warning');
        try {
            logDiagnostic(`[GOLD] Versuch 2: ETF Proxy (${CONFIG.GOLD_PROXY_SYMBOL}) abrufen...`, 'info');
            const data = await fetchData(`GOLD (${CONFIG.GOLD_PROXY_SYMBOL} Proxy)`, proxyUrl);
            const gldPrice = parseFloat(data['Global Quote']['05. price']);
            if (isNaN(gldPrice)) throw new Error("GLD Preis konnte nicht extrahiert werden.");

            const estimatedSpotPrice = gldPrice / CONFIG.OUNCES_PER_GLD_SHARE;
            logDiagnostic(`[GOLD] Proxy Berechnung: ${gldPrice.toFixed(2)} / ${CONFIG.OUNCES_PER_GLD_SHARE} = ${estimatedSpotPrice.toFixed(2)}`, 'info');
            return { source: 'ETF_PROXY', price: estimatedSpotPrice };
        } catch (fallbackError) {
            logDiagnostic(`[GOLD] Fallback ebenfalls fehlgeschlagen.`, 'error');
            throw fallbackError; // Diesen Fehler weiterwerfen, um ihn im Promise.allSettled zu fangen
        }
    }
}

// ===================================================================================
// DATENVERARBEITUNG & ANALYSE
// ===================================================================================

/**
 * Extrahiert und bereinigt die benötigten Werte aus den rohen API-Antworten.
 */
function extractMarketData(results) {
    const data = {};
    const extract = (result, path, parser = parseFloat) => {
        if (result.status !== 'fulfilled' || !result.value) return null;
        try {
            // HINWEIS: Sicherer Zugriff auf verschachtelte Objekte
            const value = path.split('.').reduce((obj, key) => obj && obj[key], result.value);
            const parsed = parser(value);
            return isNaN(parsed) ? null : parsed;
        } catch (e) {
            return null;
        }
    };

    data.eurUsdRate = extract(results.eurUsd, 'Realtime Currency Exchange Rate.5. Exchange Rate');
    data.inflationRate = extract(results.inflation, 'data.0.value');
    data.interestRate = extract(results.interest, 'data.0.value');
    data.vixValue = extract(results.volatility, `Time Series (Daily).${Object.keys(results.volatility.value?.['Time Series (Daily)'] || {})[0]}.4. close`);

    if (results.gold.status === 'fulfilled') {
        data.xauUsd = results.gold.value.price;
        data.goldSource = results.gold.value.source;
    } else {
        data.xauUsd = null;
        data.goldSource = null;
    }
    
    return data;
}

/**
 * Analysiert die Marktdaten und gibt einen Score und Details zurück.
 * (Deine Analyse-Logik ist unverändert, da sie gut war.)
 */
function analyzeFactors(data) {
    let score = 0;
    const analysis = { details: {}, missingData: false };

    // 1. Realzins
    if (data.interestRate !== null && data.inflationRate !== null) {
        const realInterestRate = data.interestRate - data.inflationRate;
        analysis.details.realInterestRate = { raw: realInterestRate, impact: 'neutral' };
        if (realInterestRate > 2.0) { score -= 3; analysis.details.realInterestRate.impact = 'negative'; }
        else if (realInterestRate > 0.5) { score -= 1; analysis.details.realInterestRate.impact = 'negative'; }
        else if (realInterestRate < -1.0) { score += 3; analysis.details.realInterestRate.impact = 'positive'; }
        else if (realInterestRate < 0) { score += 1; analysis.details.realInterestRate.impact = 'positive'; }
    } else {
        analysis.details.realInterestRate = { raw: null, impact: 'neutral' };
        if (data.interestRate === null || data.inflationRate === null) analysis.missingData = true;
    }

    // 2. Inflation
    if (data.inflationRate !== null) {
        analysis.details.inflation = { raw: data.inflationRate, impact: 'neutral' };
        if (data.inflationRate > 4.0) { score += 2; analysis.details.inflation.impact = 'positive'; }
        else if (data.inflationRate > 2.5) { score += 1; analysis.details.inflation.impact = 'positive'; }
        else if (data.inflationRate < 1.5) { score -= 1; analysis.details.inflation.impact = 'negative'; }
    } else {
        analysis.details.inflation = { raw: null, impact: 'neutral' };
    }

    // 3. USD Stärke
    if (data.eurUsdRate !== null) {
        analysis.details.eurUsd = { raw: data.eurUsdRate, impact: 'neutral' };
        if (data.eurUsdRate < 1.05) { score -= 1; analysis.details.eurUsd.impact = 'negative'; }
        else if (data.eurUsdRate > 1.15) { score += 1; analysis.details.eurUsd.impact = 'positive'; }
    } else {
        analysis.details.eurUsd = { raw: null, impact: 'neutral' };
    }

    // 4. Marktunsicherheit
    if (data.vixValue !== null) {
        analysis.details.vix = { raw: data.vixValue, name: "Marktunsicherheit (VXX Proxy)", impact: 'neutral' };
        if (data.vixValue > 45) { score += 3; analysis.details.vix.impact = 'positive'; }
        else if (data.vixValue > 30) { score += 1; analysis.details.vix.impact = 'positive'; }
        else if (data.vixValue < 20) { score -= 1; analysis.details.vix.impact = 'negative'; }
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
function updateUIErrorState(reason) {
    UI.assessmentResult.innerHTML = `<p>Analyse nicht möglich. Grund: ${reason}</p>`;
    UI.assessmentResult.className = 'assessment-caution';
    UI.factorsContainer.innerHTML = `<p>Indikatoren konnten nicht geladen werden.</p>`;
    UI.goldPrice.textContent = '--.--';
    UI.priceSourceNote.style.display = 'none';
}

function updatePriceDisplay({ xauUsd, eurUsdRate, goldSource }) {
    const usdPerGram = xauUsd / CONFIG.TROY_OUNCE_TO_GRAM;
    const eurPerGram = usdPerGram / eurUsdRate;
    UI.goldPrice.textContent = eurPerGram.toFixed(2).replace('.', ',');
    UI.priceSourceNote.style.display = (goldSource === 'ETF_PROXY') ? 'block' : 'none';
}

function displayFactors(analysis) {
    UI.factorsContainer.innerHTML = '';
    const { details } = analysis;

    const formatValue = (raw, suffix = '', decimals = 2) => 
        (raw === null || isNaN(raw)) ? 'N/A' : `${raw.toFixed(decimals).replace('.', ',')}${suffix}`;
    
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
    else if (score >= -2) { [text, cssClass, explanation] = ["Neutral / Abwarten", "assessment-wait", "Die Indikatoren sind gemischt. Positive und negative Faktoren gleichen sich aus."]; }
    else { [text, cssClass, explanation] = ["Vorsicht / Nicht Kaufen", "assessment-caution", "Die verfügbaren Indikatoren sprechen derzeit gegen eine Goldinvestition."]; }

    if (missingData) {
        explanation += " ⚠️ ACHTUNG: Nicht alle Indikatoren konnten geladen werden. Die Bewertung ist unsicher.";
        if (cssClass !== "assessment-wait") cssClass = "assessment-wait"; // Bei fehlenden Daten immer zur Vorsicht mahnen
    }

    UI.assessmentResult.innerHTML = `<h3>${text} (Score: ${score})</h3><p>${explanation}</p>`;
    UI.assessmentResult.classList.add(cssClass);
}

// ===================================================================================
// HAUPT-LOGIK & INITIALISIERUNG
// ===================================================================================
async function main() {
    if (!CONFIG.API_KEY || CONFIG.API_KEY === 'DEIN_API_SCHLUESSEL_HIER') {
        const msg = "FEHLER: Bitte API-Schlüssel konfigurieren.";
        logDiagnostic(msg, 'error');
        UI.statusMessage.textContent = 'Konfigurationsfehler.';
        updateUIErrorState("API-Schlüssel fehlt.");
        return;
    }
    
    UI.statusMessage.textContent = 'Rufe Livedaten ab...';
    logDiagnostic("Starte parallelen Datenabruf...", 'info');

    const [eurUsd, gold, inflation, interest, volatility] = await Promise.allSettled([
        fetchData("EUR/USD", `${CONFIG.API_BASE_URL}function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD&apikey=${CONFIG.API_KEY}`),
        fetchGoldPriceWithFallback(),
        fetchData("INFLATION", `${CONFIG.API_BASE_URL}function=INFLATION&apikey=${CONFIG.API_KEY}`, { cacheKey: 'inflation' }),
        fetchData("ZINSEN", `${CONFIG.API_BASE_URL}function=FEDERAL_FUNDS_RATE&interval=monthly&apikey=${CONFIG.API_KEY}`, { cacheKey: 'interest' }),
        fetchData("VOLATILITÄT", `${CONFIG.API_BASE_URL}function=TIME_SERIES_DAILY&symbol=VXX&apikey=${CONFIG.API_KEY}`, { cacheKey: 'volatility' })
    ]);

    // Bessere Protokollierung der Ergebnisse
    const results = { eurUsd, gold, inflation, interest, volatility };
    for (const [key, result] of Object.entries(results)) {
        if (result.status === 'fulfilled') {
            logDiagnostic(`[${key.toUpperCase()}] Erfolgreich abgerufen.`, 'success');
        } else {
            logDiagnostic(result.reason.message, 'error');
        }
    }

    try {
        const marketData = extractMarketData(results);
        if (marketData.xauUsd === null || marketData.eurUsdRate === null) {
            throw new Error("Kritische Preis- oder Währungsdaten fehlen. Analyse nicht möglich.");
        }

        logDiagnostic("Beginne Analyse der verfügbaren Daten.", 'info');
        const analysis = analyzeFactors(marketData);
        
        // UI Updates
        updatePriceDisplay(marketData);
        displayFactors(analysis);
        displayAssessment(analysis.score, analysis.missingData);
        UI.statusMessage.textContent = `Zuletzt aktualisiert: ${new Date().toLocaleTimeString('de-DE')}`;
        logDiagnostic("Analyse abgeschlossen und UI aktualisiert.", 'success');
        
    } catch (error) {
        logDiagnostic(`Fehler bei der Datenverarbeitung: ${error.message}`, 'error');
        UI.statusMessage.textContent = 'Fehler bei der Verarbeitung der Daten.';
        updateUIErrorState("Verarbeitung fehlgeschlagen. Siehe Diagnosepanel.");
    }
}

// Initialisierung der App
document.addEventListener('DOMContentLoaded', () => {
    // Event Listener für den Diagnose-Button
    UI.toggleDiagnosticsBtn.addEventListener('click', () => {
        const isHidden = UI.diagnosticsPanel.classList.toggle('diagnostics-hidden');
        UI.toggleDiagnosticsBtn.textContent = isHidden ? 'Diagnosepanel anzeigen' : 'Diagnosepanel ausblenden';
        UI.toggleDiagnosticsBtn.setAttribute('aria-expanded', !isHidden);
    });
    
    logDiagnostic('System initialisiert.', 'info');
    main(); // Starte die Hauptfunktion
});
