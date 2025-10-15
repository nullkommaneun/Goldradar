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
// CACHING-HELFER (Unverändert)
// ===================================================================================
function getCachedData(key) {
    const cached = localStorage.getItem(key);
    if (!cached) return null;
    const { timestamp, data } = JSON.parse(cached);
    const ageInHours = (Date.now() - timestamp) / (1000 * 60 * 60);
    const cacheDuration = CONFIG.CACHE_DURATION_HOURS[key.toUpperCase()] || 1;
    return { data, timestamp, isExpired: ageInHours > cacheDuration };
}

function setCachedData(key, data) {
    const item = { timestamp: Date.now(), data };
    localStorage.setItem(key, JSON.stringify(item));
    logDiagnostic(`[CACHE] Speichere '${key}' im Cache.`, 'info');
}

// ===================================================================================
// API-DATENABRUF (Gehärtet)
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
        // HINWEIS: Wir erstellen hier einen neuen Error, um den Namen des fehlgeschlagenen Abrufs mitzugeben
        const enhancedError = new Error(`[${name}] ${error.message}`);
        enhancedError.originalError = error;
        throw enhancedError;
    }
}

// NEU: Die Gold-Abruf-Logik mit dreistufigem Fallback
async function fetchGoldPriceMultiFallback() {
    // Versuch 1: Spotpreis (XAU) - wird wahrscheinlich fehlschlagen
    try {
        logDiagnostic("[GOLD] Versuch 1: Spotpreis (XAU/USD) abrufen...", 'info');
        const url = `${CONFIG.API_BASE_URL}function=CURRENCY_EXCHANGE_RATE&from_currency=XAU&to_currency=USD&apikey=${CONFIG.API_KEY}`;
        const data = await fetchApi("GOLD (Spot)", url);
        const price = parseFloat(data['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        if (isNaN(price)) throw new Error("Spotpreis konnte nicht extrahiert werden.");
        logDiagnostic("[GOLD] Spotpreis erfolgreich abgerufen.", 'success');
        return { source: 'SPOT', price, note: 'Preisquelle: Spot (XAU/USD)' };
    } catch (spotError) {
        logDiagnostic(`[GOLD] Versuch 1 (Spot) fehlgeschlagen: ${spotError.message}`, 'warning');
    }

    // Versuch 2: ETF Proxy (GLD) via GLOBAL_QUOTE - schnell, aber unzuverlässig
    try {
        logDiagnostic(`[GOLD] Versuch 2: ETF Proxy (${CONFIG.GOLD_PROXY_SYMBOL}) via GLOBAL_QUOTE abrufen...`, 'info');
        const url = `${CONFIG.API_BASE_URL}function=GLOBAL_QUOTE&symbol=${CONFIG.GOLD_PROXY_SYMBOL}&apikey=${CONFIG.API_KEY}`;
        const data = await fetchApi("GOLD (GLD Quote)", url);
        if (!data['Global Quote'] || Object.keys(data['Global Quote']).length === 0) {
            throw new Error("Leere GLOBAL_QUOTE Antwort.");
        }
        const gldPrice = parseFloat(data['Global Quote']['05. price']);
        if (isNaN(gldPrice)) throw new Error("GLD Quote-Preis konnte nicht extrahiert werden.");
        logDiagnostic("[GOLD] ETF-Preis (Global Quote) erfolgreich abgerufen.", 'success');
        return {
            source: 'ETF_PROXY_QUOTE',
            price: gldPrice / CONFIG.OUNCES_PER_GLD_SHARE,
            note: 'Preisquelle: Gold-ETF Proxy (GLD, aktuellster Preis)'
        };
    } catch (quoteError) {
        logDiagnostic(`[GOLD] Versuch 2 (GLD Quote) fehlgeschlagen: ${quoteError.message}`, 'warning');
    }

    // Versuch 3: ETF Proxy (GLD) via TIME_SERIES_DAILY - langsam, aber sehr zuverlässig
    try {
        logDiagnostic(`[GOLD] Versuch 3: ETF Proxy (${CONFIG.GOLD_PROXY_SYMBOL}) via TIME_SERIES_DAILY abrufen...`, 'info');
        const url = `${CONFIG.API_BASE_URL}function=TIME_SERIES_DAILY&symbol=${CONFIG.GOLD_PROXY_SYMBOL}&apikey=${CONFIG.API_KEY}`;
        const data = await fetchApi("GOLD (GLD Daily)", url);
        const timeSeries = data['Time Series (Daily)'];
        if (!timeSeries) throw new Error("Keine Zeitreihendaten gefunden.");
        const latestDate = Object.keys(timeSeries)[0];
        const latestData = timeSeries[latestDate];
        const gldPrice = parseFloat(latestData['4. close']);
        if (isNaN(gldPrice)) throw new Error("GLD Daily-Preis konnte nicht extrahiert werden.");
        logDiagnostic("[GOLD] ETF-Preis (Daily Close) erfolgreich abgerufen.", 'success');
        return {
            source: 'ETF_PROXY_DAILY',
            price: gldPrice / CONFIG.OUNCES_PER_GLD_SHARE,
            note: `Preisquelle: Gold-ETF Proxy (GLD, Schlusskurs v. ${latestDate})`
        };
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
// DATENVERARBEITUNG & UI (Angepasst)
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

function updatePriceDisplay(gold, eurUsd) {
    const goldData = gold?.value; // Das Objekt { source, price, note }
    const goldPrice = goldData?.price;
    const eurUsdRate = eurUsd?.specificValue;

    if (!goldPrice || !eurUsdRate) {
        UI.goldPrice.textContent = '--.--';
        UI.priceSourceNote.style.display = 'none';
        return;
    }

    const usdPerGram = goldPrice / CONFIG.TROY_OUNCE_TO_GRAM;
    const eurPerGram = usdPerGram / eurUsdRate;

    UI.goldPrice.textContent = eurPerGram.toFixed(2).replace('.', ',');
    
    // NEU: Dynamische Anzeige der Preisquelle basierend auf dem Erfolg der Abrufe
    if (goldData.source.startsWith('ETF_PROXY')) {
        UI.priceSourceNote.textContent = goldData.note;
        UI.priceSourceNote.style.display = 'block';
    } else {
        UI.priceSourceNote.style.display = 'none';
    }
}

// ... (Andere UI-Funktionen wie displayFactors, displayAssessment bleiben in ihrer Logik gleich)
// Sie können die Anzeige des Zeitstempels für gecachte Daten noch hinzufügen, wenn gewünscht.
// Beispiel: <span class="factor-value">${factor.value}${createStaleIndicator(factor.dataObject)}</span>


// ===================================================================================
// HAUPT-LOGIK & INITIALISIERUNG
// ===================================================================================
async function main() {
    if (!CONFIG.API_KEY || CONFIG.API_KEY === 'DEIN_API_SCHLUESSEL_HIER') {
        const msg = "FEHLER: Bitte API-Schlüssel konfigurieren.";
        logDiagnostic(msg, 'error');
        UI.statusMessage.textContent = 'Konfigurationsfehler.';
        // updateUIErrorState("API-Schlüssel fehlt.");
        return;
    }
    
    UI.statusMessage.textContent = 'Rufe Livedaten ab...';
    logDiagnostic("Starte robusten Datenabruf...", 'info');

    // HINWEIS: Die Logik hier ist jetzt sauberer. Der Gold-Abruf ist in seiner eigenen Funktion gekapselt.
    const [eurUsdResult, goldResult, inflationResult, interestResult, volatilityResult] = await Promise.all([
        getResilientData("EUR/USD", "eurusd", () => fetchApi("EUR/USD", `${CONFIG.API_BASE_URL}function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD&apikey=${CONFIG.API_KEY}`)),
        getResilientData("GOLD", "gold", fetchGoldPriceMultiFallback), // NEU: Ruft die Multi-Fallback-Funktion auf
        getResilientData("INFLATION", "inflation", () => fetchApi("INFLATION", `${CONFIG.API_BASE_URL}function=INFLATION&apikey=${CONFIG.API_KEY}`)),
        getResilientData("ZINSEN", "interest", () => fetchApi("ZINSEN", `${CONFIG.API_BASE_URL}function=FEDERAL_FUNDS_RATE&interval=monthly&apikey=${CONFIG.API_KEY}`)),
        getResilientData("VOLATILITÄT", "volatility", () => fetchApi("VOLATILITÄT", `${CONFIG.API_BASE_URL}function=TIME_SERIES_DAILY&symbol=VXX&apikey=${CONFIG.API_KEY}`))
    ]);

    // HINWEIS: Wir loggen jetzt den Status jedes einzelnen resilienten Abrufs
    logDiagnostic(`EUR/USD Status: ${eurUsdResult.source}`, 'info');
    logDiagnostic(`GOLD Status: ${goldResult.source}`, 'info');
    logDiagnostic(`INFLATION Status: ${inflationResult.source}`, 'info');
    logDiagnostic(`ZINSEN Status: ${interestResult.source}`, 'info');
    logDiagnostic(`VOLATILITÄT Status: ${volatilityResult.source}`, 'info');

    const marketData = {
        eurUsd: extractValue(eurUsdResult, 'Realtime Currency Exchange Rate.5. Exchange Rate'),
        gold: { ...goldResult, specificValue: goldResult.value?.price }, // Gold hat eine andere Struktur
        inflation: extractValue(inflationResult, 'data.0.value'),
        interest: extractValue(interestResult, 'data.0.value'),
        volatility: extractValue(volatilityResult, `Time Series (Daily).${Object.keys(volatilityResult.value?.['Time Series (Daily)'] || {})[0]}.4. close`),
    };
    
    if (marketData.gold.specificValue === null || marketData.eurUsd.specificValue === null) {
        const reason = "Kritische Preis- oder Währungsdaten konnten weder live noch aus dem Cache geladen werden. Eine Anzeige ist nicht möglich.";
        logDiagnostic(reason, 'error');
        UI.statusMessage.textContent = 'Fehler beim Laden kritischer Daten.';
        // updateUIErrorState(reason);
        return;
    }

    logDiagnostic("Datenverarbeitung und Analyse beginnen.", 'info');
    // const analysis = analyzeFactors(marketData); // Ihre Analyse-Logik hier...

    // UI Updates
    updatePriceDisplay(marketData.gold, marketData.eurUsd);
    // displayFactors(analysis, marketData);
    // displayAssessment(analysis.score, analysis.missingData, analysis.hasStaleData);
    
    // HINWEIS: Temporär, bis die Analyse-Funktion wieder aktiv ist
    UI.factorsContainer.innerHTML = `<p>Analyse wird nach erfolgreichem Datenabruf durchgeführt.</p>`;
    UI.assessmentResult.innerHTML = `<p>Bewertung steht aus.</p>`;


    UI.statusMessage.textContent = `Daten erfolgreich geladen | Letzte Aktualisierung: ${new Date().toLocaleTimeString('de-DE')}`;
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
