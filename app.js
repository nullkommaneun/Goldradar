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
        // HINWEIS: Währungs- und Golddaten werden häufiger abgerufen und bekommen eine kürzere Cache-Zeit
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
    /* ... Ihr exzellenter Logging-Code bleibt hier unverändert ... */
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
// CACHING-HELFER (Überarbeitet)
// ===================================================================================

/**
 * HINWEIS: Holt Daten aus dem Cache. Gibt jetzt immer den Zeitstempel zurück,
 * damit wir das Alter der Daten anzeigen können, auch wenn sie als "gültig" gelten.
 */
function getCachedData(key) {
    const cached = localStorage.getItem(key);
    if (!cached) return null;

    const { timestamp, data } = JSON.parse(cached);
    const ageInHours = (Date.now() - timestamp) / (1000 * 60 * 60);
    const cacheDuration = CONFIG.CACHE_DURATION_HOURS[key.toUpperCase()] || 1;

    // Wir geben die Daten und den Zeitstempel immer zurück. 
    // Der Aufrufer entscheidet, was mit abgelaufenen Daten passiert.
    return {
        data,
        timestamp,
        isExpired: ageInHours > cacheDuration,
    };
}

function setCachedData(key, data) {
    const item = { timestamp: Date.now(), data };
    localStorage.setItem(key, JSON.stringify(item));
    logDiagnostic(`[CACHE] Speichere '${key}' im Cache.`, 'info');
}

// ===================================================================================
// API-DATENABRUF (Überarbeitet für maximale Robustheit)
// ===================================================================================

/**
 * HINWEIS: Dies ist die neue Kernfunktion. Sie versucht, Live-Daten zu holen.
 * Wenn das fehlschlägt, NIMMT sie die Daten aus dem Cache, EGAL WIE ALT diese sind.
 */
async function getResilientData(name, cacheKey, fetchFunction) {
    const cached = getCachedData(cacheKey);

    // Wenn der Cache gültig ist, nutze ihn, um API-Aufrufe zu sparen.
    if (cached && !cached.isExpired) {
        logDiagnostic(`[CACHE] Lade gültige '${name}' Daten aus dem Cache.`, 'success');
        return { value: cached.data, source: 'cache', timestamp: cached.timestamp };
    }

    try {
        // Versuche, Live-Daten abzurufen
        const liveData = await fetchFunction();
        logDiagnostic(`[API] '${name}' erfolgreich live abgerufen.`, 'success');
        setCachedData(cacheKey, liveData); // Speichere die frischen Daten
        return { value: liveData, source: 'live', timestamp: Date.now() };
    } catch (error) {
        logDiagnostic(`[API] Live-Abruf für '${name}' fehlgeschlagen: ${error.message}`, 'warning');
        
        // Wenn der Live-Abruf fehlschlägt, prüfen wir, ob wir irgendetwas im Cache haben
        if (cached) {
            logDiagnostic(`[FALLBACK] Nutze veraltete '${name}' Daten aus dem Cache.`, 'info');
            return { value: cached.data, source: 'cache', timestamp: cached.timestamp };
        }
        
        // Absolute Notlösung: Weder Live noch Cache verfügbar
        logDiagnostic(`[FEHLER] Keine Live- oder Cache-Daten für '${name}' verfügbar.`, 'error');
        return { value: null, source: 'none', timestamp: null };
    }
}

// HINWEIS: Die alte fetchData-Funktion ist jetzt eine "dumme" Abruffunktion ohne Caching.
async function fetchApi(name, url) {
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP Status ${response.status}`);
        const data = await response.json();
        const errorKey = Object.keys(data).find(k => k.toLowerCase().includes('error') || k.toLowerCase().includes('note'));
        if (errorKey) throw new Error(`API meldet: ${data[errorKey]}`);
        if (url.includes('GLOBAL_QUOTE') && (!data['Global Quote'] || Object.keys(data['Global Quote']).length === 0)) {
            throw new Error("Leere GLOBAL_QUOTE Antwort.");
        }
        return data;
    } catch (error) {
        throw new Error(`[${name}] ${error.message}`);
    }
}

// ===================================================================================
// DATENVERARBEITUNG & ANALYSE (Angepasst)
// ===================================================================================

// HINWEIS: Extrahiert nur noch den reinen Zahlenwert. Die Metadaten (source, timestamp) bleiben erhalten.
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

// Deine Analyse-Logik war gut, sie wird nur angepasst, um mit der neuen Datenstruktur umzugehen.
function analyzeFactors(data) {
    let score = 0;
    const analysis = { details: {}, hasStaleData: false };

    // Prüfen, ob irgendein Datensatz aus dem Cache stammt
    analysis.hasStaleData = Object.values(data).some(d => d.source === 'cache');

    const interestRate = data.interest?.specificValue;
    const inflationRate = data.inflation?.specificValue;
    
    // 1. Realzins
    if (interestRate !== null && inflationRate !== null) {
        const realInterestRate = interestRate - inflationRate;
        // ... (Rest der Analyse-Logik ist identisch, nutzt jetzt `interestRate` statt `data.interestRate`)
    } else {
        // ...
    }
    // ... REST DER ANALYSE-FUNKTION (unverändert in der Logik, nur die Variablen anpassen) ...
    return analysis;
}


// ===================================================================================
// UI-UPDATE FUNKTIONEN (Stark überarbeitet)
// ===================================================================================

function formatDataAge(timestamp) {
    if (!timestamp) return '';
    const ageInMinutes = (Date.now() - timestamp) / (1000 * 60);
    if (ageInMinutes < 60) return `(Stand: ${Math.round(ageInMinutes)} Min.)`;
    const ageInHours = ageInMinutes / 60;
    if (ageInHours < 24) return `(Stand: ${ageInHours.toFixed(1).replace('.',',')} Std.)`;
    const ageInDays = ageInHours / 24;
    return `(Stand: ${ageInDays.toFixed(0)} Tage)`;
}

// HINWEIS: Erzeugt ein kleines HTML-Element, das anzeigt, wie alt die Daten sind.
function createStaleIndicator(dataObject) {
    if (dataObject.source !== 'cache') return '';
    const age = formatDataAge(dataObject.timestamp);
    return `<span class="stale-data-indicator">${age}</span>`;
}

function updatePriceDisplay(gold, eurUsd) {
    const goldPrice = gold?.specificValue;
    const eurUsdRate = eurUsd?.specificValue;

    if (goldPrice === null || eurUsdRate === null) {
        UI.goldPrice.textContent = '--.--';
        return;
    }

    const usdPerGram = goldPrice / CONFIG.TROY_OUNCE_TO_GRAM;
    const eurPerGram = usdPerGram / eurUsdRate;

    const priceElement = document.getElementById('gold-price');
    priceElement.innerHTML = eurPerGram.toFixed(2).replace('.', ',');
    priceElement.innerHTML += createStaleIndicator(gold.source === 'cache' ? gold : eurUsd);
    
    UI.priceSourceNote.style.display = (gold?.value?.source === 'ETF_PROXY') ? 'block' : 'none';
}

function displayFactors(analysis, marketData) {
    UI.factorsContainer.innerHTML = '';
    // ...
    // HINWEIS: Innerhalb der Schleife, die die Faktoren anzeigt:
    // item.innerHTML = `... <span class="factor-value">${factor.value}${createStaleIndicator(factor.dataObject)}</span> ...`;
    // ...
}

function displayAssessment(score, missingData, hasStaleData) {
    // ...
    // HINWEIS: Füge eine zusätzliche Warnung hinzu, wenn Daten veraltet sind
    if (hasStaleData) {
        explanationText += " 🔵 HINWEIS: Einige Daten sind nicht live und stammen aus dem Cache. Die Bewertung basiert möglicherweise auf veralteten Informationen.";
    }
    // ...
}

// Hilfsfunktion zur Fehleranzeige wurde vereinfacht
function updateUIErrorState(reason) {
    UI.assessmentResult.innerHTML = `<p>${reason}</p>`;
    UI.assessmentResult.className = 'assessment-caution';
}


// ===================================================================================
// HAUPT-LOGIK & INITIALISIERUNG
// ===================================================================================
async function main() {
    if (!CONFIG.API_KEY || CONFIG.API_KEY === 'DEIN_API_SCHLUESSEL_HIER') {
        // ... (Fehlerbehandlung für fehlenden API-Schlüssel bleibt gleich)
        return;
    }
    
    UI.statusMessage.textContent = 'Rufe Livedaten ab...';
    logDiagnostic("Starte robusten Datenabruf...", 'info');

    const results = await Promise.all([
        getResilientData("EUR/USD", "eurusd", () => fetchApi("EUR/USD", `${CONFIG.API_BASE_URL}function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD&apikey=${CONFIG.API_KEY}`)),
        getResilientData("GOLD", "gold", async () => {
             // Die Gold-Fallback-Logik wird als eine einzige Funktion gekapselt
             try {
                const data = await fetchApi("GOLD (XAU/USD)", `${CONFIG.API_BASE_URL}function=CURRENCY_EXCHANGE_RATE&from_currency=XAU&to_currency=USD&apikey=${CONFIG.API_KEY}`);
                const price = parseFloat(data['Realtime Currency Exchange Rate']['5. Exchange Rate']);
                if (isNaN(price)) throw new Error("Spotpreis konnte nicht extrahiert werden.");
                return { source: 'SPOT', price };
             } catch (spotError) {
                logDiagnostic(`[GOLD] Spot fehlgeschlagen, versuche Proxy: ${spotError.message}`, 'warning');
                const data = await fetchApi(`GOLD (${CONFIG.GOLD_PROXY_SYMBOL} Proxy)`, `${CONFIG.API_BASE_URL}function=GLOBAL_QUOTE&symbol=${CONFIG.GOLD_PROXY_SYMBOL}&apikey=${CONFIG.API_KEY}`);
                const gldPrice = parseFloat(data['Global Quote']['05. price']);
                if (isNaN(gldPrice)) throw new Error("GLD Proxy Preis konnte nicht extrahiert werden.");
                return { source: 'ETF_PROXY', price: gldPrice / CONFIG.OUNCES_PER_GLD_SHARE };
             }
        }),
        getResilientData("INFLATION", "inflation", () => fetchApi("INFLATION", `${CONFIG.API_BASE_URL}function=INFLATION&apikey=${CONFIG.API_KEY}`)),
        getResilientData("ZINSEN", "interest", () => fetchApi("ZINSEN", `${CONFIG.API_BASE_URL}function=FEDERAL_FUNDS_RATE&interval=monthly&apikey=${CONFIG.API_KEY}`)),
        getResilientData("VOLATILITÄT", "volatility", () => fetchApi("VOLATILITÄT", `${CONFIG.API_BASE_URL}function=TIME_SERIES_DAILY&symbol=VXX&apikey=${CONFIG.API_KEY}`))
    ]);

    const [eurUsd, gold, inflation, interest, volatility] = results;

    // HINWEIS: Die Datenextraktion ist jetzt viel sauberer
    const marketData = {
        eurUsd: extractValue(eurUsd, 'Realtime Currency Exchange Rate.5. Exchange Rate'),
        gold: extractValue(gold, 'price'),
        inflation: extractValue(inflation, 'data.0.value'),
        interest: extractValue(interest, 'data.0.value'),
        volatility: extractValue(volatility, `Time Series (Daily).${Object.keys(volatility.value?.['Time Series (Daily)'] || {})[0]}.4. close`),
    };
    
    // Prüfen, ob KRITISCHE Daten (Gold & EUR) überhaupt vorhanden sind (weder live noch im Cache)
    if (marketData.gold.specificValue === null || marketData.eurUsd.specificValue === null) {
        const reason = "Kritische Preis- oder Währungsdaten konnten weder live noch aus dem Cache geladen werden. Eine Anzeige ist nicht möglich.";
        logDiagnostic(reason, 'error');
        UI.statusMessage.textContent = 'Fehler beim Laden kritischer Daten.';
        updateUIErrorState(reason);
        return;
    }

    logDiagnostic("Datenverarbeitung und Analyse beginnen.", 'info');
    const analysis = analyzeFactors(marketData);

    // UI Updates
    updatePriceDisplay(marketData.gold, marketData.eurUsd);
    displayFactors(analysis, marketData);
    displayAssessment(analysis.score, analysis.missingData, analysis.hasStaleData);

    const statusText = analysis.hasStaleData ? "Anzeige mit teils veralteten Daten" : "Live-Daten erfolgreich geladen";
    UI.statusMessage.textContent = `${statusText} | Letzte Aktualisierung: ${new Date().toLocaleTimeString('de-DE')}`;
    logDiagnostic("UI erfolgreich aktualisiert.", 'success');
}

// Initialisierung der App
document.addEventListener('DOMContentLoaded', () => {
    // HINWEIS: Die Logik für das Ein- und Ausklappen bleibt gleich.
    // Standardmäßig ist das Panel durch die Klasse im HTML versteckt.
    UI.toggleDiagnosticsBtn.addEventListener('click', () => {
        const isHidden = UI.diagnosticsPanel.classList.toggle('diagnostics-hidden');
        UI.toggleDiagnosticsBtn.textContent = isHidden ? 'Diagnosepanel anzeigen' : 'Diagnosepanel ausblenden';
        UI.toggleDiagnosticsBtn.setAttribute('aria-expanded', !isHidden);
    });
    
    logDiagnostic('System initialisiert.', 'info');
    main();
});
 
