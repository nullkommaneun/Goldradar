// WICHTIG: Fügen Sie hier Ihren Alpha Vantage API Schlüssel ein
const API_KEY = '547GHY3CRL7BKWPC';

document.addEventListener('DOMContentLoaded', () => {
    setupDiagnostics();
    fetchDataAndAnalyze();
});

const statusMessage = document.getElementById('status-message');

// --- Diagnose-Funktionen ---

function setupDiagnostics() {
    const diagnosticsPanel = document.getElementById('diagnostics-panel');
    const toggleDiagnosticsBtn = document.getElementById('toggle-diagnostics');

    if (toggleDiagnosticsBtn && diagnosticsPanel) {
        toggleDiagnosticsBtn.addEventListener('click', () => {
            if (diagnosticsPanel.classList.contains('diagnostics-hidden')) {
                diagnosticsPanel.classList.remove('diagnostics-hidden');
                toggleDiagnosticsBtn.textContent = 'Diagnosepanel ausblenden';
            } else {
                diagnosticsPanel.classList.add('diagnostics-hidden');
                toggleDiagnosticsBtn.textContent = 'Diagnosepanel anzeigen';
            }
        });
    }
    logDiagnostic('System initialisiert.', 'info');
}

// Erweiterung des Typs für "warning" (wird visuell wie error behandelt)
function logDiagnostic(message, type = 'info', details = null) {
    // Konsolenausgabe
    console.log(`[${type.toUpperCase()}] ${message}`, details || '');

    // UI-Ausgabe
    const diagnosticsLog = document.getElementById('diagnostics-log');
    if (!diagnosticsLog) return;

    const entry = document.createElement('div');
    // Nutze die 'error' Klasse für 'error' und 'warning' zur besseren Sichtbarkeit (Rot)
    const logClass = (type === 'error' || type === 'warning') ? 'error' : type;
    entry.className = `log-entry log-${logClass}`;

    const timestamp = new Date().toLocaleTimeString('de-DE');
    let content = `[${timestamp}] ${message}`;

    if (details) {
        try {
            const formattedDetails = JSON.stringify(details, null, 2);
            content += `\n--- API Details ---\n${formattedDetails}`;
        } catch (e) {
            content += `\n--- Details (Rohformat) ---\n${details}`;
        }
    }

    entry.textContent = content;
    diagnosticsLog.appendChild(entry);
    diagnosticsLog.scrollTop = diagnosticsLog.scrollHeight;
}

// --- Datenabruf-Funktionen ---

/**
 * Robuste Hilfsfunktion zum Abrufen von Daten. Erkennt API-Soft-Errors und leere Antworten.
 */
async function fetchData(name, url) {
    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP Fehler! Status: ${response.status}`);
        }
        const data = await response.json();

        const errorKey = Object.keys(data).find(k =>
            k.toLowerCase().includes('error') ||
            k.toLowerCase().includes('note') ||
            k.toLowerCase().includes('information')
        );

        if (errorKey) {
            const errorMessage = data[errorKey] || "Unbekannte API-Fehlerstruktur";
            const error = new Error(`API meldet: ${errorMessage}`);
            error.apiResponse = data;
            throw error;
        }
        
        // NEU: Spezielle Prüfung für GLOBAL_QUOTE, da es leer sein kann (z.B. Symbol unbekannt oder API überlastet).
        if (url.includes('GLOBAL_QUOTE') && (!data['Global Quote'] || Object.keys(data['Global Quote']).length === 0)) {
             throw new Error("API Antwort leer (Symbol unbekannt oder API überlastet).");
        }

        return data;
    } catch (error) {
        const contextualError = new Error(`[${name}] ${error.message}`);
        if (error.apiResponse) {
            contextualError.apiResponse = error.apiResponse;
        }
        throw contextualError;
    }
}

/**
 * NEU: Versucht den Goldpreis abzurufen. Nutzt GLD ETF als Fallback, falls XAU fehlschlägt.
 */
async function fetchGoldPriceWithFallback() {
    // Berechnung basierend auf ca. 21 Jahren (Nov 2004 bis Okt 2025) bei 0.40% Expense Ratio: 0.1 * (0.996)^21
    const OUNCES_PER_GLD_SHARE = 0.09194; 

    logDiagnostic("[GOLD] Versuch 1: Spotpreis (XAU/USD) abrufen...", 'info');
    try {
        // Versuch 1: Spotpreis (XAU/USD)
        const data = await fetchData("GOLD (XAU/USD)", `https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=XAU&to_currency=USD&apikey=${API_KEY}`);

        // Extraktion für Spotpreis
        const price = parseFloat(data['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        if (isNaN(price)) throw new Error("Spotpreis konnte nicht extrahiert werden.");

        return {
            source: 'SPOT',
            price: price
        };

    } catch (error) {
        // Logge den Fehler (inkl. "Invalid API call"), aber fahre mit Fallback fort
        logDiagnostic(`[GOLD] Spotpreis fehlgeschlagen. Starte Fallback auf ETF (GLD). Grund: ${error.message}`, 'warning', error.apiResponse || null);

        try {
            // Versuch 2: Fallback auf GLD ETF (Wir nutzen GLOBAL_QUOTE für den aktuellsten Preis)
            logDiagnostic("[GOLD] Versuch 2: ETF Proxy (GLD) abrufen...", 'info');
            const data = await fetchData("GOLD (GLD Proxy)", `https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=GLD&apikey=${API_KEY}`);

            // Extraktion für ETF
            const gldPrice = parseFloat(data['Global Quote']['05. price']);

            if (isNaN(gldPrice)) throw new Error("GLD Preis konnte nicht extrahiert werden.");

            // Berechnung des geschätzten Spotpreises: Preis pro Anteil / Unzen pro Anteil
            const estimatedSpotPrice = gldPrice / OUNCES_PER_GLD_SHARE;
            logDiagnostic(`[GOLD] Proxy Berechnung: GLD Preis (${gldPrice.toFixed(2)}) / OuncesPerShare (${OUNCES_PER_GLD_SHARE}) = Spot Preis (${estimatedSpotPrice.toFixed(2)})`, 'info');


            return {
                source: 'ETF_PROXY',
                price: estimatedSpotPrice
            };

        } catch (fallbackError) {
             logDiagnostic(`[GOLD] Fallback auf ETF (GLD) ebenfalls fehlgeschlagen.`, 'error', fallbackError.apiResponse || null);
             // Werfe den Fehler, damit Promise.allSettled ihn erfassen kann
             throw fallbackError;
        }
    }
}


async function fetchDataAndAnalyze() {
    if (API_KEY === 'DEIN_API_SCHLUESSEL_HIER' || API_KEY === '') {
        logDiagnostic("FEHLER: Bitte API-Schlüssel in app.js eintragen.", 'error');
        statusMessage.textContent = 'Konfigurationsfehler.';
        updateUIErrorState("API-Schlüssel fehlt.");
        return;
    }

    statusMessage.textContent = 'Rufe Livedaten ab...';
    logDiagnostic("Starte parallelen Datenabruf...", 'info');

    // Wir führen den Goldabruf (mit Fallback) parallel zu den anderen aus
    const [eurUsdResult, goldDataResult, inflationResult, interestResult, vixResult] = await Promise.allSettled([
        // 1. EUR/USD Wechselkurs
        fetchData("EUR/USD", `https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD&apikey=${API_KEY}`),
        // 2. Goldpreis (mit Fallback-Logik)
        fetchGoldPriceWithFallback(),
        // 3. Inflation (USA)
        fetchData("INFLATION", `https://www.alphavantage.co/query?function=INFLATION&apikey=${API_KEY}`),
        // 4. Leitzins (USA - Federal Funds Rate)
        fetchData("ZINSEN (FFR)", `https://www.alphavantage.co/query?function=FEDERAL_FUNDS_RATE&interval=monthly&apikey=${API_KEY}`),
        // 5. Marktunsicherheit (VXX Proxy)
        fetchData("VOLATILITÄT (VXX)", `https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=VXX&apikey=${API_KEY}`)
    ]);

    // Ergebnisse verarbeiten und protokollieren
    const rawData = {};

    // Angepasste processResult Funktion, um den speziellen Fall für Gold zu behandeln
    function processResult(result, name) {
        if (result.status === 'fulfilled') {
            // Spezielle Logik für Gold
            if (name === "GOLD") {
                 logDiagnostic(`[GOLD] Erfolgreich abgerufen. Quelle: ${result.value.source}.`, 'success');
            } else {
                logDiagnostic(`[${name}] Erfolgreich abgerufen.`, 'success');
            }
            return result.value;
        } else {
            // Fehlerbehandlung für alle (Gold Fehler werden bereits detailliert in fetchGoldPriceWithFallback geloggt)
             if (name !== "GOLD") {
                const errorDetails = result.reason.apiResponse || null;
                logDiagnostic(result.reason.message, 'error', errorDetails);
            }
            return null;
        }
    }

    rawData.eurUsdData = processResult(eurUsdResult, "EUR/USD");
    rawData.goldData = processResult(goldDataResult, "GOLD"); // Enthält nun {source, price}
    rawData.inflationData = processResult(inflationResult, "INFLATION");
    rawData.interestData = processResult(interestResult, "ZINSEN");
    rawData.vixDataRaw = processResult(vixResult, "VOLATILITÄT");

    // Daten extrahieren und auf Vollständigkeit prüfen
    try {
        const marketData = extractMarketData(rawData);

        // Prüfen, ob kritische Daten fehlen
        if (marketData.xauUsd === null || marketData.eurUsdRate === null) {
            throw new Error("Kritische Preis- oder Währungsdaten fehlen. Analyse nicht möglich.");
        }

        // Analyse durchführen
        logDiagnostic("Beginne Analyse der verfügbaren Daten.", 'info');
        updatePriceDisplay(marketData);
        const analysis = analyzeFactors(marketData);
        displayFactors(analysis);
        displayAssessment(analysis.score, analysis.missingData);

        statusMessage.textContent = `Zuletzt aktualisiert: ${new Date().toLocaleTimeString('de-DE')}`;
        logDiagnostic("Analyse abgeschlossen und UI aktualisiert.", 'success');

    } catch (error) {
        logDiagnostic(`Fehler bei der Datenverarbeitung: ${error.message}`, 'error');
        statusMessage.textContent = 'Fehler bei der Verarbeitung der Daten.';
        updateUIErrorState("Verarbeitung fehlgeschlagen. Siehe Diagnosepanel.");
    }
}

function updateUIErrorState(reason) {
    const assessmentResult = document.getElementById('assessment-result');
    assessmentResult.innerHTML = `<p>Analyse nicht möglich. Grund: ${reason}</p>`;
    assessmentResult.classList.remove('assessment-loading', 'assessment-buy', 'assessment-wait', 'assessment-caution');
    document.getElementById('factors-container').innerHTML = `<p>Indikatoren konnten nicht geladen werden.</p>`;
    document.getElementById('gold-price').textContent = '--.--';
    // Sicherstellen, dass der Proxy-Hinweis versteckt wird, wenn ein Fehler auftritt
    const priceSourceNote = document.getElementById('price-source-note');
    if (priceSourceNote) priceSourceNote.style.display = 'none';
}

/**
 * GEÄNDERT: Extrahiert die spezifischen Werte. Angepasst an die neue Gold-Datenstruktur.
 */
function extractMarketData(rawData) {
    const marketData = {
        eurUsdRate: null,
        xauUsd: null,
        goldSource: null, // NEU: Speichert, ob SPOT oder ETF_PROXY
        inflationRate: null,
        interestRate: null,
        vixValue: null
    };

    // EUR/USD
    try {
        if (rawData.eurUsdData && rawData.eurUsdData['Realtime Currency Exchange Rate']) {
            marketData.eurUsdRate = parseFloat(rawData.eurUsdData['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        }
    } catch (e) { logDiagnostic('Fehler beim Extrahieren von EUR/USD Daten.', 'error'); }

    // GOLD (Neue Struktur)
    try {
        if (rawData.goldData && typeof rawData.goldData.price === 'number') {
            marketData.xauUsd = rawData.goldData.price;
            marketData.goldSource = rawData.goldData.source;
        }
    } catch (e) { logDiagnostic('Fehler beim Extrahieren von Gold Daten.', 'error'); }

    // INFLATION, ZINSEN, VOLATILITÄT (Bleiben gleich)
    try {
        if (rawData.inflationData && rawData.inflationData.data && rawData.inflationData.data.length > 0) {
            marketData.inflationRate = parseFloat(rawData.inflationData.data[0].value);
        }
    } catch (e) { logDiagnostic('Fehler beim Extrahieren von Inflationsdaten.', 'error'); }

    try {
         if (rawData.interestData && rawData.interestData.data && rawData.interestData.data.length > 0) {
            marketData.interestRate = parseFloat(rawData.interestData.data[0].value);
         }
    } catch (e) { logDiagnostic('Fehler beim Extrahieren von Zinsdaten.', 'error'); }

    try {
        if (rawData.vixDataRaw && rawData.vixDataRaw['Time Series (Daily)']) {
            const vixTimeSeries = rawData.vixDataRaw['Time Series (Daily)'];
            const latestVixDate = Object.keys(vixTimeSeries)[0];
            if (latestVixDate) {
                marketData.vixValue = parseFloat(vixTimeSeries[latestVixDate]['4. close']);
            }
        }
    } catch (e) { logDiagnostic('Fehler beim Extrahieren von VXX Daten.', 'error'); }

    return marketData;
}


/**
 * GEÄNDERT: Zeigt ggf. den Hinweis auf die ETF-Quelle an.
 */
function updatePriceDisplay(data) {
    // 1 Feinunze (Troy Ounce) = 31.1035 Gramm
    const TROY_OUNCE_TO_GRAM = 31.1035;

    // Berechnung: (XAU in USD) / (Gramm pro Unze) / (EUR in USD)
    const usdPerGram = data.xauUsd / TROY_OUNCE_TO_GRAM;
    const eurPerGram = usdPerGram / data.eurUsdRate;

    document.getElementById('gold-price').textContent = eurPerGram.toFixed(2).replace('.', ',');

    // Hinweis anzeigen, wenn ETF Proxy verwendet wurde
    const priceSourceNote = document.getElementById('price-source-note');
    if (priceSourceNote) {
        if (data.goldSource === 'ETF_PROXY') {
            priceSourceNote.style.display = 'block';
        } else {
            priceSourceNote.style.display = 'none';
        }
    }
}

// --- Analyse-Funktionen (Bleiben unverändert) ---
// Die Analyse-Logik muss nicht geändert werden, da sie bereits robust ist.

function analyzeFactors(data) {
    let score = 0;
    const analysis = { details: {}, missingData: false };

    // 1. Realzins (Zins - Inflation)
    if (data.interestRate !== null && data.inflationRate !== null) {
        const realInterestRate = data.interestRate - data.inflationRate;
        analysis.details.realInterestRate = { raw: realInterestRate };

        if (realInterestRate > 2.0) {
            score -= 3; // Stark negativ
            analysis.details.realInterestRate.impact = 'negative';
        } else if (realInterestRate > 0.5) {
            score -= 1; // Leicht negativ
            analysis.details.realInterestRate.impact = 'negative';
        } else if (realInterestRate < -1.0) {
            score += 3; // Stark positiv
            analysis.details.realInterestRate.impact = 'positive';
        } else if (realInterestRate < 0) {
            score += 1; // Leicht positiv
            analysis.details.realInterestRate.impact = 'positive';
        } else {
            analysis.details.realInterestRate.impact = 'neutral';
        }
    } else {
        analysis.details.realInterestRate = { impact: 'neutral', raw: null };
        if (data.interestRate === null || data.inflationRate === null) {
            analysis.missingData = true;
        }
    }

    // 2. Inflationstrend
    if (data.inflationRate !== null) {
        analysis.details.inflation = { raw: data.inflationRate };
        if (data.inflationRate > 4.0) {
            score += 2;
            analysis.details.inflation.impact = 'positive';
        } else if (data.inflationRate > 2.5) {
            score += 1;
            analysis.details.inflation.impact = 'positive';
        } else if (data.inflationRate < 1.5) {
            score -= 1;
            analysis.details.inflation.impact = 'negative';
        } else {
            analysis.details.inflation.impact = 'neutral';
        }
    } else {
         analysis.details.inflation = { impact: 'neutral', raw: null };
    }


    // 3. USD Stärke (via EUR/USD)
    if (data.eurUsdRate !== null) {
        analysis.details.eurUsd = { raw: data.eurUsdRate };
        // Abweichungen vom langfristigen Durchschnitt (angenommen ca. 1.10)
        if (data.eurUsdRate < 1.05) { // Starker USD
            score -= 1;
            analysis.details.eurUsd.impact = 'negative';
        } else if (data.eurUsdRate > 1.15) { // Schwacher USD
            score += 1;
            analysis.details.eurUsd.impact = 'positive';
        } else {
            analysis.details.eurUsd.impact = 'neutral';
        }
    } else {
        analysis.details.eurUsd = { impact: 'neutral', raw: null };
    }


    // 4. Marktunsicherheit/Volatilität (via VXX Proxy)
    if (data.vixValue !== null) {
        analysis.details.vix = { raw: data.vixValue, name: "Marktunsicherheit (VXX Proxy)" };

        // Schwellenwerte für VXX
        if (data.vixValue > 45) {
            score += 3; // Hohe Unsicherheit
            analysis.details.vix.impact = 'positive';
        } else if (data.vixValue > 30) {
            score += 1; // Moderate Unsicherheit
            analysis.details.vix.impact = 'positive';
        } else if (data.vixValue < 20) {
            score -= 1; // Geringe Unsicherheit
            analysis.details.vix.impact = 'negative';
        } else {
            analysis.details.vix.impact = 'neutral';
        }
    } else {
        analysis.details.vix = { impact: 'neutral', raw: null, name: "Marktunsicherheit (VXX Proxy)" };
        analysis.missingData = true;
    }

    analysis.score = score;
    return analysis;
}

function displayFactors(analysis) {
    const container = document.getElementById('factors-container');
    container.innerHTML = '';
    const details = analysis.details;

    const formatValue = (rawData, suffix = '', decimals = 2) => {
        if (rawData === null || isNaN(rawData)) return 'N/A';
        return rawData.toFixed(decimals).replace('.', ',') + suffix;
    };

    const factors = [
        {
            name: "Realzins (Zins - Inflation)",
            value: formatValue(details.realInterestRate.raw, '%'),
            impact: details.realInterestRate.impact
        },
        {
            name: "Inflation (USA)",
            value: formatValue(details.inflation.raw, '%', 1),
            impact: details.inflation.impact
        },
        {
            name: "EUR/USD Wechselkurs",
            value: formatValue(details.eurUsd.raw, '', 4),
            impact: details.eurUsd.impact
        },
        {
            name: details.vix.name,
            value: formatValue(details.vix.raw),
            impact: details.vix.impact
        }
    ];

    factors.forEach(factor => {
        const item = document.createElement('div');
        item.className = 'factor-item';

        let impactText = 'N/A (Daten fehlen)';
        if (factor.impact === 'positive') impactText = 'Bullish (Gut für Gold)';
        if (factor.impact === 'negative') impactText = 'Bearish (Schlecht für Gold)';
        if (factor.impact === 'neutral' && factor.value !== 'N/A') impactText = 'Neutral';

        const impactClass = factor.value === 'N/A' ? 'neutral' : factor.impact;


        item.innerHTML = `
            <span class="factor-name">${factor.name}</span>
            <div class="factor-data">
                <span class="factor-value">${factor.value}</span>
                <span class="factor-impact ${impactClass}">${impactText}</span>
            </div>
        `;
        container.appendChild(item);
    });
}

function displayAssessment(score, missingData) {
    const resultDiv = document.getElementById('assessment-result');
    resultDiv.className = '';
    resultDiv.innerHTML = '';

    let assessmentText = '';
    let assessmentClass = '';
    let explanationText = '';

    if (score >= 4) {
        assessmentText = "Starkes Kaufsignal";
        assessmentClass = "assessment-buy";
        explanationText = "Die verfügbaren Indikatoren sprechen stark für einen Goldkauf.";
    } else if (score > 0) {
        assessmentText = "Kaufsignal";
        assessmentClass = "assessment-buy";
        explanationText = "Die Marktlage ist tendenziell günstig für Goldinvestitionen.";
    } else if (score >= -2) {
        assessmentText = "Neutral / Abwarten";
        assessmentClass = "assessment-wait";
        explanationText = "Die Indikatoren sind gemischt. Positive und negative Faktoren gleichen sich aus.";
    } else {
        assessmentText = "Vorsicht / Nicht Kaufen";
        assessmentClass = "assessment-caution";
        explanationText = "Die verfügbaren Indikatoren sprechen derzeit gegen eine Goldinvestition.";
    }

    if (missingData) {
        explanationText += " ⚠️ ACHTUNG: Nicht alle Indikatoren konnten geladen werden (siehe Diagnosepanel). Die Bewertung ist unsicher.";
         if (assessmentClass === "assessment-buy" || assessmentClass === "assessment-caution") {
            assessmentClass = "assessment-wait";
        }
    }

    const header = document.createElement('h3');
    header.textContent = `${assessmentText} (Score: ${score})`;

    const explanation = document.createElement('p');
    explanation.textContent = explanationText;

    resultDiv.appendChild(header);
    resultDiv.appendChild(explanation);
    resultDiv.classList.add(assessmentClass);
}
