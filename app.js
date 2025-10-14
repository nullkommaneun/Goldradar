document.addEventListener('DOMContentLoaded', () => {
    fetchDataAndAnalyze();
});

// WICHTIG: Fügen Sie hier Ihren Alpha Vantage API Schlüssel ein
const API_KEY = '547GHY3CRL7BKWPC';
const statusMessage = document.getElementById('status-message');

// Hilfsfunktion zum Abrufen von Daten von der Alpha Vantage API
async function fetchData(url) {
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP Fehler! Status: ${response.status}`);
    }
    const data = await response.json();
    // Alpha Vantage gibt oft Fehlermeldungen oder Nutzungshinweise im JSON zurück
    if (data['Error Message'] || data['Note'] || data['Information']) {
        console.error("API Response Detail:", data);
        throw new Error("API-Limit erreicht oder ungültige Anfrage. Bitte warten Sie oder prüfen Sie den API-Schlüssel.");
    }
    return data;
}

async function fetchDataAndAnalyze() {
    if (API_KEY === 'DEIN_API_SCHLUESSEL_HIER') {
        statusMessage.textContent = 'Fehler: Bitte API-Schlüssel in app.js eintragen.';
        document.getElementById('assessment-result').innerHTML = "<p>API-Schlüssel fehlt.</p>";
        document.getElementById('factors-container').innerHTML = "<p>Keine Daten verfügbar.</p>";
        return;
    }

    try {
        statusMessage.textContent = 'Rufe Livedaten ab... (Dies kann aufgrund von API-Limits dauern)';

        // Wir führen die Aufrufe parallel aus, um die Ladezeit zu optimieren
        const [eurUsdData, goldData, inflationData, interestData, vixDataRaw] = await Promise.all([
            // 1. EUR/USD Wechselkurs
            fetchData(`https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD&apikey=${API_KEY}`),
            // 2. Goldpreis (XAU/USD)
            fetchData(`https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=XAU&to_currency=USD&apikey=${API_KEY}`),
            // 3. Inflation (USA)
            fetchData(`https://www.alphavantage.co/query?function=INFLATION&apikey=${API_KEY}`),
            // 4. Leitzins (USA - Federal Funds Rate)
            fetchData(`https://www.alphavantage.co/query?function=FEDERAL_FUNDS_RATE&interval=monthly&apikey=${API_KEY}`),
            // 5. Marktunsicherheit (VIX)
            fetchData(`https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=VIX&apikey=${API_KEY}`)
        ]);

        // Daten extrahieren
        const eurUsdRate = parseFloat(eurUsdData['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        const xauUsd = parseFloat(goldData['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        const inflationRate = parseFloat(inflationData.data[0].value);
        const interestRate = parseFloat(interestData.data[0].value);

        // VIX Daten extrahieren (letzter Schlusskurs)
        const vixTimeSeries = vixDataRaw['Time Series (Daily)'];
        const latestVixDate = Object.keys(vixTimeSeries)[0];
        const vixValue = parseFloat(vixTimeSeries[latestVixDate]['4. close']);

        // Datenobjekt zusammenstellen
        const marketData = {
            eurUsdRate,
            xauUsd,
            inflationRate,
            interestRate,
            vixValue
        };

        // Analyse durchführen
        updatePriceDisplay(marketData);
        const analysis = analyzeFactors(marketData);
        displayFactors(marketData, analysis);
        displayAssessment(analysis.score);

        statusMessage.textContent = `Zuletzt aktualisiert: ${new Date().toLocaleTimeString('de-DE')}`;

    } catch (error) {
        console.error("Fehler beim Verarbeiten der Daten:", error);
        statusMessage.textContent = `Fehler: ${error.message}`;
        const assessmentResult = document.getElementById('assessment-result');
        assessmentResult.innerHTML = `<p>Fehler beim Laden der Daten. Möglicherweise wurde das tägliche API-Limit erreicht.</p>`;
        assessmentResult.classList.remove('assessment-loading');
        document.getElementById('factors-container').innerHTML = `<p>Fehler beim Abrufen der Indikatoren.</p>`;
    }
}

function updatePriceDisplay(data) {
    // 1 Feinunze (Troy Ounce) = 31.1035 Gramm
    const TROY_OUNCE_TO_GRAM = 31.1035;

    // Berechnung: (XAU in USD) / (Gramm pro Unze) / (EUR in USD)
    // Dies ergibt den Preis pro Gramm in EUR.
    const usdPerGram = data.xauUsd / TROY_OUNCE_TO_GRAM;
    const eurPerGram = usdPerGram / data.eurUsdRate;

    document.getElementById('gold-price').textContent = eurPerGram.toFixed(2).replace('.', ',');
}

// Kernlogik: Gewichtung der Faktoren
function analyzeFactors(data) {
    let score = 0;
    const analysis = {};

    // 1. Realzins (Zins - Inflation)
    // Wichtigster Faktor. Hohe Realzinsen machen Anleihen attraktiver als Gold (das keine Zinsen abwirft).
    const realInterestRate = data.interestRate - data.inflationRate;
    analysis.realInterestRate = { value: realInterestRate };
    if (realInterestRate > 2.0) {
        score -= 3; // Stark negativ
        analysis.realInterestRate.impact = 'negative';
    } else if (realInterestRate > 0.5) {
        score -= 1; // Leicht negativ
        analysis.realInterestRate.impact = 'negative';
    } else if (realInterestRate < -1.0) {
        score += 3; // Stark positiv (Negativzinsen)
        analysis.realInterestRate.impact = 'positive';
    } else if (realInterestRate < 0) {
        score += 1; // Leicht positiv
        analysis.realInterestRate.impact = 'positive';
    } else {
        analysis.realInterestRate.impact = 'neutral';
    }

    // 2. Inflationstrend
    // Hohe Inflation begünstigt Gold als Wertspeicher.
    analysis.inflation = { value: data.inflationRate };
    if (data.inflationRate > 4.0) {
        score += 2;
        analysis.inflation.impact = 'positive';
    } else if (data.inflationRate > 2.5) {
        score += 1;
        analysis.inflation.impact = 'positive';
    } else if (data.inflationRate < 1.5) {
        score -= 1;
        analysis.inflation.impact = 'negative';
    } else {
        analysis.inflation.impact = 'neutral';
    }

    // 3. USD Stärke (via EUR/USD)
    // Ein starker USD (niedriger EUR/USD Kurs) ist tendenziell schlecht für den Goldpreis in USD.
    analysis.eurUsd = { value: data.eurUsdRate };
    // Wir betrachten Abweichungen vom langfristigen Durchschnitt (angenommen ca. 1.10)
    if (data.eurUsdRate < 1.05) { // Starker USD relativ zum EUR
        score -= 1;
        analysis.eurUsd.impact = 'negative';
    } else if (data.eurUsdRate > 1.15) { // Schwacher USD relativ zum EUR
        score += 1;
        analysis.eurUsd.impact = 'positive';
    } else {
        analysis.eurUsd.impact = 'neutral';
    }

    // 4. Marktunsicherheit/Volatilität (VIX)
    // Hohe Volatilität begünstigt Gold als "sicheren Hafen".
    analysis.vix = { value: data.vixValue };
    if (data.vixValue > 30) {
        score += 3; // Hohe Unsicherheit
        analysis.vix.impact = 'positive';
    } else if (data.vixValue > 20) {
        score += 1; // Moderate Unsicherheit
        analysis.vix.impact = 'positive';
    }
     else if (data.vixValue < 13) {
        score -= 1; // Geringe Unsicherheit
        analysis.vix.impact = 'negative';
    } else {
        analysis.vix.impact = 'neutral';
    }

    return { score, details: analysis };
}


function displayFactors(data, analysis) {
    const container = document.getElementById('factors-container');
    container.innerHTML = '';

    const factors = [
        {
            name: "Realzins (Zins - Inflation)",
            value: `${analysis.details.realInterestRate.value.toFixed(2)}%`,
            impact: analysis.details.realInterestRate.impact
        },
        {
            name: "Inflation (USA)",
            value: `${data.inflationRate.toFixed(1)}%`,
            impact: analysis.details.inflation.impact
        },
        {
            name: "EUR/USD Wechselkurs",
            value: data.eurUsdRate.toFixed(4),
            impact: analysis.details.eurUsd.impact
        },
        {
            name: "Marktunsicherheit (VIX)",
            value: data.vixValue.toFixed(2),
            impact: analysis.details.vix.impact
        }
    ];

    factors.forEach(factor => {
        const item = document.createElement('div');
        item.className = 'factor-item';
        const impactText = factor.impact === 'positive' ? 'Bullish (Gut für Gold)' : factor.impact === 'negative' ? 'Bearish (Schlecht für Gold)' : 'Neutral';
        item.innerHTML = `
            <span class="factor-name">${factor.name}</span>
            <div class="factor-data">
                <span class="factor-value">${factor.value}</span>
                <span class="factor-impact ${factor.impact}">${impactText}</span>
            </div>
        `;
        container.appendChild(item);
    });
}

function displayAssessment(score) {
    const resultDiv = document.getElementById('assessment-result');
    resultDiv.classList.remove('assessment-loading');
    resultDiv.innerHTML = '';

    let assessmentText = '';
    let assessmentClass = '';
    let explanationText = '';

    // Schwellenwerte für die Bewertung (Maximalscore ca. +9, Minimalscore ca. -8)
    if (score >= 4) {
        assessmentText = "Starkes Kaufsignal";
        assessmentClass = "assessment-buy";
        explanationText = "Die aktuellen Indikatoren (insbesondere niedrige Realzinsen und/oder hohe Unsicherheit/Inflation) sprechen stark für einen Goldkauf.";
    } else if (score > 0) {
        assessmentText = "Kaufsignal";
        assessmentClass = "assessment-buy";
        explanationText = "Die Marktlage ist tendenziell günstig für Goldinvestitionen. Ein Kauf kann sich lohnen.";
    } else if (score >= -2) {
        assessmentText = "Neutral / Abwarten";
        assessmentClass = "assessment-wait";
        explanationText = "Die Indikatoren sind gemischt. Positive und negative Faktoren gleichen sich aus.";
    } else {
        assessmentText = "Vorsicht / Nicht Kaufen";
        assessmentClass = "assessment-caution";
        explanationText = "Hohe Realzinsen und geringe Marktunsicherheit machen andere Anlageklassen derzeit attraktiver als Gold.";
    }

    const header = document.createElement('h3');
    header.textContent = `${assessmentText} (Score: ${score})`;

    const explanation = document.createElement('p');
    explanation.textContent = explanationText;

    resultDiv.appendChild(header);
    resultDiv.appendChild(explanation);
    resultDiv.classList.add(assessmentClass);
}
