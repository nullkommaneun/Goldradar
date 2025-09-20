/* preflight.js
 * Clientseitige Datenvalidierung für history.json & spot.json.
 * Ergebnis: window.PREFLIGHT = { ok, warnings, errors, stats }
 * Schreibt kurze Meldungen in #diag (falls vorhanden).
 */

(function () {
  const DIAG = document.getElementById('diag');
  const log = (msg) => {
    if (!DIAG) return;
    const now = new Date().toISOString().slice(11, 19);
    DIAG.textContent = (DIAG.textContent ? DIAG.textContent + "\n" : "") + `[${now}] ${msg}`;
  };
  const bust = () => `?t=${Date.now()}`;
  const isNum = (v) => Number.isFinite(v);
  const isoDateRE = /^\d{4}-\d{2}-\d{2}$/;
  const isoDTRE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;

  const SERIES = [
    "GOLDAMGBD228NLBM","DFII10","DTWEXBGS","VIXCLS","DCOILBRENTEU",
    "T10YIE","BAMLH0A0HYM2","NAPM","RECPROUSM156N","T10Y2Y"
  ];

  /** Validiert history.json */
  function validateHistory(obj) {
    const errors = [];
    const warnings = [];
    const stats = {
      rows: 0,
      start: null,
      end: null,
      series_counts: Object.fromEntries(SERIES.map(k => [k, 0])),
      null_counts: Object.fromEntries(SERIES.map(k => [k, 0])),
    };

    if (!obj || !Array.isArray(obj.history)) {
      errors.push("history.json: Feld `history` fehlt oder ist kein Array.");
      return { ok: false, errors, warnings, stats };
    }

    const rows = obj.history;
    stats.rows = rows.length;
    if (rows.length === 0) {
      warnings.push("history.json: Keine Zeilen.");
      return { ok: true, errors, warnings, stats };
    }

    let prevTs = -Infinity;
    const now = Date.now();
    const maxFuture = now + 36 * 3600 * 1000; // +36h Toleranz (Zeitzonen)

    rows.forEach((r, i) => {
      // timestamp
      if (typeof r.timestamp !== "string" || !isoDateRE.test(r.timestamp)) {
        errors.push(`history.json: Zeile ${i} hat ungültigen timestamp (erwartet YYYY-MM-DD).`);
        return;
      }
      const t = Date.parse(r.timestamp + "T00:00:00Z");
      if (!(t > prevTs)) {
        errors.push(`history.json: Zeile ${i} timestamp ist nicht strikt ansteigend.`);
      }
      if (t > maxFuture) {
        warnings.push(`history.json: Zeile ${i} liegt in der Zukunft (${r.timestamp}).`);
      }
      if (i === 0) stats.start = r.timestamp;
      stats.end = r.timestamp;
      prevTs = t;

      // Felder prüfen
      for (const k of SERIES) {
        const v = r[k];
        if (v == null) {
          stats.null_counts[k]++;
        } else if (!isNum(v)) {
          errors.push(`history.json: Zeile ${i} Feld ${k} ist kein numerischer Wert oder null.`);
        } else {
          stats.series_counts[k]++;
        }
      }
    });

    // Recency
    const endTs = Date.parse(stats.end + "T00:00:00Z");
    const ageDays = Math.floor((now - endTs) / 86400000);
    if (ageDays > 5) {
      warnings.push(`history.json: Letzter Tag ist ${ageDays} Tage alt (${stats.end}).`);
    }

    const ok = errors.length === 0;
    return { ok, errors, warnings, stats };
  }

  /** Validiert spot.json */
  function validateSpot(obj) {
    const errors = [];
    const warnings = [];
    const stats = { timestamp: null, hasValue: false };

    if (!obj || typeof obj !== "object") {
      errors.push("spot.json: Datei nicht lesbar.");
      return { ok: false, errors, warnings, stats };
    }

    const { timestamp, XAUUSD } = obj;
    if (timestamp != null) {
      if (typeof timestamp !== "string" || !isoDTRE.test(timestamp)) {
        errors.push("spot.json: `timestamp` muss ISO8601 (YYYY-MM-DDTHH:MM:SSZ) sein.");
      } else {
        stats.timestamp = timestamp;
        const t = Date.parse(timestamp);
        const ageH = (Date.now() - t) / 3600000;
        if (ageH > 72) {
          warnings.push(`spot.json: Spot älter als ${Math.floor(ageH)}h.`);
        }
      }
    } else {
      warnings.push("spot.json: `timestamp` fehlt.");
    }

    if (XAUUSD == null) {
      warnings.push("spot.json: `XAUUSD` ist null – UI nutzt dann Fix/History.");
    } else if (!isNum(Number(XAUUSD))) {
      errors.push("spot.json: `XAUUSD` ist kein numerischer Wert.");
    } else {
      const v = Number(XAUUSD);
      // Plausibilitätsbereich in USD/kg (nicht zu eng fassen)
      if (v < 300 || v > 150000) {
        warnings.push(`spot.json: XAUUSD außerhalb des plausiblen Bereichs (${v}).`);
      }
      stats.hasValue = true;
    }

    return { ok: errors.length === 0, errors, warnings, stats };
  }

  async function run() {
    try {
      log("Preflight startet …");
      const [h, s] = await Promise.all([
        fetch("data/history.json" + bust()).then(r => r.json()),
        fetch("data/spot.json" + bust()).then(r => r.json())
      ]);

      const vh = validateHistory(h);
      const vs = validateSpot(s);

      const ok = vh.ok && vs.ok;
      const warnings = [...vh.warnings, ...vs.warnings];
      const errors = [...vh.errors, ...vs.errors];

      // Kurzer UI-Output
      const head = ok ? (warnings.length ? "Preflight WARN" : "Preflight OK") : "Preflight FAIL";
      log(`${head}`);
      log(`history.len=${vh.stats.rows}`);
      if (vh.stats.start && vh.stats.end) {
        log(`range=${vh.stats.start} … ${vh.stats.end}`);
      }
      if (s && s.XAUUSD != null) {
        log(`spot=${Number(s.XAUUSD)}`);
      }

      if (warnings.length) log("Warnungen: " + warnings.join(" | "));
      if (errors.length)   log("Fehler: " + errors.join(" | "));

      // Weltweit verfügbar machen
      window.PREFLIGHT = {
        ok, warnings, errors,
        stats: {
          history: vh.stats,
          spot: vs.stats
        }
      };
    } catch (e) {
      log("Preflight ERROR: " + (e && e.message ? e.message : String(e)));
      window.PREFLIGHT = { ok: false, warnings: [], errors: [String(e)], stats: {} };
    }
  }

  run();
})();
