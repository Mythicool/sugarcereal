const els = {
  form: document.getElementById("analyzerForm"),
  serialInput: document.getElementById("serialInput"),
  denominationInput: document.getElementById("denominationInput"),
  seriesInput: document.getElementById("seriesInput"),
  datasetToggle: document.getElementById("datasetToggle"),
  datasetSection: document.getElementById("datasetSection"),
  birthdaysFile: document.getElementById("birthdaysFile"),
  historicalFile: document.getElementById("historicalFile"),
  eventsFile: document.getElementById("eventsFile"),
  holidaysFile: document.getElementById("holidaysFile"),
  zipReferenceFile: document.getElementById("zipReferenceFile"),
  analyzeBtn: document.getElementById("analyzeBtn"),
  sampleBtn: document.getElementById("sampleBtn"),
  exportXlsxBtn: document.getElementById("exportXlsxBtn"),
  exportCsvBtn: document.getElementById("exportCsvBtn"),
  rankingSort: document.getElementById("rankingSort"),
  serialSort: document.getElementById("serialSort"),
  patternFilter: document.getElementById("patternFilter"),
  patternFilterMeta: document.getElementById("patternFilterMeta"),
  statusLine: document.getElementById("statusLine"),
  resultsSection: document.getElementById("resultsSection"),
  metricSerials: document.getElementById("metricSerials"),
  metricRows: document.getElementById("metricRows"),
  metricTopScore: document.getElementById("metricTopScore"),
  metricTopBand: document.getElementById("metricTopBand"),
  rankingTableBody: document.getElementById("rankingTableBody"),
  sellableTableBody: document.getElementById("sellableTableBody"),
  datasetWarnings: document.getElementById("datasetWarnings"),
  zipQuickReference: document.getElementById("zipQuickReference"),
  serialCards: document.getElementById("serialCards"),
};

const state = {
  pyodide: null,
  analyzeFn: null,
  datasetsReady: false,
  lastResult: null,
};

const SAMPLE_SERIALS = ["12344321", "8566-8449", "233-82814", "00123456*", "9O817263"];
const DEFAULT_DATASET_FILES = [
  "birthdays.csv",
  "World Important Dates.csv",
  "disorder_events_sample.csv",
  "us_public_holidays.csv",
  "us_zip_reference.csv",
];

function escapeHtml(text) {
  return String(text ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setStatus(message, kind = "idle") {
  els.statusLine.textContent = message;
  els.statusLine.className = `status ${kind}`;
}

function setBusy(flag) {
  els.analyzeBtn.disabled = flag;
  els.sampleBtn.disabled = flag;
  els.exportXlsxBtn.disabled = flag;
  els.exportCsvBtn.disabled = flag;
}

async function ensurePyodide() {
  if (state.pyodide && state.analyzeFn) {
    return;
  }

  if (!window.loadPyodide) {
    throw new Error("Pyodide runtime was not loaded.");
  }

  setStatus("Loading Python runtime...", "busy");
  const pyodide = await window.loadPyodide({
    indexURL: "https://cdn.jsdelivr.net/pyodide/v0.27.5/full/",
  });

  setStatus("Loading analyzer module...", "busy");
  const analyzerCodeResponse = await fetch("./fancy_serial_analyzer.py");
  if (!analyzerCodeResponse.ok) {
    throw new Error("Unable to load analyzer module from deploy files.");
  }
  const analyzerCode = await analyzerCodeResponse.text();

  pyodide.globals.set("ANALYZER_SOURCE", analyzerCode);
  pyodide.runPython(`
import json

ANALYZER_NS = {
    "__name__": "fancy_serial_analyzer_web",
    "__file__": "fancy_serial_analyzer.py",
}
exec(ANALYZER_SOURCE, ANALYZER_NS)

def analyze_for_browser(payload_json):
    payload = json.loads(payload_json)
    result = ANALYZER_NS["analyze_serials_for_web"](
        serials=payload.get("serials", []),
        raw_input_text=payload.get("raw_input_text"),
        denomination=payload.get("denomination", "$1"),
        series=payload.get("series", "2021"),
        data_dir=payload.get("data_dir"),
        include_dataset_dates=payload.get("include_dataset_dates", False),
    )
    return json.dumps(result)
  `);

  state.pyodide = pyodide;
  state.analyzeFn = pyodide.globals.get("analyze_for_browser");
}

async function ensureBundledDatasets() {
  if (state.datasetsReady) {
    return;
  }

  setStatus("Loading bundled datasets...", "busy");
  for (const fileName of DEFAULT_DATASET_FILES) {
    const response = await fetch(`./${encodeURIComponent(fileName)}`);
    if (!response.ok) {
      throw new Error(`Bundled dataset missing: ${fileName}`);
    }
    const bytes = new Uint8Array(await response.arrayBuffer());
    state.pyodide.FS.writeFile(fileName, bytes);
  }

  state.datasetsReady = true;
}

async function writeDatasetOverridesToPyodide() {
  const files = [
    ["birthdays.csv", els.birthdaysFile.files[0]],
    ["World Important Dates.csv", els.historicalFile.files[0]],
    ["disorder_events_sample.csv", els.eventsFile.files[0]],
    ["us_public_holidays.csv", els.holidaysFile.files[0]],
    ["us_zip_reference.csv", els.zipReferenceFile.files[0]],
  ];

  for (const [targetName, file] of files) {
    if (!file) {
      continue;
    }
    const bytes = new Uint8Array(await file.arrayBuffer());
    state.pyodide.FS.writeFile(targetName, bytes);
  }
}

function renderMetrics(result) {
  const top = result.serial_rankings?.[0] || null;
  els.metricSerials.textContent = String(result.serial_count ?? 0);
  els.metricRows.textContent = String(result.pattern_row_count ?? 0);
  els.metricTopScore.textContent = String(top?.high_signal_score ?? 0);
  els.metricTopBand.textContent = top?.band ?? "LOW";
}

function bandClass(band) {
  return `band-pill band-${(band || "LOW").toUpperCase()}`;
}

function selectedPatternFilter() {
  return els.patternFilter?.value || "__ALL__";
}

function patternCountsFromResult(result) {
  const counts = new Map();
  for (const serialResult of result.serial_results || []) {
    for (const pattern of serialResult.patterns || []) {
      const name = String(pattern.pattern_name || "").trim();
      if (!name) {
        continue;
      }
      counts.set(name, (counts.get(name) || 0) + 1);
    }
  }
  return [...counts.entries()].sort((a, b) => a[0].localeCompare(b[0]));
}

function refreshPatternFilterOptions(result) {
  const options = patternCountsFromResult(result);
  const current = selectedPatternFilter();
  const validValues = new Set(["__ALL__", ...options.map(([name]) => name)]);
  const selected = validValues.has(current) ? current : "__ALL__";

  const html = [
    `<option value="__ALL__">All Patterns</option>`,
    ...options.map(([name, count]) => `<option value="${escapeHtml(name)}">${escapeHtml(name)} (${count})</option>`),
  ].join("");
  els.patternFilter.innerHTML = html;
  els.patternFilter.value = selected;
}

function filteredSerialSet(result) {
  const selected = selectedPatternFilter();
  if (!selected || selected === "__ALL__") {
    return null;
  }
  const matched = new Set();
  for (const serialResult of result.serial_results || []) {
    const hasPattern = (serialResult.patterns || []).some((p) => p.pattern_name === selected);
    if (hasPattern) {
      matched.add(serialResult.serial);
    }
  }
  return matched;
}

function sortedSellable(result) {
  const serials = filteredSerialSet(result);
  const rows = [...(result.top_sellable_serials || [])];
  if (!serials) {
    return rows;
  }
  return rows.filter((r) => serials.has(r.serial));
}

function sortedRankings(result) {
  const serials = filteredSerialSet(result);
  const rankings = [...(result.serial_rankings || [])];
  const sortMode = els.rankingSort.value;
  const filtered = serials ? rankings.filter((r) => serials.has(r.serial)) : rankings;

  if (sortMode === "lex_desc") {
    filtered.sort((a, b) => b.lexical_score - a.lexical_score || b.high_signal_score - a.high_signal_score);
  } else if (sortMode === "pattern_desc") {
    filtered.sort((a, b) => b.pattern_score - a.pattern_score || b.high_signal_score - a.high_signal_score);
  } else if (sortMode === "serial_asc") {
    filtered.sort((a, b) => String(a.serial).localeCompare(String(b.serial)));
  } else {
    filtered.sort(
      (a, b) =>
        b.high_signal_score - a.high_signal_score ||
        b.lexical_score - a.lexical_score ||
        b.pattern_score - a.pattern_score,
    );
  }

  return filtered.map((item, idx) => ({ ...item, rank: idx + 1 }));
}

function sortedSerialResults(result) {
  const serials = filteredSerialSet(result);
  const serialResults = [...(result.serial_results || [])];
  const sortMode = els.serialSort.value;
  const rankingBySerial = new Map((result.serial_rankings || []).map((r) => [r.serial, r]));
  const sellableBySerial = new Map((result.top_sellable_serials || []).map((r) => [r.serial, r]));
  const selected = selectedPatternFilter();
  let filtered = serials ? serialResults.filter((r) => serials.has(r.serial)) : serialResults;

  if (selected && selected !== "__ALL__") {
    filtered = filtered.map((serialResult) => {
      const matchingPatterns = (serialResult.patterns || []).filter((p) => p.pattern_name === selected);
      return {
        ...serialResult,
        patterns: matchingPatterns,
        pattern_count: matchingPatterns.length,
      };
    });
  }

  filtered.sort((a, b) => {
    if (sortMode === "serial_asc") {
      return String(a.serial).localeCompare(String(b.serial));
    }
    if (sortMode === "pattern_count_desc") {
      return (b.pattern_count || 0) - (a.pattern_count || 0);
    }
    if (sortMode === "high_desc") {
      const ah = rankingBySerial.get(a.serial)?.high_signal_score || 0;
      const bh = rankingBySerial.get(b.serial)?.high_signal_score || 0;
      return bh - ah;
    }
    const as = sellableBySerial.get(a.serial)?.sellability_score || 0;
    const bs = sellableBySerial.get(b.serial)?.sellability_score || 0;
    return bs - as;
  });

  return filtered;
}

function renderRankingTable(result) {
  const rankings = sortedRankings(result);
  const rows = rankings
    .map((item) => {
      return `
        <tr>
          <td class="rank-cell">${item.rank}</td>
          <td class="serial-mono">${escapeHtml(item.serial)}</td>
          <td><span class="${bandClass(item.band)}">${escapeHtml(item.band)}</span></td>
          <td class="score-cell">${item.high_signal_score}</td>
          <td class="score-cell">${item.lexical_score}</td>
          <td class="score-cell">${item.pattern_score}</td>
          <td>${escapeHtml(item.lexical_hits || "none")}</td>
        </tr>
      `;
    })
    .join("");

  els.rankingTableBody.innerHTML = rows || `<tr><td colspan="7">No rankings were generated.</td></tr>`;
}

function renderSellableTable(result) {
  const rows = sortedSellable(result)
    .map((item) => {
      return `
        <tr>
          <td class="serial-mono">${escapeHtml(item.serial)}</td>
          <td class="score-cell">${item.sellability_score}</td>
          <td class="score-cell">${item.high_signal_score}</td>
          <td>${escapeHtml(item.top_pattern_name || "-")}</td>
          <td class="score-cell">${item.top_pattern_value ?? 0}</td>
          <td class="score-cell">${item.pattern_count ?? 0}</td>
        </tr>
      `;
    })
    .join("");
  els.sellableTableBody.innerHTML = rows || `<tr><td colspan="6">No sellability summary available.</td></tr>`;
}

function renderWarnings(result) {
  const datasetWarnings = result.dataset_warnings || [];
  const inputIssues = result.input_issues || [];
  const inputCorrections = result.input_corrections || [];
  const summary = result.input_summary || {};

  if (datasetWarnings.length === 0 && inputIssues.length === 0 && inputCorrections.length === 0) {
    els.datasetWarnings.hidden = true;
    els.datasetWarnings.innerHTML = "";
    return;
  }

  const issueRows = inputIssues.slice(0, 12).map((issue) => {
    const corrections =
      issue.corrections && issue.corrections.length
        ? ` | corrections: ${issue.corrections.join(", ")}`
        : "";
    return `<div>- ${escapeHtml(issue.raw)} -> ${escapeHtml(issue.normalized || "-")} (${escapeHtml(issue.reason)}${escapeHtml(corrections)})</div>`;
  });

  const extraIssueCount = Math.max(0, inputIssues.length - 12);
  const datasetRows = datasetWarnings.map((w) => `<div>- ${escapeHtml(w)}</div>`);
  const correctionRows = inputCorrections.slice(0, 12).map((item) => {
    const note =
      item.corrections && item.corrections.length
        ? ` | ${item.corrections.join(", ")}`
        : "";
    return `<div>- ${escapeHtml(item.raw)} -> ${escapeHtml(item.normalized)}${escapeHtml(note)}</div>`;
  });
  const extraCorrectionCount = Math.max(0, inputCorrections.length - 12);

  els.datasetWarnings.hidden = false;
  els.datasetWarnings.innerHTML = `
    <strong>Input normalization:</strong>
    <div>tokens=${summary.token_count ?? 0}, valid=${summary.valid_count ?? 0}, insufficient=${summary.insufficient_count ?? 0}, too_many=${summary.too_many_count ?? 0}, star_notes=${summary.star_count ?? 0}, corrected=${summary.corrected_count ?? 0}</div>
    ${correctionRows.length ? `<div style="margin-top:6px"><strong>Applied corrections:</strong>${correctionRows.join("")}${extraCorrectionCount ? `<div>- ... ${extraCorrectionCount} more</div>` : ""}</div>` : ""}
    ${issueRows.length ? `<div style="margin-top:6px"><strong>Input issues:</strong>${issueRows.join("")}${extraIssueCount ? `<div>- ... ${extraIssueCount} more</div>` : ""}</div>` : ""}
    ${datasetRows.length ? `<div style="margin-top:6px"><strong>Dataset warnings:</strong>${datasetRows.join("")}</div>` : ""}
  `;
}

function renderZipQuickReference(result) {
  const refs = result.zip_quick_reference || [];
  if (!refs.length) {
    els.zipQuickReference.hidden = true;
    els.zipQuickReference.innerHTML = "";
    return;
  }

  const rows = refs
    .slice(0, 50)
    .map((item) => {
      const count = item.matches?.length || 0;
      return `<div>- ${escapeHtml(item.zip)}: ${escapeHtml(item.city)}, ${escapeHtml(item.state)} (${count} match${count === 1 ? "" : "es"})</div>`;
    })
    .join("");
  const extra = Math.max(0, refs.length - 50);

  els.zipQuickReference.hidden = false;
  els.zipQuickReference.innerHTML = `
    <strong>ZIP Quick Reference (city/state):</strong>
    ${rows}
    ${extra ? `<div>- ... ${extra} more</div>` : ""}
  `;
}

function renderSerialCards(result) {
  const serialResults = sortedSerialResults(result);
  const selected = selectedPatternFilter();
  const filterLabel = selected && selected !== "__ALL__" ? selected : null;
  const totalSerials = result.serial_results?.length || 0;
  if (filterLabel) {
    els.patternFilterMeta.textContent = `Showing ${serialResults.length} of ${totalSerials} serials for pattern: ${filterLabel}`;
  } else {
    els.patternFilterMeta.textContent = `Showing all serials (${totalSerials}).`;
  }

  if (!serialResults.length) {
    els.serialCards.innerHTML = `<p>No serial results found.</p>`;
    return;
  }

  const cards = serialResults
    .map((serialResult, idx) => {
      const patterns = (serialResult.patterns || [])
        .map((pattern) => {
          return `
            <article class="pattern-item">
              <h3>${escapeHtml(pattern.pattern_name)} <span class="mono">value ${pattern.pattern_value_score ?? "-"} | conf ${escapeHtml(pattern.pattern_confidence ?? "-")}</span></h3>
              <p>${escapeHtml(pattern.pattern_detail || "No extra detail")}</p>
              <p class="title-line"><strong>eBay title:</strong> ${escapeHtml(pattern.ebay_title)}</p>
            </article>
          `;
        })
        .join("");

      return `
        <details class="serial-card" ${idx < 3 ? "open" : ""}>
          <summary>
            <strong class="serial-mono">${escapeHtml(serialResult.serial)}</strong>
            <span class="meta">${serialResult.pattern_count} patterns (sorted high value to low)</span>
          </summary>
          <div class="pattern-list">${patterns}</div>
        </details>
      `;
    })
    .join("");

  els.serialCards.innerHTML = cards;
}

function renderAll(result) {
  refreshPatternFilterOptions(result);
  renderMetrics(result);
  renderRankingTable(result);
  renderSellableTable(result);
  renderWarnings(result);
  renderZipQuickReference(result);
  renderSerialCards(result);
  els.resultsSection.hidden = false;
}

function chunkContinuousDigitRuns(text) {
  const raw = String(text ?? "");
  return raw.replace(/\d{9,}/g, (run) => {
    const chunks = [];
    for (let i = 0; i < run.length; i += 8) {
      chunks.push(run.slice(i, i + 8));
    }
    return chunks.join("\n");
  });
}

function buildPayload(rawInputText) {
  const normalizedInput = chunkContinuousDigitRuns(rawInputText);
  return {
    serials: [],
    raw_input_text: normalizedInput,
    denomination: (els.denominationInput.value || "$1").trim() || "$1",
    series: (els.seriesInput.value || "2021").trim() || "2021",
    include_dataset_dates: els.datasetToggle.checked,
    data_dir: els.datasetToggle.checked ? "." : null,
  };
}

function flatPatternRows(result) {
  const rows = [];
  for (const serialResult of result.serial_results || []) {
    for (const pattern of serialResult.patterns || []) {
      rows.push({
        serial: pattern.serial,
        digits: pattern.digits,
        pattern_name: pattern.pattern_name,
        pattern_confidence: pattern.pattern_confidence ?? "",
        pattern_value_score: pattern.pattern_value_score ?? "",
        pattern_detail: pattern.pattern_detail ?? "",
        ebay_title: pattern.ebay_title ?? "",
      });
    }
  }
  return rows;
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\n]/.test(text)) {
    return `"${text.replaceAll('"', '""')}"`;
  }
  return text;
}

function exportCsv(result) {
  const rows = flatPatternRows(result);
  if (!rows.length) {
    setStatus("Nothing to export yet.", "error");
    return;
  }
  const headers = Object.keys(rows[0]);
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => csvEscape(row[h])).join(","));
  }
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `fancy_serial_analysis_${Date.now()}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function exportXlsx(result) {
  if (!window.XLSX) {
    setStatus("XLSX library not loaded yet. Try again in a moment.", "error");
    return;
  }
  const patternRows = flatPatternRows(result);
  const rankingRows = sortedRankings(result).map((r) => ({
    rank: r.rank,
    serial: r.serial,
    digits: r.digits,
    high_signal_score: r.high_signal_score,
    band: r.band,
    lexical_score: r.lexical_score,
    pattern_score: r.pattern_score,
    lexical_hits: r.lexical_hits,
  }));
  const sellableRows = sortedSellable(result);
  const wb = window.XLSX.utils.book_new();
  window.XLSX.utils.book_append_sheet(wb, window.XLSX.utils.json_to_sheet(patternRows), "Pattern Analysis");
  window.XLSX.utils.book_append_sheet(wb, window.XLSX.utils.json_to_sheet(rankingRows), "Signal Ranking");
  window.XLSX.utils.book_append_sheet(wb, window.XLSX.utils.json_to_sheet(sellableRows), "Most Sellable");
  window.XLSX.writeFile(wb, `fancy_serial_analysis_${Date.now()}.xlsx`);
}

async function analyze() {
  const rawInputText = (els.serialInput.value || "").trim();
  if (!rawInputText) {
    setStatus("Paste serial input first.", "error");
    els.resultsSection.hidden = true;
    return;
  }

  setBusy(true);
  try {
    await ensurePyodide();

    if (els.datasetToggle.checked) {
      await ensureBundledDatasets();
      setStatus("Applying optional dataset overrides...", "busy");
      await writeDatasetOverridesToPyodide();
    }

    setStatus("Normalizing and analyzing input...", "busy");
    const payload = buildPayload(rawInputText);
    const responseText = state.analyzeFn(JSON.stringify(payload));
    const result = JSON.parse(responseText);
    state.lastResult = result;

    renderAll(result);
    if ((result.serial_count || 0) === 0) {
      setStatus("No valid 8-digit serials were found after normalization.", "error");
      return;
    }
    setStatus(
      `Done. Valid serials=${result.serial_count}, pattern rows=${result.pattern_row_count}.`,
      "success",
    );
  } catch (error) {
    setStatus(`Analysis failed: ${error.message}`, "error");
  } finally {
    setBusy(false);
  }
}

els.form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await analyze();
});

els.sampleBtn.addEventListener("click", () => {
  els.serialInput.value = SAMPLE_SERIALS.join("\n");
  setStatus("Sample serials loaded. Click Analyze Serials to run.", "idle");
});

els.datasetToggle.addEventListener("change", () => {
  if (els.datasetToggle.checked) {
    els.datasetSection.open = true;
    setStatus("Bundled dataset matching enabled.", "idle");
  } else {
    setStatus("Dataset matching disabled.", "idle");
  }
});

els.rankingSort.addEventListener("change", () => {
  if (state.lastResult) {
    renderRankingTable(state.lastResult);
  }
});

els.serialSort.addEventListener("change", () => {
  if (state.lastResult) {
    renderSerialCards(state.lastResult);
  }
});

els.patternFilter.addEventListener("change", () => {
  if (state.lastResult) {
    renderRankingTable(state.lastResult);
    renderSellableTable(state.lastResult);
    renderSerialCards(state.lastResult);
  }
});

els.exportCsvBtn.addEventListener("click", () => {
  if (!state.lastResult) {
    setStatus("Run an analysis first.", "error");
    return;
  }
  exportCsv(state.lastResult);
  setStatus("CSV exported.", "success");
});

els.exportXlsxBtn.addEventListener("click", () => {
  if (!state.lastResult) {
    setStatus("Run an analysis first.", "error");
    return;
  }
  exportXlsx(state.lastResult);
  setStatus("XLSX exported.", "success");
});
