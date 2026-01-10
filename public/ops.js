const apiKeyInput = document.getElementById("apiKey");
const saveKeyButton = document.getElementById("saveKey");
const refreshButton = document.getElementById("refresh");
const statusEl = document.getElementById("status");
const lastUpdatedEl = document.getElementById("lastUpdated");
const aggregatesEl = document.getElementById("aggregates");
const ledgerEl = document.getElementById("ledger");
const selectionsEl = document.getElementById("selections");

const storedKey = localStorage.getItem("flyback_api_key") || "";
apiKeyInput.value = storedKey;

const setStatus = (message) => {
  statusEl.textContent = message;
};

const renderRows = (entries, formatter) => {
  if (!entries || entries.length === 0) {
    return "<div class=\"row\"><span>—</span><span>No data</span></div>";
  }
  return entries.map(formatter).join("");
};

const fetchReports = async () => {
  setStatus("Fetching...");
  const headers = {};
  if (apiKeyInput.value.trim()) {
    headers["x-api-key"] = apiKeyInput.value.trim();
  }
  const response = await fetch("/v1/reports?include_selections=true", { headers });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || "report_failed");
  }
  const payload = await response.json();
  return payload.reports;
};

const renderReport = (report) => {
  aggregatesEl.innerHTML = renderRows(report.aggregates || [], (row) => {
    return `
      <div class="row">
        <span>${row.campaign_id} · ${row.creative_id}</span>
        <span>${row.resolvedIntents} / ${row.impressions} · ${row.derived_value_per_1k.toFixed(2)}</span>
      </div>
    `;
  });

  const ledgerStats = report.ledger_stats || {};
  ledgerEl.innerHTML = renderRows(
    [
      {
        label: "Window payout",
        value: ledgerStats.window_payout_cents ?? 0
      },
      {
        label: "Lifetime payout",
        value: ledgerStats.lifetime_payout_cents ?? 0
      },
      {
        label: "Window entries",
        value: ledgerStats.window_entries ?? 0
      },
      {
        label: "Lifetime entries",
        value: ledgerStats.lifetime_entries ?? 0
      }
    ],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );

  selectionsEl.innerHTML = renderRows(report.selection_decisions || [], (row) => {
    return `
      <div class="row">
        <span>${row.chosen_creative?.campaign_id} · ${row.chosen_creative?.creative_id}</span>
        <span>${row.metric_used}</span>
      </div>
    `;
  });
};

const refresh = async () => {
  try {
    const report = await fetchReports();
    renderReport(report);
    lastUpdatedEl.textContent = new Date().toLocaleTimeString();
    setStatus("OK");
  } catch (error) {
    setStatus(`Error: ${error.message}`);
  }
};

saveKeyButton.addEventListener("click", () => {
  localStorage.setItem("flyback_api_key", apiKeyInput.value.trim());
  refresh();
});

refreshButton.addEventListener("click", () => {
  refresh();
});

refresh();
