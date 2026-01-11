const apiKeyInput = document.getElementById("apiKey");
const saveKeyButton = document.getElementById("saveKey");
const refreshButton = document.getElementById("refresh");
const statusEl = document.getElementById("status");
const publisherIdEl = document.getElementById("publisherId");
const lastUpdatedEl = document.getElementById("lastUpdated");
const aggregatesEl = document.getElementById("aggregates");
const ledgerEl = document.getElementById("ledger");
const selectionsEl = document.getElementById("selections");
const deliveryEl = document.getElementById("delivery");
const dlqEl = document.getElementById("dlq");
const aggregateFilterEl = document.getElementById("aggregateFilter");
const pageSizeEl = document.getElementById("pageSize");
const prevPageEl = document.getElementById("prevPage");
const nextPageEl = document.getElementById("nextPage");
const aggregateCountEl = document.getElementById("aggregateCount");
const pageInfoEl = document.getElementById("pageInfo");

let aggregateRows = [];
let aggregatePage = 1;

const storedKey = localStorage.getItem("flyback_api_key") || "";
apiKeyInput.value = storedKey;

const setStatus = (message) => {
  statusEl.textContent = message;
};

const renderRows = (entries, formatter) => {
  if (!entries || entries.length === 0) {
    return "<div class=\"row\"><span>-</span><span>No data</span></div>";
  }
  return entries.map(formatter).join("");
};

const fetchReports = async () => {
  setStatus("Fetching...");
  const headers = {};
  if (apiKeyInput.value.trim()) {
    headers["x-api-key"] = apiKeyInput.value.trim();
  }
  if (!headers["x-api-key"]) {
    setStatus("Missing API key");
    throw new Error("missing_api_key");
  }
  const response = await fetch("/v1/reports?include_selections=true", { headers });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || "report_failed");
  }
  const payload = await response.json();
  const publisherId = payload.reports?.aggregates?.[0]?.publisher_id || null;
  if (publisherId) {
    publisherIdEl.textContent = `Publisher: ${publisherId}`;
  } else {
    publisherIdEl.textContent = "Publisher: —";
  }
  return payload.reports;
};

const fetchDeliveryHealth = async () => {
  const headers = {};
  if (apiKeyInput.value.trim()) {
    headers["x-api-key"] = apiKeyInput.value.trim();
  }
  const response = await fetch("/v1/delivery", { headers });
  if (!response.ok) {
    return null;
  }
  const payload = await response.json();
  return payload.delivery_health || null;
};

const renderAggregates = () => {
  const filter = (aggregateFilterEl.value || "").trim().toLowerCase();
  const size = Number(pageSizeEl.value) || 10;
  const filtered = aggregateRows.filter((row) => {
    if (!filter) {
      return true;
    }
    return (
      row.campaign_id.toLowerCase().includes(filter) ||
      row.publisher_id.toLowerCase().includes(filter) ||
      row.creative_id.toLowerCase().includes(filter)
    );
  });
  const totalPages = Math.max(1, Math.ceil(filtered.length / size));
  if (aggregatePage > totalPages) {
    aggregatePage = totalPages;
  }
  const start = (aggregatePage - 1) * size;
  const pageRows = filtered.slice(start, start + size);
  aggregateCountEl.textContent = `${filtered.length} results`;
  pageInfoEl.textContent = `Page ${aggregatePage} / ${totalPages}`;
  aggregatesEl.innerHTML = renderRows(pageRows, (row) => {
    return `
      <div class="row">
        <span>${row.campaign_id} - ${row.creative_id}</span>
        <span>${row.resolvedIntents} / ${row.impressions} - ${row.derived_value_per_1k.toFixed(2)}</span>
      </div>
    `;
  });
};

const renderReport = (report) => {
  aggregateRows = report.aggregates || [];
  renderAggregates();

  const delivery = report.delivery_health || {};
  deliveryEl.innerHTML = renderRows(
    [
      {
        label: "Last delivered seq",
        value: delivery.last_delivered_seq ?? 0
      },
      {
        label: "Last event seq",
        value: delivery.last_event_seq ?? 0
      },
      {
        label: "Delivery lag",
        value: delivery.delivery_lag ?? 0
      },
      {
        label: "Last attempt",
        value: delivery.last_attempt_at ?? "-"
      },
      {
        label: "Retry count",
        value: delivery.retry_count ?? 0
      }
    ],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );

  const dlq = delivery.dlq || {};
  dlqEl.innerHTML = renderRows(
    [
      {
        label: "DLQ count",
        value: dlq.count ?? 0
      },
      {
        label: "Last failure",
        value: dlq.last_entry?.failed_at ?? "-"
      },
      {
        label: "Last event",
        value: dlq.last_entry?.event_id ?? "-"
      }
    ],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );

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
        <span>${row.chosen_creative?.campaign_id} - ${row.chosen_creative?.creative_id}</span>
        <span>${row.metric_used}</span>
      </div>
    `;
  });
};

const refresh = async () => {
  try {
    const report = await fetchReports();
    const delivery = await fetchDeliveryHealth();
    if (delivery) {
      report.delivery_health = delivery;
    }
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

aggregateFilterEl.addEventListener("input", () => {
  aggregatePage = 1;
  renderAggregates();
});

pageSizeEl.addEventListener("change", () => {
  aggregatePage = 1;
  renderAggregates();
});

prevPageEl.addEventListener("click", () => {
  aggregatePage = Math.max(1, aggregatePage - 1);
  renderAggregates();
});

nextPageEl.addEventListener("click", () => {
  aggregatePage += 1;
  renderAggregates();
});

refresh();
