const apiKeyInput = document.getElementById("apiKey");
const saveKeyButton = document.getElementById("saveKey");
const refreshButton = document.getElementById("refresh");
const statusEl = document.getElementById("status");
const publisherIdEl = document.getElementById("publisherId");
const lastUpdatedEl = document.getElementById("lastUpdated");
const lagBannerEl = document.getElementById("lagBanner");
const freshnessEl = document.getElementById("freshness");
const aggregatesEl = document.getElementById("aggregates");
const ledgerEl = document.getElementById("ledger");
const ledgerEntriesEl = document.getElementById("ledgerEntries");
const selectionsEl = document.getElementById("selections");
const deliveryEl = document.getElementById("delivery");
const dlqEl = document.getElementById("dlq");
const invoicesEl = document.getElementById("invoices");
const payoutRunsEl = document.getElementById("payoutRuns");
const payoutReconBadgeEl = document.getElementById("payoutReconBadge");
const payoutSummaryEl = document.getElementById("payoutSummary");
const payoutSparkEl = document.getElementById("payoutSpark");
const deliverySparkEl = document.getElementById("deliverySpark");
const systemStatusEl = document.getElementById("systemStatus");
const aggregateFilterEl = document.getElementById("aggregateFilter");
const pageSizeEl = document.getElementById("pageSize");
const prevPageEl = document.getElementById("prevPage");
const nextPageEl = document.getElementById("nextPage");
const exportCsvEl = document.getElementById("exportCsv");
const exportDeliveryEl = document.getElementById("exportDelivery");
const exportPayoutsEl = document.getElementById("exportPayouts");
const payoutStatusFilterEl = document.getElementById("payoutStatusFilter");
const aggregateCountEl = document.getElementById("aggregateCount");
const pageInfoEl = document.getElementById("pageInfo");

let aggregateRows = [];
let aggregatePage = 1;
let lastRefreshAt = null;
let currentReport = null;

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

const renderSparkline = (values, color = "#d46b32") => {
  const width = 120;
  const height = 32;
  const safeValues = values.length > 0 ? values : [0];
  const min = Math.min(...safeValues);
  const max = Math.max(...safeValues);
  const span = max - min || 1;
  const points = safeValues
    .map((value, index) => {
      const x = (index / Math.max(1, safeValues.length - 1)) * (width - 4) + 2;
      const y = height - 2 - ((value - min) / span) * (height - 4);
      return `${x},${y}`;
    })
    .join(" ");
  return `
    <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
      <polyline fill="none" stroke="${color}" stroke-width="2" points="${points}" />
    </svg>
  `;
};

const updateSparkline = (key, currentValue, targetEl) => {
  const previous = Number(localStorage.getItem(key));
  const series = Number.isFinite(previous) ? [previous, currentValue] : [currentValue, currentValue];
  targetEl.innerHTML = renderSparkline(series);
  localStorage.setItem(key, String(currentValue));
};

const getFilteredAggregates = () => {
  const filter = (aggregateFilterEl.value || "").trim().toLowerCase();
  return aggregateRows.filter((row) => {
    if (!filter) {
      return true;
    }
    return (
      row.campaign_id.toLowerCase().includes(filter) ||
      row.publisher_id.toLowerCase().includes(filter) ||
      row.creative_id.toLowerCase().includes(filter)
    );
  });
};

const renderAggregates = () => {
  const size = Number(pageSizeEl.value) || 10;
  const filtered = getFilteredAggregates();
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
  publisherIdEl.textContent = publisherId ? `Publisher: ${publisherId}` : "Publisher: -";
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

const renderReport = (report) => {
  currentReport = report;
  aggregateRows = report.aggregates || [];
  renderAggregates();

  const delivery = report.delivery_health || {};
  deliveryEl.innerHTML = renderRows(
    [
      { label: "Last delivered seq", value: delivery.last_delivered_seq ?? 0 },
      { label: "Last event seq", value: delivery.last_event_seq ?? 0 },
      { label: "Delivery lag", value: delivery.delivery_lag ?? 0 },
      { label: "Last attempt", value: delivery.last_attempt_at ?? "-" },
      { label: "Retry count", value: delivery.retry_count ?? 0 }
    ],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );

  if (delivery.delivery_lag && delivery.delivery_lag >= 100) {
    lagBannerEl.textContent = `Delivery lag warning: ${delivery.delivery_lag} events behind.`;
    lagBannerEl.classList.remove("hidden");
  } else {
    lagBannerEl.textContent = "";
    lagBannerEl.classList.add("hidden");
  }

  const dlq = delivery.dlq || {};
  dlqEl.innerHTML = renderRows(
    [
      { label: "DLQ count", value: dlq.count ?? 0 },
      { label: "Last failure", value: dlq.last_entry?.failed_at ?? "-" },
      { label: "Last event", value: dlq.last_entry?.event_id ?? "-" }
    ],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );

  const ledgerStats = report.ledger_stats || {};
  ledgerEl.innerHTML = renderRows(
    [
      { label: "Window payout", value: ledgerStats.window_payout_cents_estimate ?? 0 },
      { label: "Lifetime payout", value: ledgerStats.lifetime_payout_cents_estimate ?? 0 },
      { label: "Window entries", value: ledgerStats.window_entry_count ?? 0 },
      { label: "Lifetime entries", value: ledgerStats.lifetime_entry_count ?? 0 }
    ],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );

  const ledgerEntries = report.ledger_entries || [];
  ledgerEntriesEl.innerHTML = renderRows(ledgerEntries, (entry) => {
    return `
      <div class="row">
        <span>${entry.campaign_id} - ${entry.creative_id}</span>
        <span>${entry.payout_cents} cents</span>
      </div>
    `;
  });

  selectionsEl.innerHTML = renderRows(report.selection_decisions || [], (row) => {
    return `
      <div class="row">
        <span>${row.chosen_creative?.campaign_id} - ${row.chosen_creative?.creative_id}</span>
        <span>${row.metric_used}</span>
      </div>
    `;
  });

  invoicesEl.innerHTML = renderRows(report.invoice_drafts || [], (row) => {
    return `
      <div class="row">
        <span>${row.advertiser_id} - ${row.entry_count} entries</span>
        <span>${row.payout_cents} cents</span>
        <button
          class="btn ghost invoice-download"
          data-advertiser="${row.advertiser_id}"
          data-payout="${row.payout_cents}"
          data-entries="${row.entry_count}"
          data-created="${row.created_at || ""}"
        >
          Download
        </button>
      </div>
    `;
  });
  const allPayoutRuns = report.payout_runs || [];
  const payoutFilter = payoutStatusFilterEl ? payoutStatusFilterEl.value : "all";
  const payoutRows = allPayoutRuns.filter((run) => {
    if (!payoutFilter || payoutFilter === "all") {
      return true;
    }
    return (run.status || "") === payoutFilter;
  });
  const counts = allPayoutRuns.reduce(
    (acc, run) => {
      const status = run.status || "unknown";
      acc.total += 1;
      acc.byStatus[status] = (acc.byStatus[status] || 0) + 1;
      acc.totalCents += Number.isFinite(run.payout_cents) ? run.payout_cents : 0;
      return acc;
    },
    { total: 0, totalCents: 0, byStatus: {} }
  );
  const filteredSum = payoutRows.reduce(
    (sum, run) => sum + (Number.isFinite(run.payout_cents) ? run.payout_cents : 0),
    0
  );
  payoutSummaryEl.innerHTML = `
    <span>Total runs: ${counts.total}</span>
    <span>Pending: ${counts.byStatus.pending || 0}</span>
    <span>Sent: ${counts.byStatus.sent || 0}</span>
    <span>Settled: ${counts.byStatus.settled || 0}</span>
    <span>Total payout: ${counts.totalCents} cents</span>
    <span>Filtered payout: ${filteredSum} cents</span>
  `;
  payoutRunsEl.innerHTML = renderRows(payoutRows, (run) => {
    const created = run.created_at ? new Date(run.created_at).toLocaleString() : "-";
    return `
      <div class="row">
        <span>${run.publisher_id} - ${run.window_id}</span>
        <span>${run.payout_cents} cents (${run.status})</span>
        <span>${created}</span>
      </div>
    `;
  });
  const recon = report.payout_reconciliation || null;
  if (!recon) {
    payoutReconBadgeEl.textContent = "Recon: -";
    payoutReconBadgeEl.classList.remove("ok", "warn");
  } else if (recon.status === "ok") {
    payoutReconBadgeEl.textContent = `Recon: ok (${recon.runs_checked})`;
    payoutReconBadgeEl.classList.add("ok");
    payoutReconBadgeEl.classList.remove("warn");
  } else {
    const issues = (recon.mismatches || 0) + (recon.unassigned_entries || 0);
    payoutReconBadgeEl.textContent = `Recon: warn (${issues})`;
    payoutReconBadgeEl.classList.add("warn");
    payoutReconBadgeEl.classList.remove("ok");
  }
  const downloadButtons = invoicesEl.querySelectorAll(".invoice-download");
  downloadButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const advertiserId = button.getAttribute("data-advertiser") || "unknown";
      const payoutCents = Number(button.getAttribute("data-payout") || 0);
      const entryCount = Number(button.getAttribute("data-entries") || 0);
      const createdAt = button.getAttribute("data-created") || "";
      const payload = {
        advertiser_id: advertiserId,
        payout_cents: payoutCents,
        entry_count: entryCount,
        created_at: createdAt
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `invoice_${advertiserId}.json`;
      link.click();
      URL.revokeObjectURL(url);
    });
  });

  const payoutValue = Number(ledgerStats.window_payout_cents_estimate) || 0;
  const lagValue = Number(delivery.delivery_lag) || 0;
  updateSparkline("flyback_ops_payout", payoutValue, payoutSparkEl);
  updateSparkline("flyback_ops_lag", lagValue, deliverySparkEl);

  const systemRows = [
    { label: "Role", value: report.system_status?.role ?? "unknown" },
    { label: "Write enabled", value: String(report.system_status?.write_enabled ?? false) },
    { label: "Webhook enabled", value: String(report.system_status?.webhook_enabled ?? false) }
  ];
  systemStatusEl.innerHTML = renderRows(systemRows, (row) => {
    return `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`;
  });
};

const exportAggregatesCsv = () => {
  const rows = getFilteredAggregates();
  const header = "campaign_id,publisher_id,creative_id,impressions,intents,resolvedIntents,derived_value_per_1k\n";
  const body = rows
    .map((row) => {
      const fields = [
        row.campaign_id,
        row.publisher_id,
        row.creative_id,
        row.impressions,
        row.intents,
        row.resolvedIntents,
        row.derived_value_per_1k
      ];
      return fields.map((value) => `"${String(value).replace(/\"/g, "\"\"")}"`).join(",");
    })
    .join("\n");
  const blob = new Blob([header + body + (body ? "\n" : "")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "aggregates.csv";
  link.click();
  URL.revokeObjectURL(url);
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
    lastRefreshAt = Date.now();
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

exportCsvEl.addEventListener("click", () => {
  exportAggregatesCsv();
});

exportDeliveryEl.addEventListener("click", async () => {
  const headers = {};
  if (apiKeyInput.value.trim()) {
    headers["x-api-key"] = apiKeyInput.value.trim();
  }
  const response = await fetch("/v1/delivery", { headers });
  if (!response.ok) {
    return;
  }
  const payload = await response.json();
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "delivery_health.json";
  link.click();
  URL.revokeObjectURL(url);
});

payoutStatusFilterEl.addEventListener("change", () => {
  if (!currentReport) {
    return;
  }
  renderReport(currentReport);
});

exportPayoutsEl.addEventListener("click", () => {
  const filter = payoutStatusFilterEl ? payoutStatusFilterEl.value : "all";
  const rows = Array.isArray(currentReport?.payout_runs) ? currentReport.payout_runs : [];
  const filtered = rows.filter((run) => (filter === "all" ? true : (run.status || "") === filter));
  const header = "run_id,publisher_id,window_id,payout_cents,entry_count,status,created_at,updated_at\n";
  const body = filtered
    .map((row) => {
      const fields = [
        row.run_id || "",
        row.publisher_id || "",
        row.window_id || "",
        Number.isFinite(row.payout_cents) ? row.payout_cents : 0,
        Number.isFinite(row.entry_count) ? row.entry_count : 0,
        row.status || "",
        row.created_at || "",
        row.updated_at || ""
      ];
      return fields.map((value) => `"${String(value).replace(/\"/g, "\"\"")}"`).join(",");
    })
    .join("\n");
  const blob = new Blob([header + body + (body ? "\n" : "")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "payout_runs.csv";
  link.click();
  URL.revokeObjectURL(url);
});

refresh();

setInterval(() => {
  if (!lastRefreshAt) {
    return;
  }
  const seconds = Math.floor((Date.now() - lastRefreshAt) / 1000);
  freshnessEl.textContent = `Freshness: ${seconds}s`;
}, 1000);
