const apiKeyInput = document.getElementById("apiKey");
const saveKeyButton = document.getElementById("saveKey");
const refreshButton = document.getElementById("refresh");
const useDemoKeyButton = document.getElementById("useDemoKey");
const statusEl = document.getElementById("status");
const lastUpdatedEl = document.getElementById("lastUpdated");
const freshnessEl = document.getElementById("freshness");
const bannerEl = document.getElementById("banner");
const ledgerSummaryEl = document.getElementById("ledgerSummary");
const invoiceDraftEl = document.getElementById("invoiceDraft");
const deliveryHealthEl = document.getElementById("deliveryHealth");
const aggregatesEl = document.getElementById("aggregates");
const selectionsEl = document.getElementById("selections");
const ledgerEntriesEl = document.getElementById("ledgerEntries");
const payoutRunsEl = document.getElementById("payoutRuns");
const payoutSummaryEl = document.getElementById("payoutSummary");
const aggregateFilterEl = document.getElementById("aggregateFilter");
const pageSizeEl = document.getElementById("pageSize");
const prevPageEl = document.getElementById("prevPage");
const nextPageEl = document.getElementById("nextPage");
const aggregateCountEl = document.getElementById("aggregateCount");
const pageInfoEl = document.getElementById("pageInfo");
const campaignFilterEl = document.getElementById("campaignFilter");
const campaignSummaryEl = document.getElementById("campaignSummary");
const payoutSparkEl = document.getElementById("payoutSpark");
const deliverySparkEl = document.getElementById("deliverySpark");
const exportSelectionsEl = document.getElementById("exportSelections");
const exportLedgerEl = document.getElementById("exportLedger");
const exportPayoutsEl = document.getElementById("exportPayouts");
const payoutStatusFilterEl = document.getElementById("payoutStatusFilter");
const payoutSortEl = document.getElementById("payoutSort");
const payoutReconBadgeEl = document.getElementById("payoutReconBadge");
const payoutRunLinksEl = document.getElementById("payoutRunLinks");
const exportPayoutLinksEl = document.getElementById("exportPayoutLinks");
const publisherStatementEl = document.getElementById("publisherStatement");
const exportPublisherStatementEl = document.getElementById("exportPublisherStatement");
const exportPublisherStatementJsonEl = document.getElementById("exportPublisherStatementJson");

const storedKey = localStorage.getItem("flyback_advertiser_key") || "";
apiKeyInput.value = storedKey;

let aggregateRows = [];
let aggregatePage = 1;
let campaignOptions = [];
let selectionCache = [];
let reportCache = null;
let lastRefreshAt = null;

const setStatus = (message) => {
  statusEl.textContent = message;
};

const setBanner = (message) => {
  if (!message) {
    bannerEl.classList.add("hidden");
    bannerEl.textContent = "";
    return;
  }
  bannerEl.textContent = message;
  bannerEl.classList.remove("hidden");
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
  return payload.reports;
};

const fetchDelivery = async () => {
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

const renderCampaignOptions = () => {
  const current = campaignFilterEl.value;
  const options = ["all", ...campaignOptions];
  campaignFilterEl.innerHTML = options
    .map((id) => {
      const label = id === "all" ? "All campaigns" : id;
      return `<option value="${id}">${label}</option>`;
    })
    .join("");
  if (options.includes(current)) {
    campaignFilterEl.value = current;
  }
};

const renderAggregates = () => {
  const filter = (aggregateFilterEl.value || "").trim().toLowerCase();
  const size = Number(pageSizeEl.value) || 10;
  const campaignFilter = campaignFilterEl.value || "all";
  const filtered = aggregateRows.filter((row) => {
    const matchesFilter =
      !filter ||
      row.campaign_id.toLowerCase().includes(filter) ||
      row.publisher_id.toLowerCase().includes(filter) ||
      row.creative_id.toLowerCase().includes(filter);
    if (campaignFilter === "all") {
      return matchesFilter;
    }
    return matchesFilter && row.campaign_id === campaignFilter;
  });
  const totals = filtered.reduce(
    (acc, row) => {
      acc.impressions += row.impressions || 0;
      acc.resolvedIntents += row.resolvedIntents || 0;
      return acc;
    },
    { impressions: 0, resolvedIntents: 0 }
  );
  campaignSummaryEl.textContent = `Impressions: ${totals.impressions} - Resolved: ${totals.resolvedIntents}`;
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
  reportCache = report;
  aggregateRows = report.aggregates || [];
  campaignOptions = Array.from(new Set(aggregateRows.map((row) => row.campaign_id))).sort();
  renderCampaignOptions();
  renderAggregates();

  const ledger = report.ledger_stats || {};
  ledgerSummaryEl.innerHTML = renderRows(
    [
      { label: "Window payout", value: ledger.window_payout_cents_estimate ?? 0 },
      { label: "Lifetime payout", value: ledger.lifetime_payout_cents_estimate ?? 0 },
      { label: "Window entries", value: ledger.window_entry_count ?? 0 },
      { label: "Lifetime entries", value: ledger.lifetime_entry_count ?? 0 }
    ],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );

  const drafts = report.invoice_drafts || [];
  invoiceDraftEl.innerHTML = renderRows(
    drafts.map((draft) => ({
      label: `${draft.advertiser_id} - ${draft.entry_count} entries`,
      value: `${draft.payout_cents} cents`
    })),
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );

  const selections = (report.selection_decisions || [])
    .slice()
    .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
    .slice(0, 5);
  selectionCache = report.selection_decisions || [];
  selectionsEl.innerHTML = renderRows(selections, (row) => {
    const time = row.timestamp ? new Date(row.timestamp).toLocaleTimeString() : "-";
    return `
      <div class="row">
        <span>${row.chosen_creative?.campaign_id} - ${row.chosen_creative?.creative_id}</span>
        <span>${time}</span>
      </div>
    `;
  });

  updateSparkline("flyback_adv_payout", Number(ledger.window_payout_cents_estimate) || 0, payoutSparkEl);
  renderLedgerEntries(report.ledger_entries || []);
  const allPayoutRuns = report.payout_runs || [];
  const payoutFilter = payoutStatusFilterEl ? payoutStatusFilterEl.value : "all";
  const payoutRows = allPayoutRuns.filter((run) => {
    if (!payoutFilter || payoutFilter === "all") {
      return true;
    }
    return (run.status || "") === payoutFilter;
  });
  const sortMode = payoutSortEl ? payoutSortEl.value : "created_desc";
  payoutRows.sort((a, b) => {
    switch (sortMode) {
      case "created_asc":
        return String(a.created_at || "").localeCompare(String(b.created_at || ""));
      case "payout_desc":
        return (b.payout_cents || 0) - (a.payout_cents || 0);
      case "payout_asc":
        return (a.payout_cents || 0) - (b.payout_cents || 0);
      case "created_desc":
      default:
        return String(b.created_at || "").localeCompare(String(a.created_at || ""));
    }
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
  payoutRunsEl.innerHTML = payoutRows
    .map((run) => {
      const created = run.created_at ? new Date(run.created_at).toLocaleString() : "-";
      const statusClass = run.status ? String(run.status).toLowerCase() : "pending";
      const updated = run.updated_at ? ` (updated ${new Date(run.updated_at).toLocaleDateString()})` : "";
      const history = Array.isArray(run.status_history) ? run.status_history : [];
      const historyCount = history.length;
      const historyLabel = historyCount > 0 ? ` | history: ${historyCount}` : "";
      const historyDetails = historyCount
        ? history.map((entry) => `${entry.status || "unknown"}@${entry.updated_at || "-"}`).join(" | ")
        : "";
      const historyRow = historyCount
        ? `<div class="row note"><span>History: ${historyDetails}</span></div>`
        : "";
      return `
        <div class="row">
          <span>${run.publisher_id} - ${run.window_id}</span>
          <span title="${historyDetails}">${run.payout_cents} cents <span class="pill ${statusClass}">${run.status}</span>${historyLabel}</span>
          <span>${created}${updated}</span>
        </div>
        ${historyRow}
      `;
    })
    .join("");
  const links = report.payout_run_links || [];
  const advertiserIds = new Set(
    (report.invoice_drafts || []).map((draft) => draft.advertiser_id).filter((id) => id)
  );
  const filteredLinks =
    advertiserIds.size > 0 ? links.filter((link) => advertiserIds.has(link.advertiser_id)) : links;
  payoutRunLinksEl.innerHTML = renderRows(filteredLinks, (link) => {
    return `
      <div class="row">
        <span>${link.advertiser_id}</span>
        <span>${link.payout_cents} cents (${link.run_count} runs)</span>
      </div>
    `;
  });
  const statement = report.publisher_statement || null;
  publisherStatementEl.innerHTML = renderRows(
    statement
      ? [
          { label: "Ledger entries", value: statement.ledger_entries ?? 0 },
          { label: "Ledger payout", value: statement.ledger_payout_cents ?? 0 },
          { label: "Payout runs", value: statement.payout_runs ?? 0 },
          { label: "Pending payout", value: statement.payout_pending_cents ?? 0 },
          { label: "Sent payout", value: statement.payout_sent_cents ?? 0 },
          { label: "Settled payout", value: statement.payout_settled_cents ?? 0 },
          { label: "Last run", value: statement.last_run_at ?? "-" }
        ]
      : [],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );
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
};

const renderDelivery = (delivery) => {
  if (!delivery) {
    deliveryHealthEl.innerHTML = renderRows([], () => "");
    return;
  }
  deliveryHealthEl.innerHTML = renderRows(
    [
      { label: "Last delivered seq", value: delivery.last_delivered_seq ?? 0 },
      { label: "Last event seq", value: delivery.last_event_seq ?? 0 },
      { label: "Delivery lag", value: delivery.delivery_lag ?? 0 },
      { label: "Last attempt", value: delivery.last_attempt_at ?? "-" },
      { label: "Retry count", value: delivery.retry_count ?? 0 },
      { label: "DLQ count", value: delivery.dlq?.count ?? 0 }
    ],
    (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
  );
  updateSparkline("flyback_adv_lag", Number(delivery.delivery_lag) || 0, deliverySparkEl);
};

const renderLedgerEntries = (entries) => {
  const top = entries
    .filter((entry) => entry && entry.billable === true)
    .sort((a, b) => (b.payout_cents || 0) - (a.payout_cents || 0))
    .slice(0, 10);
  ledgerEntriesEl.innerHTML = renderRows(top, (entry) => {
    return `
      <div class="row">
        <span>${entry.campaign_id} - ${entry.creative_id}</span>
        <span>${entry.payout_cents} cents</span>
      </div>
    `;
  });
};

const refresh = async () => {
  try {
    setBanner("");
    const report = await fetchReports();
    const delivery = await fetchDelivery();
    renderReport(report);
    renderDelivery(delivery);
    lastUpdatedEl.textContent = new Date().toLocaleTimeString();
    lastRefreshAt = Date.now();
    setStatus("OK");
  } catch (error) {
    setStatus(`Error: ${error.message}`);
    setBanner("Advertiser key required or report unavailable.");
  }
};

saveKeyButton.addEventListener("click", () => {
  localStorage.setItem("flyback_advertiser_key", apiKeyInput.value.trim());
  refresh();
});

useDemoKeyButton.addEventListener("click", () => {
  apiKeyInput.value = "demo-advertiser-key";
  localStorage.setItem("flyback_advertiser_key", apiKeyInput.value.trim());
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

campaignFilterEl.addEventListener("change", () => {
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

exportSelectionsEl.addEventListener("click", () => {
  const rows = (selectionCache || []).slice();
  const header = "timestamp,campaign_id,creative_id,metric_used\n";
  const body = rows
    .map((row) => {
      const fields = [
        row.timestamp || "",
        row.chosen_creative?.campaign_id || "",
        row.chosen_creative?.creative_id || "",
        row.metric_used || ""
      ];
      return fields.map((value) => `"${String(value).replace(/\"/g, "\"\"")}"`).join(",");
    })
    .join("\n");
  const blob = new Blob([header + body + (body ? "\n" : "")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "selections.csv";
  link.click();
  URL.revokeObjectURL(url);
});

exportLedgerEl.addEventListener("click", () => {
  const rows = reportCache?.ledger_stats
    ? [
        {
          label: "window_payout_cents",
          value: reportCache.ledger_stats.window_payout_cents_estimate ?? 0
        },
        {
          label: "lifetime_payout_cents",
          value: reportCache.ledger_stats.lifetime_payout_cents_estimate ?? 0
        },
        {
          label: "window_entries",
          value: reportCache.ledger_stats.window_entry_count ?? 0
        },
        {
          label: "lifetime_entries",
          value: reportCache.ledger_stats.lifetime_entry_count ?? 0
        }
      ]
    : [];
  const header = "metric,value\n";
  const body = rows.map((row) => `"${row.label}","${row.value}"`).join("\n");
  const blob = new Blob([header + body + (body ? "\n" : "")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "ledger_summary.csv";
  link.click();
  URL.revokeObjectURL(url);
});

exportPayoutsEl.addEventListener("click", () => {
  const filter = payoutStatusFilterEl ? payoutStatusFilterEl.value : "all";
  const rows = Array.isArray(reportCache?.payout_runs) ? reportCache.payout_runs : [];
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

payoutStatusFilterEl.addEventListener("change", () => {
  if (!reportCache) {
    return;
  }
  renderReport(reportCache);
});

payoutSortEl.addEventListener("change", () => {
  if (!reportCache) {
    return;
  }
  renderReport(reportCache);
});

exportPayoutLinksEl.addEventListener("click", () => {
  const links = Array.isArray(reportCache?.payout_run_links) ? reportCache.payout_run_links : [];
  const advertiserIds = new Set(
    (reportCache?.invoice_drafts || []).map((draft) => draft.advertiser_id).filter((id) => id)
  );
  const filtered = advertiserIds.size > 0 ? links.filter((link) => advertiserIds.has(link.advertiser_id)) : links;
  const header = "advertiser_id,payout_cents,run_count,run_ids\n";
  const body = filtered
    .map((row) => {
      const fields = [
        row.advertiser_id || "",
        Number.isFinite(row.payout_cents) ? row.payout_cents : 0,
        Number.isFinite(row.run_count) ? row.run_count : 0,
        Array.isArray(row.run_ids) ? row.run_ids.join("|") : ""
      ];
      return fields.map((value) => `"${String(value).replace(/\"/g, "\"\"")}"`).join(",");
    })
    .join("\n");
  const blob = new Blob([header + body + (body ? "\n" : "")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "invoice_coverage.csv";
  link.click();
  URL.revokeObjectURL(url);
});

exportPublisherStatementEl.addEventListener("click", () => {
  if (!reportCache?.publisher_statement) {
    return;
  }
  const statement = reportCache.publisher_statement;
  const header =
    "publisher_id,ledger_entries,ledger_payout_cents,payout_runs,payout_pending_cents,payout_sent_cents,payout_settled_cents,last_run_at\n";
  const fields = [
    statement.publisher_id || "",
    Number.isFinite(statement.ledger_entries) ? statement.ledger_entries : 0,
    Number.isFinite(statement.ledger_payout_cents) ? statement.ledger_payout_cents : 0,
    Number.isFinite(statement.payout_runs) ? statement.payout_runs : 0,
    Number.isFinite(statement.payout_pending_cents) ? statement.payout_pending_cents : 0,
    Number.isFinite(statement.payout_sent_cents) ? statement.payout_sent_cents : 0,
    Number.isFinite(statement.payout_settled_cents) ? statement.payout_settled_cents : 0,
    statement.last_run_at || ""
  ];
  const body = fields.map((value) => `"${String(value).replace(/\"/g, "\"\"")}"`).join(",");
  const blob = new Blob([header + body + "\n"], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "publisher_statement.csv";
  link.click();
  URL.revokeObjectURL(url);
});

exportPublisherStatementJsonEl.addEventListener("click", () => {
  if (!reportCache?.publisher_statement) {
    return;
  }
  const blob = new Blob([JSON.stringify(reportCache.publisher_statement, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "publisher_statement.json";
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
