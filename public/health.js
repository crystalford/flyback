const apiKeyInput = document.getElementById("apiKey");
const saveKeyButton = document.getElementById("saveKey");
const refreshButton = document.getElementById("refresh");
const statusEl = document.getElementById("status");
const lastUpdatedEl = document.getElementById("lastUpdated");
const systemStatusEl = document.getElementById("systemStatus");
const deliveryStatusEl = document.getElementById("deliveryStatus");
const windowStatusEl = document.getElementById("windowStatus");
const badgeUrlEl = document.getElementById("badgeUrl");
const badgeMarkdownEl = document.getElementById("badgeMarkdown");
const copyBadgeEl = document.getElementById("copyBadge");

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
  const headers = {};
  if (apiKeyInput.value.trim()) {
    headers["x-api-key"] = apiKeyInput.value.trim();
  }
  if (!headers["x-api-key"]) {
    throw new Error("missing_api_key");
  }
  const response = await fetch("/v1/reports", { headers });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || "report_failed");
  }
  const payload = await response.json();
  return payload.reports;
};

const refresh = async () => {
  try {
    setStatus("Fetching...");
    const report = await fetchReports();
    const system = report.system_status || {};
    const delivery = report.delivery_health || {};
    const windowInfo = report.last_window_observed || {};

    systemStatusEl.innerHTML = renderRows(
      [
        { label: "Role", value: system.role ?? "-" },
        { label: "Write enabled", value: String(system.write_enabled ?? false) },
        { label: "Webhook enabled", value: String(system.webhook_enabled ?? false) },
        { label: "Webhook signing", value: String(system.webhook_signature_enabled ?? false) }
      ],
      (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
    );

    deliveryStatusEl.innerHTML = renderRows(
      [
        { label: "Last delivered seq", value: delivery.last_delivered_seq ?? 0 },
        { label: "Last event seq", value: delivery.last_event_seq ?? 0 },
        { label: "Delivery lag", value: delivery.delivery_lag ?? 0 },
        { label: "Retry count", value: delivery.retry_count ?? 0 },
        { label: "DLQ count", value: delivery.dlq?.count ?? 0 }
      ],
      (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
    );

    windowStatusEl.innerHTML = renderRows(
      [
        { label: "Window id", value: windowInfo.window_id ?? "-" },
        { label: "Impressions", value: windowInfo.impressions ?? 0 },
        { label: "Raw value / 1k", value: Number(windowInfo.raw_value_per_1k ?? 0).toFixed(2) },
        { label: "Weighted value / 1k", value: Number(windowInfo.weighted_value_per_1k ?? 0).toFixed(2) }
      ],
      (row) => `<div class="row"><span>${row.label}</span><span>${row.value}</span></div>`
    );

    lastUpdatedEl.textContent = new Date().toLocaleTimeString();
    const baseUrl = window.location.origin;
    const badgeUrl = `${baseUrl}/healthz`;
    badgeUrlEl.textContent = badgeUrl;
    badgeMarkdownEl.textContent = `![status](${badgeUrl})`;
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

copyBadgeEl.addEventListener("click", async () => {
  const text = badgeMarkdownEl.textContent || "";
  if (!text) {
    return;
  }
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand("copy");
    document.body.removeChild(textarea);
  }
});

refresh();
