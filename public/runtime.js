const isNonEmptyString = (value) => typeof value === "string" && value.trim().length > 0;

const isValidSize = (value) => typeof value === "string" && /^\d{2,4}x\d{2,4}$/.test(value);

const normalizePublisherId = (value) => (isNonEmptyString(value) ? value.trim() : "publisher-demo");

const normalizeSize = (value) => (isValidSize(value) ? value : "300x250");

const validateConfig = (config) => {
  if (!config || typeof config !== "object") {
    return false;
  }
  if (!isNonEmptyString(config.campaign_id)) {
    return false;
  }
  if (!isNonEmptyString(config.publisher_id)) {
    return false;
  }
  if (!isNonEmptyString(config.creative_id)) {
    return false;
  }
  if (!isValidSize(config.size)) {
    return false;
  }
  return true;
};

const fetchFill = async (container) => {
  const publisherId = normalizePublisherId(container.dataset.publisherId);
  const size = normalizeSize(container.dataset.size);

  const response = await fetch("/v1/fill", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      publisher_id: publisherId,
      size
    })
  });

  if (!response.ok) {
    throw new Error("Failed to load creative");
  }

  const payload = await response.json();
  if (!validateConfig(payload.config)) {
    throw new Error("Invalid fill response");
  }
  return payload;
};

const mountCreative = (container, config, creativeUrl) => {
  const header = document.createElement("div");
  header.style.display = "flex";
  header.style.justifyContent = "space-between";
  header.style.alignItems = "center";
  header.style.marginBottom = "8px";
  header.style.fontFamily = "system-ui, sans-serif";
  header.style.fontSize = "12px";
  header.style.color = "#57606a";

  const label = document.createElement("div");
  label.textContent = `Campaign: ${config.campaign_id} | Creative: ${config.creative_id}`;

  const refresh = document.createElement("button");
  refresh.type = "button";
  refresh.textContent = "Refresh fill";
  refresh.style.fontSize = "12px";
  refresh.style.border = "1px solid #d0d7de";
  refresh.style.background = "#f6f8fa";
  refresh.style.borderRadius = "6px";
  refresh.style.padding = "4px 8px";
  refresh.style.cursor = "pointer";
  refresh.dataset.flybackRefresh = "true";

  const cruRoot = document.createElement("div");
  cruRoot.className = "flyback-cru";
  cruRoot.dataset.flybackCru = "true";
  cruRoot.dataset.config = JSON.stringify(config);
  cruRoot.style.border = "1px solid #d0d7de";
  cruRoot.style.borderRadius = "8px";
  cruRoot.style.padding = "16px";
  cruRoot.style.fontFamily = "system-ui, sans-serif";

  header.appendChild(label);
  header.appendChild(refresh);
  container.replaceChildren(header, cruRoot);
  ensureCreativeScript(creativeUrl);
};

const ensureCreativeScript = (creativeUrl) => {
  const existing = document.querySelector("script[data-flyback-creative]");
  if (existing && existing.src.includes(creativeUrl)) {
    return;
  }

  const script = document.createElement("script");
  script.src = creativeUrl;
  script.async = true;
  script.dataset.flybackCreative = "true";
  document.body.appendChild(script);
};

const init = async () => {
  const containers = document.querySelectorAll("[data-flyback-container]");

  for (const container of containers) {
    try {
      const { config, creative_url: creativeUrl } = await fetchFill(container);
      mountCreative(container, config, creativeUrl || "/creative.js");
      const refreshButton = container.querySelector("[data-flyback-refresh]");
      if (refreshButton) {
        refreshButton.onclick = async () => {
          try {
            const { config: nextConfig, creative_url: nextUrl } = await fetchFill(container);
            mountCreative(container, nextConfig, nextUrl || "/creative.js");
          } catch (error) {
            console.error("Flyback refresh failed", error);
          }
        };
      }
    } catch (error) {
      console.error("Flyback runtime failed", error);
      container.textContent = "Flyback failed to load.";
    }
  }
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
