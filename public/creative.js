const DEFAULT_DWELL_MS = 2000;

const postIntent = async ({ campaignId, publisherId, creativeId, intentType, dwellSeconds, interactionCount }) => {
  const response = await fetch("/v1/intent", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      campaign_id: campaignId,
      publisher_id: publisherId,
      creative_id: creativeId,
      intent_type: intentType,
      dwell_seconds: dwellSeconds,
      interaction_count: interactionCount
    })
  });

  if (!response.ok) {
    throw new Error("Intent emission failed");
  }

  return response.json();
};

const resolveToken = async (tokenId) => {
  const response = await fetch(`/v1/postback?token_id=${encodeURIComponent(tokenId)}&value=10`);

  if (!response.ok) {
    throw new Error("Postback failed");
  }

  return response.json();
};

const formatSeconds = (ms) => Math.round(ms / 100) / 10;

const mountCru = (root, config) => {
  const status = document.createElement("div");
  status.style.marginBottom = "12px";
  status.style.fontSize = "14px";

  const headline = document.createElement("div");
  headline.textContent = "Flyback Demo Creative";
  headline.style.fontWeight = "600";
  headline.style.marginBottom = "8px";

  const description = document.createElement("div");
  description.textContent = "Engage to emit an intent token. Resolution will occur via deferred postback.";
  description.style.fontSize = "13px";
  description.style.color = "#57606a";
  description.style.marginBottom = "12px";

  const button = document.createElement("button");
  button.textContent = "I'm interested";
  button.style.background = "#1f6feb";
  button.style.color = "#fff";
  button.style.border = "none";
  button.style.borderRadius = "6px";
  button.style.padding = "8px 12px";
  button.style.cursor = "pointer";

  const metadata = document.createElement("div");
  metadata.style.fontSize = "12px";
  metadata.style.color = "#57606a";
  metadata.style.marginTop = "12px";

  let attentionQualified = false;
  let interactionCount = 0;
  let dwellStart = Date.now();
  let tokenId = null;

  status.textContent = "State: INITIALIZED";

  const attentionTimer = window.setTimeout(() => {
    attentionQualified = true;
    status.textContent = "State: ATTENTION_QUALIFIED";
  }, DEFAULT_DWELL_MS);

  const updateMetadata = () => {
    const dwellSeconds = formatSeconds(Date.now() - dwellStart);
    metadata.textContent = `Campaign: ${config.campaign_id} · Publisher: ${config.publisher_id} · Dwell: ${dwellSeconds}s`;
  };

  updateMetadata();
  const intervalId = window.setInterval(updateMetadata, 500);

  button.addEventListener("click", async () => {
    interactionCount += 1;
    status.textContent = "State: INTENT_CAPTURED";
    button.disabled = true;
    button.textContent = "Capturing intent...";

    try {
      const dwellSeconds = formatSeconds(Date.now() - dwellStart);
      const { token } = await postIntent({
        campaignId: config.campaign_id,
        publisherId: config.publisher_id,
        creativeId: config.creative_id,
        intentType: attentionQualified ? "qualified" : "unqualified",
        dwellSeconds,
        interactionCount
      });

      tokenId = token.token_id;
      status.textContent = "State: INTENT_EMITTED (pending resolution)";
      button.textContent = "Intent emitted";

      window.setTimeout(async () => {
        const postback = await resolveToken(tokenId);
        status.textContent = `State: ${postback.token.status}`;
        button.textContent = "Resolved";
      }, 3000);
    } catch (error) {
      console.error(error);
      status.textContent = "State: ERROR";
      button.textContent = "Try again";
      button.disabled = false;
    }
  });

  root.appendChild(headline);
  root.appendChild(description);
  root.appendChild(status);
  root.appendChild(button);
  root.appendChild(metadata);

  root.addEventListener("remove", () => {
    window.clearTimeout(attentionTimer);
    window.clearInterval(intervalId);
  });
};

const initCru = () => {
  const cruRoots = document.querySelectorAll("[data-flyback-cru]");

  cruRoots.forEach((root) => {
    if (root.dataset.mounted) {
      return;
    }

    root.dataset.mounted = "true";
    const config = JSON.parse(root.dataset.config || "{}");
    mountCru(root, config);
  });
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initCru);
} else {
  initCru();
}
