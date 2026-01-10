const DEFAULT_DWELL_MS = 2000;

const isNonEmptyString = (value) => typeof value === "string" && value.trim().length > 0;

const normalizeIntentPayload = (payload) => {
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid intent payload");
  }
  const {
    campaignId,
    publisherId,
    creativeId,
    intentType,
    dwellSeconds,
    interactionCount,
    parentIntentId = null
  } = payload;

  if (!isNonEmptyString(campaignId)) {
    throw new Error("Invalid campaign_id");
  }
  if (!isNonEmptyString(publisherId)) {
    throw new Error("Invalid publisher_id");
  }
  if (!isNonEmptyString(creativeId)) {
    throw new Error("Invalid creative_id");
  }
  if (!isNonEmptyString(intentType)) {
    throw new Error("Invalid intent_type");
  }
  if (parentIntentId !== null && parentIntentId !== undefined && !isNonEmptyString(parentIntentId)) {
    throw new Error("Invalid parent_intent_id");
  }

  const normalizedDwellSeconds = Number(dwellSeconds);
  if (!Number.isFinite(normalizedDwellSeconds) || normalizedDwellSeconds < 0) {
    throw new Error("Invalid dwell_seconds");
  }
  const normalizedInteractionCount = Number(interactionCount);
  if (!Number.isFinite(normalizedInteractionCount) || normalizedInteractionCount < 0) {
    throw new Error("Invalid interaction_count");
  }

  return {
    campaignId: campaignId.trim(),
    publisherId: publisherId.trim(),
    creativeId: creativeId.trim(),
    intentType: intentType.trim(),
    dwellSeconds: normalizedDwellSeconds,
    interactionCount: normalizedInteractionCount,
    parentIntentId
  };
};

const postIntent = async ({
  campaignId,
  publisherId,
  creativeId,
  intentType,
  dwellSeconds,
  interactionCount,
  parentIntentId = null
}) => {
  const normalized = normalizeIntentPayload({
    campaignId,
    publisherId,
    creativeId,
    intentType,
    dwellSeconds,
    interactionCount,
    parentIntentId
  });
  const response = await fetch("/v1/intent", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      campaign_id: normalized.campaignId,
      publisher_id: normalized.publisherId,
      creative_id: normalized.creativeId,
      intent_type: normalized.intentType,
      dwell_seconds: normalized.dwellSeconds,
      interaction_count: normalized.interactionCount,
      parent_intent_id: normalized.parentIntentId
    })
  });

  if (!response.ok) {
    throw new Error("Intent emission failed");
  }

  return response.json();
};

const resolveToken = async (tokenId, stage = "resolved", value = 10, outcomeType = "purchase") => {
  const response = await fetch(
    `/v1/postback?token_id=${encodeURIComponent(tokenId)}&value=${encodeURIComponent(value)}&stage=${encodeURIComponent(stage)}&outcome_type=${encodeURIComponent(outcomeType)}`
  );

  if (!response.ok) {
    throw new Error("Postback failed");
  }

  return response.json();
};

const formatSeconds = (ms) => Math.round(ms / 100) / 10;

const mountCru = (root, config) => {
  if (
    !config ||
    !isNonEmptyString(config.campaign_id) ||
    !isNonEmptyString(config.publisher_id) ||
    !isNonEmptyString(config.creative_id)
  ) {
    root.textContent = "Invalid creative config.";
    return;
  }

  const creativeBehavior = (() => {
    if (config.creative_id === "creative-v2" || config.creative_id === "creative-v5") {
      return { intentType: "qualified", stages: ["lead", "purchase"], values: [2, 15], outcomes: ["lead", "purchase"] };
    }
    if (config.creative_id === "creative-v3") {
      return { intentType: "affiliate_signup", stages: ["purchase"], values: [8], outcomes: ["purchase"] };
    }
    if (config.creative_id === "creative-v4") {
      return { intentType: "demo_request", stages: ["lead"], values: [4], outcomes: ["lead"] };
    }
    return { intentType: "qualified", stages: ["resolved"], values: [10], outcomes: ["signup"] };
  })();
  const status = document.createElement("div");
  status.style.marginBottom = "12px";
  status.style.fontSize = "14px";

  const headline = document.createElement("div");
  headline.textContent = "Flyback Demo Creative";
  headline.style.fontWeight = "600";
  headline.style.marginBottom = "8px";

  const description = document.createElement("div");
  description.textContent = `Engage to emit intent. Behavior: ${creativeBehavior.intentType} / ${creativeBehavior.stages.join(" → ")}.`;
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
  let lastTokenId = null;
  let emissionCount = 0;
  let resolutionTimeoutId = null;

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
        intentType: attentionQualified ? creativeBehavior.intentType : "unqualified",
        dwellSeconds,
        interactionCount,
        parentIntentId: lastTokenId
      });

      lastTokenId = token.token_id;
      emissionCount += 1;
      status.textContent = `State: INTENT_EMITTED (chain ${emissionCount})`;
      button.textContent = "Intent emitted (waiting for resolution)";

      const resolveStages = async () => {
        let lastPostback = null;
        for (let index = 0; index < creativeBehavior.stages.length; index += 1) {
          const stage = creativeBehavior.stages[index];
          const value = creativeBehavior.values[index] || 1;
          const outcomeType = creativeBehavior.outcomes[index] || "purchase";
          lastPostback = await resolveToken(token.token_id, stage, value, outcomeType);
          status.textContent = `State: ${lastPostback.token.status} (${stage})`;
        }
        return lastPostback;
      };

      resolutionTimeoutId = window.setTimeout(async () => {
        try {
          const postback = await resolveStages();
          status.textContent = `State: ${postback.token.status} (chain ${emissionCount})`;
          button.textContent = "Emit follow-up intent";
        } catch (error) {
          console.error(error);
          status.textContent = "State: ERROR";
          button.textContent = "Try again";
        } finally {
          resolutionTimeoutId = null;
          button.disabled = false;
        }
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
    if (resolutionTimeoutId) {
      window.clearTimeout(resolutionTimeoutId);
    }
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
