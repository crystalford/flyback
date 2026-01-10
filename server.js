import fs from "fs";
import http from "http";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const port = Number(process.env.PORT) || 3000;
const host = process.env.HOST || "0.0.0.0";
const publicDir = path.join(__dirname, "public");
const tokensFile = path.join(__dirname, "data", "tokens.json");

const contentTypes = {
  ".html": "text/html",
  ".js": "application/javascript",
  ".css": "text/css",
  ".json": "application/json"
};

// Phase boundaries:
// V1: token lifecycle, demo endpoints, and in-memory aggregates.
// V1.5: formal publisher/campaign/creative roles with selection scaffolding.
// V2 (deferred): auth, pricing/payouts, and persistent registries/config.
// V1.5 contract stabilization: explicit runtime ↔ server expectations.
// V2 candidate: formal schema/versioning enforcement.

// V1.5 scaffold: static registries for system roles (in-memory only, no persistence).
// V1.5 publisher control scaffolding; V2 candidate for persistence and UI management.
const publisherPolicies = {
  "publisher-demo": {
    allowed_demand_types: ["direct", "performance", "affiliate"],
    derived_value_floor: 0,
    demand_priority: ["direct", "performance", "affiliate"]
  },
  "publisher-labs": {
    allowed_demand_types: ["performance", "affiliate"],
    derived_value_floor: 0.5,
    demand_priority: ["performance", "affiliate"]
  }
};

const publisherRegistry = [
  { publisher_id: "publisher-demo", campaign_ids: ["campaign-v1", "campaign-v2"] },
  { publisher_id: "publisher-labs", campaign_ids: ["campaign-labs-1"] }
];

const campaignRegistry = [
  { campaign_id: "campaign-v1", publisher_id: "publisher-demo", creative_ids: ["creative-v1", "creative-v2"] },
  { campaign_id: "campaign-v2", publisher_id: "publisher-demo", creative_ids: ["creative-v3", "creative-v4"] },
  { campaign_id: "campaign-v3", publisher_id: "publisher-demo", creative_ids: ["creative-v5"] },
  { campaign_id: "campaign-labs-1", publisher_id: "publisher-labs", creative_ids: ["creative-v2", "creative-v4"] }
];

const creativeRegistry = [
  { creative_id: "creative-v1", creative_url: "/creative.js", sizes: ["300x250"], demand_type: "direct" },
  { creative_id: "creative-v2", creative_url: "/creative.js", sizes: ["300x250", "320x50"], demand_type: "performance" },
  { creative_id: "creative-v3", creative_url: "/creative.js", sizes: ["300x250"], demand_type: "affiliate" },
  { creative_id: "creative-v4", creative_url: "/creative.js", sizes: ["300x250"], demand_type: "direct" },
  { creative_id: "creative-v5", creative_url: "/creative.js", sizes: ["300x250"], demand_type: "performance" }
];

// Aggregations are in-memory only; tokens remain the source of truth across restarts.
const aggregations = {
  impressions: new Map(),
  intents: new Map(),
  resolvedIntents: new Map(),
  resolvedValueSum: new Map(),
  partialResolutions: new Map()
};

const AGGREGATION_WINDOW_MS = 10 * 60 * 1000;
const BUDGET_DEPRIORITIZE_THRESHOLD = 0.2;
let aggregationWindow = {
  started_at_ms: Date.now(),
  started_at: new Date().toISOString()
};

console.log("aggregate.window.start", {
  window_start: aggregationWindow.started_at,
  window_ms: AGGREGATION_WINDOW_MS
});

// V1.5 budget scaffolding (in-memory only, reset on restart).
const campaignBudgets = new Map(
  [
    { campaign_id: "campaign-v1", total: 120 },
    { campaign_id: "campaign-v2", total: 80 },
    { campaign_id: "campaign-v3", total: 60 },
    { campaign_id: "campaign-labs-1", total: 40 }
  ].map((entry) => [entry.campaign_id, { ...entry, remaining: entry.total }])
);

const getCampaignBudget = (campaignId) => campaignBudgets.get(campaignId);

const getBudgetStatus = (campaignId) => {
  const budget = getCampaignBudget(campaignId);
  if (!budget) {
    return { total: 0, remaining: 0, ratio: 0, exhausted: true, near_exhaustion: true };
  }
  const ratio = budget.total > 0 ? budget.remaining / budget.total : 0;
  return {
    total: budget.total,
    remaining: budget.remaining,
    ratio,
    exhausted: budget.remaining <= 0,
    near_exhaustion: ratio <= BUDGET_DEPRIORITIZE_THRESHOLD
  };
};

const applyBudgetCharge = (campaignId, amount) => {
  const budget = getCampaignBudget(campaignId);
  if (!budget) {
    console.log("invariant.violation", { reason: "campaign_budget_missing", campaign_id: campaignId });
    return;
  }
  const charge = Number.isFinite(amount) ? amount : 0;
  budget.remaining = Math.max(0, budget.remaining - charge);
  console.log("budget.update", {
    campaign_id: campaignId,
    total: budget.total,
    remaining: budget.remaining,
    charge
  });
};

const resetAggregationWindow = (reason) => {
  aggregations.impressions.clear();
  aggregations.intents.clear();
  aggregations.resolvedIntents.clear();
  aggregations.resolvedValueSum.clear();
  aggregations.partialResolutions.clear();
  const previousStart = aggregationWindow.started_at;
  aggregationWindow = {
    started_at_ms: Date.now(),
    started_at: new Date().toISOString()
  };
  console.log("aggregate.window.reset", {
    reason,
    previous_start: previousStart,
    window_start: aggregationWindow.started_at,
    window_ms: AGGREGATION_WINDOW_MS
  });
};

const ensureWindowFresh = () => {
  if (Date.now() - aggregationWindow.started_at_ms >= AGGREGATION_WINDOW_MS) {
    resetAggregationWindow("elapsed");
  }
};

const FINAL_RESOLUTION_STAGES = new Set(["resolved", "purchase", "final"]);

const isFinalResolutionStage = (stage) => FINAL_RESOLUTION_STAGES.has(stage);

const aggregateKey = (scope) => `${scope.campaign_id}:${scope.publisher_id}:${scope.creative_id}`;

const bumpAggregate = (aggregateMap, scope, label) => {
  ensureWindowFresh();
  const key = aggregateKey(scope);
  const current = aggregateMap.get(key) || {
    campaign_id: scope.campaign_id,
    publisher_id: scope.publisher_id,
    creative_id: scope.creative_id,
    count: 0
  };
  current.count += 1;
  aggregateMap.set(key, current);
  console.log("aggregate.update", { type: label, ...current });
};

const getAggregateCount = (aggregateMap, scope) => {
  const key = aggregateKey(scope);
  const current = aggregateMap.get(key);
  return current ? current.count : 0;
};

const bumpResolvedValueSum = (scope, value) => {
  ensureWindowFresh();
  const key = aggregateKey(scope);
  const current = aggregations.resolvedValueSum.get(key) || {
    campaign_id: scope.campaign_id,
    publisher_id: scope.publisher_id,
    creative_id: scope.creative_id,
    sum: 0
  };
  const numericValue = Number.isFinite(value) ? value : 0;
  current.sum += numericValue;
  aggregations.resolvedValueSum.set(key, current);
  return current.sum;
};

const getDerivedMetrics = (scope) => {
  ensureWindowFresh();
  const key = aggregateKey(scope);
  const impressions = getAggregateCount(aggregations.impressions, scope);
  const intents = getAggregateCount(aggregations.intents, scope);
  const resolvedIntents = getAggregateCount(aggregations.resolvedIntents, scope);
  const resolvedValueSum = aggregations.resolvedValueSum.get(key)?.sum || 0;
  const intentRate = impressions > 0 ? intents / impressions : 0;
  // Final resolutions only; partial stages are tracked separately for internal analysis.
  const resolutionRate = intents > 0 ? resolvedIntents / intents : 0;
  const derivedValuePer1k = impressions > 0 ? (resolvedValueSum / impressions) * 1000 : 0;

  return {
    impressions,
    intents,
    resolvedIntents,
    intent_rate: intentRate,
    resolution_rate: resolutionRate,
    derived_value_per_1k: derivedValuePer1k,
    partial_resolutions: getAggregateCount(aggregations.partialResolutions, scope)
  };
};

const isNonEmptyString = (value) => typeof value === "string" && value.trim().length > 0;

const isValidSize = (value) => typeof value === "string" && /^\d{2,4}x\d{2,4}$/.test(value);

const isValidStage = (value) => typeof value === "string" && /^[a-z0-9_-]+$/.test(value);

const rejectRequest = (res, endpoint, reason, details = {}) => {
  console.log("contract.reject", { endpoint, reason, ...details });
  sendJson(res, 400, { error: reason });
};

const findPublisher = (publisherId) =>
  publisherRegistry.find((publisher) => publisher.publisher_id === publisherId);

const resolvePublisherPolicy = (publisherId) =>
  publisherPolicies[publisherId] || {
    allowed_demand_types: ["performance", "direct", "affiliate"],
    derived_value_floor: 0,
    demand_priority: ["performance", "direct", "affiliate"]
  };

const resolveCampaignsForPublisher = (publisher) =>
  campaignRegistry.filter((campaign) => campaign.publisher_id === publisher.publisher_id);

const resolveCreativesForCampaign = (campaign) =>
  creativeRegistry.filter((creative) => campaign.creative_ids.includes(creative.creative_id));

const resolveCampaign = (campaignId) =>
  campaignRegistry.find((campaign) => campaign.campaign_id === campaignId);

const resolveCreative = (creativeId) =>
  creativeRegistry.find((creative) => creative.creative_id === creativeId);

const selectCreative = (candidates, publisherPolicy) => {
  // V1.5 publisher control scaffolding; V2 candidate for persistence and UI management.
  // Deterministic, side-effect free selection based on in-memory aggregates.
  // If no candidates meet the data floor, fall back to stable ordering by priority, campaign, creative.
  ensureWindowFresh();
  const allowedTypes = new Set(publisherPolicy.allowed_demand_types || []);
  const priorityOrder = publisherPolicy.demand_priority || [];
  const priorityIndex = (demandType) => {
    const idx = priorityOrder.indexOf(demandType);
    return idx === -1 ? Number.MAX_SAFE_INTEGER : idx;
  };

  console.log("selection.candidates", {
    count: candidates.length,
    candidates: candidates.map((candidate) => ({
      campaign_id: candidate.scope.campaign_id,
      creative_id: candidate.creative_id,
      demand_type: candidate.demand_type
    }))
  });
  console.log("selection.window", {
    window_start: aggregationWindow.started_at,
    window_ms: AGGREGATION_WINDOW_MS
  });

  const budgetFiltered = candidates.filter((candidate) => {
    const budgetStatus = getBudgetStatus(candidate.scope.campaign_id);
    if (budgetStatus.exhausted) {
      console.log("selection.excluded", {
        reason: "budget_exhausted",
        campaign_id: candidate.scope.campaign_id,
        creative_id: candidate.creative_id
      });
      return false;
    }
    return true;
  });

  const budgetAllowed = new Set(
    budgetFiltered.map((candidate) => `${candidate.scope.campaign_id}:${candidate.creative_id}`)
  );
  const eligible = candidates
    .filter((candidate) => budgetAllowed.has(`${candidate.scope.campaign_id}:${candidate.creative_id}`))
    .filter((candidate) => allowedTypes.has(candidate.demand_type))
    .map((candidate) => ({ candidate, metrics: getDerivedMetrics(candidate.scope) }))
    .filter((entry) => entry.metrics.derived_value_per_1k >= (publisherPolicy.derived_value_floor || 0))
    .sort((a, b) => {
      const aPriority = priorityIndex(a.candidate.demand_type);
      const bPriority = priorityIndex(b.candidate.demand_type);
      if (aPriority !== bPriority) {
        return aPriority - bPriority;
      }
      const aBudget = getBudgetStatus(a.candidate.scope.campaign_id);
      const bBudget = getBudgetStatus(b.candidate.scope.campaign_id);
      if (aBudget.near_exhaustion !== bBudget.near_exhaustion) {
        return aBudget.near_exhaustion ? 1 : -1;
      }
      if (b.metrics.derived_value_per_1k !== a.metrics.derived_value_per_1k) {
        return b.metrics.derived_value_per_1k - a.metrics.derived_value_per_1k;
      }
      if (a.candidate.scope.campaign_id !== b.candidate.scope.campaign_id) {
        return a.candidate.scope.campaign_id.localeCompare(b.candidate.scope.campaign_id);
      }
      return a.candidate.creative_id.localeCompare(b.candidate.creative_id);
    });

  console.log("selection.policy", {
    allowed_demand_types: Array.from(allowedTypes),
    derived_value_floor: publisherPolicy.derived_value_floor || 0,
    demand_priority: priorityOrder
  });
  console.log("selection.eligible", {
    count: eligible.length,
    ordered: eligible.map((entry) => ({
      campaign_id: entry.candidate.scope.campaign_id,
      creative_id: entry.candidate.creative_id,
      demand_type: entry.candidate.demand_type,
      derived_value_per_1k: entry.metrics.derived_value_per_1k,
      budget_remaining: getBudgetStatus(entry.candidate.scope.campaign_id).remaining,
      budget_total: getBudgetStatus(entry.candidate.scope.campaign_id).total,
      near_exhaustion: getBudgetStatus(entry.candidate.scope.campaign_id).near_exhaustion
    }))
  });

  if (eligible.length === 0) {
    const fallback = candidates
      .filter((candidate) => !getBudgetStatus(candidate.scope.campaign_id).exhausted)
      .filter((candidate) => allowedTypes.has(candidate.demand_type))
      .sort((a, b) => {
        const aPriority = priorityIndex(a.demand_type);
        const bPriority = priorityIndex(b.demand_type);
        if (aPriority !== bPriority) {
          return aPriority - bPriority;
        }
        const aBudget = getBudgetStatus(a.scope.campaign_id);
        const bBudget = getBudgetStatus(b.scope.campaign_id);
        if (aBudget.near_exhaustion !== bBudget.near_exhaustion) {
          return aBudget.near_exhaustion ? 1 : -1;
        }
        if (a.scope.campaign_id !== b.scope.campaign_id) {
          return a.scope.campaign_id.localeCompare(b.scope.campaign_id);
        }
        return a.creative_id.localeCompare(b.creative_id);
      });
    const selected = fallback[0] || candidates[0];
    console.log("selection.final", {
      reason: "fallback",
      campaign_id: selected.scope.campaign_id,
      creative_id: selected.creative_id,
      demand_type: selected.demand_type,
      budget_remaining: getBudgetStatus(selected.scope.campaign_id).remaining
    });
    return selected;
  }

  const selected = eligible[0].candidate;
  console.log("selection.final", {
    reason: "eligible",
    campaign_id: selected.scope.campaign_id,
    creative_id: selected.creative_id,
    demand_type: selected.demand_type,
    budget_remaining: getBudgetStatus(selected.scope.campaign_id).remaining
  });
  return selected;
};

const buildReport = () => {
  const keys = new Set();
  [aggregations.impressions, aggregations.intents, aggregations.resolvedIntents, aggregations.resolvedValueSum].forEach(
    (map) => {
      for (const key of map.keys()) {
        keys.add(key);
      }
    }
  );

  const rows = [];
  for (const key of keys) {
    const [campaignId, publisherId, creativeId] = key.split(":");
    const scope = { campaign_id: campaignId, publisher_id: publisherId, creative_id: creativeId };
    const metrics = getDerivedMetrics(scope);

    rows.push({
      campaign_id: campaignId,
      publisher_id: publisherId,
      creative_id: creativeId,
      impressions: metrics.impressions,
      intents: metrics.intents,
      resolvedIntents: metrics.resolvedIntents,
      intent_rate: metrics.intent_rate,
      resolution_rate: metrics.resolution_rate,
      derived_value_per_1k: metrics.derived_value_per_1k
    });
  }

  return rows;
};

const loadTokens = () => {
  if (!fs.existsSync(tokensFile)) {
    return [];
  }

  const raw = fs.readFileSync(tokensFile, "utf8");
  if (!raw.trim()) {
    return [];
  }

  try {
    return JSON.parse(raw);
  } catch (error) {
    console.error("tokens.load.failed", { error: error.message });
    return [];
  }
};

const saveTokens = (tokens) => {
  fs.writeFileSync(tokensFile, `${JSON.stringify(tokens, null, 2)}\n`);
};

const logLifecycle = (event, token, extra = {}) => {
  console.log(event, {
    token_id: token.token_id,
    status: token.status,
    ...extra
  });
};

const applyDefaults = (token) => {
  const normalized = { ...token };

  if (!normalized.status) {
    normalized.status = "PENDING";
  }

  if (normalized.status === "PENDING" && !normalized.pending_at) {
    normalized.pending_at = normalized.created_at || new Date().toISOString();
  }

  if (!normalized.created_at) {
    normalized.created_at = new Date().toISOString();
  }

  if (!normalized.expires_at) {
    const createdAt = new Date(normalized.created_at);
    const expiresAt = new Date(createdAt);
    expiresAt.setDate(expiresAt.getDate() + 30);
    normalized.expires_at = expiresAt.toISOString();
  }

  if (!normalized.signature) {
    normalized.signature = "ed25519-placeholder";
  }

  if (!normalized.binding) {
    normalized.binding = { type: "none", value: null };
  }

  if (!normalized.context) {
    normalized.context = { intent_type: "unknown", dwell_seconds: 0, interaction_count: 0 };
  }

  if (!normalized.scope) {
    normalized.scope = { campaign_id: "campaign-v1", publisher_id: "publisher-demo", creative_id: "creative-v1" };
  }

  if (!Object.prototype.hasOwnProperty.call(normalized, "parent_intent_id")) {
    normalized.parent_intent_id = null;
  }

  if (!Array.isArray(normalized.resolution_events)) {
    normalized.resolution_events = [];
  }

  return normalized;
};

const enforceExpiry = (token) => {
  const now = new Date();
  if (token.status !== "RESOLVED" && new Date(token.expires_at) < now) {
    return { ...token, status: "EXPIRED" };
  }
  return token;
};

const normalizeToken = (token) => enforceExpiry(applyDefaults(token));

const normalizeTokens = (storedTokens) => storedTokens.map(normalizeToken);

let tokens = normalizeTokens(loadTokens());
if (tokens.length > 0) {
  saveTokens(tokens);
  console.log("tokens.load.normalized", { count: tokens.length });
}

const findToken = (tokenId) => tokens.find((token) => token.token_id === tokenId);

const baseTokenPayload = ({
  campaignId,
  publisherId,
  creativeId,
  intentType,
  dwellSeconds,
  interactionCount,
  parentIntentId = null
}) => {
  const createdAt = new Date();
  const expiresAt = new Date(createdAt);
  expiresAt.setDate(expiresAt.getDate() + 30);

  return {
    token_id: randomUUID(),
    version: "1.0",
    created_at: createdAt.toISOString(),
    expires_at: expiresAt.toISOString(),
    scope: {
      campaign_id: campaignId,
      publisher_id: publisherId,
      creative_id: creativeId
    },
    context: {
      intent_type: intentType,
      dwell_seconds: dwellSeconds,
      interaction_count: interactionCount
    },
    binding: {
      type: "none",
      value: null
    },
    signature: "ed25519-placeholder",
    status: "CREATED",
    pending_at: null,
    resolved_at: null,
    resolved_value: null,
    parent_intent_id: parentIntentId,
    resolution_events: []
  };
};

const parseResolvedValue = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 1;
  }
  return numeric;
};

const sendJson = (res, status, payload) => {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(body)
  });
  res.end(body);
};

const readJsonBody = (req) =>
  new Promise((resolve, reject) => {
    let data = "";

    req.on("data", (chunk) => {
      data += chunk;
    });

    req.on("end", () => {
      if (!data.trim()) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(data));
      } catch (error) {
        reject(error);
      }
    });

    req.on("error", reject);
  });

// Runtime → server contract (V1.5 stabilization):
// - POST /v1/fill: required { publisher_id }, optional { size }.
// - POST /v1/intent: required { campaign_id, publisher_id, creative_id, intent_type }, optional { dwell_seconds, interaction_count, parent_intent_id }.
// - GET /v1/postback: required { token_id }, optional { value, stage }.
const handleFill = async (req, res) => {
  let body;
  try {
    body = await readJsonBody(req);
  } catch (error) {
    sendJson(res, 400, { error: "invalid_json" });
    return;
  }
  const { publisher_id: publisherId, size } = body;
  if (!isNonEmptyString(publisherId)) {
    rejectRequest(res, "/v1/fill", "invalid_publisher_id");
    return;
  }
  if (size !== undefined && !isValidSize(size)) {
    rejectRequest(res, "/v1/fill", "invalid_size");
    return;
  }

  const requestedSize = size || "300x250";
  const publisher = findPublisher(publisherId);
  if (!publisher) {
    rejectRequest(res, "/v1/fill", "publisher_unknown", { publisher_id: publisherId });
    return;
  }
  const publisherPolicy = resolvePublisherPolicy(publisher.publisher_id);
  const campaigns = resolveCampaignsForPublisher(publisher);
  if (campaigns.length === 0) {
    console.log("invariant.violation", { reason: "publisher_without_campaigns", publisher_id: publisher.publisher_id });
    sendJson(res, 500, { error: "publisher_campaigns_missing" });
    return;
  }
  const candidates = campaigns.flatMap((campaign) =>
    resolveCreativesForCampaign(campaign)
      .filter((creative) => creative.sizes.includes(requestedSize))
      .filter((creative) => {
        if (!isNonEmptyString(creative.demand_type)) {
          console.log("invariant.violation", {
            reason: "creative_missing_demand_type",
            creative_id: creative.creative_id
          });
          return false;
        }
        if (!isNonEmptyString(creative.creative_url)) {
          console.log("invariant.violation", {
            reason: "creative_missing_url",
            creative_id: creative.creative_id
          });
          return false;
        }
        return true;
      })
      .map((creative) => ({
        scope: {
          campaign_id: campaign.campaign_id,
          publisher_id: publisher.publisher_id,
          creative_id: creative.creative_id
        },
        creative_id: creative.creative_id,
        creative_url: creative.creative_url,
        demand_type: creative.demand_type
      }))
  );
  const fallbackCampaign = campaigns[0] || { campaign_id: "campaign-v1" };
  const fallbackCreative = creativeRegistry[0];
  if (!fallbackCreative || !isNonEmptyString(fallbackCreative.demand_type)) {
    console.log("invariant.violation", { reason: "fallback_creative_invalid" });
    sendJson(res, 500, { error: "creative_registry_invalid" });
    return;
  }
  const selectedCreative = candidates.length > 0 ? selectCreative(candidates, publisherPolicy) : {
    scope: {
      campaign_id: fallbackCampaign.campaign_id,
      publisher_id: publisher.publisher_id,
      creative_id: fallbackCreative.creative_id
    },
    creative_id: fallbackCreative.creative_id,
    creative_url: fallbackCreative.creative_url,
    demand_type: fallbackCreative.demand_type
  };

  bumpAggregate(aggregations.impressions, {
    campaign_id: selectedCreative.scope.campaign_id,
    publisher_id: selectedCreative.scope.publisher_id,
    creative_id: selectedCreative.creative_id
  }, "impressions");

  sendJson(res, 200, {
    creative_url: selectedCreative.creative_url,
    config: {
      campaign_id: selectedCreative.scope.campaign_id,
      publisher_id: selectedCreative.scope.publisher_id,
      creative_id: selectedCreative.creative_id,
      size: requestedSize
    }
  });
};

const handleIntent = async (req, res) => {
  let body;
  try {
    body = await readJsonBody(req);
  } catch (error) {
    sendJson(res, 400, { error: "invalid_json" });
    return;
  }
  const {
    campaign_id: campaignId,
    publisher_id: publisherId,
    creative_id: creativeId,
    intent_type: intentType,
    dwell_seconds: dwellSeconds,
    interaction_count: interactionCount,
    parent_intent_id: parentIntentId = null
  } = body;

  if (!isNonEmptyString(campaignId)) {
    rejectRequest(res, "/v1/intent", "invalid_campaign_id");
    return;
  }
  if (!isNonEmptyString(publisherId)) {
    rejectRequest(res, "/v1/intent", "invalid_publisher_id");
    return;
  }
  if (!isNonEmptyString(creativeId)) {
    rejectRequest(res, "/v1/intent", "invalid_creative_id");
    return;
  }
  if (!isNonEmptyString(intentType)) {
    rejectRequest(res, "/v1/intent", "invalid_intent_type");
    return;
  }
  if (parentIntentId !== null && parentIntentId !== undefined && !isNonEmptyString(parentIntentId)) {
    rejectRequest(res, "/v1/intent", "invalid_parent_intent_id");
    return;
  }

  const normalizedDwellSeconds = dwellSeconds === undefined ? 0 : Number(dwellSeconds);
  if (!Number.isFinite(normalizedDwellSeconds) || normalizedDwellSeconds < 0) {
    rejectRequest(res, "/v1/intent", "invalid_dwell_seconds");
    return;
  }
  const normalizedInteractionCount = interactionCount === undefined ? 1 : Number(interactionCount);
  if (!Number.isFinite(normalizedInteractionCount) || normalizedInteractionCount < 0) {
    rejectRequest(res, "/v1/intent", "invalid_interaction_count");
    return;
  }

  const publisher = findPublisher(publisherId);
  if (!publisher) {
    rejectRequest(res, "/v1/intent", "publisher_unknown", { publisher_id: publisherId });
    return;
  }
  const campaign = resolveCampaign(campaignId);
  if (!campaign) {
    rejectRequest(res, "/v1/intent", "campaign_unknown", { campaign_id: campaignId });
    return;
  }
  if (campaign.publisher_id !== publisherId) {
    rejectRequest(res, "/v1/intent", "campaign_publisher_mismatch", {
      campaign_id: campaignId,
      publisher_id: publisherId
    });
    return;
  }
  const creative = resolveCreative(creativeId);
  if (!creative) {
    rejectRequest(res, "/v1/intent", "creative_unknown", { creative_id: creativeId });
    return;
  }
  if (!campaign.creative_ids.includes(creativeId)) {
    rejectRequest(res, "/v1/intent", "creative_campaign_mismatch", {
      campaign_id: campaignId,
      creative_id: creativeId
    });
    return;
  }
  if (!isNonEmptyString(creative.demand_type)) {
    rejectRequest(res, "/v1/intent", "creative_missing_demand_type", { creative_id: creativeId });
    return;
  }

  const token = baseTokenPayload({
    campaignId,
    publisherId,
    creativeId,
    intentType,
    dwellSeconds: normalizedDwellSeconds,
    interactionCount: normalizedInteractionCount,
    parentIntentId
  });
  logLifecycle("intent.created", token, {
    campaign_id: token.scope.campaign_id,
    publisher_id: token.scope.publisher_id,
    creative_id: token.scope.creative_id
  });
  token.status = "PENDING";
  token.pending_at = new Date().toISOString();

  tokens = [...tokens, token];
  saveTokens(tokens);

  logLifecycle("intent.pending", token);
  bumpAggregate(aggregations.intents, token.scope, "intents");

  sendJson(res, 200, { token });
};

const handlePostback = (url, res) => {
  const tokenId = url.searchParams.get("token_id");
  const value = url.searchParams.get("value");
  const stage = (url.searchParams.get("stage") || "resolved").toLowerCase();

  if (!isNonEmptyString(tokenId)) {
    rejectRequest(res, "/v1/postback", "invalid_token_id");
    return;
  }
  if (!isValidStage(stage)) {
    rejectRequest(res, "/v1/postback", "invalid_stage");
    return;
  }

  const token = findToken(tokenId);
  if (!token) {
    sendJson(res, 404, { error: "token not found" });
    return;
  }
  if (
    !token.scope ||
    !isNonEmptyString(token.scope.campaign_id) ||
    !isNonEmptyString(token.scope.publisher_id) ||
    !isNonEmptyString(token.scope.creative_id)
  ) {
    console.log("invariant.violation", { reason: "token_scope_invalid", token_id: tokenId });
    sendJson(res, 500, { error: "token_scope_invalid" });
    return;
  }

  const hasStageEvent = token.resolution_events?.some((event) => event.stage === stage);
  if (
    hasStageEvent ||
    (token.status === "RESOLVED" && (!token.resolution_events || token.resolution_events.length === 0) && stage === "resolved")
  ) {
    logLifecycle("postback.idempotent", token, { stage });
    sendJson(res, 200, { token, status: "already_resolved" });
    return;
  }

  if (token.status === "EXPIRED") {
    logLifecycle("postback.idempotent", token, { state: "expired" });
    sendJson(res, 410, { token, status: "already_expired" });
    return;
  }

  const now = new Date();
  if (new Date(token.expires_at) < now) {
    token.status = "EXPIRED";
    saveTokens(tokens);
    logLifecycle("postback.expired", token);
    sendJson(res, 410, { token, status: "expired" });
    return;
  }

  const resolvedValue = parseResolvedValue(value);
  if (!Array.isArray(token.resolution_events)) {
    token.resolution_events = [];
  }
  token.resolution_events.push({
    stage,
    resolved_at: now.toISOString(),
    resolved_value: resolvedValue
  });

  const isFinal = isFinalResolutionStage(stage);
  if (isFinal) {
    token.status = "RESOLVED";
    if (!token.resolved_at) {
      token.resolved_at = now.toISOString();
    }
    if (token.resolved_value === null || token.resolved_value === undefined) {
      token.resolved_value = resolvedValue;
    }
  }
  saveTokens(tokens);

  if (isFinal) {
    logLifecycle("postback.resolved", token, { stage, value: resolvedValue });
    applyBudgetCharge(token.scope.campaign_id, resolvedValue);
    bumpAggregate(aggregations.resolvedIntents, token.scope, "resolved_intents");
    const resolvedValueSum = bumpResolvedValueSum(token.scope, resolvedValue);
    const metrics = getDerivedMetrics(token.scope);
    const derivedValuePer1k = metrics.impressions > 0 ? (resolvedValueSum / metrics.impressions) * 1000 : 0;
    console.log("aggregate.metrics", {
      campaign_id: token.scope.campaign_id,
      publisher_id: token.scope.publisher_id,
      creative_id: token.scope.creative_id,
      impressions: metrics.impressions,
      intents: metrics.intents,
      resolvedIntents: metrics.resolvedIntents,
      intent_rate: metrics.intent_rate,
      resolution_rate: metrics.resolution_rate,
      derived_value_per_1k: derivedValuePer1k
    });
    sendJson(res, 200, { token, status: "resolved" });
    return;
  }

  logLifecycle("postback.partial", token, { stage, value: resolvedValue });
  bumpAggregate(aggregations.partialResolutions, token.scope, "partial_resolutions");
  sendJson(res, 200, { token, status: "partial" });
};

const serveStatic = (res, filePath) => {
  if (!fs.existsSync(filePath)) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not found");
    return;
  }

  const ext = path.extname(filePath);
  const contentType = contentTypes[ext] || "application/octet-stream";
  const data = fs.readFileSync(filePath);

  res.writeHead(200, {
    "Content-Type": contentType,
    "Content-Length": data.length
  });
  res.end(data);
};

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === "POST" && url.pathname === "/v1/fill") {
      await handleFill(req, res);
      return;
    }

    if (req.method === "POST" && url.pathname === "/v1/intent") {
      await handleIntent(req, res);
      return;
    }

    if (req.method === "GET" && url.pathname === "/v1/postback") {
      handlePostback(url, res);
      return;
    }

    if (req.method === "GET" && url.pathname === "/v1/reports") {
      sendJson(res, 200, { reports: buildReport() });
      return;
    }

    if (req.method === "GET") {
      const requestedPath = url.pathname === "/" ? "/index.html" : url.pathname;
      const filePath = path.join(publicDir, requestedPath);
      serveStatic(res, filePath);
      return;
    }

    res.writeHead(405, { "Content-Type": "text/plain" });
    res.end("Method not allowed");
  } catch (error) {
    console.error("server.error", { message: error.message });
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "server_error" }));
  }
});

server.listen(port, host, () => {
  console.log(`Flyback server listening on http://${host}:${port}`);
});
