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
const registryFile = path.join(__dirname, "data", "registry.json");
const budgetsFile = path.join(__dirname, "data", "budgets.json");
const aggregatesFile = path.join(__dirname, "data", "aggregates.json");
const keysFile = path.join(__dirname, "data", "keys.json");

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

// V2 source of truth: file-backed registries and policies (loaded into memory).
let publisherPolicies = {};
let publisherRegistry = [];
let campaignRegistry = [];
let creativeRegistry = [];
let advertiserRegistry = [];
let publisherKeyIndex = new Map();
let advertiserKeyIndex = new Map();
let defaultPublisherKey = null;
let defaultAdvertiserKey = null;

// Aggregations are persisted per window; tokens remain the source of truth.
let aggregations = {
  impressions: new Map(),
  intents: new Map(),
  resolvedIntents: new Map(),
  resolvedValueSum: new Map(),
  partialResolutions: new Map(),
  weightedResolvedValueSum: new Map(),
  billableResolutions: new Map(),
  nonBillableResolutions: new Map()
};
let lastWindowSnapshot = null;

const AGGREGATION_WINDOW_MS = 10 * 60 * 1000;
const BUDGET_DEPRIORITIZE_THRESHOLD = 0.2;
const RECONCILIATION_TOLERANCE = 0.001;
const SELECTION_HISTORY_LIMIT = 1000;
const GUARDRAIL_DIVERGENCE_PCT = 0.3;
const GUARDRAIL_WINDOW_THRESHOLD = 2;
const CAP_DEPRIORITIZE_THRESHOLD = 0.8;
let aggregationWindow = {
  started_at_ms: Date.now(),
  started_at: new Date().toISOString()
};

let campaignBudgets = new Map();

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
  saveBudgets();
  console.log("budget.update", {
    campaign_id: campaignId,
    total: budget.total,
    remaining: budget.remaining,
    charge
  });
};

const resetAggregationWindow = (reason) => {
  const lastWindowSnapshot = {
    window: {
      started_at: aggregationWindow.started_at,
      started_at_ms: aggregationWindow.started_at_ms,
      window_ms: AGGREGATION_WINDOW_MS
    },
    aggregates: {
      impressions: mapToEntries(aggregations.impressions),
      intents: mapToEntries(aggregations.intents),
      resolved_intents: mapToEntries(aggregations.resolvedIntents),
      resolved_value_sum: mapToEntries(aggregations.resolvedValueSum),
      partial_resolutions: mapToEntries(aggregations.partialResolutions),
      weighted_resolved_value_sum: mapToEntries(aggregations.weightedResolvedValueSum),
      billable_resolutions: mapToEntries(aggregations.billableResolutions),
      non_billable_resolutions: mapToEntries(aggregations.nonBillableResolutions)
    }
  };
  aggregations.impressions.clear();
  aggregations.intents.clear();
  aggregations.resolvedIntents.clear();
  aggregations.resolvedValueSum.clear();
  aggregations.partialResolutions.clear();
  aggregations.weightedResolvedValueSum.clear();
  aggregations.billableResolutions.clear();
  aggregations.nonBillableResolutions.clear();
  const previousStart = aggregationWindow.started_at;
  aggregationWindow = {
    started_at_ms: Date.now(),
    started_at: new Date().toISOString()
  };
  saveAggregates(lastWindowSnapshot);
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

const resolutionEventKey = (tokenId, stage) => `${tokenId}:${stage}`;

const selectionHistory = [];
const guardrailState = new Map();

const recordSelectionDecision = (decision) => {
  selectionHistory.push(decision);
  if (selectionHistory.length > SELECTION_HISTORY_LIMIT) {
    selectionHistory.shift();
  }
};

const getSelectionHistory = (publisherId, limit = 50) => {
  const filtered = selectionHistory.filter((entry) => entry.publisher_id === publisherId);
  if (filtered.length <= limit) {
    return filtered;
  }
  return filtered.slice(filtered.length - limit);
};

const recordGuardrailDivergence = (publisherId, windowId, divergent) => {
  const state = guardrailState.get(publisherId) || {
    last_window_id: null,
    consecutive_divergent: 0
  };
  if (state.last_window_id !== windowId) {
    state.last_window_id = windowId;
    state.consecutive_divergent = divergent ? state.consecutive_divergent + 1 : 0;
  }
  guardrailState.set(publisherId, state);
  if (state.consecutive_divergent >= GUARDRAIL_WINDOW_THRESHOLD) {
    console.log("selection.guardrail.warning", {
      publisher_id: publisherId,
      window_id: windowId,
      consecutive_windows: state.consecutive_divergent,
      divergence_pct_threshold: GUARDRAIL_DIVERGENCE_PCT
    });
  }
};

const addResolutionEvent = (token, stage, resolvedAt, resolvedValue, outcomeType = null) => {
  if (!Array.isArray(token.resolution_events)) {
    token.resolution_events = [];
  }
  token.resolution_events.push({
    stage,
    resolved_at: resolvedAt,
    resolved_value: resolvedValue,
    outcome_type: outcomeType
  });
};

const getFinalResolutionEvent = (token) => {
  if (Array.isArray(token.resolution_events) && token.resolution_events.length > 0) {
    const finals = token.resolution_events.filter((event) => isFinalResolutionStage(event.stage));
    if (finals.length > 0) {
      return finals.reduce((earliest, current) => {
        if (!earliest) {
          return current;
        }
        return new Date(current.resolved_at) < new Date(earliest.resolved_at) ? current : earliest;
      }, null);
    }
  }
  if (token.status === "RESOLVED" && token.resolved_at) {
    return {
      stage: "resolved",
      resolved_at: token.resolved_at,
      resolved_value: token.resolved_value,
      outcome_type: token.outcome_type || "resolved"
    };
  }
  return null;
};

const readJsonFile = (filePath) => {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const raw = fs.readFileSync(filePath, "utf8");
  if (!raw.trim()) {
    return null;
  }
  try {
    return JSON.parse(raw);
  } catch (error) {
    console.error("file.read.failed", { path: filePath, error: error.message });
    return null;
  }
};

const writeJsonFile = (filePath, payload) => {
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`);
};

const validateRegistry = (registry) => {
  const errors = [];
  if (!registry || typeof registry !== "object") {
    errors.push("registry_invalid");
    return errors;
  }
  if (typeof registry.version !== "number") {
    errors.push("version_missing");
  }
  if (!Array.isArray(registry.advertisers) || registry.advertisers.length === 0) {
    errors.push("advertisers_missing");
  }
  if (!Array.isArray(registry.publishers) || registry.publishers.length === 0) {
    errors.push("publishers_missing");
  }
  if (!Array.isArray(registry.campaigns) || registry.campaigns.length === 0) {
    errors.push("campaigns_missing");
  }
  if (!Array.isArray(registry.creatives) || registry.creatives.length === 0) {
    errors.push("creatives_missing");
  }
  if (!registry.policies || typeof registry.policies !== "object") {
    errors.push("policies_missing");
  }
  if (registry.policies && typeof registry.policies === "object") {
    Object.entries(registry.policies).forEach(([publisherId, policy]) => {
      if (!policy || typeof policy !== "object") {
        errors.push(`policy_invalid:${publisherId}`);
        return;
      }
      if (policy.floor_type && !["raw", "weighted"].includes(policy.floor_type)) {
        errors.push(`policy_floor_type_invalid:${publisherId}`);
      }
      if (policy.floor_value_per_1k !== undefined && !Number.isFinite(policy.floor_value_per_1k)) {
        errors.push(`policy_floor_value_invalid:${publisherId}`);
      }
    });
  }
  return errors;
};

const validateKeys = (keysPayload) => {
  const errors = [];
  if (!keysPayload || typeof keysPayload !== "object") {
    errors.push("keys_invalid");
    return errors;
  }
  if (typeof keysPayload.version !== "number") {
    errors.push("version_missing");
  }
  if (!Array.isArray(keysPayload.publishers)) {
    errors.push("publisher_keys_missing");
  }
  if (!Array.isArray(keysPayload.advertisers)) {
    errors.push("advertiser_keys_missing");
  }
  return errors;
};

const migrateByVersion = (payload, fromVersion, toVersion, migrations, label) => {
  if (fromVersion > toVersion) {
    console.error("migrate.version.unsupported", { label, from: fromVersion, to: toVersion });
    process.exit(1);
  }
  let current = payload;
  for (let version = fromVersion; version < toVersion; version += 1) {
    const migrate = migrations[version];
    if (!migrate) {
      console.error("migrate.missing", { label, from: version, to: version + 1 });
      process.exit(1);
    }
    current = migrate(current);
    current.version = version + 1;
  }
  return current;
};

const REGISTRY_VERSION = 1;
const BUDGETS_VERSION = 1;
const AGGREGATES_VERSION = 1;
const KEYS_VERSION = 1;

const registryMigrations = {
  0: (payload) => payload
};

const budgetsMigrations = {
  0: (payload) => payload
};

const aggregatesMigrations = {
  0: (payload) => payload
};

const keysMigrations = {
  0: (payload) => payload
};

const loadRegistry = () => {
  const registry = readJsonFile(registryFile);
  if (!registry) {
    console.error("registry.load.failed", { path: registryFile });
    process.exit(1);
  }
  const version = typeof registry.version === "number" ? registry.version : 0;
  const migrated = migrateByVersion(registry, version, REGISTRY_VERSION, registryMigrations, "registry");
  const errors = validateRegistry(migrated);
  if (errors.length > 0) {
    console.error("registry.validate.failed", { errors });
    process.exit(1);
  }
  const advertiserIds = new Set(migrated.advertisers.map((entry) => entry.advertiser_id));
  const campaignIds = new Set(migrated.campaigns.map((entry) => entry.campaign_id));
  const creativeIds = new Set(migrated.creatives.map((entry) => entry.creative_id));
  migrated.campaigns.forEach((campaign) => {
    if (!isNonEmptyString(campaign.advertiser_id) || !advertiserIds.has(campaign.advertiser_id)) {
      console.error("registry.validate.failed", {
        errors: ["campaign_advertiser_invalid"],
        campaign_id: campaign.campaign_id
      });
      process.exit(1);
    }
    if (campaign.outcome_weights && typeof campaign.outcome_weights !== "object") {
      console.error("registry.validate.failed", {
        errors: ["campaign_outcome_weights_invalid"],
        campaign_id: campaign.campaign_id
      });
      process.exit(1);
    }
    if (campaign.caps && typeof campaign.caps !== "object") {
      console.error("registry.validate.failed", {
        errors: ["campaign_caps_invalid"],
        campaign_id: campaign.campaign_id
      });
      process.exit(1);
    }
    if (campaign.caps) {
      if (campaign.caps.max_outcomes !== undefined && !Number.isFinite(campaign.caps.max_outcomes)) {
        console.error("registry.validate.failed", {
          errors: ["campaign_caps_max_outcomes_invalid"],
          campaign_id: campaign.campaign_id
        });
        process.exit(1);
      }
      if (campaign.caps.max_weighted_value !== undefined && !Number.isFinite(campaign.caps.max_weighted_value)) {
        console.error("registry.validate.failed", {
          errors: ["campaign_caps_max_weighted_invalid"],
          campaign_id: campaign.campaign_id
        });
        process.exit(1);
      }
    }
    if (!Array.isArray(campaign.creative_ids) || campaign.creative_ids.length === 0) {
      console.error("registry.validate.failed", {
        errors: ["campaign_creatives_missing"],
        campaign_id: campaign.campaign_id
      });
      process.exit(1);
    }
    campaign.creative_ids.forEach((creativeId) => {
      if (!creativeIds.has(creativeId)) {
        console.error("registry.validate.failed", {
          errors: ["campaign_creative_missing"],
          campaign_id: campaign.campaign_id,
          creative_id: creativeId
        });
        process.exit(1);
      }
    });
  });
  migrated.publishers.forEach((publisher) => {
    if (!Array.isArray(publisher.campaign_ids) || publisher.campaign_ids.length === 0) {
      console.error("registry.validate.failed", {
        errors: ["publisher_campaigns_missing"],
        publisher_id: publisher.publisher_id
      });
      process.exit(1);
    }
    publisher.campaign_ids.forEach((campaignId) => {
      if (!campaignIds.has(campaignId)) {
        console.error("registry.validate.failed", {
          errors: ["publisher_campaign_missing"],
          publisher_id: publisher.publisher_id,
          campaign_id: campaignId
        });
        process.exit(1);
      }
    });
  });
  publisherRegistry = migrated.publishers;
  advertiserRegistry = migrated.advertisers;
  campaignRegistry = migrated.campaigns;
  creativeRegistry = migrated.creatives;
  publisherPolicies = migrated.policies;
  console.log("registry.load.ok", {
    publishers: publisherRegistry.length,
    advertisers: advertiserRegistry.length,
    campaigns: campaignRegistry.length,
    creatives: creativeRegistry.length
  });
};

const mapFromEntries = (entries, keySelector) => {
  const map = new Map();
  if (!Array.isArray(entries)) {
    return map;
  }
  entries.forEach((entry) => {
    map.set(keySelector(entry), entry);
  });
  return map;
};

const mapToEntries = (map) => Array.from(map.values());

const loadBudgets = () => {
  const payload = readJsonFile(budgetsFile);
  if (!payload || !Array.isArray(payload.campaigns)) {
    console.error("budgets.load.failed", { path: budgetsFile });
    process.exit(1);
  }
  const version = typeof payload.version === "number" ? payload.version : 0;
  const migrated = migrateByVersion(payload, version, BUDGETS_VERSION, budgetsMigrations, "budgets");
  if (!Array.isArray(migrated.campaigns)) {
    console.error("budgets.load.failed", { path: budgetsFile, reason: "campaigns_missing" });
    process.exit(1);
  }
  campaignBudgets = mapFromEntries(migrated.campaigns, (entry) => entry.campaign_id);
  console.log("budgets.load.ok", { campaigns: campaignBudgets.size });
};

const saveBudgets = () => {
  writeJsonFile(budgetsFile, { version: BUDGETS_VERSION, campaigns: mapToEntries(campaignBudgets) });
};

const emptyAggregateWindow = () => ({
  impressions: [],
  intents: [],
  resolved_intents: [],
  resolved_value_sum: [],
  partial_resolutions: [],
  weighted_resolved_value_sum: [],
  billable_resolutions: [],
  non_billable_resolutions: []
});

const loadAggregates = () => {
  const payload = readJsonFile(aggregatesFile);
  if (!payload) {
    saveAggregates();
    return;
  }
  const version = typeof payload.version === "number" ? payload.version : 0;
  const migrated = migrateByVersion(payload, version, AGGREGATES_VERSION, aggregatesMigrations, "aggregates");
  aggregationWindow = migrated.window || aggregationWindow;
  lastWindowSnapshot = migrated.last_window || null;
  const current = migrated.current || emptyAggregateWindow();
  aggregations = {
    impressions: mapFromEntries(current.impressions, aggregateKey),
    intents: mapFromEntries(current.intents, aggregateKey),
    resolvedIntents: mapFromEntries(current.resolved_intents, aggregateKey),
    resolvedValueSum: mapFromEntries(current.resolved_value_sum, aggregateKey),
    partialResolutions: mapFromEntries(current.partial_resolutions, aggregateKey),
    weightedResolvedValueSum: mapFromEntries(current.weighted_resolved_value_sum, aggregateKey),
    billableResolutions: mapFromEntries(current.billable_resolutions, aggregateKey),
    nonBillableResolutions: mapFromEntries(current.non_billable_resolutions, aggregateKey)
  };
  console.log("aggregates.load.ok", {
    window_start: aggregationWindow.started_at,
    impressions: aggregations.impressions.size
  });
};

const saveAggregates = (lastWindowSnapshotOverride = null) => {
  const payload = {
    version: AGGREGATES_VERSION,
    window: {
      started_at: aggregationWindow.started_at,
      started_at_ms: aggregationWindow.started_at_ms,
      window_ms: AGGREGATION_WINDOW_MS
    },
    current: {
      impressions: mapToEntries(aggregations.impressions),
      intents: mapToEntries(aggregations.intents),
      resolved_intents: mapToEntries(aggregations.resolvedIntents),
      resolved_value_sum: mapToEntries(aggregations.resolvedValueSum),
      partial_resolutions: mapToEntries(aggregations.partialResolutions),
      weighted_resolved_value_sum: mapToEntries(aggregations.weightedResolvedValueSum),
      billable_resolutions: mapToEntries(aggregations.billableResolutions),
      non_billable_resolutions: mapToEntries(aggregations.nonBillableResolutions)
    },
    last_window: lastWindowSnapshotOverride || lastWindowSnapshot
  };
  if (lastWindowSnapshotOverride) {
    lastWindowSnapshot = lastWindowSnapshotOverride;
  }
  writeJsonFile(aggregatesFile, payload);
};

const loadKeys = () => {
  const payload = readJsonFile(keysFile);
  if (!payload) {
    console.error("keys.load.failed", { path: keysFile });
    process.exit(1);
  }
  const version = typeof payload.version === "number" ? payload.version : 0;
  const migrated = migrateByVersion(payload, version, KEYS_VERSION, keysMigrations, "keys");
  const errors = validateKeys(migrated);
  if (errors.length > 0) {
    console.error("keys.validate.failed", { errors });
    process.exit(1);
  }
  publisherKeyIndex = new Map(
    migrated.publishers.map((entry) => [entry.api_key, entry.publisher_id])
  );
  advertiserKeyIndex = new Map(
    migrated.advertisers.map((entry) => [entry.api_key, entry.advertiser_id])
  );
  defaultPublisherKey = migrated.default_demo_publisher_key || null;
  defaultAdvertiserKey = migrated.default_demo_advertiser_key || null;
  console.log("keys.load.ok", {
    publishers: publisherKeyIndex.size,
    advertisers: advertiserKeyIndex.size
  });
};

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
  saveAggregates();
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
  saveAggregates();
  return current.sum;
};

const bumpWeightedResolvedValueSum = (scope, value) => {
  ensureWindowFresh();
  const key = aggregateKey(scope);
  const current = aggregations.weightedResolvedValueSum.get(key) || {
    campaign_id: scope.campaign_id,
    publisher_id: scope.publisher_id,
    creative_id: scope.creative_id,
    sum: 0
  };
  const numericValue = Number.isFinite(value) ? value : 0;
  current.sum += numericValue;
  aggregations.weightedResolvedValueSum.set(key, current);
  saveAggregates();
  return current.sum;
};

const bumpResolutionCount = (aggregateMap, scope) => {
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
  saveAggregates();
  return current.count;
};

const getDerivedMetrics = (scope) => {
  ensureWindowFresh();
  const key = aggregateKey(scope);
  const impressions = getAggregateCount(aggregations.impressions, scope);
  const intents = getAggregateCount(aggregations.intents, scope);
  const resolvedIntents = getAggregateCount(aggregations.resolvedIntents, scope);
  const resolvedValueSum = aggregations.resolvedValueSum.get(key)?.sum || 0;
  const weightedEntry = aggregations.weightedResolvedValueSum.get(key);
  const weightedResolvedValueSum = weightedEntry?.sum || 0;
  const weightedPresent = Boolean(weightedEntry);
  const intentRate = impressions > 0 ? intents / impressions : 0;
  // Final resolutions only; partial stages are tracked separately for internal analysis.
  const resolutionRate = intents > 0 ? resolvedIntents / intents : 0;
  const derivedValuePer1k = impressions > 0 ? (resolvedValueSum / impressions) * 1000 : 0;
  const weightedDerivedValuePer1k = impressions > 0 ? (weightedResolvedValueSum / impressions) * 1000 : 0;

  return {
    impressions,
    intents,
    resolvedIntents,
    intent_rate: intentRate,
    resolution_rate: resolutionRate,
    derived_value_per_1k: derivedValuePer1k,
    weighted_derived_value_per_1k: weightedDerivedValuePer1k,
    weighted_resolved_value_sum: weightedResolvedValueSum,
    weighted_present: weightedPresent,
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

const logAuth = (event, actorType, actorId, reason) => {
  console.log(event, {
    actor_type: actorType,
    actor_id: actorId,
    reason
  });
};

const extractApiKey = (req) => {
  const raw = req.headers["x-api-key"] || req.headers["authorization"];
  if (!raw) {
    return null;
  }
  if (raw.startsWith("Bearer ")) {
    return raw.slice(7);
  }
  return raw;
};

const authorizePublisher = (req, res, publisherId) => {
  const providedKey = extractApiKey(req);
  const apiKey = providedKey || defaultPublisherKey;
  if (!apiKey) {
    logAuth("auth.reject", "publisher", publisherId, "missing_key");
    sendJson(res, 401, { error: "auth_required" });
    return null;
  }
  const actorId = publisherKeyIndex.get(apiKey);
  if (!actorId) {
    logAuth("auth.reject", "publisher", publisherId, "invalid_key");
    sendJson(res, 401, { error: "auth_invalid" });
    return null;
  }
  if (publisherId && actorId !== publisherId) {
    logAuth("auth.reject", "publisher", actorId, "publisher_mismatch");
    sendJson(res, 403, { error: "auth_mismatch" });
    return null;
  }
  logAuth("auth.accept", "publisher", actorId, providedKey ? "api_key" : "default_key");
  return actorId;
};

const authorizeAdvertiser = (req, res, campaignId = null) => {
  const providedKey = extractApiKey(req);
  const apiKey = providedKey || defaultAdvertiserKey;
  if (!apiKey) {
    logAuth("auth.reject", "advertiser", campaignId, "missing_key");
    sendJson(res, 401, { error: "auth_required" });
    return null;
  }
  const actorId = advertiserKeyIndex.get(apiKey);
  if (!actorId) {
    logAuth("auth.reject", "advertiser", campaignId, "invalid_key");
    sendJson(res, 401, { error: "auth_invalid" });
    return null;
  }
  logAuth("auth.accept", "advertiser", actorId, providedKey ? "api_key" : "default_key");
  return actorId;
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

const resolveAdvertiser = (advertiserId) =>
  advertiserRegistry.find((advertiser) => advertiser.advertiser_id === advertiserId);

const resolveOutcomeWeight = (campaignId, outcomeType) => {
  const campaign = resolveCampaign(campaignId);
  if (!campaign || !campaign.outcome_weights || typeof campaign.outcome_weights !== "object") {
    return 1;
  }
  const weight = campaign.outcome_weights[outcomeType];
  return Number.isFinite(weight) ? weight : 1;
};

const getCampaignCaps = (campaignId) => {
  const campaign = resolveCampaign(campaignId);
  if (!campaign || !campaign.caps || typeof campaign.caps !== "object") {
    return { max_outcomes: null, max_weighted_value: null };
  }
  return {
    max_outcomes: Number.isFinite(campaign.caps.max_outcomes) ? campaign.caps.max_outcomes : null,
    max_weighted_value: Number.isFinite(campaign.caps.max_weighted_value) ? campaign.caps.max_weighted_value : null
  };
};

const capState = new Map();

const getCapStatus = (campaignId) => {
  const caps = getCampaignCaps(campaignId);
  const state = capState.get(campaignId) || { billable_count: 0, billable_weighted_value: 0 };
  const countRatio = caps.max_outcomes ? state.billable_count / caps.max_outcomes : 0;
  const valueRatio = caps.max_weighted_value ? state.billable_weighted_value / caps.max_weighted_value : 0;
  const ratio = Math.max(countRatio, valueRatio);
  const exhausted =
    (caps.max_outcomes !== null && state.billable_count >= caps.max_outcomes) ||
    (caps.max_weighted_value !== null && state.billable_weighted_value >= caps.max_weighted_value);
  const near = ratio >= CAP_DEPRIORITIZE_THRESHOLD && !exhausted;
  return {
    caps,
    state,
    exhausted,
    near_exhaustion: near,
    ratio
  };
};

const applyCapUsage = (campaignId, weightedValue) => {
  const status = getCapStatus(campaignId);
  const state = status.state;
  state.billable_count += 1;
  state.billable_weighted_value += weightedValue;
  capState.set(campaignId, state);
  return state;
};

const getPublisherFloorConfig = (publisherId) => {
  const policy = publisherPolicies[publisherId] || {};
  const floorType = policy.floor_type === "weighted" ? "weighted" : "raw";
  const fallbackFloorValue = Number.isFinite(policy.derived_value_floor) ? policy.derived_value_floor : 0;
  const floorValuePer1k = Number.isFinite(policy.floor_value_per_1k)
    ? policy.floor_value_per_1k
    : fallbackFloorValue;
  return {
    selection_mode: policy.selection_mode || "raw",
    floor_type: floorType,
    floor_value_per_1k: floorValuePer1k
  };
};

const getLastWindowObserved = (publisherId) => {
  if (!lastWindowSnapshot || !lastWindowSnapshot.aggregates) {
    return null;
  }
  const aggregates = lastWindowSnapshot.aggregates;
  const impressions = (aggregates.impressions || [])
    .filter((entry) => entry.publisher_id === publisherId)
    .reduce((sum, entry) => sum + (Number.isFinite(entry.count) ? entry.count : 0), 0);
  const rawSum = (aggregates.resolved_value_sum || [])
    .filter((entry) => entry.publisher_id === publisherId)
    .reduce((sum, entry) => sum + (Number.isFinite(entry.sum) ? entry.sum : 0), 0);
  const weightedSum = (aggregates.weighted_resolved_value_sum || [])
    .filter((entry) => entry.publisher_id === publisherId)
    .reduce((sum, entry) => sum + (Number.isFinite(entry.sum) ? entry.sum : 0), 0);
  const rawPer1k = impressions > 0 ? (rawSum / impressions) * 1000 : 0;
  const weightedPer1k = impressions > 0 ? (weightedSum / impressions) * 1000 : 0;
  return {
    window_id: lastWindowSnapshot.window?.started_at || null,
    impressions,
    raw_value_per_1k: rawPer1k,
    weighted_value_per_1k: weightedPer1k
  };
};

const getPublisherCapConfig = (publisherId) => {
  return campaignRegistry
    .filter((campaign) => campaign.publisher_id === publisherId)
    .map((campaign) => ({
      campaign_id: campaign.campaign_id,
      advertiser_id: campaign.advertiser_id || null,
      caps: campaign.caps || null
    }));
};

const getLastWindowBillableCounts = (publisherId) => {
  if (!lastWindowSnapshot || !lastWindowSnapshot.aggregates) {
    return null;
  }
  const aggregates = lastWindowSnapshot.aggregates;
  const billable = (aggregates.billable_resolutions || [])
    .filter((entry) => entry.publisher_id === publisherId)
    .reduce((sum, entry) => sum + (Number.isFinite(entry.count) ? entry.count : 0), 0);
  const nonBillable = (aggregates.non_billable_resolutions || [])
    .filter((entry) => entry.publisher_id === publisherId)
    .reduce((sum, entry) => sum + (Number.isFinite(entry.count) ? entry.count : 0), 0);
  return {
    window_id: lastWindowSnapshot.window?.started_at || null,
    billable_count: billable,
    non_billable_count: nonBillable
  };
};

const selectCreative = (candidates, publisherPolicy, publisherId) => {
  // V1.5 publisher control scaffolding; V2 candidate for persistence and UI management.
  // Deterministic, side-effect free selection based on in-memory aggregates.
  // If no candidates meet the data floor, fall back to stable ordering by priority, campaign, creative.
  ensureWindowFresh();
  const allowedTypes = new Set(publisherPolicy.allowed_demand_types || []);
  const priorityOrder = publisherPolicy.demand_priority || [];
  const selectionMode = publisherPolicy.selection_mode || "raw";
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
  const decisionTimestamp = new Date().toISOString();
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
    const capStatus = getCapStatus(candidate.scope.campaign_id);
    if (capStatus.exhausted) {
      const campaign = resolveCampaign(candidate.scope.campaign_id);
      console.log("selection.cap.exclude", {
        campaign_id: candidate.scope.campaign_id,
        advertiser_id: campaign ? campaign.advertiser_id : null,
        cap: capStatus.caps,
        observed: capStatus.state
      });
      return false;
    }
    return true;
  });

  const budgetAllowed = new Set(
    budgetFiltered.map((candidate) => `${candidate.scope.campaign_id}:${candidate.creative_id}`)
  );
  const evaluated = candidates
    .filter((candidate) => budgetAllowed.has(`${candidate.scope.campaign_id}:${candidate.creative_id}`))
    .filter((candidate) => allowedTypes.has(candidate.demand_type))
    .map((candidate) => ({ candidate, metrics: getDerivedMetrics(candidate.scope) }))
    .map((entry) => {
      const usesWeighted = selectionMode === "weighted" && entry.metrics.weighted_present;
      const metricUsed = usesWeighted ? "weighted" : selectionMode === "weighted" ? "raw_fallback" : "raw";
      const metricValue = usesWeighted ? entry.metrics.weighted_derived_value_per_1k : entry.metrics.derived_value_per_1k;
      return {
        ...entry,
        metric_used: metricUsed,
        metric_value: metricValue
      };
    });

  const floorType = publisherPolicy.floor_type === "weighted" ? "weighted" : "raw";
  const fallbackFloorValue = Number.isFinite(publisherPolicy.derived_value_floor) ? publisherPolicy.derived_value_floor : 0;
  const floorValuePer1k = Number.isFinite(publisherPolicy.floor_value_per_1k)
    ? publisherPolicy.floor_value_per_1k
    : fallbackFloorValue;
  const floorFiltered = evaluated.filter((entry) => {
    const useWeightedFloor = floorType === "weighted" && entry.metrics.weighted_present;
    const metricUsed = useWeightedFloor ? "weighted" : "raw";
    const metricValue = useWeightedFloor ? entry.metrics.weighted_derived_value_per_1k : entry.metrics.derived_value_per_1k;
    const allowed = metricValue >= floorValuePer1k;
    if (!allowed) {
      console.log("selection.floor.exclude", {
        publisher_id: publisherId,
        campaign_id: entry.candidate.scope.campaign_id,
        creative_id: entry.candidate.creative_id,
        floor_type: floorType,
        floor_value_per_1k: floorValuePer1k,
        metric_used: metricUsed,
        candidate_value_per_1k: metricValue
      });
    }
    return allowed;
  });

  const rawSorted = [...evaluated].sort((a, b) => {
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

  const weightedSorted = [...evaluated].sort((a, b) => {
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
    if (b.metrics.weighted_derived_value_per_1k !== a.metrics.weighted_derived_value_per_1k) {
      return b.metrics.weighted_derived_value_per_1k - a.metrics.weighted_derived_value_per_1k;
    }
    if (a.candidate.scope.campaign_id !== b.candidate.scope.campaign_id) {
      return a.candidate.scope.campaign_id.localeCompare(b.candidate.scope.campaign_id);
    }
    return a.candidate.creative_id.localeCompare(b.candidate.creative_id);
  });

  const rawTop = rawSorted[0];
  const weightedTop = weightedSorted[0];
  if (selectionMode === "weighted" && rawTop && weightedTop && rawTop.candidate.creative_id !== weightedTop.candidate.creative_id) {
    const denom = Math.max(Math.abs(rawTop.metrics.derived_value_per_1k), 1);
    const divergence = Math.abs(weightedTop.metrics.weighted_derived_value_per_1k - rawTop.metrics.derived_value_per_1k) / denom;
    const divergent = divergence >= GUARDRAIL_DIVERGENCE_PCT;
    recordGuardrailDivergence(publisherId, aggregationWindow.started_at, divergent);
  }

  console.log("selection.basis", {
    publisher_id: publisherId,
    mode: selectionMode,
    metric_used: selectionMode === "weighted" ? "weighted_with_fallback" : "raw",
    candidate_values: evaluated.map((entry) => ({
      campaign_id: entry.candidate.scope.campaign_id,
      creative_id: entry.candidate.creative_id,
      raw_value_per_1k: entry.metrics.derived_value_per_1k,
      weighted_value_per_1k: entry.metrics.weighted_derived_value_per_1k,
      used_metric: entry.metric_used,
      used_value_per_1k: entry.metric_value
    }))
  });

  if (floorFiltered.length === 0 && evaluated.length > 0) {
    console.log("selection.floor.fallback", { publisher_id: publisherId, reason: "floor_excluded_all" });
  }

  const eligible = (floorFiltered.length > 0 ? floorFiltered : evaluated)
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
      const aCap = getCapStatus(a.candidate.scope.campaign_id);
      const bCap = getCapStatus(b.candidate.scope.campaign_id);
      if (aCap.near_exhaustion !== bCap.near_exhaustion) {
        return aCap.near_exhaustion ? 1 : -1;
      }
      if (b.metric_value !== a.metric_value) {
        return b.metric_value - a.metric_value;
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
      weighted_value_per_1k: entry.metrics.weighted_derived_value_per_1k,
      used_metric: entry.metric_used,
      used_value_per_1k: entry.metric_value,
      budget_remaining: getBudgetStatus(entry.candidate.scope.campaign_id).remaining,
      budget_total: getBudgetStatus(entry.candidate.scope.campaign_id).total,
      near_exhaustion: getBudgetStatus(entry.candidate.scope.campaign_id).near_exhaustion
    }))
  });

  if (eligible.length === 0) {
    const fallback = candidates
      .filter((candidate) => !getBudgetStatus(candidate.scope.campaign_id).exhausted)
      .filter((candidate) => !getCapStatus(candidate.scope.campaign_id).exhausted)
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
        const aCap = getCapStatus(a.scope.campaign_id);
        const bCap = getCapStatus(b.scope.campaign_id);
        if (aCap.near_exhaustion !== bCap.near_exhaustion) {
          return aCap.near_exhaustion ? 1 : -1;
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
    if (evaluated.length === 0) {
      console.log("selection.floor.fallback", { publisher_id: publisherId, reason: "no_candidates" });
    } else if (floorFiltered.length === 0) {
      console.log("selection.floor.fallback", { publisher_id: publisherId, reason: "floor_excluded_all" });
    }
    recordSelectionDecision({
      timestamp: decisionTimestamp,
      publisher_id: publisherId,
      selection_mode: selectionMode,
      metric_used: selectionMode === "weighted" ? "weighted_with_fallback" : "raw",
      candidates: evaluated.map((entry) => ({
        campaign_id: entry.candidate.scope.campaign_id,
        creative_id: entry.candidate.creative_id,
        demand_type: entry.candidate.demand_type,
        used_metric: entry.metric_used,
        used_value_per_1k: entry.metric_value
      })),
      chosen_creative: {
        campaign_id: selected.scope.campaign_id,
        creative_id: selected.creative_id,
        demand_type: selected.demand_type
      }
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
  recordSelectionDecision({
    timestamp: decisionTimestamp,
    publisher_id: publisherId,
    selection_mode: selectionMode,
    metric_used: selectionMode === "weighted" ? "weighted_with_fallback" : "raw",
    candidates: evaluated.map((entry) => ({
      campaign_id: entry.candidate.scope.campaign_id,
      creative_id: entry.candidate.creative_id,
      demand_type: entry.candidate.demand_type,
      used_metric: entry.metric_used,
      used_value_per_1k: entry.metric_value
    })),
    chosen_creative: {
      campaign_id: selected.scope.campaign_id,
      creative_id: selected.creative_id,
      demand_type: selected.demand_type
    }
  });
  return selected;
};

const buildReport = (publisherFilter = null, includeSelections = false) => {
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
    if (publisherFilter && publisherId !== publisherFilter) {
      continue;
    }
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

  if (!includeSelections || !publisherFilter) {
    return {
      aggregates: rows,
      publisher_floor: publisherFilter ? getPublisherFloorConfig(publisherFilter) : null,
      last_window_observed: publisherFilter ? getLastWindowObserved(publisherFilter) : null,
      publisher_caps: publisherFilter ? getPublisherCapConfig(publisherFilter) : null,
      last_window_billable: publisherFilter ? getLastWindowBillableCounts(publisherFilter) : null
    };
  }

  return {
    aggregates: rows,
    publisher_floor: getPublisherFloorConfig(publisherFilter),
    last_window_observed: getLastWindowObserved(publisherFilter),
    publisher_caps: getPublisherCapConfig(publisherFilter),
    last_window_billable: getLastWindowBillableCounts(publisherFilter),
    selection_decisions: getSelectionHistory(publisherFilter, 50)
  };
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

  if (normalized.status === "RESOLVED" && typeof normalized.billable !== "boolean") {
    normalized.billable = true;
  }

  return normalized;
};

const reconcileCampaigns = (tokensSnapshot) => {
  const windowStartMs = aggregationWindow.started_at_ms || Date.now();
  const windowEndMs = windowStartMs + AGGREGATION_WINDOW_MS;
  campaignRegistry.forEach((campaign) => {
    const budget = getCampaignBudget(campaign.campaign_id);
    const budgetDelta = budget ? budget.total - budget.remaining : 0;
    const aggregateSum = Array.from(aggregations.resolvedValueSum.values())
      .filter((entry) => entry.campaign_id === campaign.campaign_id)
      .reduce((sum, entry) => sum + (Number.isFinite(entry.sum) ? entry.sum : 0), 0);

    let tokenSumWindow = 0;
    let tokenSumTotal = 0;
    tokensSnapshot.forEach((token) => {
      if (!token.scope || token.scope.campaign_id !== campaign.campaign_id) {
        return;
      }
      const finalEvent = getFinalResolutionEvent(token);
      if (!finalEvent) {
        return;
      }
      const value = Number.isFinite(finalEvent.resolved_value) ? finalEvent.resolved_value : 0;
      tokenSumTotal += value;
      const resolvedAtMs = new Date(finalEvent.resolved_at).getTime();
      if (resolvedAtMs >= windowStartMs && resolvedAtMs < windowEndMs) {
        tokenSumWindow += value;
      }
    });

    const aggregateMismatch = Math.abs(tokenSumWindow - aggregateSum) > RECONCILIATION_TOLERANCE;
    const budgetMismatch = Math.abs(tokenSumTotal - budgetDelta) > RECONCILIATION_TOLERANCE;
    const logPayload = {
      campaign_id: campaign.campaign_id,
      advertiser_id: campaign.advertiser_id || null,
      window_id: aggregationWindow.started_at,
      token_sum: tokenSumWindow,
      aggregate_sum: aggregateSum,
      budget_delta: budgetDelta,
      tolerance: RECONCILIATION_TOLERANCE
    };
    if (aggregateMismatch || budgetMismatch) {
      console.log("reconcile.mismatch", logPayload);
    } else {
      console.log("reconcile.ok", logPayload);
    }
  });
};

const rebuildCapState = (tokensSnapshot) => {
  capState.clear();
  tokensSnapshot.forEach((token) => {
    if (!token.scope || !token.scope.campaign_id) {
      return;
    }
    const finalEvent = getFinalResolutionEvent(token);
    if (!finalEvent) {
      return;
    }
    const billable = typeof token.billable === "boolean" ? token.billable : true;
    if (!billable) {
      return;
    }
    const rawValue = Number.isFinite(finalEvent.resolved_value) ? finalEvent.resolved_value : 0;
    const outcomeType = finalEvent.outcome_type || token.outcome_type || "resolved";
    const weightedValue = rawValue * resolveOutcomeWeight(token.scope.campaign_id, outcomeType);
    const state = capState.get(token.scope.campaign_id) || { billable_count: 0, billable_weighted_value: 0 };
    state.billable_count += 1;
    state.billable_weighted_value += weightedValue;
    capState.set(token.scope.campaign_id, state);
  });
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

loadRegistry();
loadKeys();
loadBudgets();
loadAggregates();
console.log("aggregate.window.start", {
  window_start: aggregationWindow.started_at,
  window_ms: AGGREGATION_WINDOW_MS
});

let tokens = normalizeTokens(loadTokens());
if (tokens.length > 0) {
  saveTokens(tokens);
  console.log("tokens.load.normalized", { count: tokens.length });
}
rebuildCapState(tokens);
reconcileCampaigns(tokens);

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
  if (!authorizePublisher(req, res, publisherId)) {
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
  const selectedCreative = candidates.length > 0 ? selectCreative(candidates, publisherPolicy, publisher.publisher_id) : {
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
  if (!authorizePublisher(req, res, publisherId)) {
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
  if (!isNonEmptyString(campaign.advertiser_id)) {
    rejectRequest(res, "/v1/intent", "campaign_missing_advertiser", { campaign_id: campaignId });
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

const handlePostback = (req, url, res) => {
  const tokenId = url.searchParams.get("token_id");
  const value = url.searchParams.get("value");
  const stage = (url.searchParams.get("stage") || "resolved").toLowerCase();
  const outcomeType = url.searchParams.get("outcome_type");

  if (!isNonEmptyString(tokenId)) {
    rejectRequest(res, "/v1/postback", "invalid_token_id");
    return;
  }
  if (!isValidStage(stage)) {
    rejectRequest(res, "/v1/postback", "invalid_stage");
    return;
  }
  if (isFinalResolutionStage(stage) && !isNonEmptyString(outcomeType)) {
    rejectRequest(res, "/v1/postback", "invalid_outcome_type");
    return;
  }

  const advertiserId = authorizeAdvertiser(req, res, null);
  if (!advertiserId) {
    return;
  }

  const token = findToken(tokenId);
  if (!token) {
    sendJson(res, 404, { error: "token not found" });
    return;
  }
  const campaign = resolveCampaign(token.scope.campaign_id);
  if (!campaign) {
    rejectRequest(res, "/v1/postback", "campaign_unknown", { campaign_id: token.scope.campaign_id });
    return;
  }
  if (!isNonEmptyString(campaign.advertiser_id) || campaign.advertiser_id !== advertiserId) {
    logAuth("auth.reject", "advertiser", advertiserId, "campaign_mismatch");
    sendJson(res, 403, { error: "auth_mismatch" });
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

  const stageKey = resolutionEventKey(tokenId, stage);
  const legacyResolved =
    token.status === "RESOLVED" &&
    (!token.resolution_events || token.resolution_events.length === 0) &&
    stage === "resolved";
  if (legacyResolved) {
    logLifecycle("postback.idempotent", token, { stage, key: stageKey, reason: "legacy_resolved" });
    sendJson(res, 200, { token, status: "already_resolved" });
    return;
  }
  const hasStageEvent = token.resolution_events?.some((event) => event.stage === stage);
  if (hasStageEvent) {
    logLifecycle("postback.idempotent", token, { stage, key: stageKey });
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
  const isFinal = isFinalResolutionStage(stage);
  const wasResolved = token.status === "RESOLVED";
  const hadFinalEvent = token.resolution_events?.some((event) => isFinalResolutionStage(event.stage));
  const finalAlreadyApplied = isFinal && (wasResolved || hadFinalEvent);
  const partialAfterFinal = !isFinal && (wasResolved || hadFinalEvent);

  if (finalAlreadyApplied) {
    console.log("postback.out_of_order", {
      token_id: tokenId,
      stage,
      key: stageKey,
      reason: "final_after_final"
    });
    logLifecycle("postback.idempotent", token, { stage, key: stageKey, reason: "final_already_applied" });
    sendJson(res, 200, { token, status: "already_resolved" });
    return;
  }

  if (partialAfterFinal) {
    console.log("postback.out_of_order", {
      token_id: tokenId,
      stage,
      key: stageKey,
      reason: "partial_after_final"
    });
    logLifecycle("postback.idempotent", token, { stage, key: stageKey, reason: "partial_after_final" });
    sendJson(res, 200, { token, status: "ignored" });
    return;
  }

  // Critical section: apply resolution atomically (single-process assumption).
  // Token mutation + persistence should be treated as one logical update.
  addResolutionEvent(token, stage, now.toISOString(), resolvedValue, outcomeType);
  if (isFinal) {
    token.status = "RESOLVED";
    if (!token.resolved_at) {
      token.resolved_at = now.toISOString();
    }
    if (token.resolved_value === null || token.resolved_value === undefined) {
      token.resolved_value = resolvedValue;
    }
    if (!token.outcome_type) {
      token.outcome_type = outcomeType;
    }
  }
  if (isFinal) {
    const outcomeWeight = resolveOutcomeWeight(token.scope.campaign_id, outcomeType);
    const weightedValue = resolvedValue * outcomeWeight;
    const capStatus = getCapStatus(token.scope.campaign_id);
    const projectedCount = capStatus.state.billable_count + 1;
    const projectedWeighted = capStatus.state.billable_weighted_value + weightedValue;
    const exceedsOutcomes =
      capStatus.caps.max_outcomes !== null && projectedCount > capStatus.caps.max_outcomes;
    const exceedsWeighted =
      capStatus.caps.max_weighted_value !== null && projectedWeighted > capStatus.caps.max_weighted_value;
    const billable = !(exceedsOutcomes || exceedsWeighted);

    token.billable = billable;
    if (!billable) {
      console.log("resolution.cap.hit", {
        campaign_id: token.scope.campaign_id,
        advertiser_id: campaign.advertiser_id || null,
        cap: capStatus.caps,
        observed: capStatus.state
      });
      console.log("resolution.non_billable", {
        campaign_id: token.scope.campaign_id,
        advertiser_id: campaign.advertiser_id || null,
        cap: capStatus.caps,
        observed: capStatus.state
      });
    } else if (
      (capStatus.caps.max_outcomes !== null && projectedCount === capStatus.caps.max_outcomes) ||
      (capStatus.caps.max_weighted_value !== null && projectedWeighted === capStatus.caps.max_weighted_value)
    ) {
      console.log("resolution.cap.hit", {
        campaign_id: token.scope.campaign_id,
        advertiser_id: campaign.advertiser_id || null,
        cap: capStatus.caps,
        observed: {
          billable_count: projectedCount,
          billable_weighted_value: projectedWeighted
        }
      });
    }

    saveTokens(tokens);
    logLifecycle("postback.resolved", token, { stage, value: resolvedValue, billable });
    // Critical section: apply final resolution effects (single-process assumption).
    // Budget and aggregates are updated once per unique final stage event.
    if (billable) {
      applyBudgetCharge(token.scope.campaign_id, resolvedValue);
      applyCapUsage(token.scope.campaign_id, weightedValue);
    }
    bumpAggregate(aggregations.resolvedIntents, token.scope, "resolved_intents");
    const resolvedValueSum = bumpResolvedValueSum(token.scope, resolvedValue);
    const weightedResolvedValueSum = bumpWeightedResolvedValueSum(token.scope, weightedValue);
    if (billable) {
      bumpResolutionCount(aggregations.billableResolutions, token.scope);
    } else {
      bumpResolutionCount(aggregations.nonBillableResolutions, token.scope);
    }
    const metrics = getDerivedMetrics(token.scope);
    const derivedValuePer1k = metrics.impressions > 0 ? (resolvedValueSum / metrics.impressions) * 1000 : 0;
    const weightedDerivedValuePer1k = metrics.impressions > 0 ? (weightedResolvedValueSum / metrics.impressions) * 1000 : 0;
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
    console.log("aggregate.weighted_metrics", {
      campaign_id: token.scope.campaign_id,
      publisher_id: token.scope.publisher_id,
      creative_id: token.scope.creative_id,
      window_start: aggregationWindow.started_at,
      weighted_resolved_value_sum: weightedResolvedValueSum,
      weighted_derived_value_per_1k: weightedDerivedValuePer1k,
      outcome_type: outcomeType,
      outcome_weight: resolveOutcomeWeight(token.scope.campaign_id, outcomeType)
    });
    sendJson(res, 200, { token, status: "resolved" });
    return;
  }

  saveTokens(tokens);

  if (partialAfterFinal) {
    logLifecycle("postback.idempotent", token, { stage, key: stageKey, reason: "partial_after_final" });
    sendJson(res, 200, { token, status: "ignored" });
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
      handlePostback(req, url, res);
      return;
    }

    if (req.method === "GET" && url.pathname === "/v1/reports") {
      const publisherId = authorizePublisher(req, res, null);
      if (!publisherId) {
        return;
      }
      const includeSelections = url.searchParams.get("include_selections") === "true";
      sendJson(res, 200, { reports: buildReport(publisherId, includeSelections) });
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
