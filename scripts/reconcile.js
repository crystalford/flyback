import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");

const registryFile = path.join(dataDir, "registry.json");
const budgetsFile = path.join(dataDir, "budgets.json");
const aggregatesFile = path.join(dataDir, "aggregates.json");
const tokensFile = path.join(dataDir, "tokens.json");
const ledgerFile = path.join(dataDir, "ledger.json");

const AGGREGATION_WINDOW_MS = 10 * 60 * 1000;
const RECONCILIATION_TOLERANCE = 0.001;
const FINAL_RESOLUTION_STAGES = new Set(["resolved", "purchase", "final"]);

const readJsonFile = (filePath) => {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const raw = fs.readFileSync(filePath, "utf8");
  if (!raw.trim()) {
    return null;
  }
  return JSON.parse(raw);
};

const aggregateKey = (scope) => `${scope.campaign_id}:${scope.publisher_id}:${scope.creative_id}`;

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

const getFinalResolutionEvent = (token) => {
  if (Array.isArray(token.resolution_events) && token.resolution_events.length > 0) {
    const finals = token.resolution_events.filter((event) => FINAL_RESOLUTION_STAGES.has(event.stage));
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
      resolved_value: token.resolved_value
    };
  }
  return null;
};

const registry = readJsonFile(registryFile);
const budgets = readJsonFile(budgetsFile);
const aggregatesPayload = readJsonFile(aggregatesFile);
const tokens = readJsonFile(tokensFile) || [];
const ledgerPayload = readJsonFile(ledgerFile);

if (!ledgerPayload || !Array.isArray(ledgerPayload.entries)) {
  console.error("reconcile.ledger.missing", { path: ledgerFile });
  process.exit(1);
}
const ledgerEntries = ledgerPayload.entries;

if (!registry || !budgets || !aggregatesPayload) {
  console.error("reconcile.load.failed", { registry: !!registry, budgets: !!budgets, aggregates: !!aggregatesPayload });
  process.exit(1);
}

const aggregationWindow = aggregatesPayload.window || {
  started_at_ms: Date.now(),
  started_at: new Date().toISOString()
};
const current = aggregatesPayload.current || {
  impressions: [],
  intents: [],
  resolved_intents: [],
  resolved_value_sum: [],
  partial_resolutions: []
};
const aggregations = {
  resolvedValueSum: mapFromEntries(current.resolved_value_sum, aggregateKey)
};
const budgetIndex = new Map(
  Array.isArray(budgets.campaigns) ? budgets.campaigns.map((entry) => [entry.campaign_id, entry]) : []
);

const windowStartMs = aggregationWindow.started_at_ms || Date.now();
const windowEndMs = windowStartMs + AGGREGATION_WINDOW_MS;

registry.campaigns.forEach((campaign) => {
  const budget = budgetIndex.get(campaign.campaign_id);
  const budgetDelta = budget ? budget.total - budget.remaining : 0;
  const aggregateSum = Array.from(aggregations.resolvedValueSum.values())
    .filter((entry) => entry.campaign_id === campaign.campaign_id)
    .reduce((sum, entry) => sum + (Number.isFinite(entry.sum) ? entry.sum : 0), 0);

  let tokenSumWindow = 0;
  let tokenSumTotal = 0;
  tokens.forEach((token) => {
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
  const payload = {
    campaign_id: campaign.campaign_id,
    advertiser_id: campaign.advertiser_id || null,
    window_id: aggregationWindow.started_at,
    token_sum: tokenSumWindow,
    aggregate_sum: aggregateSum,
    budget_delta: budgetDelta,
    tolerance: RECONCILIATION_TOLERANCE
  };
  if (aggregateMismatch || budgetMismatch) {
    console.log("reconcile.mismatch", payload);
  } else {
    console.log("reconcile.ok", payload);
  }
});

registry.campaigns.forEach((campaign) => {
  const windowStartMs = aggregationWindow.started_at_ms || Date.now();
  const windowEndMs = windowStartMs + AGGREGATION_WINDOW_MS;
  const revShareBps = Number.isFinite(campaign.publisher_rev_share_bps)
    ? campaign.publisher_rev_share_bps
    : registry.policies?.[campaign.publisher_id]?.rev_share_bps ?? 7000;

  let expectedWindow = 0;
  let expectedLifetime = 0;
  tokens.forEach((token) => {
    if (!token.scope || token.scope.campaign_id !== campaign.campaign_id) {
      return;
    }
    if (token.billable !== true) {
      return;
    }
    const finalEvent = getFinalResolutionEvent(token);
    if (!finalEvent) {
      return;
    }
    const value = Number.isFinite(finalEvent.resolved_value) ? finalEvent.resolved_value : 0;
    const payoutCents = Math.round(value * 100 * (revShareBps / 10000));
    expectedLifetime += payoutCents;
    const resolvedAtMs = new Date(finalEvent.resolved_at).getTime();
    if (resolvedAtMs >= windowStartMs && resolvedAtMs < windowEndMs) {
      expectedWindow += payoutCents;
    }
  });

  const ledgerWindowSum = ledgerEntries
    .filter((entry) => entry.campaign_id === campaign.campaign_id && entry.window_id === aggregationWindow.started_at)
    .reduce((sum, entry) => sum + (Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0), 0);
  const ledgerLifetimeSum = ledgerEntries
    .filter((entry) => entry.campaign_id === campaign.campaign_id)
    .reduce((sum, entry) => sum + (Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0), 0);

  const windowMismatch = Math.abs(ledgerWindowSum - expectedWindow) > RECONCILIATION_TOLERANCE;
  const lifetimeMismatch = Math.abs(ledgerLifetimeSum - expectedLifetime) > RECONCILIATION_TOLERANCE;
  const payload = {
    campaign_id: campaign.campaign_id,
    advertiser_id: campaign.advertiser_id || null,
    publisher_id: campaign.publisher_id,
    window_id: aggregationWindow.started_at,
    ledger_sum: ledgerWindowSum,
    expected_sum: expectedWindow,
    tolerance: RECONCILIATION_TOLERANCE
  };
  if (windowMismatch || lifetimeMismatch) {
    console.log("ledger.reconcile.mismatch", {
      ...payload,
      lifetime_ledger_sum: ledgerLifetimeSum,
      lifetime_expected_sum: expectedLifetime
    });
  } else {
    console.log("ledger.reconcile.ok", payload);
  }
});
