import fs from "fs";
import http from "http";
import path from "path";
import { fileURLToPath } from "url";
import { createHmac, randomUUID } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const port = Number(process.env.PORT) || 3000;
const host = process.env.HOST || "0.0.0.0";
const publicDir = path.join(__dirname, "public");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(__dirname, "data");
const schemaDir = path.join(__dirname, "schemas");
const tokensFile = path.join(dataDir, "tokens.json");
const registryFile = path.join(dataDir, "registry.json");
const budgetsFile = path.join(dataDir, "budgets.json");
const aggregatesFile = path.join(dataDir, "aggregates.json");
const keysFile = path.join(dataDir, "keys.json");
const ledgerFile = path.join(dataDir, "ledger.json");
const payoutsFile = path.join(dataDir, "payouts.json");
const eventsFile = path.join(dataDir, "events.ndjson");
const eventStateFile = path.join(dataDir, "event_state.json");
const snapshotFile = path.join(dataDir, "snapshot.json");
const projectionStateFile = path.join(dataDir, "projection_state.json");
const eventIndexFile = path.join(dataDir, "event_index.json");
const deliveryStateFile = path.join(dataDir, "delivery_state.json");
const deliveryDlqFile = path.join(dataDir, "delivery_dlq.ndjson");
const invoicesDir = path.join(dataDir, "invoices");
const schemasFile = path.join(schemaDir, "schemas.json");

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
const projectionState = {
  tokens: [],
  aggregates: {
    impressions: new Map(),
    intents: new Map(),
    resolvedIntents: new Map(),
    resolvedValueSum: new Map(),
    partialResolutions: new Map(),
    weightedResolvedValueSum: new Map(),
    billableResolutions: new Map(),
    nonBillableResolutions: new Map()
  },
  budgets: new Map(),
  ledger: []
};
let lastWindowSnapshot = null;
let ledgerIndex = new Set();
let events = [];
let projectionCursor = { applied_seq: 0 };
let tokenIndex = new Map();
let eventIdIndex = new Set();
let appliedEventIds = new Set();
let reducerFailpointType = null;
let schemaRegistry = null;
let payoutRuns = [];

const VERSION = "0.3.0";
const SCHEMA_VERSION = 1;
const AGGREGATION_WINDOW_MS = 10 * 60 * 1000;
const ROLE_ENV = (process.env.FLYBACK_ROLE || process.env.ROLE || "").toLowerCase();
const VALID_ROLES = new Set(["writer", "replica"]);
let resolvedRole = null;
if (ROLE_ENV) {
  if (VALID_ROLES.has(ROLE_ENV)) {
    resolvedRole = ROLE_ENV;
  } else {
    console.log("process.role.invalid", { role: ROLE_ENV, allowed: Array.from(VALID_ROLES) });
  }
}
let writeEnabled = process.env.WRITE_ENABLED !== "false";
if (resolvedRole === "writer") {
  writeEnabled = true;
}
if (resolvedRole === "replica") {
  writeEnabled = false;
}
const WRITE_ENABLED = writeEnabled;
const PROCESS_ROLE = resolvedRole || (WRITE_ENABLED ? "writer" : "replica");

const WEBHOOK_URL = process.env.WEBHOOK_URL || "";
const WEBHOOK_TIMEOUT_MS = Number(process.env.WEBHOOK_TIMEOUT_MS) || 5000;
const WEBHOOK_RETRY_BASE_MS = Number(process.env.WEBHOOK_RETRY_BASE_MS) || 1000;
const WEBHOOK_RETRY_MAX_MS = Number(process.env.WEBHOOK_RETRY_MAX_MS) || 30000;
const WEBHOOK_MAX_RETRIES = Number(process.env.WEBHOOK_MAX_RETRIES) || 10;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";
const OPS_TOKEN_SECRET = process.env.OPS_TOKEN_SECRET || "dev-ops-secret";
const OPS_TOKEN_TTL_MS = Number(process.env.OPS_TOKEN_TTL_MS) || 15 * 60 * 1000;
const OPS_TOKEN_COOKIE = "flyback_ops_token";
const DELIVERY_LAG_WARN = Number(process.env.DELIVERY_LAG_WARN) || 100;
const RATE_LIMIT_WINDOW_MS = Number(process.env.RATE_LIMIT_WINDOW_MS) || 60000;
const RATE_LIMIT_MAX = Number(process.env.RATE_LIMIT_MAX) || 120;
const RATE_LIMIT_BYPASS = process.env.RATE_LIMIT_BYPASS === "true";
const BUDGET_DEPRIORITIZE_THRESHOLD = 0.2;
const RECONCILIATION_TOLERANCE = 0.001;
const SELECTION_HISTORY_LIMIT = 1000;
const GUARDRAIL_DIVERGENCE_PCT = 0.3;
const GUARDRAIL_WINDOW_THRESHOLD = 2;
const CAP_DEPRIORITIZE_THRESHOLD = 0.8;
const LEDGER_VERSION = 1;
const EVENT_INDEX_VERSION = 1;
const DELIVERY_SCHEMA_VERSION = 1;
let aggregationWindow = {
  started_at_ms: Date.now(),
  started_at: new Date().toISOString()
};
let deliveryState = {
  last_delivered_seq: 0,
  last_attempt_at: null,
  retry_count: 0
};
let deliveryInFlight = false;
let deliveryRetryCount = 0;
let deliveryNextAttemptAt = 0;
let deliveryCursorIndex = 0;
const rateLimitBuckets = new Map();

const logReadViolation = (context, path) => {
  console.log("readmodel.violation", { context, path });
  throw new Error("readmodel.violation");
};

const isPlainObject = (value) =>
  value !== null && typeof value === "object" && !Array.isArray(value);

const schemaTypeMatches = (type, value) => {
  switch (type) {
    case "object":
      return isPlainObject(value);
    case "array":
      return Array.isArray(value);
    case "string":
      return typeof value === "string";
    case "number":
      return Number.isFinite(value);
    case "integer":
      return Number.isInteger(value);
    case "boolean":
      return typeof value === "boolean";
    case "null":
      return value === null;
    default:
      return true;
  }
};

const validateSchema = (schema, value, path = "$") => {
  const errors = [];
  if (!schema || typeof schema !== "object") {
    return errors;
  }
  if (schema.enum && !schema.enum.includes(value)) {
    errors.push(`${path}.enum`);
    return errors;
  }
  if (schema.type) {
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    const matches = types.some((type) => schemaTypeMatches(type, value));
    if (!matches) {
      errors.push(`${path}.type`);
      return errors;
    }
  }
  if (schema.type === "array" && Array.isArray(value)) {
    if (Number.isFinite(schema.minItems) && value.length < schema.minItems) {
      errors.push(`${path}.minItems`);
    }
    if (schema.items) {
      value.forEach((entry, index) => {
        errors.push(...validateSchema(schema.items, entry, `${path}[${index}]`));
      });
    }
  }
  if (schema.type === "object" && isPlainObject(value)) {
    if (Array.isArray(schema.required)) {
      schema.required.forEach((key) => {
        if (!Object.prototype.hasOwnProperty.call(value, key)) {
          errors.push(`${path}.required.${key}`);
        }
      });
    }
    const props = schema.properties || {};
    Object.entries(props).forEach(([key, propSchema]) => {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        if (value[key] === undefined) {
          return;
        }
        errors.push(...validateSchema(propSchema, value[key], `${path}.${key}`));
      }
    });
    if (schema.additionalProperties === false) {
      Object.keys(value).forEach((key) => {
        if (!Object.prototype.hasOwnProperty.call(props, key)) {
          errors.push(`${path}.additional.${key}`);
        }
      });
    } else if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
      Object.keys(value).forEach((key) => {
        if (!Object.prototype.hasOwnProperty.call(props, key)) {
          errors.push(...validateSchema(schema.additionalProperties, value[key], `${path}.${key}`));
        }
      });
    }
  }
  return errors;
};

const policySchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  properties: {
    allowed_demand_types: { type: "array", items: { type: "string" } },
    derived_value_floor: { type: "number" },
    demand_priority: { type: "array", items: { type: "string" } },
    selection_mode: { type: "string", enum: ["raw", "weighted"] },
    floor_type: { type: "string", enum: ["raw", "weighted"] },
    floor_value_per_1k: { type: "number" },
    rev_share_bps: { type: "number" }
  },
  additionalProperties: true
};

const registrySchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: ["version", "advertisers", "publishers", "campaigns", "creatives", "policies"],
  properties: {
    version: { type: "integer" },
    advertisers: {
      type: "array",
      minItems: 1,
      items: {
        type: "object",
        required: ["advertiser_id", "campaign_ids"],
        properties: {
          advertiser_id: { type: "string" },
          campaign_ids: { type: "array", items: { type: "string" } }
        },
        additionalProperties: true
      }
    },
    publishers: {
      type: "array",
      minItems: 1,
      items: {
        type: "object",
        required: ["publisher_id", "campaign_ids"],
        properties: {
          publisher_id: { type: "string" },
          campaign_ids: { type: "array", items: { type: "string" } }
        },
        additionalProperties: true
      }
    },
    campaigns: {
      type: "array",
      minItems: 1,
      items: {
        type: "object",
        required: ["campaign_id", "publisher_id", "advertiser_id", "creative_ids"],
        properties: {
          campaign_id: { type: "string" },
          publisher_id: { type: "string" },
          advertiser_id: { type: "string" },
          creative_ids: { type: "array", items: { type: "string" } },
          outcome_weights: { type: "object", additionalProperties: { type: "number" } },
          caps: {
            type: "object",
            properties: {
              max_outcomes: { type: "number" },
              max_weighted_value: { type: "number" }
            },
            additionalProperties: true
          },
          publisher_rev_share_bps: { type: "number" }
        },
        additionalProperties: true
      }
    },
    creatives: {
      type: "array",
      minItems: 1,
      items: {
        type: "object",
        required: ["creative_id", "creative_url", "sizes"],
        properties: {
          creative_id: { type: "string" },
          creative_url: { type: "string" },
          sizes: { type: "array", items: { type: "string" } },
          demand_type: { type: "string" }
        },
        additionalProperties: true
      }
    },
    policies: {
      type: "object",
      additionalProperties: policySchema
    }
  },
  additionalProperties: true
};

const keysSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: ["version", "publishers", "advertisers"],
  properties: {
    version: { type: "integer" },
    publishers: {
      type: "array",
      items: {
        type: "object",
        required: ["publisher_id", "api_key"],
        properties: {
          publisher_id: { type: "string" },
          api_key: { type: "string" }
        },
        additionalProperties: true
      }
    },
    advertisers: {
      type: "array",
      items: {
        type: "object",
        required: ["advertiser_id", "api_key"],
        properties: {
          advertiser_id: { type: "string" },
          api_key: { type: "string" }
        },
        additionalProperties: true
      }
    }
  },
  additionalProperties: true
};

const eventSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: ["seq", "event_id", "ts", "type", "payload"],
  properties: {
    seq: { type: "integer" },
    event_id: { type: "string" },
    ts: { type: "string" },
    type: { type: "string" },
    payload: { type: "object" }
  },
  additionalProperties: true
};

const deliveryStateSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: ["last_delivered_seq"],
  properties: {
    last_delivered_seq: { type: "integer" },
    updated_at: { type: "string" },
    last_attempt_at: { type: ["string", "null"] },
    retry_count: { type: "number" }
  },
  additionalProperties: true
};

const dlqEntrySchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: ["failed_at", "seq", "event_id", "status", "error", "payload"],
  properties: {
    failed_at: { type: "string" },
    seq: { type: "integer" },
    event_id: { type: "string" },
    status: { type: "number" },
    error: { type: ["string", "null"] },
    payload: { type: "object" }
  },
  additionalProperties: true
};

const tokenSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: ["token_id", "scope"],
  properties: {
    token_id: { type: "string" },
    scope: {
      type: "object",
      required: ["campaign_id", "publisher_id", "creative_id"],
      properties: {
        campaign_id: { type: "string" },
        publisher_id: { type: "string" },
        creative_id: { type: "string" }
      },
      additionalProperties: true
    }
  },
  additionalProperties: true
};

const ledgerEntrySchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: [
    "entry_id",
    "created_at",
    "token_id",
    "campaign_id",
    "advertiser_id",
    "publisher_id",
    "creative_id",
    "window_id",
    "outcome_type",
    "raw_value",
    "weighted_value",
    "billable",
    "payout_cents",
    "rev_share_bps",
    "final_stage"
  ],
  properties: {
    entry_id: { type: "string" },
    created_at: { type: "string" },
    token_id: { type: "string" },
    campaign_id: { type: "string" },
    advertiser_id: { type: "string" },
    publisher_id: { type: "string" },
    creative_id: { type: "string" },
    window_id: { type: "string" },
    outcome_type: { type: "string" },
    raw_value: { type: "number" },
    weighted_value: { type: "number" },
    billable: { type: "boolean" },
    payout_cents: { type: "number" },
    rev_share_bps: { type: "number" },
    final_stage: { type: "string" }
  },
  additionalProperties: true
};

const payoutRunSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: [
    "run_id",
    "created_at",
    "publisher_id",
    "window_id",
    "entry_count",
    "payout_cents",
    "entry_ids",
    "status"
  ],
  properties: {
    run_id: { type: "string" },
    created_at: { type: "string" },
    publisher_id: { type: "string" },
    window_id: { type: "string" },
    entry_count: { type: "number" },
    payout_cents: { type: "number" },
    entry_ids: { type: "array", items: { type: "string" } },
    status: { type: "string" },
    status_history: {
      type: "array",
      items: {
        type: "object",
        required: ["status", "updated_at"],
        properties: {
          status: { type: "string" },
          updated_at: { type: "string" }
        },
        additionalProperties: true
      }
    }
  },
  additionalProperties: true
};

const payoutReconciliationSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: ["status", "runs_checked", "mismatches", "missing_entries", "unassigned_entries", "checked_at"],
  properties: {
    status: { type: "string" },
    runs_checked: { type: "number" },
    mismatches: { type: "number" },
    missing_entries: { type: "number" },
    unassigned_entries: { type: "number" },
    checked_at: { type: "string" }
  },
  additionalProperties: true
};

const payoutRunLinkSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: ["advertiser_id", "run_ids", "payout_cents", "run_count"],
  properties: {
    advertiser_id: { type: "string" },
    run_ids: { type: "array", items: { type: "string" } },
    payout_cents: { type: "number" },
    run_count: { type: "number" }
  },
  additionalProperties: true
};

const publisherStatementSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: [
    "publisher_id",
    "ledger_entries",
    "ledger_payout_cents",
    "payout_runs",
    "payout_pending_cents",
    "payout_sent_cents",
    "payout_settled_cents",
    "last_run_at"
  ],
  properties: {
    publisher_id: { type: "string" },
    ledger_entries: { type: "number" },
    ledger_payout_cents: { type: "number" },
    payout_runs: { type: "number" },
    payout_pending_cents: { type: "number" },
    payout_sent_cents: { type: "number" },
    payout_settled_cents: { type: "number" },
    last_run_at: { type: ["string", "null"] }
  },
  additionalProperties: true
};

const eventPayloadSchemas = {
  "intent.created": {
    schema_version: SCHEMA_VERSION,
    type: "object",
    required: ["token"],
    properties: {
      token: tokenSchema
    },
    additionalProperties: true
  },
  "impression.recorded": {
    schema_version: SCHEMA_VERSION,
    type: "object",
    required: ["scope", "occurred_at"],
    properties: {
      scope: tokenSchema.properties.scope,
      occurred_at: { type: "string" }
    },
    additionalProperties: true
  },
  "resolution.partial": {
    schema_version: SCHEMA_VERSION,
    type: "object",
    required: ["token_id", "stage", "resolved_at", "resolved_value"],
    properties: {
      token_id: { type: "string" },
      stage: { type: "string" },
      resolved_at: { type: "string" },
      resolved_value: { type: "number" },
      outcome_type: { type: ["string", "null"] }
    },
    additionalProperties: true
  },
  "resolution.final": {
    schema_version: SCHEMA_VERSION,
    type: "object",
    required: ["token_id", "stage", "resolved_at", "resolved_value", "outcome_type", "weighted_value", "billable"],
    properties: {
      token_id: { type: "string" },
      stage: { type: "string" },
      resolved_at: { type: "string" },
      resolved_value: { type: "number" },
      outcome_type: { type: "string" },
      weighted_value: { type: "number" },
      billable: { type: "boolean" }
    },
    additionalProperties: true
  },
  "budget.decrement": {
    schema_version: SCHEMA_VERSION,
    type: "object",
    required: ["token_id", "campaign_id", "amount"],
    properties: {
      token_id: { type: "string" },
      campaign_id: { type: "string" },
      amount: { type: "number" }
    },
    additionalProperties: true
  },
  "ledger.append": {
    schema_version: SCHEMA_VERSION,
    type: "object",
    required: ["entry"],
    properties: {
      entry: ledgerEntrySchema
    },
    additionalProperties: true
  },
  "window.reset": {
    schema_version: SCHEMA_VERSION,
    type: "object",
    properties: {
      reason: { type: "string" },
      previous_window: { type: "object", additionalProperties: true },
      new_window: { type: "object", additionalProperties: true }
    },
    additionalProperties: true
  }
};

const reportRowSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: [
    "campaign_id",
    "publisher_id",
    "creative_id",
    "impressions",
    "intents",
    "resolvedIntents",
    "intent_rate",
    "resolution_rate",
    "derived_value_per_1k"
  ],
  properties: {
    campaign_id: { type: "string" },
    publisher_id: { type: "string" },
    creative_id: { type: "string" },
    impressions: { type: "number" },
    intents: { type: "number" },
    resolvedIntents: { type: "number" },
    intent_rate: { type: "number" },
    resolution_rate: { type: "number" },
    derived_value_per_1k: { type: "number" }
  },
  additionalProperties: true
};

const reportSchema = {
  schema_version: SCHEMA_VERSION,
  type: "object",
  required: [
    "aggregates",
    "publisher_floor",
    "publisher_policy",
    "publisher_statement",
    "last_window_observed",
    "publisher_caps",
    "last_window_billable",
    "payout_reconciliation",
    "payout_runs",
    "payout_run_links",
    "ledger_stats",
    "ledger_entries",
    "selection_decisions",
    "delivery_health",
    "invoice_drafts",
    "system_status"
  ],
  properties: {
    aggregates: { type: "array", items: reportRowSchema },
    publisher_floor: {
      type: ["object", "null"],
      properties: {
        selection_mode: { type: "string" },
        floor_type: { type: "string" },
        floor_value_per_1k: { type: "number" }
      },
      additionalProperties: true
    },
    publisher_policy: {
      type: ["object", "null"],
      properties: {
        selection_mode: { type: "string" },
        floor_type: { type: "string" },
        floor_value_per_1k: { type: "number" },
        derived_value_floor: { type: "number" },
        allowed_demand_types: { type: "array", items: { type: "string" } },
        demand_priority: { type: "array", items: { type: "string" } },
        rev_share_bps: { type: "number" }
      },
      additionalProperties: true
    },
    publisher_statement: {
      type: ["object", "null"],
      properties: {
        publisher_id: { type: "string" },
        ledger_entries: { type: "number" },
        ledger_payout_cents: { type: "number" },
        payout_runs: { type: "number" },
        payout_pending_cents: { type: "number" },
        payout_sent_cents: { type: "number" },
        payout_settled_cents: { type: "number" },
        last_run_at: { type: ["string", "null"] }
      },
      additionalProperties: true
    },
    last_window_observed: {
      type: ["object", "null"],
      properties: {
        window_id: { type: ["string", "null"] },
        impressions: { type: "number" },
        raw_value_per_1k: { type: "number" },
        weighted_value_per_1k: { type: "number" }
      },
      additionalProperties: true
    },
    publisher_caps: {
      type: ["array", "null"],
      items: {
        type: "object",
        required: ["campaign_id", "advertiser_id", "caps"],
        properties: {
          campaign_id: { type: "string" },
          advertiser_id: { type: ["string", "null"] },
          caps: { type: ["object", "null"], additionalProperties: true }
        },
        additionalProperties: true
      }
    },
    last_window_billable: {
      type: ["object", "null"],
      properties: {
        window_id: { type: ["string", "null"] },
        billable_count: { type: "number" },
        non_billable_count: { type: "number" }
      },
      additionalProperties: true
    },
    payout_reconciliation: {
      type: ["object", "null"],
      properties: {
        status: { type: "string" },
        runs_checked: { type: "number" },
        mismatches: { type: "number" },
        missing_entries: { type: "number" },
        unassigned_entries: { type: "number" },
        checked_at: { type: "string" }
      },
      additionalProperties: true
    },
    payout_runs: {
      type: ["array", "null"],
      items: payoutRunSchema
    },
    payout_run_links: {
      type: ["array", "null"],
      items: payoutRunLinkSchema
    },
    ledger_stats: {
      type: ["object", "null"],
      properties: {
        window_id: { type: ["string", "null"] },
        window_payout_cents_estimate: { type: "number" },
        lifetime_payout_cents_estimate: { type: "number" },
        window_entry_count: { type: "number" },
        lifetime_entry_count: { type: "number" }
      },
      additionalProperties: true
    },
    ledger_entries: {
      type: ["array", "null"],
      items: ledgerEntrySchema
    },
    selection_decisions: {
      type: ["array", "null"],
      items: {
        type: "object",
        required: ["timestamp", "publisher_id", "selection_mode", "metric_used", "candidates", "chosen_creative"],
        properties: {
          timestamp: { type: "string" },
          publisher_id: { type: "string" },
          selection_mode: { type: "string" },
          metric_used: { type: "string" },
          candidates: {
            type: "array",
            items: {
              type: "object",
              required: ["campaign_id", "creative_id", "demand_type", "used_metric", "used_value_per_1k"],
              properties: {
                campaign_id: { type: "string" },
                creative_id: { type: "string" },
                demand_type: { type: "string" },
                used_metric: { type: "string" },
                used_value_per_1k: { type: "number" }
              },
              additionalProperties: true
            }
          },
          chosen_creative: {
            type: "object",
            required: ["campaign_id", "creative_id", "demand_type"],
            properties: {
              campaign_id: { type: "string" },
              creative_id: { type: "string" },
              demand_type: { type: "string" }
            },
            additionalProperties: true
          }
        },
        additionalProperties: true
      }
    },
    delivery_health: {
      type: ["object", "null"],
      properties: {
        last_delivered_seq: { type: "number" },
        last_attempt_at: { type: ["string", "null"] },
        retry_count: { type: "number" },
        last_event_seq: { type: "number" },
        delivery_lag: { type: "number" },
        dlq: {
          type: "object",
          required: ["count", "last_entry"],
          properties: {
            count: { type: "number" },
            last_entry: {
              type: ["object", "null"],
              properties: {
                failed_at: { type: "string" },
                seq: { type: "number" },
                event_id: { type: "string" },
                status: { type: "number" },
                error: { type: ["string", "null"] }
              },
              additionalProperties: true
            }
          },
          additionalProperties: true
        }
      },
      additionalProperties: true
    },
    invoice_drafts: {
      type: "array",
      items: {
        type: "object",
        required: ["filename", "advertiser_id", "payout_cents", "entry_count", "created_at"],
        properties: {
          filename: { type: "string" },
          advertiser_id: { type: "string" },
          payout_cents: { type: "number" },
          entry_count: { type: "number" },
          created_at: { type: "string" }
        },
        additionalProperties: true
      }
    },
    system_status: {
      type: "object",
      required: ["role", "write_enabled", "webhook_enabled", "webhook_signature_enabled"],
      properties: {
        role: { type: "string" },
        write_enabled: { type: "boolean" },
        webhook_enabled: { type: "boolean" },
        webhook_signature_enabled: { type: "boolean" }
      },
      additionalProperties: true
    }
  },
  additionalProperties: true
};

const schemaDefaults = {
  policy: policySchema,
  registry: registrySchema,
  keys: keysSchema,
  event: eventSchema,
  delivery_state: deliveryStateSchema,
  dlq_entry: dlqEntrySchema,
  delivery_health: reportSchema.properties.delivery_health,
  delivery_payload: {
    schema_version: SCHEMA_VERSION,
    type: "object",
    required: ["schema_version", "delivery_ts", "seq", "event_id", "type", "ts", "payload"],
    properties: {
      schema_version: { type: "number" },
      delivery_ts: { type: "string" },
      seq: { type: "number" },
      event_id: { type: "string" },
      type: { type: "string" },
      ts: { type: "string" },
      payload: { type: "object" }
    },
    additionalProperties: true
  },
  token: tokenSchema,
  ledger_entry: ledgerEntrySchema,
  payout_run: payoutRunSchema,
  payout_reconciliation: payoutReconciliationSchema,
  payout_run_link: payoutRunLinkSchema,
  publisher_statement: publisherStatementSchema,
  event_payloads: eventPayloadSchemas,
  report_row: reportRowSchema,
  report: reportSchema
};

console.log("process.role", { role: PROCESS_ROLE, source: resolvedRole ? "env" : "derived" });
console.log("process.write_capability", { write_enabled: WRITE_ENABLED });
console.log("process.startup", {
  role: PROCESS_ROLE,
  write_enabled: WRITE_ENABLED,
  event_authority: WRITE_ENABLED ? "writer" : "read_only"
});
console.log("process.webhook", { enabled: Boolean(WEBHOOK_URL), url: WEBHOOK_URL || null });

const parseCookies = (header) => {
  const cookies = {};
  if (!header) {
    return cookies;
  }
  header.split(";").forEach((pair) => {
    const [key, ...rest] = pair.trim().split("=");
    if (!key) {
      return;
    }
    cookies[key] = rest.join("=");
  });
  return cookies;
};

const enforceConfig = () => {
  const issues = [];
  if (!WRITE_ENABLED && PROCESS_ROLE === "writer") {
    issues.push("writer_role_without_write_enabled");
  }
  if (WRITE_ENABLED && PROCESS_ROLE === "replica") {
    issues.push("replica_role_with_write_enabled");
  }
  if (OPS_TOKEN_SECRET === "dev-ops-secret") {
    issues.push("ops_token_secret_default");
  }
  if (WEBHOOK_URL && !WEBHOOK_URL.startsWith("http")) {
    issues.push("webhook_url_invalid");
  }
  if (RATE_LIMIT_MAX <= 0) {
    issues.push("rate_limit_max_invalid");
  }
  if (issues.length > 0) {
    console.log("config.warning", { issues });
  }
};

enforceConfig();

const signOpsToken = (payload) => {
  const data = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const sig = createHmac("sha256", OPS_TOKEN_SECRET).update(data).digest("base64url");
  return `${data}.${sig}`;
};

const getClientIp = (req) => {
  const forwarded = req.headers["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.trim().length > 0) {
    return forwarded.split(",")[0].trim();
  }
  return req.socket?.remoteAddress || "unknown";
};

const applyRateLimit = (req, res) => {
  if (RATE_LIMIT_BYPASS) {
    return true;
  }
  const ip = getClientIp(req);
  const now = Date.now();
  const bucket = rateLimitBuckets.get(ip) || { count: 0, resetAt: now + RATE_LIMIT_WINDOW_MS };
  if (now > bucket.resetAt) {
    bucket.count = 0;
    bucket.resetAt = now + RATE_LIMIT_WINDOW_MS;
  }
  bucket.count += 1;
  rateLimitBuckets.set(ip, bucket);
  res.setHeader("X-RateLimit-Limit", String(RATE_LIMIT_MAX));
  res.setHeader("X-RateLimit-Remaining", String(Math.max(0, RATE_LIMIT_MAX - bucket.count)));
  res.setHeader("X-RateLimit-Reset", String(Math.floor(bucket.resetAt / 1000)));
  if (bucket.count > RATE_LIMIT_MAX) {
    res.writeHead(429, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "rate_limited" }));
    console.log("rate.limit", { ip, count: bucket.count, request_id: req.request_id || null });
    return false;
  }
  return true;
};

const verifyOpsToken = (token) => {
  if (!token) {
    return null;
  }
  const parts = token.split(".");
  if (parts.length !== 2) {
    return null;
  }
  const [data, sig] = parts;
  const expected = createHmac("sha256", OPS_TOKEN_SECRET).update(data).digest("base64url");
  if (expected !== sig) {
    return null;
  }
  try {
    const payload = JSON.parse(Buffer.from(data, "base64url").toString("utf8"));
    if (!payload || !Number.isFinite(payload.exp)) {
      return null;
    }
    if (Date.now() > payload.exp) {
      return null;
    }
    return payload;
  } catch {
    return null;
  }
};

const issueOpsCookie = (res, publisherId) => {
  const exp = Date.now() + OPS_TOKEN_TTL_MS;
  const token = signOpsToken({ publisher_id: publisherId, exp });
  const maxAge = Math.floor(OPS_TOKEN_TTL_MS / 1000);
  res.setHeader("Set-Cookie", `${OPS_TOKEN_COOKIE}=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${maxAge}`);
};

const authorizeOpsRequest = (req, url, res) => {
  const cookies = parseCookies(req.headers.cookie);
  const tokenPayload = verifyOpsToken(cookies[OPS_TOKEN_COOKIE]);
  if (tokenPayload) {
    return true;
  }
  const apiKeyParam = url.searchParams.get("api_key");
  const apiKeyHeader = extractApiKey(req);
  const apiKey = apiKeyHeader || apiKeyParam;
  const publisherId = resolvePublisherFromApiKey(apiKey);
  if (publisherId) {
    issueOpsCookie(res, publisherId);
    return true;
  }
  logAuth("auth.reject", "publisher", null, "missing_key", req.request_id || null);
  sendJson(res, 401, { error: "auth_required" });
  return false;
};

const getOpsPublisherId = (req) => {
  const cookies = parseCookies(req.headers.cookie);
  const tokenPayload = verifyOpsToken(cookies[OPS_TOKEN_COOKIE]);
  if (tokenPayload && tokenPayload.publisher_id) {
    return tokenPayload.publisher_id;
  }
  return null;
};

const rejectWrites = (res, endpoint, reason = "write_disabled") => {
  console.log("write.disabled", { endpoint, reason });
  sendJson(res, 503, { error: "write_disabled" });
};

const createBlockingMutex = () => new Int32Array(new SharedArrayBuffer(4));

const acquireBlockingMutex = (mutex, label) => {
  const timeoutMs = Number(process.env.LOCK_TIMEOUT_MS) || 5000;
  const retryMs = Number(process.env.LOCK_RETRY_MS) || 50;
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (Atomics.compareExchange(mutex, 0, 0, 1) === 0) {
      if (label) {
        console.log("event.append.lock.acquire", { label });
      }
      return true;
    }
    Atomics.wait(mutex, 0, 1, retryMs);
  }
  return false;
};

const releaseBlockingMutex = (mutex, label) => {
  Atomics.store(mutex, 0, 0);
  Atomics.notify(mutex, 0, 1);
  if (label) {
    console.log("event.append.lock.release", { label });
  }
};

const createReadOnlyProxy = (target, context) => {
  const cache = new WeakMap();
  const wrapIterator = (iterator, path) => ({
    [Symbol.iterator]() {
      return this;
    },
    next() {
      const next = iterator.next();
      if (next.done) {
        return next;
      }
      const value = next.value;
      if (Array.isArray(value) && value.length === 2) {
        return { ...next, value: [value[0], wrap(value[1], `${path}.value`)] };
      }
      return { ...next, value: wrap(value, `${path}.value`) };
    }
  });
  const wrap = (value, path) => {
    if (!value || typeof value !== "object") {
      return value;
    }
    if (cache.has(value)) {
      return cache.get(value);
    }
    if (value instanceof Map) {
      const proxy = new Proxy(value, {
        get(targetMap, prop) {
          if (prop === "set" || prop === "delete" || prop === "clear") {
            return () => logReadViolation(context, `${path}.${String(prop)}`);
          }
          if (prop === "get") {
            return (key) => wrap(targetMap.get(key), `${path}.get(${String(key)})`);
          }
          if (prop === "forEach") {
            return (fn, thisArg) =>
              targetMap.forEach((val, key) => fn.call(thisArg, wrap(val, `${path}.forEach`), key, proxy));
          }
          if (prop === "values") {
            return () => wrapIterator(targetMap.values(), `${path}.values`);
          }
          if (prop === "entries") {
            return () => wrapIterator(targetMap.entries(), `${path}.entries`);
          }
          if (prop === Symbol.iterator) {
            return () => wrapIterator(targetMap[Symbol.iterator](), `${path}.iterator`);
          }
          const raw = targetMap[prop];
          return typeof raw === "function" ? raw.bind(targetMap) : raw;
        },
        set() {
          return logReadViolation(context, `${path}.set`);
        },
        defineProperty() {
          return logReadViolation(context, `${path}.defineProperty`);
        },
        deleteProperty() {
          return logReadViolation(context, `${path}.deleteProperty`);
        }
      });
      cache.set(value, proxy);
      return proxy;
    }
    if (value instanceof Set) {
      const proxy = new Proxy(value, {
        get(targetSet, prop) {
          if (prop === "add" || prop === "delete" || prop === "clear") {
            return () => logReadViolation(context, `${path}.${String(prop)}`);
          }
          if (prop === "forEach") {
            return (fn, thisArg) =>
              targetSet.forEach((val) => fn.call(thisArg, wrap(val, `${path}.forEach`), val, proxy));
          }
          if (prop === "values" || prop === "keys") {
            return () => wrapIterator(targetSet.values(), `${path}.values`);
          }
          if (prop === Symbol.iterator) {
            return () => wrapIterator(targetSet[Symbol.iterator](), `${path}.iterator`);
          }
          const raw = targetSet[prop];
          return typeof raw === "function" ? raw.bind(targetSet) : raw;
        },
        set() {
          return logReadViolation(context, `${path}.set`);
        },
        defineProperty() {
          return logReadViolation(context, `${path}.defineProperty`);
        },
        deleteProperty() {
          return logReadViolation(context, `${path}.deleteProperty`);
        }
      });
      cache.set(value, proxy);
      return proxy;
    }
    if (Array.isArray(value)) {
      const proxy = new Proxy(value, {
        get(targetArr, prop) {
          const mutating = new Set([
            "copyWithin",
            "fill",
            "pop",
            "push",
            "reverse",
            "shift",
            "sort",
            "splice",
            "unshift"
          ]);
          if (mutating.has(prop)) {
            return () => logReadViolation(context, `${path}.${String(prop)}`);
          }
          if (prop === Symbol.iterator) {
            return function* () {
              for (let i = 0; i < targetArr.length; i += 1) {
                yield wrap(targetArr[i], `${path}[${i}]`);
              }
            };
          }
          const wrapCallback = (fn, thisArg) => (value, index) =>
            fn.call(thisArg, wrap(value, `${path}[${index}]`), index, proxy);
          if (prop === "forEach") {
            return (fn, thisArg) => targetArr.forEach(wrapCallback(fn, thisArg));
          }
          if (prop === "map") {
            return (fn, thisArg) => targetArr.map(wrapCallback(fn, thisArg));
          }
          if (prop === "filter") {
            return (fn, thisArg) => targetArr.filter(wrapCallback(fn, thisArg));
          }
          if (prop === "reduce") {
            return (fn, initial) =>
              targetArr.reduce(
                (acc, val, index) => fn(acc, wrap(val, `${path}[${index}]`), index, proxy),
                initial
              );
          }
          if (prop === "reduceRight") {
            return (fn, initial) =>
              targetArr.reduceRight(
                (acc, val, index) => fn(acc, wrap(val, `${path}[${index}]`), index, proxy),
                initial
              );
          }
          if (prop === "some") {
            return (fn, thisArg) => targetArr.some(wrapCallback(fn, thisArg));
          }
          if (prop === "every") {
            return (fn, thisArg) => targetArr.every(wrapCallback(fn, thisArg));
          }
          if (prop === "find") {
            return (fn, thisArg) => targetArr.find(wrapCallback(fn, thisArg));
          }
          if (prop === "findIndex") {
            return (fn, thisArg) => targetArr.findIndex(wrapCallback(fn, thisArg));
          }
          if (prop === "entries") {
            return () =>
              wrapIterator(
                targetArr.map((val, index) => [index, val])[Symbol.iterator](),
                `${path}.entries`
              );
          }
          if (prop === "values") {
            return () => wrapIterator(targetArr.values(), `${path}.values`);
          }
          const raw = targetArr[prop];
          if (typeof raw === "function") {
            return raw.bind(targetArr);
          }
          return wrap(raw, `${path}.${String(prop)}`);
        },
        set() {
          return logReadViolation(context, `${path}.set`);
        },
        defineProperty() {
          return logReadViolation(context, `${path}.defineProperty`);
        },
        deleteProperty() {
          return logReadViolation(context, `${path}.deleteProperty`);
        }
      });
      cache.set(value, proxy);
      return proxy;
    }
    const proxy = new Proxy(value, {
      get(targetObj, prop) {
        const raw = targetObj[prop];
        if (typeof raw === "function") {
          return raw.bind(targetObj);
        }
        return wrap(raw, `${path}.${String(prop)}`);
      },
      set() {
        return logReadViolation(context, `${path}.set`);
      },
      defineProperty() {
        return logReadViolation(context, `${path}.defineProperty`);
      },
      deleteProperty() {
        return logReadViolation(context, `${path}.deleteProperty`);
      }
    });
    cache.set(value, proxy);
    return proxy;
  };
  return wrap(target, "state");
};

const cloneAggregateMap = (map) => {
  const cloned = new Map();
  map.forEach((value, key) => {
    cloned.set(key, value && typeof value === "object" ? { ...value } : value);
  });
  return cloned;
};

const cloneProjectionSnapshot = () => ({
  tokens: JSON.parse(JSON.stringify(projectionState.tokens)),
  aggregates: {
    impressions: cloneAggregateMap(projectionState.aggregates.impressions),
    intents: cloneAggregateMap(projectionState.aggregates.intents),
    resolvedIntents: cloneAggregateMap(projectionState.aggregates.resolvedIntents),
    resolvedValueSum: cloneAggregateMap(projectionState.aggregates.resolvedValueSum),
    partialResolutions: cloneAggregateMap(projectionState.aggregates.partialResolutions),
    weightedResolvedValueSum: cloneAggregateMap(projectionState.aggregates.weightedResolvedValueSum),
    billableResolutions: cloneAggregateMap(projectionState.aggregates.billableResolutions),
    nonBillableResolutions: cloneAggregateMap(projectionState.aggregates.nonBillableResolutions)
  },
  budgets: cloneAggregateMap(projectionState.budgets),
  ledger: projectionState.ledger.map((entry) => ({ ...entry }))
});

const restoreProjectionSnapshot = (snapshot) => {
  projectionState.tokens = snapshot.tokens;
  projectionState.aggregates = snapshot.aggregates;
  projectionState.budgets = snapshot.budgets;
  projectionState.ledger = snapshot.ledger;
  tokenIndex = new Map(projectionState.tokens.map((token) => [token.token_id, token]));
  ledgerIndex = new Set(
    projectionState.ledger
      .filter((entry) => entry.token_id && entry.final_stage)
      .map((entry) => ledgerKey(entry.token_id, entry.final_stage))
  );
};

const loadEventIndex = () => {
  const payload = readJsonFile(eventIndexFile);
  if (!payload) {
    eventIdIndex = new Set();
    return false;
  }
  if (!Array.isArray(payload.event_ids)) {
    console.error("event_index.load.failed", { path: eventIndexFile });
    process.exit(1);
  }
  eventIdIndex = new Set(payload.event_ids);
  return true;
};

const persistEventIndex = () => {
  writeJsonFile(eventIndexFile, {
    version: EVENT_INDEX_VERSION,
    event_ids: Array.from(eventIdIndex),
    updated_at: new Date().toISOString()
  });
  console.log("event.index.persist", { count: eventIdIndex.size });
};

const rebuildEventIndex = () => {
  const rebuilt = new Set();
  events.forEach((event) => {
    if (event.event_id) {
      rebuilt.add(event.event_id);
    }
  });
  eventIdIndex = rebuilt;
  console.log("event.index.rebuild", { count: eventIdIndex.size });
  persistEventIndex();
};

const appendMutex = createBlockingMutex();
const projectionMutex = createBlockingMutex();

const withProjectionRead = (context, fn) => {
  if (!acquireBlockingMutex(projectionMutex, null)) {
    console.log("projection.read.lock.timeout", { context });
    throw new Error("projection.read.lock.timeout");
  }
  try {
    return fn();
  } finally {
    releaseBlockingMutex(projectionMutex, null);
  }
};

let projectionWriteActive = false;
const assertProjectionWrite = (context) => {
  if (projectionWriteActive) {
    return;
  }
  console.log("projection.violation", { context });
  throw new Error("projection.violation");
};

const withProjectionWrite = (context, fn) => {
  projectionWriteActive = true;
  try {
    return fn();
  } finally {
    projectionWriteActive = false;
  }
};

const getCampaignBudget = (campaignId) => projectionState.budgets.get(campaignId);

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
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("applyBudgetCharge");
  const budget = getCampaignBudget(campaignId);
  if (!budget) {
    console.log("invariant.violation", { reason: "campaign_budget_missing", campaign_id: campaignId });
    return;
  }
  const charge = Number.isFinite(amount) ? amount : 0;
  budget.remaining = budget.remaining - charge;
  saveBudgets();
  console.log("budget.update", {
    campaign_id: campaignId,
    total: budget.total,
    remaining: budget.remaining,
    charge
  });
};

const resetAggregationWindow = (reason) => {
  // PROHIBITED: do not mutate state here outside projections.
  const nowMs = Date.now();
  const newWindow = { started_at_ms: nowMs, started_at: new Date(nowMs).toISOString() };
  const appended = appendEventBatch(
    [
      {
        type: "window.reset",
        payload: {
          reason,
          previous_window: {
            started_at: aggregationWindow.started_at,
            started_at_ms: aggregationWindow.started_at_ms,
            window_ms: AGGREGATION_WINDOW_MS
          },
          new_window: newWindow
        }
      }
    ],
    "window.reset"
  );
  if (!appended) {
    return;
  }
  applyProjectionEvents(appended, "window.reset");
};

const ensureWindowFresh = () => {
  if (projectionWriteActive) {
    console.log("projection.violation", { context: "ensureWindowFresh" });
    throw new Error("projection.violation");
  }
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
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("addResolutionEvent");
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

const loadSchemaRegistry = (defaults) => {
  const payload = readJsonFile(schemasFile);
  if (!payload) {
    console.log("schema.load.skipped", { path: schemasFile });
    return { ...defaults };
  }
  if (!isPlainObject(payload)) {
    console.error("schema.load.failed", { path: schemasFile, reason: "invalid_format" });
    return { ...defaults };
  }
  const registry = { ...defaults };
  Object.keys(defaults).forEach((key) => {
    if (payload[key]) {
      registry[key] = payload[key];
    }
  });
  console.log("schema.load.ok", { path: schemasFile, version: payload.version ?? null });
  return registry;
};

schemaRegistry = loadSchemaRegistry(schemaDefaults);

const writeJsonFile = (filePath, payload) => {
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`);
};

const loadDeliveryState = () => {
  const payload = readJsonFile(deliveryStateFile);
  if (!payload) {
    deliveryState = { last_delivered_seq: 0, last_attempt_at: null, retry_count: 0 };
    writeJsonFile(deliveryStateFile, {
      last_delivered_seq: 0,
      last_attempt_at: null,
      retry_count: 0,
      updated_at: new Date().toISOString()
    });
    return;
  }
  const schemaErrors = validateSchema(schemaRegistry.delivery_state, payload, "$");
  if (schemaErrors.length > 0 || !Number.isFinite(payload.last_delivered_seq)) {
    logSchemaErrors("delivery_state", schemaErrors.length ? schemaErrors : ["last_delivered_seq.invalid"], {
      path: deliveryStateFile
    });
    deliveryState = { last_delivered_seq: 0, last_attempt_at: null, retry_count: 0 };
    writeJsonFile(deliveryStateFile, {
      last_delivered_seq: 0,
      last_attempt_at: null,
      retry_count: 0,
      updated_at: new Date().toISOString()
    });
    return;
  }
  deliveryState = {
    last_delivered_seq: payload.last_delivered_seq,
    last_attempt_at: payload.last_attempt_at ?? null,
    retry_count: Number.isFinite(payload.retry_count) ? payload.retry_count : 0
  };
};

const saveDeliveryState = () => {
  writeJsonFile(deliveryStateFile, {
    last_delivered_seq: deliveryState.last_delivered_seq,
    last_attempt_at: deliveryState.last_attempt_at,
    retry_count: deliveryState.retry_count,
    updated_at: new Date().toISOString()
  });
};

const loadDeliveryDlqStats = () => {
  if (!fs.existsSync(deliveryDlqFile)) {
    return { count: 0, last_entry: null };
  }
  const raw = fs.readFileSync(deliveryDlqFile, "utf8");
  if (!raw.trim()) {
    return { count: 0, last_entry: null };
  }
  const lines = raw.split("\n").filter((line) => line.trim().length > 0);
  let lastEntry = null;
  if (lines.length > 0) {
    try {
      const parsed = JSON.parse(lines[lines.length - 1]);
      const schemaErrors = validateSchema(schemaRegistry.dlq_entry, parsed, "$");
      if (schemaErrors.length > 0) {
        logSchemaErrors("delivery_dlq", schemaErrors, { path: deliveryDlqFile });
      } else {
        lastEntry = {
          failed_at: parsed.failed_at,
          seq: parsed.seq,
          event_id: parsed.event_id,
          status: parsed.status,
          error: parsed.error
        };
      }
    } catch (error) {
      logSchemaErrors("delivery_dlq", ["last_entry.parse_failed"], { path: deliveryDlqFile });
    }
  }
  return { count: lines.length, last_entry: lastEntry };
};

const getLastEventSeq = () => (events.length > 0 ? events[events.length - 1].seq : 0);

const loadInvoiceDrafts = () => {
  if (!fs.existsSync(invoicesDir)) {
    return [];
  }
  const files = fs.readdirSync(invoicesDir).filter((name) => name.endsWith(".json"));
  const drafts = [];
  files.forEach((filename) => {
    const filePath = path.join(invoicesDir, filename);
    try {
      const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));
      drafts.push({
        filename,
        advertiser_id: payload.advertiser_id || "unknown",
        payout_cents: Number.isFinite(payload.payout_cents) ? payload.payout_cents : 0,
        entry_count: Number.isFinite(payload.entry_count) ? payload.entry_count : 0,
        created_at: payload.created_at || null
      });
    } catch {
      drafts.push({
        filename,
        advertiser_id: "unknown",
        payout_cents: 0,
        entry_count: 0,
        created_at: null
      });
    }
  });
  return drafts.sort((a, b) => (b.created_at || "").localeCompare(a.created_at || ""));
};

const loadPayoutRuns = () => {
  if (!fs.existsSync(payoutsFile)) {
    payoutRuns = [];
    return;
  }
  const payload = readJsonFile(payoutsFile);
  if (!payload || !Array.isArray(payload.runs)) {
    console.error("payouts.load.failed", { path: payoutsFile, reason: "invalid_payload" });
    payoutRuns = [];
    return;
  }
  const valid = payload.runs.filter((run) => {
    const errors = validateSchema(schemaRegistry.payout_run, run, "$");
    if (errors.length > 0) {
      console.error("payouts.schema.invalid", { errors, run_id: run.run_id });
      return false;
    }
    return true;
  });
  payoutRuns = valid.sort((a, b) => (b.created_at || "").localeCompare(a.created_at || ""));
};

const resolveDeliveryIndex = () => {
  if (!events.length) {
    deliveryCursorIndex = 0;
    return;
  }
  const idx = events.findIndex((event) => event.seq > deliveryState.last_delivered_seq);
  deliveryCursorIndex = idx === -1 ? events.length : idx;
};

const nextDeliverableEvent = () => {
  for (let i = deliveryCursorIndex; i < events.length; i += 1) {
    const event = events[i];
    if (event.seq <= deliveryState.last_delivered_seq) {
      deliveryCursorIndex = i + 1;
      continue;
    }
    if (event.type === "resolution.final") {
      deliveryCursorIndex = i;
      return event;
    }
  }
  deliveryCursorIndex = events.length;
  return null;
};

const signWebhookPayload = (body) => {
  if (!WEBHOOK_SECRET) {
    return null;
  }
  return createHmac("sha256", WEBHOOK_SECRET).update(body).digest("hex");
};

const postWebhook = async (payload) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);
  const body = JSON.stringify(payload);
  const signature = signWebhookPayload(body);
  const headers = { "content-type": "application/json" };
  if (signature) {
    headers["x-flyback-signature"] = signature;
  }
  headers["x-flyback-schema-version"] = String(payload.schema_version || DELIVERY_SCHEMA_VERSION);
  try {
    const response = await fetch(WEBHOOK_URL, {
      method: "POST",
      headers,
      body,
      signal: controller.signal
    });
    return { ok: response.ok, status: response.status };
  } catch (error) {
    return { ok: false, status: 0, error: error.message };
  } finally {
    clearTimeout(timeout);
  }
};

const appendDeliveryDlq = (entry) => {
  const line = `${JSON.stringify(entry)}\n`;
  fs.appendFileSync(deliveryDlqFile, line);
};

const scheduleDeliveryTick = (delayMs = 250) => {
  setTimeout(() => {
    void processWebhookDelivery();
  }, delayMs);
};

const processWebhookDelivery = async () => {
  if (!WEBHOOK_URL) {
    return;
  }
  if (!WRITE_ENABLED) {
    return;
  }
  if (process.env.REBUILD_FROM_EVENTS === "true") {
    console.log("webhook.delivery.skip", { reason: "rebuild_mode" });
    return;
  }
  if (deliveryInFlight) {
    return;
  }
  if (Date.now() < deliveryNextAttemptAt) {
    scheduleDeliveryTick(250);
    return;
  }
  const nextEvent = nextDeliverableEvent();
  if (!nextEvent) {
    scheduleDeliveryTick(500);
    return;
  }
  deliveryInFlight = true;
  const payload = {
    schema_version: DELIVERY_SCHEMA_VERSION,
    delivery_ts: new Date().toISOString(),
    seq: nextEvent.seq,
    event_id: nextEvent.event_id,
    type: nextEvent.type,
    ts: nextEvent.ts,
    payload: nextEvent.payload
  };
  const result = await postWebhook(payload);
  deliveryState.last_attempt_at = new Date().toISOString();
  if (result.ok) {
    deliveryState.last_delivered_seq = nextEvent.seq;
    deliveryState.retry_count = 0;
    saveDeliveryState();
    deliveryRetryCount = 0;
    deliveryNextAttemptAt = 0;
    console.log("webhook.delivery.success", {
      seq: nextEvent.seq,
      event_id: nextEvent.event_id,
      status: result.status
    });
    deliveryCursorIndex += 1;
  } else {
    deliveryRetryCount += 1;
    deliveryState.retry_count = deliveryRetryCount;
    const delay = Math.min(WEBHOOK_RETRY_BASE_MS * 2 ** (deliveryRetryCount - 1), WEBHOOK_RETRY_MAX_MS);
    deliveryNextAttemptAt = Date.now() + delay;
    console.log("webhook.delivery.fail", {
      seq: nextEvent.seq,
      event_id: nextEvent.event_id,
      status: result.status || 0,
      error: result.error || null,
      retry_in_ms: delay
    });
    if (deliveryRetryCount >= WEBHOOK_MAX_RETRIES) {
      const dlqEntry = {
        failed_at: new Date().toISOString(),
        seq: nextEvent.seq,
        event_id: nextEvent.event_id,
        status: result.status || 0,
        error: result.error || null,
        payload
      };
      appendDeliveryDlq(dlqEntry);
      console.log("webhook.delivery.dlq", { seq: nextEvent.seq, event_id: nextEvent.event_id });
      deliveryState.last_delivered_seq = nextEvent.seq;
      deliveryState.retry_count = 0;
      saveDeliveryState();
      deliveryRetryCount = 0;
      deliveryNextAttemptAt = 0;
      deliveryCursorIndex += 1;
    }
  }
  deliveryInFlight = false;
  scheduleDeliveryTick(0);
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
      if (policy.rev_share_bps !== undefined && !Number.isFinite(policy.rev_share_bps)) {
        errors.push(`policy_rev_share_invalid:${publisherId}`);
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

const logSchemaErrors = (label, errors, details = {}) => {
  console.error("schema.validate.failed", { schema: label, schema_version: SCHEMA_VERSION, errors, ...details });
};

const validateEventEntry = (entry, context = "event.validate") => {
  const errors = validateSchema(schemaRegistry.event, entry, "$");
  if (!errors.length) {
    const payloadSchema = schemaRegistry.event_payloads[entry.type];
    if (!payloadSchema) {
      errors.push(`$.type.unknown:${entry.type}`);
    } else {
      errors.push(...validateSchema(payloadSchema, entry.payload, "$.payload"));
    }
  }
  if (errors.length > 0) {
    console.log("event.schema.invalid", {
      context,
      type: entry.type,
      seq: entry.seq,
      errors
    });
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
  const schemaErrors = validateSchema(schemaRegistry.registry, migrated, "$");
  if (schemaErrors.length > 0) {
    logSchemaErrors("registry", schemaErrors, { path: registryFile });
    process.exit(1);
  }
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
    if (campaign.publisher_rev_share_bps !== undefined && !Number.isFinite(campaign.publisher_rev_share_bps)) {
      console.error("registry.validate.failed", {
        errors: ["campaign_rev_share_invalid"],
        campaign_id: campaign.campaign_id
      });
      process.exit(1);
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
  withProjectionWrite("loadBudgets", () => {
    projectionState.budgets = mapFromEntries(migrated.campaigns, (entry) => entry.campaign_id);
  });
  console.log("budgets.load.ok", { campaigns: projectionState.budgets.size });
};

const saveBudgets = () => {
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("saveBudgets");
  writeJsonFile(budgetsFile, { version: BUDGETS_VERSION, campaigns: mapToEntries(projectionState.budgets) });
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
    withProjectionWrite("loadAggregates.init", () => {
      saveAggregates();
    });
    return;
  }
  const version = typeof payload.version === "number" ? payload.version : 0;
  const migrated = migrateByVersion(payload, version, AGGREGATES_VERSION, aggregatesMigrations, "aggregates");
  withProjectionWrite("loadAggregates", () => {
    aggregationWindow = migrated.window || aggregationWindow;
    lastWindowSnapshot = migrated.last_window || null;
    const current = migrated.current || emptyAggregateWindow();
    projectionState.aggregates = {
      impressions: mapFromEntries(current.impressions, aggregateKey),
      intents: mapFromEntries(current.intents, aggregateKey),
      resolvedIntents: mapFromEntries(current.resolved_intents, aggregateKey),
      resolvedValueSum: mapFromEntries(current.resolved_value_sum, aggregateKey),
      partialResolutions: mapFromEntries(current.partial_resolutions, aggregateKey),
      weightedResolvedValueSum: mapFromEntries(current.weighted_resolved_value_sum, aggregateKey),
      billableResolutions: mapFromEntries(current.billable_resolutions, aggregateKey),
      nonBillableResolutions: mapFromEntries(current.non_billable_resolutions, aggregateKey)
    };
  });
  console.log("aggregates.load.ok", {
    window_start: aggregationWindow.started_at,
    impressions: projectionState.aggregates.impressions.size
  });
};

const saveAggregates = (lastWindowSnapshotOverride = null) => {
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("saveAggregates");
  const payload = {
    version: AGGREGATES_VERSION,
    window: {
      started_at: aggregationWindow.started_at,
      started_at_ms: aggregationWindow.started_at_ms,
      window_ms: AGGREGATION_WINDOW_MS
    },
    current: {
      impressions: mapToEntries(projectionState.aggregates.impressions),
      intents: mapToEntries(projectionState.aggregates.intents),
      resolved_intents: mapToEntries(projectionState.aggregates.resolvedIntents),
      resolved_value_sum: mapToEntries(projectionState.aggregates.resolvedValueSum),
      partial_resolutions: mapToEntries(projectionState.aggregates.partialResolutions),
      weighted_resolved_value_sum: mapToEntries(projectionState.aggregates.weightedResolvedValueSum),
      billable_resolutions: mapToEntries(projectionState.aggregates.billableResolutions),
      non_billable_resolutions: mapToEntries(projectionState.aggregates.nonBillableResolutions)
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
  const schemaErrors = validateSchema(schemaRegistry.keys, migrated, "$");
  if (schemaErrors.length > 0) {
    logSchemaErrors("keys", schemaErrors, { path: keysFile });
    process.exit(1);
  }
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
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("bumpAggregate");
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
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("bumpResolvedValueSum");
  const key = aggregateKey(scope);
  const current = projectionState.aggregates.resolvedValueSum.get(key) || {
    campaign_id: scope.campaign_id,
    publisher_id: scope.publisher_id,
    creative_id: scope.creative_id,
    sum: 0
  };
  const numericValue = Number.isFinite(value) ? value : 0;
  current.sum += numericValue;
  projectionState.aggregates.resolvedValueSum.set(key, current);
  saveAggregates();
  return current.sum;
};

const bumpWeightedResolvedValueSum = (scope, value) => {
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("bumpWeightedResolvedValueSum");
  const key = aggregateKey(scope);
  const current = projectionState.aggregates.weightedResolvedValueSum.get(key) || {
    campaign_id: scope.campaign_id,
    publisher_id: scope.publisher_id,
    creative_id: scope.creative_id,
    sum: 0
  };
  const numericValue = Number.isFinite(value) ? value : 0;
  current.sum += numericValue;
  projectionState.aggregates.weightedResolvedValueSum.set(key, current);
  saveAggregates();
  return current.sum;
};

const bumpResolutionCount = (aggregateMap, scope) => {
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("bumpResolutionCount");
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
  // PROHIBITED: do not mutate token state here; emit events only.
  // PROHIBITED: do not mutate token state here; emit events only.
  ensureWindowFresh();
  const key = aggregateKey(scope);
  const impressions = getAggregateCount(projectionState.aggregates.impressions, scope);
  const intents = getAggregateCount(projectionState.aggregates.intents, scope);
  const resolvedIntents = getAggregateCount(projectionState.aggregates.resolvedIntents, scope);
  const resolvedValueSum = projectionState.aggregates.resolvedValueSum.get(key)?.sum || 0;
  const weightedEntry = projectionState.aggregates.weightedResolvedValueSum.get(key);
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
    partial_resolutions: getAggregateCount(projectionState.aggregates.partialResolutions, scope)
  };
};

const deriveMetricsReadOnly = (state, scope) => {
  const key = aggregateKey(scope);
  const impressions = getAggregateCount(state.aggregates.impressions, scope);
  const intents = getAggregateCount(state.aggregates.intents, scope);
  const resolvedIntents = getAggregateCount(state.aggregates.resolvedIntents, scope);
  const resolvedValueSum = state.aggregates.resolvedValueSum.get(key)?.sum || 0;
  const weightedEntry = state.aggregates.weightedResolvedValueSum.get(key);
  const weightedResolvedValueSum = weightedEntry?.sum || 0;
  const weightedPresent = Boolean(weightedEntry);
  const intentRate = impressions > 0 ? intents / impressions : 0;
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
    partial_resolutions: getAggregateCount(state.aggregates.partialResolutions, scope)
  };
};

const getBudgetStatusFromState = (state, campaignId) => {
  const budget = state.budgets.get(campaignId);
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

const getCapStatusFromState = (state, campaignId) => {
  const caps = getCampaignCaps(campaignId);
  let billableCount = 0;
  let billableWeightedValue = 0;
  state.tokens.forEach((token) => {
    if (!token.scope || token.scope.campaign_id !== campaignId) {
      return;
    }
    if (token.billable !== true) {
      return;
    }
    const finalEvent = getFinalResolutionEvent(token);
    if (!finalEvent) {
      return;
    }
    const resolvedValue = Number.isFinite(finalEvent.resolved_value) ? finalEvent.resolved_value : 0;
    const weight = resolveOutcomeWeight(campaignId, finalEvent.outcome_type);
    billableCount += 1;
    billableWeightedValue += resolvedValue * weight;
  });
  const countRatio = caps.max_outcomes ? billableCount / caps.max_outcomes : 0;
  const valueRatio = caps.max_weighted_value ? billableWeightedValue / caps.max_weighted_value : 0;
  const ratio = Math.max(countRatio, valueRatio);
  const exhausted =
    (caps.max_outcomes !== null && billableCount >= caps.max_outcomes) ||
    (caps.max_weighted_value !== null && billableWeightedValue >= caps.max_weighted_value);
  const near = ratio >= CAP_DEPRIORITIZE_THRESHOLD && !exhausted;
  return {
    caps,
    state: { billable_count: billableCount, billable_weighted_value: billableWeightedValue },
    exhausted,
    near_exhaustion: near,
    ratio
  };
};

const getLedgerStatsFromState = (state, publisherId) => {
  const windowId = lastWindowSnapshot?.window?.started_at || null;
  const lifetimeEntries = state.ledger.filter((entry) => entry.publisher_id === publisherId);
  const windowEntries = windowId ? lifetimeEntries.filter((entry) => entry.window_id === windowId) : [];
  const sum = (entries) =>
    entries.reduce((total, entry) => total + (Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0), 0);
  return {
    window_id: windowId,
    window_payout_cents_estimate: sum(windowEntries),
    lifetime_payout_cents_estimate: sum(lifetimeEntries),
    window_entry_count: windowEntries.length,
    lifetime_entry_count: lifetimeEntries.length
  };
};

const getLedgerEntriesFromState = (state, publisherId, limit = 10) => {
  if (!publisherId) {
    return [];
  }
  return state.ledger
    .filter((entry) => entry.publisher_id === publisherId)
    .filter((entry) => entry && entry.billable === true)
    .slice()
    .sort((a, b) => (b.payout_cents || 0) - (a.payout_cents || 0))
    .slice(0, limit)
    .map((entry) => ({ ...entry }));
};

const getPayoutReconciliationFromState = (state, publisherId) => {
  if (!publisherId) {
    return null;
  }
  const ledgerById = new Map();
  state.ledger.forEach((entry) => {
    if (!entry || entry.billable !== true) {
      return;
    }
    if (entry.publisher_id !== publisherId || !entry.entry_id) {
      return;
    }
    ledgerById.set(entry.entry_id, entry);
  });
  const runs = payoutRuns.filter((run) => run.publisher_id === publisherId);
  let mismatches = 0;
  let missingEntries = 0;
  const runEntryIds = new Set();
  runs.forEach((run) => {
    if (!Array.isArray(run.entry_ids)) {
      return;
    }
    let expected = 0;
    let missing = 0;
    run.entry_ids.forEach((entryId) => {
      runEntryIds.add(entryId);
      const entry = ledgerById.get(entryId);
      if (!entry) {
        missing += 1;
        return;
      }
      expected += Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0;
    });
    const actual = Number.isFinite(run.payout_cents) ? run.payout_cents : 0;
    if (missing > 0 || expected !== actual) {
      mismatches += 1;
      missingEntries += missing;
    }
  });
  let unassigned = 0;
  ledgerById.forEach((_entry, entryId) => {
    if (!runEntryIds.has(entryId)) {
      unassigned += 1;
    }
  });
  return {
    status: mismatches > 0 || unassigned > 0 ? "mismatch" : "ok",
    runs_checked: runs.length,
    mismatches,
    missing_entries: missingEntries,
    unassigned_entries: unassigned,
    checked_at: new Date().toISOString()
  };
};

const getPayoutRunLinksFromState = (state, publisherId) => {
  if (!publisherId) {
    return [];
  }
  const ledgerById = new Map();
  state.ledger.forEach((entry) => {
    if (!entry || entry.billable !== true) {
      return;
    }
    if (!entry.entry_id || entry.publisher_id !== publisherId) {
      return;
    }
    ledgerById.set(entry.entry_id, entry);
  });
  const links = new Map();
  payoutRuns
    .filter((run) => run.publisher_id === publisherId)
    .forEach((run) => {
      if (!Array.isArray(run.entry_ids)) {
        return;
      }
      const advertisers = new Set();
      run.entry_ids.forEach((entryId) => {
        const entry = ledgerById.get(entryId);
        if (entry && entry.advertiser_id) {
          advertisers.add(entry.advertiser_id);
        }
      });
      advertisers.forEach((advertiserId) => {
        const current = links.get(advertiserId) || {
          advertiser_id: advertiserId,
          run_ids: [],
          payout_cents: 0,
          run_count: 0
        };
        current.run_ids.push(run.run_id);
        current.run_count += 1;
        current.payout_cents += Number.isFinite(run.payout_cents) ? run.payout_cents : 0;
        links.set(advertiserId, current);
      });
    });
  return Array.from(links.values()).sort((a, b) => b.payout_cents - a.payout_cents);
};

const getPublisherStatementFromState = (state, publisherId) => {
  if (!publisherId) {
    return null;
  }
  const ledgerEntries = state.ledger.filter(
    (entry) => entry.billable === true && entry.publisher_id === publisherId
  );
  const ledgerPayout = ledgerEntries.reduce(
    (sum, entry) => sum + (Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0),
    0
  );
  const runs = payoutRuns.filter((run) => run.publisher_id === publisherId);
  const totals = runs.reduce(
    (acc, run) => {
      acc.payout_runs += 1;
      const cents = Number.isFinite(run.payout_cents) ? run.payout_cents : 0;
      if (run.status === "sent") {
        acc.payout_sent_cents += cents;
      } else if (run.status === "settled") {
        acc.payout_settled_cents += cents;
      } else {
        acc.payout_pending_cents += cents;
      }
      if (!acc.last_run_at || String(run.created_at || "") > String(acc.last_run_at || "")) {
        acc.last_run_at = run.created_at || acc.last_run_at;
      }
      return acc;
    },
    {
      payout_runs: 0,
      payout_pending_cents: 0,
      payout_sent_cents: 0,
      payout_settled_cents: 0,
      last_run_at: null
    }
  );
  return {
    publisher_id: publisherId,
    ledger_entries: ledgerEntries.length,
    ledger_payout_cents: ledgerPayout,
    payout_runs: totals.payout_runs,
    payout_pending_cents: totals.payout_pending_cents,
    payout_sent_cents: totals.payout_sent_cents,
    payout_settled_cents: totals.payout_settled_cents,
    last_run_at: totals.last_run_at
  };
};

const getSelectionView = (state, publisherId, size) => {
  const readOnlyState = createReadOnlyProxy(state, "getSelectionView");
  const publisher = findPublisher(publisherId);
  const publisherPolicy = resolvePublisherPolicy(publisherId);
  const campaigns = publisher ? resolveCampaignsForPublisher(publisher) : [];
  const requestedSize = size || "300x250";
  const invalidCreatives = [];
  const candidates = campaigns.flatMap((campaign) =>
    resolveCreativesForCampaign(campaign)
      .filter((creative) => creative.sizes.includes(requestedSize))
      .filter((creative) => {
        if (!isNonEmptyString(creative.demand_type)) {
          invalidCreatives.push({ reason: "creative_missing_demand_type", creative_id: creative.creative_id });
          return false;
        }
        if (!isNonEmptyString(creative.creative_url)) {
          invalidCreatives.push({ reason: "creative_missing_url", creative_id: creative.creative_id });
          return false;
        }
        return true;
      })
      .map((creative) => {
        const scope = {
          campaign_id: campaign.campaign_id,
          publisher_id: publisherId,
          creative_id: creative.creative_id
        };
        return {
          scope,
          creative_id: creative.creative_id,
          creative_url: creative.creative_url,
          demand_type: creative.demand_type,
          metrics: deriveMetricsReadOnly(readOnlyState, scope),
          budget_status: getBudgetStatusFromState(readOnlyState, campaign.campaign_id),
          cap_status: getCapStatusFromState(readOnlyState, campaign.campaign_id)
        };
      })
  );
  const fallbackCampaign = campaigns[0] || { campaign_id: "campaign-v1" };
  const fallbackCreative = creativeRegistry[0] || null;
  const fallbackCandidate = fallbackCreative
    ? {
        scope: {
          campaign_id: fallbackCampaign.campaign_id,
          publisher_id: publisherId,
          creative_id: fallbackCreative.creative_id
        },
        creative_id: fallbackCreative.creative_id,
        creative_url: fallbackCreative.creative_url,
        demand_type: fallbackCreative.demand_type
      }
    : null;
  return {
    publisher_id: publisherId,
    publisher_policy: publisherPolicy,
    requested_size: requestedSize,
    campaigns,
    candidates,
    invalid_creatives: invalidCreatives,
    fallback_candidate: fallbackCandidate,
    window: {
      started_at: aggregationWindow.started_at,
      window_ms: AGGREGATION_WINDOW_MS
    }
  };
};

const getReportingView = (state, publisherId) => {
  const readOnlyState = createReadOnlyProxy(state, "getReportingView");
  const keys = new Set();
  [
    readOnlyState.aggregates.impressions,
    readOnlyState.aggregates.intents,
    readOnlyState.aggregates.resolvedIntents,
    readOnlyState.aggregates.resolvedValueSum
  ].forEach((map) => {
    for (const key of map.keys()) {
      keys.add(key);
    }
  });

  const rows = [];
  for (const key of keys) {
    const [campaignId, rowPublisherId, creativeId] = key.split(":");
    if (publisherId && rowPublisherId !== publisherId) {
      continue;
    }
    const scope = { campaign_id: campaignId, publisher_id: rowPublisherId, creative_id: creativeId };
    const metrics = deriveMetricsReadOnly(readOnlyState, scope);
    rows.push({
      campaign_id: campaignId,
      publisher_id: rowPublisherId,
      creative_id: creativeId,
      impressions: metrics.impressions,
      intents: metrics.intents,
      resolvedIntents: metrics.resolvedIntents,
      intent_rate: metrics.intent_rate,
      resolution_rate: metrics.resolution_rate,
      derived_value_per_1k: metrics.derived_value_per_1k
    });
  }

  return {
    aggregates: rows,
    publisher_floor: publisherId ? getPublisherFloorConfig(publisherId) : null,
    publisher_policy: publisherId ? resolvePublisherPolicy(publisherId) : null,
    publisher_statement: publisherId ? getPublisherStatementFromState(readOnlyState, publisherId) : null,
    last_window_observed: publisherId ? getLastWindowObserved(publisherId) : null,
    publisher_caps: publisherId ? getPublisherCapConfig(publisherId) : null,
    last_window_billable: publisherId ? getLastWindowBillableCounts(publisherId) : null,
    payout_reconciliation: publisherId ? getPayoutReconciliationFromState(readOnlyState, publisherId) : null,
    payout_runs: publisherId ? payoutRuns.filter((run) => run.publisher_id === publisherId).slice(0, 10) : null,
    payout_run_links: publisherId ? getPayoutRunLinksFromState(readOnlyState, publisherId) : null,
    ledger_stats: publisherId ? getLedgerStatsFromState(readOnlyState, publisherId) : null,
    ledger_entries: publisherId ? getLedgerEntriesFromState(readOnlyState, publisherId, 10) : null,
    selection_decisions: publisherId ? getSelectionHistory(publisherId, 50) : null,
    delivery_health: publisherId
      ? (() => {
          const lastEventSeq = getLastEventSeq();
          const lag = Math.max(0, lastEventSeq - deliveryState.last_delivered_seq);
          return {
            last_delivered_seq: deliveryState.last_delivered_seq,
            last_attempt_at: deliveryState.last_attempt_at,
            retry_count: deliveryState.retry_count,
            last_event_seq: lastEventSeq,
            delivery_lag: lag,
            dlq: loadDeliveryDlqStats()
          };
        })()
      : null,
    invoice_drafts: loadInvoiceDrafts(),
    system_status: {
      role: PROCESS_ROLE,
      write_enabled: WRITE_ENABLED,
      webhook_enabled: Boolean(WEBHOOK_URL),
      webhook_signature_enabled: Boolean(WEBHOOK_SECRET)
    }
  };
};

const isNonEmptyString = (value) => typeof value === "string" && value.trim().length > 0;

const isValidSize = (value) => typeof value === "string" && /^\d{2,4}x\d{2,4}$/.test(value);

const isValidStage = (value) => typeof value === "string" && /^[a-z0-9_-]+$/.test(value);

const rejectRequest = (res, endpoint, reason, details = {}) => {
  console.log("contract.reject", { endpoint, reason, ...details });
  sendJson(res, 400, { error: reason });
};

const logAuth = (event, actorType, actorId, reason, requestId = null) => {
  console.log(event, {
    actor_type: actorType,
    actor_id: actorId,
    reason,
    request_id: requestId
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

const resolvePublisherFromApiKey = (apiKey) => {
  if (!apiKey) {
    return null;
  }
  return publisherKeyIndex.get(apiKey) || null;
};

const authorizePublisher = (req, res, publisherId) => {
  const providedKey = extractApiKey(req);
  const apiKey = providedKey || defaultPublisherKey;
  if (!apiKey) {
    logAuth("auth.reject", "publisher", publisherId, "missing_key", req.request_id || null);
    sendJson(res, 401, { error: "auth_required" });
    return null;
  }
  const actorId = publisherKeyIndex.get(apiKey);
  if (!actorId) {
    logAuth("auth.reject", "publisher", publisherId, "invalid_key", req.request_id || null);
    sendJson(res, 401, { error: "auth_invalid" });
    return null;
  }
  if (publisherId && actorId !== publisherId) {
    logAuth("auth.reject", "publisher", actorId, "publisher_mismatch", req.request_id || null);
    sendJson(res, 403, { error: "auth_mismatch" });
    return null;
  }
  logAuth("auth.accept", "publisher", actorId, providedKey ? "api_key" : "default_key", req.request_id || null);
  return actorId;
};

const authorizeAdvertiser = (req, res, campaignId = null) => {
  const providedKey = extractApiKey(req);
  const apiKey = providedKey || defaultAdvertiserKey;
  if (!apiKey) {
    logAuth("auth.reject", "advertiser", campaignId, "missing_key", req.request_id || null);
    sendJson(res, 401, { error: "auth_required" });
    return null;
  }
  const actorId = advertiserKeyIndex.get(apiKey);
  if (!actorId) {
    logAuth("auth.reject", "advertiser", campaignId, "invalid_key", req.request_id || null);
    sendJson(res, 401, { error: "auth_invalid" });
    return null;
  }
  logAuth("auth.accept", "advertiser", actorId, providedKey ? "api_key" : "default_key", req.request_id || null);
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

const resolveRevShareBps = (campaignId, publisherId) => {
  const campaign = resolveCampaign(campaignId);
  if (campaign && Number.isFinite(campaign.publisher_rev_share_bps)) {
    return campaign.publisher_rev_share_bps;
  }
  const policy = publisherPolicies[publisherId] || {};
  return Number.isFinite(policy.rev_share_bps) ? policy.rev_share_bps : 7000;
};

const ledgerKey = (tokenId, stage) => `${tokenId}:${stage}`;

const validateLedgerEntry = (entry) => {
  const required = [
    "entry_id",
    "created_at",
    "token_id",
    "campaign_id",
    "advertiser_id",
    "publisher_id",
    "creative_id",
    "window_id",
    "outcome_type",
    "raw_value",
    "weighted_value",
    "billable",
    "payout_cents",
    "rev_share_bps",
    "final_stage"
  ];
  for (const field of required) {
    if (!Object.prototype.hasOwnProperty.call(entry, field)) {
      return false;
    }
  }
  if (!isNonEmptyString(entry.entry_id) || !isNonEmptyString(entry.token_id)) {
    return false;
  }
  if (!isNonEmptyString(entry.final_stage) || !isNonEmptyString(entry.outcome_type)) {
    return false;
  }
  if (!Number.isFinite(entry.raw_value) || !Number.isFinite(entry.weighted_value)) {
    return false;
  }
  if (!Number.isFinite(entry.payout_cents) || !Number.isFinite(entry.rev_share_bps)) {
    return false;
  }
  if (entry.billable !== true) {
    return false;
  }
  return true;
};

const loadLedger = () => {
  const payload = readJsonFile(ledgerFile);
  if (!payload) {
    writeJsonFile(ledgerFile, { version: LEDGER_VERSION, entries: [] });
    withProjectionWrite("loadLedger.init", () => {
      projectionState.ledger = [];
      ledgerIndex = new Set();
    });
    return;
  }
  const version = typeof payload.version === "number" ? payload.version : 0;
  if (version > LEDGER_VERSION) {
    console.error("ledger.load.failed", { path: ledgerFile, reason: "version_unsupported" });
    process.exit(1);
  }
  if (!Array.isArray(payload.entries)) {
    console.error("ledger.load.failed", { path: ledgerFile });
    process.exit(1);
  }
  withProjectionWrite("loadLedger", () => {
    projectionState.ledger = payload.entries;
    ledgerIndex = new Set(
      projectionState.ledger
        .filter((entry) => entry.token_id && entry.final_stage)
        .map((entry) => ledgerKey(entry.token_id, entry.final_stage))
    );
  });
  console.log("ledger.load.ok", { entries: projectionState.ledger.length });
};

let eventState = { last_seq: 0 };
const EVENT_SNAPSHOT_INTERVAL = Number(process.env.EVENT_SNAPSHOT_INTERVAL) || 500;

const loadEventState = () => {
  const payload = readJsonFile(eventStateFile);
  if (!payload) {
    writeJsonFile(eventStateFile, { last_seq: 0 });
    eventState = { last_seq: 0 };
    return;
  }
  if (!Number.isFinite(payload.last_seq)) {
    console.error("event_state.load.failed", { path: eventStateFile });
    process.exit(1);
  }
  eventState = { last_seq: payload.last_seq };
};

const saveEventState = () => {
  writeJsonFile(eventStateFile, { last_seq: eventState.last_seq, updated_at: new Date().toISOString() });
};

const readLockInfo = (lockPath) => {
  if (!fs.existsSync(lockPath)) {
    return null;
  }
  try {
    return JSON.parse(fs.readFileSync(lockPath, "utf8"));
  } catch {
    return null;
  }
};

const acquireLock = (targetPath) => {
  const lockPath = `${targetPath}.lock`;
  const timeoutMs = Number(process.env.LOCK_TIMEOUT_MS) || 5000;
  const retryMs = Number(process.env.LOCK_RETRY_MS) || 50;
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const fd = fs.openSync(lockPath, "wx");
      const payload = { pid: process.pid, ts: new Date().toISOString() };
      fs.writeSync(fd, JSON.stringify(payload));
      fs.closeSync(fd);
      console.log("lock.acquire", { file: targetPath, owner: payload });
      return lockPath;
    } catch (error) {
      if (error.code !== "EEXIST") {
        throw error;
      }
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, retryMs);
  }
  const owner = readLockInfo(lockPath);
  console.log("lock.timeout", { file: targetPath, owner });
  return null;
};

const releaseLock = (lockPath) => {
  if (!lockPath) {
    return;
  }
  const targetPath = lockPath.replace(/\.lock$/, "");
  if (fs.existsSync(lockPath)) {
    fs.unlinkSync(lockPath);
  }
  console.log("lock.release", { file: targetPath });
};

const migrateEventsJsonToNdjson = () => {
  const legacyPath = path.join(__dirname, "data", "events.json");
  if (fs.existsSync(eventsFile) || !fs.existsSync(legacyPath)) {
    return;
  }
  const payload = readJsonFile(legacyPath);
  if (!payload || !Array.isArray(payload.entries)) {
    console.error("events.migrate.failed", { path: legacyPath });
    process.exit(1);
  }
  let seq = 0;
  const lines = payload.entries.map((entry) => {
    seq += 1;
    return JSON.stringify({
      seq,
      event_id: entry.event_id || randomUUID(),
      ts: entry.created_at || new Date().toISOString(),
      type: entry.type,
      payload: entry.payload
    });
  });
  fs.writeFileSync(eventsFile, `${lines.join("\n")}\n`);
  eventState.last_seq = seq;
  saveEventState();
  console.log("events.migrate.ok", { entries: seq });
};

const loadEvents = () => {
  loadEventState();
  migrateEventsJsonToNdjson();
  if (!fs.existsSync(eventsFile)) {
    fs.writeFileSync(eventsFile, "");
    events = [];
    return;
  }
  const raw = fs.readFileSync(eventsFile, "utf8");
  if (!raw.trim()) {
    events = [];
    return;
  }
  const lines = raw.split("\n").filter((line) => line.trim().length > 0);
  const parsed = [];
  const seenIds = new Set();
  for (let i = 0; i < lines.length; i += 1) {
    try {
      const entry = JSON.parse(lines[i]);
      if (entry.event_id && seenIds.has(entry.event_id)) {
        console.log("event.dedupe.hit", { event_id: entry.event_id, seq: entry.seq });
        continue;
      }
      if (entry.event_id) {
        seenIds.add(entry.event_id);
      }
        const entryErrors = validateEventEntry(entry, "events.load");
        if (entryErrors.length > 0) {
          logSchemaErrors("event", entryErrors, { path: eventsFile, line: i + 1 });
          process.exit(1);
        }
        parsed.push(entry);
    } catch (error) {
      if (i === lines.length - 1 && process.env.ALLOW_EVENT_TRUNCATION === "true") {
        console.log("events.truncate", { reason: "malformed_last_line" });
        const truncated = lines.slice(0, -1);
        fs.writeFileSync(eventsFile, truncated.length ? `${truncated.join("\n")}\n` : "");
        break;
      }
      console.error("events.load.failed", { path: eventsFile, line: i + 1 });
      process.exit(1);
    }
  }
    events = parsed.sort((a, b) => a.seq - b.seq);
  const lastSeq = events.length > 0 ? events[events.length - 1].seq : 0;
  if (eventState.last_seq !== lastSeq) {
    console.log("event_state.mismatch", { file_seq: lastSeq, state_seq: eventState.last_seq });
    eventState.last_seq = lastSeq;
    saveEventState();
  }
    console.log("events.load.ok", { entries: events.length });
  };

const loadProjectionState = () => {
  const payload = readJsonFile(projectionStateFile);
  if (!payload) {
    writeJsonFile(projectionStateFile, { applied_seq: 0 });
    projectionCursor = { applied_seq: 0 };
    return;
  }
  if (!Number.isFinite(payload.applied_seq)) {
    console.error("projection_state.load.failed", { path: projectionStateFile });
    process.exit(1);
  }
  projectionCursor = { applied_seq: payload.applied_seq };
};

const verifyEventIntegrity = () => {
  const lastSeq = events.length > 0 ? events[events.length - 1].seq : 0;
  if (eventState.last_seq !== lastSeq) {
    if (process.env.ALLOW_INTEGRITY_REPAIR === "true") {
      console.log("integrity.violation", { reason: "event_state_mismatch", file_seq: lastSeq, state_seq: eventState.last_seq });
      eventState.last_seq = lastSeq;
      saveEventState();
    } else {
      console.error("integrity.violation", { reason: "event_state_mismatch", file_seq: lastSeq, state_seq: eventState.last_seq });
      process.exit(1);
    }
  }
  const snapshot = readJsonFile(snapshotFile);
  if (snapshot && Number.isFinite(snapshot.snapshot_seq) && snapshot.snapshot_seq > lastSeq) {
    if (process.env.ALLOW_INTEGRITY_REPAIR === "true") {
      console.log("integrity.violation", { reason: "snapshot_ahead", snapshot_seq: snapshot.snapshot_seq, last_seq: lastSeq });
      snapshot.snapshot_seq = lastSeq;
      writeJsonFile(snapshotFile, snapshot);
    } else {
      console.error("integrity.violation", { reason: "snapshot_ahead", snapshot_seq: snapshot.snapshot_seq, last_seq: lastSeq });
      process.exit(1);
    }
  }
};

const saveProjectionState = () => {
  writeJsonFile(projectionStateFile, { applied_seq: projectionCursor.applied_seq, updated_at: new Date().toISOString() });
};

const writeSnapshot = (snapshotSeq) => {
  const payload = {
    snapshot_seq: snapshotSeq,
    window: {
      started_at: aggregationWindow.started_at,
      started_at_ms: aggregationWindow.started_at_ms,
      window_ms: AGGREGATION_WINDOW_MS
    },
    last_window: lastWindowSnapshot,
    budgets: mapToEntries(projectionState.budgets),
    aggregates: {
      impressions: mapToEntries(projectionState.aggregates.impressions),
      intents: mapToEntries(projectionState.aggregates.intents),
      resolved_intents: mapToEntries(projectionState.aggregates.resolvedIntents),
      resolved_value_sum: mapToEntries(projectionState.aggregates.resolvedValueSum),
      partial_resolutions: mapToEntries(projectionState.aggregates.partialResolutions),
      weighted_resolved_value_sum: mapToEntries(projectionState.aggregates.weightedResolvedValueSum),
      billable_resolutions: mapToEntries(projectionState.aggregates.billableResolutions),
      non_billable_resolutions: mapToEntries(projectionState.aggregates.nonBillableResolutions)
    },
    ledger: projectionState.ledger,
    tokens: projectionState.tokens
  };
  const tmpPath = `${snapshotFile}.tmp`;
  const lock = acquireLock(snapshotFile);
  if (!lock) {
    console.log("snapshot.write.failed", { reason: "lock_timeout" });
    return;
  }
  fs.writeFileSync(tmpPath, `${JSON.stringify(payload, null, 2)}\n`);
  fs.renameSync(tmpPath, snapshotFile);
  releaseLock(lock);
  console.log("snapshot.written", { snapshot_seq: snapshotSeq });
};

const appendEventBatch = (batch, context = "event.batch") => {
  if (process.env.REBUILD_FROM_EVENTS === "true") {
    console.log("write.reject.rebuild_mode", { context });
    return null;
  }
  if (!acquireBlockingMutex(appendMutex, "append")) {
    console.log("event.batch.abort", { reason: "append_lock_timeout", context });
    return null;
  }
  const eventLock = acquireLock(eventsFile);
  if (!eventLock) {
    releaseBlockingMutex(appendMutex, "append");
    console.log("event.batch.abort", { reason: "lock_timeout", context });
    return null;
  }
  const stateLock = acquireLock(eventStateFile);
  if (!stateLock) {
    releaseLock(eventLock);
    releaseBlockingMutex(appendMutex, "append");
    console.log("event.batch.abort", { reason: "state_lock_timeout", context });
    return null;
  }
  let fd = null;
    try {
      const startSeq = eventState.last_seq + 1;
      const entries = batch.map((entry, index) => ({
        seq: startSeq + index,
        event_id: entry.event_id || randomUUID(),
        ts: new Date().toISOString(),
        type: entry.type,
        payload: entry.payload
      }));
      const schemaFailures = entries.flatMap((entry) => validateEventEntry(entry, context));
      if (schemaFailures.length > 0) {
        logSchemaErrors("event", schemaFailures, { context });
        return null;
      }
      let duplicateFound = false;
    entries.forEach((entry) => {
      if (eventIdIndex.has(entry.event_id)) {
        console.log("event.dedupe.hit", { event_id: entry.event_id, seq: entry.seq });
        duplicateFound = true;
      } else {
        console.log("event.dedupe.miss", { event_id: entry.event_id, seq: entry.seq });
      }
    });
    if (duplicateFound) {
      return [];
    }
    if (!fs.existsSync(eventsFile)) {
      fs.writeFileSync(eventsFile, "");
    }
    fd = fs.openSync(eventsFile, "r+");
    const startSize = fs.fstatSync(fd).size;
    let position = startSize;
    let appendedCount = 0;
    try {
      entries.forEach((entry) => {
        const line = `${JSON.stringify(entry)}\n`;
        fs.writeSync(fd, line, position, "utf8");
        position += Buffer.byteLength(line, "utf8");
        appendedCount += 1;
      });
      fs.fsyncSync(fd);
    } catch (error) {
      try {
        fs.ftruncateSync(fd, startSize);
      } catch (truncateError) {
        console.log("event.batch.abort", {
          reason: "truncate_failed",
          context,
          appended: appendedCount,
          expected: entries.length
        });
        throw truncateError;
      }
      console.log("event.batch.abort", {
        reason: "append_failed",
        context,
        appended: appendedCount,
        expected: entries.length
      });
      return null;
    }
      eventState.last_seq = startSeq + entries.length - 1;
      saveEventState();
      events.push(...entries);
      if (deliveryCursorIndex >= events.length - entries.length) {
        deliveryCursorIndex = Math.max(0, events.length - entries.length);
      }
      entries.forEach((entry) => eventIdIndex.add(entry.event_id));
      persistEventIndex();
      if (eventState.last_seq % EVENT_SNAPSHOT_INTERVAL === 0) {
        writeSnapshot(eventState.last_seq);
      }
    return entries;
  } catch (error) {
    if (error) {
      console.log("event.batch.abort", { reason: "append_failed", context });
    }
    return null;
  } finally {
    if (fd !== null) {
      fs.closeSync(fd);
    }
    releaseLock(stateLock);
    releaseLock(eventLock);
    releaseBlockingMutex(appendMutex, "append");
  }
};

const appendLedgerEntry = (entry) => {
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("appendLedgerEntry");
  if (!validateLedgerEntry(entry)) {
    console.log("ledger.skip", { reason: "invalid", token_id: entry.token_id });
    return false;
  }
  const key = ledgerKey(entry.token_id, entry.final_stage);
  if (ledgerIndex.has(key)) {
    console.log("ledger.skip", { reason: "duplicate", token_id: entry.token_id, stage: entry.final_stage });
    return false;
  }
  projectionState.ledger.push(entry);
  ledgerIndex.add(key);
  writeJsonFile(ledgerFile, { version: LEDGER_VERSION, entries: projectionState.ledger });
  console.log("ledger.append", {
    entry_id: entry.entry_id,
    token_id: entry.token_id,
    campaign_id: entry.campaign_id,
    publisher_id: entry.publisher_id
  });
  return true;
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

const getCapStatus = (campaignId) => {
  const caps = getCampaignCaps(campaignId);
  let billableCount = 0;
  let billableWeightedValue = 0;
  projectionState.tokens.forEach((token) => {
    if (!token.scope || token.scope.campaign_id !== campaignId) {
      return;
    }
    if (token.billable !== true) {
      return;
    }
    const finalEvent = getFinalResolutionEvent(token);
    if (!finalEvent) {
      return;
    }
    const resolvedValue = Number.isFinite(finalEvent.resolved_value) ? finalEvent.resolved_value : 0;
    const weight = resolveOutcomeWeight(campaignId, finalEvent.outcome_type);
    billableCount += 1;
    billableWeightedValue += resolvedValue * weight;
  });
  const countRatio = caps.max_outcomes ? billableCount / caps.max_outcomes : 0;
  const valueRatio = caps.max_weighted_value ? billableWeightedValue / caps.max_weighted_value : 0;
  const ratio = Math.max(countRatio, valueRatio);
  const exhausted =
    (caps.max_outcomes !== null && billableCount >= caps.max_outcomes) ||
    (caps.max_weighted_value !== null && billableWeightedValue >= caps.max_weighted_value);
  const near = ratio >= CAP_DEPRIORITIZE_THRESHOLD && !exhausted;
  return {
    caps,
    state: { billable_count: billableCount, billable_weighted_value: billableWeightedValue },
    exhausted,
    near_exhaustion: near,
    ratio
  };
};

const getCapStatusViolations = () => {
  const violations = [];
  campaignRegistry.forEach((campaign) => {
    const status = getCapStatus(campaign.campaign_id);
    if (status.state.billable_count < 0 || status.state.billable_weighted_value < 0) {
      violations.push({ campaign_id: campaign.campaign_id, state: status.state });
    }
  });
  return violations;
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

const resetAggregations = () => {
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("resetAggregations");
  projectionState.aggregates = {
    impressions: new Map(),
    intents: new Map(),
    resolvedIntents: new Map(),
    resolvedValueSum: new Map(),
    partialResolutions: new Map(),
    weightedResolvedValueSum: new Map(),
    billableResolutions: new Map(),
    nonBillableResolutions: new Map()
  };
};

const resetBudgetsFromTotals = () => {
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("resetBudgetsFromTotals");
  projectionState.budgets.forEach((budget) => {
    budget.remaining = budget.total;
  });
};

const loadSnapshot = () => {
  const payload = readJsonFile(snapshotFile);
  if (!payload) {
    return null;
  }
  if (!Number.isFinite(payload.snapshot_seq)) {
    console.error("snapshot.load.failed", { path: snapshotFile });
    process.exit(1);
  }
  return payload;
};

const applyEvent = (event, reason = "projection.apply") => {
  if (!event || !event.type) {
    return projectionState;
  }
  if (reducerFailpointType && event.type === reducerFailpointType) {
    throw new Error("projection.failpoint");
  }
  return withProjectionWrite("applyEvent", () => {
    const payload = event.payload || {};
    if (event.type === "window.reset") {
      const previousWindow = payload.previous_window || {
        started_at: aggregationWindow.started_at,
        started_at_ms: aggregationWindow.started_at_ms,
        window_ms: AGGREGATION_WINDOW_MS
      };
      const lastSnapshot = {
        window: previousWindow,
        aggregates: {
          impressions: mapToEntries(projectionState.aggregates.impressions),
          intents: mapToEntries(projectionState.aggregates.intents),
          resolved_intents: mapToEntries(projectionState.aggregates.resolvedIntents),
          resolved_value_sum: mapToEntries(projectionState.aggregates.resolvedValueSum),
          partial_resolutions: mapToEntries(projectionState.aggregates.partialResolutions),
          weighted_resolved_value_sum: mapToEntries(projectionState.aggregates.weightedResolvedValueSum),
          billable_resolutions: mapToEntries(projectionState.aggregates.billableResolutions),
          non_billable_resolutions: mapToEntries(projectionState.aggregates.nonBillableResolutions)
        }
      };
      projectionState.aggregates.impressions.clear();
      projectionState.aggregates.intents.clear();
      projectionState.aggregates.resolvedIntents.clear();
      projectionState.aggregates.resolvedValueSum.clear();
      projectionState.aggregates.partialResolutions.clear();
      projectionState.aggregates.weightedResolvedValueSum.clear();
      projectionState.aggregates.billableResolutions.clear();
      projectionState.aggregates.nonBillableResolutions.clear();
      const nextWindow = payload.new_window || {
        started_at_ms: Date.now(),
        started_at: new Date().toISOString()
      };
      aggregationWindow = {
        started_at_ms: nextWindow.started_at_ms,
        started_at: nextWindow.started_at
      };
      saveAggregates(lastSnapshot);
      console.log("aggregate.window.reset", {
        reason: payload.reason || reason,
        previous_start: previousWindow.started_at,
        window_start: aggregationWindow.started_at,
        window_ms: AGGREGATION_WINDOW_MS
      });
      return projectionState;
    }
    if (event.type === "impression.recorded") {
      if (payload.scope) {
        bumpAggregate(projectionState.aggregates.impressions, payload.scope, "impressions");
      }
      return projectionState;
    }
    if (event.type === "intent.created") {
      const token = normalizeToken(payload.token);
      token.status = "PENDING";
      token.pending_at = token.pending_at || new Date().toISOString();
      projectionState.tokens.push(token);
      tokenIndex.set(token.token_id, token);
      bumpAggregate(projectionState.aggregates.intents, token.scope, "intents");
      return projectionState;
    }
    if (event.type === "resolution.partial") {
      const token = tokenIndex.get(payload.token_id);
      if (!token) {
        return projectionState;
      }
      addResolutionEvent(token, payload.stage, payload.resolved_at, payload.resolved_value, payload.outcome_type);
      bumpAggregate(projectionState.aggregates.partialResolutions, token.scope, "partial_resolutions");
      return projectionState;
    }
    if (event.type === "resolution.final") {
      const token = tokenIndex.get(payload.token_id);
      if (!token) {
        return projectionState;
      }
      addResolutionEvent(token, payload.stage, payload.resolved_at, payload.resolved_value, payload.outcome_type);
      token.status = "RESOLVED";
      token.resolved_at = token.resolved_at || payload.resolved_at;
      token.resolved_value = token.resolved_value ?? payload.resolved_value;
      token.outcome_type = token.outcome_type || payload.outcome_type;
      token.billable = payload.billable === true;
      bumpAggregate(projectionState.aggregates.resolvedIntents, token.scope, "resolved_intents");
      bumpResolvedValueSum(token.scope, payload.resolved_value);
      bumpWeightedResolvedValueSum(token.scope, payload.weighted_value);
      if (token.billable) {
        bumpResolutionCount(projectionState.aggregates.billableResolutions, token.scope);
      } else {
        bumpResolutionCount(projectionState.aggregates.nonBillableResolutions, token.scope);
      }
      return projectionState;
    }
    if (event.type === "budget.decrement") {
      applyBudgetCharge(payload.campaign_id, payload.amount);
      return projectionState;
    }
    if (event.type === "ledger.append") {
      appendLedgerEntry(payload.entry);
      return projectionState;
    }
    return projectionState;
  });
};

const applyProjectionEvents = (eventsSubset, reason = "live") => {
  if (!eventsSubset || eventsSubset.length === 0) {
    console.log("projection.apply.skip", { reason });
    return;
  }
  if (!acquireBlockingMutex(projectionMutex, null)) {
    console.log("projection.apply.skip", { reason: "projection_lock_timeout" });
    return;
  }
  const sorted = eventsSubset.slice().sort((a, b) => a.seq - b.seq);
  const snapshot = cloneProjectionSnapshot();
  const snapshotWindow = { ...aggregationWindow };
  const snapshotLastWindow = lastWindowSnapshot ? JSON.parse(JSON.stringify(lastWindowSnapshot)) : null;
  const snapshotApplied = projectionCursor.applied_seq;
  const snapshotAppliedIds = new Set(appliedEventIds);
  try {
    sorted.forEach((event) => {
      if (event.seq <= projectionCursor.applied_seq) {
        return;
      }
      if (event.event_id && appliedEventIds.has(event.event_id)) {
        console.log("event.dedupe.hit", { event_id: event.event_id, seq: event.seq });
        return;
      }
      applyEvent(event, reason);
      if (event.event_id) {
        appliedEventIds.add(event.event_id);
      }
    });

    const lastSeq = sorted[sorted.length - 1].seq;
    if (lastSeq > projectionCursor.applied_seq) {
      projectionCursor.applied_seq = lastSeq;
      saveProjectionState();
    }
    withProjectionWrite("applyProjectionEvents.persist", () => {
      saveTokens();
      saveAggregates();
      saveBudgets();
    });
    persistEventIndex();
    console.log("projection.apply.ok", {
      reason,
      applied_events: sorted.length,
      applied_seq: projectionCursor.applied_seq
    });
    if (sorted.some((event) => event.type === "resolution.final")) {
      const budgetViolations = [];
      projectionState.budgets.forEach((budget, campaignId) => {
        if (budget.remaining < 0) {
          budgetViolations.push({ campaign_id: campaignId, remaining: budget.remaining });
        }
      });
      const capsNegative = getCapStatusViolations();
      if (budgetViolations.length > 0 || capsNegative.length > 0) {
        console.log("invariant.violation", { reason: "final_resolution_state_invalid", budgetViolations, capsNegative });
        process.exit(1);
      }
    }
  } catch (error) {
    restoreProjectionSnapshot(snapshot);
    aggregationWindow = snapshotWindow;
    lastWindowSnapshot = snapshotLastWindow;
    projectionCursor.applied_seq = snapshotApplied;
    appliedEventIds = snapshotAppliedIds;
    console.log("projection.rollback", { reason, error: error ? error.message : "unknown" });
    process.exit(1);
  } finally {
    releaseBlockingMutex(projectionMutex, null);
  }
};

const rebuildFromEvents = () => {
  const snapshot = loadSnapshot();
  let replayFromSeq = 0;
  withProjectionWrite("rebuild.reset", () => {
    projectionState.tokens = [];
    projectionState.ledger = [];
    ledgerIndex = new Set();
    tokenIndex = new Map();
    appliedEventIds = new Set();
    if (snapshot) {
      replayFromSeq = snapshot.snapshot_seq;
      aggregationWindow = snapshot.window || aggregationWindow;
      lastWindowSnapshot = snapshot.last_window || lastWindowSnapshot;
      projectionState.budgets = mapFromEntries(snapshot.budgets || [], (entry) => entry.campaign_id);
      const aggregatesPayload = snapshot.aggregates || emptyAggregateWindow();
      projectionState.aggregates = {
        impressions: mapFromEntries(aggregatesPayload.impressions, aggregateKey),
        intents: mapFromEntries(aggregatesPayload.intents, aggregateKey),
        resolvedIntents: mapFromEntries(aggregatesPayload.resolved_intents, aggregateKey),
        resolvedValueSum: mapFromEntries(aggregatesPayload.resolved_value_sum, aggregateKey),
        partialResolutions: mapFromEntries(aggregatesPayload.partial_resolutions, aggregateKey),
        weightedResolvedValueSum: mapFromEntries(aggregatesPayload.weighted_resolved_value_sum, aggregateKey),
        billableResolutions: mapFromEntries(aggregatesPayload.billable_resolutions, aggregateKey),
        nonBillableResolutions: mapFromEntries(aggregatesPayload.non_billable_resolutions, aggregateKey)
      };
      projectionState.ledger = snapshot.ledger || [];
      ledgerIndex = new Set(
        projectionState.ledger
          .filter((entry) => entry.token_id && entry.final_stage)
          .map((entry) => ledgerKey(entry.token_id, entry.final_stage))
      );
      projectionState.tokens = snapshot.tokens || [];
      tokenIndex = new Map(projectionState.tokens.map((token) => [token.token_id, token]));
    } else {
      resetAggregations();
      resetBudgetsFromTotals();
    }
  });

  projectionCursor.applied_seq = replayFromSeq;
  const replayEvents = events.filter((event) => event.seq > replayFromSeq);
  applyProjectionEvents(replayEvents, "rebuild");

  const eventCounts = events.reduce((acc, event) => {
    acc[event.type] = (acc[event.type] || 0) + 1;
    return acc;
  }, {});
  const ledgerAppendCount = eventCounts["ledger.append"] || 0;
  const intentCount = eventCounts["intent.created"] || 0;
  const projectionMismatch =
    ledgerAppendCount !== projectionState.ledger.length || intentCount !== projectionState.tokens.length;
  if (projectionMismatch) {
    console.log("projection.rebuild.mismatch", {
      snapshot_seq: replayFromSeq,
      intent_events: intentCount,
      tokens: projectionState.tokens.length,
      ledger_events: ledgerAppendCount,
      ledger_entries: projectionState.ledger.length
    });
  } else {
    console.log("projection.rebuild.ok", {
      snapshot_seq: replayFromSeq,
      replayed_events: replayEvents.length,
      final_seq: eventState.last_seq
    });
  }
};

const selectCreative = (selectionView) => {
  // V1.5 publisher control scaffolding; V2 candidate for persistence and UI management.
  // Deterministic, side-effect free selection based on in-memory aggregates.
  // If no candidates meet the data floor, fall back to stable ordering by priority, campaign, creative.
  const candidates = selectionView.candidates || [];
  const publisherPolicy = selectionView.publisher_policy || {};
  const publisherId = selectionView.publisher_id;
  const allowedTypes = new Set(publisherPolicy.allowed_demand_types || []);
  const priorityOrder = publisherPolicy.demand_priority || [];
  const selectionMode = publisherPolicy.selection_mode || "raw";
  const priorityIndex = (demandType) => {
    const idx = priorityOrder.indexOf(demandType);
    return idx === -1 ? Number.MAX_SAFE_INTEGER : idx;
  };
  const budgetStatusFor = (candidate) =>
    candidate.budget_status || {
      total: 0,
      remaining: 0,
      ratio: 0,
      exhausted: false,
      near_exhaustion: false
    };
  const capStatusFor = (candidate) =>
    candidate.cap_status || {
      caps: { max_outcomes: null, max_weighted_value: null },
      state: { billable_count: 0, billable_weighted_value: 0 },
      exhausted: false,
      near_exhaustion: false,
      ratio: 0
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
    window_start: selectionView.window?.started_at || aggregationWindow.started_at,
    window_ms: selectionView.window?.window_ms || AGGREGATION_WINDOW_MS
  });

  const budgetFiltered = candidates.filter((candidate) => {
    const budgetStatus = budgetStatusFor(candidate);
    if (budgetStatus.exhausted) {
      console.log("selection.excluded", {
        reason: "budget_exhausted",
        campaign_id: candidate.scope.campaign_id,
        creative_id: candidate.creative_id
      });
      return false;
    }
    const capStatus = capStatusFor(candidate);
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
    .map((candidate) => ({
      candidate,
      metrics: candidate.metrics || {
        derived_value_per_1k: 0,
        weighted_derived_value_per_1k: 0,
        weighted_present: false
      }
    }))
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
      const aBudget = budgetStatusFor(a.candidate);
      const bBudget = budgetStatusFor(b.candidate);
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
      const aBudget = budgetStatusFor(a.candidate);
      const bBudget = budgetStatusFor(b.candidate);
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
    recordGuardrailDivergence(
      publisherId,
      selectionView.window?.started_at || aggregationWindow.started_at,
      divergent
    );
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
      const aBudget = budgetStatusFor(a.candidate);
      const bBudget = budgetStatusFor(b.candidate);
      if (aBudget.near_exhaustion !== bBudget.near_exhaustion) {
        return aBudget.near_exhaustion ? 1 : -1;
      }
      const aCap = capStatusFor(a.candidate);
      const bCap = capStatusFor(b.candidate);
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
        budget_remaining: budgetStatusFor(entry.candidate).remaining,
        budget_total: budgetStatusFor(entry.candidate).total,
        near_exhaustion: budgetStatusFor(entry.candidate).near_exhaustion
      }))
    });

    if (eligible.length === 0) {
      const fallback = candidates
        .filter((candidate) => !budgetStatusFor(candidate).exhausted)
        .filter((candidate) => !capStatusFor(candidate).exhausted)
        .filter((candidate) => allowedTypes.has(candidate.demand_type))
      .sort((a, b) => {
      const aPriority = priorityIndex(a.demand_type);
      const bPriority = priorityIndex(b.demand_type);
      if (aPriority !== bPriority) {
        return aPriority - bPriority;
      }
      const aBudget = budgetStatusFor(a);
      const bBudget = budgetStatusFor(b);
      if (aBudget.near_exhaustion !== bBudget.near_exhaustion) {
        return aBudget.near_exhaustion ? 1 : -1;
      }
      const aCap = capStatusFor(a);
      const bCap = capStatusFor(b);
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
    budget_remaining: budgetStatusFor(selected).remaining
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
    budget_remaining: budgetStatusFor(eligible[0].candidate).remaining
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
  const report = withProjectionRead("reports.legacy", () => getReportingView(projectionState, publisherFilter));
  if (!includeSelections) {
    const { selection_decisions: _ignored, ...rest } = report;
    return rest;
  }
  return report;
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

const saveTokens = () => {
  // PROHIBITED: do not mutate state here outside projections.
  assertProjectionWrite("saveTokens");
  fs.writeFileSync(tokensFile, `${JSON.stringify(projectionState.tokens, null, 2)}\n`);
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
    const aggregateSum = Array.from(projectionState.aggregates.resolvedValueSum.values())
      .filter((entry) => entry.campaign_id === campaign.campaign_id)
      .reduce((sum, entry) => sum + (Number.isFinite(entry.sum) ? entry.sum : 0), 0);

    let tokenSumWindow = 0;
    let tokenSumBillableTotal = 0;
    tokensSnapshot.forEach((token) => {
      if (!token.scope || token.scope.campaign_id !== campaign.campaign_id) {
        return;
      }
      const finalEvent = getFinalResolutionEvent(token);
      if (!finalEvent) {
        return;
      }
      const value = Number.isFinite(finalEvent.resolved_value) ? finalEvent.resolved_value : 0;
      if (token.billable === true) {
        tokenSumBillableTotal += value;
      }
      const resolvedAtMs = new Date(finalEvent.resolved_at).getTime();
      if (resolvedAtMs >= windowStartMs && resolvedAtMs < windowEndMs) {
        tokenSumWindow += value;
      }
    });

    const aggregateMismatch = Math.abs(tokenSumWindow - aggregateSum) > RECONCILIATION_TOLERANCE;
    const budgetMismatch = Math.abs(tokenSumBillableTotal - budgetDelta) > RECONCILIATION_TOLERANCE;
    const logPayload = {
      campaign_id: campaign.campaign_id,
      advertiser_id: campaign.advertiser_id || null,
      window_id: aggregationWindow.started_at,
      token_sum: tokenSumWindow,
      billable_token_sum: tokenSumBillableTotal,
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

const reconcileLedger = (tokensSnapshot) => {
  const windowStartMs = aggregationWindow.started_at_ms || Date.now();
  const windowEndMs = windowStartMs + AGGREGATION_WINDOW_MS;
  campaignRegistry.forEach((campaign) => {
    const revShareBps = resolveRevShareBps(campaign.campaign_id, campaign.publisher_id);
    let expectedWindow = 0;
    let expectedLifetime = 0;
    tokensSnapshot.forEach((token) => {
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

    const ledgerWindowSum = projectionState.ledger
      .filter((entry) => entry.campaign_id === campaign.campaign_id && entry.window_id === aggregationWindow.started_at)
      .reduce((sum, entry) => sum + (Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0), 0);
    const ledgerLifetimeSum = projectionState.ledger
      .filter((entry) => entry.campaign_id === campaign.campaign_id)
      .reduce((sum, entry) => sum + (Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0), 0);

    const windowMismatch = Math.abs(ledgerWindowSum - expectedWindow) > RECONCILIATION_TOLERANCE;
    const lifetimeMismatch = Math.abs(ledgerLifetimeSum - expectedLifetime) > RECONCILIATION_TOLERANCE;
    const logPayload = {
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
        ...logPayload,
        lifetime_ledger_sum: ledgerLifetimeSum,
        lifetime_expected_sum: expectedLifetime
      });
    } else {
      console.log("ledger.reconcile.ok", logPayload);
    }
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
  loadLedger();
  loadPayoutRuns();
  loadEvents();
  loadDeliveryState();
  resolveDeliveryIndex();
  const eventIndexLoaded = loadEventIndex();
  loadProjectionState();
  verifyEventIntegrity();
  console.log("aggregate.window.start", {
    window_start: aggregationWindow.started_at,
    window_ms: AGGREGATION_WINDOW_MS
  });
if (!eventIndexLoaded) {
  rebuildEventIndex();
}

const rebuildFromEventsEnabled = process.env.REBUILD_FROM_EVENTS === "true";

if (rebuildFromEventsEnabled) {
  const eventLock = acquireLock(eventsFile);
  const snapshotLock = acquireLock(snapshotFile);
  if (!eventLock || !snapshotLock) {
    releaseLock(snapshotLock);
    releaseLock(eventLock);
    console.error("projection.rebuild.failed", { reason: "lock_timeout" });
    process.exit(1);
  }
  rebuildFromEvents();
  projectionCursor.applied_seq = eventState.last_seq;
  saveProjectionState();
  releaseLock(snapshotLock);
  releaseLock(eventLock);
  tokenIndex = new Map(projectionState.tokens.map((token) => [token.token_id, token]));
} else {
  withProjectionWrite("loadTokens", () => {
    projectionState.tokens = normalizeTokens(loadTokens());
  });
  tokenIndex = new Map(projectionState.tokens.map((token) => [token.token_id, token]));
  if (projectionState.tokens.length > 0) {
    console.log("tokens.load.normalized", { count: projectionState.tokens.length });
  }
  appliedEventIds = new Set(
    events
      .filter((event) => event.event_id && event.seq <= projectionCursor.applied_seq)
      .map((event) => event.event_id)
  );
  const unapplied = events.filter((event) => event.seq > projectionCursor.applied_seq);
  applyProjectionEvents(unapplied, "startup");
}
  reconcileCampaigns(projectionState.tokens);
  reconcileLedger(projectionState.tokens);

  if (WEBHOOK_URL) {
    if (!WRITE_ENABLED) {
      console.log("webhook.delivery.skip", { reason: "write_disabled" });
    } else if (process.env.REBUILD_FROM_EVENTS === "true") {
      console.log("webhook.delivery.skip", { reason: "rebuild_mode" });
    } else {
      console.log("webhook.delivery.start", { url: WEBHOOK_URL, last_delivered_seq: deliveryState.last_delivered_seq });
      scheduleDeliveryTick(0);
    }
  }

const findToken = (tokenId) => tokenIndex.get(tokenId);

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
  ensureWindowFresh();
  const selectionView = withProjectionRead("fill.selection", () =>
    getSelectionView(projectionState, publisher.publisher_id, requestedSize)
  );
  const campaigns = selectionView.campaigns || [];
  if (selectionView.invalid_creatives && selectionView.invalid_creatives.length > 0) {
    selectionView.invalid_creatives.forEach((entry) => {
      console.log("invariant.violation", entry);
    });
  }
  if (campaigns.length === 0) {
    console.log("invariant.violation", { reason: "publisher_without_campaigns", publisher_id: publisher.publisher_id });
    sendJson(res, 500, { error: "publisher_campaigns_missing" });
    return;
  }
  const candidates = selectionView.candidates || [];
  const fallbackCandidate = selectionView.fallback_candidate;
  if (!fallbackCandidate || !isNonEmptyString(fallbackCandidate.demand_type)) {
    console.log("invariant.violation", { reason: "fallback_creative_invalid" });
    sendJson(res, 500, { error: "creative_registry_invalid" });
    return;
  }
  const selectedCreative = candidates.length > 0 ? selectCreative(selectionView) : fallbackCandidate;

  const impressionEvents = appendEventBatch(
    [
      {
        type: "impression.recorded",
        payload: {
          scope: {
            campaign_id: selectedCreative.scope.campaign_id,
            publisher_id: selectedCreative.scope.publisher_id,
            creative_id: selectedCreative.creative_id
          },
          occurred_at: new Date().toISOString()
        }
      }
    ],
    "impression.recorded"
  );
  if (!impressionEvents) {
    sendJson(res, 500, { error: "event_append_failed" });
    return;
  }
  applyProjectionEvents(impressionEvents, "impression.recorded");

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
  if (!WRITE_ENABLED) {
    rejectWrites(res, "/v1/intent");
    return;
  }
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
  ensureWindowFresh();
  const eventsBatch = appendEventBatch([{ type: "intent.created", payload: { token } }], "intent.created");
  if (!eventsBatch) {
    sendJson(res, 500, { error: "event_append_failed" });
    return;
  }
  applyProjectionEvents(eventsBatch, "intent.created");

  const updatedToken = findToken(token.token_id) || token;
  logLifecycle("intent.pending", updatedToken);

  sendJson(res, 200, { token: updatedToken });
};

const handlePostback = (req, url, res) => {
  if (!WRITE_ENABLED) {
    rejectWrites(res, "/v1/postback");
    return;
  }
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
    logAuth("auth.reject", "advertiser", advertiserId, "campaign_mismatch", req.request_id || null);
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
    console.log("ledger.skip", { reason: "duplicate", token_id: token.token_id, stage });
    sendJson(res, 200, { token, status: "already_resolved" });
    return;
  }
  const hasStageEvent = token.resolution_events?.some((event) => event.stage === stage);
  if (hasStageEvent) {
    logLifecycle("postback.idempotent", token, { stage, key: stageKey });
    console.log("ledger.skip", { reason: "duplicate", token_id: token.token_id, stage });
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
      const expiredToken = enforceExpiry(token);
      logLifecycle("postback.expired", expiredToken);
      sendJson(res, 410, { token: expiredToken, status: "expired" });
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
    console.log("ledger.skip", { reason: "duplicate", token_id: token.token_id, stage });
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
  }

  ensureWindowFresh();
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

    logLifecycle("postback.resolved", token, { stage, value: resolvedValue, billable });
    const eventsBatch = [
      {
        type: "resolution.final",
        payload: {
          token_id: token.token_id,
          stage,
          resolved_at: now.toISOString(),
          resolved_value: resolvedValue,
          outcome_type: outcomeType,
          weighted_value: weightedValue,
          billable
        }
      }
    ];
    if (billable) {
      eventsBatch.push({
        type: "budget.decrement",
        payload: {
          token_id: token.token_id,
          campaign_id: token.scope.campaign_id,
          amount: resolvedValue
        }
      });
      const revShareBps = resolveRevShareBps(token.scope.campaign_id, token.scope.publisher_id);
      const payoutCents = Math.round(resolvedValue * 100 * (revShareBps / 10000));
      const ledgerEntry = {
        entry_id: randomUUID(),
        created_at: new Date().toISOString(),
        token_id: token.token_id,
        campaign_id: token.scope.campaign_id,
        advertiser_id: campaign.advertiser_id || null,
        publisher_id: token.scope.publisher_id,
        creative_id: token.scope.creative_id,
        window_id: aggregationWindow.started_at,
        outcome_type: outcomeType,
        raw_value: resolvedValue,
        weighted_value: weightedValue,
        billable: true,
        payout_cents: payoutCents,
        rev_share_bps: revShareBps,
        final_stage: stage,
        notes: null
      };
      eventsBatch.push({
        type: "ledger.append",
        payload: { entry: ledgerEntry }
      });
    } else {
      console.log("ledger.skip", { reason: "non_billable", token_id: token.token_id, stage });
    }
    const appended = appendEventBatch(eventsBatch, "resolution.final");
    if (!appended) {
      sendJson(res, 500, { error: "event_append_failed" });
      return;
    }
    applyProjectionEvents(appended, "resolution.final");
    const updatedToken = findToken(tokenId) || token;
    const metrics = getDerivedMetrics(updatedToken.scope);
    const resolvedValueSum = projectionState.aggregates.resolvedValueSum.get(aggregateKey(updatedToken.scope))?.sum || 0;
    const weightedResolvedValueSum =
      projectionState.aggregates.weightedResolvedValueSum.get(aggregateKey(updatedToken.scope))?.sum || 0;
    const derivedValuePer1k = metrics.impressions > 0 ? (resolvedValueSum / metrics.impressions) * 1000 : 0;
    const weightedDerivedValuePer1k = metrics.impressions > 0 ? (weightedResolvedValueSum / metrics.impressions) * 1000 : 0;
    console.log("aggregate.metrics", {
      campaign_id: updatedToken.scope.campaign_id,
      publisher_id: updatedToken.scope.publisher_id,
      creative_id: updatedToken.scope.creative_id,
      impressions: metrics.impressions,
      intents: metrics.intents,
      resolvedIntents: metrics.resolvedIntents,
      intent_rate: metrics.intent_rate,
      resolution_rate: metrics.resolution_rate,
      derived_value_per_1k: derivedValuePer1k
    });
    console.log("aggregate.weighted_metrics", {
      campaign_id: updatedToken.scope.campaign_id,
      publisher_id: updatedToken.scope.publisher_id,
      creative_id: updatedToken.scope.creative_id,
      window_start: aggregationWindow.started_at,
      weighted_resolved_value_sum: weightedResolvedValueSum,
      weighted_derived_value_per_1k: weightedDerivedValuePer1k,
      outcome_type: outcomeType,
      outcome_weight: resolveOutcomeWeight(updatedToken.scope.campaign_id, outcomeType)
    });
    sendJson(res, 200, { token: updatedToken, status: "resolved" });
    return;
  }

  const appended = appendEventBatch(
    [
      {
        type: "resolution.partial",
        payload: {
          token_id: token.token_id,
          stage,
          resolved_at: now.toISOString(),
          resolved_value: resolvedValue,
          outcome_type: outcomeType
        }
      }
    ],
    "resolution.partial"
  );
  if (!appended) {
    sendJson(res, 500, { error: "event_append_failed" });
    return;
  }
  applyProjectionEvents(appended, "resolution.partial");

  logLifecycle("postback.partial", token, { stage, value: resolvedValue });
  console.log("ledger.skip", { reason: "partial", token_id: token.token_id, stage });
  const updatedToken = findToken(tokenId) || token;
  sendJson(res, 200, { token: updatedToken, status: "partial" });
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
    const incomingId = req.headers["x-request-id"];
    req.request_id = typeof incomingId === "string" && incomingId.trim() ? incomingId.trim() : randomUUID();
    res.setHeader("X-Request-Id", req.request_id);
    const startAt = Date.now();
    res.on("finish", () => {
      const durationMs = Date.now() - startAt;
      console.log("http.request", {
        method: req.method,
        path: req.url,
        status: res.statusCode,
        duration_ms: durationMs,
        ip: getClientIp(req),
        request_id: req.request_id
      });
    });
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("X-Frame-Options", "DENY");
    res.setHeader("Referrer-Policy", "no-referrer");
    res.setHeader(
      "Content-Security-Policy",
      "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self';"
    );
    if (!applyRateLimit(req, res)) {
      return;
    }

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

    if (req.method === "GET" && url.pathname === "/healthz") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          status: "ok",
          role: PROCESS_ROLE,
          write_enabled: WRITE_ENABLED,
          webhook_enabled: Boolean(WEBHOOK_URL)
        })
      );
      return;
    }

      if (req.method === "GET" && url.pathname === "/v1/reports") {
        const publisherId = authorizePublisher(req, res, null);
        if (!publisherId) {
          return;
        }
        const includeSelections = url.searchParams.get("include_selections") === "true";
        const report = withProjectionRead("reports.view", () => getReportingView(projectionState, publisherId));
        const reportErrors = validateSchema(schemaRegistry.report, report, "$");
        if (reportErrors.length > 0) {
          console.log("report.schema.invalid", {
            publisher_id: publisherId,
            errors: reportErrors
          });
        }
        if (!includeSelections) {
          const { selection_decisions: _ignored, ...rest } = report;
          sendJson(res, 200, { reports: rest });
          return;
        }
        sendJson(res, 200, { reports: report });
        return;
      }

      if (req.method === "GET" && url.pathname === "/v1/delivery") {
        const publisherId = authorizePublisher(req, res, null);
        if (!publisherId) {
          return;
        }
        const dlqStats = loadDeliveryDlqStats();
        const lastEventSeq = getLastEventSeq();
        const lag = Math.max(0, lastEventSeq - deliveryState.last_delivered_seq);
        if (lag >= DELIVERY_LAG_WARN) {
          console.log("delivery.lag.warning", {
            last_event_seq: lastEventSeq,
            last_delivered_seq: deliveryState.last_delivered_seq,
            delivery_lag: lag,
            threshold: DELIVERY_LAG_WARN
          });
        }
        sendJson(res, 200, {
          delivery_health: {
            last_delivered_seq: deliveryState.last_delivered_seq,
            last_attempt_at: deliveryState.last_attempt_at,
            retry_count: deliveryState.retry_count,
            last_event_seq: lastEventSeq,
            delivery_lag: lag,
            dlq: dlqStats
          }
        });
        return;
      }

      if (req.method === "GET") {
          if (
            url.pathname === "/ops.html" ||
            url.pathname.startsWith("/schemas/") ||
            url.pathname === "/advertiser.html" ||
            url.pathname === "/health.html"
          ) {
            if (!authorizeOpsRequest(req, url, res)) {
              return;
            }
          }
          const requestedPath = url.pathname === "/" ? "/index.html" : url.pathname;
          const relativePath = requestedPath.replace(/^\/+/, "");
          const filePath = path.resolve(publicDir, relativePath);
          if (!filePath.startsWith(publicDir)) {
            res.writeHead(403, { "Content-Type": "text/plain" });
            res.end("Forbidden");
            return;
          }
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

const shouldStartServer = process.env.START_SERVER !== "false";
if (shouldStartServer) {
  server.listen(port, host, () => {
    console.log(`Flyback server listening on http://${host}:${port}`);
  });
}

const setReducerFailpoint = (type) => {
  reducerFailpointType = type;
};

const clearReducerFailpoint = () => {
  reducerFailpointType = null;
};

const getProjectionSnapshot = () => ({
  tokens: JSON.parse(JSON.stringify(projectionState.tokens)),
  aggregates: {
    impressions: mapToEntries(projectionState.aggregates.impressions),
    intents: mapToEntries(projectionState.aggregates.intents),
    resolved_intents: mapToEntries(projectionState.aggregates.resolvedIntents),
    resolved_value_sum: mapToEntries(projectionState.aggregates.resolvedValueSum),
    partial_resolutions: mapToEntries(projectionState.aggregates.partialResolutions),
    weighted_resolved_value_sum: mapToEntries(projectionState.aggregates.weightedResolvedValueSum),
    billable_resolutions: mapToEntries(projectionState.aggregates.billableResolutions),
    non_billable_resolutions: mapToEntries(projectionState.aggregates.nonBillableResolutions)
  },
  budgets: mapToEntries(projectionState.budgets),
  ledger: projectionState.ledger.map((entry) => ({ ...entry }))
});

export const __test = {
  appendEventBatch,
  applyProjectionEvents,
  getSelectionView,
  getReportingView,
  getProjectionSnapshot,
  setReducerFailpoint,
  clearReducerFailpoint,
  persistEventIndex,
  rebuildEventIndex
};
