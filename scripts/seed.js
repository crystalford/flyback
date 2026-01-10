import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = path.join(rootDir, "data");

const registry = {
  version: 1,
  advertisers: [
    { advertiser_id: "advertiser-demo", campaign_ids: ["campaign-v1", "campaign-v2", "campaign-v3"] },
    { advertiser_id: "advertiser-labs", campaign_ids: ["campaign-labs-1"] }
  ],
  publishers: [
    { publisher_id: "publisher-demo", campaign_ids: ["campaign-v1", "campaign-v2", "campaign-v3"] },
    { publisher_id: "publisher-labs", campaign_ids: ["campaign-labs-1"] }
  ],
  campaigns: [
    {
      campaign_id: "campaign-v1",
      publisher_id: "publisher-demo",
      advertiser_id: "advertiser-demo",
      creative_ids: ["creative-v1", "creative-v2"],
      outcome_weights: { lead: 1, signup: 3, purchase: 10 },
      caps: { max_outcomes: 10, max_weighted_value: 200 }
    },
    {
      campaign_id: "campaign-v2",
      publisher_id: "publisher-demo",
      advertiser_id: "advertiser-demo",
      creative_ids: ["creative-v3", "creative-v4"],
      outcome_weights: { lead: 1, signup: 2, purchase: 8 },
      caps: { max_outcomes: 8, max_weighted_value: 120 }
    },
    {
      campaign_id: "campaign-v3",
      publisher_id: "publisher-demo",
      advertiser_id: "advertiser-demo",
      creative_ids: ["creative-v5"],
      outcome_weights: { lead: 1, signup: 3, purchase: 12 },
      caps: { max_outcomes: 6, max_weighted_value: 150 }
    },
    {
      campaign_id: "campaign-labs-1",
      publisher_id: "publisher-labs",
      advertiser_id: "advertiser-labs",
      creative_ids: ["creative-v2", "creative-v4"],
      outcome_weights: { lead: 1, signup: 3, purchase: 10 },
      caps: { max_outcomes: 5, max_weighted_value: 80 }
    }
  ],
  creatives: [
    { creative_id: "creative-v1", creative_url: "/creative.js", sizes: ["300x250"], demand_type: "direct" },
    { creative_id: "creative-v2", creative_url: "/creative.js", sizes: ["300x250", "320x50"], demand_type: "performance" },
    { creative_id: "creative-v3", creative_url: "/creative.js", sizes: ["300x250"], demand_type: "affiliate" },
    { creative_id: "creative-v4", creative_url: "/creative.js", sizes: ["300x250"], demand_type: "direct" },
    { creative_id: "creative-v5", creative_url: "/creative.js", sizes: ["300x250"], demand_type: "performance" }
  ],
  policies: {
    "publisher-demo": {
      allowed_demand_types: ["direct", "performance", "affiliate"],
      derived_value_floor: 0,
      demand_priority: ["direct", "performance", "affiliate"],
      selection_mode: "raw",
      floor_type: "raw",
      floor_value_per_1k: 0
    },
    "publisher-labs": {
      allowed_demand_types: ["performance", "affiliate"],
      derived_value_floor: 0.5,
      demand_priority: ["performance", "affiliate"],
      selection_mode: "weighted",
      floor_type: "weighted",
      floor_value_per_1k: 5
    }
  }
};

const budgets = {
  version: 1,
  campaigns: [
    { campaign_id: "campaign-v1", total: 120, remaining: 120 },
    { campaign_id: "campaign-v2", total: 80, remaining: 80 },
    { campaign_id: "campaign-v3", total: 60, remaining: 60 },
    { campaign_id: "campaign-labs-1", total: 40, remaining: 40 }
  ]
};

const keys = {
  version: 1,
  default_demo_publisher_key: "demo-publisher-key",
  default_demo_advertiser_key: "demo-advertiser-key",
  publishers: [
    { publisher_id: "publisher-demo", api_key: "demo-publisher-key" },
    { publisher_id: "publisher-labs", api_key: "labs-publisher-key" }
  ],
  advertisers: [
    { advertiser_id: "advertiser-demo", api_key: "demo-advertiser-key" },
    { advertiser_id: "advertiser-labs", api_key: "labs-advertiser-key" }
  ]
};

const aggregates = {
  version: 1,
  window: {
    started_at: new Date(0).toISOString(),
    started_at_ms: 0,
    window_ms: 10 * 60 * 1000
  },
  current: {
    impressions: [],
    intents: [],
    resolved_intents: [],
    resolved_value_sum: [],
    partial_resolutions: [],
    weighted_resolved_value_sum: [],
    billable_resolutions: [],
    non_billable_resolutions: []
  },
  last_window: null
};

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

fs.writeFileSync(path.join(dataDir, "registry.json"), `${JSON.stringify(registry, null, 2)}\n`);
fs.writeFileSync(path.join(dataDir, "budgets.json"), `${JSON.stringify(budgets, null, 2)}\n`);
fs.writeFileSync(path.join(dataDir, "keys.json"), `${JSON.stringify(keys, null, 2)}\n`);
fs.writeFileSync(path.join(dataDir, "aggregates.json"), `${JSON.stringify(aggregates, null, 2)}\n`);

console.log("seed.complete", { data_dir: dataDir });
