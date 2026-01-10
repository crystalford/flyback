import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const registryPath = path.join(dataDir, "registry.json");

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
    console.error("validate.read.failed", { path: filePath, error: error.message });
    return null;
  }
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
    Object.values(registry.policies).forEach((policy) => {
      if (policy.floor_type && !["raw", "weighted"].includes(policy.floor_type)) {
        errors.push("policy_floor_type_invalid");
      }
      if (policy.floor_value_per_1k !== undefined && !Number.isFinite(policy.floor_value_per_1k)) {
        errors.push("policy_floor_value_invalid");
      }
      if (policy.rev_share_bps !== undefined && !Number.isFinite(policy.rev_share_bps)) {
        errors.push("policy_rev_share_invalid");
      }
    });
  }
  registry.campaigns?.forEach((campaign) => {
    if (campaign.outcome_weights && typeof campaign.outcome_weights !== "object") {
      errors.push("campaign_outcome_weights_invalid");
    }
    if (campaign.caps && typeof campaign.caps !== "object") {
      errors.push("campaign_caps_invalid");
    }
    if (campaign.caps?.max_outcomes !== undefined && !Number.isFinite(campaign.caps.max_outcomes)) {
      errors.push("campaign_caps_max_outcomes_invalid");
    }
    if (campaign.caps?.max_weighted_value !== undefined && !Number.isFinite(campaign.caps.max_weighted_value)) {
      errors.push("campaign_caps_max_weighted_invalid");
    }
    if (campaign.publisher_rev_share_bps !== undefined && !Number.isFinite(campaign.publisher_rev_share_bps)) {
      errors.push("campaign_rev_share_invalid");
    }
  });
  return errors;
};

const registry = readJsonFile(registryPath);
if (!registry) {
  console.error("validate.registry.missing", { path: registryPath });
  process.exit(1);
}

const errors = validateRegistry(registry);
if (errors.length > 0) {
  console.error("validate.registry.failed", { errors });
  process.exit(1);
}

console.log("validate.registry.ok", {
  publishers: registry.publishers.length,
  campaigns: registry.campaigns.length,
  creatives: registry.creatives.length
});
