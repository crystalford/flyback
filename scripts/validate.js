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

const schemaErrors = validateSchema(registrySchema, registry, "$");
if (schemaErrors.length > 0) {
  console.error("validate.registry.schema_failed", { errors: schemaErrors });
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
