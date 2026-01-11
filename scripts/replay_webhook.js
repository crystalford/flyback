import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const eventsFile = path.join(dataDir, "events.ndjson");
const dlqFile = path.join(dataDir, "delivery_dlq.ndjson");
const schemaFile = path.join(rootDir, "schemas", "schemas.json");

const WEBHOOK_URL = process.env.WEBHOOK_URL || "";
const WEBHOOK_TIMEOUT_MS = Number(process.env.WEBHOOK_TIMEOUT_MS) || 5000;

const schemaTypeMatches = (type, value) => {
  switch (type) {
    case "object":
      return value !== null && typeof value === "object" && !Array.isArray(value);
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

const validateSchema = (schema, value, pathLabel = "$") => {
  const errors = [];
  if (!schema || typeof schema !== "object") {
    return errors;
  }
  if (schema.enum && !schema.enum.includes(value)) {
    errors.push(`${pathLabel}.enum`);
    return errors;
  }
  if (schema.type) {
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    const matches = types.some((type) => schemaTypeMatches(type, value));
    if (!matches) {
      errors.push(`${pathLabel}.type`);
      return errors;
    }
  }
  if (schema.type === "array" && Array.isArray(value) && schema.items) {
    value.forEach((entry, index) => {
      errors.push(...validateSchema(schema.items, entry, `${pathLabel}[${index}]`));
    });
  }
  if (schema.type === "object" && value !== null && typeof value === "object" && !Array.isArray(value)) {
    if (Array.isArray(schema.required)) {
      schema.required.forEach((key) => {
        if (!Object.prototype.hasOwnProperty.call(value, key)) {
          errors.push(`${pathLabel}.required.${key}`);
        }
      });
    }
    const props = schema.properties || {};
    Object.entries(props).forEach(([key, propSchema]) => {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        if (value[key] === undefined) {
          return;
        }
        errors.push(...validateSchema(propSchema, value[key], `${pathLabel}.${key}`));
      }
    });
  }
  return errors;
};

const loadDeliverySchema = () => {
  if (!fs.existsSync(schemaFile)) {
    return null;
  }
  const raw = fs.readFileSync(schemaFile, "utf8");
  if (!raw.trim()) {
    return null;
  }
  try {
    const payload = JSON.parse(raw);
    return payload.delivery_payload || null;
  } catch {
    return null;
  }
};

const parseArgs = () => {
  const args = process.argv.slice(2);
  const result = { from: 1, to: null, dryRun: false, dlq: false, limit: null };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === "--from" && args[i + 1]) {
      result.from = Number(args[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--to" && args[i + 1]) {
      result.to = Number(args[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--dry-run") {
      result.dryRun = true;
    }
    if (arg === "--dlq") {
      result.dlq = true;
    }
    if (arg === "--limit" && args[i + 1]) {
      result.limit = Number(args[i + 1]);
      i += 1;
    }
  }
  return result;
};

const readEvents = () => {
  if (!fs.existsSync(eventsFile)) {
    return [];
  }
  const raw = fs.readFileSync(eventsFile, "utf8");
  if (!raw.trim()) {
    return [];
  }
  return raw
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line))
    .sort((a, b) => a.seq - b.seq);
};

const readDlq = () => {
  if (!fs.existsSync(dlqFile)) {
    return [];
  }
  const raw = fs.readFileSync(dlqFile, "utf8");
  if (!raw.trim()) {
    return [];
  }
  return raw
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line));
};

const postWebhook = async (payload) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);
  try {
    const response = await fetch(WEBHOOK_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal
    });
    return { ok: response.ok, status: response.status };
  } catch (error) {
    return { ok: false, status: 0, error: error.message };
  } finally {
    clearTimeout(timeout);
  }
};

const run = async () => {
  if (!WEBHOOK_URL) {
    console.error("replay_webhook.missing_url", { env: "WEBHOOK_URL" });
    process.exit(1);
  }
  const { from, to, dryRun, dlq, limit } = parseArgs();
  const deliverySchema = loadDeliverySchema();
  if (!deliverySchema) {
    console.error("replay_webhook.schema.missing", { path: schemaFile });
    process.exit(1);
  }
  const source = dlq ? readDlq() : readEvents();
  const eligible = source.filter((event) => {
    if (dlq) {
      return !!event.payload;
    }
    if (event.type !== "resolution.final") {
      return false;
    }
    if (!Number.isFinite(event.seq)) {
      return false;
    }
    if (event.seq < from) {
      return false;
    }
    if (Number.isFinite(to) && event.seq > to) {
      return false;
    }
    return true;
  });

  const limited = Number.isFinite(limit) ? eligible.slice(0, Math.max(0, limit)) : eligible;
  console.log("replay_webhook.start", {
    from,
    to: Number.isFinite(to) ? to : null,
    events: limited.length,
    dry_run: dryRun,
    source: dlq ? "dlq" : "events"
  });

  for (const event of limited) {
    const payload = dlq
      ? event.payload
      : {
          delivery_ts: new Date().toISOString(),
          seq: event.seq,
          event_id: event.event_id,
          type: event.type,
          ts: event.ts,
          payload: event.payload
        };
    const schemaErrors = validateSchema(deliverySchema, payload, "$");
    if (schemaErrors.length > 0) {
      console.error("replay_webhook.schema.invalid", { seq: payload.seq, event_id: payload.event_id, errors: schemaErrors });
      process.exit(1);
    }
    if (dryRun) {
      console.log("replay_webhook.dry", { seq: payload.seq, event_id: payload.event_id });
      continue;
    }
    const result = await postWebhook(payload);
    if (!result.ok) {
      console.error("replay_webhook.fail", {
        seq: payload.seq,
        event_id: payload.event_id,
        status: result.status,
        error: result.error || null
      });
      process.exit(1);
    }
    console.log("replay_webhook.ok", { seq: payload.seq, event_id: payload.event_id, status: result.status });
  }
  console.log("replay_webhook.done", { delivered: limited.length });
};

run().catch((error) => {
  console.error("replay_webhook.error", { error: error.message });
  process.exit(1);
});
