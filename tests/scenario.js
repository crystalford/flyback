import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";

process.env.START_SERVER = "false";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");

const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");

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

const createToken = ({ campaignId, publisherId, creativeId }) => {
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
      intent_type: "qualified",
      dwell_seconds: 1,
      interaction_count: 1
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
    parent_intent_id: null,
    resolution_events: []
  };
};

const scenario = process.argv[2];
const eventId = process.argv[3];

const { __test } = await import(path.join(rootDir, "server.js"));

if (scenario === "append") {
  const token = createToken({
    campaignId: "campaign-v1",
    publisherId: "publisher-demo",
    creativeId: "creative-v1"
  });
  const events = __test.appendEventBatch(
    [
      {
        type: "intent.created",
        event_id: eventId,
        payload: { token }
      }
    ],
    "test.append"
  );
  __test.applyProjectionEvents(events, "test.append");
  console.log(JSON.stringify({ appended: Array.isArray(events) ? events.length : 0 }));
  process.exit(0);
}

if (scenario === "atomicity") {
  const token = createToken({
    campaignId: "campaign-v1",
    publisherId: "publisher-demo",
    creativeId: "creative-v1"
  });
  __test.setReducerFailpoint("resolution.final");
  const events = [
    {
      seq: 1,
      event_id: randomUUID(),
      ts: new Date().toISOString(),
      type: "intent.created",
      payload: { token }
    },
    {
      seq: 2,
      event_id: randomUUID(),
      ts: new Date().toISOString(),
      type: "resolution.final",
      payload: {
        token_id: token.token_id,
        stage: "purchase",
        resolved_at: new Date().toISOString(),
        resolved_value: 10,
        outcome_type: "purchase",
        weighted_value: 10,
        billable: true
      }
    }
  ];
  __test.applyProjectionEvents(events, "test.atomicity");
  process.exit(0);
}

if (scenario === "event-count") {
  const eventsPath = path.join(dataDir, "events.ndjson");
  if (!fs.existsSync(eventsPath)) {
    console.log(JSON.stringify({ lines: 0 }));
    process.exit(0);
  }
  const raw = fs.readFileSync(eventsPath, "utf8");
  const lines = raw.split("\n").filter((line) => line.trim().length > 0);
  console.log(JSON.stringify({ lines: lines.length }));
  process.exit(0);
}

if (scenario === "tokens-count") {
  const tokens = readJsonFile(path.join(dataDir, "tokens.json")) || [];
  console.log(JSON.stringify({ tokens: tokens.length }));
  process.exit(0);
}

console.error("scenario.unknown", { scenario });
process.exit(1);
