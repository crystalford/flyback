import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const eventsFile = path.join(dataDir, "events.ndjson");

const WEBHOOK_URL = process.env.WEBHOOK_URL || "";
const WEBHOOK_TIMEOUT_MS = Number(process.env.WEBHOOK_TIMEOUT_MS) || 5000;

const parseArgs = () => {
  const args = process.argv.slice(2);
  const result = { from: 1, to: null, dryRun: false };
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
  const { from, to, dryRun } = parseArgs();
  const events = readEvents();
  const eligible = events.filter((event) => {
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

  console.log("replay_webhook.start", {
    from,
    to: Number.isFinite(to) ? to : null,
    events: eligible.length,
    dry_run: dryRun
  });

  for (const event of eligible) {
    const payload = {
      delivery_ts: new Date().toISOString(),
      seq: event.seq,
      event_id: event.event_id,
      type: event.type,
      ts: event.ts,
      payload: event.payload
    };
    if (dryRun) {
      console.log("replay_webhook.dry", { seq: event.seq, event_id: event.event_id });
      continue;
    }
    const result = await postWebhook(payload);
    if (!result.ok) {
      console.error("replay_webhook.fail", {
        seq: event.seq,
        event_id: event.event_id,
        status: result.status,
        error: result.error || null
      });
      process.exit(1);
    }
    console.log("replay_webhook.ok", { seq: event.seq, event_id: event.event_id, status: result.status });
  }
  console.log("replay_webhook.done", { delivered: eligible.length });
};

run().catch((error) => {
  console.error("replay_webhook.error", { error: error.message });
  process.exit(1);
});
