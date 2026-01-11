import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const payoutsFile = path.join(dataDir, "payouts.json");

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
    console.error("payout.update.read.failed", { path: filePath, error: error.message });
    return null;
  }
};

const writeJsonFile = (filePath, payload) => {
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`);
};

const parseArgs = () => {
  const args = process.argv.slice(2);
  const result = { runId: null, publisherId: null, windowId: null, status: null };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === "--run-id" && args[i + 1]) {
      result.runId = args[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--publisher-id" && args[i + 1]) {
      result.publisherId = args[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--window-id" && args[i + 1]) {
      result.windowId = args[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--status" && args[i + 1]) {
      result.status = args[i + 1];
      i += 1;
    }
  }
  return result;
};

const { runId, publisherId, windowId, status } = parseArgs();
if (!status) {
  console.error("payout.update.missing_status", { usage: "--status <pending|sent|settled>" });
  process.exit(1);
}

const payload = readJsonFile(payoutsFile);
if (!payload || !Array.isArray(payload.runs)) {
  console.error("payout.update.missing", { path: payoutsFile });
  process.exit(1);
}

const matches = payload.runs.filter((run) => {
  if (runId && run.run_id !== runId) {
    return false;
  }
  if (publisherId && run.publisher_id !== publisherId) {
    return false;
  }
  if (windowId && run.window_id !== windowId) {
    return false;
  }
  if (!runId && !publisherId && !windowId) {
    return false;
  }
  return true;
});

if (matches.length === 0) {
  console.error("payout.update.no_match", { run_id: runId, publisher_id: publisherId, window_id: windowId });
  process.exit(1);
}

matches.forEach((run) => {
  if (!Array.isArray(run.status_history)) {
    run.status_history = [];
    if (run.status) {
      run.status_history.push({
        status: run.status,
        updated_at: run.updated_at || run.created_at || new Date().toISOString()
      });
    }
  }
  run.status = status;
  run.updated_at = new Date().toISOString();
  run.status_history.push({ status, updated_at: run.updated_at });
  console.log("payout.update.ok", {
    run_id: run.run_id,
    publisher_id: run.publisher_id,
    window_id: run.window_id,
    status
  });
});

writeJsonFile(payoutsFile, payload);
console.log("payout.update.write", { path: payoutsFile, updated: matches.length });
