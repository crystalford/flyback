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
const ledgerFile = path.join(dataDir, "ledger.json");
const deliveryStateFile = path.join(dataDir, "delivery_state.json");
const deliveryDlqFile = path.join(dataDir, "delivery_dlq.ndjson");

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
    return { error: error.message };
  }
};

const readDlqStats = () => {
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
      lastEntry = {
        failed_at: parsed.failed_at,
        seq: parsed.seq,
        event_id: parsed.event_id,
        status: parsed.status,
        error: parsed.error
      };
    } catch {
      lastEntry = null;
    }
  }
  return { count: lines.length, last_entry: lastEntry };
};

const registry = readJsonFile(registryFile);
const budgets = readJsonFile(budgetsFile);
const aggregates = readJsonFile(aggregatesFile);
const ledger = readJsonFile(ledgerFile);
const deliveryState = readJsonFile(deliveryStateFile);
const dlqStats = readDlqStats();

const ledgerStats = Array.isArray(ledger?.entries)
  ? ledger.entries.reduce(
      (acc, entry) => {
        acc.entries += 1;
        acc.payout_cents += Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0;
        return acc;
      },
      { entries: 0, payout_cents: 0 }
    )
  : { entries: 0, payout_cents: 0 };

const snapshot = {
  ts: new Date().toISOString(),
  registry: registry
    ? {
        publishers: registry.publishers?.length || 0,
        campaigns: registry.campaigns?.length || 0,
        creatives: registry.creatives?.length || 0
      }
    : null,
  budgets: budgets
    ? {
        campaigns: budgets.campaigns?.length || 0
      }
    : null,
  aggregates: aggregates
    ? {
        window_start: aggregates.window?.started_at || null
      }
    : null,
  ledger: ledgerStats,
  delivery: {
    state: deliveryState || null,
    dlq: dlqStats
  }
};

console.log("health.snapshot", snapshot);
