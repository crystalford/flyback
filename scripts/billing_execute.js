import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");

const ledgerFile = path.join(dataDir, "ledger.json");
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
    console.error("billing.execute.read.failed", { path: filePath, error: error.message });
    return null;
  }
};

const writeJsonFile = (filePath, payload) => {
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`);
};

const parseArgs = () => {
  const args = process.argv.slice(2);
  const result = { dryRun: false };
  args.forEach((arg) => {
    if (arg === "--dry-run") {
      result.dryRun = true;
    }
  });
  return result;
};

const { dryRun } = parseArgs();

const ledgerPayload = readJsonFile(ledgerFile);
if (!ledgerPayload || !Array.isArray(ledgerPayload.entries)) {
  console.error("billing.execute.ledger.missing", { path: ledgerFile });
  process.exit(1);
}

const existing = readJsonFile(payoutsFile);
const payoutState = existing && Array.isArray(existing.runs) ? existing : { version: 1, runs: [] };

const appliedEntryIds = new Set();
payoutState.runs.forEach((run) => {
  if (Array.isArray(run.entry_ids)) {
    run.entry_ids.forEach((entryId) => appliedEntryIds.add(entryId));
  }
});

const grouped = new Map();
ledgerPayload.entries.forEach((entry) => {
  if (!entry || entry.billable !== true) {
    return;
  }
  if (!entry.entry_id) {
    return;
  }
  if (appliedEntryIds.has(entry.entry_id)) {
    console.log("billing.execute.skip", { entry_id: entry.entry_id, reason: "already_applied" });
    return;
  }
  const windowId = entry.window_id || "unknown";
  const key = `${entry.publisher_id}:${windowId}`;
  const current = grouped.get(key) || {
    publisher_id: entry.publisher_id,
    window_id: windowId,
    entry_ids: [],
    entry_count: 0,
    payout_cents: 0
  };
  current.entry_ids.push(entry.entry_id);
  current.entry_count += 1;
  current.payout_cents += Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0;
  grouped.set(key, current);
});

const runs = Array.from(grouped.values()).sort((a, b) => {
  if (a.publisher_id === b.publisher_id) {
    return a.window_id.localeCompare(b.window_id);
  }
  return a.publisher_id.localeCompare(b.publisher_id);
});

if (runs.length === 0) {
  console.log("billing.execute.noop", { reason: "no_new_entries" });
  process.exit(0);
}

if (dryRun) {
  console.log("billing.execute.dry_run", { runs: runs.length });
  runs.forEach((run) => {
    console.log("billing.execute.run", run);
  });
  process.exit(0);
}

runs.forEach((run) => {
  const record = {
    run_id: randomUUID(),
    created_at: new Date().toISOString(),
    publisher_id: run.publisher_id,
    window_id: run.window_id,
    entry_count: run.entry_count,
    payout_cents: run.payout_cents,
    entry_ids: run.entry_ids,
    status: "pending"
  };
  payoutState.runs.push(record);
  console.log("billing.execute.append", {
    run_id: record.run_id,
    publisher_id: record.publisher_id,
    payout_cents: record.payout_cents,
    entry_count: record.entry_count
  });
});

writeJsonFile(payoutsFile, payoutState);
console.log("billing.execute.ok", { runs: runs.length, path: payoutsFile });
