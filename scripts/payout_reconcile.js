import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");

const ledgerFile = path.join(dataDir, "ledger.json");
const payoutsFile = path.join(dataDir, "payouts.json");

const readJsonFile = (filePath, label) => {
  if (!fs.existsSync(filePath)) {
    console.error(`${label}.missing`, { path: filePath });
    return null;
  }
  const raw = fs.readFileSync(filePath, "utf8");
  if (!raw.trim()) {
    console.error(`${label}.empty`, { path: filePath });
    return null;
  }
  try {
    return JSON.parse(raw);
  } catch (error) {
    console.error(`${label}.parse_failed`, { path: filePath, error: error.message });
    return null;
  }
};

const ledgerPayload = readJsonFile(ledgerFile, "payout.reconcile.ledger");
const payoutsPayload = readJsonFile(payoutsFile, "payout.reconcile.payouts");

if (!ledgerPayload || !Array.isArray(ledgerPayload.entries)) {
  process.exit(1);
}
if (!payoutsPayload || !Array.isArray(payoutsPayload.runs)) {
  process.exit(1);
}

const ledgerById = new Map();
ledgerPayload.entries.forEach((entry) => {
  if (!entry || entry.billable !== true) {
    return;
  }
  if (!entry.entry_id) {
    return;
  }
  ledgerById.set(entry.entry_id, entry);
});

const payoutEntryIds = new Set();
let mismatches = 0;

payoutsPayload.runs.forEach((run) => {
  if (!run || !Array.isArray(run.entry_ids)) {
    return;
  }
  let payoutSum = 0;
  let missing = 0;
  run.entry_ids.forEach((entryId) => {
    payoutEntryIds.add(entryId);
    const entry = ledgerById.get(entryId);
    if (!entry) {
      missing += 1;
      return;
    }
    payoutSum += Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0;
  });
  const expected = payoutSum;
  const actual = Number.isFinite(run.payout_cents) ? run.payout_cents : 0;
  if (missing > 0 || expected !== actual) {
    mismatches += 1;
    console.error("payout.reconcile.mismatch", {
      run_id: run.run_id || null,
      publisher_id: run.publisher_id || null,
      window_id: run.window_id || null,
      missing_entries: missing,
      expected_payout_cents: expected,
      run_payout_cents: actual
    });
    return;
  }
  console.log("payout.reconcile.ok", {
    run_id: run.run_id || null,
    publisher_id: run.publisher_id || null,
    window_id: run.window_id || null,
    entry_count: run.entry_count || run.entry_ids.length
  });
});

const unassigned = [];
ledgerById.forEach((entry, entryId) => {
  if (!payoutEntryIds.has(entryId)) {
    unassigned.push(entryId);
  }
});

if (unassigned.length > 0) {
  mismatches += 1;
  console.error("payout.reconcile.unassigned", { count: unassigned.length });
}

if (mismatches > 0) {
  console.error("payout.reconcile.failed", { mismatches });
  process.exit(1);
}

console.log("payout.reconcile.complete", {
  runs: payoutsPayload.runs.length,
  entries: ledgerById.size
});
