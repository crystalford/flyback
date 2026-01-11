import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");

const ledgerFile = path.join(dataDir, "ledger.json");
const payoutsFile = path.join(dataDir, "payouts.json");
const outputJson = path.join(dataDir, "publisher_statements.json");
const outputCsv = path.join(dataDir, "publisher_statements.csv");

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

const ledgerPayload = readJsonFile(ledgerFile, "publisher.statement.ledger");
if (!ledgerPayload || !Array.isArray(ledgerPayload.entries)) {
  process.exit(1);
}

const payoutsPayload = readJsonFile(payoutsFile, "publisher.statement.payouts");
const payoutRuns = payoutsPayload && Array.isArray(payoutsPayload.runs) ? payoutsPayload.runs : [];

const statements = new Map();

ledgerPayload.entries.forEach((entry) => {
  if (!entry || entry.billable !== true) {
    return;
  }
  const publisherId = entry.publisher_id || "unknown";
  const current = statements.get(publisherId) || {
    publisher_id: publisherId,
    ledger_entries: 0,
    ledger_payout_cents: 0,
    payout_runs: 0,
    payout_pending_cents: 0,
    payout_sent_cents: 0,
    payout_settled_cents: 0,
    last_run_at: null
  };
  current.ledger_entries += 1;
  current.ledger_payout_cents += Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0;
  statements.set(publisherId, current);
});

payoutRuns.forEach((run) => {
  if (!run) {
    return;
  }
  const publisherId = run.publisher_id || "unknown";
  const current = statements.get(publisherId) || {
    publisher_id: publisherId,
    ledger_entries: 0,
    ledger_payout_cents: 0,
    payout_runs: 0,
    payout_pending_cents: 0,
    payout_sent_cents: 0,
    payout_settled_cents: 0,
    last_run_at: null
  };
  current.payout_runs += 1;
  const cents = Number.isFinite(run.payout_cents) ? run.payout_cents : 0;
  if (run.status === "sent") {
    current.payout_sent_cents += cents;
  } else if (run.status === "settled") {
    current.payout_settled_cents += cents;
  } else {
    current.payout_pending_cents += cents;
  }
  if (!current.last_run_at || String(run.created_at || "") > String(current.last_run_at || "")) {
    current.last_run_at = run.created_at || current.last_run_at;
  }
  statements.set(publisherId, current);
});

const rows = Array.from(statements.values()).sort((a, b) => b.ledger_payout_cents - a.ledger_payout_cents);

fs.writeFileSync(outputJson, `${JSON.stringify({ generated_at: new Date().toISOString(), rows }, null, 2)}\n`);

const header =
  "publisher_id,ledger_entries,ledger_payout_cents,payout_runs,payout_pending_cents,payout_sent_cents,payout_settled_cents,last_run_at\n";
const body = rows
  .map((row) =>
    [
      row.publisher_id,
      row.ledger_entries,
      row.ledger_payout_cents,
      row.payout_runs,
      row.payout_pending_cents,
      row.payout_sent_cents,
      row.payout_settled_cents,
      row.last_run_at || ""
    ]
      .map((value) => String(value).replace(/\"/g, "\"\""))
      .map((value) => `"${value}"`)
      .join(",")
  )
  .join("\n");

fs.writeFileSync(outputCsv, header + body + (body ? "\n" : ""));
console.log("publisher.statement.ok", { json: outputJson, csv: outputCsv, rows: rows.length });
