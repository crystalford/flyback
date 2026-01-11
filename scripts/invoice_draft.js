import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const ledgerFile = path.join(dataDir, "ledger.json");
const outDir = path.join(dataDir, "invoices");

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
    console.error("invoice.draft.read.failed", { path: filePath, error: error.message });
    return null;
  }
};

const ledgerPayload = readJsonFile(ledgerFile);
if (!ledgerPayload || !Array.isArray(ledgerPayload.entries)) {
  console.error("invoice.draft.ledger.missing", { path: ledgerFile });
  process.exit(1);
}

if (!fs.existsSync(outDir)) {
  fs.mkdirSync(outDir, { recursive: true });
}

const byAdvertiser = new Map();
ledgerPayload.entries.forEach((entry) => {
  if (!entry || entry.billable !== true) {
    return;
  }
  const advertiserId = entry.advertiser_id || "unknown";
  const current = byAdvertiser.get(advertiserId) || {
    advertiser_id: advertiserId,
    payout_cents: 0,
    entries: []
  };
  current.payout_cents += Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0;
  current.entries.push(entry);
  byAdvertiser.set(advertiserId, current);
});

const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
let written = 0;
byAdvertiser.forEach((draft) => {
  const filename = `invoice_${draft.advertiser_id}_${timestamp}.json`;
  const payload = {
    created_at: new Date().toISOString(),
    advertiser_id: draft.advertiser_id,
    payout_cents: draft.payout_cents,
    entry_count: draft.entries.length,
    entries: draft.entries
  };
  fs.writeFileSync(path.join(outDir, filename), `${JSON.stringify(payload, null, 2)}\n`);
  written += 1;
  console.log("invoice.draft.written", { advertiser_id: draft.advertiser_id, path: filename });
});

console.log("invoice.draft.complete", { drafts: written, out_dir: outDir });
