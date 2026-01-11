import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const ledgerFile = path.join(dataDir, "ledger.json");

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
    console.error("billing.preview.read.failed", { path: filePath, error: error.message });
    return null;
  }
};

const ledgerPayload = readJsonFile(ledgerFile);
if (!ledgerPayload || !Array.isArray(ledgerPayload.entries)) {
  console.error("billing.preview.ledger.missing", { path: ledgerFile });
  process.exit(1);
}

const totals = new Map();
ledgerPayload.entries.forEach((entry) => {
  const key = `${entry.campaign_id}:${entry.publisher_id}`;
  const current = totals.get(key) || {
    campaign_id: entry.campaign_id,
    publisher_id: entry.publisher_id,
    advertiser_id: entry.advertiser_id,
    payout_cents: 0,
    entries: 0
  };
  current.payout_cents += Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0;
  current.entries += 1;
  totals.set(key, current);
});

const rows = Array.from(totals.values()).sort((a, b) => b.payout_cents - a.payout_cents);
console.log("billing.preview", { rows: rows.length });
rows.forEach((row) => {
  console.log("billing.preview.row", row);
});
