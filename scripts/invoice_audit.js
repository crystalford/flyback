import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const ledgerFile = path.join(dataDir, "ledger.json");
const invoicesDir = path.join(dataDir, "invoices");

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
    console.error("invoice.audit.read.failed", { path: filePath, error: error.message });
    return null;
  }
};

const ledgerPayload = readJsonFile(ledgerFile);
if (!ledgerPayload || !Array.isArray(ledgerPayload.entries)) {
  console.error("invoice.audit.ledger.missing", { path: ledgerFile });
  process.exit(1);
}

const ledgerTotals = new Map();
ledgerPayload.entries.forEach((entry) => {
  if (!entry || entry.billable !== true) {
    return;
  }
  const advertiserId = entry.advertiser_id || "unknown";
  const current = ledgerTotals.get(advertiserId) || 0;
  ledgerTotals.set(advertiserId, current + (Number.isFinite(entry.payout_cents) ? entry.payout_cents : 0));
});

const invoiceTotals = new Map();
if (fs.existsSync(invoicesDir)) {
  const files = fs.readdirSync(invoicesDir).filter((name) => name.endsWith(".json"));
  files.forEach((filename) => {
    const payload = readJsonFile(path.join(invoicesDir, filename));
    if (!payload) {
      return;
    }
    const advertiserId = payload.advertiser_id || "unknown";
    const current = invoiceTotals.get(advertiserId) || 0;
    invoiceTotals.set(advertiserId, current + (Number.isFinite(payload.payout_cents) ? payload.payout_cents : 0));
  });
}

let mismatches = 0;
ledgerTotals.forEach((ledgerSum, advertiserId) => {
  const invoiceSum = invoiceTotals.get(advertiserId) || 0;
  if (ledgerSum !== invoiceSum) {
    mismatches += 1;
    console.log("invoice.audit.mismatch", {
      advertiser_id: advertiserId,
      ledger_payout_cents: ledgerSum,
      invoice_payout_cents: invoiceSum
    });
  } else {
    console.log("invoice.audit.ok", { advertiser_id: advertiserId, payout_cents: ledgerSum });
  }
});

if (mismatches > 0) {
  console.log("invoice.audit.failed", { mismatches });
  process.exit(1);
}

console.log("invoice.audit.complete", { advertisers: ledgerTotals.size });
