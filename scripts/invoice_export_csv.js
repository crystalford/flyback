import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const invoicesDir = path.join(dataDir, "invoices");

const outputPath = path.join(dataDir, "invoice_drafts.csv");

if (!fs.existsSync(invoicesDir)) {
  console.error("invoice.export.missing", { path: invoicesDir });
  process.exit(1);
}

const files = fs.readdirSync(invoicesDir).filter((name) => name.endsWith(".json"));
const rows = [];

files.forEach((filename) => {
  const filePath = path.join(invoicesDir, filename);
  try {
    const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));
    rows.push({
      filename,
      advertiser_id: payload.advertiser_id || "unknown",
      payout_cents: Number.isFinite(payload.payout_cents) ? payload.payout_cents : 0,
      entry_count: Number.isFinite(payload.entry_count) ? payload.entry_count : 0,
      created_at: payload.created_at || ""
    });
  } catch (error) {
    console.log("invoice.export.skip", { filename, error: error.message });
  }
});

rows.sort((a, b) => (b.created_at || "").localeCompare(a.created_at || ""));

const header = "filename,advertiser_id,payout_cents,entry_count,created_at\n";
const body = rows
  .map((row) =>
    [row.filename, row.advertiser_id, row.payout_cents, row.entry_count, row.created_at]
      .map((value) => String(value).replace(/"/g, "\"\""))
      .map((value) => `"${value}"`)
      .join(",")
  )
  .join("\n");

fs.writeFileSync(outputPath, header + body + (body ? "\n" : ""));
console.log("invoice.export.csv.ok", { path: outputPath, rows: rows.length });
