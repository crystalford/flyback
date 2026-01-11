import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const payoutsFile = path.join(dataDir, "payouts.json");
const outputPath = path.join(dataDir, "payouts.csv");

if (!fs.existsSync(payoutsFile)) {
  console.error("payout.export.missing", { path: payoutsFile });
  process.exit(1);
}

const raw = fs.readFileSync(payoutsFile, "utf8");
if (!raw.trim()) {
  console.error("payout.export.empty", { path: payoutsFile });
  process.exit(1);
}

let payload = null;
try {
  payload = JSON.parse(raw);
} catch (error) {
  console.error("payout.export.parse_failed", { path: payoutsFile, error: error.message });
  process.exit(1);
}

const runs = Array.isArray(payload.runs) ? payload.runs : [];
const rows = runs.map((run) => ({
  run_id: run.run_id || "",
  publisher_id: run.publisher_id || "",
  window_id: run.window_id || "",
  payout_cents: Number.isFinite(run.payout_cents) ? run.payout_cents : 0,
  entry_count: Number.isFinite(run.entry_count) ? run.entry_count : 0,
  status: run.status || "",
  created_at: run.created_at || "",
  updated_at: run.updated_at || ""
}));

const header = "run_id,publisher_id,window_id,payout_cents,entry_count,status,created_at,updated_at\n";
const body = rows
  .map((row) =>
    [
      row.run_id,
      row.publisher_id,
      row.window_id,
      row.payout_cents,
      row.entry_count,
      row.status,
      row.created_at,
      row.updated_at
    ]
      .map((value) => String(value).replace(/\"/g, "\"\""))
      .map((value) => `"${value}"`)
      .join(",")
  )
  .join("\n");

fs.writeFileSync(outputPath, header + body + (body ? "\n" : ""));
console.log("payout.export.csv.ok", { path: outputPath, rows: rows.length });
