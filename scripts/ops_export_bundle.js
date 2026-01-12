import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const outDir = path.join(dataDir, "exports");

const ensureDir = (dir) => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
};

const runNode = (script, args) => {
  const result = spawnSync(process.execPath, [path.join(__dirname, script), ...args], {
    env: { ...process.env, FLYBACK_DATA_DIR: dataDir },
    encoding: "utf8"
  });
  if (result.status !== 0) {
    console.error("ops.export.failed", { script, stderr: result.stderr.trim() });
    process.exit(1);
  }
};

ensureDir(outDir);

runNode("invoice_export_csv.js", []);
runNode("payout_export_csv.js", []);
runNode("publisher_statement.js", []);

const copyIfExists = (filename) => {
  const source = path.join(dataDir, filename);
  if (!fs.existsSync(source)) {
    return;
  }
  const dest = path.join(outDir, filename);
  fs.copyFileSync(source, dest);
};

copyIfExists("invoice_drafts.csv");
copyIfExists("payouts.csv");
copyIfExists("publisher_statements.csv");
copyIfExists("publisher_statements.json");
copyIfExists("ledger.json");

console.log("ops.export.bundle.ok", { outDir });
