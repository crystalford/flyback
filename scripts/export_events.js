import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHash } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");
const eventsFile = path.join(dataDir, "events.ndjson");

const parseArgs = () => {
  const args = process.argv.slice(2);
  const result = { from: 1, to: null, out: path.join(process.cwd(), "events_export.ndjson") };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === "--from" && args[i + 1]) {
      result.from = Number(args[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--to" && args[i + 1]) {
      result.to = Number(args[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--out" && args[i + 1]) {
      result.out = args[i + 1];
      i += 1;
    }
  }
  return result;
};

const readEvents = () => {
  if (!fs.existsSync(eventsFile)) {
    return [];
  }
  const raw = fs.readFileSync(eventsFile, "utf8");
  if (!raw.trim()) {
    return [];
  }
  return raw
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line))
    .sort((a, b) => a.seq - b.seq);
};

const run = () => {
  const { from, to, out } = parseArgs();
  if (!Number.isFinite(from)) {
    console.error("export.events.invalid_from");
    process.exit(1);
  }
  const events = readEvents();
  const filtered = events.filter((event) => {
    if (!Number.isFinite(event.seq)) {
      return false;
    }
    if (event.seq < from) {
      return false;
    }
    if (Number.isFinite(to) && event.seq > to) {
      return false;
    }
    return true;
  });
  const outPath = path.resolve(out);
  const payload = filtered.map((event) => `${JSON.stringify(event)}\n`).join("");
  fs.writeFileSync(outPath, payload);
  const hash = createHash("sha256").update(payload).digest("hex");
  const hashPath = `${outPath}.sha256`;
  fs.writeFileSync(hashPath, `${hash}\n`);
  console.log("export.events.ok", {
    from,
    to: Number.isFinite(to) ? to : null,
    out: outPath,
    count: filtered.length,
    sha256: hash
  });
};

run();
