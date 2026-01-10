import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");

const eventsFile = path.join(dataDir, "events.ndjson");
const snapshotFile = path.join(dataDir, "snapshot.json");
const eventStateFile = path.join(dataDir, "event_state.json");
const lockPath = (filePath) => `${filePath}.lock`;

const readJsonFile = (filePath) => {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const raw = fs.readFileSync(filePath, "utf8");
  if (!raw.trim()) {
    return null;
  }
  return JSON.parse(raw);
};

const acquireLock = (filePath) => {
  const lockFile = lockPath(filePath);
  const timeoutMs = Number(process.env.LOCK_TIMEOUT_MS) || 5000;
  const retryMs = Number(process.env.LOCK_RETRY_MS) || 50;
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const fd = fs.openSync(lockFile, "wx");
      fs.writeSync(fd, JSON.stringify({ pid: process.pid, ts: new Date().toISOString() }));
      fs.closeSync(fd);
      console.log("lock.acquire", { file: filePath });
      return lockFile;
    } catch (error) {
      if (error.code !== "EEXIST") {
        throw error;
      }
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, retryMs);
  }
  console.log("lock.timeout", { file: filePath });
  return null;
};

const releaseLock = (lockFile) => {
  if (!lockFile) {
    return;
  }
  const file = lockFile.replace(/\.lock$/, "");
  if (fs.existsSync(lockFile)) {
    fs.unlinkSync(lockFile);
  }
  console.log("lock.release", { file });
};

const eventLock = acquireLock(eventsFile);
const snapshotLock = acquireLock(snapshotFile);
const stateLock = acquireLock(eventStateFile);
if (!eventLock || !snapshotLock || !stateLock) {
  releaseLock(stateLock);
  releaseLock(snapshotLock);
  releaseLock(eventLock);
  console.error("compact.failed", { reason: "lock_timeout" });
  process.exit(1);
}

const snapshot = readJsonFile(snapshotFile);
if (!snapshot || !Number.isFinite(snapshot.snapshot_seq)) {
  console.error("compact.failed", { reason: "snapshot_missing" });
  process.exit(1);
}

if (!fs.existsSync(eventsFile)) {
  console.error("compact.failed", { reason: "events_missing" });
  process.exit(1);
}

const raw = fs.readFileSync(eventsFile, "utf8");
const lines = raw.split("\n").filter((line) => line.trim().length > 0);
const events = lines.map((line) => JSON.parse(line));
const remaining = events.filter((event) => event.seq > snapshot.snapshot_seq);
const tempFile = path.join(dataDir, "events.compacted.ndjson");
const output = remaining.map((event) => JSON.stringify(event)).join("\n");
fs.writeFileSync(tempFile, output ? `${output}\n` : "");

const backupFile = `${eventsFile}.bak`;
if (fs.existsSync(backupFile)) {
  fs.unlinkSync(backupFile);
}
fs.renameSync(eventsFile, backupFile);
fs.renameSync(tempFile, eventsFile);
fs.unlinkSync(backupFile);

const lastSeq = events.length > 0 ? events[events.length - 1].seq : 0;
fs.writeFileSync(eventStateFile, `${JSON.stringify({ last_seq: lastSeq, updated_at: new Date().toISOString() }, null, 2)}\n`);

console.log("compact.ok", {
  snapshot_seq: snapshot.snapshot_seq,
  before: events.length,
  after: remaining.length,
  last_seq: lastSeq
});

releaseLock(stateLock);
releaseLock(snapshotLock);
releaseLock(eventLock);
