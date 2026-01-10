import fs from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import { spawn, spawnSync } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = path.join(rootDir, "data");
const serverPath = path.join(rootDir, "server.js");
const reconcilePath = path.join(rootDir, "scripts", "reconcile.js");

const seedRandom = (seed) => {
  let t = seed;
  return () => {
    t += 0x6d2b79f5;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
};

const copyDataDir = (destDir) => {
  fs.cpSync(dataDir, destDir, { recursive: true });
  fs.writeFileSync(path.join(destDir, "tokens.json"), "[]\n");
};

const startServer = async (port, dataPath) => {
  const env = {
    ...process.env,
    PORT: String(port),
    START_SERVER: "true",
    FLYBACK_DATA_DIR: dataPath
  };
  const proc = spawn(process.execPath, [serverPath], { env, stdio: ["ignore", "pipe", "pipe"] });
  const ready = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("server_start_timeout")), 5000);
    proc.stdout.on("data", (chunk) => {
      if (chunk.toString().includes("Flyback server listening")) {
        clearTimeout(timeout);
        resolve();
      }
    });
  });
  await ready;
  return proc;
};

const stopServer = (proc) =>
  new Promise((resolve) => {
    proc.on("exit", resolve);
    proc.kill();
  });

const jsonFetch = async (url, options) => {
  const res = await fetch(url, options);
  const payload = await res.json();
  return { status: res.status, payload };
};

const runStress = async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-stress-"));
  copyDataDir(tempDir);
  const port = 3301;
  const server = await startServer(port, tempDir);
  const baseUrl = `http://127.0.0.1:${port}`;

  const rng = seedRandom(1337);
  const tokens = [];
  const intentCount = 25;
  for (let i = 0; i < intentCount; i += 1) {
    const intent = await jsonFetch(`${baseUrl}/v1/intent`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": "demo-publisher-key"
      },
      body: JSON.stringify({
        campaign_id: "campaign-v1",
        publisher_id: "publisher-demo",
        creative_id: rng() > 0.5 ? "creative-v1" : "creative-v2",
        intent_type: "qualified"
      })
    });
    if (intent.status !== 200) {
      throw new Error("intent_failed");
    }
    tokens.push(intent.payload.token.token_id);
  }

  const tasks = [];
  tokens.forEach((tokenId) => {
    const final = `${baseUrl}/v1/postback?token_id=${tokenId}&value=4&stage=purchase&outcome_type=purchase`;
    const partial = `${baseUrl}/v1/postback?token_id=${tokenId}&value=1&stage=lead`;
    const ordered = rng() > 0.5 ? [final, partial] : [partial, final];
    ordered.forEach((url) => {
      tasks.push(() =>
        jsonFetch(url, {
          method: "GET",
          headers: { "x-api-key": "demo-advertiser-key" }
        })
      );
    });
  });

  const concurrency = 8;
  let index = 0;
  const workers = new Array(concurrency).fill(0).map(async () => {
    while (index < tasks.length) {
      const current = index;
      index += 1;
      await tasks[current]();
    }
  });
  await Promise.all(workers);

  const reconcile = spawnSync(process.execPath, [reconcilePath], {
    env: { ...process.env, FLYBACK_DATA_DIR: tempDir },
    encoding: "utf8"
  });
  await stopServer(server);

  if (reconcile.status !== 0) {
    console.log(`STRESS FAIL reconcile_exit_${reconcile.status}`);
    return;
  }
  const mismatchLine = reconcile.stdout.split("\n").find((line) => line.includes("mismatch"));
  if (mismatchLine) {
    console.log(`STRESS FAIL ${mismatchLine.trim()}`);
    return;
  }
  console.log("STRESS OK");
};

runStress().catch((error) => {
  console.log(`STRESS FAIL ${error.message}`);
});
