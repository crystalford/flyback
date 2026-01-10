import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import { spawn, spawnSync } from "child_process";
import net from "net";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = path.join(rootDir, "data");
const scenarioPath = path.join(rootDir, "tests", "scenario.js");

const copyDataDir = (destDir) => {
  fs.cpSync(dataDir, destDir, { recursive: true });
  fs.writeFileSync(path.join(destDir, "tokens.json"), "[]\n");
};

const runScenario = (args, envOverrides = {}) => {
  const env = { ...process.env, START_SERVER: "false", ...envOverrides };
  return spawnSync(process.execPath, [scenarioPath, ...args], {
    env,
    encoding: "utf8"
  });
};

const parseScenarioJson = (stdout) => {
  if (!stdout) {
    return {};
  }
  const lines = stdout.trim().split(/\r?\n/);
  for (let idx = lines.length - 1; idx >= 0; idx -= 1) {
    const line = lines[idx].trim();
    if (!line.startsWith("{")) {
      continue;
    }
    try {
      return JSON.parse(line);
    } catch (error) {
      return {};
    }
  }
  return {};
};

const getAvailablePort = () =>
  new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });

const startServer = async (dataPath, port) => {
  const env = {
    ...process.env,
    PORT: String(port),
    START_SERVER: "true",
    FLYBACK_DATA_DIR: dataPath
  };
  const proc = spawn(process.execPath, [path.join(rootDir, "server.js")], {
    env,
    stdio: ["ignore", "pipe", "pipe"]
  });
  let stdoutBuffer = "";
  let stderrBuffer = "";
  proc.stdout.on("data", (chunk) => {
    stdoutBuffer += chunk.toString();
  });
  proc.stderr.on("data", (chunk) => {
    stderrBuffer += chunk.toString();
  });
  const ready = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("server_start_timeout")), 5000);
    proc.once("exit", (code) => {
      clearTimeout(timeout);
      const combined = `${stderrBuffer}\n${stdoutBuffer}`.trim();
      const tail = combined ? combined.split(/\r?\n/).slice(-5).join(" | ") : "";
      const detail = tail ? `:${tail}` : "";
      reject(new Error(`server_start_exit_${code}${detail}`));
    });
    proc.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      if (text.includes("Flyback server listening")) {
        clearTimeout(timeout);
        resolve(port);
      }
    });
  });
  await ready;
  return { proc, port };
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

test("batch atomicity rolls back on reducer failure", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-test-"));
  copyDataDir(tempDir);

  const result = runScenario(["atomicity"], { FLYBACK_DATA_DIR: tempDir });
  assert.notEqual(result.status, 0);

  const tokensPayload = JSON.parse(fs.readFileSync(path.join(tempDir, "tokens.json"), "utf8"));
  assert.equal(tokensPayload.length, 0);
});

test("read consistency returns post-batch state", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-test-"));
  copyDataDir(tempDir);

  const port = await getAvailablePort();
  const { proc: server } = await startServer(tempDir, port);
  const baseUrl = `http://127.0.0.1:${port}`;

  const intent = await jsonFetch(`${baseUrl}/v1/intent`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": "demo-publisher-key"
    },
    body: JSON.stringify({
      campaign_id: "campaign-v1",
      publisher_id: "publisher-demo",
      creative_id: "creative-v1",
      intent_type: "qualified"
    })
  });
  assert.equal(intent.status, 200);
  const tokenId = intent.payload.token.token_id;

  const postbackUrl = `${baseUrl}/v1/postback?token_id=${tokenId}&value=5&stage=purchase&outcome_type=purchase`;
  const reportsUrl = `${baseUrl}/v1/reports`;

  const postback = await jsonFetch(postbackUrl, { method: "GET", headers: { "x-api-key": "demo-advertiser-key" } });
  assert.equal(postback.status, 200);
  const reports = await jsonFetch(reportsUrl, { method: "GET", headers: { "x-api-key": "demo-publisher-key" } });
  const aggregates = reports.payload.reports.aggregates || [];
  const row = aggregates.find((entry) => entry.campaign_id === "campaign-v1");
  assert.ok(row);
  assert.equal(row.resolvedIntents, 1);

  await stopServer(server);
});

test("event_id dedupe persists across restart", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-test-"));
  copyDataDir(tempDir);

  const eventId = "event-dedupe-test";
  runScenario(["append", eventId], { FLYBACK_DATA_DIR: tempDir });
  const firstCount = runScenario(["event-count"], { FLYBACK_DATA_DIR: tempDir });
  const firstPayload = parseScenarioJson(firstCount.stdout);

  runScenario(["append", eventId], { FLYBACK_DATA_DIR: tempDir });
  const secondCount = runScenario(["event-count"], { FLYBACK_DATA_DIR: tempDir });
  const secondPayload = parseScenarioJson(secondCount.stdout);

  assert.equal(firstPayload.lines, 1);
  assert.equal(secondPayload.lines, 1);
});

test("partial after final is recorded without extra budget impact", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-test-"));
  copyDataDir(tempDir);

  const port = await getAvailablePort();
  const { proc: server } = await startServer(tempDir, port);
  const baseUrl = `http://127.0.0.1:${port}`;

  const intent = await jsonFetch(`${baseUrl}/v1/intent`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": "demo-publisher-key"
    },
    body: JSON.stringify({
      campaign_id: "campaign-v1",
      publisher_id: "publisher-demo",
      creative_id: "creative-v1",
      intent_type: "qualified"
    })
  });
  const tokenId = intent.payload.token.token_id;

  await jsonFetch(
    `${baseUrl}/v1/postback?token_id=${tokenId}&value=10&stage=purchase&outcome_type=purchase`,
    { method: "GET", headers: { "x-api-key": "demo-advertiser-key" } }
  );
  const budgetAfterFinal = JSON.parse(fs.readFileSync(path.join(tempDir, "budgets.json"), "utf8"));
  const remainingAfterFinal = budgetAfterFinal.campaigns.find((entry) => entry.campaign_id === "campaign-v1")
    .remaining;

  const partial = await jsonFetch(`${baseUrl}/v1/postback?token_id=${tokenId}&value=2&stage=lead`, {
    method: "GET",
    headers: { "x-api-key": "demo-advertiser-key" }
  });

  const budgetAfterPartial = JSON.parse(fs.readFileSync(path.join(tempDir, "budgets.json"), "utf8"));
  const remainingAfterPartial = budgetAfterPartial.campaigns.find((entry) => entry.campaign_id === "campaign-v1")
    .remaining;

  assert.equal(remainingAfterFinal, remainingAfterPartial);
  assert.ok(Array.isArray(partial.payload.token.resolution_events));
  assert.ok(partial.payload.token.resolution_events.some((event) => event.stage === "lead"));

  await stopServer(server);
});
