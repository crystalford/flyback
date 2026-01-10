import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import { spawn, spawnSync } from "child_process";

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

const startServer = async (port, dataPath) => {
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

  const port = 3201;
  const server = await startServer(port, tempDir);
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
  const firstPayload = JSON.parse(firstCount.stdout || "{}");

  runScenario(["append", eventId], { FLYBACK_DATA_DIR: tempDir });
  const secondCount = runScenario(["event-count"], { FLYBACK_DATA_DIR: tempDir });
  const secondPayload = JSON.parse(secondCount.stdout || "{}");

  assert.equal(firstPayload.lines, 1);
  assert.equal(secondPayload.lines, 1);
});

test("partial after final is recorded without extra budget impact", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-test-"));
  copyDataDir(tempDir);

  const port = 3202;
  const server = await startServer(port, tempDir);
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
