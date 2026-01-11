import { test } from "node:test";
import assert from "node:assert/strict";
import http from "http";
import fs from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import { spawn, spawnSync } from "child_process";
import { createHmac } from "crypto";
import net from "net";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = path.join(rootDir, "data");
const schemaFile = path.join(rootDir, "schemas", "schemas.json");

const copyDataDir = (destDir) => {
  fs.cpSync(dataDir, destDir, { recursive: true });
  fs.writeFileSync(path.join(destDir, "tokens.json"), "[]\n");
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

const createWebhookServer = async ({ failFirst = false, secret = null } = {}) => {
  const received = [];
  const signatures = [];
  let first = true;
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });
    req.on("end", () => {
      let payload = null;
      try {
        payload = body ? JSON.parse(body) : null;
      } catch {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "invalid_json" }));
        return;
      }
      received.push(payload);
      if (secret) {
        const headerSig = req.headers["x-flyback-signature"];
        const expected = createHmac("sha256", secret).update(body).digest("hex");
        signatures.push({ header: headerSig, expected });
      }
      if (failFirst && first) {
        first = false;
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "fail_once" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "ok" }));
    });
  });

  const port = await getAvailablePort();
  await new Promise((resolve) => server.listen(port, resolve));

  return {
    url: `http://127.0.0.1:${port}`,
    received,
    signatures,
    close: () => new Promise((resolve) => server.close(resolve))
  };
};

const startFlyback = async (dataPath, extraEnv = {}) => {
  const port = await getAvailablePort();
  const env = {
    ...process.env,
    PORT: String(port),
    START_SERVER: "true",
    FLYBACK_DATA_DIR: dataPath,
    ...extraEnv
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
    proc.once("exit", (code) => reject(new Error(`server_start_exit_${code}`)));
  });
  await ready;
  return { proc, port };
};

const stopFlyback = (proc) =>
  new Promise((resolve) => {
    proc.on("exit", resolve);
    proc.kill();
  });

const jsonFetch = async (url, options) => {
  const res = await fetch(url, options);
  const payload = await res.json();
  return { status: res.status, payload };
};

const waitFor = async (predicate, timeoutMs = 5000) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return false;
};

const schemaTypeMatches = (type, value) => {
  switch (type) {
    case "object":
      return value !== null && typeof value === "object" && !Array.isArray(value);
    case "array":
      return Array.isArray(value);
    case "string":
      return typeof value === "string";
    case "number":
      return Number.isFinite(value);
    case "integer":
      return Number.isInteger(value);
    case "boolean":
      return typeof value === "boolean";
    case "null":
      return value === null;
    default:
      return true;
  }
};

const validateSchema = (schema, value, pathLabel = "$") => {
  const errors = [];
  if (!schema || typeof schema !== "object") {
    return errors;
  }
  if (schema.enum && !schema.enum.includes(value)) {
    errors.push(`${pathLabel}.enum`);
    return errors;
  }
  if (schema.type) {
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    const matches = types.some((type) => schemaTypeMatches(type, value));
    if (!matches) {
      errors.push(`${pathLabel}.type`);
      return errors;
    }
  }
  if (schema.type === "array" && Array.isArray(value) && schema.items) {
    value.forEach((entry, index) => {
      errors.push(...validateSchema(schema.items, entry, `${pathLabel}[${index}]`));
    });
  }
  if (schema.type === "object" && value !== null && typeof value === "object" && !Array.isArray(value)) {
    if (Array.isArray(schema.required)) {
      schema.required.forEach((key) => {
        if (!Object.prototype.hasOwnProperty.call(value, key)) {
          errors.push(`${pathLabel}.required.${key}`);
        }
      });
    }
    const props = schema.properties || {};
    Object.entries(props).forEach(([key, propSchema]) => {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        if (value[key] === undefined) {
          return;
        }
        errors.push(...validateSchema(propSchema, value[key], `${pathLabel}.${key}`));
      }
    });
  }
  return errors;
};

const loadSchema = (key) => {
  if (!fs.existsSync(schemaFile)) {
    return null;
  }
  const raw = fs.readFileSync(schemaFile, "utf8");
  if (!raw.trim()) {
    return null;
  }
  const payload = JSON.parse(raw);
  return payload[key] || null;
};

const assertWebhookPayloadShape = (payload) => {
  const schema = loadSchema("delivery_payload");
  assert.ok(schema, "delivery_payload schema missing");
  const errors = validateSchema(schema, payload, "$");
  assert.deepEqual(errors, []);
};

test("webhook delivers resolution.final payload", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-webhook-"));
  copyDataDir(tempDir);

  const webhook = await createWebhookServer();
  const { proc, port } = await startFlyback(tempDir, {
    WEBHOOK_URL: webhook.url,
    WEBHOOK_RETRY_BASE_MS: "50",
    WEBHOOK_RETRY_MAX_MS: "200"
  });

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
  const postback = await jsonFetch(postbackUrl, { method: "GET", headers: { "x-api-key": "demo-advertiser-key" } });
  assert.equal(postback.status, 200);

  const delivered = await waitFor(() => webhook.received.length >= 1, 5000);
  assert.ok(delivered);
  const payload = webhook.received[0];
  assert.equal(payload.type, "resolution.final");
  assertWebhookPayloadShape(payload);
  assert.equal(payload.payload.token_id, tokenId);

  await stopFlyback(proc);
  await webhook.close();
});

test("webhook includes signature when secret configured", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-webhook-"));
  copyDataDir(tempDir);

  const secret = "test-secret";
  const webhook = await createWebhookServer({ secret });
  const { proc, port } = await startFlyback(tempDir, {
    WEBHOOK_URL: webhook.url,
    WEBHOOK_SECRET: secret,
    WEBHOOK_RETRY_BASE_MS: "50",
    WEBHOOK_RETRY_MAX_MS: "200"
  });

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
  const postback = await jsonFetch(postbackUrl, { method: "GET", headers: { "x-api-key": "demo-advertiser-key" } });
  assert.equal(postback.status, 200);

  const delivered = await waitFor(() => webhook.received.length >= 1, 5000);
  assert.ok(delivered);
  assert.ok(webhook.signatures.length >= 1);
  const { header, expected } = webhook.signatures[0];
  assert.equal(header, expected);

  await stopFlyback(proc);
  await webhook.close();
});

test("webhook retries after failure and advances delivery cursor", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-webhook-"));
  copyDataDir(tempDir);

  const webhook = await createWebhookServer({ failFirst: true });
  const { proc, port } = await startFlyback(tempDir, {
    WEBHOOK_URL: webhook.url,
    WEBHOOK_RETRY_BASE_MS: "50",
    WEBHOOK_RETRY_MAX_MS: "200"
  });

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
  const postback = await jsonFetch(postbackUrl, { method: "GET", headers: { "x-api-key": "demo-advertiser-key" } });
  assert.equal(postback.status, 200);

  const delivered = await waitFor(() => webhook.received.length >= 2, 5000);
  assert.ok(delivered);

  const deliveryStatePath = path.join(tempDir, "delivery_state.json");
  const cursorUpdated = await waitFor(() => {
    if (!fs.existsSync(deliveryStatePath)) {
      return false;
    }
    const raw = fs.readFileSync(deliveryStatePath, "utf8");
    const payload = JSON.parse(raw);
    return Number.isFinite(payload.last_delivered_seq) && payload.last_delivered_seq > 0;
  }, 5000);
  assert.ok(cursorUpdated);

  await stopFlyback(proc);
  await webhook.close();
});

test("webhook sends to dlq after max retries", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-webhook-"));
  copyDataDir(tempDir);

  const webhook = await createWebhookServer({ failFirst: true });
  const { proc, port } = await startFlyback(tempDir, {
    WEBHOOK_URL: webhook.url,
    WEBHOOK_RETRY_BASE_MS: "20",
    WEBHOOK_RETRY_MAX_MS: "20",
    WEBHOOK_MAX_RETRIES: "1"
  });

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
  const postback = await jsonFetch(postbackUrl, { method: "GET", headers: { "x-api-key": "demo-advertiser-key" } });
  assert.equal(postback.status, 200);

  const dlqPath = path.join(tempDir, "delivery_dlq.ndjson");
  const dlqWritten = await waitFor(() => fs.existsSync(dlqPath), 5000);
  assert.ok(dlqWritten);
  const lines = fs
    .readFileSync(dlqPath, "utf8")
    .split("\n")
    .filter((line) => line.trim().length > 0);
  assert.ok(lines.length >= 1);
  const dlqEntry = JSON.parse(lines[0]);
  assertWebhookPayloadShape(dlqEntry.payload);

  await stopFlyback(proc);
  await webhook.close();
});

test("delivery health endpoint returns cursor state", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-webhook-"));
  copyDataDir(tempDir);

  const webhook = await createWebhookServer();
  const { proc, port } = await startFlyback(tempDir, {
    WEBHOOK_URL: webhook.url,
    WEBHOOK_RETRY_BASE_MS: "50",
    WEBHOOK_RETRY_MAX_MS: "200"
  });

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

  const postbackUrl = `${baseUrl}/v1/postback?token_id=${tokenId}&value=5&stage=purchase&outcome_type=purchase`;
  await jsonFetch(postbackUrl, { method: "GET", headers: { "x-api-key": "demo-advertiser-key" } });

  const deliveryStatePath = path.join(tempDir, "delivery_state.json");
  const cursorUpdated = await waitFor(() => {
    if (!fs.existsSync(deliveryStatePath)) {
      return false;
    }
    const raw = fs.readFileSync(deliveryStatePath, "utf8");
    const payload = JSON.parse(raw);
    return Number.isFinite(payload.last_delivered_seq) && payload.last_delivered_seq > 0;
  }, 5000);
  assert.ok(cursorUpdated);

  const health = await jsonFetch(`${baseUrl}/v1/delivery`, {
    method: "GET",
    headers: { "x-api-key": "demo-publisher-key" }
  });
  assert.equal(health.status, 200);
  const healthSchema = loadSchema("delivery_health");
  assert.ok(healthSchema, "delivery_health schema missing");
  const healthErrors = validateSchema(healthSchema, health.payload.delivery_health, "$");
  assert.deepEqual(healthErrors, []);
  assert.ok(Number.isFinite(health.payload.delivery_health.last_delivered_seq));
  assert.ok(
    health.payload.delivery_health.last_attempt_at === null ||
      typeof health.payload.delivery_health.last_attempt_at === "string"
  );
  assert.ok(Number.isFinite(health.payload.delivery_health.retry_count));
  assert.ok(Number.isFinite(health.payload.delivery_health.last_event_seq));
  assert.ok(Number.isFinite(health.payload.delivery_health.delivery_lag));
  assert.ok(health.payload.delivery_health.dlq);
  assert.ok(Number.isFinite(health.payload.delivery_health.dlq.count));

  await stopFlyback(proc);
  await webhook.close();
});

test("webhook replay dry-run supports dlq source", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "flyback-webhook-"));
  copyDataDir(tempDir);

  const dlqPath = path.join(tempDir, "delivery_dlq.ndjson");
  const payload = {
    schema_version: 1,
    delivery_ts: new Date().toISOString(),
    seq: 42,
    event_id: "event-replay-test",
    type: "resolution.final",
    ts: new Date().toISOString(),
    payload: { token_id: "token-test" }
  };
  fs.writeFileSync(
    dlqPath,
    `${JSON.stringify({
      failed_at: new Date().toISOString(),
      seq: 42,
      event_id: "event-replay-test",
      status: 500,
      error: "fail",
      payload
    })}\n`
  );

  const result = spawnSync(process.execPath, [path.join(rootDir, "scripts", "replay_webhook.js"), "--dlq", "--dry-run"], {
    env: { ...process.env, FLYBACK_DATA_DIR: tempDir, WEBHOOK_URL: "http://127.0.0.1:1" },
    encoding: "utf8"
  });
  assert.equal(result.status, 0);
});
