import fs from "fs";
import http from "http";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const port = Number(process.env.PORT) || 3000;
const host = process.env.HOST || "0.0.0.0";
const publicDir = path.join(__dirname, "public");
const tokensFile = path.join(__dirname, "data", "tokens.json");

const contentTypes = {
  ".html": "text/html",
  ".js": "application/javascript",
  ".css": "text/css",
  ".json": "application/json"
};

const loadTokens = () => {
  if (!fs.existsSync(tokensFile)) {
    return [];
  }

  const raw = fs.readFileSync(tokensFile, "utf8");
  if (!raw.trim()) {
    return [];
  }

  try {
    return JSON.parse(raw);
  } catch (error) {
    console.error("tokens.load.failed", { error: error.message });
    return [];
  }
};

const saveTokens = (tokens) => {
  fs.writeFileSync(tokensFile, `${JSON.stringify(tokens, null, 2)}\n`);
};

const logLifecycle = (event, token, extra = {}) => {
  console.log(event, {
    token_id: token.token_id,
    status: token.status,
    ...extra
  });
};

const applyDefaults = (token) => {
  const normalized = { ...token };

  if (!normalized.status) {
    normalized.status = "PENDING";
  }

  if (normalized.status === "PENDING" && !normalized.pending_at) {
    normalized.pending_at = normalized.created_at || new Date().toISOString();
  }

  if (!normalized.created_at) {
    normalized.created_at = new Date().toISOString();
  }

  if (!normalized.expires_at) {
    const createdAt = new Date(normalized.created_at);
    const expiresAt = new Date(createdAt);
    expiresAt.setDate(expiresAt.getDate() + 30);
    normalized.expires_at = expiresAt.toISOString();
  }

  if (!normalized.signature) {
    normalized.signature = "ed25519-placeholder";
  }

  if (!normalized.binding) {
    normalized.binding = { type: "none", value: null };
  }

  if (!normalized.context) {
    normalized.context = { intent_type: "unknown", dwell_seconds: 0, interaction_count: 0 };
  }

  if (!normalized.scope) {
    normalized.scope = { campaign_id: "campaign-v1", publisher_id: "publisher-demo", creative_id: "creative-v1" };
  }

  return normalized;
};

const enforceExpiry = (token) => {
  const now = new Date();
  if (token.status !== "RESOLVED" && new Date(token.expires_at) < now) {
    return { ...token, status: "EXPIRED" };
  }
  return token;
};

const normalizeToken = (token) => enforceExpiry(applyDefaults(token));

const normalizeTokens = (storedTokens) => storedTokens.map(normalizeToken);

let tokens = normalizeTokens(loadTokens());
if (tokens.length > 0) {
  saveTokens(tokens);
  console.log("tokens.load.normalized", { count: tokens.length });
}

const findToken = (tokenId) => tokens.find((token) => token.token_id === tokenId);

const baseTokenPayload = ({ campaignId, publisherId, creativeId, intentType, dwellSeconds, interactionCount }) => {
  const createdAt = new Date();
  const expiresAt = new Date(createdAt);
  expiresAt.setDate(expiresAt.getDate() + 30);

  return {
    token_id: randomUUID(),
    version: "1.0",
    created_at: createdAt.toISOString(),
    expires_at: expiresAt.toISOString(),
    scope: {
      campaign_id: campaignId,
      publisher_id: publisherId,
      creative_id: creativeId
    },
    context: {
      intent_type: intentType,
      dwell_seconds: dwellSeconds,
      interaction_count: interactionCount
    },
    binding: {
      type: "none",
      value: null
    },
    signature: "ed25519-placeholder",
    status: "CREATED",
    pending_at: null,
    resolved_at: null,
    resolved_value: null
  };
};

const parseResolvedValue = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 1;
  }
  return numeric;
};

const sendJson = (res, status, payload) => {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(body)
  });
  res.end(body);
};

const readJsonBody = (req) =>
  new Promise((resolve, reject) => {
    let data = "";

    req.on("data", (chunk) => {
      data += chunk;
    });

    req.on("end", () => {
      if (!data.trim()) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(data));
      } catch (error) {
        reject(error);
      }
    });

    req.on("error", reject);
  });

const handleFill = async (req, res) => {
  let body;
  try {
    body = await readJsonBody(req);
  } catch (error) {
    sendJson(res, 400, { error: "invalid_json" });
    return;
  }
  const { publisher_id: publisherId, size } = body;
  const creativeId = "creative-v1";
  const campaignId = "campaign-v1";

  sendJson(res, 200, {
    creative_url: "/creative.js",
    config: {
      campaign_id: campaignId,
      publisher_id: publisherId || "publisher-demo",
      creative_id: creativeId,
      size: size || "300x250"
    }
  });
};

const handleIntent = async (req, res) => {
  let body;
  try {
    body = await readJsonBody(req);
  } catch (error) {
    sendJson(res, 400, { error: "invalid_json" });
    return;
  }
  const {
    campaign_id: campaignId = "campaign-v1",
    publisher_id: publisherId = "publisher-demo",
    creative_id: creativeId = "creative-v1",
    intent_type: intentType = "signup",
    dwell_seconds: dwellSeconds = 0,
    interaction_count: interactionCount = 1
  } = body;

  const token = baseTokenPayload({
    campaignId,
    publisherId,
    creativeId,
    intentType,
    dwellSeconds,
    interactionCount
  });
  logLifecycle("intent.created", token, {
    campaign_id: token.scope.campaign_id,
    publisher_id: token.scope.publisher_id,
    creative_id: token.scope.creative_id
  });
  token.status = "PENDING";
  token.pending_at = new Date().toISOString();

  tokens = [...tokens, token];
  saveTokens(tokens);

  logLifecycle("intent.pending", token);

  sendJson(res, 200, { token });
};

const handlePostback = (url, res) => {
  const tokenId = url.searchParams.get("token_id");
  const value = url.searchParams.get("value");

  if (!tokenId) {
    sendJson(res, 400, { error: "token_id is required" });
    return;
  }

  const token = findToken(tokenId);
  if (!token) {
    sendJson(res, 404, { error: "token not found" });
    return;
  }

  if (token.status === "RESOLVED") {
    logLifecycle("postback.idempotent", token);
    sendJson(res, 200, { token, status: "already_resolved" });
    return;
  }

  if (token.status === "EXPIRED") {
    logLifecycle("postback.idempotent", token, { state: "expired" });
    sendJson(res, 410, { token, status: "already_expired" });
    return;
  }

  const now = new Date();
  if (new Date(token.expires_at) < now) {
    token.status = "EXPIRED";
    saveTokens(tokens);
    logLifecycle("postback.expired", token);
    sendJson(res, 410, { token, status: "expired" });
    return;
  }

  token.status = "RESOLVED";
  token.resolved_at = now.toISOString();
  token.resolved_value = parseResolvedValue(value);
  saveTokens(tokens);

  logLifecycle("postback.resolved", token, { value: token.resolved_value });
  sendJson(res, 200, { token, status: "resolved" });
};

const serveStatic = (res, filePath) => {
  if (!fs.existsSync(filePath)) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not found");
    return;
  }

  const ext = path.extname(filePath);
  const contentType = contentTypes[ext] || "application/octet-stream";
  const data = fs.readFileSync(filePath);

  res.writeHead(200, {
    "Content-Type": contentType,
    "Content-Length": data.length
  });
  res.end(data);
};

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === "POST" && url.pathname === "/v1/fill") {
      await handleFill(req, res);
      return;
    }

    if (req.method === "POST" && url.pathname === "/v1/intent") {
      await handleIntent(req, res);
      return;
    }

    if (req.method === "GET" && url.pathname === "/v1/postback") {
      handlePostback(url, res);
      return;
    }

    if (req.method === "GET") {
      const requestedPath = url.pathname === "/" ? "/index.html" : url.pathname;
      const filePath = path.join(publicDir, requestedPath);
      serveStatic(res, filePath);
      return;
    }

    res.writeHead(405, { "Content-Type": "text/plain" });
    res.end("Method not allowed");
  } catch (error) {
    console.error("server.error", { message: error.message });
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "server_error" }));
  }
});

server.listen(port, host, () => {
  console.log(`Flyback server listening on http://${host}:${port}`);
});
