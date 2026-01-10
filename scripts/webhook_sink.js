import http from "http";

const port = Number(process.env.PORT) || 4040;
const host = process.env.HOST || "0.0.0.0";

const server = http.createServer((req, res) => {
  if (req.method !== "POST") {
    res.writeHead(405, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "method_not_allowed" }));
    return;
  }

  let body = "";
  req.on("data", (chunk) => {
    body += chunk.toString();
  });
  req.on("end", () => {
    let payload = null;
    try {
      payload = body ? JSON.parse(body) : null;
    } catch (error) {
      console.log("webhook_sink.invalid_json", { error: error.message });
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "invalid_json" }));
      return;
    }
    console.log("webhook_sink.receive", {
      seq: payload?.seq,
      event_id: payload?.event_id,
      type: payload?.type
    });
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok" }));
  });
});

server.listen(port, host, () => {
  console.log(`Webhook sink listening on http://${host}:${port}`);
});
