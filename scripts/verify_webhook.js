import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHmac } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");

const parseArgs = () => {
  const args = process.argv.slice(2);
  const result = { bodyPath: null, signature: null, secret: null };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === "--body" && args[i + 1]) {
      result.bodyPath = args[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--signature" && args[i + 1]) {
      result.signature = args[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--secret" && args[i + 1]) {
      result.secret = args[i + 1];
      i += 1;
    }
  }
  return result;
};

const { bodyPath, signature, secret } = parseArgs();
if (!bodyPath || !secret) {
  console.error("webhook.verify.missing_args", { usage: "--body <file> --secret <secret> [--signature <sig>]" });
  process.exit(1);
}

const bodyFile = path.isAbsolute(bodyPath) ? bodyPath : path.join(rootDir, bodyPath);
if (!fs.existsSync(bodyFile)) {
  console.error("webhook.verify.body_missing", { path: bodyFile });
  process.exit(1);
}

const body = fs.readFileSync(bodyFile, "utf8");
const expected = createHmac("sha256", secret).update(body).digest("hex");
console.log("webhook.verify.expected", { signature: expected });

if (signature) {
  const ok = signature === expected;
  console.log("webhook.verify.result", { ok });
  process.exit(ok ? 0 : 1);
}
