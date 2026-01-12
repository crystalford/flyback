import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");

const parseArgs = () => {
  const args = process.argv.slice(2);
  const result = { input: null, output: null, include: null };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === "--in" && args[i + 1]) {
      result.input = args[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--out" && args[i + 1]) {
      result.output = args[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--include" && args[i + 1]) {
      result.include = args[i + 1];
      i += 1;
    }
  }
  return result;
};

const { input, output, include } = parseArgs();
if (!input || !output) {
  console.error("log.export.missing_args", { usage: "--in <file> --out <file> [--include token]" });
  process.exit(1);
}

const inputPath = path.isAbsolute(input) ? input : path.join(rootDir, input);
const outputPath = path.isAbsolute(output) ? output : path.join(rootDir, output);

if (!fs.existsSync(inputPath)) {
  console.error("log.export.missing_input", { path: inputPath });
  process.exit(1);
}

const raw = fs.readFileSync(inputPath, "utf8");
if (!raw.trim()) {
  console.error("log.export.empty", { path: inputPath });
  process.exit(1);
}

const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);
const filtered = include ? lines.filter((line) => line.includes(include)) : lines;
fs.writeFileSync(outputPath, filtered.join("\n") + (filtered.length ? "\n" : ""));
console.log("log.export.ok", { input: inputPath, output: outputPath, lines: filtered.length });
