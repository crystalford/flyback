import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "..");
const dataDir = process.env.FLYBACK_DATA_DIR || path.join(rootDir, "data");

const registryFile = path.join(dataDir, "registry.json");
const ledgerFile = path.join(dataDir, "ledger.json");

const readJsonFile = (filePath) => {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const raw = fs.readFileSync(filePath, "utf8");
  if (!raw.trim()) {
    return null;
  }
  try {
    return JSON.parse(raw);
  } catch (error) {
    console.error("billing.dry_run.read.failed", { path: filePath, error: error.message });
    return null;
  }
};

const registry = readJsonFile(registryFile);
const ledgerPayload = readJsonFile(ledgerFile);

if (!registry || !ledgerPayload || !Array.isArray(ledgerPayload.entries)) {
  console.error("billing.dry_run.load.failed", {
    registry: !!registry,
    ledger: !!ledgerPayload
  });
  process.exit(1);
}

const policyIndex = registry.policies || {};
const campaignIndex = new Map(
  Array.isArray(registry.campaigns) ? registry.campaigns.map((campaign) => [campaign.campaign_id, campaign]) : []
);

const resolveRevShareBps = (campaignId, publisherId) => {
  const campaign = campaignIndex.get(campaignId);
  if (campaign && Number.isFinite(campaign.publisher_rev_share_bps)) {
    return campaign.publisher_rev_share_bps;
  }
  const policy = policyIndex[publisherId] || {};
  if (Number.isFinite(policy.rev_share_bps)) {
    return policy.rev_share_bps;
  }
  return 7000;
};

let mismatches = 0;
ledgerPayload.entries.forEach((entry) => {
  if (!entry || entry.billable !== true) {
    return;
  }
  const revShareBps = resolveRevShareBps(entry.campaign_id, entry.publisher_id);
  const expected = Math.round(Number(entry.raw_value) * 100 * (revShareBps / 10000));
  const payoutCents = Number(entry.payout_cents);
  if (!Number.isFinite(payoutCents) || payoutCents !== expected) {
    mismatches += 1;
    console.log("billing.dry_run.mismatch", {
      entry_id: entry.entry_id,
      campaign_id: entry.campaign_id,
      publisher_id: entry.publisher_id,
      payout_cents: payoutCents,
      expected_payout_cents: expected,
      rev_share_bps: revShareBps
    });
  }
});

if (mismatches > 0) {
  console.log("billing.dry_run.failed", { mismatches });
  process.exit(1);
}

console.log("billing.dry_run.ok", { entries: ledgerPayload.entries.length });
