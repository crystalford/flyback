# Flyback
Display-Native Intent & Performance System
Reference Architecture (Engineering Handoff)

Version: 0.3.0  
Date: January 2026  
Status: Codex Implementation Reference  
Audience: Engineering / Systems Implementation

## 1. Purpose and Scope

Flyback is a performance-oriented display system designed to capture user intent within display creatives while resolving real downstream outcomes such as signups, purchases, installs, or deposits.

Flyback does not eliminate clicks, redirects, landing pages, pixels, or postbacks. Instead, it introduces an additional, creative-scoped intent and measurement layer that reduces reliance on fragile, page-level pixel attribution and remains functional when traditional tracking breaks down.

This document defines:

- the system architecture
- data flows
- attribution models
- publisher control mechanisms
- demand integration patterns

This document does not guarantee universal attribution, replace advertiser tracking systems, or claim superiority over all existing ad tech. It defines a minimum coherent system intended to evolve through implementation.

## 2. System Overview

### 2.1 High-Level Concept

Traditional display advertising relies primarily on external navigation and page-level tracking to measure performance. This creates brittle attribution chains and forces all value through clicks and redirects.

Flyback treats the display creative itself as a first-class interaction and intent surface, while still supporting downstream tracking systems for final outcome resolution.

The system captures qualified intent events in-frame, binds them to durable identifiers, and resolves outcomes asynchronously via standard industry mechanisms such as affiliate postbacks, advertiser webhooks, or CRM events.

### 2.2 Design Principles

- Performance outcomes remain the ground truth
- Clicks and redirects are supported but not structurally required
- Measurement is creative-scoped, not site-scoped
- User interaction is voluntary and permission-based
- Attribution may resolve immediately or later
- Publishers retain absolute inventory control

## 3. Core Concepts and Definitions

### 3.1 Creative Runtime Unit (CRU)

A Creative Runtime Unit is a self-contained display creative that includes:

- rendering logic
- interaction handling
- intent capture logic
- embedded measurement logic

A CRU executes inside a standard ad container and does not require publisher-installed tracking pixels or site-wide scripts beyond the Flyback loader.

### 3.2 Embedded Measurement Module (EMM)

The Embedded Measurement Module is the measurement logic bundled inside each CRU. It is:

- session-scoped
- creative-scoped
- ephemeral
- non-persistent across sites

The EMM records attention, interaction, and intent signals and emits structured events to the Flyback backend.

### 3.3 Intent Event Token (IET)

An Intent Event Token is a signed event record representing a voluntary, in-frame user intent.

It is not a conversion by itself.

An IET may later be resolved to an outcome via:

- affiliate postbacks
- advertiser webhooks
- payment processor callbacks
- CRM events

## 4. In-Frame Intent Capture Model

### 4.1 State Model

Each CRU progresses through a bounded lifecycle:

```
INITIALIZED
→ VISIBLE
→ ATTENTION_QUALIFIED
→ ENGAGED (optional)
→ INTENT_CAPTURED (optional)
→ INTENT_EMITTED
→ AWAITING_RESOLUTION
```

Not all impressions produce engagement or intent. All states are valid.

### 4.2 Attention Qualification

Attention may be qualified using:

- viewport visibility
- dwell time thresholds
- configuration parameters

Default dwell threshold: 2 seconds (configurable).

### 4.3 Interaction and Clicks

Clicks are treated as interaction events, not as the sole carrier of attribution.

A click may:

- escalate engagement
- trigger navigation
- carry an intent token
- initiate a redirect

Clicks are supported, tracked, and logged, but they are not required for intent capture or outcome attribution.

## 5. Intent Event Tokens

### 5.1 Token Structure

```json
{
  "token_id": "uuid",
  "version": "1.0",
  "created_at": "ISO-8601",
  "expires_at": "ISO-8601",

  "scope": {
    "campaign_id": "uuid",
    "publisher_id": "uuid",
    "creative_id": "uuid"
  },

  "context": {
    "intent_type": "string",
    "dwell_seconds": 8.4,
    "interaction_count": 3
  },

  "binding": {
    "type": "email_hash | external_id | none",
    "value": "optional"
  },

  "signature": "ed25519"
}
```

### 5.2 Token Lifecycle

```
CREATED → EMITTED → PENDING
                ↘
              RESOLVED | EXPIRED
```

Default expiration window: 30 days.

## 6. Outcome Resolution Models

Flyback supports multiple attribution paths. Performance outcomes remain authoritative.

### 6.1 Immediate Resolution

Used when the outcome occurs in-session.

Examples:

- lead submission
- app install
- account creation

Intent → Token → Advertiser Webhook → Resolved

### 6.2 Deferred Resolution

Used when the outcome occurs later.

Examples:

- purchase
- deposit
- subscription activation

Intent → Token → Time Delay → External Conversion → Postback → Resolved

Flyback does not need to be present at the moment of sale.

### 6.3 Intent-Only Mode

Used for upper-funnel or brand campaigns.

- intent captured
- no conversion expected
- priced on attention-derived metrics

## 7. Publisher Inventory Control Model

### 7.1 Core Principle

Publishers retain absolute ownership and control over their inventory.

Flyback provides execution and measurement infrastructure, not inventory ownership.

### 7.2 Publisher Capabilities

Publishers may:

- define allowed formats
- whitelist or blacklist advertisers
- inject their own affiliate relationships
- set minimum value floors
- bypass Flyback demand entirely
- approve creatives before serving
- adjust demand priority dynamically

## 8. Inventory and Ad Containers

### 8.1 Supported Container Sizes (V1)

- 300×250
- 728×90
- 160×600
- Responsive ratio-based containers

Innovation occurs inside the container, not at the layout level.

### 8.2 Publisher Embed

```html
<div
  data-flyback-container
  data-publisher-id="uuid"
  data-size="300x250"
  data-floor="8.00">
</div>

<script src="https://cdn.flyback.io/runtime.js" async></script>
```

## 9. Demand Sources

Flyback supports multiple demand sources per inventory unit.

### 9.1 Demand Types

Outcome-Based Demand

- priced on resolved outcomes
- variable value

Direct Sales

- flat CPM or flat rate
- publisher-managed

Publisher Affiliate Demand

- publisher-owned relationships
- Flyback provides execution and measurement

### 9.2 Priority Resolution

Publishers define priority rules, for example:

1. Direct Sales
2. Publisher Affiliate Demand
3. Outcome-Based Demand Pool
4. Fallback / House

## 10. Derived Value Metrics

### 10.1 Derived Effective CPM (dCPM)

Flyback computes a derived effective CPM for comparison and routing.

```
dCPM = (Resolved Value / Impressions) × 1000
```

dCPM:

- is not a billing unit
- is not sold to advertisers
- is used for floors and prioritization only

## 11. Creative Runtime Requirements

### 11.1 Technical Constraints

- initial payload < 500 KB
- total runtime < 2 MB
- first render < 2 seconds
- audio muted until interaction
- static fallback required

### 11.2 Configuration Interface

Creatives expose configuration for:

- sponsor metadata
- visual parameters
- behavioral flags
- campaign identifiers

## 12. Client Runtime Responsibilities

The Flyback client runtime:

- detects eligible containers
- requests creative fill
- mounts the CRU
- provides viewport context
- does not perform site-wide tracking

## 13. Server-Side Components

### 13.1 Core Services

- fill service
- intent ingestion API
- token store
- outcome resolution service
- reporting pipeline

### 13.2 Required Endpoints (V1)

- POST /v1/fill
- POST /v1/intent
- GET  /v1/postback
- GET  /v1/publisher/config
- GET  /v1/reports/publisher

## 14. Explicit Non-Goals and Constraints

Flyback does not:

- eliminate pixels or postbacks
- guarantee attribution in all advertiser setups
- sell user data
- perform cross-site profiling
- replace advertiser CRMs
- optimize for clicks as a primary metric

Flyback aims to reduce dependency on fragile tracking, not deny its existence.

## 15. V1 Implementation Contract

### V1 Must

- render one creative runtime unit
- capture in-frame intent
- emit intent event tokens
- resolve at least one deferred outcome via postback
- compute derived value metrics

### V1 May

- support multiple demand sources
- allow affiliate injection
- expose basic publisher configuration

### Deferred

- complex dashboards
- advanced routing algorithms
- multiple affiliate networks
- video and audio formats

## 16. V1 Demo and Lifecycle Behavior

This repository includes a minimal V1 demo that exercises the end-to-end loop using a single CRU and the required endpoints:

1. Start the server with `node server.js`.
2. Open `http://localhost:3000` to load the demo container and creative.
3. Click the call-to-action to emit an intent token and observe deferred resolution via `/v1/postback`.

The server logs explicitly distinguish token lifecycle transitions:

- `intent.created` → token created
- `intent.pending` → token persisted and awaiting resolution
- `postback.resolved` → deferred resolution applied
- `postback.expired` → expiry enforced
- `postback.idempotent` → duplicate or late postback acknowledged

For development inspection, `GET /v1/reports` returns the current aggregate metrics (read-only, windowed).

The fill path now includes a minimal creative selection hook backed by file-based registries, so V2 structurally supports multiple creatives while still serving a single selected creative.

No dashboards, routing, or multi-demand logic is implemented in this V1 demo.

## 16.1 V2 Configuration Scripts

- `node scripts/seed.js` initializes `data/registry.json`, `data/budgets.json`, `data/keys.json`, and `data/aggregates.json`.
- `node scripts/validate.js` validates `data/registry.json`.

## 16.3 V2 API Keys

API keys live in `data/keys.json`. Send a publisher key in `x-api-key` for `/v1/fill` and `/v1/intent`, and an advertiser key for `/v1/postback`. Demo mode uses the default keys when the header is omitted.

## 16.4 V2 Reconciliation

On startup (and via `node scripts/reconcile.js`), the server performs a read-only reconciliation pass per campaign:

- windowed final-resolution token sum vs windowed aggregate resolved value
- lifetime budget delta vs total billable final-resolution token sum

Reconciliation logs `reconcile.ok` or `reconcile.mismatch` with campaign identifiers and a tolerance. Without transactions and multi-writer locks, V3 is required to guarantee exact reconciliation under concurrent updates and distributed postbacks. The ledger is a future reconciliation target but is not part of the current checks.

## 16.5 V2.5 Selection Modes

Publishers can set `selection_mode` in `data/registry.json` policies:

- `raw`: selection uses raw derived value per 1k impressions (default).
- `weighted`: selection uses weighted derived value per 1k impressions when available, and falls back to raw values deterministically.

Weighted mode is experimental and reversible; budgets and reconciliation remain based on raw resolved values only.

## 16.6 V2.5 Observability & Guardrails

Each `/v1/fill` records a lightweight selection decision (in-memory ring buffer). Add `?include_selections=true` to `/v1/reports` to inspect recent decisions for the authenticated publisher.

When weighted selection diverges materially from raw ordering for consecutive windows, the server emits `selection.guardrail.warning` logs. This is diagnostic only and does not change routing or budgets.

These guardrails precede billing, payouts, or default weighted routing.

## 16.7 Publisher Contract Semantics (Pre-Billing)

Publisher policies include `floor_type` (`raw` or `weighted`) and `floor_value_per_1k`. During selection, candidates below the active floor are excluded; if all candidates are excluded, the system deterministically falls back to the best available candidate and logs the fallback.

Floors are enforced before money exists. Billing, invoicing, and payouts remain V3.

Publisher floor config and last-window observed values are surfaced in `/v1/reports` for the authenticated publisher.

## 16.8 Advertiser Outcome Caps (Pre-Billing)

Campaigns may define `caps` (e.g., `max_outcomes`, `max_weighted_value`). Final resolutions beyond caps are accepted but marked `billable: false`; budgets are not decremented. Caps are enforced during resolution only and surfaced in `/v1/reports` along with last-window billable vs non-billable counts.

Billing, invoicing, and payout enforcement remain V3.

## 16.9 Ledger Skeleton (No Payouts)

Billable final resolutions append entries to `data/ledger.json` (append-only). Each entry records the raw and weighted values plus a computed `payout_cents` using publisher rev-share settings. This provides an auditable spine without executing payouts or advertiser billing.

## 16.10 Ledger Invariants

On startup (and via `node scripts/reconcile.js`), ledger reconciliation compares per-campaign ledger payout sums against expected payouts derived from billable final tokens and rev-share settings (window and lifetime). These checks are read-only and logged as `ledger.reconcile.ok` or `ledger.reconcile.mismatch`.

Without transactional writes across tokens, aggregates, budgets, and the ledger, V3 cannot guarantee exact ledger consistency under concurrent updates.

## 16.11 Event Log as Source of Truth

The append-only `data/events.ndjson` is authoritative. Each line is a JSON event with a monotonic `seq`, `event_id`, `ts`, `type`, and `payload`. `data/event_state.json` persists the last used sequence so restarts remain monotonic. Tokens, aggregates, budgets, and the ledger are projections derived from events.

Events are the only writes. Projections are derived, disposable, and rebuildable.

Rebuild procedure:

1. Ensure `data/snapshot.json` is present (optional but recommended).
2. Set `REBUILD_FROM_EVENTS=true` and start the server. It will load the snapshot (if present) and replay events where `seq > snapshot_seq`.

Compaction:

- Run `node scripts/compact.js` to write a new `events.ndjson` containing only events after `snapshot_seq`. The script updates `event_state.json` and logs `compact.ok`.

## 16.12 Concurrency & Atomicity

Event appends use lockfiles for `events.ndjson`, `event_state.json`, and `snapshot.json` to guard multi-event batches and snapshots. Final resolutions append a batch (`resolution.final`, `budget.decrement`, `ledger.append`) with consecutive seq or abort as a unit. Projections apply events with `seq > applied_seq` and persist `projection_state.json`. When `REBUILD_FROM_EVENTS=true`, live writes are rejected.

## 16.13 Read Model Separation

Writes mutate projections. Reads derive views. These are intentionally separate.

## 16.14 Serialized Append + Apply

Event append and projection apply are serialized to guarantee consistency under concurrency.

## 16.15 Process Model

One writer process owns event appends (`WRITE_ENABLED=true`). Read-only replicas may run with `WRITE_ENABLED=false` and serve `/v1/fill` + `/v1/reports`. Multi-process writers are unsupported.

## 16.16 Process Topology

Run a single writer service (role `writer`) to own event appends and projection updates. Run any number of read replicas (role `replica`) with writes disabled for `/v1/fill` and `/v1/reports`. Multi-writer deployments are unsupported.

## 16.17 Stability Contract (V3)

Events are the only write source of truth. Projections are derived and rebuildable. Single-writer behavior is enforced via `WRITE_ENABLED`. No backward-incompatible changes without a version bump.

## 16.18 Schemas & Validation (V4)

JSON schemas define the shape of events, registry/policies, and report read models. Schemas are loaded from `schemas/schemas.json` on startup (defaults apply if missing). Event appends and config loads validate against these schemas; unknown fields are tolerated for forward compatibility, but missing required fields are rejected.

## 16.19 External Integration Boundary (V4-C)

A writer can deliver resolved outcomes to an external webhook. Configure `WEBHOOK_URL` to enable delivery. Only writer processes send; replicas skip delivery. Delivery is at-least-once, sequential, and retry-safe (exponential backoff) using a persisted `last_delivered_seq` cursor in `data/delivery_state.json`.

Payloads include `schema_version` and a `x-flyback-schema-version` header. If `WEBHOOK_SECRET` is set, deliveries include `x-flyback-signature` (HMAC SHA256 of the raw JSON body).

For local testing, run `npm run webhook:sink` (listens on `http://0.0.0.0:4040`) and set `WEBHOOK_URL=http://127.0.0.1:4040`.

Verify signatures with `npm run webhook:verify -- --body ./payload.json --secret YOUR_SECRET --signature HEADER_VALUE`.

See `schemas/delivery_payload.example.json` for a sample payload.

Replay deliveries from a given sequence with `WEBHOOK_URL=... npm run webhook:replay -- --from 1 --to 50`.
Replay DLQ entries with `WEBHOOK_URL=... npm run webhook:replay -- --dlq`.

## 16.20 Ops Console (V5)

Visit `/ops.html` for a read-only control room that surfaces aggregates, ledger stats, and selection decisions. This is a viewer only and uses `/v1/reports` under the hood. You can export aggregates as CSV and download delivery health JSON from the UI.

Load it once with `/ops.html?api_key=YOUR_KEY` to mint a short-lived signed cookie for assets. The header includes the publisher id when available.

The advertiser view lives at `/advertiser.html` and uses the same signed-cookie flow. It surfaces invoice draft totals, payout runs, and the last 5 selections.

Payout runs now support sorting, status filtering, and advertiser coverage links derived from ledger entry IDs.

Delivery health is exposed via `/v1/reports` as `delivery_health`, including last delivered seq, last event seq, delivery lag, last attempt time, retry count, and DLQ stats.

Failed deliveries beyond `WEBHOOK_MAX_RETRIES` are written to `data/delivery_dlq.ndjson`.

Delivery health is also available via `GET /v1/delivery` for ops tooling. Set `DELIVERY_LAG_WARN` to emit `delivery.lag.warning` logs.

## 16.21 Billing Preview (V5)

`npm run billing:preview` summarizes ledger payouts per campaign/publisher pair (read-only).

## 16.22 Billing Dry Run (V5)

`npm run billing:dry-run` validates ledger payout calculations against registry rev-share settings (read-only).

## 16.23 Invoice Drafts (V5)

`npm run billing:invoice-draft` writes read-only draft invoices grouped by advertiser to `data/invoices/`.

## 16.24 Invoice Audit (V5)

`npm run billing:invoice-audit` compares invoice draft totals against ledger totals per advertiser.

## 16.25 Invoice Export (V5)

`npm run billing:invoice-export` writes `data/invoice_drafts.csv` from draft invoices.

## 16.26 Billing Execution (V5)

`npm run billing:execute` batches new billable ledger entries into `data/payouts.json` as pending payout runs (no external payout execution). Use `npm run billing:execute:dry-run` to preview without writing.

Payout runs are surfaced in `/v1/reports` as `payout_runs` (read-only) for ops visibility.

## 16.27 Payout Status (V5)

`npm run billing:payout-update -- --run-id <id> --status <pending|sent|settled>` updates payout run status in `data/payouts.json` (manual bookkeeping only).

## 16.28 Payout Export (V5)

`npm run billing:payout-export` writes `data/payouts.csv` from payout runs.

## 16.29 Payout Reconciliation (V5)

`npm run billing:payout-reconcile` compares payout runs against ledger entry sums and flags missing or mismatched entries.

Payout reconciliation status is surfaced in `/v1/reports` as `payout_reconciliation` for ops monitoring.

## 16.30 Publisher Statements (V5)

`npm run billing:publisher-statement` writes `data/publisher_statements.json` and `data/publisher_statements.csv` from ledger + payout runs.

## 16.31 Ops Snapshot (V5)

`npm run ops:snapshot` prints a compact health snapshot (registry counts, budgets, aggregates window, ledger totals, delivery state + DLQ).

## 16.32 Event Export (V5)

`npm run events:export -- --from 1 --to 500 --out ./events.ndjson` exports event ranges for audit and writes a `sha256` sidecar file.

## 16.33 Deployment Hardening (V5)

- Environment validation logs configuration warnings on startup.
- Security headers are enabled (CSP, frame denial, referrer policy, nosniff).
- Basic in-memory rate limiting is enabled (set `RATE_LIMIT_BYPASS=true` to disable).

## 16.2 V2 Config Versions & Migrations

Each config file includes a `version` integer. On startup, the server detects the version, runs in-process migrations in order, then validates the migrated shape. If a migration is missing or the version is ahead of the code, startup fails with a clear log.

To add a new migration:

1. Increment the target version constant in `server.js`.
2. Add a migration function for the prior version to the appropriate migrations map.
3. Update seed files to emit the new version.

## 17. Deployment

### Local run

1. Start the server with `node server.js`.
2. Open `http://localhost:3000`.

### Hosted run assumptions

- Provide a Node 18+ runtime with file system access for `data/tokens.json`.
- Expose port `3000` (or set `PORT`) and optionally set `HOST` if required by the platform.
- Serve `/public` as static assets alongside the API routes.

### Optional deployment targets

This service runs as a single Node process. It can be deployed to generic Node hosts or serverless platforms that support long-running processes. For Vercel-style environments, use a single Node server entrypoint and ensure the file system is writable for `data/tokens.json`.

### V1 limitations

- Single-node process with file-based persistence.
- No horizontal scaling or shared token store.
- No dashboards, routing, or advanced demand logic.

## 18. Document Status

This document defines a buildable minimum system.

Implementation is expected to surface gaps.
This document should evolve alongside the codebase.

End of Codex Handoff Document
