# Flyback
Display-Native Intent & Performance System
Reference Architecture (Engineering Handoff)

Version: 0.2  
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

For development inspection, `GET /v1/reports` returns the current in-memory aggregate metrics (read-only, reset on restart).

The fill path now includes a minimal creative selection hook and a small in-memory registry, so V1 structurally supports multiple creatives while still serving a single selected creative.

No dashboards, routing, or multi-demand logic is implemented in this V1 demo.

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
