# Backlog: Phase 0 remainder (Day 1) + local Phase 1 (Day 2)

Produced by the Product Manager agent, reviewed and approved by the Lead on
2026-11-24. Scope basis: `PROJECT.md` sections 3, 5-13, 17-19, 24-27, 29-32;
`CLAUDE.md`; ADR-0004; `docs/6-7_DAY_SPRINT.md` Day 1-2.

These are proposed stories only. Each is scoped to directories owned
exclusively by one role (Data Platform Engineer: `simulator/`; Full-Stack
Engineer: `website/`) to avoid concurrent-agent path overlap. All stories are
local-only — zero paid Azure/Fabric usage; the Fabric trial remains
unactivated per ADR-0004. Every story still requires: Lead review before
assignment, developer branch + tests, draft PR, CI, QA verdict, and human
approval to merge to `main` (`PROJECT.md` section 25).

**Lead review notes (2026-11-24):**
- WE-01 was flagged as touching the shared `.github/workflows/ci.yml`. The
  Lead will make that specific CI edit directly rather than the web-engineer,
  to avoid cross-ownership conflicts on a Lead-owned file.
- WE-03's mapping-library choice is deliberately deferred and is **not**
  approved here; it remains a cost/architecture watch-item for a later story.

## Sequencing / parallelization
- DE-01 and WE-01 have no dependencies and start immediately, concurrently.
- DE-02 -> DE-03 are sequential (same owner, overlapping `simulator/generator/`
  and `simulator/incidents/`).
- DE-04 depends only on DE-01 and touches `simulator/batch/` (disjoint from
  DE-02/DE-03); may run concurrently only with deliberately isolated agent
  sessions, otherwise sequential.
- DE-05 -> DE-06 -> DE-07 is a strict sequential chain (Bronze -> Silver ->
  Gold).
- WE-02 and WE-03 both depend only on WE-01 and touch disjoint files; they may
  run concurrently once WE-01 merges.
- WE-02/WE-03 depend on DE-01/DE-02 schemas only as a documented *contract*,
  not a code/file dependency — fixtures are hand-authored and reconciled
  later, so the Web track is never blocked waiting on the Data track.

Target vertical slice by end of Day 2: DE-01->DE-07 proves a real (if
minimal) Bronze->Silver->Gold medallion chain running locally on
simulator-generated and incident-laced data, with reconciling counts. WE-01->
WE-03 proves a real, accessible, clearly-labelled DEMO-mode website shell with
two working pages. Nothing here claims Fabric connectivity.

---

## DE-01: Synthetic Transport Domain Model & Schemas
**Objective:** Versioned, config-driven schemas and deterministic static
generation for core reference entities (vehicles, routes, stops, depots) and
the telemetry event contract, plus externalized scale/validation-threshold
config.
**Value:** Single source-of-truth contract that every later simulator story,
medallion layer, and website fixture depends on.
**Scope:** `simulator/config/` (scale config: 50 vehicles/10 routes/100-200
stops/4 depots, validation thresholds — PROJECT.md §5, §12); `simulator/
schemas/` (Vehicle, Route, Stop, Depot, TelemetryEvent incl. `schema_version`,
§6); `simulator/reference/` (deterministic generation with realistic
synthetic Sydney-area coordinates).
**Out of scope:** Telemetry generation loop (DE-02), historical/batch
datasets (DE-04), incident logic (DE-03), any Fabric/Azure schema deployment,
Warehouse DDL.
**Dependencies:** None.
**Acceptance criteria:**
- Reference-entity counts are config-driven; default config produces exactly
  50/10/100-200/4.
- Coordinates fall within a defined Sydney-area bounding box.
- Same seed -> byte-identical reference-entity output across runs.
- TelemetryEvent schema contains all §6 fields plus `schema_version`.
- Validation thresholds externalized to config, no scattered magic constants.
**Test requirements:** determinism test; count-matches-config test;
coordinate-bounding-box test; schema-validation negative test.
**Security:** synthetic only; no PII; no secrets in config.
**Cost:** zero — local only.
**Documentation:** `simulator/README.md` schema/field contract and
`schema_version` policy.
**Definition of done:** PR on `agent/data/*`, CI python job green,
deterministic tests executed with evidence, QA review, handover.

## WE-01: Website Shell, Navigation & Mode-Indicator Foundation
**Objective:** Scaffold Next.js/React/TS/Tailwind with routing for the 10
pages (§17) and a reusable ModeIndicator enforcing LIVE/CONNECTED TEST/
RECORDED/DEMO labelling (§14, §18).
**Value:** The only reviewer-facing entry point, and the compliance mechanism
every later page reuses.
**Scope:** `website/` app init, base layout/nav for all 10 pages (stubs
acceptable except Home), `ModeIndicator` defaulting to DEMO on every stub.
**Out of scope:** Page content/data (WE-02/WE-03), backend/API routes, Azure
hosting/deployment, final chart/mapping library choice, CI workflow edits
(Lead-owned).
**Dependencies:** None.
**Acceptance criteria:** `npm run lint`/`npm run build` succeed; all 10 nav
routes resolve; ModeIndicator renders one of 4 states, defaults to DEMO; no
page claims LIVE/CONNECTED TEST without a real data source.
**Test requirements:** ModeIndicator component test; route-smoke test; basic
accessibility smoke check.
**Security:** no secrets/env committed.
**Cost:** zero — no hosting provisioned.
**Documentation:** `website/README.md` test runner/styling conventions.
**Definition of done:** PR on `agent/web/*`, CI website job green, QA review,
handover.

## DE-02: Deterministic Simulator Core Engine
**Objective:** Runtime generator producing normal telemetry events for
configured vehicles moving along routes using DE-01's schemas/config —
seeded, configurable rate, realistic movement, unique `event_id`s,
`schema_version` stamping, structured logs, graceful shutdown (§19).
**Value:** First real, inspectable synthetic data artifact.
**Scope:** `simulator/generator/`: seeded RNG, route/stop movement model,
configurable emission rate, deterministic unique event IDs, structured
JSON-lines logging, SIGINT/SIGTERM graceful shutdown, CLI entrypoint.
**Out of scope:** Incident scenarios (DE-03), batch datasets (DE-04),
Bronze/Silver/Gold, streaming/cloud transport.
**Dependencies:** DE-01.
**Acceptance criteria:** same seed+config -> byte-identical sequence;
emission rate within tolerance; 100% schema conformance in no-incident run;
clean SIGINT shutdown; plausible vehicle movement.
**Test requirements:** determinism test; schema-conformance test; rate test
(synthetic clock, not wall-clock sleeps); shutdown test.
**Security:** synthetic only; no outbound network calls.
**Cost:** zero.
**Documentation:** `simulator/README.md` CLI usage and
determinism/schema_version contract.
**Definition of done:** CI green, tests executed with evidence, QA review,
handover.

## WE-02: Home Page Content with Static Demo Fixtures
**Objective:** Home page (§17) with real content backed by a small
hand-authored static fixture, clearly labelled DEMO.
**Value:** First fully "real" page; proves the DEMO-fallback pattern
end-to-end.
**Scope:** Home page route, `website/fixtures/home.json`, ModeIndicator
(DEMO), responsive Tailwind layout.
**Out of scope:** Live Operations content (WE-03), live/connected data
fetching, backend routes, chart/mapping libraries.
**Dependencies:** WE-01. Fixture shape informed by DE-01's schema
documentation only, not a code dependency.
**Acceptance criteria:** renders summary, nav links, >=1 fixture-sourced stat
block, visible DEMO label; responsive; lint/build pass.
**Test requirements:** page render test; fixture-shape-matches-TS-type test;
accessibility check.
**Security:** no secrets; fixture synthetic only.
**Cost:** zero.
**Documentation:** `website/README.md` fixture/DEMO-mode convention.
**Definition of done:** CI website job green, QA review, handover.

## DE-03: Incident Injection Scenarios in Simulator
**Objective:** Extend DE-02 with the 8 configurable, individually-triggerable
incident scenarios from §13, deterministic under seed.
**Value:** Enables the incident-centric narrative that differentiates this
project.
**Scope:** `simulator/incidents/`: one strategy per §13 item 1-8, config/CLI
toggles with configurable intensity, structured logging of which incident
fired and recovery timing.
**Out of scope:** Downstream processing consequences (DE-06/DE-07) — this
story only proves the generator can produce correct fault conditions.
**Dependencies:** DE-02.
**Acceptance criteria:** each of the 8 scenarios independently enabled
produces the described anomaly; recovery reverts to baseline.
**Test requirements:** one deterministic test per scenario; recovery test;
combined-scenario test.
**Security:** synthetic malformed data only.
**Cost:** zero.
**Documentation:** `simulator/README.md` scenario reference table mapped to
§13 items.
**Definition of done:** CI green, all scenario tests passing with evidence,
QA confirms each §13 scenario is demonstrable, handover.

## WE-03: Live Operations Page — Demo Mode Fleet View
**Objective:** Live Operations page (§17) in DEMO mode — fleet/vehicle list
(optionally simple map) from a static fixture shaped like the
vehicle/telemetry schema, explicitly labelled DEMO.
**Value:** The page reviewers most associate with "real-time transport."
**Scope:** Live Operations page, `website/fixtures/vehicles.json`, a
lightweight list/simple-map component, DEMO ModeIndicator, per-vehicle
occupancy/delay/status display.
**Out of scope:** Live data fetching, streaming client, Fabric
Eventhouse/Eventstream integration, incident-trigger controls.
**Dependencies:** WE-01. Fixture shape informed by DE-01/DE-02 documentation
only, not a code dependency.
**Acceptance criteria:** displays fixture vehicles with route/status/
occupancy/delay; DEMO label unambiguous; zero network/backend calls; lint/
build pass.
**Test requirements:** render test; test asserting no fetch calls in DEMO
mode; accessibility smoke check.
**Security:** no secrets; any paid mapping API key must be escalated, not
defaulted.
**Cost:** zero for this story; mapping-library selection is a deferred
cost-watch item, not approved here.
**Documentation:** `website/README.md` fixture-to-schema alignment and
map/list rationale.
**Definition of done:** CI website job green, QA confirms unambiguous DEMO
labelling and no backend calls, handover.

## DE-04: Synthetic Batch & Historical Reference/Transaction Datasets
**Objective:** Generate the remaining §7 batch sources not covered by DE-01's
live reference entities — schedules, historical trips, historical telemetry
snapshots, passenger journeys, service disruptions, optional weather.
**Value:** Raw material for DE-05 Bronze ingestion and future Fabric
batch/incremental-load work.
**Scope:** `simulator/batch/`: each dataset with `schema_version` and a
watermark-friendly timestamp/partition column; local files with a small
committed sample fixture (bulk data gitignored).
**Out of scope:** Incremental-load engine logic — only produces datasets.
**Dependencies:** DE-01.
**Acceptance criteria:** config-driven volume; full referential integrity
against DE-01 reference data; deterministic under seed; service_disruptions
has fields sufficient to later populate `fact_service_incident`.
**Test requirements:** referential-integrity test; determinism test;
volume-config test; per-dataset schema test.
**Security:** synthetic only; bulk generated files gitignored.
**Cost:** zero.
**Documentation:** `simulator/README.md` batch-dataset catalogue noting
eventual Gold/Warehouse facts/dimensions each dataset supports.
**Definition of done:** CI green, tests passing, QA review, handover, no
large binaries committed.

## DE-05: Local Bronze Layer Ingestion Logic
**Objective:** Local Bronze-layer logic capturing DE-02/DE-03 streaming
events and DE-04 batch datasets with minimal transformation plus required
ingestion metadata (`ingestion_time`, `source`, `run_id`).
**Value:** First medallion-layer slice, directly reusable as future Fabric
Lakehouse Bronze notebook logic.
**Scope:** `simulator/bronze/`: ingest both streaming-shaped and
batch-shaped inputs, append the 3 metadata columns, documented
idempotency/replay approach.
**Out of scope:** Schema enforcement, validation, deduplication, quarantine
(Silver, DE-06); any real Fabric Lakehouse/OneLake write.
**Dependencies:** DE-02/DE-03 (event format), DE-04 (batch format).
**Acceptance criteria:** 100% of input fields preserved unmodified plus 3
metadata columns; re-running same run_id/input produces identical output or
clearly-flagged duplicates per documented policy; works for both shapes.
**Test requirements:** streaming-source test; batch-source test; idempotent-
rerun test; malformed/truncated-input failure test (fails loudly).
**Security:** synthetic only; local file paths only.
**Cost:** zero.
**Documentation:** `simulator/README.md` Bronze contract; explicit
`IMPLEMENTED - NOT CONNECTED` label relative to a real Fabric Lakehouse.
**Definition of done:** CI green, tests executed/passing, QA review,
handover with correct Fabric-connection labelling.

## DE-06: Local Silver Layer Validation, Deduplication & Quarantine Logic
**Objective:** Local Silver logic consuming DE-05 Bronze output, applying the
full §12 validation rules, deduplication, late-event detection, quarantine
routing with required metadata, and §15 counters.
**Value:** Proves the DE-03 incident scenarios have real processing
consequences locally — the most portfolio-critical correctness demonstration
before any Fabric spend.
**Scope:** `simulator/silver/`: every §12 rule, dedup by `event_id`,
late-event flagging vs. watermark, quarantine sink (raw payload, rejection
reason, ingestion time, source, run_id), run-summary with §15 counters and
documented reconciliation identity.
**Out of scope:** Gold aggregates (DE-07); Fabric Eventstream/watermark
integration; Warehouse SQL.
**Dependencies:** DE-05; DE-03 (incident-laced events as primary fixtures).
**Acceptance criteria:** each DE-03 scenario produces the expected Silver
outcome; run-summary counters reconcile exactly to total input; quarantine
retains raw payload losslessly.
**Test requirements:** one positive+negative test per §12 rule; dedup test;
late-event test; reconciliation-identity test; end-to-end mixed-incident test.
**Security:** synthetic only; quarantine store local, no real PII.
**Cost:** zero.
**Documentation:** `simulator/README.md` Silver rule catalogue mapped to
§12; reconciliation-identity formula documented for later Fabric reuse.
**Definition of done:** CI green, full rule-coverage suite passing with
evidence, QA independently re-verifies reconciliation test and one
quarantine case, handover, `IMPLEMENTED - NOT CONNECTED` labelling.

## DE-07: Local Gold Layer Groundwork — Minimal Route Performance Aggregate
**Objective:** Small, single-metric local Gold aggregation (per-route average
`delay_seconds` and valid-event count over Silver's valid output) solely to
close the medallion chain locally. Explicitly groundwork, not the full §10
dimensional model/SCD2.
**Value:** Completes a thin but real Bronze->Silver->Gold vertical slice
before any cloud spend.
**Scope:** `simulator/gold/`: one aggregation over DE-06's "valid" bucket
producing per-route-per-period summary (route_id, period, avg_delay_seconds,
event_count, valid_percentage), CSV/JSON output.
**Out of scope:** `dim_*`/`fact_*` dimensional model, SCD Type 2, Warehouse
SQL, Power BI, vehicle/trip/passenger/incident-level Gold facts (reserved for
the later full Warehouse story).
**Dependencies:** DE-06.
**Acceptance criteria:** aggregate matches hand-computed expected values on a
known fixture; quarantined/duplicate records excluded; deterministic given
deterministic upstream inputs.
**Test requirements:** unit test against a hand-constructed fixture with
manually verified expected values; test confirming quarantined/duplicate
exclusion.
**Security:** synthetic only.
**Cost:** zero.
**Documentation:** `simulator/README.md` note explicitly scoping this as
groundwork, pointing to the later full Warehouse/dimensional-model story for
§10's complete scope.
**Definition of done:** CI green, tests executed/passing, QA confirms scope
note prevents overclaiming, handover.
