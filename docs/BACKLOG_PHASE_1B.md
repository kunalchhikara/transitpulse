# Backlog: Phase 1b — local Fabric-readiness (pre-trial)

Drafted by the Product Manager agent, reviewed and decided by the Lead on
2026-11-24; the owner approved running this local phase before activating
the 60-day Fabric trial. Everything is local: the Fabric trial stays
unactivated (ADR-0004), no Azure/Fabric API calls, zero cost, and everything
new is labelled `IMPLEMENTED - NOT CONNECTED`. No website work.

**Goal:** close the four readiness gaps with complete, tested local logic so
trial time is spent porting tested logic to Fabric, not designing it:
(1) incremental batch loading (PROJECT.md §11); (2) batch Silver + Warehouse
dimensional model with SCD2 (§9-10); (3) Silver hardening; (4) a Lead-owned
Fabric implementation plan.

**Honest schedule note:** this phase pushes trial activation back (roughly
one focused day, possibly more). In exchange, the Warehouse/SCD2 work during
the trial becomes "deploy and verify already-tested logic". The SCD2
invariants in DE-13 are never cut to save time; the phase is extended
instead.

## Lead decisions (resolved)
1. **SQL validation:** Python reference implementation + static T-SQL check
   (`sqlfluff==4.4.0 parse --dialect tsql`, CI-only). See ADR-0006, including
   the known `NOT ENFORCED` parser gap. Python implementation lives in
   `simulator/warehouse/`; T-SQL in `sql/ddl/` and `sql/load/`.
2. **Warehouse source layer:** Silver batch current-state tables (DE-12) feed
   Warehouse dimensions and batch facts; `fact_route_performance` comes from
   DE-07 Gold. ADR-0002 amended (ADR-0006).
3. **DE-10:** trips' `depot_id` follows vehicle transfers (criterion 4 is
   in scope). The committed DE-04 sample may be regenerated.
4. **DE-11:** per-dataset commit and watermark advance. A documented
   watermark meaning plus the no-loss/no-duplicate property test is
   sufficient; the engineer chooses window-upper-bound vs maximum-seen and
   justifies it.
5. **No extra ADR** for the watermark control store and merge semantics:
   DE-11/DE-12 README sections plus the Fabric mapping in ADR-0005 cover it.
6. **`fabric/README.md`:** the Lead makes a one-off pointer edit as part of
   LD-01.
7. **Scope trims accepted:** FU-11 folded into DE-08; FU-04 into DE-09;
   FU-01, FU-07, FU-09 deferred with the gates below; DE-09 is SHOULD;
   DE-14 is COULD (cut first); `fact_passenger_journey` and
   `fact_vehicle_telemetry` deferred.
8. **Concurrency cap:** at most 3 Data Engineer agents at once, each in an
   isolated git worktree. Wave 1 starts DE-11, DE-08, DE-10; DE-09 starts when
   one finishes.
9. **Time:** extend the phase rather than cut DE-13's SCD2 invariants. Cut
   order if needed: DE-14, then DE-09.

## Sequencing and concurrency

| Wave | Story | Priority | Owned paths | Can run with |
|---|---|---|---|---|
| 0 | LD-02 ADR-0006 + CI `sql` job | MUST | `docs/adr/`, `.github/workflows/ci.yml` | all — **done in this PR** |
| 0 | LD-01 Fabric implementation plan (drafting) | MUST | `docs/` | all; finalised last |
| 1 | DE-11 Incremental batch extraction with watermark control into Bronze | MUST (critical path) | `simulator/incremental/` (new) | DE-08, DE-10, DE-09 |
| 1 | DE-08 Silver/Gold quality hardening (FU-06, FU-08, FU-10, FU-11) | MUST | `simulator/silver/`, `simulator/gold/`, `simulator/config/`, `simulator/schemas/telemetry.py` | DE-11, DE-10, DE-09 |
| 1 | DE-10 Vehicle-to-depot change source (new DE-04 dataset) | MUST | `simulator/batch/` | DE-11, DE-08, DE-09 |
| 1 | DE-09 Bronze audit trail + path cleanup (FU-03, FU-04) | SHOULD | `simulator/bronze/` | DE-11, DE-08, DE-10 |
| 2 | DE-12 Batch Silver: validate + merge by primary key | MUST (critical path) | `simulator/silver_batch/` (new) | DE-08, DE-09 |
| 3 | DE-13 Warehouse core: dims, SCD2 `dim_vehicle`, `fact_trip` | MUST (critical path) | `sql/`, `simulator/warehouse/` (new) | — |
| 4 | DE-14 `fact_service_incident`, `fact_route_performance` | COULD | `sql/`, `simulator/warehouse/` | LD-01 finalisation |
| 4 | LD-01 finalised against merged state | MUST | `docs/` | DE-14 |

**Critical path:** DE-11 → DE-12 → DE-13 (also needs DE-10) → LD-01 sign-off.

**Shared touchpoints:**
- `simulator/README.md`: each story edits only its own section and adds its
  entry to the top Status line; the Lead resolves Status-line conflicts at
  merge.
- The committed DE-04 sample (`simulator/batch/sample/output/`): DE-10 may
  regenerate it. DE-11/DE-12 tests must derive expected counts from the
  source `manifest.json` or recompute them, never hardcode them. Whichever of
  DE-10/DE-11 merges second rebases and re-runs the full suite.
- Bronze manifests: DE-09 only adds fields; DE-11/DE-12 rebase if it merges
  first.
- New test files use story-specific names (`test_incremental_*.py`,
  `test_silver_batch_*.py`, `test_warehouse_*.py`).

## Baseline definition of done (every DE story)
- Branch `agent/data/<topic>`, draft PR, CI green (gitleaks, `ruff check
  simulator`, `pytest simulator -q`, and the `sql` job when SQL exists).
- No existing test disabled or weakened; any changed expected value justified
  in the handover.
- Handover per PROJECT.md §26; independent QA verdict PASS or PASS WITH
  CONDITIONS; Lead integration review; human approval to merge.
- README marks the work `IMPLEMENTED - NOT CONNECTED`; nothing claims Fabric
  execution.
- Run artifacts (manifests, audit, control files): no absolute local paths;
  byte-identical with a fixed clock, in-process and across two CLI processes.
- No new dependency without README justification and Lead approval.
- Developers don't edit `docs/`; the Lead updates FU statuses.

---

## DE-08: Silver/Gold quality hardening (FU-06, FU-08, FU-10, FU-11)
**Why one story:** FU-06/08/10 change the same rule catalogue
(`simulator/silver/rules.py`), event-time state (`pipeline.py`) and threshold
config, and overlap behaviourally (absolute bounds remove absurd timestamps
before the watermark guard sees them). FU-11 is folded in because FU-10
already requires editing Gold's attribution (`simulator/gold/aggregate.py`).
**Objective:** before any Fabric/dashboard use, reject impossible or
unrepresentable values in Silver, stop one outlier moving the watermark, and
stop Gold attributing quarantined records to periods their timestamps can't
be trusted for.
**Scope:** `simulator/config/models.py` + `default_validation.toml` (new
thresholds, single source of truth); `simulator/schemas/telemetry.py` only if
new reasons are added to `validate_against_thresholds` (backward compatible);
`simulator/silver/`; `simulator/gold/aggregate.py` (attribution only);
`simulator/tests/test_silver_*.py`, `test_gold_*.py`; README DE-06/DE-07
sections.
**Out of scope:** FU-07; batch Silver; bucket model/dedup policy changes.
**Dependencies:** none.
**Acceptance criteria:**
1. FU-08 delay bound: configurable `delay_seconds` min/max; out-of-range →
   quarantined under a new named rule; `10**30` and `-10**30` quarantined;
   values exactly at the bounds load.
2. FU-08 service area: configurable rule reusing DE-01's
   `sydney_bounding_box` (configurable margin, on/off); `(0, 0)` quarantined;
   points just inside/outside each edge behave correctly; DE-03
   `invalid_coordinates` events keep `valid_coordinates` as primary rule;
   precedence documented.
3. FU-10 time bounds: configurable absolute min/max for `event_time` and
   `generated_at`; `9999-12-31` and `0001-01-01` quarantined under a named
   rule; every clean record's `event_time` within bounds.
4. No false positives: with defaults, 100% of DE-02 baseline output, DE-03
   congestion/breakdown output and DE-04 default `historical_telemetry` pass
   every new rule (tested; zero new quarantines).
5. FU-06 guard: QA's reproduction (one valid-looking event with
   `generated_at` and `event_time` far in the future but inside FU-10 bounds)
   becomes a regression test in which every other event's late/out-of-order
   flags equal an independently computed outlier-free baseline. The outlier
   is quarantined under a named rule, or loaded-and-flagged without advancing
   the watermark. Behaviour and reference point configurable and documented.
6. With the guard enabled and no outlier present, every existing DE-06/DE-07
   test result is unchanged.
7. FU-10/FU-11 in Gold: quarantined records rejected by any event-time rule
   are unattributed under a named reason; `supported_schema_version`
   failures unattributed; Gold re-applies future-tolerance and bounds to
   stage-1 rejects (e.g. negative speed + far-future `event_time` is not
   attributed); no Gold row has a period outside the configured bounds.
8. README: fail-closed behaviour documented (a COMMITTED Silver manifest with
   `reconciliation.holds=false` blocks Gold); rule catalogue lists new rule
   ids, precedence, defaults, triggering DE-03 modes; manifest
   `rule_catalogue` includes them.
9. All existing Silver/Gold reconciliation identities still hold.
**Tests:** positive/negative/boundary per new rule; FU-06 regression vs
independent baseline; extreme dates; false-positive sweep; every new Gold
attribution branch; CLI chain byte-identical across two processes.
**DoD:** baseline + QA independently re-runs the FU-06 reproduction and one
FU-10 extreme-date case.

## DE-09: Bronze audit trail and manifest path cleanup (FU-03, FU-04) — SHOULD
**Objective:** keep earlier FAILED attempts when a run_id is retried; stop
Bronze manifests recording absolute local paths.
**Value:** DE-11 retries Bronze runs with deterministic run ids, so FU-03
directly weakens §15 "retries" evidence; manifests will become RECORDED
evidence, so FU-04 must be fixed. Silver and Gold already do both.
**Scope:** `simulator/bronze/`; `simulator/tests/test_bronze_*.py` (new +
non-weakening extensions); README DE-05 section.
**Out of scope:** Bronze record contract, idempotency table, replay policy.
**Acceptance criteria:**
1. Retrying a FAILED run_id keeps every earlier failed attempt (status,
   error, started/ended, counts) in `previous_attempts`, same shape as
   Silver/Gold. Two failures then a success → exactly 2 entries; an
   `ALREADY_COMMITTED` no-op adds nothing.
2. Called with absolute input paths, no Bronze manifest field starts with
   `/` or contains the home directory string; path policy documented.
3. Pre-existing manifests (without new fields) still accepted by
   `BronzeStore`, Silver and Gold (fixture-tested).
4. All existing Bronze/Silver/Gold tests pass unchanged.
**DoD:** baseline + QA greps a manifest produced from absolute inputs.

## DE-10: Vehicle-to-depot assignment change source (SCD2 input)
**Objective:** add a deterministic, effective-dated vehicle-to-depot
assignment dataset to DE-04 so the Warehouse has a real change history for
SCD2 `dim_vehicle`.
**Scope:** `simulator/batch/` (model + `DATASET_CATALOGUE` entry, generation
module, `[fleet_changes]`-style config section validated in `config.py`,
depot attribution in trip generation); regenerated
`simulator/batch/sample/output/` if needed; `simulator/tests/test_batch_*.py`;
README DE-04 section.
**Out of scope:** vehicle route assignment, vehicle selection, timetable
logic; FU-01; SCD2 logic (DE-13).
**Acceptance criteria:**
1. New dataset (working name `vehicle_depot_assignments`) in
   `DATASET_CATALOGUE`: primary key; `updated_at` watermark; `event_date`
   partition (local effective service date); FKs to `vehicles.vehicle_id` and
   `depots.depot_id`; change type `INITIAL` / `TRANSFER`.
2. Exactly one `INITIAL` row per DE-01 vehicle at `start_date` with its DE-01
   home depot; `TRANSFER` rows deterministic and config-driven, always to a
   different depot, effective at a local service-date boundary; no two
   changes for one vehicle share an effective date; default config yields ≥ 3
   transfers in the default 7 days including one vehicle with ≥ 2 transfers;
   `updated_at` meaning documented.
3. Volume formula documented and tested.
4. (Lead decision 3, in scope) For 100% of `schedules`, `historical_trips`
   and trip-scoped `service_disruptions` rows, `depot_id` equals the
   vehicle's effective depot on that row's `event_date`.
5. With fleet changes disabled, every pre-existing dataset file is
   byte-identical to the pre-story output (same seed). Tested.
6. Determinism, prefix stability and committed-sample reproducibility tests
   pass; referential-integrity tests cover the new FKs.
**DoD:** baseline.

## DE-11: Incremental batch extraction with watermark control into Bronze (§11)
**Objective:** a local, deterministic batch loader that reloads DE-01
reference entities in full, loads DE-04 transactional datasets incrementally
by `updated_at` watermark, lands each extract in Bronze through the DE-05
library, advances a dataset's watermark only after its Bronze commit
succeeds, and demonstrates retries, partial failure, reruns, idempotency,
duplicate prevention, reconciliation and audit logging.
**Value:** maps directly to a Fabric Data Factory pipeline (lookup activity,
watermark control table, copy into the Lakehouse); proving it locally saves
the most trial time.
**Scope:** `simulator/incremental/` (config TOML + pydantic, control store,
extractor, orchestrator, CLI `python -m simulator.incremental`);
`simulator/tests/test_incremental_*.py`; its README section. Calls
`simulator.bronze` and `simulator.batch` through public APIs only.
**Out of scope:** Silver merge (DE-12); streaming watermarks; concurrent
writers; Data Factory/Fabric artifacts.
**Dependencies:** DE-04, DE-05 (merged). Soft: DE-10 (catalogue-driven, picked
up automatically), DE-09 (rebase if it merges first).
**Acceptance criteria:**
1. Per-dataset mode (`full` | `incremental`) and order in config. `full`:
   DE-01 reference entities as JSON lines with `schema_version`.
   `incremental`: DE-04 datasets using the catalogue's watermark column.
2. Windows: initial load selects everything up to `as_of`; later loads select
   `watermark < updated_at <= as_of`. `--as-of` required with a timezone
   offset; an `as_of` earlier than a dataset's current watermark is refused
   with no state change. Watermark meaning documented with rationale.
3. Commit then advance: each extract written as a DE-04-shaped directory
   (JSON lines + manifest with `row_count`, `sha256`), ingested by
   `simulator.bronze`; watermark advances only after Bronze returns COMMITTED
   or ALREADY_COMMITTED.
4. Crash-safe idempotency: Bronze run id derived deterministically from
   (dataset, window). Injected crashes after extract, during Bronze ingest,
   after Bronze commit before the watermark write, and after the watermark
   write before the audit record: every rerun ends with zero extra Bronze
   records and the same final control state as an uninterrupted run.
5. Partial failure: if dataset X fails, X's watermark is unchanged, committed
   datasets keep their advanced watermarks, the pipeline run is `PARTIAL`, a
   rerun processes only X, and final Bronze contents + watermarks are
   byte-identical (fixed clock) to an uninterrupted run.
6. Retries: transient failures retried up to a configured maximum with
   configured backoff on an injectable clock (no real sleep); on exhaustion
   the dataset is `FAILED` with watermark unchanged.
7. No loss / no duplicates: for ≥ 3 `as_of` cut sequences (incl. cut-offs
   exactly equal to an `updated_at` value, and empty windows), the union of
   extracted primary keys equals the source keys with `updated_at <= final
   as_of`, with no key extracted twice; expected sets computed directly from
   the source files; also holds when the source is regenerated with more
   `service_days` between runs.
8. Full reference reload of unchanged content is a recorded no-op (e.g.
   `SKIPPED_UNCHANGED`): not a failure, no Bronze `DuplicateInputError`, no
   duplicate rows. Changed content creates a new Bronze run.
9. Empty window → recorded success with 0 rows. A malformed source line or a
   missing/unparseable `updated_at` is never skipped silently: configured
   policy `strict` (dataset fails, watermark unchanged) or `preserve` (Bronze
   parse-error record, counted). Both tested; default documented.
10. One immutable audit record per attempt (never overwritten, never appended
    into a reused file) with §15 fields: pipeline run id, dataset, attempt
    number, start/end/duration, window low/high, watermark before/after,
    source rows in window, extracted count, Bronze `records_written`,
    rejected/parse-error counts, status, error. Identity `source rows in
    window == extracted == Bronze records_written` asserted; if it fails, the
    dataset fails and the watermark is not advanced.
11. FU-02: no cross-dataset ordering imposed; a trip extracted before the
    disruption it references loads normally; `event_date` passed through
    unchanged.
12. Every log line carries the run id (don't repeat FU-09); control/audit
    files contain no absolute paths (FU-04); single-writer limitation
    documented.
**Tests:** the §21 batch tests (initial, incremental, rerun/idempotency,
empty source, bad source, partial failure, retry exhaustion,
reconciliation); crash injection; no-loss/no-duplicate property tests; two
CLI processes byte-identical with a fixed clock. Failures injected via
dependency injection, not hidden production flags (any demo flag documented).
**Documentation:** load-mode table, window/watermark semantics,
idempotency/retry table, audit schema, reconciliation identity, "Mapping to
Fabric" table.
**DoD:** baseline + QA independently re-runs partial-failure-then-rerun and
one crash-injection case, checking Bronze row counts match the source.

## DE-12: Batch Silver — validate, standardise, merge by primary key
**Objective:** consume committed Bronze batch runs (each applied once, in
commit order), validate strictly against DE-04/DE-01 models with lossless
quarantine, merge valid rows into current-state tables by catalogue primary
key with explicit per-row outcomes, and reconcile every run.
**Scope:** `simulator/silver_batch/` (new; separate from DE-06's
`simulator/silver/`); `simulator/tests/test_silver_batch_*.py`; its README
section. Reuses `BronzeStore` primitives and DE-04 models by import.
**Out of scope:** §12 telemetry rules on `historical_telemetry` (DE-04 model
validation only; recorded limitation); DE-05/DE-06 changes; batch Gold.
**Dependencies:** DE-11 merged; DE-10 merged if its dataset is in the
catalogue by then.
**Acceptance criteria:**
1. Reads Bronze only through committed manifests; keeps a checkpoint of
   applied Bronze run ids; re-applying one is a no-op.
2. Bronze parse errors → quarantine with raw bytes recoverable; strict model
   failures (unsupported `schema_version`, missing field, wrong type, bad
   enum, unknown field) → quarantine with named rule ids; validate before
   dedup.
3. Per-row merge outcomes: `inserted` (new key); `updated` (newer
   `updated_at`); `unchanged` (same key, same `updated_at`, same payload);
   `stale` (older `updated_at`; excluded and audited); `conflict` (same key,
   same `updated_at`, different payload; quarantined under a named rule).
4. Full-load reference tables use snapshot semantics with
   inserted/updated/unchanged/deleted counts; deletion policy documented.
5. Per-dataset reconciliation asserted before commit: `extracted == inserted
   + updated + unchanged + stale + quarantined`; `rows_after == rows_before +
   inserted` (incremental) or the valid snapshot count (full); line counts on
   disk equal counters. Any failure → nothing committed, FAILED manifest.
6. FU-02: rows referencing a not-yet-loaded parent still load and are
   counted per FK as `unresolved_references`; `event_date` unchanged.
7. k incremental DE-11 windows then batch Silver give current-state tables
   identical (apart from lineage columns) to one load at the final `as_of`.
8. Idempotency/determinism as DE-06 (same run id + input → ALREADY_COMMITTED;
   different input refused; failed attempts kept in `previous_attempts`;
   fixed clock byte-identical across processes; no absolute paths).
9. Every catalogue dataset goes through the same code path; dedicated tests
   for `historical_trips`, `service_disruptions`, the DE-10 dataset and the
   four reference entities.
**DoD:** baseline + QA independently re-runs the equivalence test and a
conflict-quarantine case.

## DE-13: Warehouse core — dimensions, SCD2 `dim_vehicle`, `fact_trip`
**Objective:** source-controlled T-SQL DDL + load scripts for a Fabric
Warehouse (`dim_date`, `dim_time`, `dim_route`, `dim_stop`, `dim_depot`,
`dim_vehicle` SCD2 on depot, `fact_trip`) plus the Python reference
implementation and tests proving SCD2 and point-in-time lookup (ADR-0006).
**Scope:** `sql/` (`ddl/`, `load/`, `README.md` with per-table Lakehouse vs
Warehouse placement rationale, `.sqlfluffignore` only per ADR-0006);
`simulator/warehouse/`; `simulator/tests/test_warehouse_*.py`.
**Out of scope:** `fact_passenger_journey`, `fact_vehicle_telemetry` (needs
FU-01); DE-14 facts; semantic model; running anything on Fabric.
**Dependencies:** LD-02 (this PR), DE-12, DE-10 merged.
**Acceptance criteria:**
1. DDL for all seven tables with surrogate keys and an unknown member (`-1`)
   in every dimension, using only types/features Microsoft documents for
   Fabric Warehouse (cite non-obvious choices to Microsoft Learn in
   `sql/README.md`: datetime precision, varchar vs nvarchar, `NOT ENFORCED`
   constraints, surrogate key generation, `MERGE` vs `UPDATE`+`INSERT`).
   Anything unverifiable locally goes on a "verify in Fabric" list.
2. `dim_vehicle`: depot Type 2, every other attribute Type 1 (documented);
   `valid_from`, `valid_to` (open-ended convention documented), `is_current`,
   version number or row hash; effective dating on local `event_date`.
3. SCD2 invariants asserted on every load and tested: exactly one current row
   per vehicle; intervals contiguous, non-overlapping, ordered; versions = 1 +
   effective changes; rerun with no changes adds 0 rows; A→B→A gives 3
   versions; multiple changes for one vehicle in one load all versioned in
   order; a backdated change follows a documented policy (correct split or
   loud failure), never overlapping intervals.
4. k incremental loads equal a rebuild from the full change log.
5. `fact_trip`: one row per Silver `historical_trips` row; `vehicle_sk`
   looked up as of `event_date` (a trip before and one after a transfer
   resolve to different versions); trip `depot_id` equals the resolved
   version's depot for 100% of rows; unknown-member lookups counted (0 on
   generated data; an injected orphan resolves to `-1`).
6. `dim_date` covers every fact `event_date`; `dim_time` has 1,440
   minute-grain rows; key formats documented.
7. Reconciliation: `fact_trip` count and sums (delay seconds, boardings)
   equal Silver; each dimension's count equals distinct natural keys + 1.
8. Every SQL script passes the CI `sql` job; `sql/README.md` states the
   T-SQL is "IMPLEMENTED - NOT CONNECTED, NOT EXECUTED"; Python and T-SQL
   describe the same rules.
9. Idempotent and deterministic.
**DoD:** baseline + QA independently checks the SCD2 invariants on a fresh
multi-transfer fixture and confirms the README does not overclaim execution.

## DE-14: `fact_service_incident`, `fact_route_performance` — COULD (cut first)
**Objective:** extend DE-13 with `fact_service_incident` (from Silver batch
`service_disruptions`) and `fact_route_performance` (from one committed Gold
`route_performance` run).
**Dependencies:** DE-13 and DE-08 merged.
**Acceptance criteria:**
1. `fact_service_incident`: one row per disruption; keyed to date, time,
   route, depot; stop key and point-in-time `dim_vehicle` key only for
   trip-scoped disruptions; route-scoped rows use a documented "not
   applicable" member distinct from unknown; measures `duration_minutes`,
   `affected_trip_count`, `estimated_delay_seconds`; reconciles to Silver.
2. `fact_route_performance`: keyed to `dim_route`, `dim_date`, `dim_time`
   (local period start); additive measures only (`event_count`,
   `sum_delay_seconds`, `quarantined_count`, late/out-of-order counts);
   averages computed in the semantic layer; reconciles to Gold manifest
   totals.
3. Same validation method as DE-13; idempotent and deterministic.
**DoD:** baseline.

## LD-02: ADR-0006 — Warehouse SQL validation and source layer (Lead) — DONE
Delivered in the PR that adds this file: ADR-0006 (Accepted), ADR-0002
amendment, CI `sql` job (`sqlfluff==4.4.0 parse --dialect tsql`, gated on
`sql/**/*.sql`). Local evidence recorded in ADR-0006.

## LD-01: Fabric implementation plan — ADR-0005 + `docs/` (Lead)
**Objective:** the outputs required by `prompts/FABRIC_READINESS_PROMPT.md`:
exact Fabric artifacts; dependency order; automated vs manual steps; trial
cost status; security/authentication; validation evidence after each step;
rollback/cleanup — plus the readiness checklist with evidence. Does not
activate anything.
**Scope:** `docs/adr/0005-fabric-implementation-plan.md`,
`docs/FABRIC_IMPLEMENTATION_PLAN.md`, a pointer line in `fabric/README.md`
(one-off Lead edit, decision 6).
**Dependencies:** drafting now; finalised against merged DE-08, DE-11, DE-12,
DE-13 (and DE-14 if done).
**Acceptance criteria:**
1. Every planned artifact listed (workspace on trial capacity; Lakehouse
   layout decided and justified; Bronze/Silver/Gold notebooks; Data Factory
   pipeline with watermark control + audit tables; Eventstream; optional
   Eventhouse; Warehouse; semantic model + report; Git integration), each
   mapped to the local component it ports and its repo path.
2. Dependency-ordered steps, each marked automated or manual with the
   identity used (interactive user during development; least-privilege
   workload identity later), noting tenant-admin settings the owner must
   enable.
3. Trial-cost status per artifact; anything that could create Azure charges
   (Key Vault, Event Hubs, storage accounts, paid capacity) marked "escalate
   to owner, not in plan by default"; Eventstream connection secrets kept in
   local `.env` only.
4. Per step: the evidence that proves it worked, and a rollback/cleanup
   procedure.
5. Before-expiry export/commit/recording plan (PROJECT.md §30).
6. Readiness checklist answered with evidence (commit SHA, test results on
   `main`, gitleaks status, AWS-dependency absence, website DEMO fallback) or
   marked as a gap.
7. Every Fabric capability claim cited to Microsoft documentation or listed
   under "verify at activation" (including DE-13's "verify in Fabric" list);
   nothing implies Fabric execution has happened.
8. States that activation requires explicit owner approval (ADR-0004,
   PROJECT.md §27).
**DoD:** ADR-0005 Accepted, merged via PR with a QA verdict; owner reviews
the go/no-go.

---

## Follow-up handling in this phase
| FU | Decision | Gate |
|---|---|---|
| FU-02 | DE-11 (criterion 11) + DE-12 (criterion 6) | Resolved when both merge |
| FU-03, FU-04 | DE-09 (SHOULD) | — |
| FU-06, FU-08, FU-10 | DE-08 | Before Fabric/dashboards |
| FU-11 | folded into DE-08 | — |
| FU-01 | deferred | Before `fact_vehicle_telemetry` from live data, any live-to-schedule join, or vehicle-utilisation metrics |
| FU-07 | deferred | Before the Data Quality page or Power BI "Data Engineering Health" uses `quality_percentage` |
| FU-09 | deferred for existing CLIs; new CLIs must not repeat it | Before any log-based reconciliation is used as evidence |

## Traceability
| Requirement | Stories |
|---|---|
| §7 full vs incremental loads | DE-11 |
| §9 Lakehouse vs Warehouse placement | DE-13, LD-02 |
| §10 dimensions, facts, SCD2 | DE-10, DE-13, DE-14 |
| §11 incremental loading behaviours | DE-11, DE-12 |
| §12 rules | DE-08 |
| §15 batch metrics | DE-11, DE-12, DE-09 |
| §21 batch + SCD tests | DE-11, DE-12, DE-13 |
| §22 SQL validation check | LD-02 |
| §23, §30 | LD-01 |

## Phase 1b exit criteria
- All MUST stories (DE-08, DE-10, DE-11, DE-12, DE-13, LD-01, LD-02) merged
  with QA PASS / PASS WITH CONDITIONS.
- `pytest simulator -q` and the `sql` job green on `main`.
- LD-01 readiness checklist complete with evidence.
- Any SHOULD/COULD story not done recorded as a backlog limitation.
- Trial activation then waits for a separate, explicit owner approval.
