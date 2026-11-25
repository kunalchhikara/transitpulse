# simulator/

Owned by: Data Platform Engineer (`.claude/agents/data-engineer.md`).

Purpose: deterministic synthetic event generator, synthetic reference/domain data
(vehicles, routes, stops, depots, schedules), incident simulation logic, and local
Bronze/Silver/Gold transformation logic that can be tested without Fabric, per
`PROJECT.md` sections 5-13, 19.

Status: DE-01 (Synthetic Transport Domain Model & Schemas), DE-02
(Deterministic Simulator Core Engine), DE-03 (Incident Injection Scenarios in
Simulator), DE-04 (Synthetic Batch & Historical Datasets), DE-05 (Local
Bronze Layer Ingestion Logic), DE-06 (Local Silver Layer for the telemetry
stream), DE-07 (Local Gold Layer Groundwork - minimal route performance
aggregate) and DE-11 (Incremental batch extraction with watermark control
into Bronze) implemented, all local only (IMPLEMENTED - NOT CONNECTED).
Remaining local work: Silver for the DE-04 batch datasets and the QA
follow-ups (FU-01..FU-08) in `docs/BACKLOG.md`; the full PROJECT.md section
10 dimensional model is the later Warehouse story.

## Dependency choice: Pydantic

`pydantic` (>=2.4,<3) is the one third-party dependency this package adds
(`simulator/requirements.txt`). Justification, per CLAUDE.md's "don't add
dependencies without a concrete reason" rule:

- The story's core deliverable *is* schema validation (reference entities and
  the `TelemetryEvent` contract), including a required negative test ("missing
  required field fails validation") and configurable business thresholds
  (`PROJECT.md` section 12) layered on top of structural validation.
- Hand-rolling that with stdlib `dataclasses` would mean writing and
  maintaining our own `__post_init__` type/range-checking and error
  aggregation for every model — pydantic already does this, with clear
  `ValidationError`s that list every failing field, which is directly useful
  for the quarantine/reconciliation work later stories build on.
- It is a single, mature, dependency-light library (no transitive C-extension
  requirements beyond its own compiled core), widely used in the Python data
  ecosystem, and does not pull in any cloud SDK, network client or AWS-related
  package — it stays inside the "Microsoft Azure/Fabric or vendor-neutral"
  boundary.
- All other config parsing uses Python 3.11's stdlib `tomllib` (read-only TOML
  parser) rather than adding a YAML dependency, keeping the dependency surface
  to the one library actually earning its place.

## Layout

- `simulator/config/` - externalized configuration. `default_scale.toml`
  (entity counts, Sydney-area bounding box, fleet/route/depot shape) and
  `default_validation.toml` (data-quality thresholds, `PROJECT.md` section 12).
  Loaded via `simulator.config.load_scale_config()` /
  `load_validation_thresholds()`, both pydantic-validated on load and both
  accepting an alternate path so thresholds/scale can be retuned without code
  changes.
- `simulator/schemas/` - the domain model contract:
  - `entities.py`: `Depot`, `Stop`, `Route`, `Vehicle` reference entities.
  - `telemetry.py`: `TelemetryEvent` (the streaming contract, `PROJECT.md`
    section 6) plus `validate_against_thresholds()`, which applies the
    *configurable* business rules (max speed, future-time tolerance, known
    route, schema-version support) on top of `TelemetryEvent`'s structural
    validation.
- `simulator/reference/` - deterministic generation of the reference dataset
  (`generate_reference_dataset(config)`) plus a small CLI
  (`python -m simulator.reference --output-dir <dir>`) to write it to JSON.
- `simulator/generator/` - DE-02's runtime telemetry generator: see
  "DE-02: Deterministic Simulator Core Engine" below.
- `simulator/incidents/` - DE-03's incident injection scenarios: see
  "DE-03: Incident Injection Scenarios in Simulator" below.
- `simulator/tests/` - pytest suite (determinism, count-matches-config,
  coordinate-bounding-box, referential integrity, schema-validation positive
  and negative cases, DE-02's generator/movement/rate/shutdown tests, plus
  DE-03's per-scenario/recovery/combined/determinism incident tests).
  Run with `pytest simulator -q` from the repo root.

## TelemetryEvent field contract (PROJECT.md section 6)

`simulator.schemas.telemetry.TelemetryEvent` is the streaming event contract.
Every field below is **required** (a missing field raises
`pydantic.ValidationError` at construction time — see
`simulator/tests/test_telemetry_schema.py::test_missing_required_field_fails_validation`):

| Field | Type | Notes |
|---|---|---|
| `event_id` | str | Unique per event; dedup key for later Silver-layer logic. |
| `schema_version` | str | See "schema_version policy" below. Defaults to `CURRENT_SCHEMA_VERSION` if omitted. |
| `generated_at` | datetime | When the simulator produced the record. |
| `event_time` | datetime | When the vehicle observed the reading. May diverge from `generated_at` to simulate late/out-of-order delivery (section 13, scenarios 3-4). |
| `vehicle_id` | str | References a generated `Vehicle.vehicle_id`. |
| `route_id` | str | References a generated `Route.route_id`. |
| `trip_id` | str | Scheduled-trip identifier (trip generation is a later story). |
| `latitude` | float | Structurally constrained to [-90, 90]; business-level Sydney-bounding-box/known-route checks are a Silver-layer concern. |
| `longitude` | float | Structurally constrained to [-180, 180]. |
| `speed_kmh` | float | Structurally >= 0; the configurable upper ceiling (`max_speed_kmh`) lives in `default_validation.toml`, not on the model. |
| `heading` | float | Degrees, structurally [0, 360). |
| `occupancy` | int | Structurally >= 0; configurable upper bound in `default_validation.toml`. |
| `delay_seconds` | int | Signed - negative means early. |
| `status` | `VehicleStatus` enum | `IN_SERVICE`, `DELAYED`, `BREAKDOWN`, `OUT_OF_SERVICE`. |

`ingestion_time`/`processing_time` (also mentioned in section 6) are
deliberately **not** part of this event contract — they are stamped by the
Bronze-layer ingestion logic that *receives* the event (a later story,
DE-05), not by the simulator that emits it.

Reference entities (`Depot`, `Stop`, `Route`, `Vehicle`, in
`simulator/schemas/entities.py`) each also carry their own `schema_version`
field (`ENTITY_SCHEMA_VERSION`, currently `"1.0.0"`), tracked independently of
the telemetry schema version since the two evolve on separate timelines.

## schema_version policy

- `CURRENT_SCHEMA_VERSION` (`simulator/schemas/telemetry.py`) is the version
  every newly-generated `TelemetryEvent` is stamped with by default. Bump it
  (e.g. `"1.1.0"`) whenever `TelemetryEvent`'s field set or semantics change
  in a way a downstream consumer must be aware of.
- `SUPPORTED_SCHEMA_VERSIONS` is the hard structural floor this codebase can
  still parse at all.
- `ValidationThresholds.supported_schema_versions`
  (`simulator/config/default_validation.toml`) is the separate,
  **externally-configurable** business rule a Silver-layer validator applies
  to decide whether an incoming event's `schema_version` is currently
  *accepted* (vs quarantined as an unsupported-schema-version failure, per
  `PROJECT.md` section 12) — this can be narrower than
  `SUPPORTED_SCHEMA_VERSIONS` while a migration is in flight, without a code
  change.
- A schema change that adds/removes/retypes a required field is a breaking
  change: bump `CURRENT_SCHEMA_VERSION`, add the new version to
  `SUPPORTED_SCHEMA_VERSIONS`, and update `default_validation.toml`
  deliberately (never silently).

## Scale configuration (PROJECT.md section 5)

`simulator/config/default_scale.toml` defaults to 50 vehicles, 10 routes, 150
stops (configurable range 100-200), 4 depots, seeded with `seed = 42`, and
constrains every generated coordinate to a Greater Sydney metro bounding box
(`sydney_bounding_box`: roughly lat -34.05..-33.55, lon 150.55..151.35). All
of these are config-driven — change the TOML (or pass an alternate path to
`load_scale_config()`) to rescale; no counts are hardcoded in
`simulator/reference/generator.py`.

## Determinism contract

`generate_reference_dataset(config)` is deterministic: the same `config`
(specifically the same `seed`) always produces the same entities, because all
randomness is drawn from a single `random.Random(seed)` instance consumed in a
fixed order (depots -> stops -> routes -> vehicles), with no other source of
non-determinism (no wall-clock reads, no unordered-collection iteration).
`ReferenceDataset.to_json()` serializes with `sort_keys=True` for a stable
byte-for-byte comparison. This is verified both in-process
(`test_reference_generation.py`) and across two separate OS-process runs of
the `python -m simulator.reference` CLI
(`test_reference_cli_determinism.py`).

## Running tests

```
pytest simulator -q
```

## DE-02: Deterministic Simulator Core Engine

`simulator/generator/` is the runtime telemetry generator: it produces
normal (non-incident) `TelemetryEvent`s for every vehicle in a DE-01
reference dataset, moving each vehicle along its assigned route's stop
sequence. Incident scenarios (bad data, duplicates, late/out-of-order
delivery, congestion, breakdown, volume spike, recovery) are **not**
implemented here — see `docs/BACKLOG.md` DE-03.

### CLI usage

```
python -m simulator.generator
python -m simulator.generator --max-events 500 --output events.ndjson
python -m simulator.generator --generator-config my_gen.toml --seed 7
python -m simulator.generator --log-file generator.log
```

| Flag | Default | Meaning |
|---|---|---|
| `--scale-config PATH` | bundled `default_scale.toml` | DE-01 `ScaleConfig` (vehicle/route/stop/depot counts, bounding box). |
| `--generator-config PATH` | bundled `simulator/generator/default_generator.toml` | DE-02 `GeneratorConfig` (emission rate, movement/dwell dynamics, value ranges). |
| `--seed INT` | config's `seed` | Overrides the generator config's seed. |
| `--max-events INT` | unbounded | Stop after this many events (otherwise runs until SIGINT/SIGTERM). |
| `--output PATH` | stdout | Where event JSON-lines are written. |
| `--log-file PATH` | stderr | Where structured JSON-lines logs are written. |
| `--log-level LEVEL` | config's `log_level` | Overrides the generator config's log level. |

One `TelemetryEvent` is written per line (`model_dump_json()`), flushed
immediately after each write. Structured JSON-lines logs (one JSON object
per line: `timestamp`, `level`, `logger`, `message`, optional `context`) go
to stderr (or `--log-file`) — **never** interleaved with the stdout/`--output`
event stream.

### Configuration: `simulator/generator/config.py` / `default_generator.toml`

`GeneratorConfig` is deliberately a *separate* config surface from
`simulator/config/` (DE-01's `ScaleConfig`/`ValidationThresholds`, which this
story does not modify) — it is specific to the telemetry-generation runtime:
`seed`, `events_per_second`, `simulated_start_time`, cruise-speed range,
dwell-speed jitter, depart probability, occupancy-fraction range, delay range
and the DELAYED-status threshold. All are externalized to
`simulator/generator/default_generator.toml`, loaded via
`load_generator_config()` (pydantic-validated, accepts an alternate path),
per `PROJECT.md` section 19.

### Movement model: `simulator/generator/movement.py`

Each vehicle alternates between **at_stop** (stationary, near-zero speed,
probabilistic per-tick departure) and **cruising** (moving in a straight
line from its current stop toward the next one at a sampled cruising speed,
position linearly interpolated by the great-circle-distance fraction
covered in the elapsed simulated time). On reaching the next stop it snaps
exactly onto that stop's coordinates and resumes dwelling; after the route's
last stop it loops back to the first (`lap_number` increments), modelling
an ongoing route service. `heading` is the compass bearing from the current
stop toward the next one. Because position only ever changes by
`speed_kmh * dt_seconds` of travel per tick — bounded by the configured
cruise-speed ceiling — consecutive events for the same vehicle cannot
"teleport" an implausible distance (verified in
`simulator/tests/test_movement.py`).

### Emission rate and scheduling: `simulator/generator/core.py` / `runner.py`

Vehicles are visited round-robin, one per tick, in the reference dataset's
fixed list order, at an overall rate of `events_per_second` (so each vehicle
is itself updated every `vehicle_count / events_per_second` simulated
seconds — used as that vehicle's movement `dt`). `runner.run()` paces ticks
against an injectable `Clock` (`simulator/generator/clock.py`):
`SystemClock` for real/live CLI runs, `FakeClock` (advances a virtual
counter instead of sleeping) for the rate test, so emission-rate pacing is
verified without any real `time.sleep` — see
`simulator/tests/test_emission_rate.py`.

### Determinism and schema_version contract

Same `ScaleConfig` + `GeneratorConfig` (hence the same seeds) ->
byte-identical event sequence, verified both in-process and across two
genuinely separate `python -m simulator.generator` subprocess runs
(`simulator/tests/test_generator_determinism.py`). This holds because:

1. The DE-01 reference dataset is itself deterministic.
2. Vehicles are visited in a fixed round-robin order.
3. All randomness comes from one `random.Random(generator_config.seed)`
   instance, consumed in a fixed per-tick order.
4. `event_time`/`generated_at` come from a deterministic *virtual* timeline
   (`simulated_start_time + tick / events_per_second`), never from
   wall-clock `datetime.now()` — this is what makes two separate runs
   byte-identical regardless of how long each run actually took in real
   time.

Every emitted event is stamped with `schema_version = CURRENT_SCHEMA_VERSION`
(DE-01's constant) and, in a no-incident run, `status` is only ever
`IN_SERVICE` or `DELAYED` (never `BREAKDOWN`/`OUT_OF_SERVICE`, which are
DE-03's scope).

### Graceful shutdown

`simulator/generator/cli.py` installs SIGINT/SIGTERM handlers that only flip
a `threading.Event` flag (never raise) so an in-progress `time.sleep` in the
pacing loop returns early and `runner.run()`'s own top-of-iteration
`should_stop()` check ends the run — always between two complete,
already-fully-written-and-flushed events, never mid-write. Exit code is 0.
Verified by sending a real SIGINT to a running subprocess and asserting
every output line is complete, parseable JSON with no truncated last line
(`simulator/tests/test_graceful_shutdown.py`).

## DE-03: Incident Injection Scenarios in Simulator

`simulator/incidents/` extends DE-02 with the 8 configurable,
individually-triggerable incident scenarios from `PROJECT.md` section 13.
It is purely additive on top of `simulator/generator/`: `IncidentInjector`
(`simulator/incidents/injector.py`) *wraps* a `TelemetryGenerator` rather
than forking it — every event starts as exactly what
`TelemetryGenerator.produce_event(tick)` would have produced, called with
strictly increasing `tick` values exactly once each, preserving DE-02's own
determinism contract. The one hook point added to `simulator/generator/`
itself is a single additive parameter on `simulator.generator.runner.run`:
`rate_multiplier` (a tick -> float callback, default `None` = unmodified
behaviour, used only by the volume-spike scenario to scale the pacing
interval) — see that module's "DE-03 hook point" docstring note. Everything
else (malformed-data corruption, duplicate replay, late/out-of-order
delivery, congestion, breakdown) lives entirely in `simulator/incidents/`
and never touches `simulator/generator/core.py`, `movement.py`,
`config.py`, or `cli.py`.

Downstream processing consequences (e.g. a real service-incident record for
a breakdown, or Silver-layer quarantine of malformed events) are explicitly
out of this story's scope — DE-03 only proves the generator can produce the
correct fault conditions; see `docs/BACKLOG.md` DE-06/DE-07.

### Scenario reference table (PROJECT.md section 13)

| # | PROJECT.md §13 scenario | Enable (config) | Enable (CLI flag) | Effect |
|---|---|---|---|---|
| 1 | Bad/malformed data | `[incidents.malformed]` `enabled = true` | `--enable-malformed` | Deterministically round-robins through configured `modes`: null `vehicle_id`, invalid lat/long (999/-999), negative `speed_kmh` (-42), non-parsable `event_time` string, unknown `route_id`, or a missing field (`occupancy` dropped) — applied to the raw JSON payload, since a structurally-valid `TelemetryEvent` cannot hold these values. |
| 2 | Duplicate-event injection | `[incidents.duplicate]` `enabled = true` | `--enable-duplicate` | With probability `probability_per_tick`, delivers one *extra* verbatim (byte-identical) copy of an original payload delivered on an earlier tick, chosen from the last `replay_window` originals, same `event_id` included. A duplicate is an addition, never a replacement: the current tick's own payload is still delivered, and the replay follows immediately after it (see "Duplicate delivery and `--max-events`" below). |
| 3 | Late-event injection | `[incidents.late_event]` `enabled = true` | `--enable-late-event` | With probability `probability_per_tick`, subtracts a uniformly-sampled `[min_skew_seconds, max_skew_seconds]` skew from `event_time` only (`generated_at` unchanged), guaranteed `>= late_threshold_seconds` by config validation. |
| 4 | Out-of-order delivery | `[incidents.out_of_order]` `enabled = true` | `--enable-out-of-order` | A `hold_ticks`-deep reorder buffer, pre-filled before the run starts; each delivery pushes the newest payload in and (with probability `probability_per_tick`) pops a *random* buffered payload instead of the oldest — genuine reordering, with no event lost or duplicated (call `IncidentInjector.flush_pending()` to drain the final `hold_ticks` events at the end of a bounded run). |
| 5 | Route congestion | `[incidents.congestion]` `enabled = true` | `--enable-congestion` | For every event on `route_id`, inside `window` (`[start_tick, start_tick + duration_ticks)`), multiplies `speed_kmh` by `speed_multiplier` (default 0.3) and adds `extra_delay_seconds` (default 240) to `delay_seconds`, recomputing `status` against the generator's own `delayed_status_threshold_seconds`. |
| 6 | Volume spike | `[incidents.volume_spike]` `enabled = true` | `--enable-volume-spike` | Inside `window`, `IncidentInjector.rate_multiplier(tick)` returns `rate_multiplier` (default 6.0) instead of `1.0`, passed to `runner.run(..., rate_multiplier=injector.rate_multiplier)` to scale the pacing interval — the emitted events themselves are untouched, only how fast they are paced. |
| 7 | Vehicle breakdown | `[incidents.breakdown]` `enabled = true` | `--enable-breakdown` | For every event whose `vehicle_id` is in `vehicle_ids`, inside `window`, forces `status = "BREAKDOWN"` and `speed_kmh = 0.0`. Creating a downstream service-incident record is explicitly out of this story's scope. |
| 8 | Recovery | (cross-cutting) | n/a | Scenarios 5-7 are all `window`-bounded (`RecoveryWindow`); once `tick >= start_tick + duration_ticks`, behaviour is byte-identical to a plain no-incident baseline run sharing the same seed/config — demonstrated explicitly in `simulator/tests/test_incidents_recovery.py` and in each of scenarios 5-7's own recovery test. |

### Configuration: `simulator/incidents/config.py` / `default_incidents.toml`

`IncidentsConfig` is a *separate* config surface from `GeneratorConfig`
(DE-02) and `ScaleConfig`/`ValidationThresholds` (DE-01) — every sub-config
below defaults to `enabled = false`, so the bundled
`simulator/incidents/default_incidents.toml` reproduces DE-02's baseline,
incident-free behaviour exactly. All intensity knobs (probabilities, skew
ranges, speed/delay multipliers, rate multiplier, windows, replay/hold
depths) are externalized to that TOML per `PROJECT.md` section 19, not
hardcoded. `IncidentsConfig.seed` seeds a `random.Random` instance *owned
by the incidents layer*, kept entirely separate from `GeneratorConfig.seed`
— enabling/disabling any subset of scenarios never changes the underlying
generator's own event content, only what `IncidentInjector` additionally
does to/with it (verified in `simulator/tests/test_incidents_determinism.py`).

### CLI usage

```
python -m simulator.incidents
python -m simulator.incidents --enable-malformed --enable-duplicate
python -m simulator.incidents --max-events 500 --output events.ndjson
python -m simulator.incidents --incidents-config my_incidents.toml
```

`--enable-<scenario>` (one flag per row of the table above, e.g.
`--enable-congestion`) force-enables that scenario on top of whatever
`--incidents-config` loads, without needing a bespoke TOML file for a quick
demo. Every other flag (`--scale-config`, `--generator-config`, `--seed`,
`--max-events`, `--output`, `--log-file`, `--log-level`) mirrors
`simulator/generator/cli.py`. Output lines are plain JSON dicts (not always
a valid `TelemetryEvent` — see scenario 1), written to stdout/`--output`;
structured JSON-lines incident logs (which scenario fired/recovered, when,
with what parameters) go to stderr/`--log-file` via the
`simulator.incidents` logger, reusing DE-02's `JsonLinesFormatter`
(`simulator/incidents/logging_setup.py`) — never interleaved with the event
stream.

### Duplicate delivery and `--max-events`

A duplicate fire queues its replay inside the injector rather than
returning it from `produce_event` (which always returns the next *original*
payload). `IncidentInjector.delivery_sink(sink)` wraps the output sink so
each queued replay is written straight after the original that triggered
it, inside the same `runner.run()` iteration: it takes no generator tick,
no RNG draw beyond the duplicate decision itself, and no extra pacing slot
(a replay is a same-instant redelivery burst, as at-least-once transports
produce). `runner.run()` itself is unchanged: `max_events` still counts
`produce_event` calls. Consequently, for the incidents CLI:

- `--max-events N` = N generator ticks in the paced main loop = N original
  events (plus the `hold_ticks` pre-filled originals flushed at the end when
  out-of-order is enabled), each delivered exactly once;
- total output lines = originals + duplicate fires;
- the set of delivered `event_id`s is identical with duplicates on or off
  (same seed and config), and each repeated `event_id` appears once per
  logged `"duplicate incident fired"` line naming it as
  `replayed_event_id` (the log also records `delivered_after_event_id`).

Example (`--generator-config simulator/tests/fixtures/fast_generator.toml
--max-events 300`): duplicates off = 300 lines / 300 distinct; with
`--enable-duplicate` = 314 lines / the same 300 distinct + 14 logged fires.
The run-complete log line reports `events_emitted` (main-loop ticks),
`payloads_delivered` and `duplicates_injected`. A caller looping
`produce_event` without `delivery_sink` loses nothing:
`flush_pending()` returns any undelivered replays first, then the
reorder buffer. (Before FU-05 the replay *replaced* the current payload, so
each fire silently dropped one original: 300 lines but 286 distinct.)

### Testing

One deterministic test module per scenario
(`simulator/tests/test_incidents_malformed.py`, `..._duplicate.py`,
`..._late_event.py`, `..._out_of_order.py`, `..._congestion.py`,
`..._volume_spike.py`, `..._breakdown.py`), plus a standalone recovery test
(`test_incidents_recovery.py`), a combined-scenario test
(`test_incidents_combined.py`, including all seven scenarios enabled at
once), a determinism test (`test_incidents_determinism.py`), a config
validation test (`test_incidents_config.py`), a CLI smoke test
(`test_incidents_cli.py`), and a direct unit test for the one
`runner.rate_multiplier` hook added to `simulator/generator/`
(`test_runner_rate_multiplier_hook.py`). Every probabilistic scenario is
exercised deterministically (`probability_per_tick = 1.0` or a fixed seed),
and every time-based assertion uses `FakeClock` — no real `time.sleep`
anywhere in this suite, per `PROJECT.md` section 21.

## IMPLEMENTED - NOT CONNECTED

Everything in this story is local Python with no Fabric/Azure dependency and
no network calls. No Fabric/Azure resource has been created, read or written
by this work.

## DE-04 batch datasets

`simulator/batch/` deterministically generates the remaining `PROJECT.md`
section 7 batch sources on top of DE-01's reference entities
(vehicles/routes/stops/depots, which are *not* regenerated or modified here):
`schedules`, `schedule_stop_times`, `historical_trips`, `historical_telemetry`,
`passenger_journeys`, `service_disruptions`, the optional
`weather_observations` and (DE-10) `vehicle_depot_assignments`, the
effective-dated vehicle-to-depot change history that feeds an SCD Type 2
`dim_vehicle`. It only *produces* datasets; incremental-load/watermark engine
logic is DE-05+ scope.

**Status: IMPLEMENTED - NOT CONNECTED.** Local Python only; no Fabric/Azure
resource has been created, read or written. Output is local files.

### CLI usage

```
python -m simulator.batch --output-dir simulator/_out/batch
python -m simulator.batch --output-dir out --batch-config my_batch.toml --seed 7
python -m simulator.batch --output-dir out --scale-config my_scale.toml --log-file batch.log
```

| Flag | Default | Meaning |
|---|---|---|
| `--output-dir PATH` | `simulator/_out/batch` | Where `<dataset>.jsonl` + `manifest.json` are written. |
| `--batch-config PATH` | `simulator/batch/default_batch.toml` | DE-04 volume/date-range/seed/value-range config. |
| `--scale-config PATH` | DE-01 `default_scale.toml` | Reference-entity scale (routes, vehicles, stops, depots). |
| `--seed INT` | config `batch.seed` | Overrides the batch seed (the DE-01 reference seed is unchanged). |
| `--log-file PATH` / `--log-level LEVEL` | stderr / `INFO` | Structured JSON-lines logs (one object per line). |

Default config (7 service days, 10 routes, 60-minute headway) writes ~54k rows
(~22 MB) in about a second. `simulator/.gitignore` excludes `simulator/_out/`,
so bulk output is never committed.

### Output format: JSON lines

One `<dataset>.jsonl` per dataset, one canonical JSON object per line (sorted
keys, compact separators, UTC timestamps as ISO-8601 `Z`). JSON lines rather
than CSV because it keeps types and explicit `null`s (CSV makes a null
`vehicle_id` indistinguishable from an empty string), matches DE-02's NDJSON
event stream so DE-05 Bronze can read both shapes with one reader, and is read
natively by Spark/Fabric (`spark.read.json`). Every dataset file is always
written (an empty file when, e.g., weather is disabled), so the file set is
stable for downstream loaders.

`manifest.json` records per dataset: file, `row_count`, `sha256`, schema
version(s), primary key, watermark/partition columns, `min_watermark` /
`max_watermark` and partition count, plus the batch/reference seeds and a
SHA-256 of the effective config. It has no wall-clock fields, so it is itself
deterministic and serves as the reconciliation anchor for a later Bronze load
(source rows = manifest `row_count`). Files are written to a temp name and
atomically renamed, so a rerun into the same directory is idempotent.

### Configuration: `simulator/batch/default_batch.toml`

Validated on load by `simulator/batch/config.py` (`load_batch_config()`,
pydantic, unknown keys rejected, inconsistent ranges rejected). No volume or
behavioural number is hardcoded in generator code.

| Section | Drives |
|---|---|
| `[batch]` | `seed`, `start_date`, `service_days` (primary volume driver), fixed `utc_offset_hours`. |
| `[schedules]` | service window, `headway_minutes`, per-segment runtime range, dwell, layover, `publish_lead_days`. |
| `[operations]` | start-delay and per-segment drift ranges, delay floor, DELAYED threshold, arrival speed range, record-settlement lags. |
| `[disruptions]` | `per_day` (exact count), severity weights + delay-impact ranges, disruption types (`effect` = `DELAY` / `BREAKDOWN` / `CANCEL`, weight, duration range). |
| `[passengers]` | journeys-per-trip range (capped at vehicle capacity), card population, settlement lag, fare types (weight + flat fare). |
| `[weather]` | `enabled`, observation interval (must divide 1440), temperature/rain/wind/cloud parameters and condition thresholds. |
| `[fleet_changes]` (DE-10) | `enabled`, `transfers_per_day` (exact count per later service date), `relief_pool_size` (sub-fleet transfers are drawn from; must be >= `transfers_per_day`), `notice_days` (record lead for INITIAL and TRANSFER rows; must be >= `publish_lead_days` whether or not transfers are generated; omitted = `publish_lead_days`). All keys optional: an absent section or `enabled = false` alone = disabled. |

Exact volume formulas (verified by `simulator/tests/test_batch_volume.py`):

- `schedules` = `historical_trips` = routes x `service_days` x
  (floor((service_end - service_start) / headway) + 1)
- `schedule_stop_times` = sum of route stop counts over all scheduled trips
- `historical_telemetry` = sum of `stops_served` over all historical trips
- `passenger_journeys` = per operated trip (>= 2 stops served), a draw in
  [`min_journeys_per_trip`, `max_journeys_per_trip`] capped at vehicle capacity
- `service_disruptions` = `disruptions.per_day` x `service_days`
- `weather_observations` = depots x `service_days` x (1440 / interval), or 0 when disabled
- `vehicle_depot_assignments` = vehicles (one `INITIAL` each) +
  `transfers_per_day` x (`service_days` - 1) when `[fleet_changes] enabled`,
  else vehicles. Default: 50 + 1 x 6 = 56 (also tested in
  `test_batch_vehicle_depot_assignments.py`).

### Determinism contract

Same batch config (incl. `batch.seed`) + same DE-01 scale config ->
byte-identical files, in-process and across two separate OS-process CLI runs
(`simulator/tests/test_batch_determinism.py`). All randomness comes from
independent `random.Random` streams derived from the seed plus a stable key
(`simulator/batch/rng.py`: per route timetable, per service day for
disruptions, per trip for operations/journeys/telemetry, per depot-day for
weather, once per seed for the DE-10 relief pool and per service day for
transfers); string seeds are SHA-512-hashed by Python, so they do not depend on
`PYTHONHASHSEED`. Nothing reads wall-clock time. Because streams are keyed per
day/trip, output is **prefix-stable**: growing `service_days` appends new days
without changing a byte of existing ones - the behaviour a watermark-based
incremental extract expects from a source system.

The committed sample in `simulator/batch/sample/output/` (2 days, 2 routes, 326
rows, ~140 KB; 2 days since DE-10 so it contains `TRANSFER` rows, including one
for a vehicle that operates trips and has a trip-scoped disruption on day 2) is
generated from `sample/sample_batch.toml` + `sample/sample_scale.toml`; a test
regenerates it and requires a byte-identical match. Its day-1 rows are
byte-identical to the pre-DE-10 1-day sample (prefix stability). Downstream
tests must derive expected counts from its `manifest.json`, not hardcode them. Regenerate it after an intentional generator change with:

```
python -m simulator.batch --output-dir simulator/batch/sample/output \
  --batch-config simulator/batch/sample/sample_batch.toml \
  --scale-config simulator/batch/sample/sample_scale.toml
```

### Operational model

- **Timetable.** Each route runs departures from `service_start_time` every
  `headway_minutes`, alternating OUTBOUND (route stop order) and INBOUND
  (reversed). Segment runtimes are sampled once per route segment, so a
  route's timetable is identical every day. Each trip gets a vehicle from the
  route's pool (DE-01 vehicles whose `assigned_route_id` is the route; fallback
  to home-depot vehicles, then the fleet) that is free after its previous trip
  plus `min_layover_minutes`; at default scale no vehicle is double-booked
  *in the timetable* (scheduled times). Actual operated times can still
  overlap — see "Known limitations (DE-04)".
- **Disruptions.** `DELAY` types hit a route for a time window: every trip on
  that route departing inside it gets the severity's delay impact added.
  `BREAKDOWN` types stop a trip after a drawn stop (trip `PARTIAL`, last
  telemetry snapshot `status = BREAKDOWN`), and the vehicle's later trips
  departing before the disruption ends are `CANCELLED`. `CANCEL` types cancel
  the trip and withdraw the vehicle for the window. At most one trip-scoped
  disruption per vehicle per day. Every non-`COMPLETED` trip references the
  disruption that caused it.
- **Operations.** Actual stop times = scheduled + delay, where delay = start
  delay + disruption impact, then per-segment drift, floored at
  `min_delay_seconds` (config validation guarantees strictly increasing stop
  times).
- **Fleet changes (DE-10).** See `vehicle_depot_assignments` below. A
  transfer changes only the depot a vehicle's trips are *attributed* to from
  its effective date (`schedules`, `historical_trips` and trip-scoped
  `service_disruptions` `depot_id`); it never changes which vehicle operates
  a trip, and transfers use their own RNG streams, so no other value in any
  dataset moves.
- **Passengers.** Journeys board/alight at served stops of the trip
  (boarding before alighting). Telemetry `occupancy` at stop *k* is exactly the
  number of journeys boarded before *k* and alighting at or after *k*, so
  occupancy, `passenger_boardings` and `peak_occupancy` reconcile by
  construction and never exceed vehicle capacity.

### Common columns, watermark and partition

Every dataset row has `schema_version`, `updated_at` (aware UTC; the
**watermark column**: when the record became final in the source system) and
`event_date` (the **partition column**: local operating/service date for
trip-derived datasets, local observation date for weather, local effective
service date for `vehicle_depot_assignments`). A later
incremental load selects `updated_at > last_watermark` and advances the
watermark only after success (PROJECT.md section 11). `updated_at` semantics:
timetables are published `publish_lead_days` before service (midnight local);
trips settle `trip_record_settlement_minutes` after they end; journeys settle
`settlement_lag_minutes` after alighting; disruptions close
`disruption_record_settlement_minutes` after resolution; telemetry and
weather are final when observed (`updated_at` = `event_time` / `observed_at`);
depot assignments are recorded `notice_days` before they take effect.

`schema_version` is `BATCH_SCHEMA_VERSION` (`"1.0.0"`,
`simulator/batch/schemas.py`) for every dataset except `historical_telemetry`,
which extends DE-01's `TelemetryEvent` and therefore carries the telemetry
`CURRENT_SCHEMA_VERSION`. Same bump policy as the telemetry schema above.

All timestamps are UTC. Local service times are converted with a fixed
`utc_offset_hours` (AEST +10, no daylight saving) - see limitations.

### Dataset catalogue

The machine-readable source of truth is `DATASET_CATALOGUE` in
`simulator/batch/schemas.py` (primary keys, watermark/partition columns,
foreign keys, downstream targets); `simulator/tests/test_batch_schemas.py`
fails if a model column is missing from the tables below. "Supports" names
the eventual Gold / Warehouse dimension or fact (PROJECT.md section 10) each
dataset feeds. Lakehouse vs Warehouse placement follows ADR 0002: all of these
land raw in Lakehouse Bronze; curated facts/dimensions go to the Warehouse.

### `schedules`

Timetable header, one row per scheduled trip. **PK** `trip_id`. **Watermark**
`updated_at` (publication time). **Partition** `event_date` (service date).
**Supports** `fact_trip` (planned measures), `dim_route`, `dim_date`, `dim_time`.

| Column | Type | Notes |
|---|---|---|
| `schema_version` | str | `BATCH_SCHEMA_VERSION`. |
| `trip_id` | str | PK. `TRIP-<route_id>-<yyyymmdd>-<seq>`. Distinct namespace from DE-02's runtime `trip_id` (see limitations). |
| `route_id` | str | FK -> DE-01 `routes.route_id`. |
| `direction` | enum | `OUTBOUND` / `INBOUND`. |
| `trip_sequence` | int | 1-based order of the trip on its route that day. |
| `vehicle_id` | str | FK -> DE-01 `vehicles.vehicle_id` (planned vehicle). |
| `depot_id` | str | FK -> DE-01 `depots.depot_id`; the vehicle's effective depot on `event_date` (`vehicle_depot_assignments`; DE-01 home depot until transferred). |
| `origin_stop_id` | str | FK -> `stops.stop_id`; first stop for this direction. |
| `destination_stop_id` | str | FK -> `stops.stop_id`; last stop for this direction. |
| `stop_count` | int | Number of stops (= `schedule_stop_times` rows for the trip). |
| `scheduled_departure` | UTC datetime | Departure from origin. |
| `scheduled_arrival` | UTC datetime | Arrival at destination. |
| `event_date` | date | Partition: local service date. |
| `updated_at` | UTC datetime | Watermark: timetable publication time. |

### `schedule_stop_times`

Stop-level timetable, one row per (trip, stop). **PK** (`trip_id`,
`stop_sequence`). **Watermark** `updated_at`. **Partition** `event_date`.
**Supports** `fact_route_performance` (stop-level punctuality), `dim_stop`.

| Column | Type | Notes |
|---|---|---|
| `schema_version` | str | `BATCH_SCHEMA_VERSION`. |
| `trip_id` | str | FK -> `schedules.trip_id`. |
| `stop_sequence` | int | 1-based position on the trip. |
| `stop_id` | str | FK -> DE-01 `stops.stop_id`; equals the route's stop list (reversed for INBOUND). |
| `scheduled_arrival` | UTC datetime | Equals departure at the origin. |
| `scheduled_departure` | UTC datetime | Arrival + dwell (no dwell at origin/terminus). |
| `event_date` | date | Partition: local service date. |
| `updated_at` | UTC datetime | Watermark: timetable publication time. |

### `historical_trips`

Operated outcome of every scheduled trip (1:1 with `schedules`). **PK**
`trip_id`. **Watermark** `updated_at` (record settlement). **Partition**
`event_date`. **Supports** `fact_trip`, `fact_route_performance`,
`dim_vehicle`, `dim_depot`.

| Column | Type | Notes |
|---|---|---|
| `schema_version` | str | `BATCH_SCHEMA_VERSION`. |
| `trip_id` | str | PK; FK -> `schedules.trip_id`. |
| `route_id` | str | FK -> `routes.route_id` (matches schedule). |
| `vehicle_id` | str | FK -> `vehicles.vehicle_id` (matches schedule). |
| `depot_id` | str | FK -> `depots.depot_id` (matches schedule: vehicle's effective depot on `event_date`). |
| `direction` | enum | Matches schedule. |
| `trip_status` | enum | `COMPLETED` / `PARTIAL` (breakdown) / `CANCELLED`. |
| `scheduled_departure` | UTC datetime | Copied from schedule. |
| `scheduled_arrival` | UTC datetime | Copied from schedule. |
| `actual_departure` | UTC datetime, nullable | Null when CANCELLED. |
| `actual_arrival` | UTC datetime, nullable | Null unless COMPLETED. |
| `departure_delay_seconds` | int, nullable | Signed; null when CANCELLED. |
| `arrival_delay_seconds` | int, nullable | Signed; null unless COMPLETED. |
| `stops_scheduled` | int | = schedule `stop_count`. |
| `stops_served` | int | = number of telemetry snapshots for the trip. |
| `passenger_boardings` | int | = number of `passenger_journeys` rows for the trip. |
| `peak_occupancy` | int | = max telemetry `occupancy` for the trip. |
| `disruption_delay_seconds` | int | Sum of DELAY-disruption impacts applied. |
| `disruption_id` | str, nullable | FK -> `service_disruptions.disruption_id`; the primary cause. Always set for PARTIAL/CANCELLED. |
| `event_date` | date | Partition: local service date. |
| `updated_at` | UTC datetime | Watermark: trip end (or scheduled arrival if cancelled) + settlement lag. |

### `historical_telemetry`

Stop-arrival AVL snapshots: one per stop actually served. A strict superset of
DE-01's `TelemetryEvent` (same fields, bounds and `schema_version`), so the
DE-06 Silver validation rules apply to history unchanged; every row passes the
default `validate_against_thresholds`. **PK** `event_id`. **Watermark**
`updated_at` (= `event_time`). **Partition** `event_date`. **Supports**
`fact_vehicle_telemetry`, `fact_route_performance`.

| Column | Type | Notes |
|---|---|---|
| `event_id` | str | PK. `HEVT-<route>-<yyyymmdd>-<trip seq>-<stop seq>` (DE-02 live events use `EVT-`). |
| `schema_version` | str | Telemetry `CURRENT_SCHEMA_VERSION`. |
| `generated_at` | UTC datetime | = `event_time` (historical, no transport lag). |
| `event_time` | UTC datetime | Actual arrival at the stop. |
| `vehicle_id` | str | FK -> `vehicles.vehicle_id`. |
| `route_id` | str | FK -> `routes.route_id`. |
| `trip_id` | str | FK -> `schedules.trip_id`. |
| `latitude` | float | Exactly the stop's DE-01 latitude. |
| `longitude` | float | Exactly the stop's DE-01 longitude. |
| `speed_kmh` | float | Arrival speed in the configured arrival range. |
| `heading` | float | Bearing to the next stop (from the previous stop at the terminus), 0.1 deg. |
| `occupancy` | int | Passengers on board on arrival, derived from journeys. |
| `delay_seconds` | int | Actual minus scheduled arrival at this stop. |
| `status` | enum | `IN_SERVICE`, `DELAYED` (>= threshold), `BREAKDOWN` (last stop of a PARTIAL trip). |
| `stop_id` | str | FK -> `stops.stop_id`; matches `schedule_stop_times`. |
| `stop_sequence` | int | Position on the trip. |
| `snapshot_type` | str | `STOP_ARRIVAL`. |
| `event_date` | date | Partition: local service date. |
| `updated_at` | UTC datetime | Watermark (= `event_time`). |

### `passenger_journeys`

One tap-on/tap-off journey on one operated trip. **PK** `journey_id`.
**Watermark** `updated_at` (tap settlement). **Partition** `event_date`.
**Supports** `fact_passenger_journey`, `dim_stop`, `dim_date`, `dim_time`.

| Column | Type | Notes |
|---|---|---|
| `schema_version` | str | `BATCH_SCHEMA_VERSION`. |
| `journey_id` | str | PK. `JRN-<route>-<yyyymmdd>-<trip seq>-<n>`. |
| `card_id` | str | Synthetic pseudonymous token `CARD-nnnnnn` from a configured population. Not PII. |
| `trip_id` | str | FK -> `schedules.trip_id`; never a CANCELLED trip. |
| `route_id` | str | FK -> `routes.route_id` (matches trip). |
| `vehicle_id` | str | FK -> `vehicles.vehicle_id` (matches trip). |
| `boarding_stop_id` | str | FK -> `stops.stop_id`; on the trip at `boarding_stop_sequence`. |
| `boarding_stop_sequence` | int | < `alighting_stop_sequence`. |
| `alighting_stop_id` | str | FK -> `stops.stop_id`; on the trip at `alighting_stop_sequence`. |
| `alighting_stop_sequence` | int | <= trip `stops_served`. |
| `boarding_time` | UTC datetime | Actual departure from the boarding stop. |
| `alighting_time` | UTC datetime | Actual arrival at the alighting stop. |
| `fare_type` | str | Configured fare product (e.g. ADULT, CONCESSION). |
| `fare_aud` | float | Configured flat fare. |
| `event_date` | date | Partition: local service date. |
| `updated_at` | UTC datetime | Watermark: alighting + settlement lag. |

### `service_disruptions`

Operational incident log. Field set chosen to populate `fact_service_incident`
(natural key; date/time keys; route/depot and, when trip-scoped,
vehicle/trip/stop dimension keys; classification attributes; duration and
impact measures) - asserted by
`test_service_disruptions_can_populate_fact_service_incident`. **PK**
`disruption_id`. **Watermark** `updated_at` (log closure). **Partition**
`event_date`. **Supports** `fact_service_incident`, `fact_route_performance`
(reliability).

| Column | Type | Notes |
|---|---|---|
| `schema_version` | str | `BATCH_SCHEMA_VERSION`. |
| `disruption_id` | str | PK. `DIS-<yyyymmdd>-<n>`. |
| `disruption_type` | str | Configured type name (e.g. `VEHICLE_BREAKDOWN`, `ROUTE_CONGESTION`). |
| `effect` | enum | `DELAY` / `BREAKDOWN` / `CANCEL`. |
| `scope` | enum | `ROUTE` (DELAY) or `TRIP` (BREAKDOWN/CANCEL). |
| `severity` | enum | `LOW` / `MEDIUM` / `HIGH` / `CRITICAL`. |
| `status` | enum | `RESOLVED` (historical log). |
| `route_id` | str | FK -> `routes.route_id`. |
| `depot_id` | str | FK -> `depots.depot_id`; route home depot (ROUTE) or the vehicle's effective depot on `event_date` (TRIP). |
| `vehicle_id` | str, nullable | FK -> `vehicles.vehicle_id`; set when TRIP-scoped. |
| `trip_id` | str, nullable | FK -> `schedules.trip_id`; set when TRIP-scoped. |
| `stop_id` | str, nullable | FK -> `stops.stop_id`; breakdown location / cancelled trip origin. |
| `start_time` | UTC datetime | Breakdown: actual arrival at the failure stop. |
| `end_time` | UTC datetime | Resolution time. |
| `duration_minutes` | int | `end_time - start_time`. |
| `estimated_delay_seconds` | int, nullable | DELAY impact added per affected trip; null otherwise. |
| `affected_trip_count` | int | Trips delayed (ROUTE) or terminated/cancelled (TRIP); reconciles with `historical_trips` by route/time window (not by `disruption_id` FK when disruptions overlap). |
| `description` | str | Human-readable synthetic summary. |
| `reported_by` | str | `SYNTHETIC_OPS_CONTROL`. |
| `event_date` | date | Partition: local service date. |
| `updated_at` | UTC datetime | Watermark: `end_time` + settlement lag. |

### `weather_observations`

Optional (`[weather] enabled`). One synthetic station per DE-01 depot.
**PK** `observation_id`. **Watermark** `updated_at` (= `observed_at`).
**Partition** `event_date`. **Supports** `fact_route_performance` (optional
weather context), `dim_date`, `dim_time`.

| Column | Type | Notes |
|---|---|---|
| `schema_version` | str | `BATCH_SCHEMA_VERSION`. |
| `observation_id` | str | PK. `WX-<depot_id>-<yyyymmddHHMM local>`. |
| `station_id` | str | `WX-<depot_id>`. |
| `depot_id` | str | FK -> `depots.depot_id`. |
| `latitude` | float | Depot latitude. |
| `longitude` | float | Depot longitude. |
| `observed_at` | UTC datetime | Observation time. |
| `temperature_c` | float | Diurnal curve + bounded noise. |
| `rainfall_mm` | float | 0 unless raining. |
| `wind_speed_kmh` | float | Uniform in configured range. |
| `condition` | enum | `CLEAR` / `CLOUDY` / `RAIN` / `HEAVY_RAIN` / `STORM`, derived from configured thresholds. |
| `event_date` | date | Partition: local observation date. |
| `updated_at` | UTC datetime | Watermark (= `observed_at`). |

### `vehicle_depot_assignments`

DE-10. Effective-dated vehicle-to-depot assignment change log, the SCD Type 2
source for `dim_vehicle` (depot as the Type 2 attribute). **PK**
`assignment_id`. **Watermark** `updated_at` (when the assignment was recorded
in the source fleet system). **Partition** `event_date` (local effective
service date). **FKs** `vehicle_id` -> `vehicles`, `depot_id` /
`previous_depot_id` -> `depots`. **Supports** `dim_vehicle` (SCD2 depot
history), `dim_depot`.

Transfer model (`simulator/batch/fleet.py`, config `[fleet_changes]`):

- Exactly one `INITIAL` row per DE-01 vehicle: its DE-01 `home_depot_id`,
  effective on `start_date`, `change_sequence` 1.
- When enabled, a relief pool of `relief_pool_size` vehicles is drawn once
  per seed from the fleet. On each later service date (`start_date + 1` ..
  `start_date + service_days - 1`) exactly `transfers_per_day` distinct pool
  vehicles move to a depot drawn uniformly from the depots *other than* their
  current one. So a transfer always changes depot, a vehicle never has two
  changes on one effective date, and A -> B -> A histories can occur. With
  more transfers than pool vehicles, some vehicle transfers at least twice
  (pigeonhole): the default (1/day, pool 3, 7 days) gives 6 transfers with
  one vehicle moving 3 times and another twice.
- A row's depot applies from `effective_from` (local midnight of
  `event_date`, a service-date boundary) until the same vehicle's next row;
  every trip / schedule / trip-scoped disruption with that vehicle on a
  service date is attributed to the depot effective on its `event_date`.
- `updated_at` = `effective_from` - `notice_days` (omitted =
  `publish_lead_days`). Validation requires `notice_days >= publish_lead_days`
  for every row, INITIAL included (so even with transfers off), so a
  published timetable never attributes a depot that was not yet recorded. Per
  vehicle, `updated_at` increases with the effective date: the generated log
  has no backdated changes (DE-13 still needs a backdating policy for real
  sources).
- Infeasible configs fail loudly (`InfeasibleFleetChangeConfigError`): pool
  larger than the fleet, or transfers with fewer than 2 depots.
- Disabled (or `[fleet_changes]` absent): `INITIAL` rows only, and every
  pre-DE-10 dataset file is byte-identical to pre-DE-10 output for the same
  seed and configs (pinned SHA-256 test against `main` at 295f7db). Only
  `manifest.json` differs: it gains this dataset and its `config_sha256`
  covers the new section.

| Column | Type | Notes |
|---|---|---|
| `schema_version` | str | `BATCH_SCHEMA_VERSION`. |
| `assignment_id` | str | PK. `VDA-<vehicle_id>-<yyyymmdd effective date>`; unique because a vehicle has at most one change per date. |
| `vehicle_id` | str | FK -> DE-01 `vehicles.vehicle_id`. |
| `depot_id` | str | FK -> DE-01 `depots.depot_id`; depot from `effective_from`. |
| `previous_depot_id` | str, nullable | FK -> `depots.depot_id`; null for `INITIAL`, otherwise the vehicle's previous row's depot (never equal to `depot_id`). |
| `change_type` | enum | `INITIAL` / `TRANSFER`. |
| `change_sequence` | int | 1 for `INITIAL`, +1 per `TRANSFER` of the vehicle (version number). |
| `effective_from` | UTC datetime | Local midnight of `event_date` in UTC. |
| `event_date` | date | Partition: local effective service date. |
| `updated_at` | UTC datetime | Watermark: `effective_from` - `notice_days`. |

### DE-04 tests

`pytest simulator -q` runs, among the rest of the suite:

- `test_batch_config.py` - default/sample config load; inconsistent configs rejected.
- `test_batch_volume.py` - exact count formulas at default and overridden configs; DE-01 rescaling; infeasible config fails loudly.
- `test_batch_referential_integrity.py` - every catalogued FK resolves (default, delay-only and committed sample data); PKs unique; trips/stop-times/telemetry/journeys/disruptions mutually consistent.
- `test_batch_consistency.py` - aggregates reconcile with detail rows; occupancy derivation; strictly increasing times; no vehicle double-booking; each disruption effect; watermark semantics.
- `test_batch_schemas.py` - per-dataset schema round trip, required/unknown-field negatives, TelemetryEvent conformance, `fact_service_incident` sufficiency, README catalogue coverage.
- `test_batch_determinism.py` - in-process and cross-process byte identity, seed sensitivity, prefix stability, committed-sample reproducibility, idempotent rerun, manifest reconciliation.
- `test_batch_vehicle_depot_assignments.py` (DE-10) - catalogue entry; INITIAL/TRANSFER invariants; volume formula; 100% depot attribution of schedules/trips/trip-scoped disruptions vs an independent point-in-time lookup (default, committed sample, heavy-transfer config, with non-vacuity checks); transfers change only `depot_id`; disabled -> byte-identical to pinned pre-DE-10 digests; config validation and infeasible configs.

### Known limitations (DE-04)

- **Timetable runtimes are not distance-derived.** DE-01 scatters stops
  uniformly across the Sydney bounding box, so consecutive stops are tens of
  km apart; a distance-based timetable would produce multi-hour trips.
  Segment runtimes are therefore sampled from a configured range, and
  historical telemetry is recorded at stop arrival (exact stop coordinates)
  rather than interpolated between stops, so position/speed stay physically
  plausible and pass section 12 thresholds.
- **Fixed UTC offset** (`utc_offset_hours`, default +10) - no daylight-saving
  transitions, to keep output deterministic and stdlib-only.
- **Trip-id namespaces differ from DE-02.** DE-02's live generator emits
  `TRIP-<route>-<lap>` trip ids that are not linked to this timetable; joining
  live telemetry to scheduled trips needs a later alignment story.
- **Depot history only (DE-10).** `vehicle_depot_assignments` provides the
  vehicle-to-depot change history for `dim_vehicle` SCD2; the SCD2 logic
  itself is DE-13. A transfer does not change the vehicle's DE-01
  `assigned_route_id` or trip vehicle selection, so a transferred vehicle
  keeps serving its route and route-scoped disruptions keep the route's home
  depot. Transfers do not check depot bay `capacity`, and the DE-01 `vehicles`
  reference entity still shows the original `home_depot_id`.
- **Planned vehicle = operated vehicle.** Cancelled trips are not re-covered
  by a spare vehicle.
- **Delay does not propagate to a vehicle's next trip.** A `DELAY`
  disruption pushes trip N's actual times later, but the same vehicle's trip
  N+1 keeps its own (undisrupted) actual times, so in `historical_trips` /
  `historical_telemetry` a vehicle can appear to operate two trips at once
  (QA found 4 such overlaps at default config, up to ~30 minutes, all caused
  by a disruption). No section 12 Silver rule quarantines these rows, but
  vehicle-utilisation metrics and `fact_vehicle_telemetry` built from actual
  times would be skewed. Tracked as a follow-up in `docs/BACKLOG.md`.
- **`affected_trip_count` reconciles by route/time window, not by FK.** Each
  trip stores only its primary `disruption_id`, so when disruptions overlap,
  counting trips by `disruption_id` can differ from `affected_trip_count`. A
  trip-disruption bridge table is a possible later enhancement.
- Weather is independent per observation (no persistence/fronts) and not
  causally linked to `SEVERE_WEATHER` disruptions.

## DE-05: Local Bronze Layer Ingestion Logic

**Status: IMPLEMENTED - NOT CONNECTED.** `simulator/bronze/` is local Python
writing local files. No Fabric Lakehouse, OneLake, Eventstream or any other
Azure/Fabric resource has been created, read or written; nothing here is
evidence of Fabric execution. See "Mapping to a future Fabric Bronze
notebook" below for how it is meant to carry over. DE-09 (failed-attempt
audit trail and path-free manifests, BACKLOG FU-03/FU-04) extends this
section and is likewise **IMPLEMENTED - NOT CONNECTED**.

Bronze (PROJECT.md section 8, ADR-0002) keeps the source representation with
minimum transformation plus ingestion metadata, so any later layer can be
replayed and any record traced to the exact input line that produced it. It
deliberately does **not** validate business rules, enforce the schema,
deduplicate or quarantine - that is Silver (DE-06). A null `vehicle_id`,
latitude 999, `"not-a-timestamp"`, a missing `occupancy`, an unknown route, a
duplicate, a late or an out-of-order event all land in Bronze exactly as
received.

### CLI usage

```
# streaming-shaped input (DE-02 / DE-03 NDJSON): files, or '-' for stdin
python -m simulator.bronze stream --input events.ndjson --run-id r1
python -m simulator.incidents --enable-malformed --enable-duplicate --max-events 500 \
  | python -m simulator.bronze stream --input - --run-id r2

# batch-shaped input (a DE-04 output directory)
python -m simulator.batch --output-dir simulator/_out/batch
python -m simulator.bronze batch --input-dir simulator/_out/batch --run-id b1
```

| Flag | Default | Meaning |
|---|---|---|
| `--input PATH` (stream, repeatable) | required | NDJSON file, or `-` for stdin. Ingested in the order given. |
| `--input-dir PATH` (batch) | config `batch_input_dir` (`simulator/_out/batch`) | DE-04 dataset directory. |
| `--run-id ID` | generated (`<UTC yyyymmddTHHMMSSZ>-<8 hex>`) | Run identifier, `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`. Pass one for reproducible/idempotent reruns. |
| `--output-dir PATH` | config `output_dir` (`simulator/_out/bronze`, gitignored) | Bronze store root. |
| `--source LABEL` (stream) | config `stream.source` | Override the stream `source` label. |
| `--strict` | off | `parse_error_policy = "fail"` (see below). |
| `--allow-replay` | off | Permit re-ingesting input a committed run already ingested; flagged in the manifest. |
| `--no-verify-source-manifest` (batch) | off | Record, but do not fail on, a mismatch with DE-04 `manifest.json`. |
| `--fixed-ingestion-time ISO` | wall clock | Inject a fixed clock (must carry an offset, e.g. `...Z`) for reproducible output. |
| `--bronze-config PATH` | `simulator/bronze/default_bronze.toml` | Alternate config. |
| `--log-file PATH` / `--log-level LEVEL` | stderr / `INFO` | Structured JSON-lines logs (same formatter as DE-02). |

Stdout gets one JSON summary line (`status`, `run_id`, `manifest`,
`records_written`, `parse_error_records`, `replay_of`; or `status: FAILED`,
`error_type`, `error`). Exit code 0 = committed or already committed with
identical input; 1 = refused/failed (nothing committed). The library API is
`simulator.bronze.ingest_stream(...)` / `ingest_batch(...)` (both accept a
`clock`), and `BronzeStore(root).read_table(table)` reads committed records.

### Store layout

```
<output_dir>/
  <table>/run_id=<run_id>/part-00000.jsonl   one data file per table per run
  _runs/<run_id>.json                         run manifest: audit record AND commit marker
  _staging/<run_id>/...                       in-flight run, never read, removed on finish
```

Stream input lands in one table (`stream.table`, default `telemetry_stream`);
batch input lands in one table per DE-04 dataset (`historical_trips`,
`service_disruptions`, ...). **A run is committed iff
`_runs/<run_id>.json` has `status: "COMMITTED"`.** Data is written to staging,
each table directory is moved into place with an atomic rename, and the
manifest is written last (temp file + atomic rename). `BronzeStore.read_table`
(and Silver) read only partitions listed by committed manifests, so a crash
at any point leaves nothing visible; the next attempt with the same run_id
removes the orphaned staging/partition directories first and records them in
`removed_uncommitted_paths`.

### Bronze record contract

One input line -> exactly one Bronze record (one output line), except
whitespace-only lines, which carry no payload and are counted
(`blank_lines_skipped`) rather than stored.

**Stored record** (the line is a UTF-8 JSON object): the output line is the
input object's *own bytes* (JSON whitespace trimmed at the ends) with the
metadata spliced in before the closing brace. So every original field is
preserved byte-for-byte - number formatting (`1.10`, `1e5`, big integers),
string escapes, nulls, nesting and key order - and parsing the record and
dropping the metadata keys gives back exactly the input object. Appended
fields, in order:

| Field | Type | Meaning |
|---|---|---|
| `ingestion_time` | str, UTC ISO-8601 `...Z`, microseconds | When the run ingested the record. One value per run, taken from the injectable clock at run start (mirrors Spark's `current_timestamp()`, evaluated once per query/micro-batch). This is PROJECT.md section 6's `ingestion_time`; `processing_time` is left to Silver (DE-06). |
| `source` | str | Logical source: `stream.source` (default `simulator.telemetry_stream`) or `simulator.batch/<dataset>` (`batch.record_source_template`). |
| `run_id` | str | The run that ingested the record. |
| `_bronze_source_file` | str | Lineage: input file basename (`<stdin>` for stdin). Never a local absolute path. |
| `_bronze_line_number` | int | Lineage: 1-based physical line in that file. With the file name it pinpoints the exact input line for troubleshooting/replay, and it preserves **arrival order** - which Silver needs for late/out-of-order reasoning, since `event_time`/`generated_at` deliberately cannot be trusted for that. |

The two lineage fields are namespaced `_bronze_*`; any input key equal to a
Bronze metadata field or starting with `_bronze_` is a collision (see below),
so metadata can never overwrite or shadow an original field.

**Parse-error record** (anything that cannot be stored as-is): the same five
fields plus `_bronze_parse_error` (code), `_bronze_parse_error_detail`
(parser message), `_bronze_raw_line` (the raw line, verbatim) and
`_bronze_raw_encoding` (`utf-8`, or `base64` when the bytes are not valid
UTF-8 - so the original bytes are always recoverable, see
`simulator.bronze.records.raw_line_bytes`). Codes:

| Code | Cause |
|---|---|
| `invalid_json` | Truncated/unparseable text, trailing garbage, or non-standard `NaN`/`Infinity`. |
| `invalid_utf8` | Line bytes are not UTF-8. |
| `not_a_json_object` | Valid JSON but an array/string/number/`null`. |
| `duplicate_json_key` | An object (at any depth) repeats a key - readers would silently keep only one value. |
| `reserved_field_collision` | The input already has `ingestion_time`/`source`/`run_id`/a `_bronze_*` key. |

### Parse-error policy

`parse_error_policy` (config) / `--strict`:

- **`preserve` (default)** - store the marked parse-error record and carry on.
  Chosen because PROJECT.md section 12 requires that invalid events never
  silently disappear *and* that quarantine preserves the raw payload, source,
  ingestion time and run ID: a preserved record gives Silver exactly that, in
  the same table, under the same run. Failing a whole stream because one
  message was truncated would stall every good event behind it, and the
  standard streaming answer is a dead-letter record, not a halt.
- **`fail`** - the whole input is still scanned (so the audit is complete),
  then the run fails loudly with `MalformedInputError` naming the first
  offending `file:line: code` locations; nothing is committed and a `FAILED`
  manifest records the counts and the error. Use it for sources that must be
  all-or-nothing.

Either way nothing is silently dropped. A final line without a trailing
newline (a typical truncated write) is additionally flagged per file as
`unterminated_final_line: true` even when it happens to parse.

### Idempotency and replay

| Situation | Behaviour |
|---|---|
| Same `run_id`, same input, run committed | No-op: returns `ALREADY_COMMITTED`, writes nothing - output stays byte-identical even under a different clock. |
| Same `run_id`, different input, run committed | Refused: `RunIdConflictError`. A committed run is never overwritten. |
| Same `run_id`, previous attempt `FAILED` or crashed | Retried from scratch (orphans removed first, listed in `removed_uncommitted_paths`). Every earlier `FAILED` attempt is kept in `previous_attempts` (DE-09, see below). |
| New `run_id`, an input file whose exact content (sha256) a committed run already ingested into the same table | Refused: `DuplicateInputError` (a `FAILED` manifest records the refusal). Renaming the file does not help - detection is by content. |
| ...the same, with `--allow-replay` | Committed and flagged: manifest `replay_of` lists the prior run ids. The copies stay distinguishable by `run_id`/`ingestion_time`; Silver's `event_id`/primary-key dedup removes them downstream. |
| The same content given twice within one run (e.g. a file and its copy) | Refused: `DuplicateInputError`; with `--allow-replay` committed and flagged in `duplicate_input_files`. |
| New `run_id`, new input | Appended as a new run partition. |

"Same input" means the same ordered list of (table, source, file sha256) -
the manifest's `input_fingerprint`. Files with no records (e.g. DE-04's empty
weather file when weather is disabled) never count as replays, because they
cannot cause double-ingestion. Determinism: with an injected clock, the same
input + run_id produce byte-identical data files and manifest in any store,
in-process or across separate processes (`--fixed-ingestion-time`).

### Run manifest and accounting identity

`_runs/<run_id>.json` (sorted keys; every time field comes from the injected clock) records:
`bronze_manifest_version`, `run_id`, `status` (`COMMITTED`/`FAILED`), `mode`, run-level `source`,
`ingestion_time`, `started_at`, `ended_at`, `duration_seconds`,
`parse_error_policy`, `allow_replay`, `replay_of`, `duplicate_input_files`, `input_fingerprint`,
`previous_attempts`, `input_path_policy`,
`input_files` (per file: name, path (see path policy), table, source, sha256, size and
all counters, `parse_errors_by_code`, `unterminated_final_line`, and for batch
`source_manifest_check`), `tables` (per table: committed path, sha256 of the
data file, counters), `totals`, `ignored_files`, `removed_uncommitted_paths`,
`source_reconciliation` and `error`.

**Failed-attempt audit trail (DE-09, BACKLOG FU-03).** Retrying a `FAILED`
run_id no longer loses the earlier error: the new attempt's manifest carries
`previous_attempts`, one entry per earlier `FAILED` attempt, oldest first -
the same list Silver (DE-06) and Gold (DE-07) keep, with the keys they record
(`started_at`, `ended_at`, `input_fingerprint`, `error`) plus `status`,
`duration_seconds` and `totals` (the attempt's counters; `null` when it failed
before counting finished, e.g. a refused duplicate input). Two failures then a
success leave exactly two entries; an `ALREADY_COMMITTED` no-op writes nothing
and so adds nothing; a first-time commit has `[]`. A *crashed* attempt (killed
before it could write a manifest) cannot be listed - its leftovers show up in
`removed_uncommitted_paths` of the next attempt instead.

**Path policy (DE-09, BACKLOG FU-04).** A manifest never records a local
absolute path, so it can be published as RECORDED evidence as-is:

| Field | Recorded as |
|---|---|
| `input_files[].path` | Relative to the file's input directory, i.e. the **basename** (`-` for stdin), whatever form the path was given in (absolute or relative). Declared in the manifest as `input_path_policy: "basename"`. Input directories are not recorded; inputs are identified by name + `sha256` + `size_bytes`. |
| `tables[].path`, `removed_uncommitted_paths` | Relative to the Bronze store root (unchanged). The store root itself is not recorded. |
| `source_reconciliation.source_manifest` | The DE-04 manifest's file name (unchanged). |
| `error.message` (incl. inside `previous_attempts`) | Exception text with the store root replaced by `<bronze_root>`, input directories by `<input_dir>` and the home directory by `~`. Each root is matched as given (including a relative spelling, since OS errors quote paths exactly as passed), absolute and symlink-resolved, only at path-component boundaries. The exception raised to the caller keeps the full text. |

Consequently the same input content + run_id + fixed clock gives a
byte-identical manifest wherever the input lives (tested across two CLI
processes reading from different absolute directories). The structured log
line "bronze run committed" also records the manifest store-relatively
(`_runs/<run_id>.json`); the CLI's stdout summary still prints the manifest's
location as a local path, because it is an operator pointer, not a persisted
artifact.

**Manifest version and compatibility.** `bronze_manifest_version` is `1.1.0`
since DE-09 (adds `previous_attempts` and `input_path_policy`; `path` is now a
basename). DE-09 only *adds* fields: `1.0.0` manifests written before it
(no `previous_attempts`, absolute `input_files[].path`) are still read by
`BronzeStore`, the same-run_id no-op / conflict checks, the replay guard, a
retry over a legacy `FAILED` manifest (whose attempt becomes the first
`previous_attempts` entry), and by Silver and Gold - fixture-tested in
`test_bronze_legacy_manifest.py`. Existing legacy manifests are not rewritten.

Accounting identity, checked per input file, per table and in total before
commit (and the line count of each staged data file is re-read and compared):

```
lines_read      == records_read + blank_lines_skipped
records_read    == records_written
records_written == json_object_records + parse_error_records
```

i.e. every non-blank input line is written exactly once; parse errors are
*part of* `records_written` (preserve policy). Under the fail policy a run
with `parse_error_records > 0` is not committed at all. This is the Bronze
leg of the PROJECT.md section 15 identity: DE-06 Silver will continue it as
`Bronze records_written = valid loaded + quarantined (incl. parse errors) +
intentionally excluded`.

### Batch input specifics

- With `manifest.json` present, exactly the datasets it lists are ingested,
  each into its own table, and each file is checked against the DE-04 manifest
  (`sha256` and `row_count == records_read`). With `verify_source_manifest =
  true` (default) any mismatch - e.g. a truncated transfer - fails the run
  before commit (`SourceManifestError`); with `--no-verify-source-manifest` the
  mismatch is recorded (`source_reconciliation.status = "MISMATCH"`) and the
  data is ingested under the parse-error policy. Without a manifest, every
  `*.jsonl` file is ingested (table = file stem) and the status is
  `NO_SOURCE_MANIFEST`. Other files in the directory are listed in
  `ignored_files` and logged, never silently skipped.
- **FU-02:** no watermark predicate or cross-dataset ordering is applied - each
  file is ingested in full - so a `historical_trips` row whose `updated_at` is
  earlier than that of the disruption it references (the committed sample has
  two) is stored like any other. `event_date` is stored byte-for-byte as
  provided (the local service date) and never re-derived from a UTC
  timestamp; the sample has 16 telemetry rows where it differs from the UTC
  date of `updated_at`, and the tests assert they are unchanged.

### Format choice: JSON lines, no new dependency

Bronze output is JSON lines, like DE-02/DE-03/DE-04 input: it is the only
format that can hold the input *byte-for-byte* plus metadata (and hold
schema-invalid rows such as `"latitude": 999` or `"event_time":
"not-a-timestamp"` without coercion), Spark/Fabric read it natively, and it
needs only the standard library. Parquet would force a typed schema onto
deliberately untyped Bronze data and add `pyarrow` (a large native dependency)
for no local benefit; columnar Delta storage is the Fabric Lakehouse's job.

### Configuration: `simulator/bronze/default_bronze.toml`

Validated by `simulator/bronze/config.py` (pydantic, unknown keys rejected):
`output_dir`, `batch_input_dir`, `parse_error_policy`,
`verify_source_manifest`, `max_logged_parse_errors` (caps per-record warning
log lines; every error is still counted and stored), `[bronze.stream]`
`source`/`table`, `[bronze.batch]` `source`/`record_source_template`/
`source_manifest_file`.

### Mapping to a future Fabric Bronze notebook

Not implemented or connected - this is the intended translation once the
Fabric trial is active (ADR-0004), recorded so the local contract carries
over unchanged:

| Local (DE-05) | Fabric Lakehouse equivalent |
|---|---|
| `<table>/run_id=<id>/part-00000.jsonl` | Append to a Delta table `bronze_<table>` in the Lakehouse `Tables/` area (partitioned by ingestion date rather than run_id). |
| Byte-preserving splice | Read landed files with `spark.read.text` (or keep the raw `value` column) so the original payload survives; parse with `from_json` for convenience columns. |
| `preserve` / `fail` policy | Spark JSON `mode=PERMISSIVE` with `columnNameOfCorruptRecord` / `mode=FAILFAST`. |
| `ingestion_time`, `source`, `run_id` | `F.current_timestamp()`, `F.lit(source)`, `F.lit(run_id)` from a pipeline parameter (e.g. the Data Factory pipeline run id). |
| `_bronze_source_file`, `_bronze_line_number` | `_metadata.file_name` and a per-file row index. |
| Staging + atomic rename + manifest commit marker | The Delta transaction log (atomic append). |
| Same-run_id no-op | Delta idempotent writes (`txnAppId`/`txnVersion` writer options) or a pre-check against the run-audit table. |
| Duplicate-input guard (file sha256) | A processed-files control table checked before load. |
| `_runs/<run_id>.json` | A `bronze_run_audit` Delta table (PROJECT.md section 15 batch metrics). |
| `previous_attempts` (DE-09) | One `bronze_run_audit` row per attempt (append-only, so no history is overwritten) instead of a nested list. |
| Basename-only `input_files[].path` (DE-09) | Record `_metadata.file_name` / a path relative to the Lakehouse `Files/` landing folder, never a workstation path. |
| `stream` mode over a bounded file/stdin | Eventstream -> Lakehouse destination, or Structured Streaming with a checkpoint where the micro-batch `batchId` plays the role of `run_id`. |

### DE-05 tests

`pytest simulator -q` includes:

- `test_bronze_records.py` - byte-for-byte preservation (number formats,
  escapes, nesting, whitespace, empty object); every parse-error code with
  lossless raw-line recovery (incl. invalid UTF-8 via base64).
- `test_bronze_stream_ingest.py` - DE-02 clean stream and a DE-03 stream with
  all six malformed modes plus duplicates, late, out-of-order, congestion and
  breakdown (a guard test proves the fixture really contains each): full
  round trip of every record, duplicates kept, arrival order kept; multi-file
  lineage; stdin; source override.
- `test_bronze_batch_ingest.py` - committed DE-04 sample: round trip of every
  dataset, counts equal the DE-04 manifest, `event_date` preserved where it
  differs from the UTC date, FU-02 rows ingested; tampered file fails loudly;
  verification-off records `MISMATCH` and preserves the truncated line;
  no-manifest directory; missing/unsafe/empty inputs.
- `test_bronze_idempotency.py` - every row of the idempotency table, a
  simulated crash (orphaned partition + staging), a failure during commit,
  unsafe run ids.
- `test_bronze_parse_errors.py` - preserve vs fail policy on a file mixing
  good, garbage, blank, array, non-UTF-8 and truncated lines; unterminated
  final line; CRLF.
- `test_bronze_manifest.py` - the accounting identity against independently
  computed counts and the bytes on disk (line count, sha256), per file/table/
  total, stream and batch.
- `test_bronze_determinism.py` - byte-identical stores with an injected clock
  (in-process and two CLI processes); wall-clock default; non-UTC clock
  normalised; naive clock rejected.
- `test_bronze_cli.py` - a real `python -m simulator.incidents` stream piped
  into `python -m simulator.bronze stream --input -`; batch rerun/duplicate/
  replay exit codes; `--strict`; missing input; run-id conflict; config
  validation.
- `test_bronze_retry_history.py` (DE-09) - two failures then a success keep
  exactly two `previous_attempts` entries, each equal to the FAILED manifest
  read back from disk before the retry (stream: strict parse failure + I/O
  failure during commit; batch: two source-manifest mismatches; CLI: strict +
  duplicate-input failures); `ALREADY_COMMITTED` adds nothing; refused
  attempts have `totals: null`; a crash after a failure; accounting identities
  and data files equal a never-failed run.
- `test_bronze_path_policy.py` (DE-09) - with absolute inputs (and `HOME`
  pointed at the test directory), no string anywhere in a COMMITTED or FAILED
  manifest starts with `/` or contains the home directory, input or output
  roots or the repository path; library and CLI, stream and batch; error
  messages carrying absolute paths are redacted; manifests from two different
  absolute input locations are byte-identical.
- `test_bronze_legacy_manifest.py` (DE-09) - pre-DE-09 manifests (a verbatim
  `1.0.0` FAILED fixture, and committed manifests rewritten to the exact
  `1.0.0` key sets) are accepted by `BronzeStore`, the no-op/replay guards, a
  retry, and by Silver and Gold, whose output is byte-identical to the same
  pipeline over a `1.1.0` manifest.

### Known limitations (DE-05)

- **Not connected to Fabric** (see status above); local filesystem only.
- **Single writer per store.** No file locking; two concurrent runs against
  the same `output_dir` are unsupported.
- **Inputs are read fully into memory** (fine at local scale - DE-04 default
  output is ~22 MB); not a long-running stream consumer. `stream` mode ingests
  bounded files/stdin as one micro-batch.
- **Replay detection is per whole file (sha256).** Overlap at row level across
  *different* files (e.g. DE-04 regenerated with more `service_days`, where
  the earlier days reappear inside new files) is not detected in Bronze; it is
  handled by Silver's key-based dedup (DE-06). No watermark-based incremental
  selection is applied in Bronze - the watermark/incremental engine
  (PROJECT.md section 11) is later scope.
- **Blank lines are counted, not stored** (they carry no payload).
- Run order in `read_table` is by (`ingestion_time`, `run_id`).
- **Path redaction in error text covers known roots only** (store root, input
  directories, home directory). An absolute path outside all of them inside
  an exception message (none is produced by Bronze's own errors) would be
  kept. Manifests written before DE-09 keep their as-given paths; they are
  read, never rewritten - sanitise them before publishing.
- **Crashed attempts are not in `previous_attempts`** (no manifest was
  written); only their leftovers are recorded via `removed_uncommitted_paths`.
- No retention/compaction of old run partitions.

## DE-06: Local Silver Layer (telemetry stream)

**Status: IMPLEMENTED - NOT CONNECTED.** `simulator/silver/` is local Python
reading the local DE-05 Bronze store and writing local files. No Fabric
Lakehouse, OneLake, Eventstream, Eventhouse or other Azure/Fabric resource
has been created, read or written; nothing here is evidence of Fabric
execution. "Mapping to Fabric" below records how it is meant to carry over.

**Scope:** the DE-02/DE-03 telemetry stream in Bronze (`telemetry_stream`).
Silver for the DE-04 batch datasets (`historical_trips`,
`service_disruptions`, ...) is **out of scope for DE-06**; Bronze batch runs
in the same store are simply not consumed.

Silver (PROJECT.md section 8, ADR-0002) is where every section 12 rule is
enforced: each Bronze record ends up in exactly one of three buckets - a
typed, enriched **clean** record; a **quarantine** record holding the raw
payload losslessly plus why it was rejected; or an excluded, audited
**duplicate** - and the run commits only if the counts reconcile.

### CLI usage

```
python -m simulator.incidents --enable-malformed --enable-duplicate --enable-late-event \
  --enable-out-of-order --max-events 500 --output events.ndjson --log-file incidents.log
python -m simulator.bronze stream --input events.ndjson --run-id b1
python -m simulator.silver --run-id s1
```

| Flag | Default | Meaning |
|---|---|---|
| `--run-id ID` | generated (`silver-<UTC>-<8 hex>`) | Silver run identifier (same alphabet as Bronze run ids). Pass one for reproducible/idempotent reruns. |
| `--bronze-dir PATH` | config `bronze_dir` (`simulator/_out/bronze`) | Bronze store root. |
| `--output-dir PATH` | config `output_dir` (`simulator/_out/silver`, gitignored) | Silver store root. |
| `--bronze-run-id ID` (repeatable) | every committed run holding `bronze_table` | Consume only these committed Bronze runs (processed in Bronze commit order, never CLI order). |
| `--fixed-processing-time ISO` | wall clock | Inject a fixed clock (must carry an offset) for byte-reproducible output. |
| `--silver-config PATH` | `simulator/silver/default_silver.toml` | Alternate Silver config. |
| `--validation-config PATH` | DE-01 `simulator/config/default_validation.toml` | Alternate section 12 thresholds. |
| `--scale-config PATH` | DE-01 `default_scale.toml` | Reference data used for the known-route rule and enrichment. |
| `--log-file PATH` / `--log-level LEVEL` | stderr / `INFO` | Structured JSON-lines logs (DE-02 formatter, logger `simulator.silver`). |

Stdout gets one JSON summary line (`status`, `run_id`, `manifest`,
`bronze_runs_consumed`, the main counters, `quarantined_by_rule`,
`reconciliation_holds`; or `status: FAILED`, `error_type`, `error`). Exit code
0 = committed or already committed with identical input; 1 = refused/failed
(nothing committed). Library API: `simulator.silver.run_silver(...)`.

### Input: committed Bronze runs only

Silver follows the DE-05 read contract (BACKLOG DE-06): Bronze is read only
through committed run manifests (`BronzeStore.committed_manifests()` and each
manifest's `tables[telemetry_stream].path`, exactly what
`committed_partition_files` uses) - never by globbing - so an orphaned
partition of a crashed Bronze run or a `FAILED` run is invisible. Each data
file is verified against its manifest (sha256, line count = `records_written`,
`run_id`, and increasing `_bronze_line_number` per source file) before use; a
mismatch fails the Silver run loudly. The consumed runs (run_id,
ingestion_time, path, sha256, records, `replay_of`) are recorded in the
Silver manifest's `bronze_runs_consumed`.

**Arrival order** = Bronze run order (`ingestion_time`, then `run_id`) and,
within a run, physical line order of its data file (input-file order, then
`_bronze_line_number`). It is recorded per record as `_silver_arrival_seq`.

### Bucket ordering

For each Bronze record, in arrival order:

1. Bronze parse-error record -> **quarantine**, rule `bronze_parse_error`
   (the Bronze code, e.g. `invalid_json`, is kept in the reason and in
   `bronze_parse_error`). Never dropped.
2. Validate against the rule catalogue -> any failure: **quarantine**.
3. Valid and its `event_id` was already loaded in this Silver run ->
   **duplicate** (excluded, counted, written to the duplicates audit table).
4. Valid and first -> **clean**, classified by the event-time watermark as
   `in_order` / `out_of_order` / `late` (late is loaded and flagged).

**Validate-then-dedup** (not dedup-then-validate) is deliberate: a corrupt
copy must never "claim" an event_id and cause a later good copy of the same
event to be discarded as a duplicate, and the quarantine table stays a
complete audit of every rejected *delivery* (a replayed corrupt message is
quarantined each time it arrives). Duplicates are therefore always valid
repeats, which is exactly what "no downstream double-counting" protects.

### Rule catalogue (PROJECT.md section 12)

Precedence is the table order. Thresholds come from DE-01's
`ValidationThresholds` (`simulator/config/default_validation.toml`); Silver
does not duplicate them.

| # | Rule id | Category | PROJECT.md section 12 rule | Check (default) | DE-03 mode that triggers it |
|---|---|---|---|---|---|
| 0 | `bronze_parse_error` | parse | (invalid events must not silently disappear) | Bronze could not store the line as a JSON object | - (any truncated/garbage line) |
| 1 | `supported_schema_version` | schema | supported schema version | `schema_version` in `supported_schema_versions` (`["1.0.0"]`); a failure short-circuits | - |
| 2 | `required_fields` | schema | required event_id/vehicle_id | every `required_fields` entry (all 14 section 6 fields) present, not null, not a blank string | `null_vehicle_id`, `missing_field` |
| 3 | `parsable_timestamps` | schema | parsable timestamps | `generated_at`/`event_time` are ISO-8601 date-time strings **with a UTC offset** | `malformed_timestamp` |
| 4 | `schema_conformance` | schema | (type enforcement, section 8) | strict `TelemetryEvent`: JSON types not coerced, `status` enum, no unknown fields, float fields finite (a number overflowing float64, e.g. `1e400` -> inf, is rejected here regardless of thresholds) | - |
| 5 | `valid_coordinates` | rule | valid latitude/longitude | latitude in [-90, 90], longitude in [-180, 180] | `invalid_coordinates` |
| 6 | `non_negative_speed` | rule | nonnegative speed | `speed_kmh >= 0` and `>= min_speed_kmh` (0) | `negative_speed` |
| 7 | `max_speed` | rule | configurable upper-speed threshold | `speed_kmh <= max_speed_kmh` (120) | - |
| 8 | `non_negative_occupancy` | rule | nonnegative occupancy | `occupancy >= 0` and `>= min_occupancy` (0) | - |
| 9 | `max_occupancy` | rule | nonnegative occupancy (configured upper bound) | `occupancy <= max_occupancy` (150) | - |
| 10 | `heading_range` | rule | (DE-01 threshold beyond section 12) | heading in [0, 360) and [`min_heading_degrees`, `max_heading_degrees`] (0, 359.9) | - |
| 11 | `known_route` | rule | known route where applicable | `route_id` is a DE-01 reference route (when `require_known_route`) | `unknown_route` |
| 12 | `future_time_tolerance` | rule | future-time tolerance | `event_time <= generated_at + future_time_tolerance_seconds` (120) | - |

**Schema failure vs rule failure vs parse error** (the section 12/15
counters): a *parse error* is a line Bronze could not store as a JSON object;
a *schema failure* is a JSON object that does not conform to the telemetry
contract's shape/types (rules 1-4); a *rule failure* is a structurally valid
event that violates a data-quality/business rule (rules 5-12). A record is
counted under the category of its primary (first) rule, so
`quarantined == parse_errors + schema_failures + rule_failures`.

**All failing rules are recorded, within the stage that rejected the record.**
Validation has two stages. Stage 1 (structural: rules 1-4 plus the field
bounds DE-01 puts on `TelemetryEvent`, which map to rules 5, 6, 8 and 10) is
evaluated on the record's exact source text with
`TelemetryEvent.model_validate_json(..., strict=True)`; pydantic reports every
failing field at once and each error is mapped to its rule. Stage 2 runs only
if stage 1 passed (the business rules need a typed event) and calls DE-01's
`validate_against_thresholds` **unchanged**; each reason it returns is mapped
to a rule id via `simulator.silver.rules.THRESHOLD_REASON_RULES`, and an
unmapped reason raises `ValidationContractError` rather than producing an
unlabelled rejection (tested against every DE-01 reason). So a record with a
negative speed *and* an unknown route is quarantined as `non_negative_speed`
only; one with two threshold failures lists both. The quarantine record keeps
`rejection_rules` (all, in precedence order), `primary_rule` (the first, used
for `quarantined_by_rule`, which therefore sums to `quarantined`) and
`rejections` (rule, category, human-readable reason with the offending
value); `rule_violations` in the manifest counts every rule hit.

**Divergences from DE-01, and why:** (a) the schema-version gate runs first,
on the raw value - the version decides which contract applies, so an
unknown-version record would otherwise be reported as a pile of misleading
structural errors; (b) timestamps must carry a UTC offset - DE-01's helper
assumes UTC for a naive value, which would silently shift a Sydney local time
by 10-11 hours, and a bare epoch number (even inside a string) is not
ISO-8601; (c) strict typing - `"12.5"` is not coerced to a speed and `5.5` is
not truncated to an occupancy (PROJECT.md section 8 "type enforcement").

### Future-time reference

`future_time_tolerance_seconds` is measured against the event's own
**`generated_at`** by default (`future_time_reference = "generated_at"`): an
event cannot be observed after the message carrying it was produced, so
`event_time > generated_at + tolerance` identifies a corrupted or clock-skewed
reading. This was chosen over the wall clock / `processing_time` (simulated
timelines start in 2025, so a wall-clock check would never fire on replayed
data, and results would depend on *when* Silver runs - neither replayable nor
byte-reproducible) and over Bronze `ingestion_time` (also a wall clock: a
stream simulated with a future `simulated_start_time` would be quarantined
wholesale). `future_time_reference = "ingestion_time"` is available for live
feeds, where it additionally catches a producer whose whole clock
(`generated_at` included) runs fast. A late event is never a future-time
failure.

### Deduplication policy

Key: the exact `event_id` string. Scope: every Bronze run consumed by the
Silver run (so a DE-05 `--allow-replay` run, or the same event re-sent in a
later file, is excluded - tested). Winner: the **first valid arrival** (Bronze
run order, then line order). Each excluded copy is written to the duplicates
table with its own lineage, the kept copy's lineage, its payload sha256 and
`payload_matches_kept`; a same-id-different-payload copy also increments
`duplicate_payload_conflicts`. Dedup state is exact and unbounded within a
run (local scale) - stronger than Spark's watermark-bounded state.

### Event time, watermark, late and out-of-order policy

`processing_time` is stamped once per Silver run from the injectable clock
(like Bronze's `ingestion_time`); `event_time` is the reading's own time;
`ingestion_time` is carried from Bronze. Event-time state is a watermark over
valid, first-seen events in arrival order:

```
watermark = max(event_time of valid, non-duplicate events seen so far) - allowed_lateness_seconds  (default 60)
event_time >= max seen               -> in_order
watermark <= event_time < max seen   -> out_of_order  (accepted, flagged, counted)
event_time <  watermark              -> late          (LOADED, flagged, counted - never dropped)
```

Only valid, non-duplicate events advance the maximum: a corrupt future
timestamp must not make every later event "late", and a replay must not move
time. **Late events are loaded and flagged rather than routed to a separate
bucket** because they are valid observations (section 12 forbids invalid
events silently disappearing; late ones are not even invalid), historical/
batch Gold (DE-07, Warehouse) should include them, and a streaming aggregate
whose window has already closed can exclude them with one predicate on
`_silver_event_time_status`. Each clean record carries
`_silver_event_time_status`, `_silver_lateness_seconds` (max seen minus
event_time; 0 when in order) and `_silver_watermark` (the watermark at
arrival). The default lateness (60 s) matches DE-03's documented consumer
threshold (`late_threshold_seconds`) but is Silver's own setting. The
watermark is evaluated per record (a micro-batch of one); a Spark job
evaluates it per micro-batch (see the mapping below).

### Enrichment and standardisation (clean table)

Clean records hold the 14 section 6 fields **in DE-01 contract order** in a
standardised typed form: timestamps rewritten to UTC ISO-8601 with
microseconds and `Z` (`2025-01-01T11:00:00.5+11:00` ->
`2025-01-01T00:00:00.500000Z`; the instant never changes; sub-microsecond
digits are truncated by Python's datetime); float fields as JSON floats (an
integer speed `12` becomes `12.0`); `status` as its enum string. Values are
otherwise unchanged. Enrichment from DE-01 reference data is added in clearly
named `ref_*` columns and never overwrites an original field:
`ref_route_number`, `ref_route_name`, `ref_vehicle_home_depot_id` (null, and
counted in `enrichment_misses`, for a vehicle not in the reference data - an
unknown vehicle is not a section 12 rule). Then lineage (`ingestion_time`,
`source`, `bronze_run_id`, `_bronze_source_file`, `_bronze_line_number`),
`silver_run_id`, `processing_time` and the `_silver_*` event-time columns.

### Quarantine record (lossless)

`silver_run_id`, `processing_time`, `ingestion_time`, `source`,
`bronze_run_id`, `_bronze_source_file`, `_bronze_line_number`,
`_silver_arrival_seq`, `primary_rule`, `rejection_category`,
`rejection_rules`, `rejections`, `event_id` (when it is a string), and the raw
payload:

| `raw_format` | `raw_payload` | `raw_payload_text` / `raw_encoding` |
|---|---|---|
| `json_object` | the original object (Bronze metadata stripped), key order kept | the **exact original text** of the object (the Bronze line minus its spliced metadata, recovered and verified against the record), `utf-8` |
| `bronze_parse_error` | null | Bronze's `_bronze_raw_line` / `_bronze_raw_encoding` (`base64` for non-UTF-8), plus `bronze_parse_error` / `bronze_parse_error_detail` |

So the original input bytes are always recoverable (number spellings such as
`-1.50`/`1e2`, escapes, big integers and invalid UTF-8 included - tested).
Bronze itself trims JSON whitespace at the line ends (DE-05 contract).

Valid JSON syntax that strict UTF-8 JSON output cannot represent - a number
overflowing float64 (`1e400` parses to inf) or a lone surrogate escape
(`"\ud800"`), in any field or key - is still quarantined, never aborts the
run: `raw_payload` is null, `raw_payload_omitted_reasons` lists why
(`non_finite_number`, `unpaired_surrogate`; `[]` otherwise), `event_id` is
null if it holds a lone surrogate, and `raw_payload_text` (ASCII escapes) keeps
the exact original (PR #10 QA D1/D2, tested).

### Output layout

```
<output_dir>/                                         (default simulator/_out/silver, gitignored)
  telemetry_clean/run_id=<id>/part-00000.jsonl        valid, deduplicated, typed, enriched
  telemetry_quarantine/run_id=<id>/part-00000.jsonl   every rejected record + raw payload + reasons
  telemetry_duplicates/run_id=<id>/part-00000.jsonl   excluded duplicate copies (lineage only)
  _runs/<id>.json                                     run manifest: audit record AND commit marker
  _staging/<id>/...                                   in-flight run, never read, removed on finish
```

The commit protocol reuses DE-05's layer-agnostic `BronzeStore` primitives by
import (staging, atomic directory rename, manifest written last): **a Silver
run is committed iff `_runs/<id>.json` says `COMMITTED`**. Each Silver run is
a complete snapshot of the Bronze runs it consumed; dedup and watermark state
are scoped to the run, so consumers (DE-07) read one committed Silver run.

### Run manifest and PROJECT.md section 15 counters

`_runs/<id>.json` (sorted keys): `run_id`, `status`, `processing_time`,
`started_at`/`ended_at`/`duration_seconds`, `input_fingerprint`,
`previous_attempts` (failed attempts of this run_id - the FU-03 audit trail,
kept from the start in Silver), `bronze_table`, `bronze_selection`
(`all_committed`/`explicit`), `bronze_runs_consumed` (with
`bronze_to_silver_latency_seconds` = processing_time - Bronze
ingestion_time), `config`, `validation_thresholds`, `reference_sha256`,
`rule_catalogue`, `tables` (path, sha256, records), `event_time` (lateness,
reference, min/max event_time, final watermark), `reconciliation`,
`removed_uncommitted_paths`, `error`, and `counters`:

| Counter | Meaning |
|---|---|
| `extracted` | Bronze records read (section 15 "received" / "source extracted") |
| `valid_loaded` | written to the clean table |
| `quarantined` | written to the quarantine table |
| `duplicates_excluded` | valid repeats excluded (section 15 "duplicates") |
| `duplicate_payload_conflicts` | duplicates whose payload differed from the kept copy |
| `in_order` / `out_of_order` / `late` | event-time status of `valid_loaded` (section 15 "late events") |
| `parse_errors` / `schema_failures` / `rule_failures` | quarantined, by primary-rule category (section 12 "schema-failure counts") |
| `quarantined_by_rule` | quarantined, by primary rule (sums to `quarantined`) |
| `rule_violations` | every rule hit (a record can hit several) |
| `parse_errors_by_code` | Bronze parse-error codes |
| `enrichment_misses` | clean records whose route/vehicle is not in the reference data |
| `quality_percentage` | `100 * valid_loaded / (valid_loaded + quarantined)`, 4 dp; null when there is nothing to judge. Duplicates are left out of the denominator: a replay is a delivery issue, not a data-quality failure. |
| `throughput_records_per_second` | `extracted / duration_seconds` (null under a fixed clock, where the duration is 0) |

The data is synthetic (no PII); the quarantine store is local only.

### Reconciliation identity (for later Fabric reuse)

Asserted in code before commit (`simulator.silver.pipeline.reconcile`) from
independently obtained numbers - bucket counters incremented at
classification, the Bronze manifests' record counts, and the lines actually
written to each staged file - and recorded in `reconciliation`
(`identities`, `checks`, `holds`, and the evaluated `formula`). Any failure
raises `SilverAccountingError`; nothing is committed and a `FAILED` manifest
records which check failed.

```
extracted    == valid_loaded + quarantined + duplicates_excluded    (section 15; "intentionally excluded" = duplicates)
extracted    == sum(bronze_runs_consumed[].records)                  (Bronze -> Silver hand-off; continues DE-05's records_written)
quarantined  == parse_errors + schema_failures + rule_failures == sum(quarantined_by_rule)
valid_loaded == in_order + out_of_order + late                       (late is a flag on loaded data, not a bucket)
lines on disk per table == valid_loaded / quarantined / duplicates_excluded
```

Together with DE-05 this gives the end-to-end chain: input lines ->
`records_written` (Bronze) -> `extracted` -> three Silver buckets.

### Idempotency and reruns

| Situation | Behaviour |
|---|---|
| Same run_id, same input fingerprint, committed | No-op: `ALREADY_COMMITTED`, nothing written (byte-identical even under a later clock). |
| Same run_id, different fingerprint (Bronze runs and their sha256, Silver config, thresholds, reference data) | Refused: `SilverRunIdConflictError`. A committed run is never overwritten. |
| Same run_id after a `FAILED`/crashed attempt | Retried from scratch: orphaned staging/partitions removed (`removed_uncommitted_paths`); the failed attempt is kept in `previous_attempts`. |
| New run_id | A new, independent snapshot (same data apart from `silver_run_id`/`processing_time`). |

Same Bronze inputs + same Silver run_id + fixed clock -> byte-identical
tables and manifest in any store, in-process or across separate CLI
processes (tested). Manifests never record local filesystem paths (BACKLOG
FU-04): Bronze paths are relative to the Bronze root and store roots are not
recorded.

### Configuration: `simulator/silver/default_silver.toml`

Validated by `simulator/silver/config.py` (pydantic, unknown keys rejected):
`output_dir`, `bronze_dir`, `bronze_table`, `clean_table`,
`quarantine_table`, `duplicates_table` (distinct, Bronze table-name
alphabet), `allowed_lateness_seconds`, `future_time_reference`
(`generated_at`/`ingestion_time`), `max_logged_rejections` (caps per-record
warning log lines; every rejection is still counted and written). Section 12
thresholds stay in DE-01's `default_validation.toml`.

### Mapping to Fabric

Not implemented or connected - the intended translation once the Fabric trial
is active (ADR-0004):

| Local (DE-06) | Fabric equivalent |
|---|---|
| Read committed Bronze runs via manifests | Read the `bronze_telemetry_stream` Delta table (only committed transactions are visible); incremental runs via Delta change data feed or Structured Streaming with a checkpoint. |
| Strict `TelemetryEvent` validation + rule mapping | `from_json` with an explicit schema + `columnNameOfCorruptRecord`, then one boolean column per rule and an array column of failed rule ids, in a PySpark notebook. |
| Validate-then-dedup, first arrival wins | Split valid/invalid, then `dropDuplicates(["event_id"])` (or `dropDuplicatesWithinWatermark`) on the valid stream; in batch, `row_number()` over `event_id` ordered by ingestion time and source position. |
| Event-time watermark, late flagged | `withWatermark("event_time", "60 seconds")` for stateful windows. Spark silently drops late rows inside stateful operators, which section 12 forbids, so late rows are flagged (compare with the max event_time per micro-batch in `foreachBatch`) or routed to a late table before aggregation. Eventhouse/KQL equivalent: `ingestion_time()` vs event time. |
| Quarantine JSONL | `silver_telemetry_quarantine` Delta table (raw `value` column kept), appended in the same job. |
| Duplicates audit JSONL | `silver_telemetry_duplicates` Delta table, or counts only in the run-audit table. |
| `ref_*` enrichment | Broadcast join with route/vehicle reference tables. |
| Manifest + reconciliation assert | `silver_run_audit` Delta table; the notebook raises before the Delta commit if the identity fails. |
| Staging + rename + manifest commit marker | The Delta transaction (with `txnAppId`/`txnVersion` for idempotent reruns). |

### DE-06 tests

`pytest simulator -q` includes 136 Silver tests:

- `test_silver_rules.py` - a negative and a positive (boundary) case for every
  section 12 rule and the extra rules; configurability of every threshold via
  DE-01 `ValidationThresholds`; future-time reference behaviour; all failing
  rules recorded in precedence order; every DE-01 reason mapped, an unmapped
  one fails loudly.
- `test_silver_quarantine.py` - every Bronze parse-error code routed to
  quarantine with its code and exact raw bytes (base64 for invalid UTF-8);
  section 12 quarantine metadata; byte-exact raw payload for unusual JSON
  spellings; each rule lands in quarantine and is counted; schema vs rule vs
  parse counters.
- `test_silver_dedup.py` - within-run duplicates; a DE-05 `--allow-replay`
  run does not double-count; Bronze commit order decides the winner;
  conflicting payloads; validate-then-dedup behaviour.
- `test_silver_event_time.py` - watermark boundaries; late loaded and flagged
  with the right lateness/watermark; out-of-order within lateness; an invalid
  future event does not poison the watermark; processing vs event vs
  ingestion time.
- `test_silver_reconciliation.py` - the identity vs independently computed
  counts and record-level lineage coverage; a dropped input record and an
  unwritten output record each fail loudly with nothing committed; the Bronze
  read contract (orphan partitions, FAILED runs, batch runs, tampered
  partition, explicit selection).
- `test_silver_idempotency.py` - byte-identical output (in-process and two
  CLI processes), no-op rerun, conflicts, failed-attempt retry with
  `previous_attempts`, crash leftovers, no absolute paths in manifests.
- `test_silver_scenarios.py` - each DE-03 scenario on its own: every
  malformed mode -> its rule; duplicates excluded; late events loaded and
  flagged exactly as injected; out-of-order accepted; congestion, volume
  spike and breakdown stay valid (with their degraded values) and recover.
- `test_silver_end_to_end.py` - all seven DE-03 scenarios at once -> Bronze
  (plus a deliberate replay run and a run of unparseable lines) -> Silver,
  asserted against counts derived independently from the DE-03 structured
  incident log and a clean DE-02 generator (`_silver_helpers.
  derive_expectations`, which uses no Silver code); and the same through the
  three real CLIs as separate processes.
- `test_silver_cli.py` - CLI commit/no-op/failure exit codes, alternate
  validation config, config validation, clean-record standardisation and
  enrichment.

### Known limitations (DE-06)

- **Not connected to Fabric** (see status above); local filesystem only.
- **Telemetry stream only.** Silver for the DE-04 batch datasets (and the
  FU-02 out-of-order `updated_at` handling there) is not implemented.
- **Full recompute per Silver run.** Each run re-reads its selected Bronze
  runs; dedup/watermark state is not carried between Silver runs (no
  checkpoint). Selecting a subset of Bronze runs dedups only within that
  subset - consumers must read one Silver run, not union several.
- **Stage-2 rules are not evaluated for a structurally invalid record**, so
  `rejection_rules` is complete only within the failing stage (see above).
- **No service-area (bounding box) GPS check.** `valid_coordinates` is the
  global lat/lon range; "null island" (0, 0) would pass. A configurable
  Sydney bounding-box rule is a candidate follow-up.
- **Resolved (FU-05): DE-03 duplicate injection used to replace the event it
  fired on**, so that event could be missing from the stream or first arrive
  later as a "replay". DE-03 now delivers each duplicate as an extra copy
  after its original (see DE-03 "Duplicate delivery and `--max-events`");
  `run_incident_stream` mirrors the corrected CLI loop and the DE-06
  end-to-end/oracle tests pass unchanged against it.
- Single writer per Silver store (no locking); inputs read fully into memory;
  per-record (not micro-batch) watermark; timestamps truncated to
  microseconds.

## DE-07: Local Gold Layer Groundwork (route performance aggregate)

**Status: IMPLEMENTED - NOT CONNECTED.** `simulator/gold/` is local Python
that reads the local DE-06 Silver store and writes local files. No Fabric
Lakehouse, Warehouse, OneLake, semantic model or any other Azure/Fabric
resource has been created, read or written. Nothing here is evidence of
Fabric execution.

**Scope: groundwork, not the full PROJECT.md section 10 model.** DE-07 adds
one small aggregate, `route_performance`, to close the local
Bronze -> Silver -> Gold chain. It does **not** implement `dim_*`/`fact_*`
tables, `dim_date`/`dim_time`, SCD Type 2, Warehouse SQL, a Power BI
semantic model, or the vehicle/trip/passenger/incident Gold facts. Those
belong to the later Warehouse/dimensional-model story in `docs/BACKLOG.md`.
That story also needs FU-01, which aligns live and scheduled trip ids before
`fact_route_performance` can be joined to the timetable. This aggregate is
an input that story can build on, not a replacement for it.

### CLI usage

```
python -m simulator.incidents --enable-malformed --enable-duplicate --enable-late-event \
  --enable-out-of-order --max-events 500 --output events.ndjson
python -m simulator.bronze stream --input events.ndjson --run-id b1
python -m simulator.silver --run-id s1
python -m simulator.gold --run-id g1
```

| Flag | Default | Meaning |
|---|---|---|
| `--run-id ID` | generated (`gold-<UTC>-<8 hex>`) | Gold run identifier (same alphabet as Bronze/Silver run ids). Pass one for reproducible or idempotent reruns. |
| `--silver-dir PATH` | config `silver_dir` (`simulator/_out/silver`) | Silver store root. |
| `--output-dir PATH` | config `output_dir` (`simulator/_out/gold`, gitignored) | Gold store root. |
| `--silver-run-id ID` | the latest committed Silver run | Aggregate exactly this committed Silver run. |
| `--fixed-processing-time ISO` | wall clock | Inject a fixed clock (must carry an offset) for byte-reproducible output. |
| `--gold-config PATH` | `simulator/gold/default_gold.toml` | Alternate Gold config. |
| `--scale-config PATH` | DE-01 `default_scale.toml` | Reference data (route names, known routes). It must be the one Silver used: Gold checks it against the Silver manifest's `reference_sha256`. |
| `--log-file PATH` / `--log-level LEVEL` | stderr / `INFO` | Structured JSON-lines logs (DE-02 formatter, logger `simulator.gold`). |

The CLI prints one JSON summary line to stdout: `status`, `run_id`,
`manifest`, `silver_runs_consumed`, `silver_selection`, the main counters
and `reconciliation_holds`. On failure it prints `status: FAILED`,
`error_type` and `error`. Exit code 0 means committed, or already committed
with identical input. Exit code 1 means the run was refused or failed, and
nothing was committed. Library API: `simulator.gold.run_gold(...)`.

### Input: one committed Silver run, read through its manifest

Gold reads Silver the same way Silver reads Bronze (BACKLOG DE-07 "Silver
input contract"). It never globs Silver data directories. It lists
`_runs/*.json` and keeps only manifests with `layer: "silver"` and
`status: "COMMITTED"`, so FAILED or crashed runs and their orphaned
partitions are invisible. Before using a committed manifest, Gold checks
that its run_id matches its file name, that it has a supported
`silver_manifest_version`, that `reconciliation.holds` is true, and that its
table paths are relative and contain no `..`. Gold then finds each table
through `tables.<role>.path` (`clean`, `quarantine`, `duplicates`) and
verifies it before using any row: the file must exist, its sha256 must match
the manifest, and its line count must equal both the manifest's `records`
and the matching Silver counter (`valid_loaded` / `quarantined` /
`duplicates_excluded`). Every record must also carry this `silver_run_id`.
Any mismatch raises `GoldInputError` and nothing is committed.

**Why one run, and why the latest by default.** Each Silver run is a
complete snapshot of the Bronze runs it consumed, with dedup and watermark
state scoped to the run (DE-06 "Output layout" and "Known limitations").
Unioning two Silver runs would therefore count every shared event twice, so
Gold consumes exactly one. By default that is the latest committed run,
meaning the greatest `processing_time` with ties broken by `run_id`, because
the latest snapshot is the most complete view. `--silver-run-id` pins a
specific run, for example to reproduce an earlier Gold result. The choice is
recorded in the manifest as `silver_selection` (`latest_committed` or
`explicit`).

### Aggregate definition (`route_performance`)

The grain is one row per (`route_id`, period). A row exists if the cell has
at least one clean record or at least one attributed quarantined record.
Rows are ordered by (`period_start`, `route_id`).

| Column | Definition |
|---|---|
| `route_id`, `route_number`, `route_name` | Route, with `ref_route_number`/`ref_route_name` from the Silver clean records (DE-01 reference). A route that has only quarantined records takes its names from the same reference data. |
| `period_start`, `period_end` | Bucket `[period_start, period_end)` in UTC (`...Z`, microseconds, the same format as Silver timestamps). |
| `period_start_local`, `period_end_local` | The same instants at the fixed local offset (`+10:00` by default). |
| `period_minutes` | Bucket length (config). |
| `event_count` | Silver **clean** records of the route whose `event_time` falls in the bucket. **Late and out-of-order rows are included.** |
| `avg_delay_seconds` | `sum_delay_seconds / event_count`, rounded to 4 dp. Null when `event_count == 0`. |
| `sum_delay_seconds` | Sum of `delay_seconds` over those records. Kept because averages are not additive, so a later rollup (daily, all routes) must use sum / count rather than average the averages. |
| `late_event_count`, `out_of_order_event_count` | How many of `event_count` Silver flagged `late` / `out_of_order` (`_silver_event_time_status`). |
| `quarantined_count` | Silver quarantine records **attributed** to this route and bucket (see below). |
| `valid_percentage` | `100 * event_count / (event_count + quarantined_count)`, rounded to 4 dp. |
| `gold_run_id`, `silver_run_id`, `processing_time` | Lineage (`processing_time` is the Gold run's injectable clock, stamped once per run). |

**Exclusions.** Quarantined records and excluded duplicates never reach
`event_count`, `avg_delay_seconds` or `sum_delay_seconds`, because they are
not in the Silver clean table. A duplicate is counted once, with the value of
its first valid arrival (Silver's dedup winner). Duplicates are also kept
out of the `valid_percentage` denominator, the same convention as Silver's
`quality_percentage`: a replay is a delivery issue, not a data-quality
failure.

**Late rows are included, never filtered.** Silver loads late events and
flags them. A historical aggregate must count every valid observation, so
`late_event_count` makes their share visible without dropping them. Only a
future streaming aggregate over closed windows may exclude late rows, with
one predicate on `_silver_event_time_status`.

### Period and timezone

Buckets are `period_minutes` long (default 60, which gives hourly route
performance). They are aligned to **local midnight at a fixed UTC offset**
(`utc_offset_hours`, default +10 AEST, no daylight saving). That is the same
convention DE-04 uses for its local service dates (`default_batch.toml`).
Timestamps are stored in UTC, because Silver standardises `event_time` to
UTC and Fabric/Spark timestamps are UTC instants. Each bucket is also
labelled in local time. With a whole-hour offset and hourly buckets, the UTC
and local bucket boundaries are identical, so the offset only changes the
`*_local` labels. With `period_minutes = 1440` it makes a bucket a local
service day (14:00 UTC to 14:00 UTC) rather than a UTC day, which keeps
Gold consistent with DE-04's `event_date`. `period_minutes` must divide 1440
so that boundaries line up every day (validated). `event_time` is the
bucket key, not ingestion or processing time, so a late event lands in the
period it describes. Limitation: the offset is fixed, so real Sydney
daylight saving (+11) is not modelled. This is the same limitation as DE-04.

### Quarantine attribution and `valid_percentage`

Some quarantined records lack a usable route or time, for example DE-03's
`missing_field`, `malformed_timestamp` and `unknown_route` modes, or a
truncated line. Each quarantined record is therefore **attributed** to a
(route, bucket) only when all of the following hold. Otherwise it is counted
once in an explicit **unattributed** total, under the first reason that
applies:

| # | Condition for attribution | Unattributed reason if it fails |
|---|---|---|
| 1 | The record is a JSON object (not a Bronze parse error). | `bronze_parse_error` |
| 2 | Silver kept its parsed form (`raw_payload` not null; null means `raw_payload_omitted_reasons` such as `non_finite_number` / `unpaired_surrogate`). | `raw_payload_unavailable` |
| 3 | `route_id` is a string naming a DE-01 reference route. An unknown route has no row of its own, because a Warehouse fact row would have no `dim_route` member. | `route_id_missing_or_unknown` |
| 4 | `event_time` is an ISO-8601 string with a UTC offset (Silver's `parsable_timestamps` check) and its bucket is representable. | `event_time_unparseable` |
| 5 | Silver did not reject the record *because of* its event_time (`future_time_tolerance` not in `rejection_rules`), since that time would place it in the wrong period. | `event_time_rejected` |

So a record with a null `vehicle_id`, an invalid coordinate or a negative
speed, but an intact route and time, lowers that route's `valid_percentage`
for that bucket. A record that cannot be placed lowers only the run-level
figure. Run totals are in the manifest: `quarantined_attributed`,
`quarantined_unattributed`, `unattributed_by_reason`, and
`overall_valid_percentage = 100 * valid_loaded / (valid_loaded +
quarantined)`. The last one must equal Silver's `quality_percentage` and is
checked.

### Reconciliation identity (asserted before commit)

`simulator.gold.pipeline.reconcile` checks the following from independently
obtained numbers: row sums, records actually read, the Silver manifest's
counters, and lines actually written. The results are recorded in
`reconciliation` (`identities`, `checks`, `holds`, `formula`). Any failure
raises `GoldAccountingError`. In that case nothing is published and a FAILED
manifest names the failing checks.

```
sum(rows.event_count)                             == clean records read == silver.valid_loaded
quarantined_attributed + quarantined_unattributed == quarantine records read == silver.quarantined
sum(rows.quarantined_count)                       == quarantined_attributed
sum(unattributed_by_reason)                       == quarantined_unattributed
sum(rows.late_event_count)                        == silver.late
sum(rows.out_of_order_event_count)                == silver.out_of_order
sum(rows.sum_delay_seconds)                       == sum(delay_seconds) over clean records read
silver.extracted == silver.valid_loaded + silver.quarantined + silver.duplicates_excluded
overall_valid_percentage                          == silver.quality_percentage
lines in the JSONL (and CSV, minus header)        == number of rows
```

With DE-05 and DE-06 this gives the full chain: input lines -> Bronze
`records_written` -> Silver `extracted` = clean + quarantined + duplicates
-> Gold `event_count` (clean) + attributed/unattributed (quarantined), with
duplicates recorded as excluded.

### Output, lineage and manifest

```
<output_dir>/                                       (default simulator/_out/gold, gitignored)
  route_performance/run_id=<id>/part-00000.jsonl    canonical aggregate (JSON lines)
  route_performance/run_id=<id>/part-00000.csv      same rows and column order (write_csv)
  _runs/<id>.json                                   run manifest: audit record AND commit marker
  _staging/<id>/...                                 in-flight run, never read, removed on finish
```

The commit protocol reuses DE-05's `BronzeStore` primitives by import, as
Silver does: staging, atomic directory rename, and the manifest written
last. **A Gold run is committed if and only if `_runs/<id>.json` says
`COMMITTED`.** The manifest (sorted keys) records `run_id`, `status`,
`processing_time`, `started_at`/`ended_at`/`duration_seconds`,
`input_fingerprint` and `previous_attempts` (FU-03-style audit of failed
attempts). It records `silver_selection` and `silver_runs_consumed`: the
Silver run id, its `processing_time` and `input_fingerprint`, the Bronze run
ids behind it and which of them were DE-05 replays, its three tables (path
relative to the Silver root, sha256, records), its main counters, and
`silver_to_gold_latency_seconds`. It also records `config`,
`reference_sha256`, `aggregate_definition` (the formulas above, verbatim),
`tables.route_performance` (path, sha256, records, CSV path and sha256,
columns), `counters`, `reconciliation`, `removed_uncommitted_paths` and
`error`. Store roots and local absolute paths are never recorded (FU-04).

### Idempotency and reruns

| Situation | Behaviour |
|---|---|
| Same run_id, same input fingerprint, committed | No-op: `ALREADY_COMMITTED`, nothing written (byte-identical even under a later clock). |
| Same run_id, different fingerprint (another Silver run or Silver tables, Gold config, reference data) | Refused: `GoldRunIdConflictError`. A committed run is never overwritten. With the default selection this includes "a newer Silver run now exists". Use a new Gold run_id, or pin `--silver-run-id`. |
| Same run_id after a FAILED or crashed attempt | Retried from scratch. Orphaned staging and partitions are removed (`removed_uncommitted_paths`) and the failed attempt is kept in `previous_attempts`. |

Same Silver input, same Gold run_id and a fixed clock produce byte-identical
JSONL, CSV and manifest in any store, in-process or across separate CLI
processes (tested).

### Configuration: `simulator/gold/default_gold.toml`

Validated by `simulator/gold/config.py` (pydantic, unknown keys rejected):
`output_dir`, `silver_dir`, `route_performance_table`, `period_minutes`
(must divide 1440), `utc_offset_hours` (-12..14), `write_csv`.

### Mapping to Fabric

None of this is implemented or connected. It is the intended translation
once the Fabric trial is active (ADR-0002, ADR-0004):

| Local (DE-07) | Fabric equivalent |
|---|---|
| Read one committed Silver run via its manifest | Read the `silver_telemetry_clean` / `_quarantine` Delta tables at a pinned table version (time travel), recorded in the run-audit table. |
| Hourly bucket at a fixed offset | `F.window("event_time", "1 hour")` (UTC); local labels via `from_utc_timestamp`. |
| Aggregate | `groupBy("route_id", window).agg(count, avg("delay_seconds"), sum(...), sum(late flag))` in a PySpark notebook. Late rows are included because this is a batch aggregate, not a stateful streaming window. |
| `route_performance` JSONL/CSV | A Gold Delta table `gold_route_performance` in the Lakehouse `Tables/` area. |
| Later Warehouse story | `fact_route_performance` in the Fabric Warehouse, keyed to `dim_route`, `dim_date` and `dim_time` and loaded from the Gold table. SCD2 dimensions and the Power BI semantic model are also part of that story, not DE-07. |
| Manifest + reconciliation assert | `gold_run_audit` Delta table. The notebook raises before the Delta commit if the identity fails. |
| Staging + rename + manifest commit marker | The Delta transaction (`txnAppId`/`txnVersion` for idempotent reruns). |

### DE-07 tests

`pytest simulator -q` includes 69 Gold tests:

- `test_gold_aggregate.py`: the **hand-computed fixture**. A 17-line stream
  (clean, late, duplicate with a conflicting payload, every unattributed
  reason, quarantine-only cell, bucket-boundary instants, a `+10:00` input
  timestamp) goes through the real Bronze and Silver code. Its expected rows
  and totals were worked out by hand in `_gold_helpers.py` (docstring
  table), not by calling Gold code. A guard test asserts the Silver outcome
  the hand computation assumes. The file also covers CSV equal to JSONL;
  quarantined and duplicate exclusion; late inclusion; daily buckets aligned
  to local midnight; period bucketing incl. negative offsets and pre-1970
  instants; config validation; and every branch of the attribution rule.
- `test_gold_reconciliation.py`: identities against counts taken directly
  from the Silver tables; lineage; a row lost in aggregation fails loudly
  with nothing committed; a DE-05 replay in the lineage sets
  `replay_sensitive` and still reconciles (FU-07).
- `test_gold_idempotency.py`: byte-identical stores (in-process, two CLI
  processes, moved stores); no-op rerun; conflict on a newer Silver snapshot
  or a config change; failed-attempt retry with `previous_attempts`; crash
  leftovers; no local paths in the manifest.
- `test_gold_silver_contract.py`: no committed Silver run; orphan partition
  of an uncommitted run; newer FAILED run; latest chosen by processing time;
  tampered or missing clean/quarantine/duplicates files; re-signed tampering
  caught by the record-run-id and counter checks; manifest with failed
  reconciliation or an unsafe path; reference-data mismatch.
- `test_gold_end_to_end.py`: all seven DE-03 scenarios -> Bronze -> Silver
  -> Gold through the four real CLIs as separate processes. Reconciliation
  is checked against the Silver tables, with per-(route, hour) counts and
  delay sums recomputed independently. Congestion makes ROUTE-01 the
  slowest route. A second Gold process produces byte-identical output.
- `test_gold_cli.py`: exit codes and the summary line, the no-op rerun,
  missing input, unknown Silver run, run-id conflict, alternate config and
  an invalid run id.

### Known limitations (DE-07)

- **Not connected to Fabric** (see status above). Local filesystem only.
- **Groundwork only.** One aggregate, with no dimensional model, SCD2,
  Warehouse or Power BI (see the scope note at the top of this section).
- **Replay-sensitive quality metrics (BACKLOG FU-07).** Silver quarantines
  each delivery of a rejected record, so a DE-05 `--allow-replay` run
  inflates `quarantined_count` and lowers `valid_percentage`. Valid events
  are not affected, because their replays are excluded duplicates. Gold
  flags this (`counters.replay_sensitive`, `replayed_bronze_runs`, plus a
  WARNING log) but does not correct it. It should be fixed in Silver before
  the Data Quality page uses these numbers.
- **Late flags inherit FU-06.** `late_event_count` is Silver's flag. A
  future-dated valid outlier can mis-flag later events as late. This does
  not change `event_count` or averages, because late rows are included
  either way.
- **Attribution sees only the failing Silver stage.** Silver evaluates
  stage-2 rules (including `future_time_tolerance`) only for records that
  pass stage 1. So a record with, for example, a negative speed *and* a
  far-future `event_time` is attributed to that future bucket.
- **Full recompute per run.** There is no incremental Gold or watermark, and
  inputs are read fully into memory (local scale). Single writer per Gold
  store.
- **Fixed UTC offset.** No daylight saving, as in DE-04.
- **`delay_seconds` is unbounded in Silver (FU-08).** An absurd but valid
  delay is averaged as-is.

## DE-11: Incremental Batch Extraction with Watermark Control into Bronze

**Status: IMPLEMENTED - NOT CONNECTED.** `simulator/incremental/` is local
Python that reads local files and writes the local Bronze store through the
DE-05 library. No Fabric Data Factory pipeline, Lakehouse, OneLake or any
other Azure/Fabric resource has been created, read or written, and nothing
here is evidence of Fabric execution. "Mapping to Fabric" below records how
it is meant to carry over.

The loader implements PROJECT.md section 11: it reloads the DE-01 reference
entities in full, loads the DE-04 transactional datasets incrementally by
a per-dataset high-watermark on `updated_at` (the maximum `updated_at`
actually committed), lands each extract in Bronze with
`simulator.bronze.ingest_batch`, and advances a dataset's watermark only
after Bronze returns `COMMITTED` or `ALREADY_COMMITTED`. Each dataset
commits and advances on its own (Lead decision 4), so a failed dataset
never blocks or rolls back another. It uses only the public APIs of
`simulator.batch` (`DATASET_CATALOGUE`) and `simulator.bronze`
(`ingest_batch`, `BronzeStore`, `load_bronze_config`).

### CLI usage

```
python -m simulator.reference --output-dir simulator/_out/reference
python -m simulator.batch --output-dir simulator/_out/batch
python -m simulator.incremental --as-of 2025-01-06T12:00:00+10:00
python -m simulator.incremental --as-of 2025-01-08T00:00:00Z --fixed-clock 2030-01-01T00:00:00Z
```

| Flag | Default | Meaning |
|---|---|---|
| `--as-of ISO` | required | Extraction time. Must carry a UTC offset (`Z` or `+hh:mm`); normalised to UTC. |
| `--config PATH` | `simulator/incremental/default_incremental.toml` | Alternate loader config. |
| `--state-dir`, `--batch-source-dir`, `--reference-source-dir`, `--bronze-output-dir`, `--bronze-config` | config | Override the config paths. |
| `--bad-source-policy strict\|preserve` | config (`strict`) | See "Bad source policy". |
| `--late-arrival-policy fail\|record` | config (`fail`) | See "Watermark semantics". |
| `--fixed-clock ISO` | wall clock | Virtual clock for reproducible runs: time stands still, and retry backoff advances it without sleeping. |
| `--pipeline-run-id ID` | `ipr-<seq>-<as_of>` | Explicit pipeline run id. Refused if it already exists. |
| `--log-file PATH` / `--log-level LEVEL` | stderr / `INFO` | JSON-lines logs, appended to. |

Stdout gets one JSON summary line: `status`, `pipeline_run_id`, `as_of`,
`summary` (the pipeline audit record, relative to the state dir),
`late_rows_skipped`, `warnings`, and per dataset its `status`, `attempts`,
`watermark_after`, `last_as_of_after`, `extracted_count`,
`late_rows_skipped` and `error_type`.

| Exit code | Pipeline status | Meaning |
|---|---|---|
| **0** | `SUCCEEDED` | Every dataset succeeded (including `SKIPPED_UNCHANGED` and `UP_TO_DATE`). |
| **1** | `PARTIAL` / `FAILED` | At least one dataset failed; failed datasets keep their control state. |
| **2** | `REFUSED` | Refused before anything was written: missing or naive `--as-of`, an `--as-of` earlier than a planned dataset's last committed `as_of` or later than the clock, invalid config, a reused `--pipeline-run-id`, or a control record of another schema version. |
| **3** | `SUCCEEDED_WITH_WARNINGS` | Every dataset succeeded, **but** under `late_arrival_policy = "record"` late rows were skipped (or source rows vanished). Never 0: a scheduler must not treat skipped rows as a clean run. |

The library entry point is
`simulator.incremental.run_incremental_load(as_of, config=..., clock=...)`.

### Load modes

| Mode | Datasets | Source | Window | Extract |
|---|---|---|---|---|
| `full` | DE-01 reference entities `depots`, `stops`, `routes`, `vehicles` (each listed in config with `source_file` and `primary_key`) | `<entity>.json` JSON array in `reference_source_dir`, as written by `python -m simulator.reference` | The whole snapshot as of `as_of` | One canonical JSON line per element (sorted keys, compact). Every element must be an object with `schema_version`. If the snapshot is byte-identical to the last committed one, the run is `SKIPPED_UNCHANGED`: no Bronze run, `last_as_of` moved to `as_of`. Full datasets have no watermark (`null`). |
| `incremental` | Every `DATASET_CATALOGUE` dataset | `<dataset>.jsonl` listed in the DE-04 `manifest.json` in `batch_source_dir` | `watermark < updated_at <= as_of`, where `watermark` = maximum committed `updated_at`; `updated_at <= as_of` while nothing is committed | The selected source lines, byte-for-byte, in source order. |

Order is the config order (`[[incremental.datasets]]`), followed by every
catalogue dataset the config doesn't list, in catalogue order
(`include_unlisted_catalogue_datasets = true`). A new DE-04 dataset, such as
DE-10's `vehicle_depot_assignments`, is therefore picked up with no config
change. `exclude_datasets` skips catalogue datasets. The watermark column is
always the catalogue's `watermark_column`. The order is processing order
only (see FU-02 below).

Every extract is a DE-04-shaped directory,
`<state_dir>/extracts/<dataset>/<bronze_run_id>/`, holding `<dataset>.jsonl`
plus a `manifest.json` with `row_count`, `sha256`, the primary key, the
watermark/partition columns, the min/max watermark, the window and
`preserved_bad_rows` (see "Bad source policy"). Bronze verifies the file
against this manifest before it commits. Extracts are kept as the audit
trail of exactly what was handed to Bronze.

### Watermark semantics (max committed `updated_at`)

Each incremental dataset has its own control record with two separate
values:

- **`watermark`**: the **maximum `updated_at` among the rows actually
  committed to Bronze** for that dataset (`null` until a row is committed).
  Invariant: every source row with `updated_at <= watermark` that was in
  the source when it was extracted is in Bronze exactly once.
- **`last_as_of`**: the `as_of` of the last committed run. It drives
  idempotency and the refusal rules, never row selection.

The window for a run is `watermark < updated_at <= as_of`
(`updated_at <= as_of` while the watermark is `null`). After Bronze commits
the window, `watermark` moves to the maximum `updated_at` the window
contained, and `last_as_of` moves to `as_of`. A row whose `updated_at`
equals a cut-off goes into the window ending at that cut-off, and never
into the next one.

**Why the maximum committed `updated_at` (Lead decision D1, after the QA
FAIL of PR #17).** The first version persisted the window upper bound
(`as_of`). DE-04 back-dates `updated_at` for `schedules` and
`schedule_stop_times` (published `publish_lead_days` = 14 days before
service) and for `vehicle_depot_assignments` (`notice_days` = 21). A DE-04
source regenerated with more `service_days` therefore adds rows whose
`updated_at` is *before* an `as_of` that was already loaded. QA's repro:
load the 7-day default source at 2025-01-12, regenerate with
`service_days = 10`, and run at 2025-01-16. The upper-bound watermark left
510 schedules, 6,528 stop times and 3 assignments unloadable for good.
DE-04 assigns `updated_at` per dataset monotonically in service date. For
every dataset, the rows a regeneration adds have `updated_at` strictly
greater than every existing row of *that* dataset. A per-dataset
maximum-`updated_at` watermark therefore sees them. The regression test
`test_qa_growth_repro_lands_every_row_at_realistic_cuts` is that exact
scenario, plus a run at 2025-01-17, checked by an oracle computed only
from the DE-04 files.

**Run ids stay pure.** The Bronze run id is
`inc-<dataset>-<watermark|initial>-<as_of>`. That is a pure function of
(dataset, control state, `as_of`), so crash recovery is unchanged: a rerun
with the same `as_of` rebuilds the identical extract under the identical
run id (see "Idempotency").

**Empty windows.** An empty window leaves `watermark` unchanged and moves
`last_as_of` to `as_of`. It **still commits an empty Bronze run** under its
own run id (the low stays the same and the `as_of` differs, so each empty
window gets a distinct id). Reasons:

- there is one commit-then-advance path, so one crash-recovery mechanism,
  not two;
- Bronze holds a record of every window the loader processed, so the §15
  "watermark before/after" and the `last_as_of` advance are always backed by
  a Bronze commit;
- this matches a Data Factory Copy activity that copies 0 rows and still
  logs a run.

The cost is one empty Bronze manifest per dataset per idle run. A rerun of
the same `as_of` is `UP_TO_DATE` (no Bronze call, control byte-identical).

**`as_of` rules.** An `as_of` earlier than any planned dataset's
`last_as_of` refuses the whole run before anything is written (no audit
directory, no control or Bronze change). This holds even when `as_of` is
later than that dataset's `watermark`: windows already processed are never
re-cut. An `as_of` equal to `last_as_of` is a recorded `UP_TO_DATE` no-op.
An `as_of` later than the clock is refused, because the loader cannot vouch
for the completeness of the future.

**Ties: the stated assumption.** Once a value has been committed as a
dataset's watermark, the source never adds another row with that **same**
`updated_at` (or any earlier one). A strict `>` window cannot see such a
row. An inclusive `>=` window would re-extract every row at the watermark,
which means duplicates. This is the standard high-watermark contract: the
source assigns `updated_at` at commit time, monotonically.

**Detection (late-arrival recount).** Each control record stores
`source_rows_at_or_below_watermark`: the number of good source rows with
`updated_at <= watermark` when that watermark was committed. Every later
window recounts them in the current source. A positive delta means rows
appeared at or behind the watermark, including a tie. A negative delta
means rows vanished. Either way:

- `late_arrival_policy = "fail"` (**default**): `LateArrivalError`; the
  dataset fails with its control unchanged.
- `late_arrival_policy = "record"`: the late rows are **not** extracted,
  but this is never hidden (decision O1). The attempt audit record has
  `late_rows_skipped` / `source_rows_vanished`. The control record keeps a
  running `late_rows_skipped_total`. `pipeline.json` has a
  `late_rows_skipped` total and a `warnings` list, and the run status is
  `SUCCEEDED_WITH_WARNINGS` (CLI exit code 3). Skipped rows are reported
  once, in the run that skipped them; the recount baseline is then reset.

The check is count-based, so a row replaced by a different row with the
same count is not detected. Tested: a tie appended at the watermark fails
the dataset (`test_tie_at_the_watermark_appearing_after_commit_fails_loudly_by_default`),
and a row strictly below the watermark fails it (`fail`) or is surfaced
(`record`, exit 3).

**Rows between `watermark` and `last_as_of` are recovered, not lost.** A
row that appears after a run, with `watermark < updated_at <= last_as_of`,
falls inside the next window, and the audit record counts it in
`late_rows_recovered`. This is the growth case above. In production the
usual extra margin is extracting at `now - safety lag`.

**Keep the DE-04 config fixed across an incremental series.** Lead note
from QA of PR #14 (DE-10): changing the DE-04 generator config, such as
`[fleet_changes]`, or regenerating an `_out/batch` produced before DE-10,
rewrites existing rows (for example `depot_id` on
`schedules`/`historical_trips`) *without changing their `updated_at`*. An
`updated_at` watermark cannot see such an in-place change, and a
count-based check cannot see it either. Across a series, only
`service_days` may grow; DE-04 guarantees growth is prefix-stable. To
change anything else, reset the series: delete `<state_dir>/control/`, use
a fresh Bronze store, then regenerate the source. Deleting only
`control/` is not enough: it is refused (see "Idempotency"). The growth
tests (`test_incremental_watermark.py`, `test_incremental_properties.py`)
regenerate with an identical config apart from `service_days`.

**Control schema.** Control records are `control_schema_version` 2.0.0.
A 1.0.0 record (written by 9a7eac2, whose watermark was the window upper
bound) is never reinterpreted. The run is refused before anything is
written (`ControlSchemaError`, exit 2), and the message tells the operator
to reset the series.

### Bad source policy

A source line is *bad* if it is not UTF-8, not JSON (including truncated or
`NaN`), not a JSON object, or has duplicate keys. For `incremental` it is
also bad if it has a missing or `null` `updated_at`, or one that is not an
ISO-8601 string with an offset. For `full`, an element that is not an object
or has no `schema_version` is bad. Bad lines are never skipped silently.

| `bad_source_policy` | Behaviour |
|---|---|
| `strict` (**default**) | Any bad line fails the dataset before anything is landed. The watermark is unchanged; the audit record has `rejected_count` and `bad_source_rows_by_reason`. Bronze also runs with `parse_error_policy = "fail"`. Default because a broken source is a source incident to fix, not data to carry forward. |
| `preserve` | Each bad line is landed **once**, byte-for-byte, in the next extract. Bronze runs with `parse_error_policy = "preserve"`, and lines are tracked by raw sha256 plus occurrence count in `preserved_bad_lines`. A line that is not UTF-8, not JSON, has duplicate keys or is not a JSON object becomes a Bronze parse-error record (`bronze_parse_error_records`). A JSON object with a missing, unparseable or naive (no offset) `updated_at` is well-formed to Bronze, so it **lands as a normal Bronze record**. Both kinds are counted in `bad_rows_preserved` and are part of the reconciliation identity, and both are **listed** (see below). |

**Preserved rows are listed (Lead decision D2).** Under `preserve`, the
attempt audit record and the extract `manifest.json`
(`datasets.<dataset>.preserved_bad_rows`) both list every bad line the
extract carries, in extract order. Each entry has:

- `extract_line`: the 1-based line number within the extract file;
- `source_line`: the line number in the source;
- `reason`;
- `sha256`: of the exact line bytes;
- `bronze_outcome`: `bronze_parse_error_record` or
  `stored_record_requires_quarantine`.

The listing is deterministic: the same window produces the same list.
**DE-12 must quarantine every row listed with
`bronze_outcome = "stored_record_requires_quarantine"`.** Bronze has stored
these rows as ordinary records, and only this listing (or DE-12's own
strict validation of `updated_at`) tells them apart. `strict` stays the
default.

An empty source file, or an empty window, is a recorded success with 0
rows. Bronze still commits an empty run (see "Watermark semantics").

### Idempotency, crash safety and retries

The Bronze run id is derived from (dataset, control state, `as_of`):
`inc-<dataset>-<watermark|initial>-<as_of>` or `full-<dataset>-<as_of>`, with
compact UTC timestamps such as `20250106T000000Z`. It is at most 121
characters, within DE-05's run-id alphabet. This is designed around Bronze's
actual semantics rather than against them: the same run id plus the same
content is a no-op, and a committed run is never overwritten.

| Situation | Behaviour |
|---|---|
| Rerun, same `as_of`, everything committed | Every dataset is `UP_TO_DATE`: no extract, no Bronze call, control byte-identical. |
| Crash after the extract is written | Rerun rewrites the identical extract and Bronze commits the run (`COMMITTED`). |
| Crash inside Bronze's commit (partitions published, no manifest) | Rerun: Bronze removes the orphans (`removed_uncommitted_paths`) and commits (`COMMITTED`). |
| Crash after the Bronze commit, before the watermark write | Rerun: same run id and content, so Bronze returns `ALREADY_COMMITTED` and the watermark advances. |
| Crash after the watermark write, before the audit record | Rerun: `UP_TO_DATE`. The crashed attempt has no audit record (see limitations). |
| A *later* `as_of` after a crash past the Bronze commit (for example the scheduler's next trigger) | **Adopted, only if all of these hold:** (1) a control record exists for the dataset; (2) exactly one committed loader run is not listed in it, and that run is exactly the next window: its low equals the committed watermark, its high is after `last_as_of` and before `as_of`, and its run id is the one that window derives; (3) its extract on disk matches the Bronze run (sha256 and record count); (4) **re-selecting that window from the current source** gives the identical sha256 and count to what Bronze committed. Control then advances: `watermark` to the window's maximum `updated_at`, and `last_as_of` to its high (`recovered_bronze_run_id` in the audit record). The normal window up to `as_of` is then loaded. Nothing is re-extracted, so nothing is duplicated. |
| Next-window run whose extract disagrees with Bronze, or whose window re-selected from the current source differs from what Bronze committed (an in-place source change) | Refused for that dataset with `ControlStateMismatchError` ("does not match its extract" / "does not match the current source"). Control is unchanged and nothing is landed. |
| Unrecorded run that isn't the next window (extract missing, more than one, wrong low/high) | Refused with `ControlStateMismatchError`, because a different window would land overlapping rows. The message says to rerun with the interrupted run's `as_of` (the last part of its run id). If the other datasets have already moved past that `as_of`, the whole-pipeline rerun is refused, so rerun with a config listing only that dataset (tested). |
| Control record lost or deleted (Bronze kept) | **Never adopted automatically.** Every dataset with committed loader runs fails with `ControlStateMismatchError` ("no control record exists"), and nothing is landed. QA's repro is tested: delete `control/`, regenerate DE-04 with `relief_pool_size = 5`, run a later `as_of`. Recovery is an operator action. If only one window was committed and the source is unchanged, rerun that window's `as_of` for that dataset alone: it returns `ALREADY_COMMITTED` and rebuilds the record (tested). Otherwise reset the series. The same applies to a *first* run interrupted after its Bronze commit and then run with a later `as_of`. |
| Control lists a Bronze run Bronze doesn't have (Bronze store lost or replaced) | `ControlStateMismatchError`. |
| Reference snapshot unchanged | `SKIPPED_UNCHANGED`: no Bronze run, so no `DuplicateInputError` and no duplicate rows. |
| Reference snapshot changed, including back to an earlier version (A to B to A) | New `full-*` run. Full loads pass `allow_replay=True`, so Bronze accepts a revert to content it has seen before and flags it in `replay_of`. Incremental loads never allow replay: Bronze's content-hash guard stays a safety net against overlapping windows. |
| Transient error (`TimeoutError`, `ConnectionError`, `simulator.incremental.TransientError`) | Retried up to `retry.max_attempts`, waiting `min(max_backoff_seconds, initial_backoff_seconds * backoff_multiplier ** (n - 1))` after failed attempt n on the injected clock. Every attempt is audited. When attempts run out, the dataset is `FAILED` with its watermark unchanged. |
| Any other error (bad source, manifest mismatch, reconciliation, Bronze refusal) | Not retried. Dataset `FAILED`, watermark unchanged, the other datasets continue, and the pipeline is `PARTIAL`. A rerun with the same `as_of` processes only the failed dataset (the rest are `UP_TO_DATE`). |

Rerunning after a partial failure ends with Bronze data, control records
and extracts byte-identical to an uninterrupted run (fixed clock, relative
paths). Bronze run manifests are identical too, **except** for a dataset
that failed *inside* Bronze. Since DE-09, Bronze keeps the failed attempt,
so that run's manifest gains a `previous_attempts` entry on the rerun. Both
cases are tested (`test_partial_failure_then_rerun_matches_an_uninterrupted_run_byte_for_byte`,
`test_failure_inside_bronze_then_rerun_keeps_data_control_and_extracts_identical`).

### Reconciliation identity

Checked twice per attempt:

```
source_rows_in_window + bad_rows_preserved == extracted_count == bronze_records_written
```

`source_rows_in_window` is counted while scanning the source.
`extracted_count` is re-counted from the extract file on disk, and its
sha256 is compared with the selected lines. `bronze_records_written` comes
from the Bronze run manifest. Under the default strict policy,
`bad_rows_preserved` is 0. The first two terms are checked **before** Bronze
is called, so a lossy extract never reaches Bronze; all three are checked
before the watermark write. On failure the dataset is `FAILED`
(`ReconciliationError`) and the watermark does not move. Bronze's own
accounting identity (DE-05) continues the chain, and DE-12 will continue it
into Silver.

### Control and audit stores

```
<state_dir>/control/<dataset>.json                               watermark control record (atomic replace)
<state_dir>/audit/<pipeline_run_id>/<dataset>/attempt-NNN.json   one immutable record per attempt
<state_dir>/audit/<pipeline_run_id>/pipeline.json                immutable pipeline summary
<state_dir>/extracts/<dataset>/<bronze_run_id>/                  extract handed to Bronze
```

Control record fields (`control_schema_version` 2.0.0): `dataset`, `mode`,
`watermark_column`, `watermark_semantics` (`max_committed_updated_at`, or
`none_full_snapshot` for full datasets), `watermark` (maximum committed
`updated_at`; `null` for full datasets or before the first committed row),
`last_as_of`, `last_window`, `last_bronze_run_id`,
`committed_bronze_run_ids`, `rows_landed_total`, `last_extract_sha256`,
`snapshot_sha256` (full), and for incremental datasets
`source_rows_at_or_below_watermark` (the late-arrival recount baseline),
`preserved_bad_lines` and `late_rows_skipped_total`.

An audit record is written once: a temp file is hard-linked to its final
name, which fails if the name already exists. It is never overwritten and
never appended to. Every pipeline run gets a new directory: `ipr-<seq>-<as_of>`
by default, and a reused id is refused. Attempt record fields (PROJECT.md
section 15): `pipeline_run_id`, `dataset`, `mode`, `attempt`,
`max_attempts`, `status` (`COMMITTED`, `ALREADY_COMMITTED`,
`SKIPPED_UNCHANGED`, `UP_TO_DATE`, `FAILED`), `started_at`, `ended_at`,
`duration_seconds`, `as_of`, `window_low`, `window_high`,
`watermark_before`, `watermark_after`, `last_as_of_before`,
`last_as_of_after`, `window_max_watermark`, `late_rows_recovered`,
`late_rows_skipped`, `source_rows_vanished`, `watermark_semantics`,
`bad_source_policy`, `source_file`, `source_sha256`,
`source_manifest_check`, `source_rows_total`, `source_blank_lines`,
`source_rows_in_window`, `source_rows_before_window`,
`source_rows_after_window`, `bad_source_rows`,
`bad_source_rows_by_reason`, `bad_rows_preserved`, `preserved_bad_rows`,
`rejected_count`, `late_arrival_check`, `extract_dir`, `extract_sha256`,
`extracted_count`, `bronze_run_id`, `bronze_status`, `bronze_manifest`,
`bronze_records_written`, `bronze_parse_error_records`,
`bronze_replay_of`, `recovered_bronze_run_id`, `reconciliation`, `error`
(`type`, `message`, `retryable`, `will_retry`) and `next_backoff_seconds`
(`audit_schema_version` 2.0.0). The pipeline summary holds the run status
(`SUCCEEDED` / `SUCCEEDED_WITH_WARNINGS` / `PARTIAL` / `FAILED`), the
policies, status counts, `late_rows_skipped` (total), `warnings` (one entry
per dataset and type: `late_rows_skipped` or `source_rows_vanished`, with a
count) and one result per dataset.

**No absolute paths (FU-04).** Control and audit files only hold paths
relative to the state dir (`extract_dir`) or the Bronze store
(`bronze_manifest`), and source *file names*. Error messages are passed
through a sanitiser that replaces the configured directories, the working
directory and the home directory with `<state_dir>`, `<bronze_output_dir>`,
`<cwd>`, `~` and so on. Tested with absolute configured paths and an
injected error that quotes an absolute path. Since DE-09, Bronze run
manifests written through the loader are path-free as well: `input_files[].path`
is the extract file's basename, and error text, including a failed attempt
carried in `previous_attempts`, is redacted. This is tested with absolute
configured paths (`test_bronze_manifests_written_through_the_loader_are_path_free`).

**Run id on every log line (FU-09 not repeated).** Every log line of the
loader *and of Bronze while it is called by the loader* carries
`pipeline_run_id`, plus `dataset` and `attempt` where they apply. This is
done with a context logger adapter that is passed to Bronze. `--log-file`
appends, so reused log files stay attributable by run id. Tested in-process
and through the CLI.

### FU-02 tolerance

No cross-dataset ordering or referential check is imposed. A
`historical_trips` row whose `updated_at` precedes that of the
`service_disruptions` row it references is landed in its own (earlier)
window like any other row. `event_date` is part of the byte-for-byte copied
line and is never re-derived. Tested with a cut-off exactly at such a
trip's `updated_at`.

### Configuration: `simulator/incremental/default_incremental.toml`

Validated by `simulator/incremental/config.py` (pydantic, unknown keys
rejected). Keys: `state_dir`, `batch_source_dir`, `reference_source_dir`,
`bronze_output_dir` (all under the gitignored `simulator/_out/`), optional
`bronze_config`, `bad_source_policy`, `verify_source_manifest` (default
`true`: compare each DE-04 file with its manifest before extracting; a
mismatch fails the dataset), `source_manifest_file`, `late_arrival_policy`,
`incremental_run_id_prefix` / `full_run_id_prefix`,
`include_unlisted_catalogue_datasets`, `exclude_datasets`,
`[incremental.retry]` (`max_attempts`, `initial_backoff_seconds`,
`backoff_multiplier`, `max_backoff_seconds`) and `[[incremental.datasets]]`
(`name`, `mode`, plus `source_file`/`primary_key` for `full`). The
`parse_error_policy` of the Bronze config is overridden from
`bad_source_policy`. No new dependency.

### Mapping to Fabric

Not implemented or connected. This is the intended translation once the
Fabric trial is active (ADR-0004):

| Local (DE-11) | Fabric equivalent |
|---|---|
| `run_incremental_load` over the plan | A Data Factory pipeline with a ForEach over a dataset list (Lookup on a config table), with sequential or batched concurrency. |
| `control/<dataset>.json` | An `etl_watermark` control table (Warehouse table or Lakehouse Delta table), keyed by dataset, with `watermark` (max committed `updated_at`) and `last_as_of` columns. It is read by a Lookup activity and updated by a Stored procedure / Script activity *after* the Copy succeeds, setting `watermark = MAX(updated_at)` of the copied rows (unchanged when 0 rows). |
| Window `watermark < updated_at <= as_of` | The Copy activity source query `WHERE updated_at > @{watermark} AND updated_at <= @{as_of}`, with `as_of` = trigger time or tumbling-window `windowEnd` (minus a safety lag). The classic high-watermark pattern. |
| Late-arrival recount | A Lookup `SELECT COUNT(*) WHERE updated_at <= @{watermark}` compared with the stored baseline before the Copy. |
| `full` + `SKIPPED_UNCHANGED` | A Copy activity for reference tables. Change detection via a Get Metadata (`contentMD5`) or a hash comparison before the copy. |
| Extract directory + DE-04 manifest | Files landed in Lakehouse `Files/landing/<dataset>/<run id>/` before the Bronze notebook loads them. |
| `ingest_batch` under a deterministic run id | The DE-05 Bronze notebook with `run_id` = `<dataset>-<window>` passed as a parameter; idempotent Delta append (`txnAppId`/`txnVersion`) or a pre-check against `bronze_run_audit`. |
| Retry + backoff | Activity retry count and interval settings. Only transient failures are retried. |
| Per-attempt audit records + `pipeline.json` | An `etl_run_audit` table written by a Script activity on success and failure paths, with the pipeline `RunId` as `pipeline_run_id`. |
| `PARTIAL` + rerun only failed datasets | ForEach continues on failure. A rerun re-reads the watermarks, so finished datasets are no-ops. |
| `ControlStateMismatchError` guard | A pre-copy Lookup comparing `etl_watermark` with `bronze_run_audit`. |

### DE-11 tests

`pytest simulator -q` includes (all expected counts recomputed from the
source files and manifests, never hardcoded):

- `test_incremental_config.py`: catalogue-driven plan and order,
  exclusions, every validation error, backoff formula, deterministic and
  valid Bronze run ids for worst-case names.
- `test_incremental_load.py`: initial load, incremental window, full
  reference load, DE-04-shaped extract verified by Bronze, **commit then
  advance** (control observed at every stage), rerun no-op, empty window,
  empty source, unchanged / changed / reverted reference snapshot, FU-02,
  `as_of` offset normalisation.
- `test_incremental_windows.py`: cut-off equal to an `updated_at` value,
  an `as_of` before `last_as_of` refused with a byte-identical state and
  Bronze tree, naive and future `as_of`, `UP_TO_DATE`.
- `test_incremental_watermark.py` (D1): **QA's growth repro**, 7-day
  default source at 2025-01-12, regenerated with 10 days, loaded at
  2025-01-16 and 2025-01-17, with all 8 datasets checked against an oracle
  from the DE-04 files. It fails on 9a7eac2. Also: three cut sequences with
  growth between cuts at realistic cuts (exact values with a repeat,
  beyond-horizon cuts with empty windows, day boundaries and between-value
  cuts), checked after every cut; watermark = max committed `updated_at`
  with `last_as_of` kept separately; empty-window semantics and
  idempotency; an `as_of` between watermark and `last_as_of` refused; a
  tie at the watermark detected; a row between watermark and `last_as_of`
  recovered; a 1.0.0 control record refused.
- `test_incremental_failures.py`: retries with backoff on the injected
  clock, retry exhaustion, non-transient errors not retried, partial failure
  then rerun byte-identical to an uninterrupted run, a failure *inside*
  Bronze then rerun (only that run's manifest gains `previous_attempts`),
  Bronze manifests path-free through the loader, strict and preserve
  bad-source policies (including the D2 `preserved_bad_rows` listing in
  the audit record and extract manifest), manifest mismatch, missing source,
  both reconciliation checks, a late row failing the dataset (`fail`) or
  surfaced as `SUCCEEDED_WITH_WARNINGS` with `late_rows_skipped` (`record`,
  O1).
- `test_incremental_crash.py`: four crash points x initial/incremental
  window, each rerun matching an uninterrupted run (Bronze records, record
  count, control bytes). Also: the same-`as_of` recovery path; adoption by
  a later `as_of` matching an uninterrupted three-window run; adoption
  refused when the current source no longer matches the committed window;
  **QA's control-loss repro** (control deleted, DE-04 regenerated with
  `relief_pool_size = 5`) refused for every dataset; an interrupted first
  run refused at a later `as_of` and recovered by a single-dataset rerun;
  a tampered or missing extract refused; a lost control record refused,
  then rebuilt by its own `as_of` (one window) or refused (several
  windows); a missing Bronze run.
- `test_incremental_properties.py`: no loss / no duplicates after every
  cut-off of five cut sequences (exact values with a repeat, between-value
  cuts with empty windows, mixed, single load, day boundaries), each row in
  the window containing its `updated_at`, and a source regenerated with
  more `service_days` (same config otherwise) at cuts below the horizon.
- `test_incremental_audit.py`: section 15 fields, the identity, records
  never overwritten, one record per retry attempt, no absolute paths,
  `pipeline_run_id` on every log record (loader and Bronze).
- `test_incremental_cli.py`: two CLI processes byte-identical (state and
  Bronze) with `--fixed-clock`, exit codes 0/1/2/3, config refusal, an
  old control schema refused, the policy flag, an appended log file
  attributable by run id.

### Known limitations (DE-11)

- **Not connected to Fabric** (see status above). Local filesystem only.
- **Single writer.** One loader process per `state_dir` and Bronze store at
  a time. There is no locking: two concurrent loaders could allocate the
  same pipeline sequence or race on a control record. Concurrent writers are
  out of scope.
- **Monotonic `updated_at` assumed (ties).** A source row added later with
  an `updated_at` at or below a dataset's committed watermark, including a
  tie with it, is never extracted by a window. It is detected by the
  count-based recount (fail by default, or surfaced with exit 3 under
  `record`). A row *replaced* by another row with the same count, and
  in-place changes that keep `updated_at`, are not detected at all. Keep
  the DE-04 config fixed across a series (only `service_days` may grow).
- **A hard crash leaves no audit record for that attempt.** Records are
  written at the end of an attempt. The rerun's record (for example
  `ALREADY_COMMITTED` or `UP_TO_DATE`) and Bronze's
  `removed_uncommitted_paths` are the evidence of recovery.
- **Automatic recovery covers one interrupted window, from an unchanged
  source.** A crash past the Bronze commit is adopted by a later run only
  with a control record present, its extract on disk matching Bronze, and
  the window re-selected from the current source identical to what Bronze
  committed. If the source changed inside that window in the meantime
  (growth included), adoption is refused. A same-`as_of` rerun is then also
  refused by Bronze (same run id, different content). The operator has to
  restore the source the window was extracted from, or reset the series.
  There is no repair command.
- **Lost control is never rebuilt automatically.** Only a single-dataset
  rerun of the window's own `as_of` (one committed window, unchanged
  source) rebuilds it. Otherwise reset the series.
- **Empty windows commit empty Bronze runs**: one empty Bronze manifest per
  incremental dataset per idle run.
- **Sources are read fully into memory**, which is fine at local scale.
  Each attempt reads all Bronze manifests for the consistency guard (linear
  in run count).
- **Preserved bad lines are tracked by content.** A byte-identical bad line
  that reappears beyond the number already landed is landed again.
- **Fixed UTC offset / no daylight saving**, inherited from DE-04.
