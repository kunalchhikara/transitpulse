# simulator/

Owned by: Data Platform Engineer (`.claude/agents/data-engineer.md`).

Purpose: deterministic synthetic event generator, synthetic reference/domain data
(vehicles, routes, stops, depots, schedules), incident simulation logic, and local
Bronze/Silver/Gold transformation logic that can be tested without Fabric, per
`PROJECT.md` sections 5-13, 19.

Status: DE-01 (Synthetic Transport Domain Model & Schemas), DE-02
(Deterministic Simulator Core Engine), DE-03 (Incident Injection Scenarios in
Simulator), DE-04 (Synthetic Batch & Historical Datasets) and DE-05 (Local
Bronze Layer Ingestion Logic) implemented. Remaining scope (Silver/Gold) is
not yet implemented — see `docs/BACKLOG.md` (DE-06..DE-07).

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
| 2 | Duplicate-event injection | `[incidents.duplicate]` `enabled = true` | `--enable-duplicate` | With probability `probability_per_tick`, replaces the current tick's payload with a verbatim (byte-identical) copy of a payload already delivered within the last `replay_window` deliveries, same `event_id` included. |
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
`passenger_journeys`, `service_disruptions` and the optional
`weather_observations`. It only *produces* datasets; incremental-load/watermark
engine logic is DE-05+ scope.

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

Exact volume formulas (verified by `simulator/tests/test_batch_volume.py`):

- `schedules` = `historical_trips` = routes x `service_days` x
  (floor((service_end - service_start) / headway) + 1)
- `schedule_stop_times` = sum of route stop counts over all scheduled trips
- `historical_telemetry` = sum of `stops_served` over all historical trips
- `passenger_journeys` = per operated trip (>= 2 stops served), a draw in
  [`min_journeys_per_trip`, `max_journeys_per_trip`] capped at vehicle capacity
- `service_disruptions` = `disruptions.per_day` x `service_days`
- `weather_observations` = depots x `service_days` x (1440 / interval), or 0 when disabled

### Determinism contract

Same batch config (incl. `batch.seed`) + same DE-01 scale config ->
byte-identical files, in-process and across two separate OS-process CLI runs
(`simulator/tests/test_batch_determinism.py`). All randomness comes from
independent `random.Random` streams derived from the seed plus a stable key
(`simulator/batch/rng.py`: per route timetable, per service day for
disruptions, per trip for operations/journeys/telemetry, per depot-day for
weather); string seeds are SHA-512-hashed by Python, so they do not depend on
`PYTHONHASHSEED`. Nothing reads wall-clock time. Because streams are keyed per
day/trip, output is **prefix-stable**: growing `service_days` appends new days
without changing a byte of existing ones - the behaviour a watermark-based
incremental extract expects from a source system.

The committed sample in `simulator/batch/sample/output/` (1 day, 2 routes, ~155
rows, ~70 KB) is generated from `sample/sample_batch.toml` +
`sample/sample_scale.toml`; a test regenerates it and requires a byte-identical
match. Regenerate it after an intentional generator change with:

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
- **Passengers.** Journeys board/alight at served stops of the trip
  (boarding before alighting). Telemetry `occupancy` at stop *k* is exactly the
  number of journeys boarded before *k* and alighting at or after *k*, so
  occupancy, `passenger_boardings` and `peak_occupancy` reconcile by
  construction and never exceed vehicle capacity.

### Common columns, watermark and partition

Every dataset row has `schema_version`, `updated_at` (aware UTC; the
**watermark column**: when the record became final in the source system) and
`event_date` (the **partition column**: local operating/service date for
trip-derived datasets, local observation date for weather). A later
incremental load selects `updated_at > last_watermark` and advances the
watermark only after success (PROJECT.md section 11). `updated_at` semantics:
timetables are published `publish_lead_days` before service (midnight local);
trips settle `trip_record_settlement_minutes` after they end; journeys settle
`settlement_lag_minutes` after alighting; disruptions close
`disruption_record_settlement_minutes` after resolution; telemetry and
weather are final when observed (`updated_at` = `event_time` / `observed_at`).

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
| `depot_id` | str | FK -> DE-01 `depots.depot_id`; the vehicle's home depot. |
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
| `depot_id` | str | FK -> `depots.depot_id` (matches schedule). |
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
| `depot_id` | str | FK -> `depots.depot_id`; route home depot (ROUTE) or vehicle home depot (TRIP). |
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

### DE-04 tests

`pytest simulator -q` runs, among the rest of the suite:

- `test_batch_config.py` - default/sample config load; inconsistent configs rejected.
- `test_batch_volume.py` - exact count formulas at default and overridden configs; DE-01 rescaling; infeasible config fails loudly.
- `test_batch_referential_integrity.py` - every catalogued FK resolves (default, delay-only and committed sample data); PKs unique; trips/stop-times/telemetry/journeys/disruptions mutually consistent.
- `test_batch_consistency.py` - aggregates reconcile with detail rows; occupancy derivation; strictly increasing times; no vehicle double-booking; each disruption effect; watermark semantics.
- `test_batch_schemas.py` - per-dataset schema round trip, required/unknown-field negatives, TelemetryEvent conformance, `fact_service_incident` sufficiency, README catalogue coverage.
- `test_batch_determinism.py` - in-process and cross-process byte identity, seed sensitivity, prefix stability, committed-sample reproducibility, idempotent rerun, manifest reconciliation.

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
- **No SCD history yet.** Vehicles keep DE-01's single home depot / route
  assignment; vehicle-to-depot change history for `dim_vehicle` SCD2 is a
  later story.
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
notebook" below for how it is meant to carry over.

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
| Same `run_id`, previous attempt `FAILED` or crashed | Retried from scratch (orphans removed first). |
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
`run_id`, `status` (`COMMITTED`/`FAILED`), `mode`, run-level `source`,
`ingestion_time`, `started_at`, `ended_at`, `duration_seconds`,
`parse_error_policy`, `allow_replay`, `replay_of`, `duplicate_input_files`, `input_fingerprint`,
`input_files` (per file: name, path as given, table, source, sha256, size and
all counters, `parse_errors_by_code`, `unterminated_final_line`, and for batch
`source_manifest_check`), `tables` (per table: committed path, sha256 of the
data file, counters), `totals`, `ignored_files`, `removed_uncommitted_paths`,
`source_reconciliation` and `error`.

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
- Run order in `read_table` is by (`ingestion_time`, `run_id`); manifests
  record input paths as given on the command line.
- No retention/compaction of old run partitions.
