# simulator/

Owned by: Data Platform Engineer (`.claude/agents/data-engineer.md`).

Purpose: deterministic synthetic event generator, synthetic reference/domain data
(vehicles, routes, stops, depots, schedules), incident simulation logic, and local
Bronze/Silver/Gold transformation logic that can be tested without Fabric, per
`PROJECT.md` sections 5-13, 19.

Status: DE-01 (Synthetic Transport Domain Model & Schemas), DE-02
(Deterministic Simulator Core Engine) and DE-03 (Incident Injection
Scenarios in Simulator) implemented. Remaining scope (batch/historical
datasets, Bronze/Silver/Gold) is not yet implemented — see
`docs/BACKLOG.md` (DE-04..DE-07).

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
  plus `min_layover_minutes`; at default scale no vehicle is double-booked.
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
| `affected_trip_count` | int | Trips delayed (ROUTE) or terminated/cancelled (TRIP); reconciles with `historical_trips`. |
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
- Weather is independent per observation (no persistence/fronts) and not
  causally linked to `SEVERE_WEATHER` disruptions.
