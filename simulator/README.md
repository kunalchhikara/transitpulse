# simulator/

Owned by: Data Platform Engineer (`.claude/agents/data-engineer.md`).

Purpose: deterministic synthetic event generator, synthetic reference/domain data
(vehicles, routes, stops, depots, schedules), incident simulation logic, and local
Bronze/Silver/Gold transformation logic that can be tested without Fabric, per
`PROJECT.md` sections 5-13, 19.

Status: DE-01 (Synthetic Transport Domain Model & Schemas) implemented. Remaining
scope (telemetry generation loop, incidents, batch/historical datasets,
Bronze/Silver/Gold) is not yet implemented — see `docs/BACKLOG.md` (DE-02..DE-07).

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
- `simulator/tests/` - pytest suite (determinism, count-matches-config,
  coordinate-bounding-box, referential integrity, schema-validation positive
  and negative cases). Run with `pytest simulator -q` from the repo root.

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

## IMPLEMENTED - NOT CONNECTED

Everything in this story is local Python with no Fabric/Azure dependency and
no network calls. No Fabric/Azure resource has been created, read or written
by this work.
