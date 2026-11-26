"""DE-12 criterion 9 (+6): every DE-04 catalogue dataset goes through the same
code path (registry driven by `DATASET_CATALOGUE`), with dedicated tests for
`historical_trips`, `service_disruptions` and DE-10's
`vehicle_depot_assignments`; FU-02 (orphan references load and are counted;
`event_date` never re-derived); the `historical_telemetry` limitation."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from simulator.batch import DATASET_CATALOGUE
from simulator.silver_batch import CATALOGUE_DATASETS, SILVER_DATASETS, LoadMode
from simulator.tests._incremental_helpers import parse_ts, source_rows
from simulator.tests._silver_batch_helpers import (
    at,
    bronze_record_counts,
    ingest_rows,
    make_sb_env,
    pk,
    run_sb,
    sample_row,
    sample_rows,
    sb_config,
    t,
    table,
    table_files,
)


def _same_value(source, silver) -> bool:
    """Independent comparison: an ISO timestamp with an offset is compared
    as an instant (Silver standardises its spelling); anything else exactly."""
    if isinstance(source, str) and "T" in source and len(source) > 10:
        try:
            left = datetime.fromisoformat(source)
        except ValueError:
            return source == silver
        if left.tzinfo is not None:
            return left.astimezone(UTC) == datetime.fromisoformat(silver).astimezone(UTC)
    if isinstance(source, float) or isinstance(silver, float):
        return float(source) == float(silver)
    return source == silver


@pytest.fixture(scope="module")
def loaded(tmp_path_factory):
    env = make_sb_env(tmp_path_factory.mktemp("sb-datasets"), service_days=2)
    env.load(at(2030, 1, 1))
    manifest = env.silver("s1").manifest
    return env, manifest


def test_registry_is_the_catalogue_plus_the_four_reference_entities() -> None:
    assert set(CATALOGUE_DATASETS) == set(DATASET_CATALOGUE)
    for name, spec in DATASET_CATALOGUE.items():
        ds = SILVER_DATASETS[name]
        assert (ds.mode, ds.model, ds.primary_key, ds.watermark_column) == (
            LoadMode.MERGE, spec.model, spec.primary_key, spec.watermark_column)
        assert {fk.column for fk in ds.foreign_keys} == {fk.column for fk in spec.foreign_keys}


@pytest.mark.parametrize("dataset", list(DATASET_CATALOGUE))
def test_every_catalogue_dataset_loads_through_the_same_path(loaded, dataset: str) -> None:
    env, manifest = loaded
    source = source_rows(env.inc.batch_dir, dataset)
    info = manifest["datasets"][dataset]
    c = info["counters"]
    assert c["extracted"] == bronze_record_counts(env.bronze_dir)[dataset] == len(source)
    assert (c["inserted"], c["quarantined"], c["stale"], c["updated"]) == (len({pk(dataset, r) for r in source}), 0, 0, 0)
    assert set(c) == set(manifest["datasets"]["schedules"]["counters"])  # one counter shape for all
    rows = {pk(dataset, r): r for r in env.table(dataset)}
    assert len(rows) == info["rows_after"] == len(source)
    for src in source:
        silver = rows[pk(dataset, src)]
        assert all(_same_value(value, silver[field]) for field, value in src.items()), (dataset, src)
        assert silver["event_date"] == src["event_date"]  # FU-02: passed through, never re-derived
    assert list(env.table(dataset)[0])[: len(SILVER_DATASETS[dataset].fields)] == list(
        SILVER_DATASETS[dataset].fields)


def test_event_date_is_the_local_service_date_not_the_utc_date(loaded) -> None:
    env, _ = loaded
    differing = [r for r in env.table("historical_trips")
                 if datetime.fromisoformat(r["scheduled_departure"]).astimezone(UTC).date().isoformat()
                 != r["event_date"]]
    assert differing, "fixture guard: some trips depart on the previous UTC date"
    by_id = {r["trip_id"]: r for r in source_rows(env.inc.batch_dir, "historical_trips")}
    assert all(r["event_date"] == by_id[r["trip_id"]]["event_date"] for r in differing)


# -- FU-02 ----------------------------------------------------------------------


def test_fu02_trip_loaded_before_its_disruption_loads_and_is_counted_unresolved(tmp_path: Path) -> None:
    env = make_sb_env(tmp_path)
    disruptions = {r["disruption_id"]: r for r in source_rows(env.inc.batch_dir, "service_disruptions")}
    trips = source_rows(env.inc.batch_dir, "historical_trips")
    early = [tr for tr in trips if tr["disruption_id"]
             and parse_ts(tr["updated_at"]) < parse_ts(disruptions[tr["disruption_id"]]["updated_at"])]
    assert early, "fixture guard: a trip settled before the disruption it references"
    cut = parse_ts(early[0]["updated_at"])
    env.load(cut)
    first = env.silver("s1").manifest
    # independent oracle: trips in the first window whose disruption is not yet in it
    window_trips = [tr for tr in trips if parse_ts(tr["updated_at"]) <= cut]
    loaded_dis = {d for d, row in disruptions.items() if parse_ts(row["updated_at"]) <= cut}
    expected = sum(1 for tr in window_trips if tr["disruption_id"] and tr["disruption_id"] not in loaded_dis)
    assert expected >= 1
    fk = "disruption_id->service_disruptions.disruption_id"
    assert first["datasets"]["historical_trips"]["unresolved_references"][fk] == expected
    assert first["referential_integrity"]["historical_trips"][fk] == expected
    assert early[0]["trip_id"] in {r["trip_id"] for r in env.table("historical_trips")}

    env.load(at(2030, 1, 1))
    second = env.silver("s2").manifest
    assert second["referential_integrity"]["historical_trips"][fk] == 0
    assert all(v == 0 for counts in second["referential_integrity"].values() for v in counts.values())


def test_orphan_reference_to_a_parent_that_never_arrives_still_loads(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    trip = sample_row("historical_trips", 0, vehicle_id="VEH-9999")
    ingest_rows(bronze, "b1", {"historical_trips": [trip]}, at=t(1), work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    unresolved = m["datasets"]["historical_trips"]["unresolved_references"]
    # no parent table loaded at all: every non-null reference is unresolved
    assert unresolved["vehicle_id->vehicles.vehicle_id"] == 1
    assert unresolved["disruption_id->service_disruptions.disruption_id"] == 0  # null -> not a reference
    assert [r["vehicle_id"] for r in table(out, "historical_trips")] == ["VEH-9999"]


# -- dedicated dataset tests -------------------------------------------------------


def test_historical_trips_rules_and_measures(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    rows = [sample_row("historical_trips", i) for i in range(5)]
    no_disruption = dict(next(r for r in sample_rows("historical_trips") if r["trip_status"] != "COMPLETED"),
                         disruption_id=None)
    ingest_rows(bronze, "b1", {"historical_trips": [*rows, no_disruption]}, at=t(1), work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    c = m["datasets"]["historical_trips"]["counters"]
    assert (c["inserted"], c["quarantined"], c["quarantined_by_rule"]) == (5, 1, {"model_constraint": 1})
    silver = {r["trip_id"]: r for r in table(out, "historical_trips")}
    for row in rows:
        s = silver[row["trip_id"]]
        assert (s["departure_delay_seconds"], s["arrival_delay_seconds"], s["passenger_boardings"]) == (
            row["departure_delay_seconds"], row["arrival_delay_seconds"], row["passenger_boardings"])


def test_service_disruptions_scope_rules_and_nullable_references(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    sample = sample_rows("service_disruptions")
    trip_scoped = next(r for r in sample if r["scope"] == "TRIP")
    broken = dict(trip_scoped, disruption_id="DIS-BROKEN", trip_id=None)
    route_scoped = dict(trip_scoped, disruption_id="DIS-ROUTE", scope="ROUTE", vehicle_id=None, trip_id=None,
                        stop_id=None)
    ingest_rows(bronze, "b1", {"service_disruptions": [trip_scoped, broken, route_scoped]}, at=t(1),
                work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    c = m["datasets"]["service_disruptions"]["counters"]
    assert (c["inserted"], c["quarantined_by_rule"]) == (2, {"model_constraint": 1})
    unresolved = m["datasets"]["service_disruptions"]["unresolved_references"]
    # the TRIP-scoped row references a trip/vehicle not loaded; the ROUTE row's nulls are not references
    assert unresolved["trip_id->schedules.trip_id"] == 1
    assert unresolved["vehicle_id->vehicles.vehicle_id"] == 1
    assert {r["disruption_id"] for r in table(out, "service_disruptions")} == {trip_scoped["disruption_id"], "DIS-ROUTE"}


def test_vehicle_depot_assignments_change_log_and_rules(loaded, tmp_path: Path) -> None:
    env, manifest = loaded
    source = source_rows(env.inc.batch_dir, "vehicle_depot_assignments")
    silver = env.table("vehicle_depot_assignments")
    assert sorted(r["assignment_id"] for r in silver) == sorted(r["assignment_id"] for r in source)
    assert manifest["referential_integrity"]["vehicle_depot_assignments"] == {
        "vehicle_id->vehicles.vehicle_id": 0, "depot_id->depots.depot_id": 0,
        "previous_depot_id->depots.depot_id": 0}

    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    initial = source[0]
    bad_initial = dict(initial, assignment_id="VDA-BAD-1", previous_depot_id="DEPOT-02")
    bad_transfer = dict(initial, assignment_id="VDA-BAD-2", change_type="TRANSFER", change_sequence=2,
                        previous_depot_id=initial["depot_id"])  # moves to the same depot
    ingest_rows(bronze, "b1", {"vehicle_depot_assignments": [initial, bad_initial, bad_transfer]}, at=t(1),
                work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    c = m["datasets"]["vehicle_depot_assignments"]["counters"]
    assert (c["inserted"], c["quarantined_by_rule"]) == (1, {"model_constraint": 2})
    assert {q["primary_key"]["assignment_id"] for q in table_files(out, "quarantine", "vehicle_depot_assignments")} \
        == {"VDA-BAD-1", "VDA-BAD-2"}


def test_historical_telemetry_gets_model_validation_only(tmp_path: Path) -> None:
    """Recorded limitation: the PROJECT.md section 12 telemetry rules (DE-06)
    are NOT applied - a 500 km/h snapshot (above the section 12 maximum)
    loads; only DE-04/DE-01 model bounds (e.g. speed >= 0) are enforced."""
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    fast = sample_row("historical_telemetry", 0, speed_kmh=500.0)
    negative = sample_row("historical_telemetry", 1, speed_kmh=-1.0)
    ingest_rows(bronze, "b1", {"historical_telemetry": [fast, negative]}, at=t(1), work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    c = m["datasets"]["historical_telemetry"]["counters"]
    assert (c["inserted"], c["quarantined_by_rule"]) == (1, {"value_out_of_range": 1})
    assert table(out, "historical_telemetry")[0]["speed_kmh"] == 500.0
