"""DE-12 criterion 4 (+9): full-load reference tables use snapshot semantics
(inserted / updated / unchanged / deleted, soft-delete policy), with
dedicated tests for each of the four DE-01 reference entities loaded by the
real DE-11 loader in `full` mode."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from simulator.silver_batch import REFERENCE_DATASETS, SnapshotDeleteGuardError
from simulator.tests._silver_batch_helpers import (
    at,
    ingest_rows,
    make_sb_env,
    manifest,
    run_sb,
    sb_config,
    t,
    table,
    table_files,
)

KEYS = {"depots": "depot_id", "stops": "stop_id", "routes": "route_id", "vehicles": "vehicle_id"}


def _source(env, entity: str) -> list[dict]:
    return json.loads((env.inc.ref_dir / f"{entity}.json").read_text())


def _write_source(env, entity: str, rows: list[dict]) -> None:
    (env.inc.ref_dir / f"{entity}.json").write_text(json.dumps(rows, sort_keys=True, indent=2) + "\n")


@pytest.mark.parametrize("entity", REFERENCE_DATASETS)
def test_initial_snapshot_of_each_reference_entity_loads_every_element(tmp_path: Path, entity: str) -> None:
    env = make_sb_env(tmp_path)
    env.load(at(2029, 12, 29))
    m = env.silver("s1").manifest
    source = _source(env, entity)
    info = m["datasets"][entity]
    assert info["mode"] == "snapshot" and info["primary_key"] == [KEYS[entity]]
    c = info["counters"]
    assert (c["extracted"], c["inserted"], c["deleted"], c["quarantined"]) == (len(source), len(source), 0, 0)
    assert info["rows_after"] == info["last_valid_snapshot_keys"] == len(source)
    (step,) = [s for r in m["bronze_runs_applied"] for s in r["tables"] if s["dataset"] == entity]
    assert step["snapshot_as_of"] == "2029-12-29T00:00:00.000000Z"
    assert step["snapshot_as_of_source"] == "de11_window_high"
    rows = env.table(entity)
    fields = list(source[0])
    assert sorted(({k: r[k] for k in fields} for r in rows), key=lambda r: r[KEYS[entity]]) == sorted(
        source, key=lambda r: r[KEYS[entity]])
    assert not any(r["_silver_is_deleted"] for r in rows)


@pytest.mark.parametrize("entity", REFERENCE_DATASETS)
def test_changed_snapshot_of_each_reference_entity(tmp_path: Path, entity: str) -> None:
    """A later DE-11 full run with one element changed, one removed and one
    added: updated / deleted (soft) / inserted / unchanged, and current state
    equals the new snapshot."""
    env = make_sb_env(tmp_path)
    env.load(at(2029, 12, 29))
    env.silver("s1")
    source = _source(env, entity)
    key = KEYS[entity]
    changed = [dict(row) for row in source]
    changed[0]["name" if entity != "vehicles" else "fleet_number"] = "CHANGED"
    removed = changed.pop()
    added = dict(source[0], **{key: f"{source[0][key]}-NEW"})
    changed.append(added)
    _write_source(env, entity, changed)
    env.load(at(2029, 12, 30))
    m = env.silver("s2").manifest
    c = m["datasets"][entity]["counters"]
    assert (c["inserted"], c["updated"], c["deleted"], c["unchanged"]) == (1, 1, 1, len(source) - 2)
    info = m["datasets"][entity]
    assert info["rows_after"] == len(changed) == info["rows_before"] + c["inserted"] - c["deleted"]
    active = env.table(entity)
    assert sorted(r[key] for r in active) == sorted(r[key] for r in changed)
    tombstones = [r for r in env.table(entity, include_deleted=True) if r["_silver_is_deleted"]]
    assert [r[key] for r in tombstones] == [removed[key]]
    assert tombstones[0]["_silver_deleted_by_run_id"] == "s2"
    assert info["tombstones_after"] == 1


def test_revert_to_an_earlier_snapshot_reactivates_the_deleted_row(tmp_path: Path) -> None:
    env = make_sb_env(tmp_path)
    env.load(at(2029, 12, 29))
    env.silver("s1")
    original = _source(env, "vehicles")
    _write_source(env, "vehicles", original[:-1])
    env.load(at(2029, 12, 30))
    env.silver("s2")
    _write_source(env, "vehicles", original)  # A -> B -> A: DE-11 replays content (allow_replay)
    env.load(at(2029, 12, 31))
    m = env.silver("s3").manifest
    c = m["datasets"]["vehicles"]["counters"]
    assert (c["inserted"], c["deleted"], c["unchanged"]) == (1, 0, len(original) - 1)
    assert len(env.table("vehicles")) == len(original)
    assert m["datasets"]["vehicles"]["tombstones_after"] == 0


def _vehicle(n: int, **overrides) -> dict:
    return {"assigned_route_id": "ROUTE-01", "capacity": 60, "fleet_number": f"{n:04d}", "home_depot_id": "DEPOT-01",
            "schema_version": "1.0.0", "vehicle_id": f"VEH-{n:04d}", "vehicle_type": "BUS", **overrides}


def test_older_snapshot_applied_after_a_newer_one_is_stale_as_a_whole(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    cfg = sb_config(out, bronze)
    ingest_rows(bronze, "snap-a", {"vehicles": [_vehicle(1), _vehicle(2)]}, at=t(1), work_dir=tmp_path)
    ingest_rows(bronze, "snap-b", {"vehicles": [_vehicle(1, capacity=70), _vehicle(3)]}, at=t(2), work_dir=tmp_path)
    run_sb(cfg, "s1", bronze_run_ids=["snap-b"])  # newer applied first
    m = run_sb(cfg, "s2").manifest  # the older snapshot becomes pending now
    c = m["datasets"]["vehicles"]["counters"]
    assert (c["stale"], c["inserted"], c["updated"], c["deleted"]) == (2, 0, 0, 0)
    (step,) = m["bronze_runs_applied"][0]["tables"]
    assert step["snapshot_stale"] is True and step["snapshot_as_of_source"] == "bronze_ingestion_time"
    assert sorted((r["vehicle_id"], r["capacity"]) for r in table(out, "vehicles")) == [
        ("VEH-0001", 70), ("VEH-0003", 60)]
    audits = table_files(out, "stale", "vehicles")
    assert {a["stale_reason"] for a in audits} == {"older_snapshot"} and len(audits) == 2


def test_duplicate_key_within_one_snapshot_first_occurrence_wins(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    rows = [_vehicle(1), _vehicle(2), _vehicle(1, capacity=99)]
    ingest_rows(bronze, "snap", {"vehicles": rows}, at=t(1), work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    c = m["datasets"]["vehicles"]["counters"]
    assert (c["inserted"], c["quarantined"], c["duplicate_keys_in_snapshot"]) == (2, 1, 1)
    assert c["quarantined_by_rule"] == {"duplicate_key_in_snapshot": 1}
    assert next(r for r in table(out, "vehicles") if r["vehicle_id"] == "VEH-0001")["capacity"] == 60
    assert m["datasets"]["vehicles"]["rows_after"] == m["datasets"]["vehicles"]["last_valid_snapshot_keys"] == 2


def test_quarantined_row_in_a_snapshot_deletes_its_key_and_is_counted(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    cfg = sb_config(out, bronze)
    ingest_rows(bronze, "snap-a", {"vehicles": [_vehicle(n) for n in range(1, 5)]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    bad = _vehicle(4, capacity=-1)
    ingest_rows(bronze, "snap-b", {"vehicles": [*(_vehicle(n) for n in range(1, 4)), bad]}, at=t(2), work_dir=tmp_path)
    m = run_sb(cfg, "s2").manifest
    c = m["datasets"]["vehicles"]["counters"]
    assert (c["deleted"], c["deleted_with_quarantined_row"], c["quarantined"], c["unchanged"]) == (1, 1, 1, 3)
    assert m["datasets"]["vehicles"]["rows_after"] == 3


def test_snapshot_delete_guard_fails_a_truncated_snapshot(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    cfg = sb_config(out, bronze)
    ingest_rows(bronze, "snap-a", {"vehicles": [_vehicle(n) for n in range(1, 7)]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    ingest_rows(bronze, "snap-b", {"vehicles": [_vehicle(1)]}, at=t(2), work_dir=tmp_path)
    with pytest.raises(SnapshotDeleteGuardError):
        run_sb(cfg, "s2")
    assert manifest(out, "s2")["status"] == "FAILED"
    assert len(table(out, "vehicles")) == 6  # head is still s1
    relaxed = sb_config(out, bronze, max_snapshot_delete_fraction=1.0)
    m = run_sb(relaxed, "s3").manifest
    assert m["datasets"]["vehicles"]["counters"]["deleted"] == 5
    assert len(table(out, "vehicles")) == 1


def test_routes_stop_ids_list_foreign_key_is_resolved_per_element(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    stops = [{"latitude": -33.8, "longitude": 151.2, "name": f"S{n}", "schema_version": "1.0.0",
              "stop_id": f"STOP-{n:04d}"} for n in (1, 2)]
    route = {"home_depot_id": "DEPOT-01", "name": "R", "route_id": "ROUTE-01", "route_number": "100",
             "schema_version": "1.0.0", "stop_ids": ["STOP-0001", "STOP-0009"]}
    ingest_rows(bronze, "ref", {"stops": stops, "routes": [route]}, at=t(1), work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    assert m["datasets"]["routes"]["unresolved_references"] == {
        "home_depot_id->depots.depot_id": 1, "stop_ids->stops.stop_id": 1}
    assert len(table(out, "routes")) == 1  # loaded anyway (FU-02)
