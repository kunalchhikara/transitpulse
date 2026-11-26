"""DE-12 criterion 3 (+1): one test per per-row merge outcome, Bronze runs
applied out of order and twice. Bronze input is landed with the real DE-05
`ingest_batch`; expected outcomes follow from how each case is constructed."""

from __future__ import annotations

from pathlib import Path

import pytest

from simulator.tests._silver_batch_helpers import (
    business,
    ingest_rows,
    manifest,
    run_sb,
    sample_row,
    sb_config,
    t,
    table,
    table_files,
)

DS = "historical_trips"
T0 = "2025-01-05T20:25:28Z"
T1 = "2025-01-05T21:25:28Z"  # one hour newer


def _env(tmp_path: Path):
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    return bronze, out, sb_config(out, bronze)


def _trip(index: int = 0, **overrides):
    return sample_row(DS, index, **overrides)


def _counters(out: Path, run_id: str, dataset: str = DS) -> dict:
    return manifest(out, run_id)["datasets"][dataset]["counters"]


def test_inserted_new_keys(tmp_path: Path) -> None:
    bronze, out, cfg = _env(tmp_path)
    ingest_rows(bronze, "b1", {DS: [_trip(0), _trip(1)]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    c = _counters(out, "s1")
    assert (c["extracted"], c["inserted"], c["updated"], c["unchanged"], c["stale"], c["quarantined"]) == (
        2, 2, 0, 0, 0, 0)
    info = manifest(out, "s1")["datasets"][DS]
    assert (info["rows_before"], info["rows_after"]) == (0, 2)
    rows = table(out, DS)
    assert [r["trip_id"] for r in rows] == sorted([_trip(0)["trip_id"], _trip(1)["trip_id"]])
    assert all(r["_bronze_run_id"] == "b1" and r["_silver_batch_run_id"] == "s1" for r in rows)


def test_updated_when_incoming_updated_at_is_newer(tmp_path: Path) -> None:
    bronze, out, cfg = _env(tmp_path)
    ingest_rows(bronze, "b1", {DS: [_trip(0, updated_at=T0)]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    newer = _trip(0, updated_at=T1, passenger_boardings=99)
    ingest_rows(bronze, "b2", {DS: [newer]}, at=t(2), work_dir=tmp_path)
    run_sb(cfg, "s2")
    c = _counters(out, "s2")
    assert (c["updated"], c["inserted"], c["stale"], c["quarantined"]) == (1, 0, 0, 0)
    (row,) = table(out, DS)
    assert row["passenger_boardings"] == 99
    assert row["updated_at"] == "2025-01-05T21:25:28.000000Z"
    assert (row["_bronze_run_id"], row["_silver_batch_run_id"]) == ("b2", "s2")
    info = manifest(out, "s2")["datasets"][DS]
    assert (info["rows_before"], info["rows_after"]) == (1, 1)


def test_unchanged_same_key_same_updated_at_same_payload_even_if_spelled_differently(tmp_path: Path) -> None:
    bronze, out, cfg = _env(tmp_path)
    ingest_rows(bronze, "b1", {DS: [_trip(0)]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    first = table(out, DS)
    # Same instants written with a +10:00 offset: standardised to the same payload.
    respelled = _trip(0, updated_at="2025-01-06T06:25:28+10:00", scheduled_departure="2025-01-06T06:00:00+10:00")
    ingest_rows(bronze, "b2", {DS: [respelled, _trip(1)]}, at=t(2), work_dir=tmp_path)
    run_sb(cfg, "s2")
    c = _counters(out, "s2")
    assert (c["unchanged"], c["inserted"], c["updated"], c["quarantined"]) == (1, 1, 0, 0)
    row = next(r for r in table(out, DS) if r["trip_id"] == _trip(0)["trip_id"])
    assert row == first[0]  # untouched, lineage included (still b1 / s1)


def test_stale_older_updated_at_is_excluded_and_audited(tmp_path: Path) -> None:
    bronze, out, cfg = _env(tmp_path)
    ingest_rows(bronze, "b1", {DS: [_trip(0, updated_at=T1)]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    before = table(out, DS)
    older = _trip(0, updated_at=T0, passenger_boardings=1)
    ingest_rows(bronze, "b2", {DS: [older]}, at=t(2), work_dir=tmp_path)
    run_sb(cfg, "s2")
    c = _counters(out, "s2")
    assert (c["stale"], c["updated"], c["unchanged"], c["quarantined"], c["extracted"]) == (1, 0, 0, 0, 1)
    assert table(out, DS) == before
    (audit,) = table_files(out, "stale", DS)
    assert audit["stale_reason"] == "older_updated_at"
    assert audit["bronze_run_id"] == "b2" and audit["current_bronze_run_id"] == "b1"
    assert audit["payload"]["passenger_boardings"] == 1
    assert audit["current_updated_at"] == "2025-01-05T21:25:28.000000Z"
    assert not table_files(out, "quarantine", DS)


def test_conflict_same_updated_at_different_payload_is_quarantined(tmp_path: Path) -> None:
    bronze, out, cfg = _env(tmp_path)
    ingest_rows(bronze, "b1", {DS: [_trip(0)]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    before = table(out, DS)
    clash = _trip(0, passenger_boardings=_trip(0)["passenger_boardings"] + 7)
    ingest_rows(bronze, "b2", {DS: [clash]}, at=t(2), work_dir=tmp_path)
    run_sb(cfg, "s2")
    c = _counters(out, "s2")
    assert (c["conflicts"], c["quarantined"], c["merge_quarantined"], c["updated"], c["unchanged"]) == (1, 1, 1, 0, 0)
    assert c["quarantined_by_rule"] == {"merge_conflict": 1}
    assert table(out, DS) == before  # the current row is kept
    (q,) = table_files(out, "quarantine", DS)
    assert q["primary_rule"] == "merge_conflict" and q["rejection_category"] == "merge"
    assert q["conflict_with"]["bronze_run_id"] == "b1"
    assert q["raw_payload"] == clash
    assert q["primary_key"] == {"trip_id": clash["trip_id"]}


def test_outcomes_within_one_bronze_run_follow_line_order(tmp_path: Path) -> None:
    bronze, out, cfg = _env(tmp_path)
    rows = [_trip(0, updated_at=T0), _trip(0, updated_at=T1, stops_served=_trip(0)["stops_served"]),
            _trip(0, updated_at=T0), _trip(0, updated_at=T1)]
    ingest_rows(bronze, "b1", {DS: rows}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    c = _counters(out, "s1")
    assert (c["inserted"], c["updated"], c["stale"], c["unchanged"]) == (1, 1, 1, 1)
    info = manifest(out, "s1")["datasets"][DS]
    assert info["rows_after"] == 1 == info["rows_before"] + c["inserted"]


def test_validate_before_merge_an_invalid_copy_never_claims_the_key(tmp_path: Path) -> None:
    bronze, out, cfg = _env(tmp_path)
    bad = _trip(0, updated_at=T1, trip_status="TELEPORTED")  # newer, but invalid
    good = _trip(0, updated_at=T0)
    ingest_rows(bronze, "b1", {DS: [bad, good]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    c = _counters(out, "s1")
    assert (c["inserted"], c["quarantined"], c["stale"]) == (1, 1, 0)
    (row,) = table(out, DS)
    assert row["updated_at"] == "2025-01-05T20:25:28.000000Z"


@pytest.mark.parametrize("split", [False, True])
def test_bronze_runs_applied_out_of_order_converge_to_the_in_order_state(tmp_path: Path, split: bool) -> None:
    """The newer version is committed to Bronze FIRST (earlier ingestion_time),
    the older one after it: the older row is stale, and the final state equals
    applying them in chronological order. `split`: the newer run is applied by
    an explicit-selection Silver run before the older one is even pending."""
    in_order_bronze, in_order_out = tmp_path / "a" / "bronze", tmp_path / "a" / "sb"
    older, newer = _trip(0, updated_at=T0), _trip(0, updated_at=T1, peak_occupancy=3)
    ingest_rows(in_order_bronze, "old", {DS: [older]}, at=t(1), work_dir=tmp_path / "a")
    ingest_rows(in_order_bronze, "new", {DS: [newer]}, at=t(2), work_dir=tmp_path / "a")
    run_sb(sb_config(in_order_out, in_order_bronze), "s1")

    bronze, out = tmp_path / "b" / "bronze", tmp_path / "b" / "sb"
    cfg = sb_config(out, bronze)
    ingest_rows(bronze, "new", {DS: [newer]}, at=t(1), work_dir=tmp_path / "b")
    if split:
        run_sb(cfg, "s1", bronze_run_ids=["new"])
    ingest_rows(bronze, "old", {DS: [older]}, at=t(2), work_dir=tmp_path / "b")
    run_sb(cfg, "s2")
    totals = _sum_counters(out, ["s1", "s2"] if split else ["s2"])
    assert (totals["inserted"], totals["stale"], totals["updated"]) == (1, 1, 0)
    assert [business(r) for r in table(out, DS)] == [business(r) for r in table(in_order_out, DS)]


def _sum_counters(out: Path, run_ids: list[str]) -> dict[str, int]:
    total: dict[str, int] = {}
    for rid in run_ids:
        for key, value in _counters(out, rid).items():
            if isinstance(value, int):
                total[key] = total.get(key, 0) + value
    return total


def test_reapplying_an_applied_bronze_run_is_a_no_op(tmp_path: Path) -> None:
    bronze, out, cfg = _env(tmp_path)
    ingest_rows(bronze, "b1", {DS: [_trip(0), _trip(1)]}, at=t(1), work_dir=tmp_path)
    run_sb(cfg, "s1")
    before = manifest(out, "s1")["checkpoint"]
    result = run_sb(cfg, "s2", bronze_run_ids=["b1"])
    m = result.manifest
    assert m["bronze_runs_applied"] == [] and m["bronze_runs_skipped_already_applied"] == ["b1"]
    assert m["datasets"] == {}
    assert m["checkpoint"] == before  # same applied ids, same table versions (paths)
    # and an all-pending run with nothing new applies nothing either
    m3 = run_sb(cfg, "s3").manifest
    assert m3["bronze_runs_applied"] == [] and m3["checkpoint"] == before
    assert m3["commit_sequence"] == 3 and m3["base_run_id"] == "s2"
    assert len(table(out, DS)) == 2


def test_unknown_explicit_bronze_run_is_refused_before_anything_is_written(tmp_path: Path) -> None:
    from simulator.silver_batch import SilverBatchInputError

    bronze, out, cfg = _env(tmp_path)
    ingest_rows(bronze, "b1", {DS: [_trip(0)]}, at=t(1), work_dir=tmp_path)
    with pytest.raises(SilverBatchInputError):
        run_sb(cfg, "s1", bronze_run_ids=["nope"])
    assert not out.exists()
