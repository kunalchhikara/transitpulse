"""DE-12 criteria 1 and 5: Bronze is read only through committed manifests;
the per-dataset reconciliation is asserted before commit against
independently computed counts, and any failure commits nothing and leaves a
FAILED manifest."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from simulator.bronze import (
    MalformedInputError,
    ParseErrorPolicy,
    ingest_batch,
    ingest_stream,
    load_bronze_config,
)
from simulator.silver_batch import (
    STAGE_STAGED,
    BronzeContractError,
    SilverBatchAccountingError,
    head_manifest,
)
from simulator.tests._silver_batch_helpers import (
    bronze_record_counts,
    ingest_rows,
    line,
    manifest,
    run_sb,
    sample_row,
    sb_config,
    t,
    table,
    table_files,
    write_batch_dir,
)

DS = "historical_trips"
T0, T1, T2 = "2025-01-05T20:25:28Z", "2025-01-05T21:25:28Z", "2025-01-05T22:25:28Z"


def _scenario(tmp_path: Path) -> tuple[Path, Path, Counter]:
    """Two Bronze runs whose rows are tagged with the outcome they must have,
    by construction (the oracle never runs batch-Silver code)."""
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    tagged_1 = [
        ("inserted", sample_row(DS, 0, updated_at=T1)),
        ("inserted", sample_row(DS, 1, updated_at=T1)),
        ("inserted", sample_row(DS, 2, updated_at=T1)),
        ("quarantined", b'{"broken": '),
        ("quarantined", sample_row(DS, 3, trip_status="NOPE")),
        ("inserted", sample_row(DS, 4, updated_at=T0)),
        ("updated", sample_row(DS, 4, updated_at=T1)),  # same run, newer
    ]
    tagged_2 = [
        ("updated", sample_row(DS, 0, updated_at=T2, passenger_boardings=50)),
        ("stale", sample_row(DS, 1, updated_at=T0)),
        ("unchanged", sample_row(DS, 2, updated_at=T1)),
        ("quarantined", sample_row(DS, 4, updated_at=T1, peak_occupancy=77)),  # conflict
        ("inserted", sample_row(DS, 5)),
        ("quarantined", sample_row(DS, 6, unknown="x")),
    ]
    ingest_rows(bronze, "b1", {DS: [r for _, r in tagged_1]}, at=t(1), work_dir=tmp_path)
    ingest_rows(bronze, "b2", {DS: [r for _, r in tagged_2]}, at=t(2), work_dir=tmp_path)
    return bronze, out, Counter(tag for tag, _ in tagged_1 + tagged_2)


def test_identity_holds_against_independently_computed_counts(tmp_path: Path) -> None:
    bronze, out, expected = _scenario(tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    info = m["datasets"][DS]
    c = info["counters"]
    for outcome in ("inserted", "updated", "unchanged", "stale", "quarantined"):
        assert c[outcome] == expected[outcome], outcome
    assert c["extracted"] == bronze_record_counts(bronze)[DS] == sum(expected.values())
    assert c["conflicts"] == 1 and c["parse_errors"] == 1 and c["schema_failures"] == 2
    distinct_valid_keys = 5  # trips 0, 1, 2, 4, 5 (3 and 6 are only ever invalid)
    assert info["rows_after"] == distinct_valid_keys == info["rows_before"] + c["inserted"]
    assert m["reconciliation"]["holds"] is True and m["reconciliation"]["failed_checks"] == []
    assert {check["check"] for check in m["reconciliation"]["checks"]} >= {
        "extracted == inserted+updated+unchanged+stale+quarantined", "rows_after == rows_before + inserted",
        "current table lines == rows_after + tombstones_after", "quarantine lines == quarantined",
        "stale lines == stale"}
    # lines on disk == counters (read back independently)
    assert len(table(out, DS)) == info["rows_after"]
    assert len(table_files(out, "quarantine", DS)) == c["quarantined"]
    assert len(table_files(out, "stale", DS)) == c["stale"]
    # record-level lineage: every written record points at a distinct real Bronze line
    written = [(r["_bronze_run_id"], r["_bronze_line_number"]) for r in table(out, DS)]
    written += [(q["bronze_run_id"], q["_bronze_line_number"]) for q in table_files(out, "quarantine", DS)]
    written += [(s["bronze_run_id"], s["_bronze_line_number"]) for s in table_files(out, "stale", DS)]
    assert len(written) == len(set(written))
    assert all(1 <= number <= (7 if run == "b1" else 6) for run, number in written)


def test_identity_holds_per_bronze_table_run(tmp_path: Path) -> None:
    bronze, out, _ = _scenario(tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    for run in m["bronze_runs_applied"]:
        for step in run["tables"]:
            c = step["counters"]
            assert c["extracted"] == step["records"] == (
                c["inserted"] + c["updated"] + c["unchanged"] + c["stale"] + c["quarantined"])


@pytest.mark.parametrize("target", ["quarantine_historical_trips", "current_historical_trips"])
def test_lines_on_disk_disagreeing_with_counters_fail_and_commit_nothing(tmp_path: Path, target: str) -> None:
    bronze, out, _ = _scenario(tmp_path)

    def corrupt(stage: str, root: Path) -> None:
        if stage == STAGE_STAGED:
            path = root / "_staging" / "s1" / target / "part-00000.jsonl"
            path.write_text(path.read_text() + '{"injected": true}\n')

    with pytest.raises(SilverBatchAccountingError):
        run_sb(sb_config(out, bronze), "s1", hook=corrupt)
    m = manifest(out, "s1")
    assert m["status"] == "FAILED" and m["checkpoint"] is None
    assert m["reconciliation"]["holds"] is False
    assert [c["check"] for c in m["reconciliation"]["failed_checks"]] == [
        "quarantine lines == quarantined" if target.startswith("quarantine")
        else "current table lines == rows_after + tombstones_after"]
    assert head_manifest(out) is None
    assert sorted(p.name for p in out.iterdir()) == ["_runs"]  # nothing published, staging removed


def test_only_committed_bronze_runs_are_read_never_globbed(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    ingest_rows(bronze, "b1", {DS: [sample_row(DS, 0)]}, at=t(1), work_dir=tmp_path)
    # an abandoned, uncommitted partition (as a DE-11 crash recovered at a later as_of leaves behind)
    orphan = bronze / DS / "run_id=ghost" / "part-00000.jsonl"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(line(sample_row(DS, 1)) + b"\n")
    # a FAILED Bronze run (parse_error_policy = fail): manifest FAILED, no partition
    config = load_bronze_config().model_copy(update={"parse_error_policy": ParseErrorPolicy.FAIL})
    bad_dir = write_batch_dir(tmp_path / "bad", {DS: [line(sample_row(DS, 2)), b"{not json"]})
    with pytest.raises(MalformedInputError):
        ingest_batch(bad_dir, run_id="failed-run", output_dir=bronze, config=config)
    # a stream run (DE-06's input) is not a batch run
    stream = tmp_path / "events.jsonl"
    stream.write_text(json.dumps({"event_id": "E1"}) + "\n")
    ingest_stream([stream], run_id="stream-run", output_dir=bronze)

    m = run_sb(sb_config(out, bronze), "s1").manifest
    assert [r["run_id"] for r in m["bronze_runs_applied"]] == ["b1"]
    assert m["checkpoint"]["applied_bronze_run_ids"] == ["b1"]
    assert [r["trip_id"] for r in table(out, DS)] == [sample_row(DS, 0)["trip_id"]]


def test_tampered_committed_bronze_partition_fails_loudly(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    bm = ingest_rows(bronze, "b1", {DS: [sample_row(DS, 0), sample_row(DS, 1)]}, at=t(1), work_dir=tmp_path)
    path = bronze / bm["tables"][DS]["path"]
    path.write_bytes(path.read_bytes().splitlines(keepends=True)[0])  # a line lost after commit
    with pytest.raises(BronzeContractError):
        run_sb(sb_config(out, bronze), "s1")
    m = manifest(out, "s1")
    assert m["status"] == "FAILED" and m["error"]["type"] == "BronzeContractError"
    assert head_manifest(out) is None


def test_unknown_batch_table_is_recorded_not_silently_dropped(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    ingest_rows(bronze, "b1", {DS: [sample_row(DS, 0)], "mystery_feed": [{"a": 1}]}, at=t(1), work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze), "s1").manifest
    (run,) = m["bronze_runs_applied"]
    assert run["ignored_tables"] == ["mystery_feed"]
    assert list(m["datasets"]) == [DS]
