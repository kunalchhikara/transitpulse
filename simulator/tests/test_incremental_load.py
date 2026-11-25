"""DE-11 criteria 1-3, 8, 11 and the PROJECT.md section 21 initial /
incremental / rerun / empty-source batch tests. Expected rows are recomputed
from the source files."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import timedelta, timezone

from simulator.bronze import BronzeStore
from simulator.incremental import (
    STAGE_BRONZE_COMMITTED,
    STAGE_EXTRACT_WRITTEN,
    STAGE_WATERMARK_WRITTEN,
    STATUS_COMMITTED,
    STATUS_SKIPPED_UNCHANGED,
    STATUS_UP_TO_DATE,
    WATERMARK_SEMANTICS,
    format_ts,
)
from simulator.tests._incremental_helpers import (
    INCREMENTAL_DATASETS,
    REFERENCE_ENTITIES,
    REFERENCE_KEYS,
    all_watermarks,
    bronze_keys,
    bronze_records,
    committed_run_ids,
    expected_keys,
    make_env,
    parse_ts,
    pk,
    rewrite_source_file,
    source_rows,
    source_watermark,
    strip,
    tree_bytes,
)


def _middle_cut(env):
    values = all_watermarks(env.batch_dir)
    return values[len(values) // 2]


def test_initial_load_selects_everything_up_to_as_of(tmp_path):
    env = make_env(tmp_path)
    as_of = _middle_cut(env)
    result = env.run(as_of)
    assert result.status == "SUCCEEDED"
    for dataset in INCREMENTAL_DATASETS:
        expected = expected_keys(env.batch_dir, dataset, as_of)
        assert bronze_keys(env.bronze_dir, dataset) == expected, dataset
        record = env.control(dataset)
        landed = [source_watermark(r, dataset) for r in source_rows(env.batch_dir, dataset)
                  if source_watermark(r, dataset) <= as_of]
        assert record["watermark"] == (format_ts(max(landed)) if landed else None)
        assert record["last_as_of"] == format_ts(as_of)
        assert record["watermark_semantics"] == WATERMARK_SEMANTICS == "max_committed_updated_at"
        assert record["last_window"] == {"low": None, "high": format_ts(as_of)}
        assert result.dataset(dataset).status == STATUS_COMMITTED
        assert result.dataset(dataset).extracted_count == sum(expected.values())
    # Sanity: the cut really splits the source (some rows left for later).
    assert any(
        sum(expected_keys(env.batch_dir, d, as_of).values()) < len(source_rows(env.batch_dir, d))
        for d in INCREMENTAL_DATASETS
    )


def test_incremental_load_selects_only_the_new_window(tmp_path):
    env = make_env(tmp_path)
    first = _middle_cut(env)
    second = all_watermarks(env.batch_dir)[-1]
    env.run(first)
    lows = {d: env.control(d)["watermark"] for d in INCREMENTAL_DATASETS}
    assert all(lows.values()), "fixture guard: every dataset committed rows at the first cut"
    result = env.run(second)
    for dataset in INCREMENTAL_DATASETS:
        low = parse_ts(lows[dataset])  # max committed updated_at, <= first
        assert low <= first
        run_id = result.dataset(dataset).bronze_run_id
        assert run_id == f"inc-{dataset}-{low:%Y%m%dT%H%M%S}Z-{second:%Y%m%dT%H%M%S}Z"
        window_rows = [strip(r) for r in bronze_records(env.bronze_dir, dataset) if r["run_id"] == run_id]
        assert Counter(pk(r, dataset) for r in window_rows) == expected_keys(env.batch_dir, dataset, second, first)
        assert all(low < source_watermark(r, dataset) <= second for r in window_rows)
        # Whole source now in Bronze exactly once.
        assert bronze_keys(env.bronze_dir, dataset) == expected_keys(env.batch_dir, dataset, second)
        assert max(bronze_keys(env.bronze_dir, dataset).values()) == 1
        assert env.control(dataset)["last_window"] == {"low": lows[dataset], "high": format_ts(second)}


def test_full_reference_load_lands_every_entity_with_schema_version(tmp_path):
    env = make_env(tmp_path)
    as_of = all_watermarks(env.batch_dir)[0]
    result = env.run(as_of)
    for entity in REFERENCE_ENTITIES:
        source = json.loads((env.ref_dir / f"{entity}.json").read_text())
        landed = [strip(r) for r in bronze_records(env.bronze_dir, entity)]
        assert landed == source  # same objects, same order
        assert all("schema_version" in r for r in landed)
        assert result.dataset(entity).status == STATUS_COMMITTED
        assert result.dataset(entity).bronze_run_id == f"full-{entity}-{as_of:%Y%m%dT%H%M%S}Z"
        assert {r[REFERENCE_KEYS[entity]] for r in landed} == {r[REFERENCE_KEYS[entity]] for r in source}


def test_extract_is_a_de04_shaped_directory_with_verifiable_manifest(tmp_path):
    env = make_env(tmp_path)
    as_of = all_watermarks(env.batch_dir)[-1]
    result = env.run(as_of)
    for dataset in INCREMENTAL_DATASETS:
        record = env.audit_records(result.pipeline_run_id, dataset)[-1]
        directory = env.state_dir / record["extract_dir"]
        manifest = json.loads((directory / "manifest.json").read_text())
        entry = manifest["datasets"][dataset]
        data = (directory / entry["file"]).read_bytes()
        assert entry["row_count"] == data.count(b"\n") == record["extracted_count"]
        assert entry["sha256"] == hashlib.sha256(data).hexdigest()
        # Selected lines are the source's own bytes.
        source_lines = set((env.batch_dir / f"{dataset}.jsonl").read_bytes().splitlines())
        assert set(data.splitlines()) <= source_lines
        bronze_manifest = BronzeStore(env.bronze_dir).read_manifest(record["bronze_run_id"])
        assert bronze_manifest["source_reconciliation"]["status"] == "MATCHED"


def test_watermark_advances_only_after_the_bronze_commit(tmp_path):
    """Criterion 3: observe control state at every stage of one attempt."""
    env = make_env(tmp_path)
    as_of = _middle_cut(env)
    seen = []

    def hook(stage, dataset, attempt):
        control = env.control(dataset)
        last_as_of = control["last_as_of"] if control else None
        run_ids = committed_run_ids(env.bronze_dir)
        committed = any(r.startswith(("inc-" + dataset + "-", "full-" + dataset + "-")) for r in run_ids)
        seen.append((stage, dataset, control, last_as_of, committed))

    env.run(as_of, hook=hook)
    for stage, dataset, control, last_as_of, committed in seen:
        if stage == STAGE_EXTRACT_WRITTEN:
            assert control is None and not committed
        elif stage == STAGE_BRONZE_COMMITTED:
            assert control is None and committed, dataset
        else:
            assert stage == STAGE_WATERMARK_WRITTEN
            assert last_as_of == format_ts(as_of) and committed
            if dataset in INCREMENTAL_DATASETS:
                landed = [source_watermark(strip(r), dataset) for r in bronze_records(env.bronze_dir, dataset)]
                assert control["watermark"] == (format_ts(max(landed)) if landed else None)
    assert {s for s, *_ in seen} == {STAGE_EXTRACT_WRITTEN, STAGE_BRONZE_COMMITTED, STAGE_WATERMARK_WRITTEN}


def test_rerun_with_same_as_of_is_an_up_to_date_no_op(tmp_path):
    env = make_env(tmp_path)
    as_of = _middle_cut(env)
    env.run(as_of)
    bronze_before, control_before = tree_bytes(env.bronze_dir), tree_bytes(env.state_dir / "control")
    result = env.run(as_of)
    assert result.status == "SUCCEEDED"
    assert {r.status for r in result.datasets} == {STATUS_UP_TO_DATE}
    assert tree_bytes(env.bronze_dir) == bronze_before
    assert tree_bytes(env.state_dir / "control") == control_before


def test_empty_window_is_a_recorded_success_with_zero_rows(tmp_path):
    env = make_env(tmp_path)
    last = all_watermarks(env.batch_dir)[-1]
    env.run(last)
    later = last + timedelta(days=1)
    result = env.run(later)
    assert result.status == "SUCCEEDED"
    store = BronzeStore(env.bronze_dir)
    for dataset in INCREMENTAL_DATASETS:
        outcome = result.dataset(dataset)
        assert outcome.status == STATUS_COMMITTED and outcome.extracted_count == 0
        assert outcome.watermark_after == outcome.watermark_before  # empty window: watermark kept
        assert outcome.last_as_of_after == format_ts(later)
        audit = env.audit_records(result.pipeline_run_id, dataset)[-1]
        assert audit["source_rows_in_window"] == 0 and audit["bronze_records_written"] == 0
        assert audit["reconciliation"]["holds"] is True
        assert store.read_manifest(outcome.bronze_run_id)["status"] == "COMMITTED"


def test_empty_source_file_loads_zero_rows(tmp_path):
    env = make_env(tmp_path)
    dataset = INCREMENTAL_DATASETS[-1]
    rewrite_source_file(env.batch_dir, dataset, b"")
    result = env.run(all_watermarks(env.batch_dir)[-1])
    assert result.dataset(dataset).status == STATUS_COMMITTED
    assert result.dataset(dataset).extracted_count == 0
    assert bronze_records(env.bronze_dir, dataset) == []


def test_unchanged_reference_reload_is_a_recorded_no_op_and_changes_create_new_runs(tmp_path):
    """Criterion 8, including a revert to earlier content (A -> B -> A)."""
    env = make_env(tmp_path)
    cuts = all_watermarks(env.batch_dir)
    t1, t2, t3, t4 = cuts[0], cuts[1], cuts[2], cuts[3]
    env.run(t1)
    runs_after_first = committed_run_ids(env.bronze_dir)
    result = env.run(t2)
    for entity in REFERENCE_ENTITIES:
        outcome = result.dataset(entity)
        assert outcome.status == STATUS_SKIPPED_UNCHANGED and outcome.error is None
        assert outcome.watermark_after is None and outcome.last_as_of_after == format_ts(t2)  # full: no watermark
        assert len(bronze_records(env.bronze_dir, entity)) == len(json.loads((env.ref_dir / f"{entity}.json").read_text()))
    assert not any(r.startswith("full-") for r in set(committed_run_ids(env.bronze_dir)) - set(runs_after_first))

    original = (env.ref_dir / "vehicles.json").read_text()
    vehicles = json.loads(original)
    vehicles[0]["capacity"] += 1
    (env.ref_dir / "vehicles.json").write_text(json.dumps(vehicles, sort_keys=True, indent=2) + "\n")
    result = env.run(t3)
    assert result.dataset("vehicles").status == STATUS_COMMITTED
    assert result.dataset("depots").status == STATUS_SKIPPED_UNCHANGED
    new_run = result.dataset("vehicles").bronze_run_id
    landed = [strip(r) for r in bronze_records(env.bronze_dir, "vehicles") if r["run_id"] == new_run]
    assert landed == vehicles

    (env.ref_dir / "vehicles.json").write_text(original)  # back to A
    result = env.run(t4)
    assert result.dataset("vehicles").status == STATUS_COMMITTED  # no DuplicateInputError
    manifest = BronzeStore(env.bronze_dir).read_manifest(result.dataset("vehicles").bronze_run_id)
    assert manifest["replay_of"] == [f"full-vehicles-{t1:%Y%m%dT%H%M%S}Z"]
    assert [m for m in committed_run_ids(env.bronze_dir) if m.startswith("full-vehicles-")] == [
        f"full-vehicles-{t:%Y%m%dT%H%M%S}Z" for t in (t1, t3, t4)
    ]


def test_fu02_trip_loads_before_the_disruption_it_references(tmp_path):
    """Criterion 11: no cross-dataset ordering; event_date passed through."""
    env = make_env(tmp_path)
    disruptions = {r["disruption_id"]: r for r in source_rows(env.batch_dir, "service_disruptions")}
    trips = [
        t for t in source_rows(env.batch_dir, "historical_trips")
        if t["disruption_id"] and parse_ts(t["updated_at"]) < parse_ts(disruptions[t["disruption_id"]]["updated_at"])
    ]
    assert trips, "fixture guard: the source must contain a trip settled before its disruption"
    trip = trips[0]
    disruption = disruptions[trip["disruption_id"]]
    cut = parse_ts(trip["updated_at"])  # exactly the trip's updated_at
    first = env.run(cut)
    assert first.dataset("historical_trips").status == STATUS_COMMITTED
    assert first.dataset("service_disruptions").status == STATUS_COMMITTED
    loaded_trips = {r["trip_id"]: r for r in map(strip, bronze_records(env.bronze_dir, "historical_trips"))}
    loaded_disruptions = {r["disruption_id"] for r in map(strip, bronze_records(env.bronze_dir, "service_disruptions"))}
    assert trip["trip_id"] in loaded_trips
    assert disruption["disruption_id"] not in loaded_disruptions
    second = env.run(parse_ts(disruption["updated_at"]))
    assert second.status == "SUCCEEDED"
    loaded_disruptions = {r["disruption_id"] for r in map(strip, bronze_records(env.bronze_dir, "service_disruptions"))}
    assert disruption["disruption_id"] in loaded_disruptions
    # event_date unchanged for every landed row of every dataset.
    for dataset in INCREMENTAL_DATASETS:
        by_key = {json.dumps(sorted(r.items())): r for r in source_rows(env.batch_dir, dataset)}
        for record in map(strip, bronze_records(env.bronze_dir, dataset)):
            assert by_key[json.dumps(sorted(record.items()))]["event_date"] == record["event_date"]


def test_as_of_offset_is_normalised_to_utc(tmp_path):
    env = make_env(tmp_path)
    as_of = _middle_cut(env)
    sydney = as_of.astimezone(timezone(timedelta(hours=10)))
    result = env.run(sydney)
    assert result.as_of == format_ts(as_of)
    assert env.control(INCREMENTAL_DATASETS[0])["last_as_of"] == format_ts(as_of)
    assert env.control(INCREMENTAL_DATASETS[0])["watermark"].endswith("Z")
