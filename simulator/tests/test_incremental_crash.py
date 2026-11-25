"""DE-11 criterion 4: crash-safe idempotency. A `SimulatedCrash`
(BaseException = process death: nothing audited, nothing cleaned up) is
injected after the extract, inside the Bronze commit, after the Bronze commit
(before the watermark write) and after the watermark write (before the audit
record). Every rerun with the same `as_of` must end with zero extra Bronze
records and the same control state as an uninterrupted run."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta

import pytest

from simulator.bronze import BronzeStore, ingest_batch
from simulator.incremental import (
    STAGE_BRONZE_COMMITTED,
    STAGE_EXTRACT_WRITTEN,
    STAGE_WATERMARK_WRITTEN,
    STATUS_ALREADY_COMMITTED,
    STATUS_COMMITTED,
    STATUS_FAILED,
    STATUS_UP_TO_DATE,
    AsOfError,
    bronze_run_id_for,
    format_ts,
    resolve_plan,
)
from simulator.tests._incremental_helpers import (
    INCREMENTAL_DATASETS,
    REFERENCE_ENTITIES,
    SimulatedCrash,
    all_watermarks,
    assert_bronze_matches_source,
    bronze_keys,
    bronze_record_count,
    bronze_records,
    committed_run_ids,
    expected_keys,
    make_env,
    parse_ts,
    pk,
    rewrite_source_file,
    source_rows,
    tree_bytes,
    write_batch_source,
)

DURING_BRONZE = "during_bronze_ingest"
CRASH_POINTS = (STAGE_EXTRACT_WRITTEN, DURING_BRONZE, STAGE_BRONZE_COMMITTED, STAGE_WATERMARK_WRITTEN)
EXPECTED_RERUN_STATUS = {
    STAGE_EXTRACT_WRITTEN: STATUS_COMMITTED,
    DURING_BRONZE: STATUS_COMMITTED,
    STAGE_BRONZE_COMMITTED: STATUS_ALREADY_COMMITTED,
    STAGE_WATERMARK_WRITTEN: STATUS_UP_TO_DATE,
}
TARGET = "historical_trips"  # mid-plan: datasets before it commit, after it never start


def _cuts(env):
    values = all_watermarks(env.batch_dir)
    return values[len(values) // 3], values[-1]


def crash_hook(stage_to_crash: str, dataset: str):
    def hook(stage, name, attempt):
        if stage == stage_to_crash and name == dataset:
            raise SimulatedCrash(f"crash at {stage}")

    return hook


def crash_inside_bronze_commit(dataset: str):
    """Bronze reads its clock at run start and again just before writing the
    commit manifest; dying on the second read leaves published-but-uncommitted
    partitions behind - a crash in the middle of Bronze's commit."""

    def ingest(batch_dir, *, clock, **kwargs):
        if batch_dir.parent.name != dataset:
            return ingest_batch(batch_dir, clock=clock, **kwargs)
        calls = []

        def dying_clock():
            calls.append(1)
            if len(calls) == 2:
                raise SimulatedCrash("crash inside Bronze commit")
            return clock()

        return ingest_batch(batch_dir, clock=dying_clock, **kwargs)

    return ingest


def _snapshot(env):
    tables = [*REFERENCE_ENTITIES, *INCREMENTAL_DATASETS]
    return {t: bronze_records(env.bronze_dir, t) for t in tables}, tree_bytes(env.state_dir / "control")


@pytest.fixture(scope="module")
def uninterrupted(tmp_path_factory):
    env = make_env(tmp_path_factory.mktemp("uninterrupted"))
    first, second = _cuts(env)
    env.run(first)
    after_first = _snapshot(env), bronze_record_count(env.bronze_dir)
    env.run(second)
    return {0: after_first, 1: (_snapshot(env), bronze_record_count(env.bronze_dir))}


@pytest.mark.parametrize("window", [0, 1], ids=["initial-window", "incremental-window"])
@pytest.mark.parametrize("stage", CRASH_POINTS)
def test_crash_then_rerun_matches_an_uninterrupted_run(tmp_path, uninterrupted, stage, window):
    env = make_env(tmp_path)
    cuts = _cuts(env)
    if window == 1:
        env.run(cuts[0])
    as_of = cuts[window]
    kwargs = (
        {"bronze_ingest": crash_inside_bronze_commit(TARGET)}
        if stage == DURING_BRONZE
        else {"hook": crash_hook(stage, TARGET)}
    )
    with pytest.raises(SimulatedCrash):
        env.run(as_of, **kwargs)

    target_runs = [r for r in committed_run_ids(env.bronze_dir) if r.startswith(f"inc-{TARGET}-")]
    if stage == DURING_BRONZE:
        # Evidence the crash hit mid-commit: an orphaned partition, no commit marker.
        orphans = [p for p in (env.bronze_dir / TARGET).iterdir() if p.name.removeprefix("run_id=") not in target_runs]
        assert orphans
    if stage in (STAGE_EXTRACT_WRITTEN, DURING_BRONZE):
        assert len(target_runs) == window  # nothing new committed for the target

    result = env.run(as_of)
    assert result.status == "SUCCEEDED"
    assert result.dataset(TARGET).status == EXPECTED_RERUN_STATUS[stage]
    (expected_records, expected_control), expected_count = uninterrupted[window]
    records, control = _snapshot(env)
    assert bronze_record_count(env.bronze_dir) == expected_count  # zero extra Bronze records
    assert records == expected_records
    assert control == expected_control
    if stage == DURING_BRONZE:
        manifest = BronzeStore(env.bronze_dir).read_manifest(result.dataset(TARGET).bronze_run_id)
        assert manifest["removed_uncommitted_paths"]


def _three_cuts(env):
    values = all_watermarks(env.batch_dir)
    return values[len(values) // 4], values[len(values) // 2], values[-1]


def test_later_as_of_after_an_unrecorded_commit_adopts_it_and_matches_uninterrupted(tmp_path):
    """A scheduler's next trigger (a later as_of) after a crash past the
    Bronze commit of an incremental window: the committed window is adopted
    (after re-selecting it from the current source), not re-extracted."""
    clean = make_env(tmp_path / "clean")
    first, second, third = _three_cuts(clean)
    for cut in (first, second, third):
        clean.run(cut)

    env = make_env(tmp_path / "crashed")
    env.run(first)
    control_before = env.control(TARGET)
    with pytest.raises(SimulatedCrash):
        env.run(second, hook=crash_hook(STAGE_BRONZE_COMMITTED, TARGET))
    assert env.control(TARGET) == control_before
    result = env.run(third)
    outcome = result.dataset(TARGET)
    assert result.status == "SUCCEEDED" and outcome.status == STATUS_COMMITTED
    audit = env.audit_records(result.pipeline_run_id, TARGET)[0]
    low = control_before["watermark"]
    assert audit["recovered_bronze_run_id"] == bronze_run_id_for(
        env.config, _plan(env), parse_ts(low), second
    )
    assert audit["watermark_before"] == low and audit["last_as_of_before"] == format_ts(first)
    assert audit["window_low"] == clean.control(TARGET)["last_window"]["low"]
    assert bronze_record_count(env.bronze_dir) == bronze_record_count(clean.bronze_dir)  # nothing landed twice
    assert_bronze_matches_source(env.bronze_dir, env.batch_dir, third)
    # TARGET and every dataset before it went through the same three windows
    # as the uninterrupted run, so their control records and Bronze rows are
    # identical. (Datasets after TARGET never ran at `second`.)
    plan = [*REFERENCE_ENTITIES, *INCREMENTAL_DATASETS]
    for dataset in plan[: plan.index(TARGET) + 1]:
        assert env.control(dataset) == clean.control(dataset), dataset
        assert bronze_records(env.bronze_dir, dataset) == bronze_records(clean.bronze_dir, dataset), dataset


def _plan(env, name=TARGET):
    return next(p for p in resolve_plan(env.config) if p.name == name)


def _crash_in_second_window(tmp_path):
    env = make_env(tmp_path)
    first, second, third = _three_cuts(env)
    env.run(first)
    with pytest.raises(SimulatedCrash):
        env.run(second, hook=crash_hook(STAGE_BRONZE_COMMITTED, TARGET))
    return env, first, second, third


def _refused_without_landing(env, result, match):
    outcome = result.dataset(TARGET)
    assert outcome.status == STATUS_FAILED and outcome.error["type"] == "ControlStateMismatchError"
    assert match in outcome.error["message"], outcome.error["message"]
    return outcome


def test_adoption_refused_when_the_current_source_no_longer_matches_the_committed_window(tmp_path):
    """QA: before adopting an interrupted window, it is re-selected from the
    CURRENT source; a source edited in place (same updated_at, different
    content) must not be adopted silently."""
    env, _first, second, third = _crash_in_second_window(tmp_path)
    control_before = env.control(TARGET)
    low = parse_ts(control_before["watermark"])
    data = (env.batch_dir / f"{TARGET}.jsonl").read_bytes()
    lines = data.split(b"\n")
    index = next(
        i for i, line in enumerate(lines)
        if line.strip() and low < parse_ts(json.loads(line)["updated_at"]) <= second
    )
    row = json.loads(lines[index])
    row["arrival_delay_seconds"] += 1  # in-place edit, updated_at unchanged
    lines[index] = json.dumps(row).encode()
    rewrite_source_file(env.batch_dir, TARGET, b"\n".join(lines))
    target_runs = [r for r in committed_run_ids(env.bronze_dir) if r.startswith(f"inc-{TARGET}-")]
    result = env.run(third)
    _refused_without_landing(env, result, "current source")
    assert [r for r in committed_run_ids(env.bronze_dir) if r.startswith(f"inc-{TARGET}-")] == target_runs
    assert env.control(TARGET) == control_before


def test_adoption_refused_when_no_control_record_exists_qa_repro(tmp_path):
    """QA repro: load T1; delete control/ but keep Bronze; regenerate DE-04
    with relief_pool_size=5; run T2. 9a7eac2 adopted every dataset's T1 run
    silently (51 schedules rows kept a stale depot_id). Lost control means
    operator action: every dataset is refused and nothing is landed."""
    t1, t2 = datetime(2025, 1, 12, tzinfo=UTC), datetime(2025, 1, 13, tzinfo=UTC)
    env = make_env(tmp_path, service_days=7, profile="default")
    assert env.run(t1).status == "SUCCEEDED"
    shutil.rmtree(env.state_dir / "control")
    old = {pk(r, "schedules"): r for r in source_rows(env.batch_dir, "schedules")}
    write_batch_source(env.batch_dir, service_days=7, profile="default", overrides={"fleet_changes": {"relief_pool_size": 5}})
    stale = [r for r in source_rows(env.batch_dir, "schedules") if pk(r, "schedules") in old and old[pk(r, "schedules")] != r]
    assert stale and all(old[pk(r, "schedules")]["updated_at"] == r["updated_at"] for r in stale)  # guard
    runs_before = committed_run_ids(env.bronze_dir)
    result = env.run(t2)
    assert result.status == "FAILED"
    for outcome in result.datasets:
        assert outcome.status == STATUS_FAILED and outcome.error["type"] == "ControlStateMismatchError", outcome
        assert "no control record" in outcome.error["message"]
    assert committed_run_ids(env.bronze_dir) == runs_before
    assert not (env.state_dir / "control").exists() or not list((env.state_dir / "control").glob("*.json"))


def test_initial_window_crash_then_later_as_of_is_refused_with_a_same_as_of_recovery_path(tmp_path, uninterrupted):
    env = make_env(tmp_path)
    first, second = _cuts(env)
    with pytest.raises(SimulatedCrash):
        env.run(first, hook=crash_hook(STAGE_BRONZE_COMMITTED, TARGET))
    assert env.control(TARGET) is None
    result = env.run(second)
    _refused_without_landing(env, result, "same --as-of")
    assert env.control(TARGET) is None
    # Operator recovery: rerun only that dataset with the interrupted as_of
    # (the others already moved past it), then continue normally.
    narrow = env.with_config(include_unlisted_catalogue_datasets=False, datasets=[{"name": TARGET, "mode": "incremental"}])
    assert narrow.run(first).dataset(TARGET).status == STATUS_ALREADY_COMMITTED
    assert env.run(second).dataset(TARGET).status == STATUS_COMMITTED
    (expected_records, _), expected_count = uninterrupted[1]
    assert bronze_record_count(env.bronze_dir) == expected_count
    assert bronze_records(env.bronze_dir, TARGET) == expected_records[TARGET]


def test_unrecorded_run_whose_extract_disagrees_with_bronze_is_refused(tmp_path):
    env, _first, second, third = _crash_in_second_window(tmp_path)
    control_before = env.control(TARGET)
    run_id = bronze_run_id_for(env.config, _plan(env), parse_ts(control_before["watermark"]), second)
    extract = env.state_dir / "extracts" / TARGET / run_id / f"{TARGET}.jsonl"
    extract.write_bytes(extract.read_bytes() + b"{}\n")
    count_after_crash = bronze_record_count(env.bronze_dir)
    result = env.run(third)
    _refused_without_landing(env, result, "does not match its extract")
    landed_by_others = sum(
        r.extracted_count or 0 for r in result.datasets if r.dataset != TARGET and r.status == STATUS_COMMITTED
    )
    assert bronze_record_count(env.bronze_dir) == count_after_crash + landed_by_others  # nothing extra for TARGET
    assert env.control(TARGET) == control_before


def test_unrecorded_run_without_its_extract_is_refused_with_guidance(tmp_path):
    env, _first, second, third = _crash_in_second_window(tmp_path)
    control_before = env.control(TARGET)
    shutil.rmtree(env.state_dir / "extracts" / TARGET)
    result = env.run(third)
    _refused_without_landing(env, result, "same --as-of")
    assert env.control(TARGET) == control_before
    # The other datasets have moved past `second`, so the whole-pipeline
    # rerun at `second` is refused; the single-dataset rerun recovers.
    with pytest.raises(AsOfError, match="earlier than the last committed as_of"):
        env.run(second)
    narrow = env.with_config(include_unlisted_catalogue_datasets=False, datasets=[{"name": TARGET, "mode": "incremental"}])
    assert narrow.run(second).dataset(TARGET).status == STATUS_ALREADY_COMMITTED
    assert env.run(third).status == "SUCCEEDED"
    assert_bronze_matches_source(env.bronze_dir, env.batch_dir, third)


def test_recovery_path_with_same_as_of_then_next_window(tmp_path, uninterrupted):
    env = make_env(tmp_path)
    first, second = _cuts(env)
    with pytest.raises(SimulatedCrash):
        env.run(first, hook=crash_hook(STAGE_BRONZE_COMMITTED, TARGET))
    assert env.run(first).dataset(TARGET).status == STATUS_ALREADY_COMMITTED
    env.run(second)
    (expected_records, expected_control), expected_count = uninterrupted[1]
    assert bronze_record_count(env.bronze_dir) == expected_count
    assert _snapshot(env) == (expected_records, expected_control)


def test_lost_control_record_after_one_window_is_refused_then_rebuilt_by_the_same_as_of(tmp_path):
    env = make_env(tmp_path)
    first, second = _cuts(env)
    env.run(first)
    expected_control = env.control(TARGET)
    (env.state_dir / "control" / f"{TARGET}.json").unlink()
    runs_before = committed_run_ids(env.bronze_dir)
    result = env.run(second)
    _refused_without_landing(env, result, "no control record")
    assert [r for r in committed_run_ids(env.bronze_dir) if r.startswith(f"inc-{TARGET}-")] == [
        r for r in runs_before if r.startswith(f"inc-{TARGET}-")
    ]
    narrow = env.with_config(include_unlisted_catalogue_datasets=False, datasets=[{"name": TARGET, "mode": "incremental"}])
    assert narrow.run(first).dataset(TARGET).status == STATUS_ALREADY_COMMITTED
    assert env.control(TARGET) == expected_control
    assert env.run(second).dataset(TARGET).status == STATUS_COMMITTED
    keys = bronze_keys(env.bronze_dir, TARGET)
    assert keys == expected_keys(env.batch_dir, TARGET, second) and max(keys.values()) == 1


def test_lost_control_record_after_several_windows_is_refused_not_reloaded(tmp_path):
    env = make_env(tmp_path)
    first, second = _cuts(env)
    env.run(first)
    env.run(second)
    (env.state_dir / "control" / f"{TARGET}.json").unlink()
    target_runs_before = [r for r in committed_run_ids(env.bronze_dir) if r.startswith(f"inc-{TARGET}-")]
    result = env.run(second + timedelta(days=1))
    assert result.dataset(TARGET).error["type"] == "ControlStateMismatchError"
    # Not silently re-extracted from the beginning (which would duplicate rows).
    assert [r for r in committed_run_ids(env.bronze_dir) if r.startswith(f"inc-{TARGET}-")] == target_runs_before


def test_recorded_run_missing_from_bronze_is_detected(tmp_path):
    env = make_env(tmp_path)
    first, second = _cuts(env)
    env.run(first)
    run_id = env.control(TARGET)["last_bronze_run_id"]
    (env.bronze_dir / "_runs" / f"{run_id}.json").unlink()
    result = env.run(second)
    assert result.dataset(TARGET).error["type"] == "ControlStateMismatchError"
    assert env.control(TARGET)["last_as_of"] == format_ts(first)
