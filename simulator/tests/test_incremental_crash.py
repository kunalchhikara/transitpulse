"""DE-11 criterion 4: crash-safe idempotency. A `SimulatedCrash`
(BaseException = process death: nothing audited, nothing cleaned up) is
injected after the extract, inside the Bronze commit, after the Bronze commit
(before the watermark write) and after the watermark write (before the audit
record). Every rerun with the same `as_of` must end with zero extra Bronze
records and the same control state as an uninterrupted run."""

from __future__ import annotations

import shutil
from datetime import timedelta

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
    format_ts,
)
from simulator.tests._incremental_helpers import (
    INCREMENTAL_DATASETS,
    REFERENCE_ENTITIES,
    SimulatedCrash,
    all_watermarks,
    bronze_keys,
    bronze_record_count,
    bronze_records,
    committed_run_ids,
    expected_keys,
    make_env,
    tree_bytes,
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


def test_later_as_of_after_an_unrecorded_commit_adopts_it_and_matches_uninterrupted(tmp_path, uninterrupted):
    """A scheduler's next trigger (a later as_of) after a crash past the
    Bronze commit: the committed window is adopted, not re-extracted."""
    env = make_env(tmp_path)
    first, second = _cuts(env)
    with pytest.raises(SimulatedCrash):
        env.run(first, hook=crash_hook(STAGE_BRONZE_COMMITTED, TARGET))
    assert env.control(TARGET) is None
    result = env.run(second)
    outcome = result.dataset(TARGET)
    assert result.status == "SUCCEEDED" and outcome.status == STATUS_COMMITTED
    audit = env.audit_records(result.pipeline_run_id, TARGET)[0]
    assert audit["recovered_bronze_run_id"] == f"inc-{TARGET}-initial-{first:%Y%m%dT%H%M%S}Z"
    # Watermark before the attempt was unset; the new window starts at the adopted window's end.
    assert audit["watermark_before"] is None and audit["window_low"] == format_ts(first)
    (_, expected_control), expected_count = uninterrupted[1]
    assert bronze_record_count(env.bronze_dir) == expected_count  # nothing landed twice
    for dataset in INCREMENTAL_DATASETS:
        keys = bronze_keys(env.bronze_dir, dataset)
        assert keys == expected_keys(env.batch_dir, dataset, second) and max(keys.values()) == 1
    # TARGET and every dataset before it went through the same two windows as
    # the uninterrupted run, so their control records are byte-identical.
    # (Datasets after TARGET never ran at `first`; they loaded one window.)
    _, control = _snapshot(env)
    plan = [*REFERENCE_ENTITIES, *INCREMENTAL_DATASETS]
    for dataset in plan[: plan.index(TARGET) + 1]:
        assert control[f"{dataset}.json"] == expected_control[f"{dataset}.json"], dataset


def test_unrecorded_run_whose_extract_disagrees_with_bronze_is_refused(tmp_path):
    env = make_env(tmp_path)
    first, second = _cuts(env)
    with pytest.raises(SimulatedCrash):
        env.run(first, hook=crash_hook(STAGE_BRONZE_COMMITTED, TARGET))
    extract = next((env.state_dir / "extracts" / TARGET).glob("*/historical_trips.jsonl"))
    extract.write_bytes(extract.read_bytes() + b"{}\n")
    count_after_crash = bronze_record_count(env.bronze_dir)
    result = env.run(second)
    outcome = result.dataset(TARGET)
    assert outcome.status == STATUS_FAILED and outcome.error["type"] == "ControlStateMismatchError"
    assert "does not match its extract" in outcome.error["message"]
    landed_by_others = sum(
        r.extracted_count or 0 for r in result.datasets if r.dataset != TARGET and r.status == STATUS_COMMITTED
    )
    assert bronze_record_count(env.bronze_dir) == count_after_crash + landed_by_others  # nothing extra for TARGET
    assert env.control(TARGET) is None


def test_unrecorded_run_without_its_extract_is_refused_with_guidance(tmp_path):
    env = make_env(tmp_path)
    first, second = _cuts(env)
    with pytest.raises(SimulatedCrash):
        env.run(first, hook=crash_hook(STAGE_BRONZE_COMMITTED, TARGET))
    shutil.rmtree(env.state_dir / "extracts" / TARGET)
    result = env.run(second)
    outcome = result.dataset(TARGET)
    assert outcome.error["type"] == "ControlStateMismatchError"
    assert "same --as-of" in outcome.error["message"]
    assert env.control(TARGET) is None
    # The guidance works while the other datasets have not moved past `first`
    # ... here they have, so the whole-pipeline rerun at `first` is refused:
    with pytest.raises(AsOfError, match="earlier than the current watermark"):
        env.run(first)


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


def test_lost_control_record_after_one_window_is_rebuilt_without_duplicates(tmp_path):
    env = make_env(tmp_path)
    first, second = _cuts(env)
    env.run(first)
    (env.state_dir / "control" / f"{TARGET}.json").unlink()
    result = env.run(second)
    assert result.dataset(TARGET).status == STATUS_COMMITTED
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
    assert env.control(TARGET)["watermark"] != result.as_of
