"""DE-11 criterion 2: window boundaries and `as_of` refusal rules."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from simulator.incremental import STATUS_UP_TO_DATE, AsOfError, ManualClock, format_ts
from simulator.tests._incremental_helpers import (
    INCREMENTAL_DATASETS,
    all_watermarks,
    bronze_keys,
    bronze_records,
    expected_keys,
    make_env,
    pk,
    source_rows,
    source_watermark,
    strip,
    tree_bytes,
)


def test_cut_equal_to_an_updated_at_value_includes_it_and_the_next_window_excludes_it(tmp_path):
    env = make_env(tmp_path)
    dataset = "historical_telemetry"
    values = sorted({source_watermark(r, dataset) for r in source_rows(env.batch_dir, dataset)})
    cut = values[len(values) // 2]
    at_cut = expected_keys(env.batch_dir, dataset, cut, cut - timedelta(microseconds=1))
    assert at_cut, "fixture guard"
    env.run(cut)
    assert set(at_cut) <= set(bronze_keys(env.bronze_dir, dataset))
    result = env.run(values[-1])
    run_id = result.dataset(dataset).bronze_run_id
    second_window = {pk(strip(r), dataset) for r in bronze_records(env.bronze_dir, dataset) if r["run_id"] == run_id}
    assert not second_window & set(at_cut)
    assert max(bronze_keys(env.bronze_dir, dataset).values()) == 1


def test_as_of_earlier_than_the_last_committed_as_of_is_refused_with_no_state_change(tmp_path):
    env = make_env(tmp_path)
    values = all_watermarks(env.batch_dir)
    env.run(values[-1])
    before = (tree_bytes(env.state_dir), tree_bytes(env.bronze_dir))
    with pytest.raises(AsOfError, match="earlier than the last committed as_of"):
        env.run(values[0])
    assert (tree_bytes(env.state_dir), tree_bytes(env.bronze_dir)) == before


def test_regression_for_a_single_dataset_refuses_the_whole_run(tmp_path):
    env = make_env(tmp_path)
    values = all_watermarks(env.batch_dir)
    narrow = env.with_config(include_unlisted_catalogue_datasets=False, datasets=[{"name": "schedules", "mode": "incremental"}])
    narrow.run(values[-1])
    before = (tree_bytes(env.state_dir), tree_bytes(env.bronze_dir))
    with pytest.raises(AsOfError, match="schedules"):
        env.run(values[0])
    assert (tree_bytes(env.state_dir), tree_bytes(env.bronze_dir)) == before


def test_naive_as_of_is_refused(tmp_path):
    env = make_env(tmp_path)
    with pytest.raises(AsOfError):
        env.run(datetime(2025, 1, 6, 12, 0, 0))
    assert tree_bytes(env.state_dir) == {}


def test_as_of_later_than_the_clock_is_refused(tmp_path):
    env = make_env(tmp_path)
    as_of = all_watermarks(env.batch_dir)[-1]
    with pytest.raises(AsOfError, match="later than the clock"):
        env.run(as_of, clock=ManualClock(as_of - timedelta(seconds=1)))
    assert tree_bytes(env.state_dir) == {} and tree_bytes(env.bronze_dir) == {}


def test_as_of_equal_to_the_last_committed_as_of_is_up_to_date(tmp_path):
    env = make_env(tmp_path)
    as_of = all_watermarks(env.batch_dir)[3]
    env.run(as_of)
    watermarks = {d: env.control(d)["watermark"] for d in INCREMENTAL_DATASETS}
    result = env.run(as_of)
    for dataset in INCREMENTAL_DATASETS:
        assert result.dataset(dataset).status == STATUS_UP_TO_DATE
        assert result.dataset(dataset).watermark_after == watermarks[dataset]
        assert result.dataset(dataset).last_as_of_after == format_ts(as_of)
        audit = env.audit_records(result.pipeline_run_id, dataset)
        assert len(audit) == 1 and audit[0]["status"] == STATUS_UP_TO_DATE
