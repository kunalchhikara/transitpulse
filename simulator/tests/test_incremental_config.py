"""DE-11 criterion 1: per-dataset mode and order in config, catalogue-driven
dataset list, config validation; deterministic Bronze run ids."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from simulator.batch import DATASET_CATALOGUE
from simulator.bronze import BronzeStore
from simulator.incremental import (
    BadSourcePolicy,
    IncrementalConfig,
    LoadMode,
    RetryConfig,
    bronze_run_id_for,
    load_incremental_config,
    resolve_plan,
)
from simulator.incremental.config import DatasetPlan
from simulator.tests._incremental_helpers import REFERENCE_ENTITIES


def _data(**overrides):
    data = load_incremental_config().model_dump()
    data.update(overrides)
    return data


def test_default_plan_is_reference_full_then_every_catalogue_dataset_incremental():
    plan = resolve_plan(load_incremental_config())
    assert [p.name for p in plan] == [*REFERENCE_ENTITIES, *DATASET_CATALOGUE]
    for p in plan[: len(REFERENCE_ENTITIES)]:
        assert p.mode is LoadMode.FULL and p.source_file == f"{p.name}.json" and p.primary_key
        assert p.watermark_column is None
    for p in plan[len(REFERENCE_ENTITIES):]:
        spec = DATASET_CATALOGUE[p.name]
        assert p.mode is LoadMode.INCREMENTAL
        assert p.watermark_column == spec.watermark_column
        assert p.primary_key == tuple(spec.primary_key)


def test_default_policies():
    config = load_incremental_config()
    assert config.bad_source_policy is BadSourcePolicy.STRICT
    assert config.retry.max_attempts >= 2
    assert config.verify_source_manifest is True


def test_configured_order_comes_first_and_unlisted_catalogue_datasets_are_appended():
    names = list(DATASET_CATALOGUE)
    chosen = [names[-1], names[0]]
    config = IncrementalConfig.model_validate(
        _data(datasets=[{"name": n, "mode": "incremental"} for n in chosen])
    )
    plan = [p.name for p in resolve_plan(config)]
    assert plan[:2] == chosen
    assert plan[2:] == [n for n in names if n not in chosen]


def test_exclusions_and_unlisted_switch():
    names = list(DATASET_CATALOGUE)
    config = IncrementalConfig.model_validate(_data(exclude_datasets=[names[1]]))
    assert names[1] not in [p.name for p in resolve_plan(config)]
    config = IncrementalConfig.model_validate(_data(include_unlisted_catalogue_datasets=False))
    assert [p.name for p in resolve_plan(config)] == list(REFERENCE_ENTITIES)


@pytest.mark.parametrize(
    "entry",
    [
        {"name": next(iter(DATASET_CATALOGUE)), "mode": "full", "source_file": "x.json", "primary_key": ["k"]},
        {"name": "not_in_catalogue", "mode": "incremental"},
        {"name": "depots2", "mode": "full", "primary_key": ["depot_id"]},
        {"name": "depots2", "mode": "full", "source_file": "../depots.json", "primary_key": ["depot_id"]},
        {"name": "depots2", "mode": "full", "source_file": "depots.json", "primary_key": []},
        {"name": next(iter(DATASET_CATALOGUE)), "mode": "incremental", "primary_key": ["x"]},
        {"name": "../evil", "mode": "full", "source_file": "a.json", "primary_key": ["k"]},
        {"name": "depots2", "mode": "sometimes", "source_file": "a.json", "primary_key": ["k"]},
    ],
)
def test_invalid_dataset_entries_are_rejected(entry):
    data = _data()
    data["datasets"] = [*data["datasets"], entry]
    with pytest.raises(ValidationError):
        IncrementalConfig.model_validate(data)


@pytest.mark.parametrize(
    "overrides",
    [
        {"exclude_datasets": ["nope"]},
        {"incremental_run_id_prefix": "full"},
        {"full_run_id_prefix": "bad-prefix"},
        {"bad_source_policy": "lenient"},
        {"late_arrival_policy": "ignore"},
        {"unknown_key": 1},
        {"source_manifest_file": "../manifest.json"},
        {"include_unlisted_catalogue_datasets": False, "datasets": []},
    ],
)
def test_invalid_config_is_rejected(overrides):
    with pytest.raises(ValidationError):
        IncrementalConfig.model_validate(_data(**overrides))


def test_duplicate_dataset_names_rejected():
    data = _data()
    data["datasets"] = [*data["datasets"], data["datasets"][0]]
    with pytest.raises(ValidationError):
        IncrementalConfig.model_validate(data)


def test_backoff_schedule_follows_config():
    retry = RetryConfig(max_attempts=5, initial_backoff_seconds=1.5, backoff_multiplier=3, max_backoff_seconds=10)
    assert [retry.backoff_for(n) for n in (1, 2, 3, 4)] == [1.5, 4.5, 10, 10]
    with pytest.raises(ValueError):
        retry.backoff_for(0)


def test_bronze_run_id_is_deterministic_and_valid_for_worst_case_names(tmp_path):
    config = load_incremental_config()
    low = datetime(2025, 1, 6, 0, 0, 0, 1, tzinfo=UTC)
    high = datetime(2025, 1, 7, 23, 59, 59, 999999, tzinfo=UTC)
    store = BronzeStore(tmp_path)
    longest = "x" * 64
    for plan in (
        DatasetPlan(longest, LoadMode.INCREMENTAL, ("k",), "updated_at", "event_date", None),
        DatasetPlan(longest, LoadMode.FULL, ("k",), None, None, "x.json"),
    ):
        for window_low in (None, low):
            run_id = bronze_run_id_for(config, plan, window_low, high)
            assert run_id == bronze_run_id_for(config, plan, window_low, high)
            store.manifest_path(run_id)  # raises InvalidRunIdError if not a valid Bronze run id
    inc = DatasetPlan("schedules", LoadMode.INCREMENTAL, ("trip_id",), "updated_at", "event_date", None)
    assert bronze_run_id_for(config, inc, None, high) != bronze_run_id_for(config, inc, low, high)
    assert bronze_run_id_for(config, inc, low, high) == "inc-schedules-20250106T000000.000001Z-20250107T235959.999999Z"
