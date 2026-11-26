"""DE-12 criterion 7: k incremental DE-11 windows followed by batch Silver
give current-state tables identical (apart from lineage columns) to one load
at the final `as_of` - using the real `simulator.incremental` and
`simulator.bronze` libraries, for several cut sequences, with Silver run once
at the end or after every window, and with source growth (DE-04
regenerated with more `service_days`, identical config otherwise)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from simulator.silver_batch import SILVER_DATASETS
from simulator.tests._incremental_helpers import (
    all_watermarks,
    source_rows,
    write_batch_source,
)
from simulator.tests._silver_batch_helpers import (
    FIXED_NOW,
    SbEnv,
    make_sb_env,
    manifest,
    pk,
)

FINAL = FIXED_NOW


def _cut_sequences(values: list) -> dict[str, list]:
    n = len(values)
    return {
        "exact_values": [values[n // 4], values[n // 2], values[(3 * n) // 4], FINAL],
        "between_values_and_empty_windows": [
            values[0] - timedelta(days=30),  # before every row: empty windows everywhere
            values[n // 3] + timedelta(microseconds=1),
            values[n // 3] + timedelta(microseconds=2),  # empty window
            values[(2 * n) // 3] + timedelta(seconds=1),
            FINAL,
        ],
        "day_boundaries": sorted({v.replace(hour=0, minute=0, second=0, microsecond=0) for v in values})[1:] + [FINAL],
    }


def _reference_state(root: Path, service_days: int) -> dict[str, list[dict]]:
    env = make_sb_env(root, service_days=service_days)
    env.load(FINAL)
    env.silver("once")
    return env.business_tables()


def _assert_tables_equal(env: SbEnv, expected: dict[str, list[dict]]) -> None:
    actual = env.business_tables()
    for dataset in SILVER_DATASETS:
        assert actual[dataset] == expected[dataset], dataset
        assert actual[dataset], f"{dataset}: empty table - the comparison would be vacuous"


@pytest.mark.parametrize("sequence", ["exact_values", "between_values_and_empty_windows", "day_boundaries"])
@pytest.mark.parametrize("silver_each_window", [False, True])
def test_k_windows_then_silver_equal_one_load_at_the_final_as_of(
    tmp_path: Path, sequence: str, silver_each_window: bool
) -> None:
    expected = _reference_state(tmp_path / "single", service_days=2)
    env = make_sb_env(tmp_path / "windows", service_days=2)
    cuts = _cut_sequences(all_watermarks(env.inc.batch_dir))[sequence]
    assert cuts == sorted(cuts) and len(cuts) >= 3
    for index, cut in enumerate(cuts):
        env.load(cut)
        if silver_each_window:
            env.silver(f"w{index}")
    if not silver_each_window:
        env.silver("final")
    _assert_tables_equal(env, expected)
    # and every source row is there exactly once (oracle from the DE-04 files)
    for dataset in ("historical_trips", "service_disruptions", "vehicle_depot_assignments"):
        keys = sorted(pk(dataset, r) for r in source_rows(env.inc.batch_dir, dataset))
        assert sorted(pk(dataset, r) for r in env.table(dataset)) == keys


def test_growth_regenerated_with_more_service_days_equals_one_load(tmp_path: Path) -> None:
    expected = _reference_state(tmp_path / "single", service_days=3)
    env = make_sb_env(tmp_path / "windows", service_days=1)
    one_day = all_watermarks(env.inc.batch_dir)
    env.load(one_day[len(one_day) // 2])
    env.silver("w0")
    env.load(one_day[-1])
    env.silver("w1")
    write_batch_source(env.inc.batch_dir, 3)  # DE-04 grows: identical config apart from service_days
    three_days = all_watermarks(env.inc.batch_dir)
    env.load(three_days[(2 * len(three_days)) // 3])
    env.silver("w2")
    env.load(FINAL)
    m = env.silver("w3").manifest
    assert m["reconciliation"]["holds"] is True
    _assert_tables_equal(env, expected)
    # nothing was ever stale/updated/conflicting: each source row merged once
    totals = {k: 0 for k in ("updated", "stale", "conflicts", "unchanged")}
    for run_id in ("w0", "w1", "w2", "w3"):
        for key in totals:
            totals[key] += manifest(env.output_dir, run_id)["totals"][key]
    assert totals == {"updated": 0, "stale": 0, "conflicts": 0, "unchanged": 0}
