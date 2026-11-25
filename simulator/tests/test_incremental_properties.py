"""DE-11 criterion 7: no loss / no duplicates over several `as_of` cut
sequences (cuts exactly equal to `updated_at` values, cuts between values,
repeated cuts, empty windows), and across a source regenerated with more
`service_days` (same DE-04 config otherwise) between runs. Expected key sets
are computed directly from the source files."""

from __future__ import annotations

import json
import random
from collections import Counter
from datetime import timedelta

import pytest

from simulator.incremental import STATUS_UP_TO_DATE
from simulator.tests._incremental_helpers import (
    INCREMENTAL_DATASETS,
    all_watermarks,
    bronze_keys,
    bronze_records,
    expected_keys,
    make_env,
    parse_ts,
    pk,
    source_rows,
    strip,
    write_batch_source,
)

SERVICE_DAYS = 3


def _sequences(values):
    rng = random.Random(20250106)
    first, last = values[0], values[-1]
    exact = sorted(rng.sample(values, 6))
    between = sorted(v + timedelta(microseconds=rng.randint(1, 999_999)) for v in rng.sample(values[:-1], 6))
    return {
        "exact-values-with-repeat": [*exact[:3], exact[2], *exact[3:], last],
        "between-values-with-empty-windows": [first - timedelta(days=30), *between, last + timedelta(days=1),
                                              last + timedelta(days=2)],
        "mixed": sorted({*exact[::2], *between[1::2]}) + [last],
        "single-initial-load": [last],
        "every-day-boundary": sorted({v.replace(hour=0, minute=0, second=0) for v in values}) + [last],
    }


def _assert_no_loss_no_duplicates(env, final_as_of):
    for dataset in INCREMENTAL_DATASETS:
        landed = bronze_keys(env.bronze_dir, dataset)
        expected = expected_keys(env.batch_dir, dataset, final_as_of)
        assert landed == expected, dataset
        assert not landed or max(landed.values()) == 1, f"{dataset}: a key was extracted twice"


def _assert_each_row_in_its_window(env, dataset):
    windows = {}
    for path in env.state_dir.glob(f"audit/*/{dataset}/attempt-*.json"):
        record = json.loads(path.read_text())
        if record["bronze_run_id"]:
            windows[record["bronze_run_id"]] = (record["window_low"], record["window_high"])
    for record in bronze_records(env.bronze_dir, dataset):
        low, high = windows[record["run_id"]]
        updated = parse_ts(strip(record)["updated_at"])
        assert (low is None or parse_ts(low) < updated) and updated <= parse_ts(high)


@pytest.fixture(scope="module")
def sequences(tmp_path_factory):
    env = make_env(tmp_path_factory.mktemp("probe"), service_days=SERVICE_DAYS)
    return _sequences(all_watermarks(env.batch_dir))


@pytest.mark.parametrize(
    "name",
    ["exact-values-with-repeat", "between-values-with-empty-windows", "mixed", "single-initial-load",
     "every-day-boundary"],
)
def test_no_loss_no_duplicates_over_cut_sequences(tmp_path, sequences, name):
    env = make_env(tmp_path, service_days=SERVICE_DAYS)
    cuts = sequences[name]
    assert len(cuts) >= 1
    if name == "exact-values-with-repeat":
        assert len(set(cuts)) < len(cuts)  # guard: really repeats a cut
    statuses = []
    for cut in cuts:
        result = env.run(cut)
        assert result.status == "SUCCEEDED", (cut, [(r.dataset, r.error) for r in result.datasets if r.error])
        statuses.extend(r.status for r in result.datasets)
        # Invariant holds after every cut, not just the last.
        _assert_no_loss_no_duplicates(env, cut)
    if name == "exact-values-with-repeat":
        assert STATUS_UP_TO_DATE in statuses
    for dataset in INCREMENTAL_DATASETS:
        _assert_each_row_in_its_window(env, dataset)


def test_guard_sequences_include_cuts_equal_to_values_and_empty_windows(tmp_path, sequences):
    env = make_env(tmp_path, service_days=SERVICE_DAYS)
    values = set(all_watermarks(env.batch_dir))
    assert set(sequences["exact-values-with-repeat"]) <= values
    assert not set(sequences["between-values-with-empty-windows"][1:-3]) & values
    # The last two cuts of that sequence are past every row: empty windows.
    assert sequences["between-values-with-empty-windows"][-2] > max(values)


def test_no_loss_no_duplicates_when_the_source_grows_between_runs(tmp_path):
    """DE-04 is prefix-stable in `service_days`; the source is regenerated in
    place with an otherwise identical config. A cut is valid for a source
    only up to its completeness horizon (the earliest `updated_at` the next
    regeneration adds - DE-04 publishes timetables days ahead)."""
    env = make_env(tmp_path, service_days=1)
    small = {d: source_rows(env.batch_dir, d) for d in INCREMENTAL_DATASETS}
    probe = tmp_path / "probe"
    write_batch_source(probe, service_days=SERVICE_DAYS)
    big = {d: source_rows(probe, d) for d in INCREMENTAL_DATASETS}
    for dataset in INCREMENTAL_DATASETS:  # guard: prefix-stable source
        rows = [json.dumps(r, sort_keys=True) for r in big[dataset]]
        assert Counter(json.dumps(r, sort_keys=True) for r in small[dataset]) <= Counter(rows), dataset
    new = [
        parse_ts(row["updated_at"])
        for d in INCREMENTAL_DATASETS
        for row in big[d]
        if pk(row, d) not in {pk(r, d) for r in small[d]}
    ]
    horizon = min(new)
    small_values = [v for v in all_watermarks(env.batch_dir) if v < horizon]
    assert small_values, "guard: the 1-day source has rows below its horizon"

    cut1 = small_values[-1]  # exactly equal to an updated_at value
    assert env.run(cut1).status == "SUCCEEDED"
    _assert_no_loss_no_duplicates(env, cut1)

    write_batch_source(env.batch_dir, service_days=SERVICE_DAYS)  # regenerate, more days
    values = all_watermarks(env.batch_dir)
    cut2 = values[len(values) // 2]
    for cut in (cut2, values[-1]):
        result = env.run(cut)
        assert result.status == "SUCCEEDED", [(r.dataset, r.error) for r in result.datasets if r.error]
        for dataset in INCREMENTAL_DATASETS:
            audit = env.audit_records(result.pipeline_run_id, dataset)[-1]
            assert audit["late_arrival_check"]["holds"] is True  # no false positive
        _assert_no_loss_no_duplicates(env, cut)
