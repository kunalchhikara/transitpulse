"""DE-11 criteria 2 and 7 after the QA FAIL of PR #17 (Lead decision D1):
the per-dataset high-watermark is the maximum `updated_at` actually
committed, so a DE-04 source regenerated with more `service_days` at
*realistic* cuts (past the cross-dataset publication horizon) loses nothing.

Covers: QA's exact growth repro on the default DE-04 config; >= 3 cut
sequences with growth between cuts; ties at the watermark (the stated
assumption, detected by the late-arrival recount); rows recovered between
the watermark and the last `as_of`; empty windows; the `as_of` refusal rule
against the last committed `as_of`; refusal of an old (9a7eac2) control
record. Expected key sets are computed from the DE-04 files only."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from simulator.incremental import AsOfError, StateError, format_ts
from simulator.tests._incremental_helpers import (
    INCREMENTAL_DATASETS,
    all_watermarks,
    assert_bronze_matches_source,
    bronze_keys,
    bronze_records,
    committed_run_ids,
    make_env,
    parse_ts,
    pk,
    rewrite_source_file,
    source_rows,
    source_watermark,
    strip,
    tree_bytes,
    write_batch_source,
)

SUCCESS = {"COMMITTED", "ALREADY_COMMITTED", "SKIPPED_UNCHANGED", "UP_TO_DATE"}
#: DE-04 datasets whose `updated_at` is back-dated before the service date
#: (timetables published `publish_lead_days` ahead, fleet changes recorded
#: `notice_days` ahead) - the ones 9a7eac2 lost on growth.
BACKDATED = ("schedules", "schedule_stop_times", "vehicle_depot_assignments")
QA_T1 = datetime(2025, 1, 12, tzinfo=UTC)
QA_T2 = datetime(2025, 1, 16, tzinfo=UTC)
QA_T3 = datetime(2025, 1, 17, tzinfo=UTC)


def _errors(result):
    return [(r.dataset, r.status, r.error) for r in result.datasets if r.status not in SUCCESS]


def _keys(batch_dir, dataset):
    return {pk(r, dataset) for r in source_rows(batch_dir, dataset)}


# -- QA repro: default DE-04 config, 7 -> 10 service days --------------------------


def test_qa_growth_repro_lands_every_row_at_realistic_cuts(tmp_path):
    """7-day default source loaded at 2025-01-12; source regenerated with
    service_days=10; loads at 2025-01-16 and 2025-01-17. On 9a7eac2 the
    second run was PARTIAL (LateArrivalError on the three back-dated
    datasets) and 510 schedules / 6,528 stop times / 3 assignments never
    landed."""
    env = make_env(tmp_path, service_days=7, profile="default")
    first = env.run(QA_T1)
    assert first.status == "SUCCEEDED", _errors(first)
    assert_bronze_matches_source(env.bronze_dir, env.batch_dir, QA_T1)
    before = {d: _keys(env.batch_dir, d) for d in INCREMENTAL_DATASETS}

    write_batch_source(env.batch_dir, service_days=10, profile="default")
    behind = {
        d: [r for r in source_rows(env.batch_dir, d) if pk(r, d) not in before[d] and source_watermark(r, d) <= QA_T1]
        for d in INCREMENTAL_DATASETS
    }
    # Guard: the regeneration really adds rows *behind* the first as_of for
    # the back-dated datasets (what broke 9a7eac2), and only for them.
    assert all(behind[d] for d in BACKDATED), {d: len(behind[d]) for d in BACKDATED}
    assert not any(behind[d] for d in INCREMENTAL_DATASETS if d not in BACKDATED)

    for number, cut in enumerate((QA_T2, QA_T3)):
        result = env.run(cut)
        assert result.status == "SUCCEEDED", _errors(result)
        for dataset in INCREMENTAL_DATASETS:
            assert result.dataset(dataset).status == "COMMITTED", dataset
            audit = env.audit_records(result.pipeline_run_id, dataset)[-1]
            assert audit["late_arrival_check"]["holds"] is True, dataset
            if number == 0:
                # The rows behind the previous as_of are visible in the audit.
                assert audit["late_rows_recovered"] == len(behind[dataset]), dataset
        assert_bronze_matches_source(env.bronze_dir, env.batch_dir, cut)
    for dataset in BACKDATED:
        assert set(bronze_keys(env.bronze_dir, dataset)) >= {pk(r, dataset) for r in behind[dataset]}


# -- >= 3 cut sequences with growth between cuts (sample config) -----------------

START = datetime(2025, 1, 6, tzinfo=UTC) - timedelta(hours=10)  # local midnight of start_date (UTC+10)


def _day_start(day):  # 1-based service day -> UTC instant of its local midnight
    return START + timedelta(days=day - 1)


def _resolve(spec, values, previous):
    kind = spec[0]
    if kind == "day_end":
        return _day_start(spec[1] + 1)
    if kind in ("exact_in_day", "between_in_day"):
        day, fraction = spec[1], spec[2]
        in_day = [v for v in values if _day_start(day) <= v < _day_start(day + 1)]
        value = in_day[int(fraction * (len(in_day) - 1))]
        if kind == "exact_in_day":
            assert value in values
            return value
        between = value + timedelta(microseconds=1)
        assert between not in values  # guard: really between two values
        return between
    if kind == "last":
        return values[-1]
    if kind == "after_last":
        return values[-1] + timedelta(days=spec[1])
    assert kind == "repeat"
    return previous


GROWTH_SEQUENCES = {
    "exact-values-with-repeat": [
        (1, ("exact_in_day", 1, 0.5)), (2, ("exact_in_day", 2, 0.3)), (2, ("repeat",)),
        (3, ("exact_in_day", 3, 0.7)), (4, ("last",)),
    ],
    "beyond-horizon-with-empty-windows": [
        (1, ("day_end", 1)), (1, ("after_last", 1)), (3, ("between_in_day", 3, 0.5)),
        (3, ("after_last", 2)), (6, ("day_end", 6)),
    ],
    "day-boundaries-and-between-values": [
        (2, ("between_in_day", 1, 0.9)), (2, ("day_end", 2)), (4, ("exact_in_day", 3, 0.1)),
        (4, ("day_end", 4)), (5, ("last",)),
    ],
}


@pytest.mark.parametrize("name", sorted(GROWTH_SEQUENCES))
def test_no_loss_no_duplicates_with_growth_between_cuts(tmp_path, name):
    env = make_env(tmp_path, service_days=GROWTH_SEQUENCES[name][0][0])
    days, previous, statuses, rows_behind = None, None, [], 0
    for service_days, spec in GROWTH_SEQUENCES[name]:
        if service_days != days:
            if days is not None:
                old = {d: _keys(env.batch_dir, d) for d in INCREMENTAL_DATASETS}
                write_batch_source(env.batch_dir, service_days=service_days)
                rows_behind += sum(
                    1
                    for d in INCREMENTAL_DATASETS
                    for r in source_rows(env.batch_dir, d)
                    if pk(r, d) not in old[d] and source_watermark(r, d) <= previous
                )
            days = service_days
        cut = _resolve(spec, all_watermarks(env.batch_dir), previous)
        assert previous is None or cut >= previous, (spec, cut, previous)  # guard: sequence is ordered
        result = env.run(cut)
        assert result.status == "SUCCEEDED", (spec, _errors(result))
        statuses.extend(r.status for r in result.datasets)
        assert_bronze_matches_source(env.bronze_dir, env.batch_dir, cut)  # after every cut
        previous = cut
    assert rows_behind > 0, "guard: growth must add rows behind an earlier cut"
    if name == "exact-values-with-repeat":
        assert "UP_TO_DATE" in statuses
    # Every landed row sits inside the window of the Bronze run that landed it.
    for dataset in INCREMENTAL_DATASETS:
        windows = {}
        for path in env.state_dir.glob(f"audit/*/{dataset}/attempt-*.json"):
            record = json.loads(path.read_text())
            if record["bronze_run_id"]:
                windows[record["bronze_run_id"]] = (record["window_low"], record["window_high"])
        for record in bronze_records(env.bronze_dir, dataset):
            low, high = windows[record["run_id"]]
            updated = parse_ts(strip(record)["updated_at"])
            assert (low is None or parse_ts(low) < updated) and updated <= parse_ts(high)


# -- watermark = max committed updated_at ------------------------------------------


def test_watermark_is_the_max_committed_updated_at_and_last_as_of_is_kept_separately(tmp_path):
    env = make_env(tmp_path)
    values = all_watermarks(env.batch_dir)
    cut = values[len(values) // 2] + timedelta(microseconds=1)  # between values
    env.run(cut)
    for dataset in INCREMENTAL_DATASETS:
        landed = [source_watermark(strip(r), dataset) for r in bronze_records(env.bronze_dir, dataset)]
        control = env.control(dataset)
        assert control["last_as_of"] == format_ts(cut)
        assert control["watermark"] == (format_ts(max(landed)) if landed else None), dataset
        assert control["watermark_semantics"] == "max_committed_updated_at"


def test_empty_window_keeps_the_watermark_records_the_run_and_reruns_are_idempotent(tmp_path):
    env = make_env(tmp_path)
    last = all_watermarks(env.batch_dir)[-1]
    env.run(last)
    watermarks = {d: env.control(d)["watermark"] for d in INCREMENTAL_DATASETS}
    later = last + timedelta(days=1)
    result = env.run(later)
    assert result.status == "SUCCEEDED"
    for dataset in INCREMENTAL_DATASETS:
        outcome = result.dataset(dataset)
        assert outcome.status == "COMMITTED" and outcome.extracted_count == 0
        assert outcome.watermark_after == outcome.watermark_before == watermarks[dataset]
        control = env.control(dataset)
        assert control["watermark"] == watermarks[dataset] and control["last_as_of"] == format_ts(later)
        audit = env.audit_records(result.pipeline_run_id, dataset)[-1]
        assert audit["source_rows_in_window"] == 0 and audit["bronze_records_written"] == 0
        assert audit["last_as_of_before"] == format_ts(last) and audit["last_as_of_after"] == format_ts(later)
        # The empty window is a committed (empty) Bronze run: auditable in Bronze too.
        assert outcome.bronze_run_id in committed_run_ids(env.bronze_dir)
    snapshot = tree_bytes(env.bronze_dir), tree_bytes(env.state_dir / "control")
    rerun = env.run(later)
    assert {r.status for r in rerun.datasets} == {"UP_TO_DATE"}
    assert (tree_bytes(env.bronze_dir), tree_bytes(env.state_dir / "control")) == snapshot
    # A further empty window gets its own run id (low = unchanged watermark).
    again = env.run(later + timedelta(days=1))
    assert again.status == "SUCCEEDED"
    ids = {r.bronze_run_id for r in again.datasets if r.dataset in INCREMENTAL_DATASETS}
    assert not ids & {r.bronze_run_id for r in result.datasets}


def test_as_of_between_watermark_and_last_as_of_is_refused(tmp_path):
    env = make_env(tmp_path)
    last = all_watermarks(env.batch_dir)[-1]
    env.run(last + timedelta(days=1))
    before = tree_bytes(env.state_dir), tree_bytes(env.bronze_dir)
    # Later than every watermark (max committed updated_at) but earlier than
    # the last committed as_of: refused, nothing written.
    with pytest.raises(AsOfError, match="earlier than the last committed as_of"):
        env.run(last + timedelta(hours=12))
    assert (tree_bytes(env.state_dir), tree_bytes(env.bronze_dir)) == before


# -- ties and rows behind the last as_of ----------------------------------------------

TIE_DATASET = "historical_trips"


def _add_row(env, dataset, updated_at, suffix):
    """Append a copy of a source row with a new primary key and the given
    `updated_at` (as the source system would write a late row)."""
    data = (env.batch_dir / f"{dataset}.jsonl").read_bytes()
    row = dict(source_rows(env.batch_dir, dataset)[0])
    row["trip_id"] = f"{row['trip_id']}-{suffix}"
    row["updated_at"] = updated_at
    rewrite_source_file(env.batch_dir, dataset, data + json.dumps(row, sort_keys=True).encode() + b"\n")
    return pk(row, TIE_DATASET)


def _tie_setup(env):
    values = all_watermarks(env.batch_dir)
    cut = values[len(values) // 2] + timedelta(minutes=1)
    env.run(cut)
    watermark = env.control(TIE_DATASET)["watermark"]
    assert parse_ts(watermark) < cut  # guard: watermark (max updated_at) is below as_of
    # Exact text of a source row's updated_at at the watermark.
    text = next(
        r["updated_at"] for r in source_rows(env.batch_dir, TIE_DATASET) if parse_ts(r["updated_at"]) == parse_ts(watermark)
    )
    return cut, watermark, text


def test_tie_at_the_watermark_appearing_after_commit_fails_loudly_by_default(tmp_path):
    """Assumption: once a value has been committed as the watermark, the
    source never adds another row with that same `updated_at`. A strict `>`
    window cannot see such a row; the late-arrival recount at `updated_at <=
    watermark` detects it."""
    env = make_env(tmp_path)
    cut, watermark, text = _tie_setup(env)
    key = _add_row(env, TIE_DATASET, text, "TIE")
    result = env.run(cut + timedelta(days=1))
    outcome = result.dataset(TIE_DATASET)
    assert result.status == "PARTIAL"
    assert outcome.status == "FAILED" and outcome.error["type"] == "LateArrivalError"
    audit = env.audit_records(result.pipeline_run_id, TIE_DATASET)[-1]
    assert audit["late_arrival_check"]["delta"] == 1
    assert env.control(TIE_DATASET)["watermark"] == watermark
    assert env.control(TIE_DATASET)["last_as_of"] == format_ts(cut)
    assert key not in bronze_keys(env.bronze_dir, TIE_DATASET)


def test_row_between_watermark_and_last_as_of_is_recovered_not_lost(tmp_path):
    """A row that appears after a run with `watermark < updated_at <=
    previous as_of` is picked up by the next window (max-updated_at
    semantics), and counted as `late_rows_recovered`."""
    env = make_env(tmp_path)
    cut, watermark, _ = _tie_setup(env)
    late = parse_ts(watermark) + (cut - parse_ts(watermark)) / 2
    key = _add_row(env, TIE_DATASET, format_ts(late), "LATE")
    result = env.run(cut + timedelta(days=1))
    assert result.status == "SUCCEEDED", _errors(result)
    audit = env.audit_records(result.pipeline_run_id, TIE_DATASET)[-1]
    assert audit["late_rows_recovered"] == 1
    assert audit["late_arrival_check"]["holds"] is True
    assert bronze_keys(env.bronze_dir, TIE_DATASET)[key] == 1


# -- control schema migration ----------------------------------------------------------


def test_control_record_from_the_window_upper_bound_version_is_refused(tmp_path):
    """9a7eac2 control records (schema 1.0.0, watermark = window upper
    bound) are never reinterpreted as max-updated_at watermarks."""
    env = make_env(tmp_path)
    values = all_watermarks(env.batch_dir)
    env.run(values[3])
    path = env.state_dir / "control" / "schedules.json"
    old = json.loads(path.read_text())
    old["control_schema_version"] = "1.0.0"
    old["watermark_semantics"] = "window_upper_bound"
    path.write_text(json.dumps(old))
    before = tree_bytes(env.state_dir), tree_bytes(env.bronze_dir)
    with pytest.raises(StateError, match=r"1\.0\.0.*window_upper_bound|window_upper_bound.*1\.0\.0"):
        env.run(values[-1])
    assert (tree_bytes(env.state_dir), tree_bytes(env.bronze_dir)) == before
