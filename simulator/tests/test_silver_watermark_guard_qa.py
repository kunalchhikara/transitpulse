"""DE-08 QA condition D1 (PR #16): the FU-06 watermark guard must be
STREAM-relative, not clock-relative. Every test here uses the DEFAULT Silver
configuration, and every expectation for the non-outlier events comes from
the independent oracle (`_silver_helpers.derive_expectations`: the DE-03
structured incident log + a clean DE-02 generator + delivered event_id order;
no Silver code).

QA's reproductions, as regression tests:

(a) a 2025-dated simulated stream ingested at a wall-clock-like (2026) Bronze
    ingestion time, plus ONE valid-looking outlier one day ahead of the
    stream -> the guard used to miss it (late 979 vs 53 baseline);
(b) the same with an outlier dated 2026-06-01 (still before the ingestion
    clock, so the clock-relative guard missed it too);
(c) the original 2090 outlier (caught before, but quarantined: valid-looking
    data is now loaded-and-flagged, see README "Watermark guard");
(d) a stream simulated from 2027-01-01, ingested at an earlier (2026) clock
    -> used to be quarantined wholesale (500/500);
(e) `--fixed-ingestion-time` earlier than the data (incl. DE-04 history
    ingested with a clock before it) -> used to be quarantined wholesale;
(f) with no outlier, the guard changes nothing (flags equal the oracle and
    the tables are byte-identical with the guard on and off).

(a) and (e) also run through the real incidents -> Bronze -> Silver CLIs."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime
from functools import lru_cache

import pytest

from simulator.batch.config import load_batch_config
from simulator.batch.generator import generate_batch_datasets
from simulator.bronze import fixed_clock
from simulator.config import load_scale_config
from simulator.generator.config import load_generator_config
from simulator.incidents.config import (
    BreakdownConfig,
    CongestionConfig,
    DuplicateConfig,
    IncidentsConfig,
    LateEventConfig,
    MalformedDataConfig,
    OutOfOrderConfig,
    RecoveryWindow,
    VolumeSpikeConfig,
)
from simulator.reference.generator import generate_reference_dataset
from simulator.schemas.telemetry import TelemetryEvent
from simulator.silver import IN_ORDER, LATE, SilverConfig
from simulator.tests._bronze_helpers import FAST_GENERATOR_CONFIG, REPO_ROOT, read_jsonl
from simulator.tests._silver_helpers import (
    SILVER_CONFIG,
    bronze_from_lines,
    delivered_event_ids,
    derive_expectations,
    parse_incident_log,
    run_incident_stream,
    silver,
    table,
    valid_payload,
)

#: A wall-clock-like ingestion/processing instant: long after the 2025 data
#: (QA ran the CLIs at the real 2026 wall clock).
WALL_CLOCK = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)
WALL_PROCESSING = fixed_clock(datetime(2026, 11, 25, 10, 0, tzinfo=UTC))
#: An ingestion clock BEFORE the 2025-01-01 data (e.g. a fixed test clock).
EARLY_CLOCK = datetime(2024, 12, 31, tzinfo=UTC)
EARLY_PROCESSING = fixed_clock(datetime(2024, 12, 31, 1, 0, tzinfo=UTC))

STREAM_CONFIG = IncidentsConfig(
    seed=60606,
    malformed=MalformedDataConfig(enabled=True, probability_per_tick=0.05),
    duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.05),
    late_event=LateEventConfig(enabled=True, probability_per_tick=0.03),
    out_of_order=OutOfOrderConfig(enabled=True, probability_per_tick=0.2, hold_ticks=3),
    congestion=CongestionConfig(enabled=True, route_id="ROUTE-01",
                                window=RecoveryWindow(start_tick=50, duration_ticks=300)),
    volume_spike=VolumeSpikeConfig(enabled=True, rate_multiplier=6.0,
                                   window=RecoveryWindow(start_tick=200, duration_ticks=200)),
    breakdown=BreakdownConfig(enabled=True, vehicle_ids=["VEH-0003"],
                              window=RecoveryWindow(start_tick=0, duration_ticks=400)),
)
MAX_EVENTS = 1000
OUTLIER_ID = "EVT-D1-OUTLIER"

#: Outliers ahead of the 2025-01-01T00:00Z..00:01:40Z stream, by
#: 1 day / 1 month / ~17 months / 65 years.
OUTLIERS = {
    "1_day": "2025-01-02T00:00:00Z",
    "1_month": "2025-02-01T00:00:00Z",
    "2026_06_01": "2026-06-01T00:00:00Z",
    "2090": "2090-01-01T00:00:00Z",
}
CLOCKS = {"wall_clock_2026": (WALL_CLOCK, WALL_PROCESSING), "before_data_2024": (EARLY_CLOCK, EARLY_PROCESSING)}


@lru_cache(maxsize=1)
def _stream():
    return run_incident_stream(STREAM_CONFIG, MAX_EVENTS)


def _baseline():
    """Outlier-free expectation, computed without any Silver code."""
    run = _stream()
    return derive_expectations(run.log, delivered_event_ids(run.lines), run.total_ticks)


def _outlier_line(when: str, event_id: str = OUTLIER_ID, vehicle_id: str | None = None) -> str:
    overrides = {"event_id": event_id, "generated_at": when, "event_time": when}
    if vehicle_id is not None:
        overrides["vehicle_id"] = vehicle_id
    return json.dumps(valid_payload(**overrides))


def _with(lines: list[str], position: int, *extra: str) -> list[str]:
    return lines[:position] + list(extra) + lines[position:]


def _run(tmp_path, lines, clocks):
    ingestion, processing = clocks
    bronze = bronze_from_lines(tmp_path / "bronze", lines, clock=fixed_clock(ingestion))
    return silver(bronze, clock=processing)


def _assert_other_flags_equal_baseline(clean, counters, baseline, outlier_ids=(OUTLIER_ID,)) -> None:
    got = {r["event_id"]: r["_silver_event_time_status"] for r in clean if r["event_id"] not in outlier_ids}
    assert set(got) == baseline.valid_ids
    changed = sorted(event_id for event_id, status in got.items() if status != baseline.status_of[event_id])
    late = sum(1 for status in got.values() if status == LATE)
    assert not changed, (
        f"{len(changed)} events' event-time flags differ from the outlier-free baseline "
        f"({late} late vs {baseline.late} in the baseline); first: {changed[:5]}"
    )
    assert (counters["late"], counters["out_of_order"]) == (baseline.late, baseline.out_of_order)
    assert all(r["_silver_watermark_guarded"] is False for r in clean if r["event_id"] not in outlier_ids)


def _assert_outlier_loaded_and_flagged(clean, counters, baseline, outlier_ids=(OUTLIER_ID,)) -> None:
    """Valid-looking data is never quarantined by the guard: the outlier is
    loaded, flagged `_silver_watermark_guarded`, and did not move the
    watermark (it is ahead of everything, so classified in_order)."""
    for outlier_id in outlier_ids:
        (outlier,) = [r for r in clean if r["event_id"] == outlier_id]
        assert outlier["_silver_watermark_guarded"] is True
        assert outlier["_silver_event_time_status"] == IN_ORDER
    assert counters["watermark_guarded"] == len(outlier_ids)
    assert counters["quarantined"] == baseline.quarantined
    assert counters["valid_loaded"] == baseline.valid_loaded + len(outlier_ids)


def test_the_stream_exercises_every_event_time_scenario():
    baseline = _baseline()
    assert baseline.late > 0 and baseline.out_of_order > 0 and baseline.quarantined > 0
    assert baseline.duplicates_excluded > 0


# -- (a), (b), (c): one valid-looking outlier ahead of the stream -----------------------


@pytest.mark.parametrize("clock", sorted(CLOCKS))
@pytest.mark.parametrize("outlier", sorted(OUTLIERS))
@pytest.mark.parametrize("position", [0, 25, 500])
def test_one_outlier_ahead_of_the_stream_changes_no_other_flag(tmp_path, outlier, clock, position):
    baseline = _baseline()
    lines = _with(list(_stream().lines), position, _outlier_line(OUTLIERS[outlier]))
    result = _run(tmp_path, lines, CLOCKS[clock])
    clean, counters = table(result, "clean"), result.manifest["counters"]
    _assert_other_flags_equal_baseline(clean, counters, baseline)
    _assert_outlier_loaded_and_flagged(clean, counters, baseline)
    assert result.manifest["event_time"]["max_event_time"] < "2025-01-02"
    assert result.manifest["reconciliation"]["holds"]


def test_qa_a_one_day_ahead_outlier_at_wall_clock_ingestion(tmp_path):
    """QA (a) verbatim: the clock-relative guard did not fire (the outlier is
    long before the 2026 ingestion clock) and the outlier made ~all later
    events late, exactly as with the guard off."""
    baseline = _baseline()
    lines = _with(list(_stream().lines), 25, _outlier_line(OUTLIERS["1_day"]))
    result = _run(tmp_path, lines, CLOCKS["wall_clock_2026"])
    counters = result.manifest["counters"]
    assert counters["late"] == baseline.late, f"late {counters['late']} vs baseline {baseline.late}"
    _assert_other_flags_equal_baseline(table(result, "clean"), counters, baseline)


def test_qa_b_outlier_dated_2026_06_01_at_wall_clock_ingestion(tmp_path):
    baseline = _baseline()
    lines = _with(list(_stream().lines), 25, _outlier_line(OUTLIERS["2026_06_01"]))
    result = _run(tmp_path, lines, CLOCKS["wall_clock_2026"])
    counters = result.manifest["counters"]
    assert counters["late"] == baseline.late, f"late {counters['late']} vs baseline {baseline.late}"
    _assert_other_flags_equal_baseline(table(result, "clean"), counters, baseline)


def test_qa_c_2090_outlier_is_loaded_flagged_not_quarantined(tmp_path):
    baseline = _baseline()
    lines = _with(list(_stream().lines), 25, _outlier_line(OUTLIERS["2090"]))
    result = _run(tmp_path, lines, CLOCKS["wall_clock_2026"])
    clean, counters = table(result, "clean"), result.manifest["counters"]
    _assert_other_flags_equal_baseline(clean, counters, baseline)
    _assert_outlier_loaded_and_flagged(clean, counters, baseline)
    assert not [r for r in table(result, "quarantine") if r["event_id"] == OUTLIER_ID]


def test_two_outliers_from_distinct_vehicles_are_both_held_back(tmp_path):
    """The default tolerance holds back up to two clustered valid-looking
    outliers from distinct vehicles (they cannot corroborate each other)."""
    baseline = _baseline()
    ids = (OUTLIER_ID, "EVT-D1-OUTLIER-2")
    lines = _with(list(_stream().lines), 25, _outlier_line(OUTLIERS["1_day"]),
                  _outlier_line("2025-01-02T00:00:05Z", event_id=ids[1], vehicle_id="VEH-0042"))
    result = _run(tmp_path, lines, CLOCKS["wall_clock_2026"])
    clean, counters = table(result, "clean"), result.manifest["counters"]
    _assert_other_flags_equal_baseline(clean, counters, baseline, outlier_ids=ids)
    _assert_outlier_loaded_and_flagged(clean, counters, baseline, outlier_ids=ids)


def test_a_vehicle_whose_clock_runs_a_day_fast_is_held_back(tmp_path):
    """A single producer with a fast clock emits MANY outliers; they come
    from one vehicle, so they never corroborate each other."""
    baseline = _baseline()
    fast = [_outlier_line(f"2025-01-02T00:00:{i:02d}Z", event_id=f"EVT-FAST-{i}", vehicle_id="VEH-0050")
            for i in range(12)]
    lines = list(_stream().lines)
    for i, line in enumerate(fast):
        lines.insert(20 + 60 * i, line)
    result = _run(tmp_path, lines, CLOCKS["wall_clock_2026"])
    ids = tuple(json.loads(line)["event_id"] for line in fast)
    clean, counters = table(result, "clean"), result.manifest["counters"]
    _assert_other_flags_equal_baseline(clean, counters, baseline, outlier_ids=ids)
    _assert_outlier_loaded_and_flagged(clean, counters, baseline, outlier_ids=ids)


# -- (d), (e): valid data ahead of the ingestion clock is never quarantined ----------------


def _assert_loads_as_the_oracle_says(result, expected) -> None:
    counters = result.manifest["counters"]
    assert counters["quarantined"] == expected.quarantined, counters["quarantined_by_rule"]
    assert counters["valid_loaded"] == expected.valid_loaded
    assert counters["duplicates_excluded"] == expected.duplicates_excluded
    assert counters["watermark_guarded"] == 0
    assert (counters["late"], counters["out_of_order"], counters["in_order"]) == (
        expected.late, expected.out_of_order, expected.in_order)
    got = {r["event_id"]: r["_silver_event_time_status"] for r in table(result, "clean")}
    assert got == expected.status_of
    assert result.manifest["reconciliation"]["holds"]


def test_qa_d_stream_simulated_from_2027_at_an_earlier_ingestion_clock(tmp_path):
    generator_config = load_generator_config().model_copy(
        update={"simulated_start_time": datetime(2027, 1, 1, tzinfo=UTC)})
    run = run_incident_stream(STREAM_CONFIG, 500, generator_config=generator_config)
    expected = derive_expectations(run.log, delivered_event_ids(run.lines), run.total_ticks,
                                   generator_config=generator_config)
    assert json.loads(run.lines[0])["event_time"].startswith("2027-01-01")
    result = _run(tmp_path, list(run.lines), CLOCKS["wall_clock_2026"])
    _assert_loads_as_the_oracle_says(result, expected)


def test_qa_e_fixed_ingestion_time_before_the_data(tmp_path):
    run = _stream()
    expected = derive_expectations(run.log, delivered_event_ids(run.lines), run.total_ticks)
    result = _run(tmp_path, list(run.lines), CLOCKS["before_data_2024"])
    _assert_loads_as_the_oracle_says(result, expected)


def test_qa_e_de04_history_ingested_with_a_clock_before_it(tmp_path):
    """DE-04 history (2025-01-05..12) ingested with a fixed clock before it
    loads exactly as with a clock after it (main's behaviour)."""
    reference = generate_reference_dataset(load_scale_config())
    history = generate_batch_datasets(load_batch_config(), reference=reference).historical_telemetry
    fields = set(TelemetryEvent.model_fields)
    lines = [json.dumps(row.model_dump(mode="json", include=fields)) for row in history]
    before = _run(tmp_path / "before", lines, (datetime(2025, 1, 1, tzinfo=UTC), fixed_clock(
        datetime(2025, 1, 1, 1, tzinfo=UTC))))
    after = _run(tmp_path / "after", lines, CLOCKS["wall_clock_2026"])
    for result in (before, after):
        counters = result.manifest["counters"]
        assert counters["quarantined"] == 0, counters["quarantined_by_rule"]
        assert counters["valid_loaded"] == len(history)
        assert counters["watermark_guarded"] == 0
    strip = ("ingestion_time", "processing_time", "_silver_watermark")
    assert [{k: v for k, v in r.items() if k not in strip} for r in table(before, "clean")] == \
        [{k: v for k, v in r.items() if k not in strip} for r in table(after, "clean")]


# -- (f): no outlier -> the guard changes nothing ----------------------------------------------


def _guard_off() -> SilverConfig:
    data = SILVER_CONFIG.model_dump(mode="json")
    data["watermark_guard"] = {**data["watermark_guard"], "mode": "off"}
    return SilverConfig.model_validate(data)


def test_qa_f_without_an_outlier_flags_equal_the_oracle_and_guard_on_equals_off(tmp_path):
    run = _stream()
    expected = derive_expectations(run.log, delivered_event_ids(run.lines), run.total_ticks)
    on = _run(tmp_path / "on", list(run.lines), CLOCKS["wall_clock_2026"])
    _assert_loads_as_the_oracle_says(on, expected)
    bronze = bronze_from_lines(tmp_path / "off" / "bronze", list(run.lines), clock=fixed_clock(WALL_CLOCK))
    off = silver(bronze, clock=WALL_PROCESSING, config=_guard_off())
    for role in ("clean", "quarantine", "duplicates"):
        assert table(on, role) == table(off, role), role


def test_qa_f_genuine_jump_between_bronze_runs_is_followed(tmp_path):
    """A genuinely progressing stream that jumps (a second simulation five
    months later, in a later Bronze run) is followed exactly as without the
    guard: the jump is corroborated by the rest of the fleet."""
    later = load_generator_config().model_copy(update={"simulated_start_time": datetime(2025, 6, 1, tzinfo=UTC)})
    first = _stream()
    second = run_incident_stream(STREAM_CONFIG.model_copy(update={"seed": 70707}), 300, generator_config=later)
    second_lines = [line.replace('"EVT-', '"EVT-B-') for line in second.lines]  # distinct event ids
    bronze = tmp_path / "bronze"
    bronze_from_lines(bronze, list(first.lines), run_id="b1", clock=fixed_clock(WALL_CLOCK))
    bronze_from_lines(bronze, second_lines, run_id="b2", clock=fixed_clock(WALL_CLOCK.replace(hour=10)))
    on = silver(bronze, clock=WALL_PROCESSING)
    off = silver(bronze, tmp_path / "off", clock=WALL_PROCESSING, config=_guard_off())
    assert on.manifest["counters"]["watermark_guarded"] == 0
    assert on.manifest["event_time"]["max_event_time"].startswith("2025-06-01")
    for role in ("clean", "quarantine", "duplicates"):
        assert table(on, role) == table(off, role), role


# -- real CLIs: (a) and (e) ------------------------------------------------------------------

SCENARIO_FLAGS = [f"--enable-{s}" for s in ("malformed", "duplicate", "late-event", "out-of-order", "congestion",
                                            "volume-spike", "breakdown")]
CLI_EVENTS = 500


def _cli(*args: str) -> dict:
    done = subprocess.run([sys.executable, "-m", *args], cwd=REPO_ROOT, check=True, capture_output=True, text=True)
    return json.loads(done.stdout.strip().splitlines()[-1]) if done.stdout.strip() else {}


def _cli_chain(tmp_path, *, outlier: str | None, ingestion: str, processing: str):
    events, log_file = tmp_path / "events.ndjson", tmp_path / "incidents.log"
    _cli("simulator.incidents", "--generator-config", str(FAST_GENERATOR_CONFIG), "--max-events", str(CLI_EVENTS),
         "--output", str(events), "--log-file", str(log_file), *SCENARIO_FLAGS)
    lines = events.read_text().splitlines()
    log = parse_incident_log(log_file.read_text())
    expected = derive_expectations(log, delivered_event_ids(lines), CLI_EVENTS + 3,
                                   generator_config=load_generator_config(FAST_GENERATOR_CONFIG))
    if outlier is not None:
        events.write_text("\n".join(_with(lines, 25, _outlier_line(outlier))) + "\n")
    _cli("simulator.bronze", "stream", "--input", str(events), "--run-id", "b1", "--output-dir",
         str(tmp_path / "bronze"), "--fixed-ingestion-time", ingestion)
    summary = _cli("simulator.silver", "--run-id", "s1", "--bronze-dir", str(tmp_path / "bronze"),
                   "--output-dir", str(tmp_path / "silver"), "--fixed-processing-time", processing)
    assert summary["status"] == "COMMITTED" and summary["reconciliation_holds"] is True
    manifest = json.loads((tmp_path / "silver" / "_runs" / "s1.json").read_text())
    clean = read_jsonl(tmp_path / "silver" / manifest["tables"]["clean"]["path"])
    return expected, manifest, clean


def test_qa_a_through_the_real_clis(tmp_path):
    expected, manifest, clean = _cli_chain(tmp_path, outlier=OUTLIERS["1_day"],
                                           ingestion="2026-11-25T09:00:00Z", processing="2026-11-25T10:00:00Z")
    assert expected.late > 0
    counters = manifest["counters"]
    _assert_other_flags_equal_baseline(clean, counters, expected)
    _assert_outlier_loaded_and_flagged(clean, counters, expected)


def test_qa_e_through_the_real_clis(tmp_path):
    expected, manifest, clean = _cli_chain(tmp_path, outlier=None,
                                           ingestion="2024-12-31T00:00:00Z", processing="2024-12-31T01:00:00Z")
    counters = manifest["counters"]
    assert counters["quarantined"] == expected.quarantined, counters["quarantined_by_rule"]
    assert counters["valid_loaded"] == expected.valid_loaded
    assert counters["watermark_guarded"] == 0
    assert {r["event_id"]: r["_silver_event_time_status"] for r in clean} == expected.status_of
