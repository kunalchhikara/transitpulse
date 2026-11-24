"""PROJECT.md section 13 item 2: duplicate-event injection -- replay a
prior event_id verbatim.

A duplicate is an *additional* delivery, never a replacement (FU-05): every
original event the generator produces is delivered exactly once, and each
duplicate fire adds exactly one extra delivery that is a verbatim copy of a
payload already delivered earlier in the stream. The regression tests below
(marked "FU-05 regression") fail on the pre-fix code, where the replay was
returned *instead of* the current tick's payload and so silently dropped one
original event per fire.
"""

from __future__ import annotations

import io
import json
import logging
import subprocess
import sys
from collections import Counter
from pathlib import Path

from simulator.incidents.config import DuplicateConfig, OutOfOrderConfig
from simulator.incidents.logging_setup import LOGGER_NAME, configure_incident_logging
from simulator.tests._generator_helpers import build_generator
from simulator.tests._incident_helpers import baseline_incidents_config, build_injector
from simulator.tests._silver_helpers import (
    fired,
    parse_incident_log,
    run_incident_stream,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
FAST_GENERATOR_CONFIG = Path(__file__).resolve().parent / "fixtures" / "fast_generator.toml"
MAX_EVENTS = 300


def _ids(lines: list[str]) -> list[str]:
    return [json.loads(line)["event_id"] for line in lines]


def _assert_duplicates_are_additional(
    dup_lines: list[str], dup_log: list[dict], base_lines: list[str], originals: int
) -> None:
    """The FU-05 contract, checked against a same-seed run with duplicates
    disabled. Reads only event identity, the raw delivered lines and the
    structured incident log -- not injector internals."""
    dup_ids, base_ids = _ids(dup_lines), _ids(base_lines)
    fires = fired(dup_log, "duplicate")
    assert fires, "guard: the duplicate scenario must actually fire"

    # Baseline sanity: every original exactly once.
    assert len(base_ids) == len(set(base_ids)) == originals

    # Every original event is still delivered (none replaced) ...
    assert set(dup_ids) == set(base_ids)
    # ... and each fire adds exactly one extra delivery.
    assert len(dup_ids) == originals + len(fires)

    # The extra copies are exactly the logged replays, id for id.
    extra_copies = Counter(dup_ids) - Counter(set(dup_ids))
    assert extra_copies == Counter(c["replayed_event_id"] for c in fires)

    # Every repeat is a verbatim (byte-identical) replay of an earlier line.
    first_line: dict[str, str] = {}
    for line, event_id in zip(dup_lines, dup_ids, strict=True):
        assert line == first_line.setdefault(event_id, line)


# -- FU-05 regression: whole-stream conservation -------------------------------


def test_fu05_regression_duplicates_are_added_not_substituted():
    dup_config = baseline_incidents_config(duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.1))
    dup = run_incident_stream(dup_config, MAX_EVENTS)
    base = run_incident_stream(baseline_incidents_config(), MAX_EVENTS)

    _assert_duplicates_are_additional(dup.lines, dup.log, base.lines, MAX_EVENTS)
    # With no reordering scenario, first occurrences keep generation order.
    assert list(dict.fromkeys(_ids(dup.lines))) == _ids(base.lines)


def test_fu05_regression_duplicate_with_out_of_order_conserves_every_original():
    out_of_order = OutOfOrderConfig(enabled=True, probability_per_tick=0.3, hold_ticks=3)
    dup_config = baseline_incidents_config(
        duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.15), out_of_order=out_of_order
    )
    dup = run_incident_stream(dup_config, MAX_EVENTS)
    base = run_incident_stream(baseline_incidents_config(out_of_order=out_of_order), MAX_EVENTS)

    assert fired(dup.log, "out_of_order"), "guard: reordering must actually fire alongside duplicates"
    _assert_duplicates_are_additional(dup.lines, dup.log, base.lines, MAX_EVENTS + out_of_order.hold_ticks)


def _run_cli(tmp_path: Path, name: str, *flags: str, max_events: int = MAX_EVENTS) -> tuple[list[str], list]:
    events, log_file = tmp_path / f"{name}.ndjson", tmp_path / f"{name}.log"
    result = subprocess.run(
        [sys.executable, "-m", "simulator.incidents", "--generator-config", str(FAST_GENERATOR_CONFIG),
         "--max-events", str(max_events), "--output", str(events), "--log-file", str(log_file), *flags],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, f"CLI run failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    return events.read_text(encoding="utf-8").splitlines(), parse_incident_log(log_file.read_text())


def test_fu05_regression_cli_duplicates_are_added_not_substituted(tmp_path):
    """The Lead's reproduction: `--max-events 300 --enable-duplicate` must
    deliver all 300 originals plus one extra line per logged fire."""
    dup_lines, dup_log = _run_cli(tmp_path, "dup", "--enable-duplicate")
    base_lines, _ = _run_cli(tmp_path, "base")

    _assert_duplicates_are_additional(dup_lines, dup_log, base_lines, MAX_EVENTS)
    assert list(dict.fromkeys(_ids(dup_lines))) == _ids(base_lines)


def test_cli_duplicate_and_out_of_order_are_byte_identical_across_processes(tmp_path):
    flags = ("--enable-duplicate", "--enable-out-of-order", "--enable-malformed")
    lines_a, log_a = _run_cli(tmp_path, "a", *flags, max_events=200)
    lines_b, _ = _run_cli(tmp_path, "b", *flags, max_events=200)

    assert fired(log_a, "duplicate"), "guard: the duplicate scenario must actually fire"
    assert lines_a == lines_b


# -- injector-level semantics --------------------------------------------------


def test_duplicate_disabled_never_repeats_an_event_id():
    injector = build_injector()
    delivered: list[dict] = []
    sink = injector.delivery_sink(delivered.append)
    for tick in range(30):
        sink(injector.produce_event(tick))
    event_ids = [p["event_id"] for p in delivered]
    assert len(event_ids) == len(set(event_ids)) == 30


def test_produce_event_always_returns_the_original_never_a_replay():
    """Even at probability 1.0, `produce_event` returns exactly the
    generator's own sequence -- replays travel separately."""
    reference = build_generator()
    real_payloads = [reference.produce_event(tick).model_dump(mode="json") for tick in range(10)]

    injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=1.0))
    assert [injector.produce_event(tick) for tick in range(10)] == real_payloads


def test_delivery_sink_adds_a_verbatim_replay_after_the_original():
    """probability_per_tick=1.0 forces a fire on every tick once history is
    non-empty (tick 0 cannot replay anything -- nothing was delivered
    before it)."""
    injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=1.0))
    delivered: list[dict] = []
    sink = injector.delivery_sink(delivered.append)

    sink(injector.produce_event(0))
    assert len(delivered) == 1
    first = delivered[0]

    sink(injector.produce_event(1))
    assert len(delivered) == 3
    second, replay = delivered[1], delivered[2]
    assert second["event_id"] != first["event_id"], "tick 1's own original must still be delivered"
    assert replay == first, "the extra delivery is tick 0's payload, every field verbatim"

    sink(injector.produce_event(2))
    assert len(delivered) == 5
    assert delivered[4] in (first, second), "a replay is always a payload delivered earlier"


def test_every_delivery_is_an_already_generated_real_payload():
    """Every delivered payload, original or replay, is a verbatim copy of
    something the underlying generator had *already* produced by that
    point -- never invented, never "from the future"."""
    reference = build_generator()
    real_payloads = [reference.produce_event(tick).model_dump(mode="json") for tick in range(10)]

    injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=1.0))
    for tick in range(10):
        delivered: list[dict] = []
        sink = injector.delivery_sink(delivered.append)
        sink(injector.produce_event(tick))
        assert delivered[0] == real_payloads[tick]
        for payload in delivered[1:]:
            assert payload in real_payloads[:tick]


def test_replay_is_an_independent_copy():
    injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=1.0))
    delivered: list[dict] = []
    sink = injector.delivery_sink(delivered.append)
    sink(injector.produce_event(0))
    original_snapshot = dict(delivered[0])
    delivered[0]["speed_kmh"] = -999.0  # a consumer mutating what it received
    sink(injector.produce_event(1))
    assert delivered[2] == original_snapshot


def test_undrained_replays_are_not_lost_flush_pending_returns_them():
    """A caller that loops `produce_event` directly (without
    `delivery_sink`) still receives every replay from `flush_pending`."""
    injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=1.0))
    originals = [injector.produce_event(tick) for tick in range(5)]
    replays = injector.flush_pending()

    assert len(replays) == 4  # ticks 1-4 each fired; tick 0 had no history
    assert injector.duplicates_injected == 4
    for replay in replays:
        assert replay in originals
    assert injector.flush_pending() == [], "drained exactly once"


def test_duplicate_log_identifies_each_replayed_event_id():
    stream = io.StringIO()
    configure_incident_logging(level="INFO", stream=stream)
    try:
        injector = build_injector(duplicate=DuplicateConfig(enabled=True, probability_per_tick=0.5))
        delivered: list[dict] = []
        sink = injector.delivery_sink(delivered.append)
        originals = []
        for tick in range(40):
            original = injector.produce_event(tick)
            originals.append(original["event_id"])
            sink(original)
    finally:
        logger = logging.getLogger(LOGGER_NAME)
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    fires = fired(parse_incident_log(stream.getvalue()), "duplicate")
    assert fires and len(fires) == injector.duplicates_injected
    replayed = [p["event_id"] for p in delivered]
    for original in originals:
        replayed.remove(original)
    assert replayed == [c["replayed_event_id"] for c in fires], "log order == replay delivery order"
