"""Shared helpers for the DE-07 Gold test suite (not collected by pytest: no
`test_` prefix).

`FIXTURE_LINES` is a small, hand-constructed telemetry stream; the expected
Gold output for it (`EXPECTED_ROWS`, `EXPECTED_TOTALS`) was computed BY HAND
from the table in the comments below - not by calling any Gold code. The
fixture goes through the real DE-05 Bronze and DE-06 Silver code, so the
late/duplicate/quarantine classification it relies on is Silver's own.

All instants are 2025-01-01 UTC. Silver allowed lateness = 60 s.

| # | id      | route    | event_time (UTC)   | delay | Silver outcome                          | Gold cell / attribution         |
|---|---------|----------|--------------------|-------|-----------------------------------------|---------------------------------|
| 1 | E1      | ROUTE-01 | 00:10              |    60 | clean, in_order                         | R01 h00                         |
| 2 | E2      | ROUTE-01 | 00:20              |   120 | clean, in_order                         | R01 h00                         |
| 3 | E3      | ROUTE-02 | 00:30              |    30 | clean, in_order                         | R02 h00                         |
| 4 | E4      | ROUTE-01 | 01:05              |   300 | clean, in_order (max seen -> 01:05)     | R01 h01                         |
| 5 | E2 copy | ROUTE-01 | 00:20              |  9999 | DUPLICATE (payload conflict)            | excluded                        |
| 6 | E5      | ROUTE-01 | 00:50              |   100 | clean, LATE (< 01:05 - 60 s)            | R01 h00 (late counted)          |
| 7 | Q1      | ROUTE-01 | 00:40              |  5000 | quarantine non_negative_speed           | attributed R01 h00              |
| 8 | Q2      | ROUTE-02 | 00:15              |  5000 | quarantine required_fields (null veh.)  | attributed R02 h00              |
| 9 | Q3      | ROUTE-99 | 00:15              |  5000 | quarantine known_route                  | unattributed: route             |
|10 | Q4      | ROUTE-01 | "not-a-time"       |  5000 | quarantine parsable_timestamps          | unattributed: event_time        |
|11 | (trunc) | -        | -                  |     - | quarantine bronze_parse_error           | unattributed: parse error       |
|12 | Q7      | ROUTE-01 | 00:45              |  5000 | quarantine schema_conformance (1e400)   | unattributed: payload omitted   |
|13 | E6      | ROUTE-02 | 01:59:59.999999    |     0 | clean, in_order                         | R02 h01 (end boundary)          |
|14 | E7      | ROUTE-02 | 02:00:00           |    45 | clean, in_order (max -> 02:00)          | R02 h02 (start boundary)        |
|15 | E8      | ROUTE-02 | 11:30+10:00 = 01:30|    15 | clean, LATE (< 02:00 - 60 s)            | R02 h01 (late counted)          |
|16 | Q5      | ROUTE-02 | 02:00 (gen 00:30)  |  5000 | quarantine future_time_tolerance        | unattributed: event_time reject |
|17 | Q6      | ROUTE-03 | 03:10              |  5000 | quarantine non_negative_occupancy       | attributed R03 h03              |

Hand computation per (route, hour):
* R01 h00: E1 60 + E2 120 + E5 100 = 280 / 3 = 93.3333; late 1; Q1 -> 3/(3+1) = 75.0 %
* R02 h00: E3 30 / 1 = 30.0; Q2 -> 1/(1+1) = 50.0 %
* R01 h01: E4 300 / 1 = 300.0; 100.0 %
* R02 h01: E6 0 + E8 15 = 15 / 2 = 7.5; late 1; 100.0 %
* R02 h02: E7 45 / 1 = 45.0; 100.0 %
* R03 h03: no clean events -> avg null, sum 0; Q6 -> 0/(0+1) = 0.0 %
Totals: 17 extracted = 8 clean + 8 quarantined + 1 duplicate; attributed
3 (Q1, Q2, Q6) + unattributed 5 = 8; overall 8 / (8 + 8) = 50.0 %.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from simulator.bronze import fixed_clock
from simulator.gold import load_gold_config, run_gold
from simulator.tests._bronze_helpers import read_jsonl
from simulator.tests._silver_helpers import bronze_from_lines, silver, valid_payload

GOLD_TIME_STR = "2025-02-01T11:00:00.000000Z"
GOLD_TIME = datetime(2025, 2, 1, 11, 0, 0, tzinfo=UTC)
GOLD_CLOCK = fixed_clock(GOLD_TIME)
GOLD_CONFIG = load_gold_config()

DAY = "2025-01-01"
TRUNCATED_LINE = '{"event_id": "EVT-TRUNC", "route_id": "ROUTE-01"'
OVERFLOW_PLACEHOLDER = "12345.678"


def _event(event_id: str, route_id: str, event_time: str, delay: int, *, generated_at: str | None = None,
           **overrides: Any) -> dict[str, Any]:
    return valid_payload(
        event_id=event_id, route_id=route_id, event_time=event_time,
        generated_at=generated_at if generated_at is not None else event_time,
        trip_id=f"TRIP-{route_id}-0000", delay_seconds=delay, **overrides,
    )


def _t(hhmmss: str) -> str:
    return f"{DAY}T{hhmmss}Z"


def fixture_lines() -> list[str]:
    q7 = json.dumps(_event("Q7", "ROUTE-01", _t("00:45:00"), 5000, speed_kmh=float(OVERFLOW_PLACEHOLDER)))
    assert OVERFLOW_PLACEHOLDER in q7
    return [
        json.dumps(_event("E1", "ROUTE-01", _t("00:10:00"), 60)),
        json.dumps(_event("E2", "ROUTE-01", _t("00:20:00"), 120)),
        json.dumps(_event("E3", "ROUTE-02", _t("00:30:00"), 30)),
        json.dumps(_event("E4", "ROUTE-01", _t("01:05:00"), 300)),
        json.dumps(_event("E2", "ROUTE-01", _t("00:20:00"), 9999)),
        json.dumps(_event("E5", "ROUTE-01", _t("00:50:00"), 100)),
        json.dumps(_event("Q1", "ROUTE-01", _t("00:40:00"), 5000, speed_kmh=-5.0)),
        json.dumps(_event("Q2", "ROUTE-02", _t("00:15:00"), 5000, vehicle_id=None)),
        json.dumps(_event("Q3", "ROUTE-99", _t("00:15:00"), 5000)),
        json.dumps(_event("Q4", "ROUTE-01", "not-a-time", 5000, generated_at=_t("00:15:00"))),
        TRUNCATED_LINE,
        q7.replace(OVERFLOW_PLACEHOLDER, "1e400"),
        json.dumps(_event("E6", "ROUTE-02", _t("01:59:59.999999"), 0)),
        json.dumps(_event("E7", "ROUTE-02", _t("02:00:00"), 45)),
        json.dumps(_event("E8", "ROUTE-02", f"{DAY}T11:30:00+10:00", 15)),
        json.dumps(_event("Q5", "ROUTE-02", _t("02:00:00"), 5000, generated_at=_t("00:30:00"))),
        json.dumps(_event("Q6", "ROUTE-03", _t("03:10:00"), 5000, occupancy=-1)),
    ]


ROUTE_NAMES = {
    "ROUTE-01": ("100", "Route 100 (Parramatta line)"),
    "ROUTE-02": ("101", "Route 101 (Chatswood line)"),
    "ROUTE-03": ("102", "Route 102 (Bondi line)"),
}


def _row(route: str, hour: int, count: int, avg: float | None, total: int, late: int, quarantined: int,
         valid: float) -> dict[str, Any]:
    number, name = ROUTE_NAMES[route]
    return {
        "route_id": route, "route_number": number, "route_name": name,
        "period_start": f"{DAY}T{hour:02d}:00:00.000000Z",
        "period_end": f"{DAY}T{hour + 1:02d}:00:00.000000Z",
        "period_start_local": f"{DAY}T{hour + 10:02d}:00:00.000000+10:00",
        "period_end_local": f"{DAY}T{hour + 11:02d}:00:00.000000+10:00",
        "period_minutes": 60,
        "event_count": count, "avg_delay_seconds": avg, "sum_delay_seconds": total,
        "late_event_count": late, "out_of_order_event_count": 0, "quarantined_count": quarantined,
        "valid_percentage": valid,
        "gold_run_id": "g1", "silver_run_id": "s1", "processing_time": GOLD_TIME_STR,
    }


#: Hand-computed (see module docstring), ordered by (period_start, route_id).
EXPECTED_ROWS: list[dict[str, Any]] = [
    _row("ROUTE-01", 0, 3, 93.3333, 280, 1, 1, 75.0),
    _row("ROUTE-02", 0, 1, 30.0, 30, 0, 1, 50.0),
    _row("ROUTE-01", 1, 1, 300.0, 300, 0, 0, 100.0),
    _row("ROUTE-02", 1, 2, 7.5, 15, 1, 0, 100.0),
    _row("ROUTE-02", 2, 1, 45.0, 45, 0, 0, 100.0),
    _row("ROUTE-03", 3, 0, None, 0, 0, 1, 0.0),
]

EXPECTED_TOTALS = {
    "extracted": 17, "valid_loaded": 8, "quarantined": 8, "duplicates_excluded": 1, "late": 2,
    "quarantined_attributed": 3, "quarantined_unattributed": 5,
    "unattributed_by_reason": {
        "bronze_parse_error": 1, "raw_payload_unavailable": 1, "route_id_missing_or_unknown": 1,
        "event_time_unparseable": 1, "event_time_rejected": 1,
        # DE-08 reasons: none of the fixture records triggers them (Q1, Q2 and
        # Q6 have a supported schema and in-bounds times consistent with their
        # generated_at, and none is a jump ahead of the stream).
        "schema_version_unsupported": 0, "event_time_rejected_on_recheck": 0, "event_time_unverifiable": 0,
        "event_time_held_back_by_watermark_guard": 0,
    },
    "overall_valid_percentage": 50.0,
}


# -- pipeline helpers ----------------------------------------------------------


def silver_from_lines(root: Path, lines: list[str], *, silver_run_id: str = "s1", **silver_kwargs: Any):
    """lines -> DE-05 Bronze (run b1) -> DE-06 Silver (`silver_run_id`) under `root`."""
    bronze = bronze_from_lines(root / "bronze", lines)
    return silver(bronze, root / "silver", run_id=silver_run_id, **silver_kwargs)


def fixture_silver(root: Path):
    return silver_from_lines(root, fixture_lines())


def gold(root: Path, *, run_id: str = "g1", output_dir: Path | None = None, **kwargs: Any):
    kwargs.setdefault("clock", GOLD_CLOCK)
    return run_gold(
        run_id=run_id, silver_dir=root / "silver",
        output_dir=output_dir if output_dir is not None else root / "gold", **kwargs,
    )


def gold_rows(result) -> list[dict[str, Any]]:
    root = result.manifest_path.parent.parent
    return read_jsonl(root / result.manifest["tables"]["route_performance"]["path"])


def silver_table(result, role: str) -> list[dict[str, Any]]:
    root = result.manifest_path.parent.parent
    return read_jsonl(root / result.manifest["tables"][role]["path"])


def store_files(root: Path) -> dict[str, bytes]:
    """Every file under a store root, by relative path (for byte comparison)."""
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}
