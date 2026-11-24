"""DE-07 route-performance aggregate: pure functions over the records of one
committed Silver run (no I/O here; `pipeline.py` reads, verifies and commits).

Grain: one row per (`route_id`, period) - see README "DE-07: Aggregate
definition". In short:

* **event_count** - Silver *clean* records of that route whose `event_time`
  falls in [period_start, period_end). Late and out-of-order rows are
  INCLUDED (Silver loads and flags them; a historical aggregate must count
  them - BACKLOG DE-07 "Silver input contract"). Quarantined records and
  excluded duplicates are never in the clean table, so they can never reach
  a count or an average.
* **avg_delay_seconds** - mean `delay_seconds` over exactly those records
  (null when `event_count == 0`); `sum_delay_seconds` is kept so the
  average can be re-aggregated correctly (averages are not additive).
* **quarantined_count** - Silver quarantine records *attributed* to that
  route and period (`attribute_quarantined`).
* **valid_percentage** - `100 * event_count / (event_count +
  quarantined_count)`, 4 dp. Duplicates are not in the denominator (same
  convention as Silver's `quality_percentage`: a replay is a delivery issue,
  not a data-quality failure).

Periods are fixed-length buckets aligned to *local* midnight at a fixed UTC
offset (default +10, DE-04's convention); timestamps are emitted in UTC (as
Silver stores them) and labelled in local time.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from pydantic import TypeAdapter, ValidationError

from simulator.bronze.ingest import format_timestamp
from simulator.gold.errors import GoldInputError
from simulator.silver.pipeline import IN_ORDER, LATE, OUT_OF_ORDER
from simulator.silver.rules import FUTURE_TIME_TOLERANCE, timestamp_problem

#: Rounding of `avg_delay_seconds` / `valid_percentage` (Silver rounds its
#: `quality_percentage` to the same 4 dp, so the two are comparable).
DECIMAL_PLACES = 4

_NAIVE_EPOCH = datetime(1970, 1, 1)
_DATETIME = TypeAdapter(datetime)

#: Why a quarantined record could not be attributed to a (route, period),
#: in the precedence order they are evaluated.
UNATTRIBUTED_BRONZE_PARSE_ERROR = "bronze_parse_error"  # no JSON object at all
UNATTRIBUTED_PAYLOAD_UNAVAILABLE = "raw_payload_unavailable"  # parsed payload omitted by Silver (inf / lone surrogate)
UNATTRIBUTED_ROUTE_UNKNOWN = "route_id_missing_or_unknown"  # absent, null, not a string, or not a reference route
UNATTRIBUTED_EVENT_TIME_UNPARSEABLE = "event_time_unparseable"  # absent / not offset ISO-8601 / out of range
UNATTRIBUTED_EVENT_TIME_REJECTED = "event_time_rejected"  # parseable, but Silver rejected it (future_time_tolerance)
UNATTRIBUTED_REASONS: tuple[str, ...] = (
    UNATTRIBUTED_BRONZE_PARSE_ERROR,
    UNATTRIBUTED_PAYLOAD_UNAVAILABLE,
    UNATTRIBUTED_ROUTE_UNKNOWN,
    UNATTRIBUTED_EVENT_TIME_UNPARSEABLE,
    UNATTRIBUTED_EVENT_TIME_REJECTED,
)

#: Silver rules whose failure means the record's event_time itself is wrong,
#: so it must not place the record in a period.
EVENT_TIME_RULES = frozenset({FUTURE_TIME_TOLERANCE})

#: Output columns, in order (JSON lines key order and CSV header).
COLUMNS: tuple[str, ...] = (
    "route_id",
    "route_number",
    "route_name",
    "period_start",
    "period_end",
    "period_start_local",
    "period_end_local",
    "period_minutes",
    "event_count",
    "avg_delay_seconds",
    "sum_delay_seconds",
    "late_event_count",
    "out_of_order_event_count",
    "quarantined_count",
    "valid_percentage",
    "gold_run_id",
    "silver_run_id",
    "processing_time",
)

_EVENT_TIME_STATUSES = (IN_ORDER, OUT_OF_ORDER, LATE)


@dataclass(frozen=True)
class Periods:
    """Fixed-length buckets aligned to local midnight at a fixed UTC offset."""

    period_minutes: int
    utc_offset_hours: int

    @property
    def length(self) -> timedelta:
        return timedelta(minutes=self.period_minutes)

    @property
    def offset(self) -> timedelta:
        return timedelta(hours=self.utc_offset_hours)

    @property
    def local_tz(self) -> timezone:
        return timezone(self.offset)

    def start_of(self, instant: datetime) -> datetime:
        """UTC start of the bucket holding `instant` (timezone-aware).
        Raises OverflowError for an instant whose bucket is not representable."""
        local = instant.astimezone(UTC).replace(tzinfo=None) + self.offset
        index = (local - _NAIVE_EPOCH) // self.length
        return (_NAIVE_EPOCH + index * self.length - self.offset).replace(tzinfo=UTC)

    def labels(self, start: datetime) -> dict[str, Any]:
        end = start + self.length
        return {
            "period_start": format_timestamp(start),
            "period_end": format_timestamp(end),
            "period_start_local": start.astimezone(self.local_tz).isoformat(timespec="microseconds"),
            "period_end_local": end.astimezone(self.local_tz).isoformat(timespec="microseconds"),
            "period_minutes": self.period_minutes,
        }


@dataclass
class _Cell:
    event_count: int = 0
    sum_delay_seconds: int = 0
    late_event_count: int = 0
    out_of_order_event_count: int = 0
    quarantined_count: int = 0


@dataclass
class Aggregate:
    """Aggregation result before serialisation."""

    rows: list[dict[str, Any]]
    clean_records: int = 0
    clean_sum_delay_seconds: int = 0
    clean_by_status: dict[str, int] = field(default_factory=dict)
    quarantined_records: int = 0
    quarantined_attributed: int = 0
    unattributed_by_reason: dict[str, int] = field(default_factory=dict)

    @property
    def quarantined_unattributed(self) -> int:
        return sum(self.unattributed_by_reason.values())


def _parse_event_time(value: Any) -> datetime | None:
    """A Silver-strict event_time (ISO-8601 string with a UTC offset, same
    check as Silver rule `parsable_timestamps`), or None."""
    if timestamp_problem(value) is not None:
        return None
    try:
        return _DATETIME.validate_json(json.dumps(value), strict=True)
    except ValidationError:  # pragma: no cover - timestamp_problem already validated it
        return None


def attribute_quarantined(
    record: Mapping[str, Any], known_route_ids: frozenset[str], periods: Periods
) -> tuple[str, datetime] | str:
    """(route_id, period start) for a quarantined Silver record whose
    route_id and event_time are both usable; otherwise the
    `UNATTRIBUTED_*` reason (first that applies, in `UNATTRIBUTED_REASONS`
    order). Usable means: the record is a JSON object Silver kept in parsed
    form, `route_id` is a DE-01 reference route, `event_time` is an
    offset-carrying ISO-8601 timestamp, and Silver did not reject the record
    *because of* its event_time."""
    if record.get("raw_format") == "bronze_parse_error":
        return UNATTRIBUTED_BRONZE_PARSE_ERROR
    payload = record.get("raw_payload")
    if not isinstance(payload, dict):
        return UNATTRIBUTED_PAYLOAD_UNAVAILABLE
    route_id = payload.get("route_id")
    if not isinstance(route_id, str) or route_id not in known_route_ids:
        return UNATTRIBUTED_ROUTE_UNKNOWN
    event_time = _parse_event_time(payload.get("event_time"))
    if event_time is None:
        return UNATTRIBUTED_EVENT_TIME_UNPARSEABLE
    try:
        start = periods.start_of(event_time)
    except (OverflowError, ValueError):
        return UNATTRIBUTED_EVENT_TIME_UNPARSEABLE
    if EVENT_TIME_RULES.intersection(record.get("rejection_rules") or ()):
        return UNATTRIBUTED_EVENT_TIME_REJECTED
    return route_id, start


def _clean_fields(record: Mapping[str, Any]) -> tuple[str, datetime, int, str]:
    """route_id, event_time, delay_seconds, status of a Silver clean record;
    a record that breaks the Silver clean contract fails loudly."""
    route_id, delay, status = record.get("route_id"), record.get("delay_seconds"), record.get("_silver_event_time_status")
    try:
        event_time = datetime.fromisoformat(record["event_time"])
    except (KeyError, TypeError, ValueError) as exc:
        raise GoldInputError(f"Silver clean record {record.get('event_id')!r}: bad event_time ({exc})") from exc
    if (
        not isinstance(route_id, str)
        or not isinstance(delay, int)
        or isinstance(delay, bool)
        or status not in _EVENT_TIME_STATUSES
        or event_time.tzinfo is None
    ):
        raise GoldInputError(
            f"Silver clean record {record.get('event_id')!r} breaks the clean contract "
            f"(route_id={route_id!r}, delay_seconds={delay!r}, _silver_event_time_status={status!r})"
        )
    return route_id, event_time, delay, status


def aggregate_route_performance(
    clean: Iterable[Mapping[str, Any]],
    quarantine: Iterable[Mapping[str, Any]],
    *,
    periods: Periods,
    reference_routes: Mapping[str, tuple[str, str]],
    lineage: Mapping[str, Any],
) -> Aggregate:
    """Build the route-performance rows. `reference_routes` maps DE-01
    route_id -> (route_number, name); `lineage` holds `gold_run_id`,
    `silver_run_id` and `processing_time`, copied onto every row.

    Rows are ordered by (period_start, route_id); every (route, period) with
    at least one clean or attributed quarantined record gets a row."""
    cells: dict[tuple[str, datetime], _Cell] = {}
    names: dict[str, tuple[Any, Any]] = {}
    result = Aggregate(rows=[], clean_by_status=dict.fromkeys(_EVENT_TIME_STATUSES, 0),
                       unattributed_by_reason=dict.fromkeys(UNATTRIBUTED_REASONS, 0))

    for record in clean:
        route_id, event_time, delay, status = _clean_fields(record)
        cell = cells.setdefault((route_id, periods.start_of(event_time)), _Cell())
        cell.event_count += 1
        cell.sum_delay_seconds += delay
        if status == LATE:
            cell.late_event_count += 1
        elif status == OUT_OF_ORDER:
            cell.out_of_order_event_count += 1
        result.clean_records += 1
        result.clean_sum_delay_seconds += delay
        result.clean_by_status[status] += 1
        label = (record.get("ref_route_number"), record.get("ref_route_name"))
        if names.setdefault(route_id, label) != label:
            raise GoldInputError(f"Silver clean records disagree on the reference names of route {route_id!r}")

    known = frozenset(reference_routes)
    for record in quarantine:
        result.quarantined_records += 1
        target = attribute_quarantined(record, known, periods)
        if isinstance(target, str):
            result.unattributed_by_reason[target] += 1
            continue
        cells.setdefault(target, _Cell()).quarantined_count += 1
        result.quarantined_attributed += 1

    for (route_id, start), cell in sorted(cells.items(), key=lambda item: (item[0][1], item[0][0])):
        route_number, route_name = names.get(route_id) or reference_routes.get(route_id, (None, None))
        denominator = cell.event_count + cell.quarantined_count
        row = {
            "route_id": route_id,
            "route_number": route_number,
            "route_name": route_name,
            **periods.labels(start),
            "event_count": cell.event_count,
            "avg_delay_seconds": (
                round(cell.sum_delay_seconds / cell.event_count, DECIMAL_PLACES) if cell.event_count else None
            ),
            "sum_delay_seconds": cell.sum_delay_seconds,
            "late_event_count": cell.late_event_count,
            "out_of_order_event_count": cell.out_of_order_event_count,
            "quarantined_count": cell.quarantined_count,
            "valid_percentage": round(100.0 * cell.event_count / denominator, DECIMAL_PLACES),
            "gold_run_id": lineage["gold_run_id"],
            "silver_run_id": lineage["silver_run_id"],
            "processing_time": lineage["processing_time"],
        }
        result.rows.append({column: row[column] for column in COLUMNS})
    return result
