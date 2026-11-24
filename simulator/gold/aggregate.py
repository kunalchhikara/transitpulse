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
from simulator.config.models import ValidationThresholds
from simulator.gold.errors import GoldInputError
from simulator.schemas.telemetry import future_time_reason, timestamp_bounds_reasons
from simulator.silver.config import (
    FutureTimeReference,
    WatermarkGuardConfig,
    WatermarkGuardMode,
)
from simulator.silver.pipeline import IN_ORDER, LATE, OUT_OF_ORDER
from simulator.silver.rules import EVENT_TIME_RULES as SILVER_EVENT_TIME_RULES
from simulator.silver.rules import (
    FUTURE_TIME_TOLERANCE,
    SUPPORTED_SCHEMA_VERSION,
    TIMESTAMP_BOUNDS,
    WATERMARK_GUARD,
    WatermarkGuard,
    timestamp_problem,
)

#: Rounding of `avg_delay_seconds` / `valid_percentage` (Silver rounds its
#: `quality_percentage` to the same 4 dp, so the two are comparable).
DECIMAL_PLACES = 4

_NAIVE_EPOCH = datetime(1970, 1, 1)
_DATETIME = TypeAdapter(datetime)

#: Why a quarantined record could not be attributed to a (route, period),
#: in the precedence order they are evaluated.
UNATTRIBUTED_BRONZE_PARSE_ERROR = "bronze_parse_error"  # no JSON object at all
UNATTRIBUTED_PAYLOAD_UNAVAILABLE = "raw_payload_unavailable"  # parsed payload omitted by Silver (inf / lone surrogate)
#: DE-08 / FU-11: Silver rejected the schema_version, so no field has a defined meaning.
UNATTRIBUTED_SCHEMA_VERSION = "schema_version_unsupported"
UNATTRIBUTED_ROUTE_UNKNOWN = "route_id_missing_or_unknown"  # absent, null, not a string, or not a reference route
UNATTRIBUTED_EVENT_TIME_UNPARSEABLE = "event_time_unparseable"  # absent / not offset ISO-8601 / out of range
#: Silver rejected the record under an event-time rule (`EVENT_TIME_RULES`).
UNATTRIBUTED_EVENT_TIME_REJECTED = "event_time_rejected"
#: DE-08 / FU-10, FU-11: Silver never evaluated its stage-2 event-time rules
#: (the record failed stage 1), and Gold's re-check of them fails.
UNATTRIBUTED_EVENT_TIME_RECHECK = "event_time_rejected_on_recheck"
#: DE-08: the re-check needs a reference instant the record does not carry in
#: usable form (e.g. an unparseable generated_at under the generated_at reference).
UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE = "event_time_unverifiable"
UNATTRIBUTED_REASONS: tuple[str, ...] = (
    UNATTRIBUTED_BRONZE_PARSE_ERROR,
    UNATTRIBUTED_PAYLOAD_UNAVAILABLE,
    UNATTRIBUTED_SCHEMA_VERSION,
    UNATTRIBUTED_ROUTE_UNKNOWN,
    UNATTRIBUTED_EVENT_TIME_UNPARSEABLE,
    UNATTRIBUTED_EVENT_TIME_REJECTED,
    UNATTRIBUTED_EVENT_TIME_RECHECK,
    UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE,
)

#: Silver rules whose failure means the record's event_time itself cannot be
#: trusted, so it must not place the record in a period (`timestamp_bounds`,
#: `future_time_tolerance`, `watermark_guard`).
EVENT_TIME_RULES = frozenset(SILVER_EVENT_TIME_RULES)


@dataclass(frozen=True)
class EventTimeCheck:
    """DE-08: the Silver event-time rules, re-applied by Gold to quarantined
    records before attributing them (Silver evaluates its stage-2 rules only
    for records that pass stage 1, so e.g. a negative speed with a far-future
    event_time carries only `non_negative_speed`). Built from the settings the
    consumed Silver run RECORDED in its manifest, so Gold applies exactly the
    rules that Silver run applied:

    * `timestamp_bounds` on event_time and (when parseable) generated_at;
    * `future_time_tolerance` against generated_at or the record's Bronze
      ingestion_time (Silver `future_time_reference`);
    * `watermark_guard` against the record's ingestion_time / processing_time,
      only when Silver ran it as a rule (`mode = "quarantine"`)."""

    thresholds: ValidationThresholds
    future_time_reference: FutureTimeReference
    watermark_guard: WatermarkGuard

    @classmethod
    def from_silver_manifest(cls, manifest: Mapping[str, Any]) -> EventTimeCheck:
        try:
            thresholds = ValidationThresholds.model_validate(manifest["validation_thresholds"])
            config = manifest["config"]
            future = FutureTimeReference(config["future_time_reference"])
            guard = WatermarkGuard.from_config(WatermarkGuardConfig.model_validate(config["watermark_guard"]))
        except (KeyError, TypeError, ValueError) as exc:  # pydantic ValidationError is a ValueError
            raise GoldInputError(
                f"Silver run {manifest.get('run_id')!r} does not record the DE-08 event-time settings Gold "
                f"re-applies to quarantined records ({type(exc).__name__}: {exc}); re-run Silver with the "
                "current code"
            ) from exc
        return cls(thresholds, future, guard)

    @property
    def rechecked_rules(self) -> tuple[str, ...]:
        guard = (WATERMARK_GUARD,) if self.watermark_guard.mode is WatermarkGuardMode.QUARANTINE else ()
        return (TIMESTAMP_BOUNDS, FUTURE_TIME_TOLERANCE, *guard)

    def within_bounds(self, instant: datetime) -> bool:
        return not timestamp_bounds_reasons(self.thresholds, instant=instant)

    def as_dict(self) -> dict[str, Any]:
        """Recorded in the Gold manifest (`attribution`)."""
        dumped = self.thresholds.model_dump(mode="json")
        return {
            "event_time_rules": list(SILVER_EVENT_TIME_RULES),
            "rechecked_rules": list(self.rechecked_rules),
            "min_timestamp": dumped["min_timestamp"],
            "max_timestamp": dumped["max_timestamp"],
            "future_time_tolerance_seconds": self.thresholds.future_time_tolerance_seconds,
            "future_time_reference": self.future_time_reference.value,
            "watermark_guard": {
                "mode": self.watermark_guard.mode.value,
                "reference": self.watermark_guard.reference.value,
                "max_skew_seconds": self.watermark_guard.max_skew_seconds,
            },
        }

    def recheck(self, payload: Mapping[str, Any], record: Mapping[str, Any], event_time: datetime) -> str | None:
        """None if `event_time` passes every re-checked rule; otherwise
        `UNATTRIBUTED_EVENT_TIME_RECHECK` or `UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE`."""
        generated_at = _parse_timestamp(payload.get("generated_at"))
        named = {"event_time": event_time}
        if generated_at is not None:
            named["generated_at"] = generated_at
        if timestamp_bounds_reasons(self.thresholds, **named):
            return UNATTRIBUTED_EVENT_TIME_RECHECK
        ingestion_time = _parse_timestamp(record.get("ingestion_time"))
        reference = generated_at if self.future_time_reference is FutureTimeReference.GENERATED_AT else ingestion_time
        if reference is None:
            return UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE
        if future_time_reason(event_time, self.thresholds, reference) is not None:
            return UNATTRIBUTED_EVENT_TIME_RECHECK
        guard = self.watermark_guard
        if guard.mode is WatermarkGuardMode.QUARANTINE:
            arrival = guard.arrival(ingestion_time=ingestion_time,
                                    processing_time=_parse_timestamp(record.get("processing_time")))
            if arrival is None:
                return UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE
            if guard.reason(event_time, arrival) is not None:
                return UNATTRIBUTED_EVENT_TIME_RECHECK
        return None

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
        start = (_NAIVE_EPOCH + index * self.length - self.offset).replace(tzinfo=UTC)
        # The bucket's end and local labels must be representable too (see `labels`).
        end = start + self.length
        start.astimezone(self.local_tz)
        end.astimezone(self.local_tz)
        return start

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


def _parse_timestamp(value: Any) -> datetime | None:
    """A Silver-strict timestamp (ISO-8601 string with a UTC offset, same
    check as Silver rule `parsable_timestamps`), or None."""
    if timestamp_problem(value) is not None:
        return None
    try:
        return _DATETIME.validate_json(json.dumps(value), strict=True)
    except ValidationError:  # pragma: no cover - timestamp_problem already validated it
        return None


def attribute_quarantined(
    record: Mapping[str, Any],
    known_route_ids: frozenset[str],
    periods: Periods,
    *,
    event_time_check: EventTimeCheck | None = None,
) -> tuple[str, datetime] | str:
    """(route_id, period start) for a quarantined Silver record whose
    route_id and event_time are both usable; otherwise the
    `UNATTRIBUTED_*` reason (first that applies, in `UNATTRIBUTED_REASONS`
    order). Usable means: the record is a JSON object Silver kept in parsed
    form under a supported schema_version, `route_id` is a DE-01 reference
    route, `event_time` is an offset-carrying ISO-8601 timestamp, Silver did
    not reject the record under an event-time rule, it passes Gold's re-check
    of those rules (`event_time_check`; `run_gold` always passes one), and
    its bucket is representable."""
    if record.get("raw_format") == "bronze_parse_error":
        return UNATTRIBUTED_BRONZE_PARSE_ERROR
    payload = record.get("raw_payload")
    if not isinstance(payload, dict):
        return UNATTRIBUTED_PAYLOAD_UNAVAILABLE
    rules = record.get("rejection_rules") or ()
    if SUPPORTED_SCHEMA_VERSION in rules:
        return UNATTRIBUTED_SCHEMA_VERSION
    route_id = payload.get("route_id")
    if not isinstance(route_id, str) or route_id not in known_route_ids:
        return UNATTRIBUTED_ROUTE_UNKNOWN
    event_time = _parse_timestamp(payload.get("event_time"))
    if event_time is None:
        return UNATTRIBUTED_EVENT_TIME_UNPARSEABLE
    if EVENT_TIME_RULES.intersection(rules):
        return UNATTRIBUTED_EVENT_TIME_REJECTED
    if event_time_check is not None:
        problem = event_time_check.recheck(payload, record, event_time)
        if problem is not None:
            return problem
    try:
        start = periods.start_of(event_time)
    except (OverflowError, ValueError):
        return UNATTRIBUTED_EVENT_TIME_UNPARSEABLE
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
    event_time_check: EventTimeCheck | None = None,
) -> Aggregate:
    """Build the route-performance rows. `reference_routes` maps DE-01
    route_id -> (route_number, name); `lineage` holds `gold_run_id`,
    `silver_run_id` and `processing_time`, copied onto every row;
    `event_time_check` (from the Silver manifest) drives quarantine
    attribution and, as a contract check, every clean event_time must lie
    within its timestamp bounds.

    Rows are ordered by (period_start, route_id); every (route, period) with
    at least one clean or attributed quarantined record gets a row."""
    cells: dict[tuple[str, datetime], _Cell] = {}
    names: dict[str, tuple[Any, Any]] = {}
    result = Aggregate(rows=[], clean_by_status=dict.fromkeys(_EVENT_TIME_STATUSES, 0),
                       unattributed_by_reason=dict.fromkeys(UNATTRIBUTED_REASONS, 0))

    for record in clean:
        route_id, event_time, delay, status = _clean_fields(record)
        if event_time_check is not None and not event_time_check.within_bounds(event_time):
            raise GoldInputError(
                f"Silver clean record {record.get('event_id')!r}: event_time {record.get('event_time')!r} is "
                "outside the Silver timestamp bounds recorded in its manifest (breaks the Silver clean contract)"
            )
        try:
            period_start = periods.start_of(event_time)
        except OverflowError as exc:
            raise GoldInputError(
                f"Silver clean record {record.get('event_id')!r}: event_time {record.get('event_time')!r} "
                f"falls in a period that cannot be represented ({exc})"
            ) from exc
        cell = cells.setdefault((route_id, period_start), _Cell())
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
        target = attribute_quarantined(record, known, periods, event_time_check=event_time_check)
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
