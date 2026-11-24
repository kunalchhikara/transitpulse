"""DE-06 Silver validation: the rule catalogue (PROJECT.md section 12) and the
validator that applies it to one Bronze telemetry record.

Rule precedence is the order of `RULE_CATALOGUE`. A rejected record carries
**every** rule it violates within the validation stage that rejected it, in
catalogue order; the first one is its `primary_rule` (used for the
one-bucket-per-record `quarantined_by_rule` counters).

Two stages (see README "DE-06: Rule catalogue"):

1. **Structural** - the record against the DE-01 `TelemetryEvent` contract:
   schema-version gate, required fields, parsable timezone-aware ISO-8601
   timestamps, then `TelemetryEvent.model_validate_json(<original text>,
   strict=True)`. Pydantic reports every failing field at once; each error is
   mapped to the section 12 rule it represents (latitude out of range ->
   `valid_coordinates`, negative speed -> `non_negative_speed`, ...).
2. **Thresholds** - only for a record that passed stage 1 (the business rules
   need a typed event): DE-01's `validate_against_thresholds` is called
   *unchanged* with the DE-01 `ValidationThresholds`, the DE-01 reference
   route ids, the DE-01 service area (`sydney_bounding_box`) and the
   future-time reference instant; each reason string it returns is mapped to
   a rule id through `THRESHOLD_REASON_RULES`. An unmapped reason raises
   `ValidationContractError` (DE-01 drift fails loudly instead of producing
   an unlabelled rejection). The Silver-owned `watermark_guard` rule (DE-08,
   BACKLOG FU-06; configured in `[silver.watermark_guard]`) is evaluated in
   the same stage.

Divergences from DE-01 (justified in the README): the schema-version gate is
evaluated first, on the raw value, because the version decides which contract
applies (a record of an unknown version would otherwise be reported as a pile
of misleading structural errors); timestamps must carry a UTC offset (DE-01's
helper assumes UTC for naive values, which would silently shift a Sydney
local time by 10-11 hours); string-typed numbers/timestamps are not coerced
(strict mode - section 8 "type enforcement").
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from pydantic import TypeAdapter, ValidationError

from simulator.config.models import BoundingBox, ValidationThresholds
from simulator.schemas.telemetry import TelemetryEvent, validate_against_thresholds
from simulator.silver.config import (
    WatermarkGuardConfig,
    WatermarkGuardMode,
    WatermarkGuardReference,
)
from simulator.silver.errors import ValidationContractError

# -- categories ----------------------------------------------------------------

#: Bronze could not store the line as a JSON object at all.
CATEGORY_PARSE = "parse"
#: A JSON object that does not conform to the telemetry contract (shape/types).
CATEGORY_SCHEMA = "schema"
#: A structurally valid event that violates a data-quality/business rule.
CATEGORY_RULE = "rule"
CATEGORIES: tuple[str, ...] = (CATEGORY_PARSE, CATEGORY_SCHEMA, CATEGORY_RULE)

# -- rule ids ----------------------------------------------------------------

BRONZE_PARSE_ERROR = "bronze_parse_error"
SUPPORTED_SCHEMA_VERSION = "supported_schema_version"
REQUIRED_FIELDS = "required_fields"
PARSABLE_TIMESTAMPS = "parsable_timestamps"
SCHEMA_CONFORMANCE = "schema_conformance"
VALID_COORDINATES = "valid_coordinates"
SERVICE_AREA = "service_area"
NON_NEGATIVE_SPEED = "non_negative_speed"
MAX_SPEED = "max_speed"
NON_NEGATIVE_OCCUPANCY = "non_negative_occupancy"
MAX_OCCUPANCY = "max_occupancy"
HEADING_RANGE = "heading_range"
DELAY_RANGE = "delay_range"
KNOWN_ROUTE = "known_route"
TIMESTAMP_BOUNDS = "timestamp_bounds"
FUTURE_TIME_TOLERANCE = "future_time_tolerance"
WATERMARK_GUARD = "watermark_guard"


@dataclass(frozen=True)
class RuleSpec:
    rule_id: str
    category: str
    #: The PROJECT.md section 12 bullet this rule implements (None: a rule
    #: Silver needs beyond the section 12 list).
    project_rule: str | None
    description: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule_id,
            "category": self.category,
            "project_section_12_rule": self.project_rule,
            "description": self.description,
        }


#: Precedence order. Configured values come from DE-01 `ValidationThresholds`.
RULE_CATALOGUE: tuple[RuleSpec, ...] = (
    RuleSpec(
        BRONZE_PARSE_ERROR, CATEGORY_PARSE, None,
        "Bronze stored the line as a parse-error record (not UTF-8 / not JSON / not an object / duplicate "
        "key / metadata collision); the Bronze error code is kept in the reason.",
    ),
    RuleSpec(
        SUPPORTED_SCHEMA_VERSION, CATEGORY_SCHEMA, "supported schema version",
        "schema_version is in thresholds.supported_schema_versions. Checked first; a failure short-circuits "
        "the record (an unknown version's shape cannot be judged against this contract).",
    ),
    RuleSpec(
        REQUIRED_FIELDS, CATEGORY_SCHEMA, "required event_id/vehicle_id",
        "Every field in thresholds.required_fields (and every field TelemetryEvent requires) is present, "
        "not null and, for strings, not blank.",
    ),
    RuleSpec(
        PARSABLE_TIMESTAMPS, CATEGORY_SCHEMA, "parsable timestamps",
        "generated_at and event_time are ISO-8601 date-time strings with a UTC offset.",
    ),
    RuleSpec(
        SCHEMA_CONFORMANCE, CATEGORY_SCHEMA, None,
        "Strict TelemetryEvent conformance not covered by a more specific rule: JSON types (no coercion of "
        "strings/bools to numbers), status enum, no unknown fields, float fields finite (a JSON number "
        "outside the float64 range, e.g. 1e400, parses to +/-inf and is rejected here, whatever the "
        "configured thresholds).",
    ),
    RuleSpec(
        VALID_COORDINATES, CATEGORY_RULE, "valid latitude/longitude",
        "latitude in [-90, 90] and longitude in [-180, 180] (TelemetryEvent field bounds).",
    ),
    RuleSpec(
        SERVICE_AREA, CATEGORY_RULE, "valid latitude/longitude (service area)",
        "DE-08/FU-08: position inside DE-01 sydney_bounding_box expanded by "
        "thresholds.service_area_margin_degrees on every side, edges inclusive (when "
        "thresholds.require_service_area). Stage 2, so a position outside the global range keeps "
        "valid_coordinates as its only rule.",
    ),
    RuleSpec(
        NON_NEGATIVE_SPEED, CATEGORY_RULE, "nonnegative speed",
        "speed_kmh >= 0 (TelemetryEvent) and >= thresholds.min_speed_kmh.",
    ),
    RuleSpec(
        MAX_SPEED, CATEGORY_RULE, "configurable upper-speed threshold",
        "speed_kmh <= thresholds.max_speed_kmh.",
    ),
    RuleSpec(
        NON_NEGATIVE_OCCUPANCY, CATEGORY_RULE, "nonnegative occupancy",
        "occupancy >= 0 (TelemetryEvent) and >= thresholds.min_occupancy.",
    ),
    RuleSpec(
        MAX_OCCUPANCY, CATEGORY_RULE, "nonnegative occupancy (configured upper bound)",
        "occupancy <= thresholds.max_occupancy.",
    ),
    RuleSpec(
        HEADING_RANGE, CATEGORY_RULE, None,
        "heading within TelemetryEvent's [0, 360) and thresholds [min_heading_degrees, max_heading_degrees] "
        "(DE-01 threshold beyond the section 12 list).",
    ),
    RuleSpec(
        DELAY_RANGE, CATEGORY_RULE, None,
        "DE-08/FU-08: thresholds.min_delay_seconds <= delay_seconds <= thresholds.max_delay_seconds "
        "(inclusive; keeps absurd values such as 10**30 out of clean and inside int64).",
    ),
    RuleSpec(
        KNOWN_ROUTE, CATEGORY_RULE, "known route where applicable",
        "route_id is a DE-01 reference route (when thresholds.require_known_route).",
    ),
    RuleSpec(
        TIMESTAMP_BOUNDS, CATEGORY_RULE, "parsable timestamps (absolute bounds)",
        "DE-08/FU-10: thresholds.min_timestamp <= event_time <= thresholds.max_timestamp, and the same "
        "for generated_at (inclusive). An event-time rule: Gold never attributes such a record to a period.",
    ),
    RuleSpec(
        FUTURE_TIME_TOLERANCE, CATEGORY_RULE, "future-time tolerance",
        "event_time <= reference + thresholds.future_time_tolerance_seconds, where reference is the event's "
        "generated_at (default) or the Bronze ingestion_time (silver.future_time_reference).",
    ),
    RuleSpec(
        WATERMARK_GUARD, CATEGORY_RULE, "future-time tolerance (watermark guard)",
        "DE-08/FU-06: event_time <= arrival + silver.watermark_guard.max_skew_seconds, where arrival is the "
        "Bronze ingestion_time (default) or the Silver processing_time (watermark_guard.reference). A rule "
        "only with watermark_guard.mode = 'quarantine'; in 'flag' mode such an event is loaded with "
        "_silver_watermark_guarded = true and does not advance the watermark.",
    ),
)

RULES_BY_ID: dict[str, RuleSpec] = {rule.rule_id: rule for rule in RULE_CATALOGUE}
RULE_IDS: tuple[str, ...] = tuple(rule.rule_id for rule in RULE_CATALOGUE)
_PRECEDENCE = {rule_id: index for index, rule_id in enumerate(RULE_IDS)}

#: DE-01 `validate_against_thresholds` reason prefixes -> rule id. Every reason
#: that function can return must match exactly one prefix (tested).
THRESHOLD_REASON_RULES: tuple[tuple[str, str], ...] = (
    ("unsupported schema_version", SUPPORTED_SCHEMA_VERSION),
    ("speed_kmh below configured minimum", NON_NEGATIVE_SPEED),
    ("speed_kmh exceeds configured maximum", MAX_SPEED),
    ("occupancy below configured minimum", NON_NEGATIVE_OCCUPANCY),
    ("occupancy exceeds configured maximum", MAX_OCCUPANCY),
    ("heading below configured minimum", HEADING_RANGE),
    ("heading exceeds configured maximum", HEADING_RANGE),
    ("unknown route_id", KNOWN_ROUTE),
    ("event_time is beyond the future-time tolerance", FUTURE_TIME_TOLERANCE),
    ("position outside the configured service area", SERVICE_AREA),
    ("delay_seconds below configured minimum", DELAY_RANGE),
    ("delay_seconds exceeds configured maximum", DELAY_RANGE),
    ("event_time outside configured timestamp bounds", TIMESTAMP_BOUNDS),
    ("generated_at outside configured timestamp bounds", TIMESTAMP_BOUNDS),
)

#: Rules whose failure means the record's event_time cannot be trusted to
#: place it in time: Gold never attributes such a record to a period.
EVENT_TIME_RULES: tuple[str, ...] = (TIMESTAMP_BOUNDS, FUTURE_TIME_TOLERANCE, WATERMARK_GUARD)

TIMESTAMP_FIELDS: tuple[str, ...] = ("generated_at", "event_time")
_RANGE_ERROR_TYPES = frozenset({"greater_than_equal", "less_than_equal", "greater_than", "less_than"})
#: ISO-8601 calendar date-time shape. Pydantic would otherwise also accept a
#: unix-epoch number inside a string ("1735689600"), which is not ISO-8601.
_ISO_DATETIME_SHAPE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")
_DATETIME = TypeAdapter(datetime)
#: Float fields of the contract. A JSON number that overflows float64 (valid
#: JSON syntax, so Bronze stores it) parses to +/-inf; field bounds catch most
#: of them, but e.g. `speed_kmh` has no upper bound in the model and the
#: configured `max_speed_kmh` may itself be `inf` (TOML allows it), so
#: finiteness is checked explicitly - a non-finite value never reaches clean.
FLOAT_FIELDS: tuple[str, ...] = tuple(
    name for name, info in TelemetryEvent.model_fields.items() if info.annotation is float
)


@dataclass(frozen=True)
class Violation:
    rule: str
    reason: str

    @property
    def category(self) -> str:
        return RULES_BY_ID[self.rule].category

    def as_dict(self) -> dict[str, str]:
        return {"rule": self.rule, "category": self.category, "reason": self.reason}


@dataclass(frozen=True)
class ValidationOutcome:
    """`event` is set iff the record is valid (`violations` empty).
    `watermark_guarded`: valid, but tripped the FU-06 watermark guard in
    `flag` mode (loaded flagged; must not advance the watermark)."""

    event: TelemetryEvent | None
    violations: tuple[Violation, ...]
    watermark_guarded: bool = False

    @property
    def is_valid(self) -> bool:
        return not self.violations


def parse_error_violation(code: str, detail: str | None) -> Violation:
    """The single violation of a Bronze parse-error record."""
    suffix = f": {detail}" if detail else ""
    return Violation(BRONZE_PARSE_ERROR, f"Bronze parse error '{code}'{suffix}")


def _ordered(found: dict[str, list[str]]) -> tuple[Violation, ...]:
    return tuple(
        Violation(rule, "; ".join(reasons)) for rule, reasons in sorted(found.items(), key=lambda kv: _PRECEDENCE[kv[0]])
    )


def _is_missing(record: dict[str, Any], field: str) -> bool:
    if field not in record or record[field] is None:
        return True
    value = record[field]
    return isinstance(value, str) and not value.strip()


def timestamp_problem(value: Any) -> str | None:
    """Why `value` is not a parsable, offset-carrying ISO-8601 date-time
    (None if it is)."""
    if not isinstance(value, str):
        return f"is {type(value).__name__}, not an ISO-8601 string"
    if not _ISO_DATETIME_SHAPE.match(value):
        return f"{value!r} is not an ISO-8601 date-time"
    try:
        parsed = _DATETIME.validate_json(json.dumps(value), strict=True)
    except ValidationError:
        return f"{value!r} is not a parsable ISO-8601 date-time"
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return f"{value!r} has no UTC offset"
    return None


def _model_error_rule(error: dict[str, Any], *, already_reported: set[str]) -> str | None:
    """Map one pydantic error to a rule id (None: already reported by an
    earlier, more specific check on the same field)."""
    loc = error.get("loc") or ()
    field = loc[0] if loc else None
    if field in already_reported:
        return None
    error_type = error.get("type", "")
    if field is None:
        return SCHEMA_CONFORMANCE
    if field in TIMESTAMP_FIELDS:
        return PARSABLE_TIMESTAMPS
    if error_type == "missing":
        return REQUIRED_FIELDS
    if field in ("latitude", "longitude") and error_type in _RANGE_ERROR_TYPES:
        return VALID_COORDINATES
    if field == "speed_kmh" and error_type in _RANGE_ERROR_TYPES:
        return NON_NEGATIVE_SPEED
    if field == "occupancy" and error_type in _RANGE_ERROR_TYPES:
        return NON_NEGATIVE_OCCUPANCY
    if field == "heading" and error_type in _RANGE_ERROR_TYPES:
        return HEADING_RANGE
    return SCHEMA_CONFORMANCE


def threshold_reason_rule(reason: str) -> str:
    for prefix, rule in THRESHOLD_REASON_RULES:
        if reason.startswith(prefix):
            return rule
    raise ValidationContractError(f"unmapped DE-01 validate_against_thresholds reason: {reason!r}")


def _threshold_detail(rule: str, event: TelemetryEvent, thresholds: ValidationThresholds, reference: datetime) -> str:
    if rule in (NON_NEGATIVE_SPEED, MAX_SPEED):
        return f"speed_kmh={event.speed_kmh}, allowed [{thresholds.min_speed_kmh}, {thresholds.max_speed_kmh}]"
    if rule in (NON_NEGATIVE_OCCUPANCY, MAX_OCCUPANCY):
        return f"occupancy={event.occupancy}, allowed [{thresholds.min_occupancy}, {thresholds.max_occupancy}]"
    if rule == HEADING_RANGE:
        return (
            f"heading={event.heading}, allowed "
            f"[{thresholds.min_heading_degrees}, {thresholds.max_heading_degrees}]"
        )
    if rule == FUTURE_TIME_TOLERANCE:
        return (
            f"event_time={event.event_time.isoformat()} > reference {reference.isoformat()} "
            f"+ {thresholds.future_time_tolerance_seconds}s"
        )
    if rule == DELAY_RANGE:
        return (
            f"delay_seconds={event.delay_seconds}, allowed "
            f"[{thresholds.min_delay_seconds}, {thresholds.max_delay_seconds}]"
        )
    if rule == SERVICE_AREA:
        return (
            f"latitude={event.latitude}, longitude={event.longitude}; service area = DE-01 sydney_bounding_box "
            f"+ {thresholds.service_area_margin_degrees} deg"
        )
    if rule == TIMESTAMP_BOUNDS:
        return (
            f"event_time={event.event_time.isoformat()}, generated_at={event.generated_at.isoformat()}, allowed "
            f"[{thresholds.min_timestamp.isoformat()}, {thresholds.max_timestamp.isoformat()}]"
        )
    return ""


@dataclass(frozen=True)
class WatermarkGuard:
    """DE-08 / BACKLOG FU-06: an event cannot have been observed long after
    it arrived. `reason` is None when `event_time <= arrival +
    max_skew_seconds`, where arrival is the Bronze `ingestion_time` or the
    Silver `processing_time` (`reference`). Stateless per record, so Gold can
    re-apply it to quarantined records (the same idea as a broker rejecting
    messages whose timestamp is too far ahead of its own clock)."""

    mode: WatermarkGuardMode
    reference: WatermarkGuardReference
    max_skew_seconds: int

    @classmethod
    def from_config(cls, config: WatermarkGuardConfig) -> WatermarkGuard:
        return cls(config.mode, config.reference, config.max_skew_seconds)

    @property
    def enabled(self) -> bool:
        return self.mode is not WatermarkGuardMode.OFF

    def arrival(self, *, ingestion_time: datetime | None, processing_time: datetime | None) -> datetime | None:
        """The configured reference instant (None if not available)."""
        return ingestion_time if self.reference is WatermarkGuardReference.INGESTION_TIME else processing_time

    def reason(self, event_time: datetime, arrival: datetime) -> str | None:
        try:
            limit = arrival + timedelta(seconds=self.max_skew_seconds)
        except OverflowError:  # arrival at the end of time: nothing can exceed it
            return None
        if event_time <= limit:
            return None
        return (
            f"event_time {event_time.isoformat()} is more than {self.max_skew_seconds}s after its "
            f"{self.reference.value} {arrival.isoformat()} (watermark guard, mode {self.mode.value})"
        )


class TelemetryValidator:
    """Applies `RULE_CATALOGUE` to one Bronze object record. Stateless per
    record (dedup and event-time state live in the pipeline)."""

    def __init__(
        self,
        thresholds: ValidationThresholds,
        known_route_ids: Iterable[str],
        *,
        use_ingestion_time_reference: bool = False,
        service_area: BoundingBox | None = None,
        watermark_guard: WatermarkGuard | None = None,
    ) -> None:
        """`service_area` (DE-01 `sydney_bounding_box`) and `watermark_guard`
        are optional so existing callers keep the pre-DE-08 behaviour;
        `run_silver` always passes both."""
        self.thresholds = thresholds
        self.known_route_ids = frozenset(known_route_ids)
        self.use_ingestion_time_reference = use_ingestion_time_reference
        self.service_area = service_area
        self.watermark_guard = watermark_guard

    def validate(
        self,
        original: dict[str, Any],
        original_text: str,
        *,
        ingestion_time: datetime,
        processing_time: datetime | None = None,
    ) -> ValidationOutcome:
        """`original` is the input object (Bronze metadata stripped) and
        `original_text` its exact source text; `ingestion_time` the Bronze
        ingestion instant (future-time and watermark-guard reference) and
        `processing_time` the Silver run instant (watermark-guard reference)."""
        thresholds = self.thresholds

        # Stage 1a: the version gate decides which contract applies.
        if not _is_missing(original, "schema_version"):
            version = original["schema_version"]
            if not isinstance(version, str) or version not in thresholds.supported_schema_versions:
                return ValidationOutcome(
                    None,
                    (Violation(
                        SUPPORTED_SCHEMA_VERSION,
                        f"unsupported schema_version {version!r}; supported {thresholds.supported_schema_versions}",
                    ),),
                )

        found: dict[str, list[str]] = {}
        missing = [f for f in thresholds.required_fields if _is_missing(original, f)]
        if missing:
            found[REQUIRED_FIELDS] = [f"missing, null or blank required field(s): {missing}"]

        reported_fields = set(missing)
        problems = []
        for field in TIMESTAMP_FIELDS:
            if field in reported_fields or field not in original:
                continue
            problem = timestamp_problem(original[field])
            if problem is not None:
                problems.append(f"{field} {problem}")
                reported_fields.add(field)
        if problems:
            found[PARSABLE_TIMESTAMPS] = problems

        # Stage 1b: strict DE-01 contract on the exact source text.
        event: TelemetryEvent | None = None
        try:
            event = TelemetryEvent.model_validate_json(original_text, strict=True)
        except ValidationError as exc:
            for error in exc.errors():
                rule = _model_error_rule(error, already_reported=reported_fields)
                if rule is None:
                    continue
                loc = ".".join(str(part) for part in error.get("loc") or ()) or "<record>"
                found.setdefault(rule, []).append(f"{loc}: {error.get('msg', error.get('type'))}")
        if event is not None:
            for name in FLOAT_FIELDS:
                value = getattr(event, name)
                if not math.isfinite(value):
                    found.setdefault(SCHEMA_CONFORMANCE, []).append(
                        f"{name}: non-finite number {value} (JSON number outside the float64 range)"
                    )
        if found:
            return ValidationOutcome(None, _ordered(found))
        assert event is not None  # stage 1 passed

        # Stage 2: DE-01 thresholds, reused unchanged.
        reference = ingestion_time if self.use_ingestion_time_reference else event.generated_at
        for reason in validate_against_thresholds(
            event, thresholds, known_route_ids=set(self.known_route_ids), now=reference,
            service_area=self.service_area,
        ):
            rule = threshold_reason_rule(reason)
            detail = _threshold_detail(rule, event, thresholds, reference)
            found.setdefault(rule, []).append(f"{reason} ({detail})" if detail else reason)

        # Stage 2, Silver-owned: the FU-06 watermark guard.
        guarded = False
        guard = self.watermark_guard
        if guard is not None and guard.enabled:
            arrival = guard.arrival(ingestion_time=ingestion_time, processing_time=processing_time)
            if arrival is None:
                raise ValueError(f"watermark guard reference {guard.reference.value!r} needs that instant")
            reason = guard.reason(event.event_time, arrival)
            if reason is not None:
                if guard.mode is WatermarkGuardMode.QUARANTINE:
                    found.setdefault(WATERMARK_GUARD, []).append(reason)
                else:
                    guarded = True
        if found:
            return ValidationOutcome(None, _ordered(found))
        return ValidationOutcome(event, (), watermark_guarded=guarded)
