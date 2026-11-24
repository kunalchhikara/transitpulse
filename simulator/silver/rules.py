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
   route ids and the future-time reference instant; each reason string it
   returns is mapped to a rule id through `THRESHOLD_REASON_RULES`. An
   unmapped reason raises `ValidationContractError` (DE-01 drift fails loudly
   instead of producing an unlabelled rejection).

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
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import TypeAdapter, ValidationError

from simulator.config.models import ValidationThresholds
from simulator.schemas.telemetry import TelemetryEvent, validate_against_thresholds
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
NON_NEGATIVE_SPEED = "non_negative_speed"
MAX_SPEED = "max_speed"
NON_NEGATIVE_OCCUPANCY = "non_negative_occupancy"
MAX_OCCUPANCY = "max_occupancy"
HEADING_RANGE = "heading_range"
KNOWN_ROUTE = "known_route"
FUTURE_TIME_TOLERANCE = "future_time_tolerance"


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
        "strings/bools to numbers), status enum, no unknown fields.",
    ),
    RuleSpec(
        VALID_COORDINATES, CATEGORY_RULE, "valid latitude/longitude",
        "latitude in [-90, 90] and longitude in [-180, 180] (TelemetryEvent field bounds).",
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
        KNOWN_ROUTE, CATEGORY_RULE, "known route where applicable",
        "route_id is a DE-01 reference route (when thresholds.require_known_route).",
    ),
    RuleSpec(
        FUTURE_TIME_TOLERANCE, CATEGORY_RULE, "future-time tolerance",
        "event_time <= reference + thresholds.future_time_tolerance_seconds, where reference is the event's "
        "generated_at (default) or the Bronze ingestion_time (silver.future_time_reference).",
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
)

TIMESTAMP_FIELDS: tuple[str, ...] = ("generated_at", "event_time")
_RANGE_ERROR_TYPES = frozenset({"greater_than_equal", "less_than_equal", "greater_than", "less_than"})
#: ISO-8601 calendar date-time shape. Pydantic would otherwise also accept a
#: unix-epoch number inside a string ("1735689600"), which is not ISO-8601.
_ISO_DATETIME_SHAPE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")
_DATETIME = TypeAdapter(datetime)


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
    """`event` is set iff the record is valid (`violations` empty)."""

    event: TelemetryEvent | None
    violations: tuple[Violation, ...]

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
    return ""


class TelemetryValidator:
    """Applies `RULE_CATALOGUE` to one Bronze object record. Stateless per
    record (dedup and event-time state live in the pipeline)."""

    def __init__(
        self,
        thresholds: ValidationThresholds,
        known_route_ids: Iterable[str],
        *,
        use_ingestion_time_reference: bool = False,
    ) -> None:
        self.thresholds = thresholds
        self.known_route_ids = frozenset(known_route_ids)
        self.use_ingestion_time_reference = use_ingestion_time_reference

    def validate(
        self,
        original: dict[str, Any],
        original_text: str,
        *,
        ingestion_time: datetime,
    ) -> ValidationOutcome:
        """`original` is the input object (Bronze metadata stripped) and
        `original_text` its exact source text; `ingestion_time` the Bronze
        ingestion instant (used only with the ingestion-time reference)."""
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
        if found:
            return ValidationOutcome(None, _ordered(found))
        assert event is not None  # stage 1 passed

        # Stage 2: DE-01 thresholds, reused unchanged.
        reference = ingestion_time if self.use_ingestion_time_reference else event.generated_at
        for reason in validate_against_thresholds(
            event, thresholds, known_route_ids=set(self.known_route_ids), now=reference
        ):
            rule = threshold_reason_rule(reason)
            detail = _threshold_detail(rule, event, thresholds, reference)
            found.setdefault(rule, []).append(f"{reason} ({detail})" if detail else reason)
        if found:
            return ValidationOutcome(None, _ordered(found))
        return ValidationOutcome(event, ())
