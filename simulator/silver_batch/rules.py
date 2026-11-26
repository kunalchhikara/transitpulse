"""DE-12 rule catalogue, strict model validation and standardisation.

Validation happens **before** any merge/dedup decision (validate-then-merge,
as DE-06): an invalid row never claims a primary key.

For a stored Bronze record of dataset D (precedence = catalogue order):

1. `supported_schema_version` - `schema_version` must be present (else
   `missing_field`) and one D supports; a failure short-circuits (the version
   decides which contract applies).
2. Every field of D's model must be **present** (`missing_field`), even one
   the model gives a default: defaults are a producer convenience, not a
   consumer contract.
3. `D.model.model_validate_json(<exact original text>, strict=True)`: JSON
   types are not coerced (a string is not a number, an epoch number is not a
   timestamp), unknown fields are rejected (`extra="forbid"`), enums are
   closed. Every pydantic error is mapped to a named rule.
4. Post-model checks: every timestamp carries a UTC offset (DE-01's
   `TelemetryEvent` allows naive datetimes; Silver never guesses a zone),
   floats are finite, primary-key strings are not blank.

`historical_telemetry` gets DE-04 model validation only - the PROJECT.md
section 12 telemetry rules (DE-06) are not applied here (README limitation).
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import ValidationError

from simulator.bronze.ingest import format_timestamp
from simulator.silver_batch.datasets import SilverDataset

CATEGORY_PARSE = "parse"
CATEGORY_SCHEMA = "schema"
CATEGORY_SOURCE = "source"
CATEGORY_MERGE = "merge"
CATEGORIES: tuple[str, ...] = (CATEGORY_PARSE, CATEGORY_SCHEMA, CATEGORY_SOURCE, CATEGORY_MERGE)

RULE_BRONZE_PARSE_ERROR = "bronze_parse_error"
RULE_SCHEMA_VERSION = "supported_schema_version"
RULE_MISSING_FIELD = "missing_field"
RULE_UNKNOWN_FIELD = "unknown_field"
RULE_WRONG_TYPE = "wrong_type"
RULE_INVALID_ENUM = "invalid_enum"
RULE_TIMESTAMP_WITHOUT_OFFSET = "timestamp_without_offset"
RULE_VALUE_OUT_OF_RANGE = "value_out_of_range"
RULE_MODEL_CONSTRAINT = "model_constraint"
RULE_NON_FINITE_NUMBER = "non_finite_number"
RULE_BLANK_PRIMARY_KEY = "blank_primary_key"
RULE_SCHEMA_CONFORMANCE = "schema_conformance"
RULE_PRESERVED_BAD_SOURCE_ROW = "preserved_bad_source_row"
RULE_MERGE_CONFLICT = "merge_conflict"
RULE_DUPLICATE_KEY_IN_SNAPSHOT = "duplicate_key_in_snapshot"


@dataclass(frozen=True)
class RuleSpec:
    rule_id: str
    category: str
    check: str

    def as_dict(self) -> dict[str, str]:
        return {"rule_id": self.rule_id, "category": self.category, "check": self.check}


#: Precedence = order. Recorded verbatim in every run manifest.
RULE_CATALOGUE: tuple[RuleSpec, ...] = (
    RuleSpec(RULE_BRONZE_PARSE_ERROR, CATEGORY_PARSE,
             "Bronze could not store the line as a JSON object (DE-05 parse-error record); raw bytes kept"),
    RuleSpec(RULE_SCHEMA_VERSION, CATEGORY_SCHEMA,
             "schema_version is a string the dataset's contract supports; short-circuits"),
    RuleSpec(RULE_MISSING_FIELD, CATEGORY_SCHEMA,
             "every field of the dataset model is present (defaults are not applied)"),
    RuleSpec(RULE_UNKNOWN_FIELD, CATEGORY_SCHEMA, "no field outside the dataset model (extra='forbid')"),
    RuleSpec(RULE_WRONG_TYPE, CATEGORY_SCHEMA,
             "strict JSON types: no coercion (string->number, number->timestamp, bool->int, ...)"),
    RuleSpec(RULE_INVALID_ENUM, CATEGORY_SCHEMA, "enum fields hold one of the model's values"),
    RuleSpec(RULE_TIMESTAMP_WITHOUT_OFFSET, CATEGORY_SCHEMA, "every timestamp carries a UTC offset"),
    RuleSpec(RULE_VALUE_OUT_OF_RANGE, CATEGORY_SCHEMA, "model field bounds (ge/gt/le/lt, min_length)"),
    RuleSpec(RULE_MODEL_CONSTRAINT, CATEGORY_SCHEMA,
             "cross-field model validators (e.g. arrival after departure, TRIP scope needs trip_id)"),
    RuleSpec(RULE_NON_FINITE_NUMBER, CATEGORY_SCHEMA, "floats are finite (1e400 overflows to inf)"),
    RuleSpec(RULE_BLANK_PRIMARY_KEY, CATEGORY_SCHEMA, "primary-key string columns are not blank"),
    RuleSpec(RULE_SCHEMA_CONFORMANCE, CATEGORY_SCHEMA, "any other strict-model failure (fallback)"),
    RuleSpec(RULE_PRESERVED_BAD_SOURCE_ROW, CATEGORY_SOURCE,
             "row listed by DE-11 in preserved_bad_rows (bad source line landed under the preserve policy)"),
    RuleSpec(RULE_MERGE_CONFLICT, CATEGORY_MERGE,
             "same primary key and same updated_at as the current row, but a different payload"),
    RuleSpec(RULE_DUPLICATE_KEY_IN_SNAPSHOT, CATEGORY_MERGE,
             "primary key repeated within one full snapshot; the first occurrence wins"),
)
RULES_BY_ID: dict[str, RuleSpec] = {rule.rule_id: rule for rule in RULE_CATALOGUE}
RULE_IDS: tuple[str, ...] = tuple(RULES_BY_ID)

#: pydantic error `type` -> rule id. Anything unmapped -> `schema_conformance`.
_ERROR_TYPE_RULES: dict[str, str] = {
    "missing": RULE_MISSING_FIELD,
    "extra_forbidden": RULE_UNKNOWN_FIELD,
    "enum": RULE_INVALID_ENUM,
    "timezone_aware": RULE_TIMESTAMP_WITHOUT_OFFSET,
    "greater_than": RULE_VALUE_OUT_OF_RANGE,
    "greater_than_equal": RULE_VALUE_OUT_OF_RANGE,
    "less_than": RULE_VALUE_OUT_OF_RANGE,
    "less_than_equal": RULE_VALUE_OUT_OF_RANGE,
    "too_short": RULE_VALUE_OUT_OF_RANGE,
    "too_long": RULE_VALUE_OUT_OF_RANGE,
    "finite_number": RULE_NON_FINITE_NUMBER,
    "value_error": RULE_MODEL_CONSTRAINT,
    "assertion_error": RULE_MODEL_CONSTRAINT,
}


def error_type_rule(error_type: str) -> str:
    if error_type in _ERROR_TYPE_RULES:
        return _ERROR_TYPE_RULES[error_type]
    if error_type.endswith(("_type", "_parsing")) or error_type in ("datetime_from_date_parsing", "int_from_float", "date_from_datetime_inexact", "date_from_datetime_parsing"):
        return RULE_WRONG_TYPE
    return RULE_SCHEMA_CONFORMANCE


@dataclass(frozen=True)
class Violation:
    rule: str
    reason: str

    @property
    def category(self) -> str:
        return RULES_BY_ID[self.rule].category

    def as_dict(self) -> dict[str, str]:
        return {"rule": self.rule, "category": self.category, "reason": _escape_surrogates(self.reason)}


def ordered(violations: list[Violation]) -> list[Violation]:
    """Stable precedence order (catalogue order, then discovery order)."""
    rank = {rule_id: index for index, rule_id in enumerate(RULE_IDS)}
    return sorted(violations, key=lambda v: rank[v.rule])


def _escape_surrogates(text: str) -> str:
    return text.encode("utf-8", "backslashreplace").decode("utf-8")


def _short(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 80 else text[:77] + "..."


# -- standardisation -----------------------------------------------------------


def _standard_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_timestamp(value)  # UTC, microseconds, "Z"
    if isinstance(value, date):
        return value.isoformat()  # event_date: the LOCAL service date, passed through
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, list):
        return [_standard_value(item) for item in value]
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def row_hash(payload: dict[str, Any]) -> str:
    """sha256 of the standardised business payload (lineage excluded)."""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ValidationResult:
    payload: dict[str, Any] | None  # standardised, model field order; None if invalid
    violations: tuple[Violation, ...]

    @property
    def is_valid(self) -> bool:
        return not self.violations


def _datetimes(value: Any) -> list[datetime]:
    if isinstance(value, datetime):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, datetime)]
    return []


def validate_record(dataset: SilverDataset, original: dict[str, Any], original_text: str) -> ValidationResult:
    """Strictly validate one stored Bronze record (metadata already
    stripped). `original_text` is the exact original object text."""
    found: list[Violation] = []
    if "schema_version" not in original:
        return ValidationResult(None, (Violation(RULE_MISSING_FIELD, "field 'schema_version' is missing"),))
    version = original["schema_version"]
    if not isinstance(version, str) or version not in dataset.supported_schema_versions:
        return ValidationResult(
            None,
            (Violation(
                RULE_SCHEMA_VERSION,
                f"schema_version {_short(version)} not supported for {dataset.name} "
                f"(supported: {list(dataset.supported_schema_versions)})",
            ),),
        )

    reported_missing: set[str] = set()
    for field in dataset.fields:
        if field not in original:
            reported_missing.add(field)
            found.append(Violation(RULE_MISSING_FIELD, f"field {field!r} is missing"))

    instance = None
    try:
        instance = dataset.model.model_validate_json(original_text, strict=True)
    except ValidationError as exc:
        for error in exc.errors(include_url=False):
            loc = ".".join(str(part) for part in error.get("loc", ())) or "<record>"
            if error["type"] == "missing" and loc in reported_missing:
                continue
            rule = error_type_rule(error["type"])
            found.append(Violation(rule, f"{loc}: {error['msg']} (input {_short(error.get('input'))})"))

    if instance is not None and not found:
        for name in dataset.fields:
            value = getattr(instance, name)
            for moment in _datetimes(value):
                if moment.tzinfo is None or moment.utcoffset() is None:
                    found.append(Violation(RULE_TIMESTAMP_WITHOUT_OFFSET, f"{name}: timestamp has no UTC offset"))
            if isinstance(value, float) and not math.isfinite(value):
                found.append(Violation(RULE_NON_FINITE_NUMBER, f"{name}: non-finite number {value!r}"))
        for column in dataset.primary_key:
            value = getattr(instance, column)
            if isinstance(value, str) and not value.strip():
                found.append(Violation(RULE_BLANK_PRIMARY_KEY, f"{column}: primary-key value is blank"))

    if found:
        return ValidationResult(None, tuple(ordered(found)))
    assert instance is not None
    payload = {name: _standard_value(getattr(instance, name)) for name in dataset.fields}
    return ValidationResult(payload, ())


def primary_key_of(dataset: SilverDataset, original: dict[str, Any]) -> dict[str, Any] | None:
    """The primary-key columns of a (possibly invalid) parsed object, for the
    quarantine record; None if any is absent or not a JSON scalar."""
    key = {}
    for column in dataset.primary_key:
        value = original.get(column)
        if not isinstance(value, (str, int)) or isinstance(value, bool):
            return None
        key[column] = _escape_surrogates(value) if isinstance(value, str) else value
    return key
