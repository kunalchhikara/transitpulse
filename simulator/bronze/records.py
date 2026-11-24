"""Line-level Bronze record construction (DE-05).

Bronze keeps the source representation with minimum transformation plus
ingestion metadata (PROJECT.md section 8, ADR-0002). For one input line this
module produces exactly one Bronze record (one output line):

* **Stored record** - the line is a UTF-8 JSON object whose keys are unique
  and do not collide with Bronze metadata. The output line is the input
  object's *own bytes* (JSON whitespace trimmed at both ends) with the
  metadata members spliced in before the closing brace. Every original field
  therefore survives byte-for-byte (number formatting, escapes, key order,
  nulls), and parsing the output yields the original object plus the
  metadata fields - nothing else. No business rule or schema is checked:
  a null `vehicle_id`, latitude 999, `"not-a-timestamp"`, a missing field or
  an unknown route is stored exactly as received.

* **Parse-error record** - anything else (truncated/unparseable text,
  invalid UTF-8, a JSON array/scalar, duplicate keys, a key that collides
  with Bronze metadata). The raw line is kept verbatim in
  `_bronze_raw_line` (base64 when it is not valid UTF-8) together with an
  explicit `_bronze_parse_error` code, so Silver (DE-06) can quarantine it
  with its raw payload (PROJECT.md section 12: invalid events must not
  silently disappear).

Blank (whitespace-only) lines carry no payload; the caller counts them in the
run manifest instead of storing them.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from typing import Any

# -- metadata contract -----------------------------------------------------

#: The three ingestion-metadata columns required by the DE-05 story.
INGESTION_TIME = "ingestion_time"
SOURCE = "source"
RUN_ID = "run_id"
REQUIRED_METADATA_FIELDS: tuple[str, ...] = (INGESTION_TIME, SOURCE, RUN_ID)

#: Namespace for Bronze-owned lineage/diagnostic fields. Any input key with
#: this prefix is treated as a metadata collision.
BRONZE_FIELD_PREFIX = "_bronze_"

#: Lineage: input file name (basename only, never a local absolute path) and
#: 1-based physical line number. Together they identify the exact input line
#: for replay/troubleshooting and preserve arrival order, which Silver needs
#: to reason about late/out-of-order delivery.
SOURCE_FILE = "_bronze_source_file"
LINE_NUMBER = "_bronze_line_number"

#: Present only on parse-error records.
PARSE_ERROR = "_bronze_parse_error"
PARSE_ERROR_DETAIL = "_bronze_parse_error_detail"
RAW_LINE = "_bronze_raw_line"
RAW_ENCODING = "_bronze_raw_encoding"

#: Exact metadata members appended to a stored record, in output order.
STORED_RECORD_METADATA_FIELDS: tuple[str, ...] = (*REQUIRED_METADATA_FIELDS, SOURCE_FILE, LINE_NUMBER)
#: Exact members of a parse-error record, in output order.
PARSE_ERROR_RECORD_FIELDS: tuple[str, ...] = (
    *STORED_RECORD_METADATA_FIELDS,
    PARSE_ERROR,
    PARSE_ERROR_DETAIL,
    RAW_LINE,
    RAW_ENCODING,
)
#: Every key Bronze may add; any of these (or any `_bronze_*` key) in the
#: input is a collision.
RESERVED_FIELDS = frozenset(PARSE_ERROR_RECORD_FIELDS)

# -- parse-error codes -----------------------------------------------------

ERR_INVALID_UTF8 = "invalid_utf8"
ERR_INVALID_JSON = "invalid_json"
ERR_NOT_A_JSON_OBJECT = "not_a_json_object"
ERR_DUPLICATE_KEY = "duplicate_json_key"
ERR_RESERVED_FIELD = "reserved_field_collision"
PARSE_ERROR_CODES: tuple[str, ...] = (
    ERR_INVALID_UTF8,
    ERR_INVALID_JSON,
    ERR_NOT_A_JSON_OBJECT,
    ERR_DUPLICATE_KEY,
    ERR_RESERVED_FIELD,
)

#: JSON's insignificant whitespace (RFC 8259) - deliberately not
#: `str.strip()`'s wider Unicode whitespace set.
_JSON_WHITESPACE = " \t\n\r"


@dataclass(frozen=True)
class RecordMetadata:
    """Per-record metadata values (identical for every record of one input
    file except `line_number`)."""

    ingestion_time: str
    source: str
    run_id: str
    source_file: str

    def as_fields(self, line_number: int) -> dict[str, Any]:
        return {
            INGESTION_TIME: self.ingestion_time,
            SOURCE: self.source,
            RUN_ID: self.run_id,
            SOURCE_FILE: self.source_file,
            LINE_NUMBER: line_number,
        }


@dataclass(frozen=True)
class BronzeLine:
    """One Bronze output line (without trailing newline) and, for a
    parse-error record, its error code/detail."""

    text: str
    parse_error: str | None = None
    detail: str | None = None

    @property
    def is_parse_error(self) -> bool:
        return self.parse_error is not None


class _DuplicateKeyError(ValueError):
    pass


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(f"duplicate key {key!r}")
        result[key] = value
    return result


def _reject_constant(name: str) -> Any:
    # Python's json accepts NaN/Infinity/-Infinity, which are not JSON.
    raise ValueError(f"non-standard JSON constant {name!r}")


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _parse_error_line(raw: bytes, metadata: RecordMetadata, line_number: int, code: str, detail: str) -> BronzeLine:
    try:
        raw_text, encoding = raw.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        raw_text, encoding = base64.b64encode(raw).decode("ascii"), "base64"
    record = {
        **metadata.as_fields(line_number),
        PARSE_ERROR: code,
        PARSE_ERROR_DETAIL: detail,
        RAW_LINE: raw_text,
        RAW_ENCODING: encoding,
    }
    return BronzeLine(text=_dumps(record), parse_error=code, detail=detail)


def build_bronze_line(raw: bytes, metadata: RecordMetadata, line_number: int) -> BronzeLine:
    """Turn one non-blank input line (bytes, without its `\\n` terminator)
    into exactly one Bronze output line. Never raises for bad input: every
    failure becomes a parse-error record (the caller enforces the "fail"
    policy, if configured)."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        return _parse_error_line(raw, metadata, line_number, ERR_INVALID_UTF8, str(exc))

    try:
        parsed = json.loads(text, object_pairs_hook=_reject_duplicate_keys, parse_constant=_reject_constant)
    except _DuplicateKeyError as exc:
        return _parse_error_line(raw, metadata, line_number, ERR_DUPLICATE_KEY, str(exc))
    except (ValueError, RecursionError) as exc:  # JSONDecodeError is a ValueError
        return _parse_error_line(raw, metadata, line_number, ERR_INVALID_JSON, str(exc))

    if not isinstance(parsed, dict):
        return _parse_error_line(
            raw, metadata, line_number, ERR_NOT_A_JSON_OBJECT, f"top-level JSON value is {type(parsed).__name__}"
        )

    collisions = sorted(k for k in parsed if k in RESERVED_FIELDS or k.startswith(BRONZE_FIELD_PREFIX))
    if collisions:
        return _parse_error_line(
            raw, metadata, line_number, ERR_RESERVED_FIELD, f"input keys collide with Bronze metadata: {collisions}"
        )

    # Splice metadata into the original object text. `json.loads` succeeded
    # and returned a dict, so after trimming JSON whitespace the text starts
    # with "{" and ends with "}".
    body = text.strip(_JSON_WHITESPACE)
    fragment = _dumps(metadata.as_fields(line_number))[1:-1]  # drop braces
    separator = "," if parsed else ""
    return BronzeLine(text=f"{body[:-1]}{separator}{fragment}}}")


def strip_metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Return a stored Bronze record without Bronze metadata, i.e. the
    original input object (key order preserved)."""
    return {k: v for k, v in record.items() if k not in RESERVED_FIELDS}


def raw_line_bytes(record: dict[str, Any]) -> bytes:
    """Recover the exact original input bytes of a parse-error record."""
    if record[RAW_ENCODING] == "base64":
        try:
            return base64.b64decode(record[RAW_LINE], validate=True)
        except binascii.Error as exc:  # pragma: no cover - only for corrupted Bronze data
            raise ValueError("corrupted base64 raw line") from exc
    return record[RAW_LINE].encode("utf-8")
