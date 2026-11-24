"""DE-05 line-level Bronze record construction: original fields are stored
byte-for-byte; anything that cannot be stored as-is becomes a marked
parse-error record that keeps the raw line losslessly."""

from __future__ import annotations

import json

import pytest

from simulator.bronze.records import (
    ERR_DUPLICATE_KEY,
    ERR_INVALID_JSON,
    ERR_INVALID_UTF8,
    ERR_NOT_A_JSON_OBJECT,
    ERR_RESERVED_FIELD,
    PARSE_ERROR,
    PARSE_ERROR_RECORD_FIELDS,
    RAW_ENCODING,
    STORED_RECORD_METADATA_FIELDS,
    RecordMetadata,
    build_bronze_line,
    raw_line_bytes,
    strip_metadata,
)

META = RecordMetadata(ingestion_time="2025-02-01T09:30:00.000000Z", source="src", run_id="r1", source_file="f.ndjson")


def _store(raw: bytes) -> tuple[dict, str]:
    line = build_bronze_line(raw, META, 7)
    assert not line.is_parse_error, line.detail
    return json.loads(line.text), line.text


def test_stored_record_is_original_object_plus_exact_metadata():
    raw = b'{"event_id":"E1","vehicle_id":null,"latitude":999.0,"event_time":"not-a-timestamp","n":1}'
    record, text = _store(raw)
    assert strip_metadata(record) == json.loads(raw)
    assert list(record) == ["event_id", "vehicle_id", "latitude", "event_time", "n", *STORED_RECORD_METADATA_FIELDS]
    assert record["ingestion_time"] == META.ingestion_time
    assert record["source"] == "src"
    assert record["run_id"] == "r1"
    assert record["_bronze_source_file"] == "f.ndjson"
    assert record["_bronze_line_number"] == 7
    assert text.startswith(raw.decode()[:-1])


@pytest.mark.parametrize(
    "raw",
    [
        b'{"speed": 1e5, "x": 1.10, "big": 123456789012345678901234567890, "neg0": -0.0}',
        b'{"esc": "caf\\u00e9 \\"q\\" \\n", "uni": "caf\xc3\xa9", "nested": {"a": [1, {"b": null}]}}',
        b'  {"a": 1}\t \r',
        b"{}",
        b"{ }",
    ],
)
def test_original_bytes_are_preserved_verbatim(raw):
    """Number formatting, escapes, nesting and big ints survive byte-for-byte
    (no float/round-trip normalisation)."""
    record, text = _store(raw)
    body = raw.decode("utf-8").strip(" \t\r\n")
    assert text.startswith(body[:-1])
    assert strip_metadata(record) == json.loads(raw)
    assert set(record) - set(json.loads(raw)) == set(STORED_RECORD_METADATA_FIELDS)


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        (b'{"event_id": "E1", "speed', ERR_INVALID_JSON),  # truncated
        (b"not json at all", ERR_INVALID_JSON),
        (b'{"a": NaN}', ERR_INVALID_JSON),  # non-standard constant
        (b'{"a": 1} trailing', ERR_INVALID_JSON),
        (b"[1, 2, 3]", ERR_NOT_A_JSON_OBJECT),
        (b'"a string"', ERR_NOT_A_JSON_OBJECT),
        (b"null", ERR_NOT_A_JSON_OBJECT),
        (b'{"a": 1, "a": 2}', ERR_DUPLICATE_KEY),
        (b'{"a": {"b": 1, "b": 2}}', ERR_DUPLICATE_KEY),
        (b'{"run_id": "upstream"}', ERR_RESERVED_FIELD),
        (b'{"source": "x"}', ERR_RESERVED_FIELD),
        (b'{"_bronze_anything": 1}', ERR_RESERVED_FIELD),
        (b'{"a": "\xff\xfe"}', ERR_INVALID_UTF8),
    ],
)
def test_unstorable_lines_become_marked_parse_error_records_with_lossless_raw(raw, code):
    line = build_bronze_line(raw, META, 3)
    assert line.is_parse_error
    record = json.loads(line.text)
    assert list(record) == list(PARSE_ERROR_RECORD_FIELDS)
    assert record[PARSE_ERROR] == code
    assert record["_bronze_parse_error_detail"]
    assert record["run_id"] == "r1"
    assert record["source"] == "src"
    assert record["_bronze_line_number"] == 3
    assert raw_line_bytes(record) == raw  # exact original bytes recoverable
    assert record[RAW_ENCODING] == ("base64" if code == ERR_INVALID_UTF8 else "utf-8")
