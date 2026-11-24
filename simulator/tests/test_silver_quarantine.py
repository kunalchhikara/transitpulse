"""DE-06 quarantine sink: Bronze parse-error records are routed to quarantine
(never dropped) with their original error code; every quarantined record
keeps its raw payload losslessly (exact bytes) plus rejection rule(s)/reason,
ingestion_time, source, Bronze run_id, Silver run_id and processing_time."""

from __future__ import annotations

import base64
import json

import pytest

from simulator.bronze.records import raw_line_bytes
from simulator.tests._bronze_helpers import FIXED_TIME_STR
from simulator.tests._silver_helpers import (
    PROCESSING_TIME_STR,
    SILVER_CONFIG,
    bronze_from_bytes,
    clean_payloads,
    lines_to_silver,
    silver,
    table,
    valid_payload,
)

GOOD = [json.dumps(p) for p in clean_payloads(3)]

# (raw line bytes, Bronze parse-error code)
PARSE_ERROR_LINES = [
    (b'{"event_id": "EVT-TRUNCATED", "speed_kmh": 4', "invalid_json"),
    (b"\xff\xfe not utf-8 \x80", "invalid_utf8"),
    (b"[1, 2, 3]", "not_a_json_object"),
    (b'{"event_id": "A", "event_id": "B"}', "duplicate_json_key"),
    (b'{"event_id": "X", "run_id": "spoofed"}', "reserved_field_collision"),
    (b"NaN", "invalid_json"),
]


def test_every_bronze_parse_error_code_is_quarantined_with_its_code_and_exact_raw_bytes(tmp_path):
    data = b"\n".join([GOOD[0].encode(), *(line for line, _ in PARSE_ERROR_LINES), GOOD[1].encode()]) + b"\n"
    result = silver(bronze_from_bytes(tmp_path / "bronze", data))
    counters = result.manifest["counters"]
    assert counters["extracted"] == 2 + len(PARSE_ERROR_LINES)
    assert counters["valid_loaded"] == 2
    assert counters["parse_errors"] == counters["quarantined"] == len(PARSE_ERROR_LINES)
    assert counters["quarantined_by_rule"]["bronze_parse_error"] == len(PARSE_ERROR_LINES)
    assert counters["schema_failures"] == counters["rule_failures"] == 0
    assert counters["parse_errors_by_code"] == {
        "invalid_json": 2, "invalid_utf8": 1, "not_a_json_object": 1, "duplicate_json_key": 1,
        "reserved_field_collision": 1,
    }
    quarantine = table(result, "quarantine")
    assert len(quarantine) == len(PARSE_ERROR_LINES)
    for record, (raw, code) in zip(quarantine, PARSE_ERROR_LINES, strict=True):
        assert record["primary_rule"] == "bronze_parse_error"
        assert record["rejection_rules"] == ["bronze_parse_error"]
        assert record["rejection_category"] == "parse"
        assert record["bronze_parse_error"] == code
        assert code in record["rejections"][0]["reason"]
        assert record["raw_format"] == "bronze_parse_error"
        assert record["raw_payload"] is None
        # Lossless: the exact original bytes are recoverable.
        recovered = raw_line_bytes({"_bronze_raw_line": record["raw_payload_text"],
                                    "_bronze_raw_encoding": record["raw_encoding"]})
        assert recovered == raw
    assert quarantine[1]["raw_encoding"] == "base64"
    assert base64.b64decode(quarantine[1]["raw_payload_text"]) == PARSE_ERROR_LINES[1][0]


def test_quarantine_record_carries_all_section_12_metadata(tmp_path):
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload(vehicle_id=None))])
    (record,) = table(result, "quarantine")
    assert record["ingestion_time"] == FIXED_TIME_STR
    assert record["source"] == "simulator.telemetry_stream"
    assert record["bronze_run_id"] == "b1"
    assert record["silver_run_id"] == "s1"
    assert record["processing_time"] == PROCESSING_TIME_STR
    assert record["_bronze_source_file"] == "b1.ndjson"
    assert record["_bronze_line_number"] == 1
    assert record["primary_rule"] == "required_fields"
    assert "vehicle_id" in record["rejections"][0]["reason"]
    assert record["event_id"] == valid_payload()["event_id"]


def test_object_payload_is_kept_byte_for_byte(tmp_path):
    # Unusual-but-valid JSON spellings that a re-serialisation would change.
    lines = [
        '{"event_id": "E-1", "speed_kmh": -1.50, "latitude": 1e2, "note": "caf\\u00e9 é", "vehicle_id": null}',
        '  {"event_id":"E-2","nested":{"b":1,"a":[1.0,2.10]},"big":123456789012345678901234567890}\t',
        "{}",
    ]
    result = lines_to_silver(tmp_path, lines)
    quarantine = table(result, "quarantine")
    assert len(quarantine) == 3
    for record, line in zip(quarantine, lines, strict=True):
        assert record["raw_format"] == "json_object"
        assert record["raw_encoding"] == "utf-8"
        # Bronze trims JSON whitespace at the line ends; everything else is exact.
        assert record["raw_payload_text"].encode("utf-8") == line.strip(" \t").encode("utf-8")
        assert record["raw_payload"] == json.loads(line)
        assert list(record["raw_payload"]) == list(json.loads(line))
    assert quarantine[0]["raw_payload"]["vehicle_id"] is None
    assert quarantine[1]["raw_payload"]["big"] == 123456789012345678901234567890


@pytest.mark.parametrize(
    ("overrides", "rule"),
    [
        ({"vehicle_id": None}, "required_fields"),
        ({"latitude": 999.0, "longitude": -999.0}, "valid_coordinates"),
        ({"speed_kmh": -42.0}, "non_negative_speed"),
        ({"event_time": "not-a-timestamp"}, "parsable_timestamps"),
        ({"route_id": "ROUTE-UNKNOWN-9999"}, "known_route"),
        ({"schema_version": "2.0.0"}, "supported_schema_version"),
        ({"speed_kmh": 300.0}, "max_speed"),
        ({"occupancy": 999}, "max_occupancy"),
        ({"occupancy": -3}, "non_negative_occupancy"),
        ({"event_time": "2025-01-01T01:00:00Z"}, "future_time_tolerance"),
        ({"status": "FLYING"}, "schema_conformance"),
        ({"heading": 400.0}, "heading_range"),
    ],
)
def test_invalid_record_goes_to_quarantine_not_clean_and_is_counted_by_rule(tmp_path, overrides, rule):
    result = lines_to_silver(tmp_path, [GOOD[0], json.dumps(valid_payload(event_id="BAD-1", **overrides))])
    counters = result.manifest["counters"]
    assert counters["valid_loaded"] == 1 and counters["quarantined"] == 1
    assert counters["quarantined_by_rule"][rule] == 1
    assert sum(counters["quarantined_by_rule"].values()) == 1
    assert [r["event_id"] for r in table(result, "clean")] != ["BAD-1"]
    (record,) = table(result, "quarantine")
    assert record["primary_rule"] == rule


def test_schema_vs_rule_failure_counters(tmp_path):
    lines = [
        json.dumps(valid_payload(event_id="S1", vehicle_id=None)),  # schema
        json.dumps(valid_payload(event_id="S2", event_time="x")),  # schema
        json.dumps(valid_payload(event_id="S3", status="NOPE")),  # schema
        json.dumps(valid_payload(event_id="R1", speed_kmh=-1.0)),  # rule
        json.dumps(valid_payload(event_id="R2", route_id="ROUTE-X")),  # rule
        "garbage",  # parse
    ]
    counters = lines_to_silver(tmp_path, lines).manifest["counters"]
    assert (counters["schema_failures"], counters["rule_failures"], counters["parse_errors"]) == (3, 2, 1)
    assert counters["quality_percentage"] == 0.0


def test_quarantine_table_name_comes_from_config(tmp_path):
    result = lines_to_silver(tmp_path, ["garbage"])
    assert result.manifest["tables"]["quarantine"]["table"] == SILVER_CONFIG.quarantine_table
    assert result.manifest["tables"]["quarantine"]["path"].startswith(f"{SILVER_CONFIG.quarantine_table}/run_id=s1/")
