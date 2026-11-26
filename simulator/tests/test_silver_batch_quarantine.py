"""DE-12 criterion 2: Bronze parse errors -> quarantine with raw bytes
recoverable; strict model failures -> quarantine under named rule ids
(one test per rule); rows DE-11 lists with
`bronze_outcome = stored_record_requires_quarantine` are quarantined
(tested with a real DE-11 `preserve` run, and with a listing that names a
row the model would accept)."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import pytest

from simulator.silver_batch import RULE_IDS, BronzeContractError
from simulator.tests._incremental_helpers import rewrite_source_file
from simulator.tests._silver_batch_helpers import (
    DELETE,
    at,
    ingest_rows,
    line,
    make_sb_env,
    manifest,
    run_sb,
    sample_row,
    sb_config,
    t,
    table,
    table_files,
)

TRIPS = "historical_trips"


def _land_and_run(tmp_path: Path, dataset: str, rows: list) -> tuple[Path, dict]:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    ingest_rows(bronze, "b1", {dataset: rows}, at=t(1), work_dir=tmp_path)
    result = run_sb(sb_config(out, bronze), "s1")
    return out, result.manifest


# (case id, dataset, row overrides or raw line, expected primary rule)
RULE_CASES = [
    ("unsupported_schema_version", TRIPS, {"schema_version": "9.9.9"}, "supported_schema_version"),
    ("non_string_schema_version", TRIPS, {"schema_version": 1}, "supported_schema_version"),
    ("missing_schema_version", TRIPS, {"schema_version": DELETE}, "missing_field"),
    ("missing_required_field", TRIPS, {"vehicle_id": DELETE}, "missing_field"),
    ("missing_defaulted_field", "historical_telemetry", {"snapshot_type": DELETE}, "missing_field"),
    ("missing_updated_at", TRIPS, {"updated_at": DELETE}, "missing_field"),
    ("unknown_field", TRIPS, {"driver_name": "x"}, "unknown_field"),
    ("string_for_int", TRIPS, {"passenger_boardings": "4"}, "wrong_type"),
    ("bool_for_int", TRIPS, {"passenger_boardings": True}, "wrong_type"),
    ("epoch_number_updated_at", TRIPS, {"updated_at": 1736108728}, "wrong_type"),
    ("null_updated_at", TRIPS, {"updated_at": None}, "wrong_type"),
    ("invalid_updated_at_text", TRIPS, {"updated_at": "not-a-timestamp"}, "wrong_type"),
    ("datetime_as_event_date", TRIPS, {"event_date": "2025-01-06T00:00:00Z"}, "wrong_type"),
    ("bad_enum", TRIPS, {"trip_status": "TELEPORTED"}, "invalid_enum"),
    ("naive_updated_at", TRIPS, {"updated_at": "2025-01-05T20:25:28"}, "timestamp_without_offset"),
    ("naive_telemetry_event_time", "historical_telemetry", {"event_time": "2025-01-05T20:00:44"},
     "timestamp_without_offset"),
    ("negative_count", TRIPS, {"passenger_boardings": -1}, "value_out_of_range"),
    ("route_with_one_stop", "routes", None, "value_out_of_range"),
    ("cross_field_constraint", TRIPS, {"stops_served": 99}, "model_constraint"),
    ("blank_primary_key", TRIPS, {"trip_id": "  "}, "blank_primary_key"),
]


def _reference_route(**overrides):
    return {"home_depot_id": "DEPOT-01", "name": "Route 100", "route_id": "ROUTE-01", "route_number": "100",
            "schema_version": "1.0.0", "stop_ids": ["STOP-0001", "STOP-0002"], **overrides}


@pytest.mark.parametrize(("case", "dataset", "overrides", "rule"), RULE_CASES, ids=[c[0] for c in RULE_CASES])
def test_each_strict_model_failure_is_quarantined_under_its_named_rule(tmp_path, case, dataset, overrides, rule):
    row = _reference_route(stop_ids=["STOP-0001"]) if dataset == "routes" else sample_row(dataset, **overrides)
    out, m = _land_and_run(tmp_path, dataset, [row])
    c = m["datasets"][dataset]["counters"]
    assert (c["quarantined"], c["inserted"], c["schema_failures"]) == (1, 0, 1)
    assert c["quarantined_by_rule"] == {rule: 1}
    (q,) = table_files(out, "quarantine", dataset)
    assert q["primary_rule"] == rule and q["rejection_rules"][0] == rule
    assert q["raw_format"] == "json_object" and q["raw_payload"] == row
    assert json.loads(q["raw_payload_text"]) == row
    # PROJECT.md section 12 quarantine metadata: reason/rule, ingestion time, source, run id
    assert q["ingestion_time"] == m["bronze_runs_applied"][0]["ingestion_time"]
    assert q["source"] == f"simulator.batch/{dataset}" and q["bronze_run_id"] == "b1"
    assert q["silver_batch_run_id"] == "s1" and q["rejections"][0]["reason"]
    assert table(out, dataset) == []


def test_every_catalogue_rule_id_is_exercised_somewhere() -> None:
    covered = {c[3] for c in RULE_CASES} | {
        "bronze_parse_error", "non_finite_number", "schema_conformance", "preserved_bad_source_row",
        "merge_conflict", "duplicate_key_in_snapshot",
    }
    assert set(RULE_IDS) == covered


def test_non_finite_number_keeps_the_exact_text(tmp_path: Path) -> None:
    row = sample_row("weather_observations")
    text = line(row).decode().replace(f'"temperature_c":{row["temperature_c"]}', '"temperature_c":1e400')
    assert "1e400" in text
    out, _ = _land_and_run(tmp_path, "weather_observations", [text.encode()])
    (q,) = table_files(out, "quarantine", "weather_observations")
    assert q["primary_rule"] == "non_finite_number"
    assert q["raw_payload"] is None and q["raw_payload_omitted_reasons"] == ["non_finite_number"]
    assert q["raw_payload_text"] == text


def test_lone_surrogate_is_quarantined_by_the_fallback_rule_without_aborting(tmp_path: Path) -> None:
    """Valid JSON syntax that strict UTF-8 output cannot hold: pydantic's
    parser rejects it as invalid JSON (unmapped type -> `schema_conformance`);
    the parsed payload is omitted and the exact text kept (as DE-06)."""
    raw = line(sample_row(TRIPS)).replace(b'"vehicle_id":"VEH-0001"', b'"vehicle_id":"VEH\\ud800"')
    assert b"\\ud800" in raw
    out, m = _land_and_run(tmp_path, TRIPS, [raw])
    (q,) = table_files(out, "quarantine", TRIPS)
    assert q["primary_rule"] == "schema_conformance"
    assert q["raw_payload"] is None and q["raw_payload_omitted_reasons"] == ["unpaired_surrogate"]
    assert q["raw_payload_text"].encode() == raw
    assert m["reconciliation"]["holds"] is True


def test_all_failing_fields_are_recorded_in_precedence_order(tmp_path: Path) -> None:
    row = sample_row(TRIPS, trip_status="TELEPORTED", passenger_boardings="4", extra_col=1, vehicle_id=DELETE)
    out, _ = _land_and_run(tmp_path, TRIPS, [row])
    (q,) = table_files(out, "quarantine", TRIPS)
    assert q["rejection_rules"] == ["missing_field", "unknown_field", "wrong_type", "invalid_enum"]
    assert q["primary_rule"] == "missing_field"


@pytest.mark.parametrize(
    ("raw", "code", "encoding"),
    [
        (b'{"trip_id": "TRIP-1", "updated_at": ', "invalid_json", "utf-8"),
        (b"\xff\xfe not utf-8 \x00", "invalid_utf8", "base64"),
        (b"[1, 2, 3]", "not_a_json_object", "utf-8"),
        (b'{"a": 1, "a": 2}', "duplicate_json_key", "utf-8"),
    ],
)
def test_bronze_parse_errors_are_quarantined_with_raw_bytes_recoverable(tmp_path, raw, code, encoding) -> None:
    out, m = _land_and_run(tmp_path, TRIPS, [raw, sample_row(TRIPS)])
    c = m["datasets"][TRIPS]["counters"]
    assert (c["quarantined"], c["parse_errors"], c["inserted"]) == (1, 1, 1)
    assert c["parse_errors_by_code"] == {code: 1}
    (q,) = table_files(out, "quarantine", TRIPS)
    assert q["primary_rule"] == "bronze_parse_error" and q["rejection_category"] == "parse"
    assert q["raw_format"] == "bronze_parse_error" and q["bronze_parse_error"] == code
    assert q["raw_encoding"] == encoding
    recovered = base64.b64decode(q["raw_payload_text"]) if encoding == "base64" else q["raw_payload_text"].encode()
    assert recovered == raw
    assert q["_bronze_line_number"] == 1


def test_stored_record_raw_text_is_byte_exact(tmp_path: Path) -> None:
    raw = line(sample_row(TRIPS)).replace(b'"passenger_boardings":4', b'"passenger_boardings":-4.50')
    assert raw != line(sample_row(TRIPS))
    out, _ = _land_and_run(tmp_path, TRIPS, [raw])
    (q,) = table_files(out, "quarantine", TRIPS)
    assert q["raw_payload_text"].encode() == raw


# -- DE-11 preserved_bad_rows (Lead decision D2) ----------------------------------


def _bad_trip_lines(env) -> list[bytes]:
    rows = [json.loads(x) for x in (env.inc.batch_dir / f"{TRIPS}.jsonl").read_text().splitlines()]
    base = dict(rows[0])
    variants = []
    for overrides in (
        {"updated_at": DELETE},  # missing_watermark
        {"updated_at": None},  # missing_watermark (null)
        {"updated_at": "2025-01-05T20:25:28"},  # invalid_watermark (naive)
        {"updated_at": "yesterday"},  # invalid_watermark
        {"updated_at": 1736108728},  # invalid_watermark (non-string)
    ):
        row = dict(base, trip_id=f"{base['trip_id']}-BAD{len(variants)}")
        for key, value in overrides.items():
            if value is DELETE:
                row.pop(key)
            else:
                row[key] = value
        variants.append(line(row))
    variants.append(b'{"trip_id": "TRUNCATED", ')  # bronze parse-error record
    return variants


def test_real_de11_preserve_run_listed_rows_are_all_quarantined(tmp_path: Path) -> None:
    env = make_sb_env(tmp_path, bad_source_policy="preserve")
    source = env.inc.batch_dir / f"{TRIPS}.jsonl"
    bad = _bad_trip_lines(env)
    good = source.read_bytes()
    rewrite_source_file(env.inc.batch_dir, TRIPS, good + b"\n".join(bad) + b"\n")
    # a reference element without schema_version (DE-11 full mode, preserve)
    vehicles = json.loads((env.inc.ref_dir / "vehicles.json").read_text())
    vehicles.append({k: v for k, v in dict(vehicles[0], vehicle_id="VEH-NOVERSION").items() if k != "schema_version"})
    (env.inc.ref_dir / "vehicles.json").write_text(json.dumps(vehicles, sort_keys=True, indent=2) + "\n")

    env.load(at(2030, 1, 1))
    result = env.silver("s1")
    m = result.manifest

    for dataset, expected_stored, expected_parse in ((TRIPS, 5, 1), ("vehicles", 1, 0)):
        listings = [s["de11_extract"] for r in m["bronze_runs_applied"] for s in r["tables"] if s["dataset"] == dataset]
        assert all(entry["status"] == "FOUND" for entry in listings)
        assert sum(entry["stored_records_requiring_quarantine"] for entry in listings) == expected_stored
        c = m["datasets"][dataset]["counters"]
        assert c["de11_listed_rows"] == expected_stored + expected_parse
        assert c["de11_listed_stored_rows_quarantined"] == expected_stored
        assert c["de11_listed_parse_error_rows_quarantined"] == expected_parse
        quarantined = table_files(env.output_dir, "quarantine", dataset)
        flagged = [q for q in quarantined if "preserved_bad_source_row" in q["rejection_rules"]]
        assert len(flagged) == expected_stored + expected_parse
        assert all(q["de11_preserved_bad_row"]["sha256"] for q in flagged)
        loaded_ids = {r[f"{'trip' if dataset == TRIPS else 'vehicle'}_id"] for r in env.table(dataset)}
        assert not any("BAD" in i or i == "VEH-NOVERSION" for i in loaded_ids)

    # every listed stored row is identified by its exact bytes
    trips_flagged = [q for q in table_files(env.output_dir, "quarantine", TRIPS) if q["raw_format"] == "json_object"]
    assert sorted(hashlib.sha256(q["raw_payload_text"].encode()).hexdigest() for q in trips_flagged) == sorted(
        hashlib.sha256(b).hexdigest() for b in bad[:5]
    )
    assert m["reconciliation"]["holds"] is True


def _fake_de11_extract(state_dir: Path, run_manifest: dict, dataset: str, listed: list[dict]) -> None:
    run_id = run_manifest["run_id"]
    (entry,) = [e for e in run_manifest["input_files"] if e["table"] == dataset]
    directory = state_dir / "extracts" / dataset / run_id
    directory.mkdir(parents=True)
    (directory / "manifest.json").write_text(json.dumps({
        "bronze_run_id": run_id, "mode": "incremental", "window": {"low": None, "high": "2030-01-01T00:00:00.000000Z"},
        "datasets": {dataset: {"file": f"{dataset}.jsonl", "sha256": entry["sha256"], "preserved_bad_rows": listed}},
    }))


def test_listing_alone_forces_quarantine_of_a_row_the_model_accepts(tmp_path: Path) -> None:
    bronze, out, state = tmp_path / "bronze", tmp_path / "sb", tmp_path / "state"
    rows = [sample_row(TRIPS, 0), sample_row(TRIPS, 1)]
    run_id = "inc-historical_trips-initial-20300101T000000Z"
    bm = ingest_rows(bronze, run_id, {TRIPS: rows}, at=t(1), work_dir=tmp_path)
    _fake_de11_extract(state, bm, TRIPS, [{
        "extract_line": 2, "source_line": 2, "reason": "invalid_watermark",
        "sha256": hashlib.sha256(line(rows[1])).hexdigest(), "bronze_outcome": "stored_record_requires_quarantine",
    }])
    m = run_sb(sb_config(out, bronze, state), "s1").manifest
    c = m["datasets"][TRIPS]["counters"]
    assert (c["inserted"], c["quarantined"], c["source_flagged"]) == (1, 1, 1)
    (q,) = table_files(out, "quarantine", TRIPS)
    assert q["primary_rule"] == "preserved_bad_source_row" and q["raw_payload"] == rows[1]
    assert [r["trip_id"] for r in table(out, TRIPS)] == [rows[0]["trip_id"]]


@pytest.mark.parametrize("problem", ["missing", "sha_mismatch", "line_not_in_bronze", "outcome_mismatch"])
def test_inconsistent_de11_lineage_fails_the_run_with_nothing_committed(tmp_path: Path, problem: str) -> None:
    bronze, out, state = tmp_path / "bronze", tmp_path / "sb", tmp_path / "state"
    rows = [sample_row(TRIPS, 0)]
    run_id = "inc-historical_trips-initial-20300101T000000Z"
    bm = ingest_rows(bronze, run_id, {TRIPS: rows}, at=t(1), work_dir=tmp_path)
    listed = [{"extract_line": 1, "source_line": 1, "reason": "invalid_watermark", "sha256": "x",
               "bronze_outcome": "stored_record_requires_quarantine"}]
    if problem == "line_not_in_bronze":
        listed[0]["extract_line"] = 5
    if problem == "outcome_mismatch":
        listed[0]["bronze_outcome"] = "bronze_parse_error_record"
    if problem != "missing":
        _fake_de11_extract(state, bm, TRIPS, listed)
    if problem == "sha_mismatch":
        path = state / "extracts" / TRIPS / run_id / "manifest.json"
        data = json.loads(path.read_text())
        data["datasets"][TRIPS]["sha256"] = "0" * 64
        path.write_text(json.dumps(data))
    with pytest.raises(BronzeContractError):
        run_sb(sb_config(out, bronze, state), "s1")
    m = manifest(out, "s1")
    assert m["status"] == "FAILED" and m["error"]["type"] == "BronzeContractError"
    assert m["checkpoint"] is None
    assert table(out, TRIPS) == []
    assert not [p for p in out.iterdir() if p.name not in ("_runs",)]


def test_listing_lookup_can_be_disabled_explicitly(tmp_path: Path) -> None:
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    ingest_rows(bronze, "inc-historical_trips-initial-20300101T000000Z", {TRIPS: [sample_row(TRIPS)]}, at=t(1),
                work_dir=tmp_path)
    m = run_sb(sb_config(out, bronze, None), "s1").manifest
    (step,) = m["bronze_runs_applied"][0]["tables"]
    assert step["de11_extract"]["status"] == "LOOKUP_DISABLED"
    assert m["config"]["incremental_listing_enabled"] is False
