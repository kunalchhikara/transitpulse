"""DE-06 CLI (`python -m simulator.silver`) as a separate process, Silver
config validation, and the standardised/enriched shape of a clean record."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest
from pydantic import ValidationError

from simulator.bronze import load_bronze_config
from simulator.config import load_scale_config
from simulator.reference.generator import generate_reference_dataset
from simulator.schemas.telemetry import TelemetryEvent
from simulator.silver import SilverConfig, load_silver_config
from simulator.tests._bronze_helpers import FIXED_TIME_STR, REPO_ROOT
from simulator.tests._silver_helpers import (
    PROCESSING_TIME_STR,
    bronze_from_lines,
    clean_payloads,
    lines_to_silver,
    table,
    valid_payload,
)


def _cli(*args):
    return subprocess.run([sys.executable, "-m", "simulator.silver", *args], cwd=REPO_ROOT, capture_output=True, check=False)


def _summary(done):
    assert done.stdout, done.stderr.decode()
    return json.loads(done.stdout.decode().strip().splitlines()[-1])


def test_cli_commits_and_reruns_as_noop(tmp_path):
    bronze = bronze_from_lines(tmp_path / "bronze", [json.dumps(p) for p in clean_payloads(10)] + ["junk"])
    args = ("--run-id", "s1", "--bronze-dir", str(bronze), "--output-dir", str(tmp_path / "silver"),
            "--fixed-processing-time", "2025-02-01T10:00:00Z", "--log-file", str(tmp_path / "silver.log"))
    first = _cli(*args)
    assert first.returncode == 0, first.stderr.decode()
    summary = _summary(first)
    assert summary["status"] == "COMMITTED"
    assert (summary["extracted"], summary["valid_loaded"], summary["quarantined"]) == (11, 10, 1)
    assert summary["bronze_runs_consumed"] == ["b1"]
    assert summary["reconciliation_holds"] is True
    logs = [json.loads(line) for line in (tmp_path / "silver.log").read_text().splitlines()]
    assert {r["logger"] for r in logs} == {"simulator.silver"}
    assert any(r["message"] == "silver run committed" for r in logs)
    again = _cli(*args)
    assert again.returncode == 0 and _summary(again)["status"] == "ALREADY_COMMITTED"


def test_cli_failures_exit_nonzero_with_a_json_summary(tmp_path):
    bronze = bronze_from_lines(tmp_path / "bronze", [json.dumps(clean_payloads(1)[0])])
    missing = _cli("--run-id", "s1", "--bronze-dir", str(tmp_path / "nope"), "--output-dir", str(tmp_path / "o"))
    assert missing.returncode == 1 and _summary(missing)["error_type"] == "SilverInputError"
    unknown = _cli("--run-id", "s1", "--bronze-dir", str(bronze), "--output-dir", str(tmp_path / "o"),
                   "--bronze-run-id", "does-not-exist")
    assert unknown.returncode == 1 and "does-not-exist" in _summary(unknown)["error"]
    unsafe = _cli("--run-id", "../evil", "--bronze-dir", str(bronze), "--output-dir", str(tmp_path / "o"))
    assert unsafe.returncode == 1 and _summary(unsafe)["error_type"] == "InvalidRunIdError"
    naive = _cli("--run-id", "s1", "--fixed-processing-time", "2025-01-01T00:00:00")
    assert naive.returncode == 2


def test_cli_alternate_validation_config(tmp_path):
    bronze = bronze_from_lines(tmp_path / "bronze", [json.dumps(valid_payload(speed_kmh=55.0))])
    strict = tmp_path / "validation.toml"
    default = (REPO_ROOT / "simulator" / "config" / "default_validation.toml").read_text()
    strict.write_text(default.replace("max_speed_kmh = 120.0", "max_speed_kmh = 50.0"))
    done = _cli("--run-id", "s1", "--bronze-dir", str(bronze), "--output-dir", str(tmp_path / "silver"),
                "--validation-config", str(strict))
    assert done.returncode == 0, done.stderr.decode()
    assert _summary(done)["quarantined_by_rule"]["max_speed"] == 1


def test_default_config_points_at_gitignored_output_and_the_bronze_stream_table():
    config = load_silver_config()
    assert config.output_dir.as_posix() == "simulator/_out/silver"
    assert config.bronze_dir == load_bronze_config().output_dir
    assert config.bronze_table == load_bronze_config().stream.table
    done = subprocess.run(["git", "check-ignore", "-q", "simulator/_out/silver/x"], cwd=REPO_ROOT, check=False)
    assert done.returncode == 0


@pytest.mark.parametrize(
    "update",
    [
        {"future_time_reference": "wall_clock"},
        {"allowed_lateness_seconds": -1},
        {"clean_table": "_runs"},
        {"quarantine_table": "telemetry_clean"},
        {"surprise": 1},
    ],
)
def test_invalid_silver_config_is_rejected(update):
    data = load_silver_config().model_dump(mode="json")
    data.update(update)
    with pytest.raises(ValidationError):
        SilverConfig.model_validate(data)


def test_clean_record_is_standardised_enriched_and_keeps_lineage(tmp_path):
    payload = valid_payload(event_time="2025-01-01T11:00:00.5+11:00", speed_kmh=12)
    result = lines_to_silver(tmp_path, [json.dumps(payload)])
    (record,) = table(result, "clean")
    contract = list(TelemetryEvent.model_fields)
    assert list(record)[: len(contract)] == contract
    # Standardisation: same instant, UTC with microseconds; int speed typed as float.
    assert record["event_time"] == "2025-01-01T00:00:00.500000Z"
    assert record["generated_at"] == "2025-01-01T00:00:00.000000Z"
    assert record["speed_kmh"] == 12.0 and isinstance(record["speed_kmh"], float)
    for field in ("event_id", "vehicle_id", "route_id", "trip_id", "latitude", "longitude", "heading",
                  "occupancy", "delay_seconds", "status", "schema_version"):
        assert record[field] == payload[field], field
    # Enrichment from DE-01 reference data, in clearly named columns.
    reference = generate_reference_dataset(load_scale_config())
    route = next(r for r in reference.routes if r.route_id == payload["route_id"])
    vehicle = next(v for v in reference.vehicles if v.vehicle_id == payload["vehicle_id"])
    assert record["ref_route_name"] == route.name
    assert record["ref_route_number"] == route.route_number
    assert record["ref_vehicle_home_depot_id"] == vehicle.home_depot_id
    # Lineage + Silver metadata.
    assert record["ingestion_time"] == FIXED_TIME_STR
    assert record["processing_time"] == PROCESSING_TIME_STR
    assert (record["bronze_run_id"], record["silver_run_id"]) == ("b1", "s1")
    assert (record["_bronze_source_file"], record["_bronze_line_number"]) == ("b1.ndjson", 1)


def test_unknown_vehicle_is_valid_but_counted_as_an_enrichment_miss(tmp_path):
    result = lines_to_silver(tmp_path, [json.dumps(valid_payload(vehicle_id="VEH-9999"))])
    assert result.manifest["counters"]["enrichment_misses"]["vehicle_not_in_reference"] == 1
    (record,) = table(result, "clean")
    assert record["ref_vehicle_home_depot_id"] is None and record["ref_route_name"]
