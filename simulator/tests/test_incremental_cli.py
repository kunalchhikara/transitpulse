"""DE-11 CLI (`python -m simulator.incremental`): two OS processes with a
fixed clock produce byte-identical state + Bronze; exit codes; every log
line carries the run id; a log file is appended to, never confusingly."""

from __future__ import annotations

import json

import pytest

from simulator.incremental import format_ts, parse_aware
from simulator.tests._incremental_helpers import (
    all_watermarks,
    make_env,
    rewrite_source_file,
    run_cli,
    tree_bytes,
)

FIXED_CLOCK = "2030-01-01T00:00:00Z"
COMMON = (
    "--state-dir", "state", "--batch-source-dir", "batch", "--reference-source-dir", "ref",
    "--bronze-output-dir", "bronze", "--fixed-clock", FIXED_CLOCK,
)


def _cli_env(root):
    env = make_env(root, relative=True)
    values = all_watermarks(root / "batch")
    return env, values


def test_two_cli_processes_are_byte_identical_with_a_fixed_clock(tmp_path):
    outputs = []
    for name in ("one", "two"):
        root = tmp_path / name
        _, values = _cli_env(root)
        stdout = []
        for cut in (values[len(values) // 2], values[-1]):
            proc = run_cli("--as-of", format_ts(cut), *COMMON, "--log-file", "run.log", cwd=root)
            assert proc.returncode == 0, proc.stderr.decode()
            stdout.append(proc.stdout)
        outputs.append((tree_bytes(root / "state"), tree_bytes(root / "bronze"), stdout))
    (state_a, bronze_a, out_a), (state_b, bronze_b, out_b) = outputs
    assert state_a and bronze_a
    assert state_a == state_b
    assert bronze_a == bronze_b
    assert out_a == out_b
    assert any(k.startswith("control/") for k in state_a) and any(k.startswith("audit/") for k in state_a)


def test_exit_codes_and_summary(tmp_path):
    root = tmp_path / "cli"
    _, values = _cli_env(root)
    ok = run_cli("--as-of", format_ts(values[-1]), *COMMON, cwd=root)
    assert ok.returncode == 0
    summary = json.loads(ok.stdout)
    assert summary["status"] == "SUCCEEDED" and summary["summary"].startswith("audit/")
    assert set(summary["datasets"]) >= {"depots", "schedules"}

    # earlier than the watermark -> refused, nothing written
    before = tree_bytes(root / "state"), tree_bytes(root / "bronze")
    refused = run_cli("--as-of", format_ts(values[0]), *COMMON, cwd=root)
    assert refused.returncode == 2 and json.loads(refused.stdout)["status"] == "REFUSED"
    assert (tree_bytes(root / "state"), tree_bytes(root / "bronze")) == before

    # naive --as-of rejected by argument parsing
    naive = run_cli("--as-of", "2025-01-06T12:00:00", *COMMON, cwd=root)
    assert naive.returncode == 2 and b"UTC offset" in naive.stderr
    missing = run_cli(*COMMON, cwd=root)
    assert missing.returncode == 2

    # a failing dataset -> PARTIAL, exit 1
    data = (root / "batch" / "historical_trips.jsonl").read_bytes() + b"{not json\n"
    rewrite_source_file(root / "batch", "historical_trips", data)
    later = run_cli("--as-of", format_ts(values[-1].replace(year=values[-1].year + 1)), *COMMON, cwd=root)
    assert later.returncode == 1
    result = json.loads(later.stdout)
    assert result["status"] == "PARTIAL"
    assert result["datasets"]["historical_trips"]["error_type"] == "BadSourceError"


def test_skipped_late_rows_under_record_policy_exit_3(tmp_path):
    """O1: `record` must not hide loss - a distinct exit code."""
    root = tmp_path / "late"
    _, values = _cli_env(root)
    first = run_cli("--as-of", format_ts(values[len(values) // 2]), *COMMON, cwd=root)
    assert first.returncode == 0
    control = json.loads((root / "state" / "control" / "historical_trips.json").read_text())
    rows = [json.loads(line) for line in (root / "batch" / "historical_trips.jsonl").read_text().splitlines()]
    late = dict(next(r for r in rows if parse_aware(r["updated_at"]) < parse_aware(control["watermark"])))
    late["trip_id"] += "-LATE"
    data = (root / "batch" / "historical_trips.jsonl").read_bytes() + json.dumps(late).encode() + b"\n"
    rewrite_source_file(root / "batch", "historical_trips", data)
    args = ("--as-of", format_ts(values[-1]), *COMMON)
    proc = run_cli(*args, "--late-arrival-policy", "record", cwd=root)
    assert proc.returncode == 3, proc.stderr.decode()
    summary = json.loads(proc.stdout)
    assert summary["status"] == "SUCCEEDED_WITH_WARNINGS" and summary["late_rows_skipped"] == 1
    assert summary["datasets"]["historical_trips"]["late_rows_skipped"] == 1
    assert summary["warnings"] == [{"count": 1, "dataset": "historical_trips", "type": "late_rows_skipped"}]


def test_old_control_schema_is_refused_with_exit_2(tmp_path):
    root = tmp_path / "old"
    _, values = _cli_env(root)
    assert run_cli("--as-of", format_ts(values[3]), *COMMON, cwd=root).returncode == 0
    path = root / "state" / "control" / "schedules.json"
    old = json.loads(path.read_text())
    old["control_schema_version"] = "1.0.0"
    path.write_text(json.dumps(old))
    proc = run_cli("--as-of", format_ts(values[-1]), *COMMON, cwd=root)
    assert proc.returncode == 2 and json.loads(proc.stdout)["status"] == "REFUSED"
    assert "1.0.0" in json.loads(proc.stdout)["error"]


def test_invalid_config_is_refused(tmp_path):
    root = tmp_path / "cfg"
    _cli_env(root)
    (root / "bad.toml").write_text('[incremental]\nstate_dir = "s"\nbad_source_policy = "lenient"\n')
    proc = run_cli("--as-of", "2025-01-06T00:00:00Z", "--config", "bad.toml", cwd=root)
    assert proc.returncode == 2 and json.loads(proc.stdout)["status"] == "REFUSED"


@pytest.mark.parametrize("policy", ["strict", "preserve"])
def test_bad_source_policy_flag(tmp_path, policy):
    root = tmp_path / policy
    _, values = _cli_env(root)
    data = (root / "batch" / "service_disruptions.jsonl").read_bytes() + b"[1,2]\n"
    rewrite_source_file(root / "batch", "service_disruptions", data)
    proc = run_cli("--as-of", format_ts(values[-1]), *COMMON, "--bad-source-policy", policy, cwd=root)
    assert proc.returncode == (1 if policy == "strict" else 0)


def test_log_file_lines_all_carry_the_pipeline_run_id_across_reruns(tmp_path):
    root = tmp_path / "logs"
    _, values = _cli_env(root)
    run_ids = []
    for cut in (values[3], values[-1], values[-1]):
        proc = run_cli("--as-of", format_ts(cut), *COMMON, "--log-file", "run.log", cwd=root)
        assert proc.returncode == 0
        run_ids.append(json.loads(proc.stdout)["pipeline_run_id"])
    lines = [json.loads(line) for line in (root / "run.log").read_text().splitlines()]
    assert len(set(run_ids)) == 3
    assert {line["context"]["pipeline_run_id"] for line in lines} == set(run_ids)
    assert {line["logger"] for line in lines} == {"simulator.incremental", "simulator.bronze"}
