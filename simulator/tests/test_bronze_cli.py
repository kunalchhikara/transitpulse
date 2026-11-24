"""DE-05 CLI (`python -m simulator.bronze`) as a separate process: stream and
batch subcommands, a real DE-03 CLI stream piped through stdin, and loud
non-zero exits for refused/failed runs. Also covers config validation."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest
from pydantic import ValidationError

from simulator.bronze import BronzeConfig, load_bronze_config
from simulator.tests._bronze_helpers import (
    FAST_GENERATOR_CONFIG,
    REPO_ROOT,
    SAMPLE_BATCH_DIR,
    clean_stream_lines,
    read_jsonl,
    run_cli,
    write_lines,
)

TABLE = load_bronze_config().stream.table


def _summary(result):
    assert result.stdout, result.stderr.decode()
    return json.loads(result.stdout.decode().strip().splitlines()[-1])


def test_real_de03_cli_stream_piped_into_bronze_preserves_every_line(tmp_path):
    incidents = subprocess.run(
        [sys.executable, "-m", "simulator.incidents", "--generator-config", str(FAST_GENERATOR_CONFIG),
         "--max-events", "60", "--enable-malformed", "--enable-duplicate", "--enable-late-event",
         "--enable-out-of-order"],
        cwd=REPO_ROOT, capture_output=True, check=True,
    )
    lines = incidents.stdout.decode().splitlines()
    assert len(lines) >= 60
    out = tmp_path / "bronze"
    result = run_cli("stream", "--input", "-", "--run-id", "piped", "--output-dir", str(out), stdin=incidents.stdout)
    assert result.returncode == 0, result.stderr.decode()
    summary = _summary(result)
    assert summary["status"] == "COMMITTED"
    assert summary["records_written"] == len(lines)
    records = read_jsonl(out / TABLE / "run_id=piped" / "part-00000.jsonl")
    for line, record in zip(lines, records, strict=True):
        original = json.loads(line)
        assert {k: record[k] for k in original} == original


def test_batch_subcommand_and_rerun_codes(tmp_path):
    out = tmp_path / "bronze"
    args = ("batch", "--input-dir", str(SAMPLE_BATCH_DIR), "--output-dir", str(out))
    first = run_cli(*args, "--run-id", "b1")
    assert first.returncode == 0, first.stderr.decode()
    assert _summary(first)["status"] == "COMMITTED"
    again = run_cli(*args, "--run-id", "b1")
    assert again.returncode == 0
    assert _summary(again)["status"] == "ALREADY_COMMITTED"
    dup = run_cli(*args, "--run-id", "b2")
    assert dup.returncode == 1
    assert _summary(dup)["error_type"] == "DuplicateInputError"
    replay = run_cli(*args, "--run-id", "b3", "--allow-replay")
    assert replay.returncode == 0
    assert _summary(replay)["replay_of"] == ["b1"]


def test_strict_flag_fails_loudly_on_truncated_input(tmp_path):
    src = tmp_path / "events.ndjson"
    src.write_bytes(("\n".join(clean_stream_lines(3)) + '\n{"event_id": "E').encode())
    out = tmp_path / "bronze"
    result = run_cli("stream", "--input", str(src), "--run-id", "s", "--output-dir", str(out), "--strict")
    assert result.returncode == 1
    assert _summary(result)["error_type"] == "MalformedInputError"
    assert b"bronze run failed" in result.stderr
    assert not (out / TABLE).exists()
    lenient = run_cli("stream", "--input", str(src), "--run-id", "s", "--output-dir", str(out))
    assert lenient.returncode == 0
    assert _summary(lenient)["parse_error_records"] == 1


def test_missing_input_and_conflicting_run_id_exit_nonzero(tmp_path):
    out = tmp_path / "bronze"
    missing = run_cli("stream", "--input", str(tmp_path / "nope.ndjson"), "--run-id", "m", "--output-dir", str(out))
    assert missing.returncode == 1
    assert _summary(missing)["error_type"] == "InputError"
    lines = clean_stream_lines(4)
    a = write_lines(tmp_path / "a.ndjson", lines[:2])
    b = write_lines(tmp_path / "b.ndjson", lines[2:])
    assert run_cli("stream", "--input", str(a), "--run-id", "c", "--output-dir", str(out)).returncode == 0
    conflict = run_cli("stream", "--input", str(b), "--run-id", "c", "--output-dir", str(out))
    assert conflict.returncode == 1
    assert _summary(conflict)["error_type"] == "RunIdConflictError"


def test_generated_run_id_when_omitted(tmp_path):
    src = write_lines(tmp_path / "e.ndjson", clean_stream_lines(2))
    result = run_cli("stream", "--input", str(src), "--output-dir", str(tmp_path / "bronze"))
    assert result.returncode == 0, result.stderr.decode()
    run_id = _summary(result)["run_id"]
    assert (tmp_path / "bronze" / "_runs" / f"{run_id}.json").exists()


def test_default_config_values_and_validation():
    config = load_bronze_config()
    assert config.parse_error_policy.value == "preserve"
    assert config.verify_source_manifest is True
    assert str(config.output_dir).startswith("simulator/_out/")
    data = config.model_dump(mode="json")
    with pytest.raises(ValidationError):
        BronzeConfig.model_validate(data | {"parse_error_policy": "drop"})
    with pytest.raises(ValidationError):
        BronzeConfig.model_validate(data | {"unexpected": 1})
    with pytest.raises(ValidationError):
        BronzeConfig.model_validate(data | {"stream": {"source": "s", "table": "../x"}})
    with pytest.raises(ValidationError):
        BronzeConfig.model_validate(
            data | {"batch": data["batch"] | {"record_source_template": "no-placeholder"}}
        )
