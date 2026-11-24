"""Smoke test for `python -m simulator.incidents` -- confirms the CLI
wiring (config loading, --enable-* flags, IncidentInjector/runner
composition) actually runs end-to-end as a separate process, writing valid
JSON lines, mirroring `simulator/tests/test_generator_determinism.py`'s CLI
subprocess pattern."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FAST_GENERATOR_CONFIG = Path(__file__).resolve().parent / "fixtures" / "fast_generator.toml"


def test_cli_runs_with_enable_flags_and_writes_valid_json_lines(tmp_path):
    output_path = tmp_path / "events.ndjson"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simulator.incidents",
            "--generator-config",
            str(FAST_GENERATOR_CONFIG),
            "--max-events",
            "40",
            "--output",
            str(output_path),
            "--enable-malformed",
            "--enable-duplicate",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"CLI run failed:\nstdout={result.stdout}\nstderr={result.stderr}"

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 40
    for line in lines:
        json.loads(line)  # every line is well-formed JSON, malformed or not


def test_cli_with_no_flags_matches_default_disabled_baseline(tmp_path):
    output_path = tmp_path / "events.ndjson"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simulator.incidents",
            "--generator-config",
            str(FAST_GENERATOR_CONFIG),
            "--max-events",
            "10",
            "--output",
            str(output_path),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"CLI run failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 10
    for line in lines:
        payload = json.loads(line)
        assert payload["status"] in ("IN_SERVICE", "DELAYED")
        assert payload["vehicle_id"] is not None
