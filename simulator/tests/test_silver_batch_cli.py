"""DE-12 CLI (`python -m simulator.silver_batch`): the full DE-11 CLI ->
Bronze -> batch-Silver CLI chain in two OS processes per root with a fixed
clock is byte-identical across roots; exit codes 0/1/2; every log line
carries the run id; a bad --log-file is a structured refusal (FU-12)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from simulator.incremental import format_ts
from simulator.tests._incremental_helpers import all_watermarks
from simulator.tests._silver_batch_helpers import (
    REPO_ROOT,
    make_sb_env,
    run_cli,
    tree_bytes,
)

FIXED = "2031-01-01T00:00:00Z"
SB_COMMON = ("--bronze-dir", "bronze", "--output-dir", "silver_batch", "--incremental-state-dir", "state",
             "--fixed-processing-time", FIXED)
INC_COMMON = ("--state-dir", "state", "--batch-source-dir", "batch", "--reference-source-dir", "ref",
              "--bronze-output-dir", "bronze", "--fixed-clock", "2030-01-01T00:00:00Z")


def _incremental_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    return subprocess.run([sys.executable, "-m", "simulator.incremental", *args], cwd=cwd, env=env,
                          capture_output=True, check=False)


def _root(tmp_path: Path, name: str) -> Path:
    root = tmp_path / name
    make_sb_env(root, relative=True)
    return root


def test_two_cli_chains_are_byte_identical_with_a_fixed_clock(tmp_path: Path) -> None:
    results = []
    for name in ("one", "two"):
        root = _root(tmp_path, name)
        values = all_watermarks(root / "batch")
        stdout = []
        for index, cut in enumerate((values[len(values) // 2], values[-1])):
            inc = _incremental_cli("--as-of", format_ts(cut), *INC_COMMON, cwd=root)
            assert inc.returncode == 0, inc.stderr.decode()
            proc = run_cli("--run-id", f"sb-{index}", *SB_COMMON, "--log-file", "sb.log", cwd=root)
            assert proc.returncode == 0, proc.stdout.decode() + proc.stderr.decode()
            stdout.append(proc.stdout)
        results.append((tree_bytes(root / "silver_batch"), stdout))
    (tree_a, out_a), (tree_b, out_b) = results
    assert tree_a and tree_a == tree_b and out_a == out_b
    summary = json.loads(out_a[-1])
    assert summary["status"] == "COMMITTED" and summary["commit_sequence"] == 2 and summary["base_run_id"] == "sb-0"
    assert summary["reconciliation_holds"] is True and summary["manifest"] == "_runs/sb-1.json"
    assert any(k.startswith("current_historical_trips/") for k in tree_a)


def test_exit_codes(tmp_path: Path) -> None:
    root = _root(tmp_path, "codes")
    inc = _incremental_cli("--as-of", "2030-01-01T00:00:00Z", *INC_COMMON, cwd=root)
    assert inc.returncode == 0, inc.stderr.decode()

    ok = run_cli("--run-id", "s1", *SB_COMMON, cwd=root)
    assert ok.returncode == 0, ok.stdout
    first = json.loads(ok.stdout)
    assert first["status"] == "COMMITTED" and first["datasets"]["schedules"]["inserted"] > 0
    again = run_cli("--run-id", "s1", *SB_COMMON, cwd=root)
    assert again.returncode == 0 and json.loads(again.stdout)["status"] == "ALREADY_COMMITTED"

    conflict = run_cli("--run-id", "s1", *SB_COMMON, "--no-incremental-listing", cwd=root)
    assert conflict.returncode == 2
    assert json.loads(conflict.stdout)["error_type"] == "SilverBatchRunIdConflictError"
    unknown = run_cli("--run-id", "s2", *SB_COMMON, "--bronze-run-id", "nope", cwd=root)
    assert unknown.returncode == 2 and json.loads(unknown.stdout)["error_type"] == "SilverBatchInputError"
    bad_id = run_cli("--run-id", "../escape", *SB_COMMON, cwd=root)
    assert bad_id.returncode == 2 and json.loads(bad_id.stdout)["error_type"] == "InvalidRunIdError"
    (root / "bad.toml").write_text('[silver_batch]\noutput_dir = "x"\nbronze_dir = "y"\nsurprise = 1\n')
    bad_config = run_cli("--run-id", "s3", *SB_COMMON, "--config", "bad.toml", cwd=root)
    assert bad_config.returncode == 2 and json.loads(bad_config.stdout)["error_type"] == "ValidationError"
    no_log_dir = run_cli("--run-id", "s4", *SB_COMMON, "--log-file", "missing/dir/sb.log", cwd=root)
    assert no_log_dir.returncode == 2 and json.loads(no_log_dir.stdout)["status"] == "REFUSED"
    assert b"Traceback" not in no_log_dir.stderr

    # a FAILED run (exit 1): the DE-11 extract manifests are not where the config says
    (root / "fake_state").mkdir()
    failed = run_cli("--run-id", "s5", "--bronze-dir", "bronze", "--output-dir", "other_sb",
                     "--incremental-state-dir", "fake_state", "--fixed-processing-time", FIXED, cwd=root)
    assert failed.returncode == 1
    assert json.loads(failed.stdout)["error_type"] == "BronzeContractError"
    m = json.loads((root / "other_sb" / "_runs" / "s5.json").read_text())
    assert m["status"] == "FAILED" and m["checkpoint"] is None
    assert str(root) not in json.dumps(m) and str(root.resolve()) not in json.dumps(m)


def test_log_file_lines_all_carry_the_run_id_and_are_appended(tmp_path: Path) -> None:
    root = _root(tmp_path, "logs")
    assert _incremental_cli("--as-of", "2030-01-01T00:00:00Z", *INC_COMMON, cwd=root).returncode == 0
    for run_id in ("s1", "s1", "s2"):
        proc = run_cli("--run-id", run_id, *SB_COMMON, "--log-file", "sb.log", cwd=root)
        assert proc.returncode == 0
    lines = [json.loads(x) for x in (root / "sb.log").read_text().splitlines()]
    assert len(lines) >= 4
    assert all(line["context"]["silver_batch_run_id"] in ("s1", "s2") for line in lines)
    assert {line["context"]["silver_batch_run_id"] for line in lines} == {"s1", "s2"}
