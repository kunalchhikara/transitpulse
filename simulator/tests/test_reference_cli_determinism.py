"""Determinism across two genuinely separate process runs (not just two
in-process calls): invoke the `python -m simulator.reference` CLI twice,
each in its own subprocess, and assert the written files are byte-for-byte
identical (diff is empty)."""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_cli(output_dir: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "simulator.reference", "--output-dir", str(output_dir)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"CLI run failed:\nstdout={result.stdout}\nstderr={result.stderr}"


def test_two_separate_process_runs_produce_byte_identical_files(tmp_path):
    run_a_dir = tmp_path / "run_a"
    run_b_dir = tmp_path / "run_b"

    _run_cli(run_a_dir)
    _run_cli(run_b_dir)

    for filename in ("depots.json", "stops.json", "routes.json", "vehicles.json"):
        content_a = (run_a_dir / filename).read_bytes()
        content_b = (run_b_dir / filename).read_bytes()
        assert content_a == content_b, f"{filename} differed between separate runs"
        assert content_a, f"{filename} was empty"
