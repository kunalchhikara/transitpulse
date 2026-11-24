"""Determinism tests for the DE-02 telemetry generator: same seed + config
-> byte-identical event sequence, both across two in-process generator
instances and across two genuinely separate CLI subprocess runs (mirroring
simulator/tests/test_reference_cli_determinism.py's pattern for DE-01)."""

import subprocess
import sys
from pathlib import Path

from simulator.tests._generator_helpers import build_generator

REPO_ROOT = Path(__file__).resolve().parents[2]
FAST_GENERATOR_CONFIG = Path(__file__).resolve().parent / "fixtures" / "fast_generator.toml"

_EVENT_COUNT = 400


def test_same_seed_config_produces_byte_identical_event_sequence_in_process():
    generator_a = build_generator()
    generator_b = build_generator()

    lines_a = [e.model_dump_json() for e in generator_a.events(_EVENT_COUNT)]
    lines_b = [e.model_dump_json() for e in generator_b.events(_EVENT_COUNT)]

    assert lines_a, "no events were generated"
    assert lines_a == lines_b
    diff = [(a, b) for a, b in zip(lines_a, lines_b) if a != b]
    assert diff == []


def test_different_seed_produces_different_event_sequence():
    from simulator.generator.config import load_generator_config

    generator_config = load_generator_config()
    varied_config = generator_config.model_copy(update={"seed": generator_config.seed + 1})

    baseline = build_generator()
    varied = build_generator(generator_config=varied_config)

    baseline_lines = [e.model_dump_json() for e in baseline.events(_EVENT_COUNT)]
    varied_lines = [e.model_dump_json() for e in varied.events(_EVENT_COUNT)]

    assert baseline_lines != varied_lines


def _run_cli(output_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simulator.generator",
            "--generator-config",
            str(FAST_GENERATOR_CONFIG),
            "--max-events",
            str(_EVENT_COUNT),
            "--output",
            str(output_path),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"CLI run failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    )


def test_two_separate_process_runs_produce_byte_identical_output(tmp_path):
    run_a = tmp_path / "run_a.ndjson"
    run_b = tmp_path / "run_b.ndjson"

    _run_cli(run_a)
    _run_cli(run_b)

    content_a = run_a.read_bytes()
    content_b = run_b.read_bytes()
    assert content_a, "first run produced no output"
    assert content_a == content_b, "output differed between two separate CLI process runs"
