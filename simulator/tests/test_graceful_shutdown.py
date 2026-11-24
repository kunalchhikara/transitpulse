"""Graceful-shutdown test: sending SIGINT to the running CLI must produce a
clean exit (code 0) with no corrupt partial record -- every line written to
the output stream must be complete, parseable JSON, and the stream must end
with a newline.

No real `time.sleep` is used for synchronization: this test blocks on the
subprocess's own stdout pipe (`readline()`), which returns as soon as the
subprocess actually writes and flushes a line -- a direct I/O wait, not an
arbitrary fixed delay.
"""

import json
import signal
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FAST_GENERATOR_CONFIG = Path(__file__).resolve().parent / "fixtures" / "fast_generator.toml"

_MIN_LINES_BEFORE_SIGINT = 5


def test_sigint_exits_cleanly_with_no_corrupt_partial_record():
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "simulator.generator",
            "--generator-config",
            str(FAST_GENERATOR_CONFIG),
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout_data = ""
    stderr_data = ""
    try:
        collected_lines: list[str] = []
        for _ in range(_MIN_LINES_BEFORE_SIGINT):
            line = proc.stdout.readline()
            assert line, (
                "process produced no more output before reaching "
                f"{_MIN_LINES_BEFORE_SIGINT} lines (exited early?); "
                f"stderr so far={proc.stderr.read()}"
            )
            collected_lines.append(line)

        proc.send_signal(signal.SIGINT)
        remaining_stdout, stderr_data = proc.communicate(timeout=15)
        stdout_data = "".join(collected_lines) + remaining_stdout
    finally:
        if proc.poll() is None:
            proc.kill()
            leftover_out, leftover_err = proc.communicate()
            stdout_data += leftover_out
            stderr_data += leftover_err

    assert proc.returncode == 0, (
        f"non-clean exit, returncode={proc.returncode}\nstderr={stderr_data}"
    )

    assert stdout_data, "no output was produced at all"
    assert stdout_data.endswith("\n"), "output does not end with a complete line"

    lines = stdout_data.splitlines()
    assert len(lines) >= _MIN_LINES_BEFORE_SIGINT

    for line in lines:
        event = json.loads(line)  # raises if any line is truncated/corrupt
        assert event["schema_version"]
        assert event["event_id"]

    # The structured shutdown log line must also be valid JSON-lines.
    for log_line in stderr_data.splitlines():
        if log_line.strip():
            json.loads(log_line)
    assert any("shutdown signal received" in line for line in stderr_data.splitlines())
