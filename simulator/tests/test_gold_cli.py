"""DE-07 Gold CLI: exit codes, summary line, alternate config, refusal paths."""

from __future__ import annotations

import hashlib
import json

from simulator.gold.cli import main
from simulator.tests._gold_helpers import (
    EXPECTED_TOTALS,
    _event,
    fixture_silver,
    silver_from_lines,
)


def _main(capsys, tmp_path, *extra: str) -> tuple[int, dict]:
    code = main(["--silver-dir", str(tmp_path / "silver"), "--output-dir", str(tmp_path / "gold"),
                 "--fixed-processing-time", "2025-02-01T11:00:00Z", "--log-file", str(tmp_path / "gold.log"),
                 *extra])
    return code, json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_commit_then_no_op(tmp_path, capsys):
    fixture_silver(tmp_path)
    code, summary = _main(capsys, tmp_path, "--run-id", "g1")
    assert code == 0 and summary["status"] == "COMMITTED"
    assert summary["silver_runs_consumed"] == ["s1"] and summary["silver_selection"] == "latest_committed"
    assert summary["event_count"] == EXPECTED_TOTALS["valid_loaded"]
    assert summary["quarantined_unattributed"] == EXPECTED_TOTALS["quarantined_unattributed"]
    assert summary["reconciliation_holds"] is True
    code, summary = _main(capsys, tmp_path, "--run-id", "g1")
    assert code == 0 and summary["status"] == "ALREADY_COMMITTED"
    logs = [json.loads(line) for line in (tmp_path / "gold.log").read_text().splitlines()]
    assert any(rec["message"] == "gold run committed" for rec in logs)


def test_missing_silver_input_exits_1(tmp_path, capsys):
    code, summary = _main(capsys, tmp_path, "--run-id", "g1")
    assert code == 1 and summary["status"] == "FAILED" and summary["error_type"] == "GoldInputError"


def test_clean_event_outside_the_silver_timestamp_bounds_fails_with_the_json_contract(tmp_path, capsys):
    """QA of PR #12 (D1): Silver loaded an event dated 9999-12-31T20:00Z as
    clean, and its +10 local bucket overflowed datetime. Since DE-08 (FU-10)
    Silver quarantines such an event (`timestamp_bounds`), so the clean record
    is planted by editing the committed clean file and re-signing its sha256
    in the Silver manifest. What this now exercises is Gold's clean-record
    BOUNDS contract check (the event_time is outside the bounds recorded in
    the Silver manifest, which is checked before any period is computed): it
    must fail the run through the documented FAILED summary line, not an
    uncaught traceback."""
    far = "9999-12-31T20:00:00.000000Z"
    ok = _event("OK1", "ROUTE-01", "2025-01-01T00:10:00Z", 30)
    result = silver_from_lines(
        tmp_path, [json.dumps(ok), json.dumps(_event("FAR1", "ROUTE-01", "2025-01-01T00:20:00Z", 30))]
    )
    clean = tmp_path / "silver" / result.manifest["tables"]["clean"]["path"]
    planted = clean.read_bytes().replace(b'"event_time":"2025-01-01T00:20:00.000000Z"',
                                         f'"event_time":"{far}"'.encode())
    assert far.encode() in planted
    clean.write_bytes(planted)
    manifest_path = tmp_path / "silver" / "_runs" / "s1.json"
    silver_manifest = json.loads(manifest_path.read_text())
    silver_manifest["tables"]["clean"]["sha256"] = hashlib.sha256(planted).hexdigest()
    manifest_path.write_text(json.dumps(silver_manifest))
    code, summary = _main(capsys, tmp_path, "--run-id", "g1")
    assert code == 1 and summary["status"] == "FAILED" and summary["error_type"] == "GoldInputError"
    assert "FAR1" in summary["error"]
    assert "outside the Silver timestamp bounds" in summary["error"]
    manifest = json.loads((tmp_path / "gold" / "_runs" / "g1.json").read_text())
    assert manifest["status"] == "FAILED"


def test_unknown_silver_run_id_exits_1(tmp_path, capsys):
    fixture_silver(tmp_path)
    code, summary = _main(capsys, tmp_path, "--run-id", "g1", "--silver-run-id", "nope")
    assert code == 1 and summary["error_type"] == "GoldInputError"


def test_run_id_conflict_exits_1(tmp_path, capsys):
    fixture_silver(tmp_path)
    assert _main(capsys, tmp_path, "--run-id", "g1")[0] == 0
    alt = tmp_path / "alt.toml"
    alt.write_text(
        '[gold]\noutput_dir = "x"\nsilver_dir = "y"\nroute_performance_table = "route_performance"\n'
        "period_minutes = 30\nutc_offset_hours = 10\n"
    )
    code, summary = _main(capsys, tmp_path, "--run-id", "g1", "--gold-config", str(alt))
    assert code == 1 and summary["error_type"] == "GoldRunIdConflictError"
    code, summary = _main(capsys, tmp_path, "--run-id", "g30", "--gold-config", str(alt))
    assert code == 0 and summary["status"] == "COMMITTED"
    manifest = json.loads((tmp_path / "gold" / "_runs" / "g30.json").read_text())
    assert manifest["config"]["period_minutes"] == 30


def test_invalid_run_id_exits_1(tmp_path, capsys):
    fixture_silver(tmp_path)
    code, summary = _main(capsys, tmp_path, "--run-id", "../evil")
    assert code == 1 and summary["status"] == "FAILED"
