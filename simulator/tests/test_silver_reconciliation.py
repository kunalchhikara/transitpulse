"""DE-06 reconciliation identity and the Bronze read contract.

`extracted == valid_loaded + quarantined + duplicates_excluded` (plus the
supporting identities) is asserted in code before commit; these tests check
it against counts computed here independently, and prove that a broken
pipeline fails loudly with nothing committed. Bronze is read only through
committed run manifests."""

from __future__ import annotations

import json
import shutil

import pytest

from simulator.bronze import (
    BronzeStore,
    MalformedInputError,
    ParseErrorPolicy,
    ingest_batch,
    ingest_stream,
    load_bronze_config,
)
from simulator.silver import (
    RECONCILIATION_IDENTITIES,
    SilverAccountingError,
    SilverInputError,
)
from simulator.silver import pipeline as silver_pipeline
from simulator.tests._bronze_helpers import FIXED_CLOCK, SAMPLE_BATCH_DIR, read_jsonl
from simulator.tests._silver_helpers import (
    SILVER_CONFIG,
    bronze_from_lines,
    clean_payloads,
    silver,
    table,
    valid_payload,
)

TABLE = SILVER_CONFIG.bronze_table


def _mixed_lines():
    good = clean_payloads(30)
    lines = [json.dumps(p) for p in good]
    lines += [json.dumps(good[3]), json.dumps(good[7])]  # 2 duplicates
    lines += [json.dumps(valid_payload(event_id="Q1", speed_kmh=-1.0)), "not json", "[]"]  # 3 quarantined
    return lines


def test_identity_holds_and_matches_independent_counts(tmp_path):
    lines = _mixed_lines()
    result = silver(bronze_from_lines(tmp_path / "bronze", lines))
    c = result.manifest["counters"]
    # Independent: what was fed in.
    assert c["extracted"] == len(lines) == 35
    assert (c["valid_loaded"], c["duplicates_excluded"], c["quarantined"]) == (30, 2, 3)
    assert c["extracted"] == c["valid_loaded"] + c["quarantined"] + c["duplicates_excluded"]
    assert c["quarantined"] == c["parse_errors"] + c["schema_failures"] + c["rule_failures"]
    assert c["valid_loaded"] == c["in_order"] + c["out_of_order"] + c["late"]
    assert c["quality_percentage"] == round(100 * 30 / 33, 4)
    recon = result.manifest["reconciliation"]
    assert recon["holds"] is True and all(recon["checks"].values())
    assert recon["identities"] == list(RECONCILIATION_IDENTITIES)
    # Every input record is in exactly one output table, identified by its Bronze lineage.
    seen = [
        (r["bronze_run_id"], r["_bronze_line_number"])
        for role in ("clean", "quarantine", "duplicates")
        for r in table(result, role)
    ]
    assert sorted(seen) == [("b1", n) for n in range(1, len(lines) + 1)]
    for role, info in result.manifest["tables"].items():
        assert info["records"] == len(table(result, role))


def test_empty_bronze_run_reconciles_with_undefined_quality(tmp_path):
    bronze = tmp_path / "bronze"
    bronze_from_lines(bronze, [""])  # only a blank line: a committed run with zero records
    result = silver(bronze)
    c = result.manifest["counters"]
    assert c["extracted"] == c["valid_loaded"] == 0
    assert c["quality_percentage"] is None
    assert result.manifest["reconciliation"]["holds"]


def test_dropped_input_record_fails_loudly_and_commits_nothing(tmp_path, monkeypatch):
    bronze = bronze_from_lines(tmp_path / "bronze", _mixed_lines())
    real_iter_rows = silver_pipeline.iter_rows

    def lossy(*args, **kwargs):
        for index, row in enumerate(real_iter_rows(*args, **kwargs)):
            if index != 4:
                yield row

    monkeypatch.setattr(silver_pipeline, "iter_rows", lossy)
    out = tmp_path / "silver"
    with pytest.raises(SilverAccountingError, match="reconciliation failed"):
        silver(bronze, out)
    manifest = json.loads((out / "_runs" / "s1.json").read_text())
    assert manifest["status"] == "FAILED"
    assert manifest["reconciliation"]["checks"]["extracted_equals_bronze_manifests"] is False
    assert not (out / SILVER_CONFIG.clean_table).exists()
    assert not (out / "_staging").exists()


def test_unwritten_output_record_fails_loudly(tmp_path, monkeypatch):
    bronze = bronze_from_lines(tmp_path / "bronze", _mixed_lines())
    real = silver_pipeline._dumps_line

    def drop_one_duplicate(record):
        return "" if "payload_matches_kept" in record else real(record)

    monkeypatch.setattr(silver_pipeline, "_dumps_line", drop_one_duplicate)
    with pytest.raises(SilverAccountingError):
        silver(bronze)


# -- Bronze read contract --------------------------------------------------------


def test_orphaned_uncommitted_bronze_partition_is_never_read(tmp_path):
    bronze = bronze_from_lines(tmp_path / "bronze", [json.dumps(p) for p in clean_payloads(5)])
    committed = bronze / TABLE / "run_id=b1"
    orphan = bronze / TABLE / "run_id=crashed"
    shutil.copytree(committed, orphan)  # e.g. a crash after publish, before the manifest
    result = silver(bronze)
    assert [r["run_id"] for r in result.manifest["bronze_runs_consumed"]] == ["b1"]
    assert result.manifest["counters"]["extracted"] == 5
    with pytest.raises(SilverInputError, match="not committed"):
        silver(bronze, run_id="s2", bronze_run_ids=["crashed"])


def test_failed_bronze_run_is_not_consumed(tmp_path):
    bronze = bronze_from_lines(tmp_path / "bronze", [json.dumps(p) for p in clean_payloads(5)])
    strict = load_bronze_config().model_copy(update={"parse_error_policy": ParseErrorPolicy.FAIL})
    src = tmp_path / "bad.ndjson"
    src.write_text("garbage\n")
    with pytest.raises(MalformedInputError):
        ingest_stream([src], run_id="b-failed", output_dir=bronze, config=strict, clock=FIXED_CLOCK)
    assert BronzeStore(bronze).read_manifest("b-failed")["status"] == "FAILED"
    result = silver(bronze)
    assert [r["run_id"] for r in result.manifest["bronze_runs_consumed"]] == ["b1"]


def test_batch_runs_in_the_same_bronze_store_are_not_consumed(tmp_path):
    bronze = bronze_from_lines(tmp_path / "bronze", [json.dumps(p) for p in clean_payloads(5)])
    ingest_batch(SAMPLE_BATCH_DIR, run_id="batch-1", output_dir=bronze, clock=FIXED_CLOCK)
    result = silver(bronze)
    assert [r["run_id"] for r in result.manifest["bronze_runs_consumed"]] == ["b1"]
    with pytest.raises(SilverInputError):
        silver(bronze, run_id="s2", bronze_run_ids=["batch-1"])


def test_tampered_committed_partition_fails_loudly(tmp_path):
    bronze = bronze_from_lines(tmp_path / "bronze", [json.dumps(p) for p in clean_payloads(5)])
    part = bronze / TABLE / "run_id=b1" / "part-00000.jsonl"
    records = read_jsonl(part)
    part.write_text("\n".join(json.dumps(r) for r in records[:-1]) + "\n")
    out = tmp_path / "silver"
    with pytest.raises(SilverInputError, match="sha256"):
        silver(bronze, out)
    assert json.loads((out / "_runs" / "s1.json").read_text())["status"] == "FAILED"


def test_no_committed_bronze_input_is_refused(tmp_path):
    with pytest.raises(SilverInputError, match="no committed Bronze run"):
        silver(tmp_path / "empty-bronze")


def test_explicit_selection_records_only_the_selected_runs(tmp_path):
    bronze = tmp_path / "bronze"
    p = clean_payloads(10)
    bronze_from_lines(bronze, [json.dumps(x) for x in p[:5]], run_id="b1")
    bronze_from_lines(bronze, [json.dumps(x) for x in p[5:]], run_id="b2")
    result = silver(bronze, bronze_run_ids=["b2"])
    assert result.manifest["bronze_selection"] == "explicit"
    assert [r["run_id"] for r in result.manifest["bronze_runs_consumed"]] == ["b2"]
    assert result.manifest["counters"]["extracted"] == 5
    default = silver(bronze, run_id="s-all")
    assert default.manifest["bronze_selection"] == "all_committed"
    assert default.manifest["counters"]["extracted"] == 10
