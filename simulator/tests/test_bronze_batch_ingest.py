"""DE-05 batch-shaped ingestion of the committed DE-04 sample: one Bronze
table per dataset, every row round-trips unmodified, counts reconcile with
the DE-04 manifest, `event_date` is never re-derived, and FU-02 rows
(`updated_at` earlier than a referenced row's) are ingested, not rejected."""

from __future__ import annotations

import json
import shutil

import pytest

from simulator.bronze import (
    BronzeStore,
    InputError,
    SourceManifestError,
    ingest_batch,
    load_bronze_config,
)
from simulator.tests._bronze_helpers import (
    FIXED_CLOCK,
    SAMPLE_BATCH_DIR,
    assert_round_trip,
    read_jsonl,
)

CONFIG = load_bronze_config()
SOURCE_MANIFEST = json.loads((SAMPLE_BATCH_DIR / "manifest.json").read_text(encoding="utf-8"))
DATASETS = sorted(SOURCE_MANIFEST["datasets"])


@pytest.fixture
def sample_run(tmp_path):
    out = tmp_path / "bronze"
    result = ingest_batch(SAMPLE_BATCH_DIR, run_id="batch-1", output_dir=out, clock=FIXED_CLOCK)
    return result, out


def test_every_sample_dataset_round_trips_unmodified(sample_run):
    result, out = sample_run
    assert result.status == "COMMITTED"
    assert sorted(result.manifest["tables"]) == DATASETS
    for dataset in DATASETS:
        input_lines = (SAMPLE_BATCH_DIR / f"{dataset}.jsonl").read_text(encoding="utf-8").splitlines()
        part = out / result.manifest["tables"][dataset]["path"]
        assert_round_trip(
            input_lines,
            read_jsonl(part),
            part.read_text(encoding="utf-8").splitlines(),
            run_id="batch-1",
            source=CONFIG.batch.record_source(dataset),
            source_file=f"{dataset}.jsonl",
        )


def test_counts_reconcile_with_de04_manifest(sample_run):
    result, _ = sample_run
    reconciliation = result.manifest["source_reconciliation"]
    assert reconciliation["status"] == "MATCHED"
    assert reconciliation["verified"] is True
    for dataset in DATASETS:
        assert result.manifest["tables"][dataset]["records_written"] == SOURCE_MANIFEST["datasets"][dataset]["row_count"]
    assert result.manifest["totals"]["records_written"] == sum(
        d["row_count"] for d in SOURCE_MANIFEST["datasets"].values()
    )
    assert result.manifest["ignored_files"] == []


def test_event_date_preserved_exactly_even_where_it_differs_from_utc_date(sample_run):
    _, out = sample_run
    store = BronzeStore(out)
    differing = 0
    for dataset in DATASETS:
        source_rows = read_jsonl(SAMPLE_BATCH_DIR / f"{dataset}.jsonl")
        bronze_rows = list(store.read_table(dataset))
        assert [r["event_date"] for r in bronze_rows] == [r["event_date"] for r in source_rows]
        differing += sum(1 for r in bronze_rows if r["event_date"] != r["updated_at"][:10])
    # The sample has local service dates that differ from the UTC date of
    # updated_at - so the assertion above really proves no re-derivation.
    assert differing > 0


def test_fu02_rows_with_updated_at_before_referenced_row_are_ingested(sample_run):
    _, out = sample_run
    store = BronzeStore(out)
    disruptions = {d["disruption_id"]: d["updated_at"] for d in store.read_table("service_disruptions")}
    trips = list(store.read_table("historical_trips"))
    fu02 = [t for t in trips if t["disruption_id"] and t["updated_at"] < disruptions[t["disruption_id"]]]
    assert fu02, "sample no longer contains an FU-02 case; test would prove nothing"
    source_trip_ids = {r["trip_id"] for r in read_jsonl(SAMPLE_BATCH_DIR / "historical_trips.jsonl")}
    assert {t["trip_id"] for t in trips} == source_trip_ids


def _copy_sample(tmp_path):
    batch_dir = tmp_path / "batch"
    shutil.copytree(SAMPLE_BATCH_DIR, batch_dir)
    return batch_dir


def test_tampered_dataset_fails_loudly_against_source_manifest(tmp_path):
    batch_dir = _copy_sample(tmp_path)
    path = batch_dir / "historical_trips.jsonl"
    path.write_bytes(path.read_bytes()[:-50])  # truncated transfer
    out = tmp_path / "bronze"
    with pytest.raises(SourceManifestError, match="historical_trips.jsonl"):
        ingest_batch(batch_dir, run_id="tampered", output_dir=out, clock=FIXED_CLOCK)
    store = BronzeStore(out)
    assert store.read_manifest("tampered")["status"] == "FAILED"
    assert store.committed_manifests() == []
    assert not (out / "historical_trips").exists()


def test_verification_disabled_records_mismatch_and_preserves_truncated_line(tmp_path):
    batch_dir = _copy_sample(tmp_path)
    path = batch_dir / "historical_trips.jsonl"
    original = path.read_bytes()
    path.write_bytes(original[:-50])
    config = CONFIG.model_copy(update={"verify_source_manifest": False})
    out = tmp_path / "bronze"
    result = ingest_batch(batch_dir, run_id="unverified", output_dir=out, clock=FIXED_CLOCK, config=config)
    assert result.manifest["source_reconciliation"]["status"] == "MISMATCH"
    assert result.manifest["source_reconciliation"]["mismatched_files"] == ["historical_trips.jsonl"]
    trips = result.manifest["tables"]["historical_trips"]
    assert trips["records_written"] == SOURCE_MANIFEST["datasets"]["historical_trips"]["row_count"]
    assert trips["parse_error_records"] == 1
    last = list(BronzeStore(out).read_table("historical_trips"))[-1]
    assert last["_bronze_parse_error"] == "invalid_json"
    assert last["_bronze_raw_line"].encode() == original[:-50].split(b"\n")[-1]


def test_directory_without_manifest_ingests_every_jsonl_file(tmp_path):
    batch_dir = _copy_sample(tmp_path)
    (batch_dir / "manifest.json").unlink()
    (batch_dir / "notes.txt").write_text("not a dataset")
    out = tmp_path / "bronze"
    result = ingest_batch(batch_dir, run_id="nomanifest", output_dir=out, clock=FIXED_CLOCK)
    assert sorted(result.manifest["tables"]) == DATASETS
    assert result.manifest["source_reconciliation"] == {"status": "NO_SOURCE_MANIFEST"}
    assert result.manifest["ignored_files"] == ["notes.txt"]


def test_missing_listed_dataset_and_empty_dir_fail_loudly(tmp_path):
    batch_dir = _copy_sample(tmp_path)
    (batch_dir / "schedules.jsonl").unlink()
    with pytest.raises(InputError, match="schedules"):
        ingest_batch(batch_dir, run_id="missing", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK)
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(InputError, match="no datasets"):
        ingest_batch(empty, run_id="empty", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK)


def test_unsafe_file_name_in_source_manifest_rejected(tmp_path):
    batch_dir = _copy_sample(tmp_path)
    manifest = json.loads((batch_dir / "manifest.json").read_text())
    manifest["datasets"]["schedules"]["file"] = "../outside.jsonl"
    (batch_dir / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(InputError, match="unsafe"):
        ingest_batch(batch_dir, run_id="unsafe", output_dir=tmp_path / "bronze", clock=FIXED_CLOCK)


def test_empty_dataset_file_is_committed_as_empty_table(tmp_path):
    batch_dir = _copy_sample(tmp_path)
    manifest = json.loads((batch_dir / "manifest.json").read_text())
    (batch_dir / "weather_observations.jsonl").write_bytes(b"")
    manifest["datasets"]["weather_observations"]["row_count"] = 0
    manifest["datasets"]["weather_observations"]["sha256"] = (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    (batch_dir / "manifest.json").write_text(json.dumps(manifest))
    out = tmp_path / "bronze"
    result = ingest_batch(batch_dir, run_id="emptyweather", output_dir=out, clock=FIXED_CLOCK)
    assert result.manifest["tables"]["weather_observations"]["records_written"] == 0
    assert (out / result.manifest["tables"]["weather_observations"]["path"]).read_bytes() == b""
