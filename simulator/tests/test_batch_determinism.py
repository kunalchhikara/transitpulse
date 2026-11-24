"""DE-04 determinism: same seed + config -> byte-identical output, in-process
and across two genuinely separate `python -m simulator.batch` OS processes;
different seed -> different output; output is prefix-stable as service_days
grows; the committed sample fixture is reproducible; reruns are idempotent."""

import hashlib
import json

from simulator.batch.config import load_batch_config
from simulator.batch.generator import serialize_row
from simulator.batch.schemas import DATASET_CATALOGUE
from simulator.batch.writer import MANIFEST_FILE_NAME
from simulator.tests._batch_helpers import (
    SAMPLE_BATCH_CONFIG,
    SAMPLE_OUTPUT_DIR,
    SAMPLE_SCALE_CONFIG,
    generate,
    run_cli,
    with_overrides,
)

OUTPUT_FILES = [spec.file_name for spec in DATASET_CATALOGUE.values()] + [MANIFEST_FILE_NAME]
SAMPLE_ARGS = ("--batch-config", str(SAMPLE_BATCH_CONFIG), "--scale-config", str(SAMPLE_SCALE_CONFIG))


def _serialized(datasets):
    return {name: [serialize_row(r) for r in datasets.rows(name)] for name in DATASET_CATALOGUE}


def test_same_seed_and_config_is_identical_in_process():
    cfg = with_overrides(load_batch_config(), batch={"service_days": 2})
    a, _ = generate(cfg)
    b, _ = generate(cfg)
    assert _serialized(a) == _serialized(b)


def test_different_seed_changes_output():
    cfg = with_overrides(load_batch_config(), batch={"service_days": 2})
    a, _ = generate(cfg)
    b, _ = generate(cfg.with_seed(cfg.batch.seed + 1))
    sa, sb = _serialized(a), _serialized(b)
    # Timetable structure (trip ids) is seed-independent; operated values are not.
    assert [json.loads(r)["trip_id"] for r in sa["schedules"]] == [
        json.loads(r)["trip_id"] for r in sb["schedules"]
    ]
    for name in ("historical_trips", "historical_telemetry", "passenger_journeys",
                 "service_disruptions", "weather_observations", "vehicle_depot_assignments"):
        assert sa[name] != sb[name], name


def test_output_is_prefix_stable_when_service_days_grows():
    base = load_batch_config()
    short, _ = generate(with_overrides(base, batch={"service_days": 2}))
    long, _ = generate(with_overrides(base, batch={"service_days": 4}))
    first_days = set(with_overrides(base, batch={"service_days": 2}).batch.service_dates)
    for name in DATASET_CATALOGUE:
        short_rows = [serialize_row(r) for r in short.rows(name)]
        long_prefix = [serialize_row(r) for r in long.rows(name) if r.event_date in first_days]
        assert short_rows == long_prefix, name


def test_two_separate_cli_processes_produce_byte_identical_files_default_config(tmp_path):
    run_a, run_b = tmp_path / "a", tmp_path / "b"
    run_cli(run_a)
    run_cli(run_b)
    for filename in OUTPUT_FILES:
        content_a = (run_a / filename).read_bytes()
        content_b = (run_b / filename).read_bytes()
        assert content_a, f"{filename} empty"
        assert content_a == content_b, f"{filename} differed between separate processes"


def test_cli_seed_override_is_deterministic_and_effective(tmp_path):
    run_cli(tmp_path / "s7a", *SAMPLE_ARGS, "--seed", "7")
    run_cli(tmp_path / "s7b", *SAMPLE_ARGS, "--seed", "7")
    run_cli(tmp_path / "s8", *SAMPLE_ARGS, "--seed", "8")
    trips = "historical_trips.jsonl"
    assert (tmp_path / "s7a" / trips).read_bytes() == (tmp_path / "s7b" / trips).read_bytes()
    assert (tmp_path / "s7a" / trips).read_bytes() != (tmp_path / "s8" / trips).read_bytes()
    manifest = json.loads((tmp_path / "s7a" / MANIFEST_FILE_NAME).read_text())
    assert manifest["batch_seed"] == 7


def test_committed_sample_fixture_is_reproducible(tmp_path):
    """Regenerating the sample with its committed configs reproduces the
    committed files byte-for-byte (guards against silent generator drift)."""
    run_cli(tmp_path, *SAMPLE_ARGS)
    for filename in OUTPUT_FILES:
        assert (tmp_path / filename).read_bytes() == (SAMPLE_OUTPUT_DIR / filename).read_bytes(), filename


def test_rerun_into_same_directory_is_idempotent(tmp_path):
    run_cli(tmp_path, *SAMPLE_ARGS)
    first = {f: (tmp_path / f).read_bytes() for f in OUTPUT_FILES}
    run_cli(tmp_path, *SAMPLE_ARGS)
    second = {f: (tmp_path / f).read_bytes() for f in OUTPUT_FILES}
    assert first == second
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(OUTPUT_FILES)  # no .tmp leftovers


def test_manifest_reconciles_with_written_files():
    manifest = json.loads((SAMPLE_OUTPUT_DIR / MANIFEST_FILE_NAME).read_text())
    assert set(manifest["datasets"]) == set(DATASET_CATALOGUE)
    for name, entry in manifest["datasets"].items():
        data = (SAMPLE_OUTPUT_DIR / entry["file"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == entry["sha256"], name
        lines = data.decode("utf-8").splitlines()
        assert len(lines) == entry["row_count"], name
        watermarks = sorted(json.loads(line)[entry["watermark_column"]] for line in lines)
        assert entry["min_watermark"] == watermarks[0]
        assert entry["max_watermark"] == watermarks[-1]
        assert entry["primary_key"] == list(DATASET_CATALOGUE[name].primary_key)
