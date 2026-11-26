"""DE-12 criteria 1 and 8: checkpoint + idempotency/determinism as DE-06 -
same run id + input -> ALREADY_COMMITTED; different input refused; failed
attempts kept in `previous_attempts`; crash then retry; byte-identical output
with a fixed clock; no absolute paths / home / username in manifests (FU-04);
the run id on every log line (FU-09 not repeated)."""

from __future__ import annotations

import getpass
import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from simulator.bronze import fixed_clock
from simulator.silver_batch import (
    STAGE_LOADED,
    STAGE_PUBLISHED,
    SilverBatchRunIdConflictError,
    head_manifest,
    read_current_table,
)
from simulator.tests._silver_batch_helpers import (
    at,
    ingest_rows,
    make_sb_env,
    manifest,
    run_sb,
    sample_row,
    sb_config,
    t,
    tree_bytes,
)

DS = "historical_trips"


class SimulatedCrash(BaseException):
    """Process death: no `except Exception` can audit or clean up after it."""


def _two_runs(tmp_path: Path):
    bronze, out = tmp_path / "bronze", tmp_path / "sb"
    ingest_rows(bronze, "b1", {DS: [sample_row(DS, 0), sample_row(DS, 1)]}, at=t(1), work_dir=tmp_path)
    return bronze, out, sb_config(out, bronze)


def test_same_run_id_and_input_is_a_byte_identical_no_op(tmp_path: Path) -> None:
    _bronze, out, cfg = _two_runs(tmp_path)
    assert run_sb(cfg, "s1").status == "COMMITTED"
    before = tree_bytes(out)
    again = run_sb(cfg, "s1", clock=fixed_clock(datetime(2040, 1, 1, tzinfo=UTC)))
    assert again.status == "ALREADY_COMMITTED"
    assert tree_bytes(out) == before


def test_an_older_committed_run_stays_a_no_op_after_later_commits(tmp_path: Path) -> None:
    bronze, _out, cfg = _two_runs(tmp_path)
    run_sb(cfg, "s1")
    ingest_rows(bronze, "b2", {DS: [sample_row(DS, 2)]}, at=t(2), work_dir=tmp_path)
    run_sb(cfg, "s2")
    assert run_sb(cfg, "s1").status == "ALREADY_COMMITTED"  # recomputed against s1's own base
    assert run_sb(cfg, "s2").status == "ALREADY_COMMITTED"


def test_a_retry_of_a_committed_run_ignores_bronze_runs_committed_after_it(tmp_path: Path) -> None:
    bronze, out, cfg = _two_runs(tmp_path)
    run_sb(cfg, "s1")
    before = tree_bytes(out)
    ingest_rows(bronze, "b2", {DS: [sample_row(DS, 2)]}, at=t(2), work_dir=tmp_path)
    assert run_sb(cfg, "s1").status == "ALREADY_COMMITTED"
    assert tree_bytes(out) == before
    assert run_sb(cfg, "s2").manifest["checkpoint"]["applied_bronze_run_ids"] == ["b1", "b2"]


@pytest.mark.parametrize("change", ["config", "selection", "bronze_content"])
def test_same_run_id_with_different_input_is_refused(tmp_path: Path, change: str) -> None:
    bronze, out, cfg = _two_runs(tmp_path)
    run_sb(cfg, "s1")
    before = tree_bytes(out)
    kwargs: dict = {}
    if change == "config":
        cfg = sb_config(out, bronze, max_snapshot_delete_fraction=0.9)
    elif change == "selection":
        kwargs["bronze_run_ids"] = ["b1"]
    else:  # the Bronze store was replaced: run b1 now holds other content
        shutil.rmtree(bronze)
        ingest_rows(bronze, "b1", {DS: [sample_row(DS, 5)]}, at=t(1), work_dir=tmp_path / "other")
    with pytest.raises(SilverBatchRunIdConflictError):
        run_sb(cfg, "s1", **kwargs)
    assert tree_bytes(out) == before  # refusals write nothing


def test_failed_attempts_are_kept_in_previous_attempts(tmp_path: Path) -> None:
    _bronze, out, cfg = _two_runs(tmp_path)

    def boom(stage: str, root: Path) -> None:
        if stage == STAGE_LOADED:
            raise RuntimeError("transient storage error")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            run_sb(cfg, "s1", hook=boom)
    failed = manifest(out, "s1")
    assert failed["status"] == "FAILED" and len(failed["previous_attempts"]) == 1
    m = run_sb(cfg, "s1").manifest
    assert m["status"] == "COMMITTED"
    assert [a["status"] for a in m["previous_attempts"]] == ["FAILED", "FAILED"]
    assert all(a["error"]["type"] == "RuntimeError" for a in m["previous_attempts"])
    assert m["commit_sequence"] == 1


def test_crash_after_publish_before_manifest_then_retry_matches_an_uninterrupted_run(tmp_path: Path) -> None:
    _clean_bronze, clean_out, clean_cfg = _two_runs(tmp_path / "clean")
    run_sb(clean_cfg, "s1")

    _bronze, out, cfg = _two_runs(tmp_path / "crash")

    def crash(stage: str, root: Path) -> None:
        if stage == STAGE_PUBLISHED:
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        run_sb(cfg, "s1", hook=crash)
    # partitions are on disk but nothing is committed: readers see nothing
    assert (out / f"current_{DS}" / "run_id=s1").is_dir()
    assert not (out / "_runs" / "s1.json").exists()
    assert head_manifest(out) is None and read_current_table(out, DS) == []

    m = run_sb(cfg, "s1").manifest
    assert m["removed_uncommitted_paths"] == [f"current_{DS}/run_id=s1"]
    clean, crashed = tree_bytes(clean_out), tree_bytes(out)
    assert set(clean) == set(crashed)
    assert {k: v for k, v in crashed.items() if not k.startswith("_runs")} == {
        k: v for k, v in clean.items() if not k.startswith("_runs")}
    clean_manifest = manifest(clean_out, "s1")
    assert {k: v for k, v in m.items() if k != "removed_uncommitted_paths"} == {
        k: v for k, v in clean_manifest.items() if k != "removed_uncommitted_paths"}


def test_fixed_clock_output_is_byte_identical_across_stores(tmp_path: Path) -> None:
    """Two independent DE-11 -> Bronze -> batch-Silver chains under different
    absolute roots: every batch-Silver file is byte-identical (no paths leak)."""
    trees = []
    for name in ("a", "b"):
        env = make_sb_env(tmp_path / name / "deep" / name, service_days=1)
        env.load(at(2029, 12, 1))
        env.silver("s1")
        env.load(at(2030, 1, 1))
        env.silver("s2")
        trees.append(tree_bytes(env.output_dir))
    assert trees[0] == trees[1] and trees[0]


def test_manifests_hold_no_absolute_paths_home_or_username(tmp_path: Path) -> None:
    env = make_sb_env(tmp_path, service_days=1)
    env.load(at(2030, 1, 1))
    env.silver("s1")

    def fail(stage: str, root: Path) -> None:
        if stage == STAGE_LOADED:
            raise OSError(f"cannot write {root.resolve()}/current_x, {env.bronze_dir.absolute()}/b, "
                          f"{env.inc.state_dir}/extracts and {Path.home()}/tmp")

    with pytest.raises(OSError):
        env.silver("s2", hook=fail)
    failed = manifest(env.output_dir, "s2")
    assert failed["error"]["message"] == (
        "cannot write <output_dir>/current_x, <bronze_dir>/b, <incremental_state_dir>/extracts and ~/tmp")
    forbidden = {str(tmp_path), str(tmp_path.resolve()), str(Path.home()), "/Users/", "/home/"}
    user = getpass.getuser()
    for path in (env.output_dir / "_runs").glob("*.json"):
        text = path.read_text()
        assert not any(f in text for f in forbidden), path.name
        assert user not in text, path.name


def test_every_log_line_carries_the_run_id(tmp_path: Path) -> None:
    records: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("test.silver_batch.capture")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(Capture())
    bronze, _out, cfg = _two_runs(tmp_path)
    ingest_rows(bronze, "b2", {DS: [b"{bad", sample_row(DS, 3, trip_status="X")]}, at=t(2), work_dir=tmp_path)
    run_sb(cfg, "s1", logger=logger)
    run_sb(cfg, "s1", logger=logger)  # no-op path logs too
    ingest_rows(bronze, "b3", {DS: [sample_row(DS, 4)]}, at=t(3), work_dir=tmp_path)

    def fail(stage: str, root: Path) -> None:
        raise RuntimeError("injected")

    with pytest.raises(RuntimeError):
        run_sb(cfg, "s2", logger=logger, hook=fail)
    assert {r.silver_batch_run_id for r in records} == {"s1", "s2"}  # type: ignore[attr-defined]
    messages = [r.getMessage() for r in records]
    assert "row quarantined" in messages and "batch-silver run failed; nothing committed" in messages
    assert "batch-silver run already committed with identical input; no-op" in messages
    assert all(hasattr(r, "silver_batch_run_id") for r in records)
