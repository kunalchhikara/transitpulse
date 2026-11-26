"""Batch-Silver store layout, checkpoint model and the read contract for
downstream consumers (DE-13 Warehouse).

Layout under `output_dir` (default `simulator/_out/silver_batch`)::

    current_<dataset>/run_id=<id>/part-00000.jsonl     full current-state table as of run <id>
                                                       (written only when the table changed)
    quarantine_<dataset>/run_id=<id>/part-00000.jsonl  rows rejected by run <id> (raw payload kept)
    stale_<dataset>/run_id=<id>/part-00000.jsonl       rows excluded as stale by run <id> (audited)
    _runs/<id>.json                                    run manifest: audit record AND commit marker
    _staging/<id>/...                                  in-flight run (never read)

Commit protocol: DE-05's layer-agnostic `BronzeStore` primitives, by import
(staging directory, atomic rename, manifest written last). **A run is
committed iff `_runs/<id>.json` says `COMMITTED`.**

Checkpoint model: every committed manifest carries a `commit_sequence`
(base + 1) and a cumulative `checkpoint`: the applied Bronze run ids and,
per dataset, the path/sha256/row counts of its current-state table. The
**head** is the committed manifest with the highest `commit_sequence`; it
is the checkpoint the next run starts from. There is no separate pointer
file that could disagree with the manifests.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from simulator.bronze.store import RUNS_DIR_NAME, STATUS_COMMITTED, BronzeStore
from simulator.silver_batch.datasets import SILVER_DATASETS
from simulator.silver_batch.errors import SilverBatchError, SilverBatchStateError

#: Columns Silver adds to every current-state row, after the model fields.
#: `_silver_row_hash` (sha256 of the standardised business payload) and
#: `_silver_is_deleted` describe the row; the rest is lineage of the run and
#: Bronze record that last changed it.
SILVER_STATE_COLUMNS: tuple[str, ...] = ("_silver_row_hash", "_silver_is_deleted")
LINEAGE_COLUMNS: tuple[str, ...] = (
    "_silver_deleted_by_run_id",
    "_silver_batch_run_id",
    "_silver_processing_time",
    "_bronze_run_id",
    "_bronze_ingestion_time",
    "_bronze_source",
    "_bronze_source_file",
    "_bronze_line_number",
)
SILVER_COLUMNS: tuple[str, ...] = (*SILVER_STATE_COLUMNS, *LINEAGE_COLUMNS)


class SilverBatchStore(BronzeStore):
    """`BronzeStore` primitives (staging, publish, manifests) on the batch
    Silver root, plus head/checkpoint discovery."""

    def committed_manifests(self) -> list[dict[str, Any]]:  # type: ignore[override]
        """Every committed batch-Silver manifest, by `commit_sequence`."""
        if not self.runs_dir.is_dir():
            return []
        manifests = []
        for path in sorted(self.runs_dir.glob("*.json")):
            manifest = json.loads(path.read_text(encoding="utf-8"))
            if manifest.get("status") == STATUS_COMMITTED:
                manifests.append(manifest)
        manifests.sort(key=lambda m: (m["commit_sequence"], m["run_id"]))
        sequences = [m["commit_sequence"] for m in manifests]
        duplicated = sorted({s for s in sequences if sequences.count(s) > 1})
        if duplicated:
            raise SilverBatchStateError(
                f"several committed batch-Silver runs claim commit_sequence {duplicated} "
                "(concurrent writers? one writer per store is required)"
            )
        return manifests

    def head(self) -> dict[str, Any] | None:
        manifests = self.committed_manifests()
        return manifests[-1] if manifests else None

    def committed(self, run_id: str) -> dict[str, Any]:
        manifest = self.read_manifest(run_id)
        if manifest is None or manifest.get("status") != STATUS_COMMITTED:
            raise SilverBatchStateError(f"batch-Silver run {run_id!r} is not committed")
        return manifest

    def read_table_file(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """Rows of a committed current-state table entry
        (`{path, sha256, rows, tombstones}`), verified against it."""
        path = self.root / entry["path"]
        if not path.is_file():
            raise SilverBatchStateError(f"committed table file {entry['path']} is missing")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise SilverBatchStateError(f"committed table file {entry['path']} sha256 differs from its manifest")
        rows = [json.loads(line) for line in data.decode("utf-8").splitlines()]
        if len(rows) != entry["rows"] + entry["tombstones"]:
            raise SilverBatchStateError(
                f"committed table file {entry['path']}: {len(rows)} lines != rows + tombstones in the manifest"
            )
        return rows


# -- read contract for consumers (DE-13) ---------------------------------------


def head_manifest(output_dir: str | Path) -> dict[str, Any] | None:
    """The latest committed batch-Silver manifest (highest commit_sequence),
    or None if nothing is committed. Uncommitted/FAILED runs and orphaned
    partitions are never visible."""
    return SilverBatchStore(output_dir).head()


def read_current_table(
    output_dir: str | Path,
    dataset: str,
    *,
    run_id: str | None = None,
    include_deleted: bool = False,
) -> list[dict[str, Any]]:
    """The current-state rows of `dataset` as of the committed run `run_id`
    (default: the head), sorted by primary key, verified against the
    manifest (sha256 and row counts). Soft-deleted (tombstoned) reference
    rows are excluded unless `include_deleted`. A dataset never loaded
    returns []. Only committed manifests are read - never by globbing."""
    if dataset not in SILVER_DATASETS:
        raise SilverBatchError(f"unknown batch-Silver dataset {dataset!r}")
    store = SilverBatchStore(output_dir)
    manifest = store.committed(run_id) if run_id is not None else store.head()
    if manifest is None:
        return []
    entry = manifest["checkpoint"]["tables"].get(dataset)
    if entry is None:
        return []
    rows = store.read_table_file(entry)
    return rows if include_deleted else [row for row in rows if not row["_silver_is_deleted"]]


__all__ = [
    "LINEAGE_COLUMNS",
    "RUNS_DIR_NAME",
    "SILVER_COLUMNS",
    "SILVER_STATE_COLUMNS",
    "SilverBatchStore",
    "head_manifest",
    "read_current_table",
]
