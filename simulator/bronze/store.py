"""Local Bronze store layout, atomic commit protocol and run manifests (DE-05).

Layout under `output_dir`::

    <table>/run_id=<run_id>/part-00000.jsonl   one data file per table per run
    _runs/<run_id>.json                         run manifest (audit + commit marker)
    _staging/<run_id>/<table>/part-00000.jsonl  in-flight run (never read)

Commit protocol (the local stand-in for a Delta transaction):

1. Records are written to `_staging/<run_id>/`.
2. Each staged table directory is moved into place with `os.replace`
   (atomic rename on the same filesystem).
3. The run manifest is written last (temp file + `os.replace`) with
   `status = "COMMITTED"`. **A run is committed iff its manifest says so.**
   Readers (`read_table`, Silver/DE-06) only read partitions of committed
   runs, so a crash between steps leaves nothing visible; the next attempt
   with the same `run_id` removes the orphaned staging/partition directories
   before writing again.

Single-writer assumption: one ingestion process per `output_dir` at a time
(no file locking). See README "Known limitations (DE-05)".
"""

from __future__ import annotations

import json
import os
import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

RUNS_DIR_NAME = "_runs"
STAGING_DIR_NAME = "_staging"
PART_FILE_NAME = "part-00000.jsonl"
STATUS_COMMITTED = "COMMITTED"
STATUS_FAILED = "FAILED"

#: run_id becomes a directory/file name, so it is restricted to a safe
#: alphabet: no path separators, no leading dot (no `.`/`..`/hidden names).
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


class InvalidRunIdError(ValueError):
    """run_id is not a safe identifier (see `RUN_ID_PATTERN`)."""


def validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise InvalidRunIdError(f"invalid run_id {run_id!r}; must match {RUN_ID_PATTERN.pattern}")
    return run_id


def partition_dir_name(run_id: str) -> str:
    return f"run_id={run_id}"


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def dump_manifest(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


class BronzeStore:
    """Filesystem operations on one local Bronze store root."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    # -- paths ---------------------------------------------------------

    @property
    def runs_dir(self) -> Path:
        return self.root / RUNS_DIR_NAME

    def manifest_path(self, run_id: str) -> Path:
        return self.runs_dir / f"{validate_run_id(run_id)}.json"

    def staging_dir(self, run_id: str) -> Path:
        return self.root / STAGING_DIR_NAME / validate_run_id(run_id)

    def partition_dir(self, table: str, run_id: str) -> Path:
        return self.root / table / partition_dir_name(validate_run_id(run_id))

    def partition_file(self, table: str, run_id: str) -> Path:
        return self.partition_dir(table, run_id) / PART_FILE_NAME

    # -- manifests -----------------------------------------------------

    def read_manifest(self, run_id: str) -> dict[str, Any] | None:
        path = self.manifest_path(run_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def write_manifest(self, manifest: dict[str, Any]) -> Path:
        path = self.manifest_path(manifest["run_id"])
        _atomic_write_text(path, dump_manifest(manifest))
        return path

    def committed_manifests(self) -> list[dict[str, Any]]:
        """Every committed run manifest, ordered by (ingestion_time, run_id)."""
        if not self.runs_dir.is_dir():
            return []
        manifests = []
        for path in sorted(self.runs_dir.glob("*.json")):
            manifest = json.loads(path.read_text(encoding="utf-8"))
            if manifest.get("status") == STATUS_COMMITTED:
                manifests.append(manifest)
        manifests.sort(key=lambda m: (m["ingestion_time"], m["run_id"]))
        return manifests

    # -- staging / commit ---------------------------------------------

    def reset_uncommitted(self, run_id: str) -> list[str]:
        """Remove any staging directory and orphaned (uncommitted) partition
        directories left by an earlier failed/crashed attempt of `run_id`.
        Must only be called when `run_id` has no committed manifest. Returns
        the removed paths (relative to the store root) for the audit log."""
        removed: list[str] = []
        staging = self.staging_dir(run_id)
        if staging.exists():
            shutil.rmtree(staging)
            removed.append(staging.relative_to(self.root).as_posix())
        if self.root.is_dir():
            for table_dir in sorted(p for p in self.root.iterdir() if p.is_dir()):
                if table_dir.name in (RUNS_DIR_NAME, STAGING_DIR_NAME):
                    continue
                orphan = table_dir / partition_dir_name(run_id)
                if orphan.exists():
                    shutil.rmtree(orphan)
                    removed.append(orphan.relative_to(self.root).as_posix())
        return removed

    def publish_table(self, table: str, run_id: str) -> None:
        """Move one staged table directory into its final partition."""
        target = self.partition_dir(table, run_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self.staging_dir(run_id) / table, target)

    def discard_staging(self, run_id: str) -> None:
        staging = self.staging_dir(run_id)
        if staging.exists():
            shutil.rmtree(staging)
        staging_root = self.root / STAGING_DIR_NAME
        if staging_root.is_dir() and not any(staging_root.iterdir()):
            staging_root.rmdir()

    # -- readers -------------------------------------------------------

    def committed_partition_files(self, table: str) -> list[Path]:
        """Data files of `table` from committed runs only, in commit order.
        Orphaned partitions of uncommitted runs are never returned."""
        files = []
        for manifest in self.committed_manifests():
            if table in manifest.get("tables", {}):
                files.append(self.root / manifest["tables"][table]["path"])
        return files

    def read_table(self, table: str) -> Iterator[dict[str, Any]]:
        """Yield every Bronze record of `table` from committed runs."""
        for path in self.committed_partition_files(table):
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    yield json.loads(line)
