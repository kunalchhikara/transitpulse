"""Control (watermark) store and immutable audit store for the DE-11 loader.

Layout under `state_dir`::

    control/<dataset>.json                         one control record per dataset
    audit/<pipeline_run_id>/<dataset>/attempt-NNN.json
                                                   one immutable record per attempt
    audit/<pipeline_run_id>/pipeline.json          immutable pipeline summary
    extracts/<dataset>/<bronze_run_id>/            DE-04-shaped extract given to Bronze

* Control records are replaced atomically (temp file + `os.replace`), one
  file per dataset, so committing one dataset's watermark can never touch
  another's (Lead decision 4: per-dataset commit and watermark advance).
* Audit records are written once: temp file + hard link to the final name,
  which fails if the name exists, so a record can never be overwritten or
  appended to; a crash mid-write leaves only a dot-prefixed temp file.
* Nothing in either store holds an absolute path: paths are relative to
  `state_dir` and error text is passed through `PathSanitizer`.

Single-writer assumption: one loader process per `state_dir` at a time (no
file locking), like the Bronze store it writes to.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

CONTROL_DIR_NAME = "control"
AUDIT_DIR_NAME = "audit"
EXTRACTS_DIR_NAME = "extracts"
PIPELINE_SUMMARY_FILE = "pipeline.json"
#: 2.0.0: `watermark` is the maximum committed `updated_at` (was the window
#: upper bound in 1.0.0) and `last_as_of` is recorded separately. A 1.0.0
#: record is refused, never reinterpreted (`ControlSchemaError`).
CONTROL_SCHEMA_VERSION = "2.0.0"
#: 2.0.0: adds `last_as_of_before/after`, `late_rows_recovered`,
#: `late_rows_skipped`, `window_max_watermark`, `preserved_bad_rows`; the
#: pipeline summary adds `late_rows_skipped` and `warnings`.
AUDIT_SCHEMA_VERSION = "2.0.0"
PIPELINE_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")
_AUTO_PIPELINE_RUN_ID = re.compile(r"^ipr-(\d{6})-")


class StateError(Exception):
    """Control/audit store misuse (e.g. an existing audit record or pipeline run id)."""


class ControlSchemaError(StateError):
    """A control record written by another control schema version. Refused
    before anything is written: a 1.0.0 watermark (window upper bound) read
    as a 2.0.0 watermark (max committed `updated_at`) would skip rows."""


def dump_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def atomic_replace(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def write_immutable(path: Path, text: str) -> None:
    """Create `path` with `text`; refuse (StateError) if it already exists.
    Atomic: readers see either no file or the complete file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        os.link(tmp, path)
    except FileExistsError as exc:
        raise StateError(f"refusing to overwrite immutable record {path.name}") from exc
    finally:
        tmp.unlink()


class PathSanitizer:
    """Replace known absolute local paths in free text (error messages) with
    stable labels, so audit records never carry a local path (FU-04)."""

    def __init__(self, labelled: dict[str, Path | str]) -> None:
        pairs: list[tuple[str, str]] = []
        for label, path in labelled.items():
            text = str(path)
            if not text:
                continue
            absolute = str(Path(text).expanduser().resolve())
            pairs.append((absolute, f"<{label}>"))
            if text != absolute and os.path.isabs(text):
                pairs.append((text, f"<{label}>"))
        home = str(Path.home())
        pairs.append((home, "~"))
        # Longest first, so a nested path wins over its parent.
        self._pairs = sorted(set(pairs), key=lambda pair: len(pair[0]), reverse=True)

    def __call__(self, text: str) -> str:
        for absolute, label in self._pairs:
            text = text.replace(absolute, label)
        return text


class ControlStore:
    """Per-dataset control records (the local watermark control table)."""

    def __init__(self, state_dir: Path) -> None:
        self.root = Path(state_dir) / CONTROL_DIR_NAME

    def path(self, dataset: str) -> Path:
        return self.root / f"{dataset}.json"

    def read(self, dataset: str) -> dict[str, Any] | None:
        path = self.path(dataset)
        if not path.exists():
            return None
        record = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(record, dict) or record.get("dataset") != dataset:
            raise StateError(f"control record for {dataset!r} has an unexpected shape")
        version = record.get("control_schema_version")
        if version != CONTROL_SCHEMA_VERSION:
            raise ControlSchemaError(
                f"control record for {dataset!r} has control_schema_version {version!r} "
                f"(watermark_semantics {record.get('watermark_semantics')!r}); this loader reads only "
                f"{CONTROL_SCHEMA_VERSION!r} (watermark = maximum committed updated_at, last_as_of kept "
                "separately). An older record is never reinterpreted: reset the series (delete "
                "<state_dir>/control/ and use a fresh Bronze store) and reload"
            )
        return record

    def write(self, record: dict[str, Any]) -> None:
        atomic_replace(self.path(record["dataset"]), dump_json(record))


class AuditStore:
    """Immutable per-attempt audit records grouped by pipeline run."""

    def __init__(self, state_dir: Path) -> None:
        self.root = Path(state_dir) / AUDIT_DIR_NAME

    def _existing_runs(self) -> list[str]:
        if not self.root.is_dir():
            return []
        return sorted(p.name for p in self.root.iterdir() if p.is_dir())

    def next_pipeline_run_id(self, as_of_compact: str) -> str:
        """Deterministic id `ipr-<seq>-<as_of>`: seq = 1 + highest existing
        auto-allocated sequence number in this state store."""
        seqs = [int(m.group(1)) for name in self._existing_runs() if (m := _AUTO_PIPELINE_RUN_ID.match(name))]
        return f"ipr-{(max(seqs) + 1) if seqs else 1:06d}-{as_of_compact}"

    def claim_pipeline_run(self, pipeline_run_id: str) -> Path:
        if not PIPELINE_RUN_ID_PATTERN.fullmatch(pipeline_run_id):
            raise StateError(
                f"invalid pipeline run id {pipeline_run_id!r}; must match {PIPELINE_RUN_ID_PATTERN.pattern}"
            )
        self.root.mkdir(parents=True, exist_ok=True)
        run_dir = self.root / pipeline_run_id
        try:
            run_dir.mkdir()
        except FileExistsError as exc:
            raise StateError(f"pipeline run id {pipeline_run_id!r} already exists in the audit store") from exc
        return run_dir

    def attempt_path(self, pipeline_run_id: str, dataset: str, attempt: int) -> Path:
        return self.root / pipeline_run_id / dataset / f"attempt-{attempt:03d}.json"

    def write_attempt(self, record: dict[str, Any]) -> Path:
        path = self.attempt_path(record["pipeline_run_id"], record["dataset"], record["attempt"])
        write_immutable(path, dump_json(record))
        return path

    def write_pipeline_summary(self, summary: dict[str, Any]) -> Path:
        path = self.root / summary["pipeline_run_id"] / PIPELINE_SUMMARY_FILE
        write_immutable(path, dump_json(summary))
        return path


def extract_dir(state_dir: Path, dataset: str, bronze_run_id: str) -> Path:
    return Path(state_dir) / EXTRACTS_DIR_NAME / dataset / bronze_run_id
