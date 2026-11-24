"""Read the DE-05 Bronze telemetry table for Silver - only through committed
run manifests (BACKLOG DE-06 "Bronze read contract").

Silver never globs Bronze directories: a crashed Bronze run can leave an
orphaned `run_id=<id>` partition on disk until that run_id is retried, and
only a `COMMITTED` manifest makes a partition visible. Run selection and order
come from `BronzeStore.committed_manifests()` (ordered by `ingestion_time`,
`run_id`); each data file is located through the manifest's
`tables[<table>].path` (exactly what `BronzeStore.committed_partition_files`
does, but keeping the manifest so a subset of runs can be selected and the
file's sha256 / record count verified against it).

Arrival order = Bronze run order, then physical line order inside the run's
data file (which is input-file order, then `_bronze_line_number`).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from simulator.bronze.records import (
    INGESTION_TIME,
    LINE_NUMBER,
    PARSE_ERROR,
    RUN_ID,
    SOURCE,
    SOURCE_FILE,
    STORED_RECORD_METADATA_FIELDS,
    strip_metadata,
)
from simulator.bronze.store import BronzeStore
from simulator.silver.errors import SilverInputError


@dataclass(frozen=True)
class BronzeRun:
    """One committed Bronze run that holds the telemetry table."""

    run_id: str
    ingestion_time: str
    path: str  # data file, relative to the Bronze root (as in the manifest)
    sha256: str
    records: int
    replay_of: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "ingestion_time": self.ingestion_time,
            "path": self.path,
            "sha256": self.sha256,
            "records": self.records,
            "replay_of": list(self.replay_of),
        }


@dataclass(frozen=True)
class BronzeRow:
    """One Bronze record, in arrival order."""

    run: BronzeRun
    record: dict[str, Any]
    #: Exact text of the original input object for a stored record (the
    #: Bronze line minus the spliced metadata); None for a parse-error record.
    original_text: str | None

    @property
    def is_parse_error(self) -> bool:
        return PARSE_ERROR in self.record

    def lineage(self) -> dict[str, Any]:
        return {
            INGESTION_TIME: self.record[INGESTION_TIME],
            SOURCE: self.record[SOURCE],
            "bronze_run_id": self.record[RUN_ID],
            SOURCE_FILE: self.record[SOURCE_FILE],
            LINE_NUMBER: self.record[LINE_NUMBER],
        }


def select_runs(store: BronzeStore, table: str, run_ids: Sequence[str] | None = None) -> list[BronzeRun]:
    """Committed runs holding `table`, in Bronze commit order. With `run_ids`,
    exactly those runs (each must be committed and hold the table); the
    order is still Bronze commit order, never the order given."""
    committed = [m for m in store.committed_manifests() if table in m.get("tables", {})]
    runs = [
        BronzeRun(
            run_id=m["run_id"],
            ingestion_time=m["ingestion_time"],
            path=m["tables"][table]["path"],
            sha256=m["tables"][table]["sha256"],
            records=m["tables"][table]["records_written"],
            replay_of=tuple(m.get("replay_of", [])),
        )
        for m in committed
    ]
    if run_ids is None:
        if not runs:
            raise SilverInputError(f"no committed Bronze run holds table {table!r} in {store.root.name!r}")
        return runs
    wanted = list(dict.fromkeys(run_ids))
    available = {run.run_id for run in runs}
    unknown = [rid for rid in wanted if rid not in available]
    if unknown:
        raise SilverInputError(
            f"Bronze run(s) {unknown} are not committed or do not hold table {table!r}; "
            f"committed runs with that table: {sorted(available)}"
        )
    return [run for run in runs if run.run_id in set(wanted)]


def original_object_text(line: str, record: dict[str, Any]) -> str:
    """Recover the exact text of the original input object from a stored
    Bronze line. DE-05 splices `,<metadata>` before the closing brace (no
    comma for an empty object), serialising the metadata compactly with
    `ensure_ascii=False`; we rebuild that suffix from the record's own
    metadata values and strip it, then verify the result parses back to the
    record minus metadata. Any mismatch is a Bronze-contract violation and
    raises rather than guessing."""
    metadata = {field: record[field] for field in STORED_RECORD_METADATA_FIELDS}
    fragment = json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))[1:-1]
    original = strip_metadata(record)
    suffix = ("," if original else "") + fragment + "}"
    if not line.endswith(suffix):
        raise SilverInputError(f"Bronze line does not end with its metadata fragment: {line[-200:]!r}")
    text = line[: -len(suffix)] + "}"
    reparsed = json.loads(text)
    if json.dumps(reparsed, separators=(",", ":")) != json.dumps(original, separators=(",", ":")):
        raise SilverInputError("recovered original text does not match the Bronze record")
    return text


def iter_rows(store: BronzeStore, runs: Sequence[BronzeRun]) -> Iterator[BronzeRow]:
    """Yield every record of `runs` in arrival order, verifying each data
    file against its committed manifest first (sha256 and record count)."""
    for run in runs:
        path = store.root / run.path
        if not path.is_file():
            raise SilverInputError(f"committed Bronze run {run.run_id!r}: data file {run.path} is missing")
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if digest != run.sha256:
            raise SilverInputError(
                f"committed Bronze run {run.run_id!r}: {run.path} sha256 {digest} != manifest {run.sha256}"
            )
        lines = data.decode("utf-8").split("\n")
        if lines and lines[-1] == "":
            lines.pop()
        if len(lines) != run.records:
            raise SilverInputError(
                f"committed Bronze run {run.run_id!r}: {len(lines)} lines != manifest records_written {run.records}"
            )
        previous: dict[str, int] = {}
        for line in lines:
            record = json.loads(line)
            if record.get(RUN_ID) != run.run_id:
                raise SilverInputError(f"record run_id {record.get(RUN_ID)!r} in partition of run {run.run_id!r}")
            source_file, number = record[SOURCE_FILE], record[LINE_NUMBER]
            if number <= previous.get(source_file, 0):
                raise SilverInputError(
                    f"Bronze run {run.run_id!r}: {source_file} line numbers not increasing ({number})"
                )
            previous[source_file] = number
            text = None if PARSE_ERROR in record else original_object_text(line, record)
            yield BronzeRow(run=run, record=record, original_text=text)
