"""Read committed DE-05 Bronze batch runs for batch Silver - only through
committed run manifests, never by globbing partition directories.

After a DE-11 crash recovered at a later `as_of`, an abandoned run's
uncommitted `run_id=<id>` partition can stay on disk (QA of PR #17, O-1).
Only a `COMMITTED` manifest makes a partition visible: run selection and
order come from `BronzeStore.committed_manifests()` (Bronze commit order:
`ingestion_time`, then `run_id`), and each data file is located through the
manifest's `tables[<table>].path` and verified against it (sha256, line
count = `records_written`, `run_id` on every record, increasing
`_bronze_line_number`) before any row is used.

DE-11 lineage: for a Bronze run written by the DE-11 loader, the extract
manifest `<incremental_state_dir>/extracts/<dataset>/<run_id>/manifest.json`
lists every bad source line it carried (`preserved_bad_rows`, Lead decision
D2). It is matched to the Bronze run by run id AND by the sha256 of the
extract file Bronze ingested, so a stale or foreign listing is never applied.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from simulator.bronze.records import (
    LINE_NUMBER,
    PARSE_ERROR,
    RUN_ID,
    SOURCE_FILE,
    STORED_RECORD_METADATA_FIELDS,
    strip_metadata,
)
from simulator.bronze.store import BronzeStore
from simulator.silver_batch.errors import BronzeContractError

MODE_BATCH = "batch"
EXTRACTS_DIR = "extracts"
EXTRACT_MANIFEST_FILE = "manifest.json"
#: DE-11 `bronze_outcome` values (simulator.incremental.extract).
OUTCOME_STORED_REQUIRES_QUARANTINE = "stored_record_requires_quarantine"
OUTCOME_BRONZE_PARSE_ERROR = "bronze_parse_error_record"

#: `de11_extract.status` values recorded per applied table run.
EXTRACT_FOUND = "FOUND"
EXTRACT_NOT_FOUND = "NOT_FOUND"  # not a DE-11 run id, or lookup disabled
EXTRACT_LOOKUP_DISABLED = "LOOKUP_DISABLED"


@dataclass(frozen=True)
class De11Listing:
    """What DE-11 says about one extract (absent -> status NOT_FOUND/DISABLED)."""

    status: str
    mode: str | None = None  # "full" | "incremental"
    window_high: str | None = None  # the extract's as_of
    rows: dict[int, dict[str, Any]] = field(default_factory=dict)  # extract_line -> listing entry
    manifest_sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "window_high": self.window_high,
            "preserved_bad_rows": len(self.rows),
            "stored_records_requiring_quarantine": sum(
                1 for entry in self.rows.values() if entry.get("bronze_outcome") == OUTCOME_STORED_REQUIRES_QUARANTINE
            ),
            "extract_manifest_sha256": self.manifest_sha256,
        }


@dataclass(frozen=True)
class BronzeTableRun:
    """One table of one committed Bronze batch run."""

    run_id: str
    table: str
    ingestion_time: str
    path: str  # relative to the Bronze root, as in the manifest
    sha256: str
    records: int
    parse_error_records: int
    input_sha256: tuple[str, ...]  # sha256 of the input file(s) Bronze ingested into this table
    replay_of: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "path": self.path,
            "sha256": self.sha256,
            "records": self.records,
            "parse_error_records": self.parse_error_records,
            "input_sha256": list(self.input_sha256),
        }


@dataclass(frozen=True)
class BronzeBatchRun:
    run_id: str
    ingestion_time: str
    replay_of: tuple[str, ...]
    tables: tuple[BronzeTableRun, ...]


def committed_batch_runs(store: BronzeStore) -> list[BronzeBatchRun]:
    """Every committed Bronze run of mode `batch`, in Bronze commit order."""
    runs = []
    for manifest in store.committed_manifests():
        if manifest.get("mode") != MODE_BATCH:
            continue
        inputs: dict[str, list[str]] = {}
        for entry in manifest.get("input_files", []):
            inputs.setdefault(entry["table"], []).append(entry["sha256"])
        tables = tuple(
            BronzeTableRun(
                run_id=manifest["run_id"],
                table=table,
                ingestion_time=manifest["ingestion_time"],
                path=info["path"],
                sha256=info["sha256"],
                records=info["records_written"],
                parse_error_records=info.get("parse_error_records", 0),
                input_sha256=tuple(inputs.get(table, [])),
                replay_of=tuple(manifest.get("replay_of", [])),
            )
            for table, info in sorted(manifest.get("tables", {}).items())
        )
        runs.append(
            BronzeBatchRun(manifest["run_id"], manifest["ingestion_time"], tuple(manifest.get("replay_of", [])), tables)
        )
    return runs


@dataclass(frozen=True)
class BronzeRow:
    record: dict[str, Any]
    #: exact text of the original input object (Bronze line minus the spliced
    #: metadata) for a stored record; None for a parse-error record.
    original_text: str | None

    @property
    def is_parse_error(self) -> bool:
        return PARSE_ERROR in self.record

    @property
    def line_number(self) -> int:
        return self.record[LINE_NUMBER]


def original_object_text(line: str, record: dict[str, Any]) -> str:
    """Recover the exact original object text of a stored Bronze line (DE-05
    splices `,<metadata>` before the closing brace) and verify it parses back
    to the record minus metadata."""
    metadata = {name: record[name] for name in STORED_RECORD_METADATA_FIELDS}
    fragment = json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))[1:-1]
    original = strip_metadata(record)
    suffix = ("," if original else "") + fragment + "}"
    if not line.endswith(suffix):
        raise BronzeContractError("Bronze line does not end with its metadata fragment")
    text = line[: -len(suffix)] + "}"
    if json.dumps(json.loads(text), separators=(",", ":")) != json.dumps(original, separators=(",", ":")):
        raise BronzeContractError("recovered original text does not match the Bronze record")
    return text


def iter_table_rows(store: BronzeStore, run: BronzeTableRun) -> Iterator[BronzeRow]:
    """Every record of one committed table run, in physical line order,
    after verifying the data file against the committed manifest."""
    path = store.root / run.path
    if not path.is_file():
        raise BronzeContractError(f"committed Bronze run {run.run_id!r}: data file {run.path} is missing")
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != run.sha256:
        raise BronzeContractError(f"committed Bronze run {run.run_id!r}: {run.path} sha256 differs from its manifest")
    lines = data.decode("utf-8").split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    if len(lines) != run.records:
        raise BronzeContractError(
            f"committed Bronze run {run.run_id!r}: {len(lines)} lines != manifest records_written {run.records}"
        )
    previous: dict[str, int] = {}
    for line in lines:
        record = json.loads(line)
        if record.get(RUN_ID) != run.run_id:
            raise BronzeContractError(f"record run_id {record.get(RUN_ID)!r} in a partition of run {run.run_id!r}")
        source_file, number = record[SOURCE_FILE], record[LINE_NUMBER]
        if number <= previous.get(source_file, 0):
            raise BronzeContractError(f"Bronze run {run.run_id!r}: {source_file} line numbers not increasing")
        previous[source_file] = number
        text = None if PARSE_ERROR in record else original_object_text(line, record)
        yield BronzeRow(record=record, original_text=text)


def is_de11_run(run_id: str, prefixes: Sequence[str]) -> bool:
    return any(run_id.startswith(prefix) for prefix in prefixes)


def load_de11_listing(
    state_dir: Path | None, run: BronzeTableRun, prefixes: Sequence[str]
) -> De11Listing:
    """The DE-11 `preserved_bad_rows` listing of the extract Bronze ingested
    as `run`. Required (BronzeContractError if absent or inconsistent) for a
    run id with a DE-11 prefix when `state_dir` is configured."""
    if state_dir is None:
        return De11Listing(EXTRACT_LOOKUP_DISABLED)
    if not is_de11_run(run.run_id, prefixes):
        return De11Listing(EXTRACT_NOT_FOUND)
    path = state_dir / EXTRACTS_DIR / run.table / run.run_id / EXTRACT_MANIFEST_FILE
    if not path.is_file():
        raise BronzeContractError(
            f"Bronze run {run.run_id!r} has a DE-11 run id but its extract manifest "
            f"extracts/{run.table}/{run.run_id}/{EXTRACT_MANIFEST_FILE} is missing from the incremental state dir; "
            "the preserved_bad_rows listing cannot be checked (point incremental_state_dir at the DE-11 state dir)"
        )
    raw = path.read_bytes()
    try:
        manifest = json.loads(raw.decode("utf-8"))
        entry = manifest["datasets"][run.table]
        listed = entry.get("preserved_bad_rows", [])
    except (ValueError, KeyError, TypeError) as exc:
        raise BronzeContractError(f"unreadable DE-11 extract manifest for {run.run_id!r}: {type(exc).__name__}") from exc
    if manifest.get("bronze_run_id") != run.run_id:
        raise BronzeContractError(f"DE-11 extract manifest of {run.run_id!r} names run {manifest.get('bronze_run_id')!r}")
    if (entry.get("sha256"),) != run.input_sha256:
        raise BronzeContractError(
            f"DE-11 extract of {run.run_id!r} does not match the file Bronze ingested (sha256 differs)"
        )
    rows: dict[int, dict[str, Any]] = {}
    for item in listed:
        rows[int(item["extract_line"])] = dict(item)
    window = manifest.get("window") or {}
    return De11Listing(
        status=EXTRACT_FOUND,
        mode=manifest.get("mode"),
        window_high=window.get("high"),
        rows=rows,
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
    )
