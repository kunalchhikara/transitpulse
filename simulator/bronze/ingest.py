"""DE-05 local Bronze ingestion: streaming-shaped (DE-02/DE-03 NDJSON) and
batch-shaped (DE-04 dataset directory) inputs into the local Bronze store.

Bronze does **not** validate business rules, enforce schemas, deduplicate or
quarantine (that is Silver, DE-06). It stores every input line as exactly one
record - the original object plus ingestion metadata, or a marked
parse-error record holding the raw line (see `simulator/bronze/records.py`) -
and writes an audit manifest per run whose accounting identity is checked
before commit.

Idempotency / replay policy (see README "DE-05" for the full table):

* same `run_id`, same input, already committed  -> no-op (`ALREADY_COMMITTED`);
  the committed output is left byte-identical.
* same `run_id`, different input, already committed -> `RunIdConflictError`;
  a committed run is never overwritten.
* same `run_id`, previous attempt FAILED / crashed -> retried from scratch
  (orphaned staging/partitions removed first); every earlier FAILED attempt
  is kept in the manifest's `previous_attempts` (DE-09, BACKLOG FU-03).
* new `run_id`, an input file already ingested by a committed run ->
  `DuplicateInputError`, unless `allow_replay=True`, in which case the run is
  committed and flagged (`replay_of`) in its manifest.
* the same content twice within one run -> `DuplicateInputError`, unless
  `allow_replay=True` (flagged in `duplicate_input_files`).
* new `run_id`, new input -> appended as a new run partition.

Path policy (DE-09, BACKLOG FU-04): manifests never record local absolute
paths. `input_files[].path` is the file name relative to its input directory
(the basename; `-` for stdin), declared as `input_path_policy`; store paths
are relative to the Bronze root; absolute store/input roots and the home
directory are replaced by placeholders in persisted error messages.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

from simulator.bronze.config import (
    TABLE_NAME_PATTERN,
    BronzeConfig,
    ParseErrorPolicy,
    load_bronze_config,
)
from simulator.bronze.records import (
    PARSE_ERROR_CODES,
    RecordMetadata,
    build_bronze_line,
)
from simulator.bronze.store import (
    PART_FILE_NAME,
    STATUS_COMMITTED,
    STATUS_FAILED,
    BronzeStore,
    validate_run_id,
)

LOGGER_NAME = "simulator.bronze"
#: 1.1.0 (DE-09): adds `previous_attempts` and `input_path_policy`;
#: `input_files[].path` is the basename. 1.0.0 manifests remain readable.
BRONZE_MANIFEST_VERSION = "1.1.0"
#: `input_files[].path` is relative to the file's input directory, i.e. its
#: basename (`-` for stdin). Directory locations are never recorded.
INPUT_PATH_POLICY = "basename"
#: Fields copied from a FAILED manifest into the retry's `previous_attempts`:
#: the keys Silver/Gold record, plus status, duration and counts (`totals` is
#: null when the attempt failed before counting finished).
PREVIOUS_ATTEMPT_KEYS = (
    "status", "started_at", "ended_at", "duration_seconds", "input_fingerprint", "error", "totals",
)
BRONZE_ROOT_PLACEHOLDER = "<bronze_root>"
INPUT_DIR_PLACEHOLDER = "<input_dir>"
HOME_PLACEHOLDER = "~"
STATUS_ALREADY_COMMITTED = "ALREADY_COMMITTED"
STDIN_NAME = "<stdin>"
MODE_STREAM = "stream"
MODE_BATCH = "batch"

ACCOUNTING_IDENTITY = (
    "lines_read == records_read + blank_lines_skipped; "
    "records_read == records_written; "
    "records_written == json_object_records + parse_error_records"
)

#: `Clock` returns the current time as a timezone-aware datetime. Injected so
#: tests and deliberate replays are deterministic; real runs use the wall clock.
Clock = Callable[[], datetime]


def system_clock() -> datetime:
    return datetime.now(UTC)


def fixed_clock(at: datetime) -> Clock:
    """A clock that always returns `at` (for tests / reproducible reruns)."""
    _require_aware(at)
    return lambda: at


def _require_aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"clock must return a timezone-aware datetime, got {value!r}")
    return value.astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    return _require_aware(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


# -- errors ------------------------------------------------------------------


class BronzeError(Exception):
    """Base class for every loud Bronze ingestion failure."""


class InputError(BronzeError):
    """Input files/directory missing or unusable (raised before a run starts)."""


class RunIdConflictError(BronzeError):
    """run_id already committed with different input; never overwritten."""


class DuplicateInputError(BronzeError):
    """Input already ingested by a committed run and replay was not allowed."""


class MalformedInputError(BronzeError):
    """Parse errors found while `parse_error_policy = "fail"`."""


class SourceManifestError(BronzeError):
    """Batch input disagrees with its DE-04 manifest (sha256/row_count)."""


class AccountingError(BronzeError):
    """Internal self-check failed: written output does not reconcile."""


# -- inputs / results --------------------------------------------------------


@dataclass(frozen=True)
class SourceFile:
    """One input file, fully read. `name` (basename) is stamped into records
    as lineage; `path` (as given) is used only to locate the file and to
    redact its directory from persisted error messages - the manifest records
    `manifest_input_path(path)`, never the path as given."""

    table: str
    source: str
    name: str
    path: str
    data: bytes
    expected_records: int | None = None
    expected_sha256: str | None = None

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


@dataclass
class FileStats:
    lines_read: int = 0
    blank_lines_skipped: int = 0
    records_read: int = 0
    records_written: int = 0
    json_object_records: int = 0
    parse_error_records: int = 0
    parse_errors_by_code: dict[str, int] = field(default_factory=dict)
    unterminated_final_line: bool = False

    def counters(self) -> dict[str, int]:
        return {
            "lines_read": self.lines_read,
            "blank_lines_skipped": self.blank_lines_skipped,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "json_object_records": self.json_object_records,
            "parse_error_records": self.parse_error_records,
        }


@dataclass(frozen=True)
class RunResult:
    status: str  # COMMITTED | ALREADY_COMMITTED
    run_id: str
    manifest: dict[str, Any]
    manifest_path: Path


def split_lines(data: bytes) -> list[bytes]:
    """Physical lines (without `\\n`). A trailing newline does not start a
    new line; a final line without one (e.g. a truncated write) is kept."""
    if not data:
        return []
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()
    return lines


def _is_blank(line: bytes) -> bool:
    return not line.strip()


def accounting_identity_holds(counters: dict[str, int]) -> bool:
    return (
        counters["lines_read"] == counters["records_read"] + counters["blank_lines_skipped"]
        and counters["records_read"] == counters["records_written"]
        and counters["records_written"] == counters["json_object_records"] + counters["parse_error_records"]
    )


def _sum_counters(items: Sequence[dict[str, int]]) -> dict[str, int]:
    keys = FileStats().counters().keys()
    return {key: sum(item[key] for item in items) for key in keys}


def _fingerprint(mode: str, inputs: Sequence[SourceFile]) -> str:
    payload = json.dumps(
        {"mode": mode, "inputs": [{"table": s.table, "source": s.source, "sha256": s.sha256} for s in inputs]},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _get_logger(logger: logging.Logger | None) -> logging.Logger:
    return logger if logger is not None else logging.getLogger(LOGGER_NAME)


# -- path policy (DE-09 / FU-04) ---------------------------------------------


def manifest_input_path(path_as_given: str) -> str:
    """The manifest form of an input path: relative to its input directory,
    i.e. the basename (either separator style); `-` (stdin) unchanged."""
    if path_as_given == "-":
        return path_as_given
    return re.split(r"[\\/]", path_as_given.rstrip("\\/"))[-1]


def _home_dir() -> str | None:
    try:
        return str(Path.home())
    except (RuntimeError, KeyError):  # no HOME and no passwd entry
        return None


class PathRedactor:
    """Replaces known absolute local roots inside free text (exception
    messages) with placeholders before the text is persisted: the Bronze
    store root -> `<bronze_root>`, input directories -> `<input_dir>`, the
    home directory -> `~`. Each root is matched as given, absolute and
    resolved (e.g. macOS `/var` -> `/private/var`), longest first, and only
    at a path-component boundary."""

    def __init__(self, store_root: Path, input_dirs: Sequence[Path] = ()) -> None:
        forms: dict[str, str] = {}

        def add(path: Path | str | None, label: str) -> None:
            if path is None:
                return
            candidates = {str(path)}
            if isinstance(path, Path):
                candidates |= {str(path.absolute()), str(path.resolve())}
            for form in candidates:
                form = form.rstrip("\\/")
                if form and os.path.isabs(form):
                    forms.setdefault(form, label)

        for directory in input_dirs:
            add(directory, INPUT_DIR_PLACEHOLDER)
        add(store_root, BRONZE_ROOT_PLACEHOLDER)
        add(_home_dir(), HOME_PLACEHOLDER)
        self._rules = [
            (re.compile(re.escape(form) + r"(?![\w.-])"), label)
            for form, label in sorted(forms.items(), key=lambda item: len(item[0]), reverse=True)
        ]

    def __call__(self, text: str) -> str:
        for pattern, label in self._rules:
            text = pattern.sub(label, text)
        return text

    def error(self, error: Any) -> Any:
        """A manifest `error` object with its message redacted."""
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            return {**error, "message": self(error["message"])}
        return error


def _previous_attempts(existing: dict[str, Any], redact: PathRedactor) -> list[dict[str, Any]]:
    """`previous_attempts` for a retry of the FAILED run in `existing`: the
    attempts it already listed, then the FAILED attempt itself. A manifest
    written before DE-09 has no list; its own attempt is still recorded."""
    carried = existing.get("previous_attempts")
    history = [dict(entry) for entry in carried if isinstance(entry, dict)] if isinstance(carried, list) else []
    history.append({key: existing.get(key) for key in PREVIOUS_ATTEMPT_KEYS})
    for entry in history:
        entry["error"] = redact.error(entry.get("error"))
    return history


# -- core run ----------------------------------------------------------------


def _run(
    *,
    mode: str,
    run_source: str,
    inputs: Sequence[SourceFile],
    run_id: str,
    config: BronzeConfig,
    output_dir: Path,
    clock: Clock,
    allow_replay: bool,
    logger: logging.Logger,
    source_manifest: str | None = None,
    ignored_files: Sequence[str] = (),
) -> RunResult:
    validate_run_id(run_id)
    store = BronzeStore(output_dir)
    fingerprint = _fingerprint(mode, inputs)
    log_ctx = {"run_id": run_id, "mode": mode, "source": run_source}
    redact = PathRedactor(output_dir, [Path(s.path).parent for s in inputs if s.path != "-"])

    existing = store.read_manifest(run_id)
    previous_attempts: list[dict[str, Any]] = []
    if existing is not None:
        if existing.get("status") == STATUS_COMMITTED:
            if existing.get("input_fingerprint") == fingerprint:
                logger.info("run already committed with identical input; no-op", extra=log_ctx)
                return RunResult(STATUS_ALREADY_COMMITTED, run_id, existing, store.manifest_path(run_id))
            raise RunIdConflictError(
                f"run_id {run_id!r} is already committed with different input "
                f"(committed fingerprint {existing.get('input_fingerprint')}, new {fingerprint}); "
                "committed runs are never overwritten - use a new run_id"
            )
        previous_attempts = _previous_attempts(existing, redact)

    started = _require_aware(clock())
    ingestion_time = format_timestamp(started)
    policy = config.parse_error_policy
    manifest: dict[str, Any] = {
        "bronze_manifest_version": BRONZE_MANIFEST_VERSION,
        "run_id": run_id,
        "status": STATUS_FAILED,
        "mode": mode,
        "source": run_source,
        "ingestion_time": ingestion_time,
        "started_at": ingestion_time,
        "ended_at": None,
        "duration_seconds": None,
        "parse_error_policy": policy.value,
        "allow_replay": allow_replay,
        "replay_of": [],
        "duplicate_input_files": [],
        "input_fingerprint": fingerprint,
        "previous_attempts": previous_attempts,
        "input_path_policy": INPUT_PATH_POLICY,
        "input_files": [
            {"index": i, "file": s.name, "path": manifest_input_path(s.path), "table": s.table, "source": s.source,
             "sha256": s.sha256, "size_bytes": len(s.data)}
            for i, s in enumerate(inputs)
        ],
        "ignored_files": list(ignored_files),
        "removed_uncommitted_paths": [],
        "tables": {},
        "totals": None,
        "accounting_identity": ACCOUNTING_IDENTITY,
        "accounting_identity_holds": None,
        "source_reconciliation": None,
        "error": None,
    }
    logger.info("bronze run started", extra={**log_ctx, "inputs": [s.name for s in inputs]})

    try:
        manifest["removed_uncommitted_paths"] = store.reset_uncommitted(run_id)
        if manifest["removed_uncommitted_paths"]:
            logger.warning(
                "removed uncommitted output of an earlier attempt",
                extra={**log_ctx, "paths": manifest["removed_uncommitted_paths"]},
            )

        # Replay / double-ingestion guard: any input file (with at least one
        # record) whose exact content a committed run already ingested into
        # the same table.
        prior: dict[tuple[str, str], list[str]] = {}
        for committed in store.committed_manifests():
            for entry in committed.get("input_files", []):
                if entry.get("records_read", 0) > 0:
                    prior.setdefault((entry["table"], entry["sha256"]), []).append(committed["run_id"])
        with_records = [s for s in inputs if any(not _is_blank(line) for line in split_lines(s.data))]
        # The same content given twice within this run is double-ingestion too.
        first_seen: dict[tuple[str, str], str] = {}
        for s in with_records:
            key = (s.table, s.sha256)
            if key in first_seen:
                manifest["duplicate_input_files"].append([first_seen[key], s.name])
            else:
                first_seen[key] = s.name
        if manifest["duplicate_input_files"]:
            if not allow_replay:
                raise DuplicateInputError(
                    f"identical input content given more than once in this run: {manifest['duplicate_input_files']}. "
                    "Refusing silent double-ingestion; pass allow_replay/--allow-replay to replay deliberately"
                )
            logger.warning(
                "deliberate duplicate input within run",
                extra={**log_ctx, "duplicate_input_files": manifest["duplicate_input_files"]},
            )
        replays = [(s, prior[(s.table, s.sha256)]) for s in with_records if (s.table, s.sha256) in prior]
        if replays:
            detail = "; ".join(f"{s.name} -> {sorted(set(ids))}" for s, ids in replays)
            if not allow_replay:
                raise DuplicateInputError(
                    f"input already ingested by committed run(s): {detail}. "
                    "Refusing silent double-ingestion; pass allow_replay/--allow-replay to replay deliberately"
                )
            manifest["replay_of"] = sorted({rid for _, ids in replays for rid in ids})
            logger.warning("deliberate replay of already-ingested input", extra={**log_ctx, "replays": detail})

        # Write every record to staging.
        staging = store.staging_dir(run_id)
        handles: dict[str, Any] = {}
        hashers: dict[str, Any] = {}
        table_files: dict[str, list[int]] = {}
        file_stats: list[FileStats] = []
        error_locations: list[str] = []
        logged_errors = 0
        try:
            for index, src in enumerate(inputs):
                if src.table not in handles:
                    (staging / src.table).mkdir(parents=True, exist_ok=True)
                    handles[src.table] = open(staging / src.table / PART_FILE_NAME, "wb")  # noqa: SIM115
                    hashers[src.table] = hashlib.sha256()
                    table_files[src.table] = []
                table_files[src.table].append(index)
                fh, hasher = handles[src.table], hashers[src.table]
                metadata = RecordMetadata(ingestion_time, src.source, run_id, src.name)
                stats = FileStats()
                lines = split_lines(src.data)
                stats.unterminated_final_line = bool(src.data) and not src.data.endswith(b"\n")
                for line_number, raw in enumerate(lines, start=1):
                    stats.lines_read += 1
                    if _is_blank(raw):
                        stats.blank_lines_skipped += 1
                        continue
                    stats.records_read += 1
                    bronze_line = build_bronze_line(raw, metadata, line_number)
                    encoded = (bronze_line.text + "\n").encode("utf-8")
                    fh.write(encoded)
                    hasher.update(encoded)
                    stats.records_written += 1
                    if bronze_line.is_parse_error:
                        stats.parse_error_records += 1
                        code = bronze_line.parse_error
                        stats.parse_errors_by_code[code] = stats.parse_errors_by_code.get(code, 0) + 1
                        location = f"{src.name}:{line_number}: {code}"
                        if len(error_locations) < 10:
                            error_locations.append(location)
                        if logged_errors < config.max_logged_parse_errors:
                            logged_errors += 1
                            logger.warning(
                                "parse-error record",
                                extra={**log_ctx, "file": src.name, "line_number": line_number,
                                       "parse_error": code, "detail": bronze_line.detail},
                            )
                    else:
                        stats.json_object_records += 1
                file_stats.append(stats)
                entry = manifest["input_files"][index]
                entry.update(stats.counters())
                entry["parse_errors_by_code"] = {c: stats.parse_errors_by_code.get(c, 0) for c in PARSE_ERROR_CODES}
                entry["unterminated_final_line"] = stats.unterminated_final_line
                if src.expected_records is not None or src.expected_sha256 is not None:
                    matched = src.expected_records == stats.records_read and src.expected_sha256 == src.sha256
                    entry["source_manifest_check"] = {
                        "expected_records": src.expected_records,
                        "expected_sha256": src.expected_sha256,
                        "status": "MATCHED" if matched else "MISMATCH",
                    }
                logger.info(
                    "input file ingested",
                    extra={**log_ctx, "file": src.name, "table": src.table, **stats.counters()},
                )
        finally:
            for fh in handles.values():
                fh.close()

        # Source reconciliation against the DE-04 manifest (batch only).
        if source_manifest is None:
            reconciliation = {"status": "NOT_APPLICABLE" if mode == MODE_STREAM else "NO_SOURCE_MANIFEST"}
        else:
            mismatched = [
                e["file"] for e in manifest["input_files"] if e["source_manifest_check"]["status"] != "MATCHED"
            ]
            reconciliation = {
                "status": "MISMATCH" if mismatched else "MATCHED",
                "source_manifest": source_manifest,
                "verified": config.verify_source_manifest,
                "mismatched_files": mismatched,
            }
        manifest["source_reconciliation"] = reconciliation
        if reconciliation["status"] == "MISMATCH":
            if config.verify_source_manifest:
                raise SourceManifestError(
                    f"input disagrees with {source_manifest} (sha256/row_count) for: {reconciliation['mismatched_files']}"
                )
            logger.warning("source manifest mismatch (verification disabled)", extra={**log_ctx, **reconciliation})

        # Per-table summary + self-check: re-count what was actually written.
        for table, indexes in table_files.items():
            counters = _sum_counters([file_stats[i].counters() for i in indexes])
            staged = staging / table / PART_FILE_NAME
            written_lines = staged.read_bytes().count(b"\n")
            if written_lines != counters["records_written"]:
                raise AccountingError(
                    f"table {table}: {written_lines} lines on disk but {counters['records_written']} records counted"
                )
            manifest["tables"][table] = {
                "source": inputs[indexes[0]].source,
                "path": f"{table}/run_id={run_id}/{PART_FILE_NAME}",
                "sha256": hashers[table].hexdigest(),
                "input_files": [inputs[i].name for i in indexes],
                **counters,
            }
        totals = _sum_counters([s.counters() for s in file_stats])
        manifest["totals"] = totals
        manifest["accounting_identity_holds"] = accounting_identity_holds(totals) and all(
            accounting_identity_holds(s.counters()) for s in file_stats
        )
        if not manifest["accounting_identity_holds"]:
            raise AccountingError(f"accounting identity violated: {totals}")

        if policy is ParseErrorPolicy.FAIL and totals["parse_error_records"]:
            raise MalformedInputError(
                f"{totals['parse_error_records']} unparseable line(s) with parse_error_policy='fail' "
                f"(first: {error_locations}); nothing was committed"
            )

        # Commit: publish partitions, then the manifest (the commit marker).
        for table in table_files:
            store.publish_table(table, run_id)
        store.discard_staging(run_id)
        ended = _require_aware(clock())
        manifest["ended_at"] = format_timestamp(ended)
        manifest["duration_seconds"] = (ended - started).total_seconds()
        manifest["status"] = STATUS_COMMITTED
        path = store.write_manifest(manifest)
    except Exception as exc:
        store.discard_staging(run_id)
        ended = _require_aware(clock())
        manifest["status"] = STATUS_FAILED
        manifest["ended_at"] = format_timestamp(ended)
        manifest["duration_seconds"] = (ended - started).total_seconds()
        manifest["error"] = {"type": type(exc).__name__, "message": redact(str(exc))}
        store.write_manifest(manifest)
        logger.error("bronze run failed; nothing committed", extra={**log_ctx, "error": manifest["error"]})
        raise

    logger.info(
        "bronze run committed",
        extra={**log_ctx, "manifest": path.relative_to(store.root).as_posix(), "replay_of": manifest["replay_of"],
               **manifest["totals"]},
    )
    return RunResult(STATUS_COMMITTED, run_id, manifest, path)


# -- public entry points -----------------------------------------------------


def ingest_stream(
    inputs: Sequence[str | Path],
    *,
    run_id: str,
    output_dir: str | Path | None = None,
    config: BronzeConfig | None = None,
    source: str | None = None,
    clock: Clock = system_clock,
    allow_replay: bool = False,
    stdin: BinaryIO | None = None,
    logger: logging.Logger | None = None,
) -> RunResult:
    """Ingest streaming-shaped NDJSON (DE-02 generator / DE-03 incidents
    output) into the configured stream table. Files are ingested in the
    given order (arrival order is preserved via `_bronze_line_number`).
    `"-"` reads standard input (or `stdin`)."""
    config = config if config is not None else load_bronze_config()
    if not inputs:
        raise InputError("no stream inputs given")
    record_source = source if source is not None else config.stream.source
    files: list[SourceFile] = []
    for item in inputs:
        if str(item) == "-":
            data = (stdin if stdin is not None else sys.stdin.buffer).read()
            files.append(SourceFile(config.stream.table, record_source, STDIN_NAME, "-", data))
            continue
        path = Path(item)
        if not path.is_file():
            raise InputError(f"stream input not found or not a file: {path}")
        files.append(SourceFile(config.stream.table, record_source, path.name, str(item), path.read_bytes()))
    names = [f.name for f in files]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise InputError(f"stream inputs must have distinct file names (lineage key); duplicated: {duplicates}")
    return _run(
        mode=MODE_STREAM,
        run_source=record_source,
        inputs=files,
        run_id=run_id,
        config=config,
        output_dir=Path(output_dir) if output_dir is not None else config.output_dir,
        clock=clock,
        allow_replay=allow_replay,
        logger=_get_logger(logger),
    )


def _discover_batch_inputs(batch_dir: Path, config: BronzeConfig) -> tuple[list[SourceFile], str | None, list[str]]:
    if not batch_dir.is_dir():
        raise InputError(f"batch input directory not found: {batch_dir}")
    manifest_name = config.batch.source_manifest_file
    manifest_path = batch_dir / manifest_name
    files: list[SourceFile] = []
    if manifest_path.is_file():
        try:
            source_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            datasets = source_manifest["datasets"]
        except (ValueError, KeyError, TypeError) as exc:
            raise InputError(f"unreadable source manifest {manifest_path}: {exc}") from exc
        for dataset in sorted(datasets):
            entry = datasets[dataset]
            file_name = entry.get("file")
            if not TABLE_NAME_PATTERN.fullmatch(dataset) or not isinstance(file_name, str) or Path(file_name).name != file_name:
                raise InputError(f"unsafe dataset/file name in {manifest_name}: {dataset!r} -> {file_name!r}")
            path = batch_dir / file_name
            if not path.is_file():
                raise InputError(f"dataset {dataset!r} listed in {manifest_name} but {path} is missing")
            files.append(
                SourceFile(
                    table=dataset,
                    source=config.batch.record_source(dataset),
                    name=file_name,
                    path=str(path),
                    data=path.read_bytes(),
                    expected_records=entry.get("row_count"),
                    expected_sha256=entry.get("sha256"),
                )
            )
        used = {manifest_name, *(f.name for f in files)}
        reconciliation_source: str | None = manifest_name
    else:
        for path in sorted(batch_dir.glob("*.jsonl")):
            if not TABLE_NAME_PATTERN.fullmatch(path.stem):
                raise InputError(f"unsafe dataset name derived from file {path.name!r}")
            files.append(
                SourceFile(path.stem, config.batch.record_source(path.stem), path.name, str(path), path.read_bytes())
            )
        used = {f.name for f in files}
        reconciliation_source = None
    if not files:
        raise InputError(f"no datasets found in {batch_dir} (no {manifest_name} and no *.jsonl files)")
    ignored = sorted(p.name for p in batch_dir.iterdir() if p.is_file() and p.name not in used)
    return files, reconciliation_source, ignored


def ingest_batch(
    batch_dir: str | Path | None = None,
    *,
    run_id: str,
    output_dir: str | Path | None = None,
    config: BronzeConfig | None = None,
    clock: Clock = system_clock,
    allow_replay: bool = False,
    logger: logging.Logger | None = None,
) -> RunResult:
    """Ingest a DE-04 batch directory (one `<dataset>.jsonl` per dataset,
    plus `manifest.json`) - one Bronze table per dataset. Rows are stored
    unfiltered: no watermark predicate is applied, so rows whose `updated_at`
    precedes that of rows they reference (BACKLOG FU-02) are never rejected,
    and `event_date` is stored exactly as provided."""
    config = config if config is not None else load_bronze_config()
    log = _get_logger(logger)
    directory = Path(batch_dir) if batch_dir is not None else config.batch_input_dir
    files, source_manifest, ignored = _discover_batch_inputs(directory, config)
    if ignored:
        log.warning("files in batch directory not ingested", extra={"run_id": run_id, "ignored_files": ignored})
    return _run(
        mode=MODE_BATCH,
        run_source=config.batch.source,
        inputs=files,
        run_id=run_id,
        config=config,
        output_dir=Path(output_dir) if output_dir is not None else config.output_dir,
        clock=clock,
        allow_replay=allow_replay,
        logger=log,
        source_manifest=source_manifest,
        ignored_files=ignored,
    )
