"""DE-12 batch Silver run: apply pending committed Bronze batch runs (each
once, in Bronze commit order) to the current-state tables, with strict
validation, lossless quarantine, explicit per-row merge outcomes, FU-02
unresolved-reference counts and a per-dataset reconciliation asserted before
commit.

Checkpoint: the run starts from the head commit (highest `commit_sequence`);
"pending" = committed Bronze batch runs whose id is not in the head's
`checkpoint.applied_bronze_run_ids`. Re-applying an applied run is a no-op.

Idempotency (as DE-06): same run_id + same input fingerprint already
committed -> `ALREADY_COMMITTED` (nothing written); same run_id, different
fingerprint -> `SilverBatchRunIdConflictError` (refused); same run_id after a
FAILED/crashed attempt -> retried from scratch, the failed attempt kept in
`previous_attempts`. A committed run's fingerprint is recomputed against
*its own* base commit and the Bronze runs it applied (each must still be
committed with the same sha256), so later Silver commits and Bronze runs
committed after it never turn a retry of a finished run into a conflict.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from simulator.bronze.ingest import format_timestamp, system_clock
from simulator.bronze.store import (
    PART_FILE_NAME,
    STATUS_COMMITTED,
    STATUS_FAILED,
    BronzeStore,
    validate_run_id,
)
from simulator.silver_batch.bronze_input import (
    EXTRACT_FOUND,
    BronzeBatchRun,
    committed_batch_runs,
    iter_table_rows,
    load_de11_listing,
)
from simulator.silver_batch.config import SilverBatchConfig, load_silver_batch_config
from simulator.silver_batch.datasets import SILVER_DATASETS, LoadMode
from simulator.silver_batch.errors import (
    SilverBatchAccountingError,
    SilverBatchInputError,
    SilverBatchRunIdConflictError,
)
from simulator.silver_batch.merge import (
    Counters,
    DatasetState,
    RunContext,
    apply_merge_run,
    apply_snapshot_run,
    dumps_line,
    parent_key_sets,
    unresolved_counts,
)
from simulator.silver_batch.rules import RULE_CATALOGUE, RULE_MERGE_CONFLICT, Violation
from simulator.silver_batch.store import SilverBatchStore

LOGGER_NAME = "simulator.silver_batch"
SILVER_BATCH_MANIFEST_VERSION = "1.0.0"
STATUS_ALREADY_COMMITTED = "ALREADY_COMMITTED"
SELECTION_ALL_PENDING = "all_pending"
SELECTION_EXPLICIT = "explicit"

#: Hook stages (dependency-injected crash/fault points for tests; no flags).
STAGE_LOADED = "loaded"  # base tables loaded and verified
STAGE_STAGED = "staged"  # every output file written to _staging, before the line re-count
STAGE_PUBLISHED = "published"  # partitions published, manifest not yet written
Hook = Callable[[str, Path], None]
Clock = Callable[[], datetime]

#: Asserted per dataset before commit; recorded verbatim in every manifest.
RECONCILIATION_IDENTITIES: tuple[str, ...] = (
    "extracted == inserted + updated + unchanged + stale + quarantined",
    "extracted == sum(records of the Bronze table runs applied)",
    ("quarantined == parse_errors + schema_failures + source_flagged + merge_quarantined "
    "== sum(quarantined_by_rule)"),
    "conflicts == quarantined_by_rule[merge_conflict]",
    "merge mode: rows_after == rows_before + inserted; tombstones_after == 0",
    ("snapshot mode: rows_after == rows_before + inserted - deleted; "
    "rows_after == valid keys of the last applied (non-stale) snapshot"),
    "lines on disk: current table == rows_after + tombstones_after; quarantine == quarantined; stale == stale",
    ("DE-11 listing: listed rows == listed rows quarantined (stored + parse-error); "
    "listed stored rows == stored_record_requires_quarantine in the extract manifests"),
)


@dataclass(frozen=True)
class SilverBatchResult:
    status: str  # COMMITTED | ALREADY_COMMITTED
    run_id: str
    manifest: dict[str, Any]
    manifest_path: Path


class ContextLogger(logging.LoggerAdapter):
    """Merges `silver_batch_run_id` into every record's extra (FU-09 not
    repeated: every log line of a run carries its run id)."""

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        kwargs["extra"] = {**self.extra, **(kwargs.get("extra") or {})}
        return msg, kwargs


class PathRedactor:
    """Replaces configured roots, the working directory and the home
    directory in free text (error messages) before it is persisted (FU-04)."""

    def __init__(self, roots: dict[str, Path | None]) -> None:
        forms: dict[str, str] = {}
        candidates: list[tuple[Path | str | None, str]] = [(path, label) for label, path in roots.items()]
        candidates.append((Path.cwd(), "<cwd>"))
        try:
            candidates.append((Path.home(), "~"))
        except (RuntimeError, KeyError):  # pragma: no cover - no HOME
            pass
        for path, label in candidates:
            if path is None:
                continue
            spellings = {str(path)}
            if isinstance(path, Path):
                spellings |= {str(path.absolute()), str(path.resolve())}
            for form in spellings:
                form = form.rstrip("\\/")
                if form and form not in {".", ".."}:
                    forms.setdefault(form, label)
        self._rules = []
        for form, label in sorted(forms.items(), key=lambda item: len(item[0]), reverse=True):
            left = "" if os.path.isabs(form) else r"(?<![\w.~/\\-])"
            self._rules.append((re.compile(left + re.escape(form) + r"(?![\w.-])"), label))

    def __call__(self, text: str) -> str:
        for pattern, label in self._rules:
            text = pattern.sub(label, text)
        return text


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"clock must return a timezone-aware datetime, got {value!r}")
    return value.astimezone(UTC)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _fingerprint(
    config: SilverBatchConfig,
    base: dict[str, Any] | None,
    selection: str,
    selected: Sequence[BronzeBatchRun],
    skipped: Sequence[str],
) -> str:
    return _sha256_json(
        {
            "manifest_version": SILVER_BATCH_MANIFEST_VERSION,
            "rule_catalogue": [rule.rule_id for rule in RULE_CATALOGUE],
            "config": config.fingerprint_fields(),
            "base_run_id": base["run_id"] if base else None,
            "base_checkpoint_sha256": _sha256_json(base["checkpoint"]) if base else None,
            "selection": selection,
            "bronze_runs": [
                [run.run_id, [[t.table, t.sha256, t.records] for t in run.tables]] for run in selected
            ],
            "skipped_already_applied": list(skipped),
        }
    )


def _sort_key(value: tuple) -> tuple:
    # Primary keys are str/int tuples; order by (type name, value) per column
    # so a mixed-type corruption cannot raise TypeError mid-commit.
    return tuple((type(v).__name__, v) for v in value)


def _load_states(store: SilverBatchStore, base: dict[str, Any] | None) -> dict[str, DatasetState]:
    states: dict[str, DatasetState] = {}
    if base is None:
        return states
    for name, entry in base["checkpoint"]["tables"].items():
        dataset = SILVER_DATASETS[name]
        rows = store.read_table_file(entry)
        keyed = {tuple(row[col] for col in dataset.primary_key): row for row in rows}
        if len(keyed) != len(rows):
            raise SilverBatchAccountingError(f"committed table {entry['path']} repeats a primary key")
        state = DatasetState(
            dataset=dataset,
            rows=keyed,
            rows_before=entry["rows"],
            tombstones_before=entry["tombstones"],
            last_snapshot_as_of=entry.get("last_snapshot_as_of"),
        )
        if state.active_rows != entry["rows"]:
            raise SilverBatchAccountingError(f"committed table {entry['path']}: active rows differ from the manifest")
        states[name] = state
    return states


def _new_state(name: str) -> DatasetState:
    return DatasetState(dataset=SILVER_DATASETS[name], rows={}, rows_before=0, tombstones_before=0,
                        last_snapshot_as_of=None)


def _check(checks: list[dict[str, Any]], dataset: str, name: str, left: int, right: int) -> None:
    checks.append({"dataset": dataset, "check": name, "left": left, "right": right, "holds": left == right})


def reconcile(datasets: dict[str, dict[str, Any]], listing_expected: dict[str, int]) -> dict[str, Any]:
    """Evaluate every identity per dataset from independently obtained
    numbers: counters incremented at classification, Bronze manifest record
    counts, row counts of the in-memory state, and lines re-counted in the
    files actually written (or carried forward)."""
    checks: list[dict[str, Any]] = []
    for name, info in datasets.items():
        c = info["counters"]
        _check(checks, name, "extracted == inserted+updated+unchanged+stale+quarantined", c["extracted"],
               c["inserted"] + c["updated"] + c["unchanged"] + c["stale"] + c["quarantined"])
        _check(checks, name, "extracted == bronze records", c["extracted"], c["bronze_records"])
        _check(checks, name, "quarantined == by category", c["quarantined"],
               c["parse_errors"] + c["schema_failures"] + c["source_flagged"] + c["merge_quarantined"])
        _check(checks, name, "quarantined == sum(quarantined_by_rule)", c["quarantined"],
               sum(c["quarantined_by_rule"].values()))
        _check(checks, name, "conflicts == quarantined_by_rule[merge_conflict]", c["conflicts"],
               c["quarantined_by_rule"].get(RULE_MERGE_CONFLICT, 0))
        if info["mode"] == LoadMode.MERGE.value:
            _check(checks, name, "rows_after == rows_before + inserted", info["rows_after"],
                   info["rows_before"] + c["inserted"])
            _check(checks, name, "tombstones_after == 0", info["tombstones_after"], 0)
        else:
            _check(checks, name, "rows_after == rows_before + inserted - deleted", info["rows_after"],
                   info["rows_before"] + c["inserted"] - c["deleted"])
            if info["last_valid_snapshot_keys"] is not None:
                _check(checks, name, "rows_after == valid snapshot keys", info["rows_after"],
                       info["last_valid_snapshot_keys"])
        tables = info["tables"]
        _check(checks, name, "current table lines == rows_after + tombstones_after",
               tables["current"]["lines_on_disk"] if tables["current"] else 0,
               info["rows_after"] + info["tombstones_after"])
        _check(checks, name, "quarantine lines == quarantined",
               tables["quarantine"]["records"] if tables["quarantine"] else 0, c["quarantined"])
        _check(checks, name, "stale lines == stale", tables["stale"]["records"] if tables["stale"] else 0, c["stale"])
        _check(checks, name, "DE-11 listed rows == listed rows quarantined", c["de11_listed_rows"],
               c["de11_listed_stored_rows_quarantined"] + c["de11_listed_parse_error_rows_quarantined"])
        _check(checks, name, "DE-11 stored_record_requires_quarantine == listed stored rows quarantined",
               listing_expected.get(name, 0), c["de11_listed_stored_rows_quarantined"])
    return {
        "identities": list(RECONCILIATION_IDENTITIES),
        "checks": checks,
        "holds": all(check["holds"] for check in checks),
        "failed_checks": [check for check in checks if not check["holds"]],
    }


def _counted_lines(path: Path) -> tuple[int, str]:
    data = path.read_bytes()
    return data.count(b"\n"), hashlib.sha256(data).hexdigest()


def run_silver_batch(
    *,
    run_id: str,
    bronze_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    bronze_run_ids: Sequence[str] | None = None,
    config: SilverBatchConfig | None = None,
    clock: Clock = system_clock,
    logger: logging.Logger | None = None,
    hook: Hook | None = None,
) -> SilverBatchResult:
    """Apply every pending committed Bronze batch run (or exactly
    `bronze_run_ids`, still in Bronze commit order) and commit run `run_id`."""
    validate_run_id(run_id)
    config = config if config is not None else load_silver_batch_config()
    log = ContextLogger(logger if logger is not None else logging.getLogger(LOGGER_NAME),
                        {"silver_batch_run_id": run_id})
    bronze_root = Path(bronze_dir) if bronze_dir is not None else config.bronze_dir
    output_root = Path(output_dir) if output_dir is not None else config.output_dir
    state_dir = config.incremental_state_dir
    bronze = BronzeStore(bronze_root)
    store = SilverBatchStore(output_root)
    redact = PathRedactor({"<bronze_dir>": bronze_root, "<output_dir>": output_root, "<incremental_state_dir>": state_dir})

    # -- input selection (refusals happen here, before anything is written) --
    existing = store.read_manifest(run_id)
    if existing is not None and existing.get("status") == STATUS_COMMITTED:
        base = store.committed(existing["base_run_id"]) if existing.get("base_run_id") else None
    else:
        base = store.head()
    applied = set(base["checkpoint"]["applied_bronze_run_ids"]) if base else set()
    all_runs = committed_batch_runs(bronze)
    known = {run.run_id for run in all_runs}
    if bronze_run_ids is not None:
        wanted = list(dict.fromkeys(bronze_run_ids))
        unknown = [rid for rid in wanted if rid not in known]
        if unknown:
            raise SilverBatchInputError(
                f"Bronze run(s) {unknown} are not committed batch runs in the Bronze store; nothing was written"
            )
        selection = SELECTION_EXPLICIT
        skipped = [run.run_id for run in all_runs if run.run_id in wanted and run.run_id in applied]
        selected = [run for run in all_runs if run.run_id in wanted and run.run_id not in applied]
    else:
        selection = SELECTION_ALL_PENDING
        skipped = []
        selected = [run for run in all_runs if run.run_id not in applied]
        if existing is not None and existing.get("status") == STATUS_COMMITTED:
            # A retry of a committed run is checked against what IT applied;
            # Bronze runs committed after it are left for the next run id.
            recorded = {entry["run_id"] for entry in existing.get("bronze_runs_applied", [])}
            selected = [run for run in selected if run.run_id in recorded]
    fingerprint = _fingerprint(config, base, selection, selected, skipped)

    previous_attempts: list[dict[str, Any]] = []
    if existing is not None:
        if existing.get("status") == STATUS_COMMITTED:
            if existing.get("input_fingerprint") == fingerprint:
                log.info("batch-silver run already committed with identical input; no-op")
                return SilverBatchResult(STATUS_ALREADY_COMMITTED, run_id, existing, store.manifest_path(run_id))
            raise SilverBatchRunIdConflictError(
                f"batch-silver run_id {run_id!r} is already committed with different input/config "
                f"(committed fingerprint {existing.get('input_fingerprint')}, new {fingerprint}; e.g. Bronze runs "
                "committed since); committed runs are never overwritten - use a new run_id"
            )
        previous_attempts = [
            *existing.get("previous_attempts", []),
            {key: existing.get(key) for key in ("status", "started_at", "ended_at", "base_run_id",
                                                "input_fingerprint", "error")},
        ]

    started = _aware_utc(clock())
    processing_time = format_timestamp(started)
    manifest: dict[str, Any] = {
        "silver_batch_manifest_version": SILVER_BATCH_MANIFEST_VERSION,
        "layer": "silver_batch",
        "run_id": run_id,
        "status": STATUS_FAILED,
        "commit_sequence": (base["commit_sequence"] + 1) if base else 1,
        "base_run_id": base["run_id"] if base else None,
        "processing_time": processing_time,
        "started_at": processing_time,
        "ended_at": None,
        "duration_seconds": None,
        "input_fingerprint": fingerprint,
        "previous_attempts": previous_attempts,
        "bronze_selection": selection,
        "bronze_runs_applied": [],
        "bronze_runs_skipped_already_applied": skipped,
        "config": config.fingerprint_fields(),
        "rule_catalogue": [rule.as_dict() for rule in RULE_CATALOGUE],
        "datasets": {},
        "totals": None,
        "referential_integrity": None,
        "reconciliation": None,
        "checkpoint": None,
        "removed_uncommitted_paths": [],
        "error": None,
    }
    log.info("batch-silver run started", extra={
        "base_run_id": manifest["base_run_id"], "bronze_runs_pending": [run.run_id for run in selected],
        "bronze_runs_skipped_already_applied": skipped, "bronze_selection": selection})

    logged = {"count": 0}

    def log_rejection(dataset: str, record: dict[str, Any], violations: list[Violation]) -> None:
        if logged["count"] < config.max_logged_rejections:
            logged["count"] += 1
            log.warning("row quarantined", extra={
                "dataset": dataset, "bronze_run_id": record["bronze_run_id"],
                "line_number": record["_bronze_line_number"], "rules": [v.rule for v in violations]})

    ctx = RunContext(run_id, processing_time, config.max_snapshot_delete_fraction, log_rejection)
    try:
        manifest["removed_uncommitted_paths"] = store.reset_uncommitted(run_id)
        if manifest["removed_uncommitted_paths"]:
            log.warning("removed uncommitted output of an earlier attempt",
                        extra={"paths": manifest["removed_uncommitted_paths"]})
        states = _load_states(store, base)
        if hook is not None:
            hook(STAGE_LOADED, store.root)

        touched: list[str] = []
        listing_expected: dict[str, int] = {}
        for run in selected:
            run_entry: dict[str, Any] = {"run_id": run.run_id, "ingestion_time": run.ingestion_time,
                                         "replay_of": list(run.replay_of), "tables": [], "ignored_tables": []}
            for table_run in run.tables:
                if table_run.table not in SILVER_DATASETS:
                    run_entry["ignored_tables"].append(table_run.table)
                    log.warning("Bronze table is not a batch-Silver dataset; ignored",
                                extra={"bronze_run_id": run.run_id, "table": table_run.table})
                    continue
                name = table_run.table
                state = states.setdefault(name, _new_state(name))
                if name not in touched:
                    touched.append(name)
                listing = load_de11_listing(state_dir, table_run, config.incremental_run_id_prefixes)
                rows = iter_table_rows(bronze, table_run)
                step_entry: dict[str, Any] = {**table_run.as_dict(), "dataset": name,
                                              "mode": state.dataset.mode.value, "de11_extract": listing.as_dict()}
                if state.dataset.mode is LoadMode.SNAPSHOT:
                    as_of = listing.window_high if listing.status == EXTRACT_FOUND and listing.window_high \
                        else table_run.ingestion_time
                    counters, was_stale = apply_snapshot_run(ctx, state, rows, listing, run.run_id, as_of)
                    step_entry["snapshot_as_of"] = as_of
                    step_entry["snapshot_as_of_source"] = (
                        "de11_window_high" if as_of == listing.window_high else "bronze_ingestion_time")
                    step_entry["snapshot_stale"] = was_stale
                else:
                    counters = apply_merge_run(ctx, state, rows, listing, run.run_id)
                counters.bronze_records = table_run.records
                listing_expected[name] = listing_expected.get(name, 0) + listing.as_dict()[
                    "stored_records_requiring_quarantine"]
                step_entry["counters"] = counters.as_dict()
                step_entry["rows_after"] = state.active_rows
                state.steps.append(step_entry)
                state.counters.add(counters)
                run_entry["tables"].append(step_entry)
                log.info("Bronze table run applied", extra={
                    "bronze_run_id": run.run_id, "dataset": name,
                    **{k: getattr(counters, k) for k in ("extracted", "inserted", "updated", "unchanged", "stale",
                                                          "quarantined", "deleted")}})
            manifest["bronze_runs_applied"].append(run_entry)

        # -- stage output -----------------------------------------------------
        staging = store.staging_dir(run_id)
        staged: dict[str, Path] = {}

        def stage(table: str, lines: Sequence[str]) -> None:
            directory = staging / table
            directory.mkdir(parents=True, exist_ok=True)
            with open(directory / PART_FILE_NAME, "wb") as fh:
                fh.writelines(line.encode("utf-8") for line in lines)
            staged[table] = directory / PART_FILE_NAME

        for name in touched:
            state = states[name]
            ds = state.dataset
            base_entry = base["checkpoint"]["tables"].get(name) if base else None
            if state.changed or base_entry is None:
                ordered_rows = [state.rows[key] for key in sorted(state.rows, key=_sort_key)]
                stage(ds.current_table, [dumps_line(row) for row in ordered_rows])
            if state.quarantine_lines:
                stage(ds.quarantine_table, state.quarantine_lines)
            if state.stale_lines:
                stage(ds.stale_table, state.stale_lines)
        if hook is not None:
            hook(STAGE_STAGED, store.root)

        # -- re-count what is on disk, FU-02 counts, reconcile ------------------
        parents = parent_key_sets(states)
        checkpoint_tables: dict[str, Any] = {}
        for name, state in states.items():
            ds = state.dataset
            base_entry = base["checkpoint"]["tables"].get(name) if base else None
            if ds.current_table in staged:
                lines, digest = _counted_lines(staged[ds.current_table])
                entry = {"path": f"{ds.current_table}/run_id={run_id}/{PART_FILE_NAME}", "sha256": digest,
                         "version_run_id": run_id}
            else:
                assert base_entry is not None
                lines, digest = _counted_lines(store.root / base_entry["path"])
                if digest != base_entry["sha256"]:
                    raise SilverBatchAccountingError(f"carried table {base_entry['path']} changed on disk")
                entry = {"path": base_entry["path"], "sha256": digest, "version_run_id": base_entry["version_run_id"]}
            entry.update(rows=state.active_rows, tombstones=state.tombstones, lines_on_disk=lines,
                         mode=ds.mode.value, primary_key=list(ds.primary_key),
                         last_snapshot_as_of=state.last_snapshot_as_of)
            checkpoint_tables[name] = entry

        for name in touched:
            state = states[name]
            ds = state.dataset
            tables: dict[str, Any] = {"current": {**checkpoint_tables[name],
                                                  "written_this_run": ds.current_table in staged}}
            for role, table in (("quarantine", ds.quarantine_table), ("stale", ds.stale_table)):
                if table in staged:
                    lines, digest = _counted_lines(staged[table])
                    tables[role] = {"path": f"{table}/run_id={run_id}/{PART_FILE_NAME}", "sha256": digest,
                                    "records": lines}
                else:
                    tables[role] = None
            counters = state.counters.as_dict()
            extracted, quarantined = counters["extracted"], counters["quarantined"]
            manifest["datasets"][name] = {
                "mode": ds.mode.value,
                "primary_key": list(ds.primary_key),
                "watermark_column": ds.watermark_column,
                "rows_before": state.rows_before,
                "tombstones_before": state.tombstones_before,
                "rows_after": state.active_rows,
                "tombstones_after": state.tombstones,
                "last_snapshot_as_of": state.last_snapshot_as_of,
                "last_valid_snapshot_keys": state.last_valid_snapshot_keys,
                "counters": counters,
                "quality_percentage": round(100 * (extracted - quarantined) / extracted, 4) if extracted else None,
                "unresolved_references": unresolved_counts(state, parents, state.written_keys),
                "tables": tables,
                "bronze_table_runs": [step["path"] for step in state.steps],
            }
        manifest["referential_integrity"] = {
            name: unresolved_counts(states[name], parents) for name in SILVER_DATASETS if name in states
        }
        totals = Counters()
        for name in touched:
            totals.add(states[name].counters)
        manifest["totals"] = totals.as_dict()

        reconciliation = reconcile(manifest["datasets"], listing_expected)
        manifest["reconciliation"] = reconciliation
        if not reconciliation["holds"]:
            raise SilverBatchAccountingError(f"reconciliation failed: {reconciliation['failed_checks']}")

        manifest["checkpoint"] = {
            "applied_bronze_run_ids": sorted(applied | {run.run_id for run in selected}),
            "tables": {name: {k: v for k, v in checkpoint_tables[name].items() if k != "lines_on_disk"}
                       for name in SILVER_DATASETS if name in checkpoint_tables},
        }

        # -- commit -------------------------------------------------------------
        for table in staged:
            store.publish_table(table, run_id)
        store.discard_staging(run_id)
        if hook is not None:
            hook(STAGE_PUBLISHED, store.root)
        ended = _aware_utc(clock())
        manifest["ended_at"] = format_timestamp(ended)
        manifest["duration_seconds"] = (ended - started).total_seconds()
        manifest["status"] = STATUS_COMMITTED
        path = store.write_manifest(manifest)
    except Exception as exc:
        store.discard_staging(run_id)
        ended = _aware_utc(clock())
        manifest["status"] = STATUS_FAILED
        manifest["ended_at"] = format_timestamp(ended)
        manifest["duration_seconds"] = (ended - started).total_seconds()
        manifest["checkpoint"] = None
        manifest["error"] = {"type": type(exc).__name__, "message": redact(str(exc))}
        store.write_manifest(manifest)
        log.error("batch-silver run failed; nothing committed", extra={"error": manifest["error"]})
        raise

    log.info("batch-silver run committed", extra={
        "manifest": path.name, "commit_sequence": manifest["commit_sequence"],
        "bronze_runs_applied": [run.run_id for run in selected],
        **{k: manifest["totals"][k] for k in ("extracted", "inserted", "updated", "unchanged", "stale",
                                             "quarantined", "deleted")}})
    return SilverBatchResult(STATUS_COMMITTED, run_id, manifest, path)
