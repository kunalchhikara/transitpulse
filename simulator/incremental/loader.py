"""DE-11 orchestrator: extract -> land in Bronze -> advance watermark -> audit.

For each dataset of the resolved plan, in order, with per-dataset commit
(Lead decision 4):

1. read the dataset's control record and check it against the Bronze store
   (no unrecorded loader run, no recorded run missing);
2. derive the window: `full` = the whole snapshot as of `as_of`;
   `incremental` = `watermark < updated_at <= as_of` (`updated_at <= as_of`
   on the initial load);
3. read + classify the source; apply the bad-source policy and the
   watermark-completeness (late-arrival) check;
4. write the extract (DE-04-shaped directory) and re-count it on disk;
5. land it with `simulator.bronze.ingest_batch` under the deterministic run
   id `<prefix>-<dataset>-<window>`;
6. assert `selected == extracted == Bronze records_written`;
7. only then replace the control record (the watermark advance);
8. write one immutable audit record for the attempt.

A crash between any two steps is safe to rerun with the same `as_of`: the
extract is rewritten byte-identically, Bronze returns `ALREADY_COMMITTED`
for the same run id + content (or cleans up its own uncommitted attempt),
and a dataset whose watermark already equals `as_of` is `UP_TO_DATE`.
Watermark meaning: see `WATERMARK_SEMANTICS` and README "DE-11".
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from simulator.bronze import (
    BronzeStore,
    ParseErrorPolicy,
    RunResult,
    ingest_batch,
    load_bronze_config,
)
from simulator.incremental.clock import (
    Clock,
    SystemClock,
    compact_ts,
    format_ts,
    parse_aware,
    require_aware,
)
from simulator.incremental.config import (
    BadSourcePolicy,
    DatasetPlan,
    IncrementalConfig,
    LateArrivalPolicy,
    load_incremental_config,
    resolve_plan,
)
from simulator.incremental.extract import (
    EXTRACT_MANIFEST_FILE,
    ExtractInfo,
    SourceError,
    SourceSnapshot,
    classify_line,
    count_at_or_below,
    count_file_lines,
    extract_bytes,
    read_incremental_source,
    read_reference_source,
    select_full,
    select_window,
    split_lines,
    write_extract,
)
from simulator.incremental.state import (
    AUDIT_SCHEMA_VERSION,
    CONTROL_SCHEMA_VERSION,
    PIPELINE_SUMMARY_FILE,
    AuditStore,
    ControlStore,
    PathSanitizer,
    StateError,
    extract_dir,
)

LOGGER_NAME = "simulator.incremental"
BRONZE_LOGGER_NAME = "simulator.bronze"

#: The watermark is the upper bound (`as_of`) of the last window whose
#: extract Bronze committed: every source row with `updated_at <= watermark`
#: that was visible in the source at extraction time is in Bronze.
WATERMARK_SEMANTICS = "window_upper_bound"

RECONCILIATION_IDENTITY = (
    "source_rows_in_window + bad_rows_preserved == extracted_count == bronze_records_written"
)

# Attempt statuses (audit records).
STATUS_COMMITTED = "COMMITTED"
STATUS_ALREADY_COMMITTED = "ALREADY_COMMITTED"
STATUS_SKIPPED_UNCHANGED = "SKIPPED_UNCHANGED"
STATUS_UP_TO_DATE = "UP_TO_DATE"
STATUS_FAILED = "FAILED"
SUCCESS_STATUSES = frozenset({STATUS_COMMITTED, STATUS_ALREADY_COMMITTED, STATUS_SKIPPED_UNCHANGED, STATUS_UP_TO_DATE})

# Pipeline statuses.
PIPELINE_SUCCEEDED = "SUCCEEDED"
PIPELINE_PARTIAL = "PARTIAL"
PIPELINE_FAILED = "FAILED"

# Hook stages (fault injection points, see `Hook`).
STAGE_EXTRACT_WRITTEN = "extract_written"
STAGE_BRONZE_COMMITTED = "bronze_committed"
STAGE_WATERMARK_WRITTEN = "watermark_written"
STAGES: tuple[str, ...] = (STAGE_EXTRACT_WRITTEN, STAGE_BRONZE_COMMITTED, STAGE_WATERMARK_WRITTEN)


# -- errors ------------------------------------------------------------------


class IncrementalError(Exception):
    """Base class for loud DE-11 failures."""


class TransientError(IncrementalError):
    """A failure worth retrying (raise it from an injected dependency)."""


class AsOfError(IncrementalError):
    """`as_of` refused before anything is written (naive, in the future, or
    earlier than a dataset's current watermark)."""


class BadSourceError(IncrementalError):
    """Malformed source line(s) under `bad_source_policy = "strict"`."""


class ReconciliationError(IncrementalError):
    """`selected == extracted == Bronze records_written` does not hold."""


class ControlStateMismatchError(IncrementalError):
    """Control records and the Bronze store disagree (e.g. a crash after a
    Bronze commit followed by a rerun with a different `as_of`)."""


class LateArrivalError(IncrementalError):
    """Source rows appeared at/below the current watermark (or vanished)."""


#: Retried up to `retry.max_attempts`; everything else fails at once.
RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (TransientError, TimeoutError, ConnectionError)

#: `hook(stage, dataset, attempt)` is called after each step in `STAGES`.
#: Tests raise from it to inject failures/crashes (dependency injection, not
#: a production flag).
Hook = Callable[[str, str, int], None]
BronzeIngest = Callable[..., RunResult]
ExtractWriter = Callable[..., ExtractInfo]


# -- results -----------------------------------------------------------------


@dataclass(frozen=True)
class DatasetResult:
    dataset: str
    mode: str
    status: str
    attempts: int
    watermark_before: str | None
    watermark_after: str | None
    bronze_run_id: str | None
    bronze_status: str | None
    extracted_count: int | None
    error: dict[str, Any] | None

    @property
    def succeeded(self) -> bool:
        return self.status in SUCCESS_STATUSES


@dataclass(frozen=True)
class PipelineResult:
    pipeline_run_id: str
    status: str
    as_of: str
    datasets: tuple[DatasetResult, ...]
    summary_path: str  # relative to state_dir

    def dataset(self, name: str) -> DatasetResult:
        for result in self.datasets:
            if result.dataset == name:
                return result
        raise KeyError(name)


class ContextLogger(logging.LoggerAdapter):
    """Merges fixed context (always `pipeline_run_id`) into every record's
    `extra`, keeping the caller's own extra fields - so every log line of a
    run, including Bronze's, carries the run id (FU-09 not repeated)."""

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        kwargs["extra"] = {**self.extra, **(kwargs.get("extra") or {})}
        return msg, kwargs


# -- helpers -----------------------------------------------------------------


def bronze_run_id_for(config: IncrementalConfig, plan: DatasetPlan, low: datetime | None, high: datetime) -> str:
    """Deterministic Bronze run id for (dataset, window). Fits DE-05's
    128-character run-id alphabet for any valid dataset name/prefix."""
    if plan.is_full:
        return f"{config.full_run_id_prefix}-{plan.name}-{compact_ts(high)}"
    low_text = compact_ts(low) if low is not None else "initial"
    return f"{config.incremental_run_id_prefix}-{plan.name}-{low_text}-{compact_ts(high)}"


def _run_prefix(config: IncrementalConfig, plan: DatasetPlan) -> str:
    prefix = config.full_run_id_prefix if plan.is_full else config.incremental_run_id_prefix
    return f"{prefix}-{plan.name}-"


def pipeline_status(results: list[DatasetResult]) -> str:
    failed = sum(1 for r in results if not r.succeeded)
    if failed == 0:
        return PIPELINE_SUCCEEDED
    return PIPELINE_FAILED if failed == len(results) else PIPELINE_PARTIAL


# -- the loader --------------------------------------------------------------


class _Loader:
    def __init__(
        self,
        *,
        as_of: datetime,
        config: IncrementalConfig,
        clock: Clock,
        bronze_ingest: BronzeIngest,
        hook: Hook | None,
        extract_writer: ExtractWriter,
        logger: logging.Logger | None,
    ) -> None:
        self.as_of = require_aware(as_of, "as_of")
        self.config = config
        self.clock = clock
        self.bronze_ingest = bronze_ingest
        self.hook = hook
        self.extract_writer = extract_writer
        self.base_logger = logger if logger is not None else logging.getLogger(LOGGER_NAME)
        self.state_dir = Path(config.state_dir)
        self.control = ControlStore(self.state_dir)
        self.audit = AuditStore(self.state_dir)
        self.plan = resolve_plan(config)
        self.preserve_bad = config.bad_source_policy is BadSourcePolicy.PRESERVE
        bronze_config = load_bronze_config(config.bronze_config)
        self.bronze_config = bronze_config.model_copy(
            update={
                "output_dir": Path(config.bronze_output_dir),
                # strict -> Bronze refuses parse errors before commit too.
                "parse_error_policy": ParseErrorPolicy.PRESERVE if self.preserve_bad else ParseErrorPolicy.FAIL,
                # The extract manifest is ours, so it must always match.
                "verify_source_manifest": True,
            }
        )
        self.sanitize = PathSanitizer(
            {
                "state_dir": config.state_dir,
                "batch_source_dir": config.batch_source_dir,
                "reference_source_dir": config.reference_source_dir,
                "bronze_output_dir": config.bronze_output_dir,
                "cwd": Path.cwd(),
            }
        )
        self.pipeline_run_id = ""
        self.log = ContextLogger(self.base_logger, {"pipeline_run_id": ""})

    # -- pre-flight --------------------------------------------------------

    def preflight(self, pipeline_run_id: str | None) -> None:
        planned = pipeline_run_id or self.audit.next_pipeline_run_id(compact_ts(self.as_of))
        self.pipeline_run_id = planned
        self.log = ContextLogger(self.base_logger, {"pipeline_run_id": planned, "as_of": format_ts(self.as_of)})
        now = require_aware(self.clock.now(), "clock")
        if self.as_of > now:
            self._refuse(f"as_of {format_ts(self.as_of)} is later than the clock ({format_ts(now)})")
        ahead = []
        for plan in self.plan:
            try:
                record = self.control.read(plan.name)
            except (StateError, ValueError):
                continue  # surfaced as that dataset's failure
            if record is not None and parse_aware(record["watermark"], "watermark") > self.as_of:
                ahead.append(f"{plan.name} (watermark {record['watermark']})")
        if ahead:
            self._refuse(
                f"as_of {format_ts(self.as_of)} is earlier than the current watermark of: {', '.join(ahead)}"
            )
        self.audit.claim_pipeline_run(planned)

    def _refuse(self, message: str) -> None:
        self.log.error("pipeline run refused; no state changed", extra={"error": message})
        raise AsOfError(message)

    # -- run ---------------------------------------------------------------

    def run(self) -> PipelineResult:
        started = require_aware(self.clock.now(), "clock")
        self.log.info(
            "pipeline run started",
            extra={"datasets": [p.name for p in self.plan], "bad_source_policy": self.config.bad_source_policy.value},
        )
        results = [self._load_dataset(plan) for plan in self.plan]
        status = pipeline_status(results)
        ended = require_aware(self.clock.now(), "clock")
        summary = {
            "audit_schema_version": AUDIT_SCHEMA_VERSION,
            "record_type": "pipeline_run",
            "pipeline_run_id": self.pipeline_run_id,
            "as_of": format_ts(self.as_of),
            "status": status,
            "started_at": format_ts(started),
            "ended_at": format_ts(ended),
            "duration_seconds": (ended - started).total_seconds(),
            "watermark_semantics": WATERMARK_SEMANTICS,
            "bad_source_policy": self.config.bad_source_policy.value,
            "late_arrival_policy": self.config.late_arrival_policy.value,
            "dataset_count": len(results),
            "status_counts": {s: sum(1 for r in results if r.status == s) for s in sorted({r.status for r in results})},
            "datasets": [asdict(r) for r in results],
        }
        self.audit.write_pipeline_summary(summary)
        log = self.log.info if status == PIPELINE_SUCCEEDED else self.log.warning
        log("pipeline run finished", extra={"status": status, "status_counts": summary["status_counts"]})
        return PipelineResult(
            self.pipeline_run_id,
            status,
            format_ts(self.as_of),
            tuple(results),
            f"audit/{self.pipeline_run_id}/{PIPELINE_SUMMARY_FILE}",
        )

    def _load_dataset(self, plan: DatasetPlan) -> DatasetResult:
        max_attempts = self.config.retry.max_attempts
        first_before: str | None = None
        record: dict[str, Any] = {}
        for attempt in range(1, max_attempts + 1):
            record = self._new_record(plan, attempt)
            log = ContextLogger(self.log, {"dataset": plan.name, "attempt": attempt})
            log.info("dataset attempt started", extra={"mode": plan.mode.value})
            try:
                self._attempt(plan, attempt, record, log)
            except Exception as exc:  # noqa: BLE001 - every failure is audited, then re-classified
                retryable = isinstance(exc, RETRYABLE_ERRORS)
                will_retry = retryable and attempt < max_attempts
                backoff = self.config.retry.backoff_for(attempt) if will_retry else None
                record["status"] = STATUS_FAILED
                record["error"] = {
                    "type": type(exc).__name__,
                    "message": self.sanitize(str(exc)),
                    "retryable": retryable,
                    "will_retry": will_retry,
                }
                record["next_backoff_seconds"] = backoff
                record["watermark_after"] = self._current_watermark(plan)
                self._finish(record)
                if first_before is None:
                    first_before = record["watermark_before"]
                if will_retry:
                    log.warning(
                        "dataset attempt failed; retrying",
                        extra={"error": record["error"], "backoff_seconds": backoff},
                    )
                    self.clock.sleep(backoff or 0.0)
                    continue
                log.error("dataset failed; watermark unchanged", extra={"error": record["error"]})
                return self._result(plan, record, attempt, first_before)
            self._finish(record)
            if first_before is None:
                first_before = record["watermark_before"]
            log.info(
                "dataset attempt succeeded",
                extra={"status": record["status"], "watermark_after": record["watermark_after"]},
            )
            return self._result(plan, record, attempt, first_before)
        raise AssertionError("unreachable")  # pragma: no cover

    def _result(self, plan: DatasetPlan, record: dict[str, Any], attempts: int, before: str | None) -> DatasetResult:
        return DatasetResult(
            dataset=plan.name,
            mode=plan.mode.value,
            status=record["status"],
            attempts=attempts,
            watermark_before=before,
            watermark_after=record["watermark_after"],
            bronze_run_id=record["bronze_run_id"],
            bronze_status=record["bronze_status"],
            extracted_count=record["extracted_count"],
            error=record["error"],
        )

    def _current_watermark(self, plan: DatasetPlan) -> str | None:
        try:
            control = self.control.read(plan.name)
        except (StateError, ValueError):
            return None
        return control["watermark"] if control else None

    def _new_record(self, plan: DatasetPlan, attempt: int) -> dict[str, Any]:
        return {
            "audit_schema_version": AUDIT_SCHEMA_VERSION,
            "record_type": "dataset_attempt",
            "pipeline_run_id": self.pipeline_run_id,
            "dataset": plan.name,
            "mode": plan.mode.value,
            "attempt": attempt,
            "max_attempts": self.config.retry.max_attempts,
            "status": None,
            "started_at": format_ts(require_aware(self.clock.now(), "clock")),
            "ended_at": None,
            "duration_seconds": None,
            "as_of": format_ts(self.as_of),
            "watermark_semantics": WATERMARK_SEMANTICS,
            "watermark_column": plan.watermark_column,
            "window_low": None,
            "window_high": None,
            "watermark_before": None,
            "watermark_after": None,
            "bad_source_policy": self.config.bad_source_policy.value,
            "source_file": None,
            "source_sha256": None,
            "source_manifest_check": None,
            "source_rows_total": None,
            "source_blank_lines": None,
            "source_rows_in_window": None,
            "source_rows_before_window": None,
            "source_rows_after_window": None,
            "bad_source_rows": None,
            "bad_source_rows_by_reason": None,
            "bad_rows_preserved": None,
            "rejected_count": 0,
            "late_arrival_check": None,
            "extract_dir": None,
            "extract_sha256": None,
            "extracted_count": None,
            "bronze_run_id": None,
            "bronze_status": None,
            "bronze_manifest": None,
            "bronze_records_written": None,
            "bronze_parse_error_records": None,
            "bronze_replay_of": None,
            "recovered_bronze_run_id": None,
            "reconciliation": None,
            "error": None,
            "next_backoff_seconds": None,
        }

    def _finish(self, record: dict[str, Any]) -> None:
        ended = require_aware(self.clock.now(), "clock")
        record["ended_at"] = format_ts(ended)
        record["duration_seconds"] = (ended - parse_aware(record["started_at"])).total_seconds()
        self.audit.write_attempt(record)

    def _stage(self, stage: str, plan: DatasetPlan, attempt: int) -> None:
        if self.hook is not None:
            self.hook(stage, plan.name, attempt)

    # -- one attempt -------------------------------------------------------

    def _attempt(self, plan: DatasetPlan, attempt: int, rec: dict[str, Any], log: logging.LoggerAdapter) -> None:
        control = self.control.read(plan.name)
        if control is not None and control.get("mode") != plan.mode.value:
            raise ControlStateMismatchError(
                f"control record for {plan.name!r} is mode {control.get('mode')!r}, config says {plan.mode.value!r}"
            )
        before = parse_aware(control["watermark"], "watermark") if control else None
        rec["watermark_before"] = control["watermark"] if control else None
        rec["watermark_after"] = rec["watermark_before"]
        if before is not None and before > self.as_of:
            raise AsOfError(f"as_of is earlier than the {plan.name!r} watermark {control['watermark']}")  # type: ignore[index]
        if before is not None and before == self.as_of:
            self._check_bronze_consistency(plan, control, planned_run_id=None)
            rec["status"] = STATUS_UP_TO_DATE
            log.info("watermark already equals as_of; nothing to extract")
            return
        control, before = self._adopt_unrecorded_run(plan, control, before, rec, log)

        low = None if plan.is_full else before
        run_id = bronze_run_id_for(self.config, plan, low, self.as_of)
        rec["window_low"] = format_ts(low) if low is not None else None
        rec["window_high"] = format_ts(self.as_of)
        self._check_bronze_consistency(plan, control, planned_run_id=run_id)

        snapshot = self._read_source(plan)
        rec["source_file"] = snapshot.file_name
        rec["source_sha256"] = snapshot.sha256
        rec["source_manifest_check"] = snapshot.manifest_check
        rec["source_rows_total"] = len(snapshot.lines)
        rec["source_blank_lines"] = snapshot.blank_lines
        rec["bad_source_rows"] = len(snapshot.bad_lines)
        rec["bad_source_rows_by_reason"] = snapshot.bad_by_reason()
        if snapshot.bad_lines and not self.preserve_bad:
            rec["rejected_count"] = len(snapshot.bad_lines)
            first = snapshot.bad_lines[0]
            raise BadSourceError(
                f"{len(snapshot.bad_lines)} malformed line(s) in {snapshot.file_name} under bad_source_policy=strict "
                f"(first: line {first.line_number}: {first.bad_reason}); nothing landed"
            )

        if plan.is_full:
            selection = select_full(snapshot, preserve_bad=self.preserve_bad)
        else:
            self._check_late_arrivals(plan, control, snapshot, before, rec, log)
            preserved = dict(control.get("preserved_bad_lines") or {}) if control else {}
            selection = select_window(
                snapshot, low=low, high=self.as_of, preserved_bad_lines=preserved, preserve_bad=self.preserve_bad
            )
            rec["source_rows_before_window"] = selection.below_or_at_low
            rec["source_rows_after_window"] = selection.after_high
        rec["source_rows_in_window"] = selection.in_window
        rec["bad_rows_preserved"] = selection.bad_preserved
        expected_sha = hashlib.sha256(extract_bytes(selection.lines)).hexdigest()

        if plan.is_full and control is not None and control.get("snapshot_sha256") == expected_sha:
            rec["extracted_count"] = 0
            rec["reconciliation"] = {
                "identity": RECONCILIATION_IDENTITY,
                "applicable": False,
                "reason": "snapshot unchanged since the last committed full load; nothing landed",
                "holds": None,
            }
            self._write_control(plan, control, rec, run_id=None, selection_count=0, snapshot=snapshot,
                                extract_sha=None, snapshot_sha=expected_sha, preserved=None)
            rec["status"] = STATUS_SKIPPED_UNCHANGED
            log.info("reference snapshot unchanged; recorded no-op", extra={"snapshot_sha256": expected_sha})
            return

        # 4. extract
        directory = extract_dir(self.state_dir, plan.name, run_id)
        window = {"low": rec["window_low"], "high": rec["window_high"]}
        self.extract_writer(directory, plan, selection.lines, window=window, bronze_run_id=run_id)
        rec["extract_dir"] = directory.relative_to(self.state_dir).as_posix()
        on_disk_count, on_disk_sha = count_file_lines(directory / f"{plan.name}.jsonl")
        rec["extracted_count"] = on_disk_count
        rec["extract_sha256"] = on_disk_sha
        log.info("extract written", extra={"bronze_run_id": run_id, "rows": on_disk_count})
        self._stage(STAGE_EXTRACT_WRITTEN, plan, attempt)
        if on_disk_count != selection.count or on_disk_sha != expected_sha:
            rec["reconciliation"] = self._reconciliation(selection.in_window, selection.bad_preserved, on_disk_count, None)
            raise ReconciliationError(
                f"extract on disk ({on_disk_count} lines) does not match the {selection.count} selected source "
                "rows; nothing landed"
            )

        # 5. Bronze
        rec["bronze_run_id"] = run_id
        bronze_log = ContextLogger(
            logging.getLogger(BRONZE_LOGGER_NAME),
            {**self.log.extra, "dataset": plan.name, "attempt": attempt},
        )
        result = self.bronze_ingest(
            directory,
            run_id=run_id,
            output_dir=Path(self.config.bronze_output_dir),
            config=self.bronze_config,
            clock=self.clock.now,
            allow_replay=plan.is_full,
            logger=bronze_log,
        )
        table = result.manifest.get("tables", {}).get(plan.name) or {}
        rec["bronze_status"] = result.status
        rec["bronze_manifest"] = f"_runs/{run_id}.json"
        rec["bronze_records_written"] = table.get("records_written")
        rec["bronze_parse_error_records"] = table.get("parse_error_records")
        rec["bronze_replay_of"] = list(result.manifest.get("replay_of") or [])
        log.info("bronze ingest returned", extra={"bronze_run_id": run_id, "bronze_status": result.status})
        self._stage(STAGE_BRONZE_COMMITTED, plan, attempt)

        # 6. reconciliation identity
        rec["reconciliation"] = self._reconciliation(
            selection.in_window, selection.bad_preserved, on_disk_count, rec["bronze_records_written"]
        )
        if result.status not in (STATUS_COMMITTED, STATUS_ALREADY_COMMITTED) or not rec["reconciliation"]["holds"]:
            raise ReconciliationError(
                f"reconciliation failed for Bronze run {run_id} (status {result.status}): "
                f"{rec['reconciliation']}; watermark not advanced"
            )

        # 7. commit: advance the watermark
        preserved = None
        if not plan.is_full:
            preserved = dict(control.get("preserved_bad_lines") or {}) if control else {}
            for line in selection.lines:
                if line.is_bad:
                    preserved[line.sha256] = preserved.get(line.sha256, 0) + 1
        self._write_control(plan, control, rec, run_id=run_id, selection_count=selection.count, snapshot=snapshot,
                            extract_sha=on_disk_sha, snapshot_sha=expected_sha if plan.is_full else None,
                            preserved=preserved)
        rec["status"] = result.status
        log.info("watermark advanced", extra={"watermark_after": rec["watermark_after"], "bronze_run_id": run_id})
        self._stage(STAGE_WATERMARK_WRITTEN, plan, attempt)

    def _read_source(self, plan: DatasetPlan) -> SourceSnapshot:
        if plan.is_full:
            return read_reference_source(Path(self.config.reference_source_dir), plan)
        return read_incremental_source(
            Path(self.config.batch_source_dir),
            plan,
            manifest_file=self.config.source_manifest_file,
            verify_manifest=self.config.verify_source_manifest,
        )

    @staticmethod
    def _reconciliation(in_window: int, preserved: int, extracted: int, bronze: int | None) -> dict[str, Any]:
        selected = in_window + preserved
        return {
            "identity": RECONCILIATION_IDENTITY,
            "applicable": True,
            "source_rows_in_window": in_window,
            "bad_rows_preserved": preserved,
            "extracted_count": extracted,
            "bronze_records_written": bronze,
            "holds": bronze is not None and selected == extracted == bronze,
        }

    def _check_late_arrivals(
        self,
        plan: DatasetPlan,
        control: dict[str, Any] | None,
        snapshot: SourceSnapshot,
        before: datetime | None,
        rec: dict[str, Any],
        log: logging.LoggerAdapter,
    ) -> None:
        if control is None or before is None:
            return
        recorded = control.get("source_rows_at_or_below_watermark")
        current = count_at_or_below(snapshot, before)
        delta = None if recorded is None else current - recorded
        rec["late_arrival_check"] = {
            "recorded_rows_at_or_below_watermark": recorded,
            "current_rows_at_or_below_watermark": current,
            "delta": delta,
            "policy": self.config.late_arrival_policy.value,
            "holds": delta == 0,
        }
        if delta == 0 or delta is None:
            return
        message = (
            f"{plan.name}: {current} source rows have updated_at <= watermark {control['watermark']} but "
            f"{recorded} did when it was set (delta {delta:+d}); rows arriving behind the watermark are not "
            "extracted by a later window"
        )
        if self.config.late_arrival_policy is LateArrivalPolicy.FAIL:
            raise LateArrivalError(message + "; failing the dataset (late_arrival_policy=fail)")
        log.warning("late-arriving source rows recorded, not extracted", extra={"late_arrival": rec["late_arrival_check"]})

    def _control_record(
        self,
        plan: DatasetPlan,
        control: dict[str, Any] | None,
        *,
        watermark: datetime,
        window: dict[str, str | None] | None,
        run_id: str | None,
        rows_landed: int,
        extract_sha: str | None,
        snapshot_sha: str | None,
        rows_at_or_below: int | None,
        preserved: dict[str, int] | None,
    ) -> dict[str, Any]:
        """The next control record. `run_id is None` = no new Bronze run
        (SKIPPED_UNCHANGED): the last-load fields are carried over."""
        prior = control or {}
        runs = list(prior.get("committed_bronze_run_ids") or [])
        if run_id is not None and run_id not in runs:
            runs.append(run_id)
        return {
            "control_schema_version": CONTROL_SCHEMA_VERSION,
            "dataset": plan.name,
            "mode": plan.mode.value,
            "watermark_column": plan.watermark_column,
            "watermark_semantics": WATERMARK_SEMANTICS,
            "watermark": format_ts(watermark),
            "last_window": window if run_id is not None else prior.get("last_window"),
            "last_bronze_run_id": run_id if run_id is not None else prior.get("last_bronze_run_id"),
            "committed_bronze_run_ids": runs,
            "rows_landed_total": (prior.get("rows_landed_total") or 0) + rows_landed,
            "last_extract_sha256": extract_sha if run_id is not None else prior.get("last_extract_sha256"),
            "snapshot_sha256": snapshot_sha,
            "source_rows_at_or_below_watermark": None if plan.is_full else rows_at_or_below,
            "preserved_bad_lines": dict(sorted(preserved.items())) if preserved is not None else None,
        }

    def _write_control(
        self,
        plan: DatasetPlan,
        control: dict[str, Any] | None,
        rec: dict[str, Any],
        *,
        run_id: str | None,
        selection_count: int,
        snapshot: SourceSnapshot,
        extract_sha: str | None,
        snapshot_sha: str | None,
        preserved: dict[str, int] | None,
    ) -> None:
        record = self._control_record(
            plan,
            control,
            watermark=self.as_of,
            window={"low": rec["window_low"], "high": rec["window_high"]},
            run_id=run_id,
            rows_landed=selection_count,
            extract_sha=extract_sha,
            snapshot_sha=snapshot_sha,
            rows_at_or_below=None if plan.is_full else count_at_or_below(snapshot, self.as_of),
            preserved=preserved,
        )
        self.control.write(record)
        rec["watermark_after"] = record["watermark"]

    def _adopt_unrecorded_run(
        self,
        plan: DatasetPlan,
        control: dict[str, Any] | None,
        before: datetime | None,
        rec: dict[str, Any],
        log: logging.LoggerAdapter,
    ) -> tuple[dict[str, Any] | None, datetime | None]:
        """Crash recovery for a *later* `as_of`: a loader run that Bronze
        committed but whose watermark write never happened is adopted -
        control advanced to its window end - when it is exactly the next
        window (its low equals the current watermark, its high is before
        `as_of`) and its extract on disk matches the Bronze run (sha256 and
        record count). Anything else is left to `_check_bronze_consistency`,
        which refuses loudly."""
        prefix = _run_prefix(self.config, plan)
        manifests = {
            m["run_id"]: m
            for m in BronzeStore(Path(self.config.bronze_output_dir)).committed_manifests()
            if plan.name in m.get("tables", {}) and str(m.get("run_id", "")).startswith(prefix)
        }
        recorded = set((control or {}).get("committed_bronze_run_ids") or [])
        unrecorded = sorted(set(manifests) - recorded)
        if len(unrecorded) != 1:
            return control, before
        run_id = unrecorded[0]
        directory = extract_dir(self.state_dir, plan.name, run_id)
        try:
            extract_manifest = json.loads((directory / EXTRACT_MANIFEST_FILE).read_text(encoding="utf-8"))
            window = extract_manifest["window"]
            entry = extract_manifest["datasets"][plan.name]
            high = parse_aware(window["high"], "window high")
            low = parse_aware(window["low"], "window low") if window["low"] is not None else None
            data = (directory / entry["file"]).read_bytes()
        except (OSError, ValueError, KeyError, TypeError):
            return control, before
        expected_low = None if plan.is_full else before
        is_next_window = (
            extract_manifest.get("bronze_run_id") == run_id
            and run_id == bronze_run_id_for(self.config, plan, low, high)
            and low == expected_low
            and high < self.as_of
            and (before is None or high > before)
        )
        if not is_next_window:
            return control, before
        lines = split_lines(data)
        sha = hashlib.sha256(data).hexdigest()
        bronze = manifests[run_id]
        bronze_input = [f.get("sha256") for f in bronze.get("input_files", []) if f.get("table") == plan.name]
        if not (
            sha == entry.get("sha256")
            and len(lines) == entry.get("row_count") == bronze["tables"][plan.name].get("records_written")
            and bronze_input == [sha]
        ):
            raise ControlStateMismatchError(
                f"unrecorded Bronze run {run_id} for {plan.name!r} does not match its extract on disk; "
                "refusing to adopt it"
            )
        rows_at_or_below = None
        preserved = None
        if not plan.is_full:
            assert plan.watermark_column is not None
            classified = [classify_line(raw, n, plan.watermark_column) for n, raw in enumerate(lines, start=1)]
            rows_at_or_below = ((control or {}).get("source_rows_at_or_below_watermark") or 0) + sum(
                1 for line in classified if not line.is_bad
            )
            preserved = dict((control or {}).get("preserved_bad_lines") or {})
            for line in classified:
                if line.is_bad:
                    preserved[line.sha256] = preserved.get(line.sha256, 0) + 1
        record = self._control_record(
            plan,
            control,
            watermark=high,
            window={"low": window["low"], "high": format_ts(high)},
            run_id=run_id,
            rows_landed=len(lines),
            extract_sha=sha,
            snapshot_sha=sha if plan.is_full else None,
            rows_at_or_below=rows_at_or_below,
            preserved=preserved,
        )
        self.control.write(record)
        rec["recovered_bronze_run_id"] = run_id
        log.warning(
            "adopted a Bronze run committed by an interrupted earlier run; watermark advanced to its window end",
            extra={"bronze_run_id": run_id, "watermark": record["watermark"]},
        )
        return record, high

    def _check_bronze_consistency(
        self, plan: DatasetPlan, control: dict[str, Any] | None, *, planned_run_id: str | None
    ) -> None:
        """Duplicate/loss guard: every loader run of this dataset committed
        in Bronze must be recorded in the control record (except the run
        about to be (re)used - crash recovery), and vice versa."""
        prefix = _run_prefix(self.config, plan)
        in_bronze = {
            m["run_id"]
            for m in BronzeStore(Path(self.config.bronze_output_dir)).committed_manifests()
            if plan.name in m.get("tables", {}) and str(m.get("run_id", "")).startswith(prefix)
        }
        recorded = set(control.get("committed_bronze_run_ids") or []) if control else set()
        missing = sorted(recorded - in_bronze)
        if missing:
            raise ControlStateMismatchError(
                f"control record for {plan.name!r} lists Bronze run(s) {missing} that are not committed in the "
                "Bronze store; refusing to continue (restore the Bronze store or reset this dataset's control)"
            )
        unrecorded = sorted(in_bronze - recorded - ({planned_run_id} if planned_run_id else set()))
        if unrecorded:
            raise ControlStateMismatchError(
                f"Bronze has committed loader run(s) {unrecorded} for {plan.name!r} that the control record does "
                "not list (an earlier run probably stopped after its Bronze commit). Rerun with the same --as-of "
                "as that run (its window end is the last part of the run id) to recover; a different window "
                "would land overlapping rows"
            )


def run_incremental_load(
    as_of: datetime,
    *,
    config: IncrementalConfig | None = None,
    clock: Clock | None = None,
    bronze_ingest: BronzeIngest = ingest_batch,
    hook: Hook | None = None,
    extract_writer: ExtractWriter = write_extract,
    logger: logging.Logger | None = None,
    pipeline_run_id: str | None = None,
) -> PipelineResult:
    """Run one pipeline over every planned dataset for `as_of`.

    Raises `AsOfError` (nothing written) when `as_of` is naive, later than
    the clock, or earlier than any dataset's watermark, and `StateError` for
    a reused `pipeline_run_id`. Dataset failures do not raise: they are
    audited and reflected in `PipelineResult.status` (`PARTIAL`/`FAILED`).
    """
    try:
        aware_as_of = require_aware(as_of, "as_of")
    except ValueError as exc:
        raise AsOfError(str(exc)) from exc
    loader = _Loader(
        as_of=aware_as_of,
        config=config if config is not None else load_incremental_config(),
        clock=clock if clock is not None else SystemClock(),
        bronze_ingest=bronze_ingest,
        hook=hook,
        extract_writer=extract_writer,
        logger=logger,
    )
    loader.preflight(pipeline_run_id)
    return loader.run()


__all__ = [
    "AsOfError",
    "BadSourceError",
    "ControlStateMismatchError",
    "DatasetResult",
    "IncrementalError",
    "LateArrivalError",
    "PipelineResult",
    "ReconciliationError",
    "SourceError",
    "TransientError",
    "run_incremental_load",
]
