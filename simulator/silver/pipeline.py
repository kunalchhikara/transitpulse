"""DE-06 Silver run: Bronze telemetry -> clean / quarantine / duplicates,
with event-time watermarking, reference enrichment, PROJECT.md section 15
counters and a reconciliation identity asserted before commit.

Per-record flow, in arrival order (README "DE-06: Bucket ordering"):

1. Bronze parse-error record          -> QUARANTINE (`bronze_parse_error`)
2. validate (`rules.TelemetryValidator`) fails -> QUARANTINE (all failing rules)
3. valid, `event_id` already loaded   -> DUPLICATE (excluded, counted, audited)
4. valid, first `event_id`            -> CLEAN, flagged by the event-time
   watermark as `in_order` / `out_of_order` / `late` (late is loaded and
   flagged, never dropped)

So every Bronze record lands in exactly one of three buckets and
`extracted == valid_loaded + quarantined + duplicates_excluded`.

Each Silver run is a complete snapshot computed from the Bronze runs it
consumes (default: all committed runs of the stream table); dedup and
watermark state are scoped to the run. Store layout and commit protocol reuse
DE-05's layer-agnostic `BronzeStore` primitives by import (staging dir,
atomic rename, manifest written last as the commit marker).
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO, Any

from simulator.bronze.ingest import format_timestamp, system_clock
from simulator.bronze.records import (
    INGESTION_TIME,
    LINE_NUMBER,
    PARSE_ERROR,
    PARSE_ERROR_CODES,
    PARSE_ERROR_DETAIL,
    RAW_ENCODING,
    RAW_LINE,
    SOURCE_FILE,
    strip_metadata,
)
from simulator.bronze.store import (
    PART_FILE_NAME,
    STATUS_COMMITTED,
    STATUS_FAILED,
    BronzeStore,
    validate_run_id,
)
from simulator.config.loader import load_scale_config, load_validation_thresholds
from simulator.config.models import ScaleConfig, ValidationThresholds
from simulator.reference.generator import ReferenceDataset, generate_reference_dataset
from simulator.schemas.telemetry import TelemetryEvent
from simulator.silver.bronze_reader import BronzeRow, BronzeRun, iter_rows, select_runs
from simulator.silver.config import (
    FutureTimeReference,
    SilverConfig,
    load_silver_config,
)
from simulator.silver.errors import SilverAccountingError, SilverRunIdConflictError
from simulator.silver.rules import (
    CATEGORY_PARSE,
    CATEGORY_RULE,
    CATEGORY_SCHEMA,
    RULE_CATALOGUE,
    RULE_IDS,
    TelemetryValidator,
    Violation,
    WatermarkGuard,
    parse_error_violation,
)

LOGGER_NAME = "simulator.silver"
SILVER_MANIFEST_VERSION = "1.0.0"
STATUS_ALREADY_COMMITTED = "ALREADY_COMMITTED"

#: `_silver_event_time_status` values of a clean record.
IN_ORDER = "in_order"
OUT_OF_ORDER = "out_of_order"
LATE = "late"

#: Asserted in code before commit; recorded verbatim in every manifest.
RECONCILIATION_IDENTITIES: tuple[str, ...] = (
    "extracted == valid_loaded + quarantined + duplicates_excluded",
    "extracted == sum(bronze_runs_consumed[].records)",
    "quarantined == parse_errors + schema_failures + rule_failures == sum(quarantined_by_rule.values())",
    "valid_loaded == in_order + out_of_order + late",
    "lines in each written table == its bucket counter",
)

Clock = Callable[[], datetime]


@dataclass(frozen=True)
class SilverRunResult:
    status: str  # COMMITTED | ALREADY_COMMITTED
    run_id: str
    manifest: dict[str, Any]
    manifest_path: Path


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"clock must return a timezone-aware datetime, got {value!r}")
    return value.astimezone(UTC)


def _parse_utc(text: str) -> datetime:
    return _aware_utc(datetime.fromisoformat(text))


def _dumps_line(record: dict[str, Any]) -> str:
    """Strict: raises on a non-finite float or a lone surrogate. Clean and
    duplicate records cannot carry either (validation rejects them); the
    quarantine builder sanitises its parsed fields first (`_quarantine_record`)."""
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"


#: `raw_payload_omitted_reasons` values: why a quarantined object's parsed
#: form (`raw_payload`) is null although Bronze stored it as an object. Both
#: are valid JSON *syntax* that strict JSON / UTF-8 output cannot represent;
#: the exact original text stays in `raw_payload_text` (ASCII escapes), so the
#: record is still lossless.
OMITTED_NON_FINITE_NUMBER = "non_finite_number"  # e.g. 1e400 -> inf
OMITTED_UNPAIRED_SURROGATE = "unpaired_surrogate"  # e.g. "\ud800"


def _has_surrogate(text: str) -> bool:
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return True
    return False


def _escape_surrogates(text: str) -> str:
    """Diagnostic text only (rejection reasons): lone surrogates become their
    `\\udXXX` spelling so the reason stays writable as UTF-8."""
    return text.encode("utf-8", "backslashreplace").decode("utf-8")


def strict_json_problems(value: Any) -> list[str]:
    """Sorted `OMITTED_*` codes for the parts of a parsed JSON value that
    strict JSON (no NaN/inf) in UTF-8 cannot represent (keys included);
    empty if `_dumps_line` can write it."""
    problems: set[str] = set()
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, float):
            if not math.isfinite(item):
                problems.add(OMITTED_NON_FINITE_NUMBER)
        elif isinstance(item, str):
            if _has_surrogate(item):
                problems.add(OMITTED_UNPAIRED_SURROGATE)
        elif isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return sorted(problems)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


# -- event time ----------------------------------------------------------------


class EventTimeTracker:
    """Per-record event-time watermark over valid, first-seen events in
    arrival order: `watermark = max(event_time seen) - allowed_lateness`.

    * event_time >= max seen               -> in_order
    * watermark <= event_time < max seen   -> out_of_order (within lateness)
    * event_time < watermark               -> late
    Only events that pass validation and dedup advance the maximum (a
    corrupt future timestamp must not make everything after it "late"), and
    the DE-08 watermark guard (FU-06) keeps a valid-looking far-future event
    from advancing it too: quarantined by default, or observed with
    `advance=False` in `flag` mode."""

    def __init__(self, allowed_lateness_seconds: int) -> None:
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.max_event_time: datetime | None = None
        self.min_event_time: datetime | None = None

    @property
    def watermark(self) -> datetime | None:
        return None if self.max_event_time is None else self.max_event_time - self.allowed_lateness

    def observe(self, event_time: datetime, *, advance: bool = True) -> tuple[str, float, datetime | None]:
        """Classify `event_time`, then advance (unless `advance` is False: a
        guarded event is classified against the current state but never moves
        the maximum). Returns (status, lateness_seconds behind the max seen so
        far, watermark at arrival)."""
        watermark = self.watermark
        if self.max_event_time is None or event_time >= self.max_event_time:
            status, lateness = IN_ORDER, 0.0
        else:
            lateness = (self.max_event_time - event_time).total_seconds()
            status = LATE if event_time < watermark else OUT_OF_ORDER
        if advance and (self.max_event_time is None or event_time > self.max_event_time):
            self.max_event_time = event_time
        if self.min_event_time is None or event_time < self.min_event_time:
            self.min_event_time = event_time
        return status, lateness, watermark


# -- record builders -----------------------------------------------------------


@dataclass
class _Context:
    silver_run_id: str
    processing_time: str


def _quarantine_record(
    ctx: _Context, row: BronzeRow, seq: int, violations: Sequence[Violation]
) -> dict[str, Any]:
    record = row.record
    primary = violations[0]
    common = {
        "silver_run_id": ctx.silver_run_id,
        "processing_time": ctx.processing_time,
        **row.lineage(),
        "_silver_arrival_seq": seq,
        "primary_rule": primary.rule,
        "rejection_category": primary.category,
        "rejection_rules": [v.rule for v in violations],
        "rejections": [
            {key: _escape_surrogates(text) for key, text in v.as_dict().items()} for v in violations
        ],
    }
    if row.is_parse_error:
        return {
            **common,
            "event_id": None,
            "raw_format": "bronze_parse_error",
            "raw_payload": None,
            "raw_payload_omitted_reasons": [],
            "raw_payload_text": record[RAW_LINE],
            "raw_encoding": record[RAW_ENCODING],
            "bronze_parse_error": record[PARSE_ERROR],
            "bronze_parse_error_detail": record[PARSE_ERROR_DETAIL],
        }
    original = strip_metadata(record)
    event_id = original.get("event_id")
    # QA D1/D2 (PR #10): a parsed object holding an overflowed number (inf) or
    # a lone surrogate cannot be written as strict UTF-8 JSON; keep the exact
    # text, null the parsed form and say why, instead of aborting the run.
    omitted = strict_json_problems(original)
    return {
        **common,
        "event_id": event_id if isinstance(event_id, str) and not _has_surrogate(event_id) else None,
        "raw_format": "json_object",
        "raw_payload": None if omitted else original,
        "raw_payload_omitted_reasons": omitted,
        "raw_payload_text": row.original_text,
        "raw_encoding": "utf-8",
        "bronze_parse_error": None,
        "bronze_parse_error_detail": None,
    }


def _standardised_event(event: TelemetryEvent) -> dict[str, Any]:
    """Section 6 fields in their typed, standardised form: timestamps as UTC
    ISO-8601 with microseconds and `Z`; numbers as validated by the strict
    contract (float fields are floats, int fields ints); status as its enum
    value. Field order follows the DE-01 contract."""
    out: dict[str, Any] = {}
    for name in TelemetryEvent.model_fields:
        value = getattr(event, name)
        if isinstance(value, datetime):
            value = format_timestamp(value)
        elif hasattr(value, "value"):  # VehicleStatus
            value = value.value
        out[name] = value
    return out


# -- the run -----------------------------------------------------------------


@dataclass
class _Buckets:
    valid_loaded: int = 0
    quarantined: int = 0
    duplicates_excluded: int = 0
    duplicate_payload_conflicts: int = 0
    extracted: int = 0


def _reference_fingerprint(reference: ReferenceDataset) -> str:
    return hashlib.sha256(reference.to_json(indent=None).encode("utf-8")).hexdigest()


def _service_area(scale_config: ScaleConfig, thresholds: ValidationThresholds) -> dict[str, Any]:
    """What the DE-08 `service_area` rule checks against (manifest record)."""
    return {
        "bounding_box": scale_config.sydney_bounding_box.model_dump(mode="json"),
        "margin_degrees": thresholds.service_area_margin_degrees,
        "enabled": thresholds.require_service_area,
    }


def _fingerprint(
    config: SilverConfig,
    thresholds: ValidationThresholds,
    reference_sha: str,
    runs: Sequence[BronzeRun],
    service_area: dict[str, Any],
) -> str:
    return _sha256_json(
        {
            "silver": config.fingerprint_fields(),
            "thresholds": thresholds.model_dump(mode="json"),
            "reference_sha256": reference_sha,
            "service_area": service_area,
            "bronze_runs": [[run.run_id, run.sha256] for run in runs],
        }
    )


def _get_logger(logger: logging.Logger | None) -> logging.Logger:
    return logger if logger is not None else logging.getLogger(LOGGER_NAME)


def run_silver(
    *,
    run_id: str,
    bronze_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    bronze_run_ids: Sequence[str] | None = None,
    config: SilverConfig | None = None,
    thresholds: ValidationThresholds | None = None,
    scale_config: ScaleConfig | None = None,
    clock: Clock = system_clock,
    logger: logging.Logger | None = None,
) -> SilverRunResult:
    """Run Silver over the committed Bronze telemetry runs `bronze_run_ids`
    (default: all of them) and commit one Silver snapshot as `run_id`.

    Idempotency (README "DE-06: Idempotency"): same run_id + same input
    fingerprint already committed -> no-op `ALREADY_COMMITTED`; same run_id,
    different fingerprint -> `SilverRunIdConflictError`; same run_id after a
    FAILED/crashed attempt -> retried from scratch (the failed attempt is kept
    in `previous_attempts`)."""
    validate_run_id(run_id)
    config = config if config is not None else load_silver_config()
    thresholds = thresholds if thresholds is not None else load_validation_thresholds()
    scale_config = scale_config if scale_config is not None else load_scale_config()
    log = _get_logger(logger)
    reference = generate_reference_dataset(scale_config)
    reference_sha = _reference_fingerprint(reference)

    bronze = BronzeStore(Path(bronze_dir) if bronze_dir is not None else config.bronze_dir)
    runs = select_runs(bronze, config.bronze_table, bronze_run_ids)
    selection = "all_committed" if bronze_run_ids is None else "explicit"
    service_area = _service_area(scale_config, thresholds)
    fingerprint = _fingerprint(config, thresholds, reference_sha, runs, service_area)

    store = BronzeStore(Path(output_dir) if output_dir is not None else config.output_dir)
    log_ctx = {"silver_run_id": run_id}
    existing = store.read_manifest(run_id)
    previous_attempts: list[dict[str, Any]] = []
    if existing is not None:
        if existing.get("status") == STATUS_COMMITTED:
            if existing.get("input_fingerprint") == fingerprint:
                log.info("silver run already committed with identical input; no-op", extra=log_ctx)
                return SilverRunResult(STATUS_ALREADY_COMMITTED, run_id, existing, store.manifest_path(run_id))
            raise SilverRunIdConflictError(
                f"silver run_id {run_id!r} is already committed with different input/config "
                f"(committed fingerprint {existing.get('input_fingerprint')}, new {fingerprint}); "
                "committed runs are never overwritten - use a new run_id"
            )
        previous_attempts = [
            *existing.get("previous_attempts", []),
            {key: existing.get(key) for key in ("started_at", "ended_at", "input_fingerprint", "error")},
        ]

    started = _aware_utc(clock())
    processing_time = format_timestamp(started)
    ctx = _Context(silver_run_id=run_id, processing_time=processing_time)
    manifest: dict[str, Any] = {
        "silver_manifest_version": SILVER_MANIFEST_VERSION,
        "layer": "silver",
        "run_id": run_id,
        "status": STATUS_FAILED,
        "processing_time": processing_time,
        "started_at": processing_time,
        "ended_at": None,
        "duration_seconds": None,
        "input_fingerprint": fingerprint,
        "previous_attempts": previous_attempts,
        "bronze_table": config.bronze_table,
        "bronze_selection": selection,
        "bronze_runs_consumed": [
            {
                **run.as_dict(),
                "bronze_to_silver_latency_seconds": (started - _parse_utc(run.ingestion_time)).total_seconds(),
            }
            for run in runs
        ],
        "config": config.fingerprint_fields(),
        "validation_thresholds": thresholds.model_dump(mode="json"),
        "service_area": service_area,
        "reference_sha256": reference_sha,
        "rule_catalogue": [rule.as_dict() for rule in RULE_CATALOGUE],
        "tables": {},
        "counters": None,
        "event_time": None,
        "reconciliation": None,
        "removed_uncommitted_paths": [],
        "error": None,
    }
    log.info(
        "silver run started",
        extra={**log_ctx, "bronze_runs": [run.run_id for run in runs], "bronze_selection": selection},
    )

    try:
        manifest["removed_uncommitted_paths"] = store.reset_uncommitted(run_id)
        if manifest["removed_uncommitted_paths"]:
            log.warning(
                "removed uncommitted output of an earlier attempt",
                extra={**log_ctx, "paths": manifest["removed_uncommitted_paths"]},
            )
        tables = (config.clean_table, config.quarantine_table, config.duplicates_table)
        staging = store.staging_dir(run_id)
        handles: dict[str, IO[bytes]] = {}
        hashers = {table: hashlib.sha256() for table in tables}
        try:
            for table in tables:
                (staging / table).mkdir(parents=True, exist_ok=True)
                handles[table] = open(staging / table / PART_FILE_NAME, "wb")  # noqa: SIM115

            def write(table: str, record: dict[str, Any]) -> None:
                encoded = _dumps_line(record).encode("utf-8")
                handles[table].write(encoded)
                hashers[table].update(encoded)

            summary = _process(
                rows=iter_rows(bronze, runs),
                ctx=ctx,
                processing_time=started,
                config=config,
                validator=TelemetryValidator(
                    thresholds,
                    (route.route_id for route in reference.routes),
                    use_ingestion_time_reference=config.future_time_reference is FutureTimeReference.INGESTION_TIME,
                    service_area=scale_config.sydney_bounding_box,
                    watermark_guard=WatermarkGuard.from_config(config.watermark_guard),
                ),
                reference=reference,
                write=write,
                write_tables=tables,
                log=log,
                log_ctx=log_ctx,
            )
        finally:
            for handle in handles.values():
                handle.close()

        written = {table: (staging / table / PART_FILE_NAME).read_bytes().count(b"\n") for table in tables}
        counters = summary["counters"]
        reconciliation = reconcile(counters, runs, written, config)
        manifest["counters"] = counters
        manifest["event_time"] = summary["event_time"]
        manifest["reconciliation"] = reconciliation
        if not reconciliation["holds"]:
            raise SilverAccountingError(f"reconciliation failed: {reconciliation['checks']}")
        manifest["tables"] = {
            role: {
                "table": table,
                "path": f"{table}/run_id={run_id}/{PART_FILE_NAME}",
                "sha256": hashers[table].hexdigest(),
                "records": written[table],
            }
            for role, table in zip(("clean", "quarantine", "duplicates"), tables, strict=True)
        }

        for table in tables:
            store.publish_table(table, run_id)
        store.discard_staging(run_id)
        ended = _aware_utc(clock())
        manifest["ended_at"] = format_timestamp(ended)
        manifest["duration_seconds"] = (ended - started).total_seconds()
        manifest["counters"]["throughput_records_per_second"] = (
            round(counters["extracted"] / manifest["duration_seconds"], 3) if manifest["duration_seconds"] > 0 else None
        )
        manifest["status"] = STATUS_COMMITTED
        path = store.write_manifest(manifest)
    except Exception as exc:
        store.discard_staging(run_id)
        ended = _aware_utc(clock())
        manifest["status"] = STATUS_FAILED
        manifest["ended_at"] = format_timestamp(ended)
        manifest["duration_seconds"] = (ended - started).total_seconds()
        manifest["error"] = {"type": type(exc).__name__, "message": str(exc)}
        store.write_manifest(manifest)
        log.error("silver run failed; nothing committed", extra={**log_ctx, "error": manifest["error"]})
        raise

    log.info(
        "silver run committed",
        extra={
            **log_ctx,
            "manifest": path.name,
            **{k: counters[k] for k in ("extracted", "valid_loaded", "quarantined", "duplicates_excluded", "late")},
            "quality_percentage": counters["quality_percentage"],
        },
    )
    return SilverRunResult(STATUS_COMMITTED, run_id, manifest, path)


def _process(
    *,
    rows,
    ctx: _Context,
    processing_time: datetime,
    config: SilverConfig,
    validator: TelemetryValidator,
    reference: ReferenceDataset,
    write: Callable[[str, dict[str, Any]], None],
    write_tables: tuple[str, str, str],
    log: logging.Logger,
    log_ctx: dict[str, Any],
) -> dict[str, Any]:
    clean_table, quarantine_table, duplicates_table = write_tables
    routes = {route.route_id: route for route in reference.routes}
    vehicles = {vehicle.vehicle_id: vehicle for vehicle in reference.vehicles}
    tracker = EventTimeTracker(config.allowed_lateness_seconds)

    buckets = _Buckets()
    quarantined_by_rule: Counter[str] = Counter()
    rule_violations: Counter[str] = Counter()
    by_category: Counter[str] = Counter()
    parse_errors_by_code: Counter[str] = Counter()
    event_time_status: Counter[str] = Counter()
    enrichment_misses: Counter[str] = Counter()
    seen: dict[str, dict[str, Any]] = {}
    logged = 0
    watermark_guarded = 0

    for seq, row in enumerate(rows, start=1):
        buckets.extracted += 1
        guarded = False
        if row.is_parse_error:
            violations: Sequence[Violation] = (
                parse_error_violation(row.record[PARSE_ERROR], row.record.get(PARSE_ERROR_DETAIL)),
            )
            parse_errors_by_code[row.record[PARSE_ERROR]] += 1
            event = None
        else:
            outcome = validator.validate(
                strip_metadata(row.record),
                row.original_text,
                ingestion_time=_parse_utc(row.record[INGESTION_TIME]),
                processing_time=processing_time,
            )
            violations, event, guarded = outcome.violations, outcome.event, outcome.watermark_guarded

        if violations:
            buckets.quarantined += 1
            quarantined_by_rule[violations[0].rule] += 1
            by_category[violations[0].category] += 1
            rule_violations.update(v.rule for v in violations)
            write(quarantine_table, _quarantine_record(ctx, row, seq, violations))
            if logged < config.max_logged_rejections:
                logged += 1
                log.warning(
                    "record quarantined",
                    extra={
                        **log_ctx,
                        "bronze_run_id": row.run.run_id,
                        "file": row.record[SOURCE_FILE],
                        "line_number": row.record[LINE_NUMBER],
                        "rules": [v.rule for v in violations],
                    },
                )
            continue

        assert event is not None
        payload_sha = _sha256_json(strip_metadata(row.record))
        kept = seen.get(event.event_id)
        if kept is not None:
            buckets.duplicates_excluded += 1
            matches = kept["payload_sha256"] == payload_sha
            if not matches:
                buckets.duplicate_payload_conflicts += 1
            write(
                duplicates_table,
                {
                    "silver_run_id": ctx.silver_run_id,
                    "processing_time": ctx.processing_time,
                    "event_id": event.event_id,
                    **row.lineage(),
                    "_silver_arrival_seq": seq,
                    "payload_sha256": payload_sha,
                    "payload_matches_kept": matches,
                    "kept": {k: kept[k] for k in kept if k != "payload_sha256"},
                },
            )
            continue

        lineage = row.lineage()
        seen[event.event_id] = {
            "bronze_run_id": lineage["bronze_run_id"],
            SOURCE_FILE: lineage[SOURCE_FILE],
            LINE_NUMBER: lineage[LINE_NUMBER],
            "_silver_arrival_seq": seq,
            "payload_sha256": payload_sha,
        }
        status, lateness, watermark = tracker.observe(event.event_time.astimezone(UTC), advance=not guarded)
        event_time_status[status] += 1
        watermark_guarded += guarded
        route = routes.get(event.route_id)
        vehicle = vehicles.get(event.vehicle_id)
        if route is None:
            enrichment_misses["route_not_in_reference"] += 1
        if vehicle is None:
            enrichment_misses["vehicle_not_in_reference"] += 1
        buckets.valid_loaded += 1
        write(
            clean_table,
            {
                **_standardised_event(event),
                "ref_route_number": route.route_number if route else None,
                "ref_route_name": route.name if route else None,
                "ref_vehicle_home_depot_id": vehicle.home_depot_id if vehicle else None,
                **lineage,
                "silver_run_id": ctx.silver_run_id,
                "processing_time": ctx.processing_time,
                "_silver_arrival_seq": seq,
                "_silver_event_time_status": status,
                "_silver_lateness_seconds": lateness,
                "_silver_watermark": format_timestamp(watermark) if watermark is not None else None,
                "_silver_watermark_guarded": guarded,
            },
        )

    denominator = buckets.valid_loaded + buckets.quarantined
    counters: dict[str, Any] = {
        "extracted": buckets.extracted,
        "valid_loaded": buckets.valid_loaded,
        "quarantined": buckets.quarantined,
        "duplicates_excluded": buckets.duplicates_excluded,
        "duplicate_payload_conflicts": buckets.duplicate_payload_conflicts,
        "in_order": event_time_status[IN_ORDER],
        "out_of_order": event_time_status[OUT_OF_ORDER],
        "late": event_time_status[LATE],
        "watermark_guarded": watermark_guarded,
        "parse_errors": by_category[CATEGORY_PARSE],
        "schema_failures": by_category[CATEGORY_SCHEMA],
        "rule_failures": by_category[CATEGORY_RULE],
        "quarantined_by_rule": {rule: quarantined_by_rule[rule] for rule in RULE_IDS},
        "rule_violations": {rule: rule_violations[rule] for rule in RULE_IDS},
        "parse_errors_by_code": {code: parse_errors_by_code[code] for code in PARSE_ERROR_CODES},
        "enrichment_misses": {
            key: enrichment_misses[key] for key in ("route_not_in_reference", "vehicle_not_in_reference")
        },
        "quality_percentage": round(100.0 * buckets.valid_loaded / denominator, 4) if denominator else None,
    }
    unknown_codes = set(parse_errors_by_code) - set(PARSE_ERROR_CODES)
    if unknown_codes:
        counters["parse_errors_by_code"].update({code: parse_errors_by_code[code] for code in sorted(unknown_codes)})
    event_time = {
        "allowed_lateness_seconds": config.allowed_lateness_seconds,
        "future_time_reference": config.future_time_reference.value,
        "watermark_guard": config.watermark_guard.model_dump(mode="json"),
        "min_event_time": format_timestamp(tracker.min_event_time) if tracker.min_event_time else None,
        "max_event_time": format_timestamp(tracker.max_event_time) if tracker.max_event_time else None,
        "final_watermark": format_timestamp(tracker.watermark) if tracker.watermark else None,
    }
    return {"counters": counters, "event_time": event_time}


def reconcile(
    counters: dict[str, Any], runs: Sequence[BronzeRun], written: dict[str, int], config: SilverConfig
) -> dict[str, Any]:
    """Evaluate every `RECONCILIATION_IDENTITIES` entry from independently
    obtained numbers: bucket counters (incremented at classification), the
    Bronze manifests' record counts, and the lines actually on disk."""
    c = counters
    checks = {
        "extracted_equals_buckets": c["extracted"] == c["valid_loaded"] + c["quarantined"] + c["duplicates_excluded"],
        "extracted_equals_bronze_manifests": c["extracted"] == sum(run.records for run in runs),
        "quarantine_categories": c["quarantined"] == c["parse_errors"] + c["schema_failures"] + c["rule_failures"],
        "quarantine_rules": c["quarantined"] == sum(c["quarantined_by_rule"].values()),
        "valid_event_time_statuses": c["valid_loaded"] == c["in_order"] + c["out_of_order"] + c["late"],
        "clean_lines_on_disk": written[config.clean_table] == c["valid_loaded"],
        "quarantine_lines_on_disk": written[config.quarantine_table] == c["quarantined"],
        "duplicate_lines_on_disk": written[config.duplicates_table] == c["duplicates_excluded"],
    }
    return {
        "identities": list(RECONCILIATION_IDENTITIES),
        "checks": checks,
        "holds": all(checks.values()),
        "formula": (
            f"{c['extracted']} extracted == {c['valid_loaded']} valid_loaded + {c['quarantined']} quarantined "
            f"+ {c['duplicates_excluded']} duplicates_excluded"
        ),
    }
