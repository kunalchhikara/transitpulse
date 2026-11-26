"""DE-07 Gold run: one committed Silver run -> the `route_performance`
aggregate (per route per period), with lineage, a reconciliation identity
asserted before commit and an idempotent, byte-reproducible commit.

Store layout and commit protocol reuse DE-05's layer-agnostic `BronzeStore`
primitives by import, exactly as Silver does (staging dir, atomic directory
rename, manifest written last as the commit marker):

    <output_dir>/
      route_performance/run_id=<id>/part-00000.jsonl   canonical aggregate
      route_performance/run_id=<id>/part-00000.csv     same rows as CSV (write_csv)
      _runs/<id>.json                                  run manifest: audit record AND commit marker
      _staging/<id>/...                                in-flight run, never read, removed on finish
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from collections.abc import Callable
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
from simulator.config.loader import load_scale_config
from simulator.config.models import ScaleConfig
from simulator.gold.aggregate import (
    COLUMNS,
    DECIMAL_PLACES,
    UNATTRIBUTED_REASONS,
    Aggregate,
    EventTimeCheck,
    Periods,
    aggregate_route_performance,
)
from simulator.gold.config import GoldConfig, load_gold_config
from simulator.gold.errors import (
    GoldAccountingError,
    GoldInputError,
    GoldRunIdConflictError,
)
from simulator.gold.silver_reader import (
    ROLE_CLEAN,
    ROLE_DUPLICATES,
    ROLE_QUARANTINE,
    SilverRun,
    read_silver_table,
    select_silver_run,
)
from simulator.reference.generator import ReferenceDataset, generate_reference_dataset
from simulator.silver.pipeline import LATE, OUT_OF_ORDER

LOGGER_NAME = "simulator.gold"
GOLD_MANIFEST_VERSION = "1.0.0"
STATUS_ALREADY_COMMITTED = "ALREADY_COMMITTED"
CSV_FILE_NAME = "part-00000.csv"

#: Recorded verbatim in every manifest (README "DE-07: Aggregate definition").
AGGREGATE_DEFINITION: dict[str, str] = {
    "grain": "one row per (route_id, period); a row exists iff the cell has >= 1 clean or attributed quarantined record",
    "period": "[period_start, period_end) of period_minutes, aligned to local midnight at utc_offset_hours; "
              "period_start/period_end in UTC, *_local in the fixed local offset",
    "event_count": "Silver clean records of the route with event_time in the period - late and out_of_order "
                   "rows INCLUDED; quarantined and duplicate records are never clean, so never counted",
    "avg_delay_seconds": f"sum_delay_seconds / event_count, rounded to {DECIMAL_PLACES} dp; null if event_count == 0",
    "quarantined_count": "Silver quarantine records attributed to the route and period (supported schema_version, "
                         "route_id is a reference route, event_time parseable with an offset, not rejected by a "
                         "Silver event-time rule, passing Gold's re-check of those rules with the thresholds "
                         "recorded in the Silver manifest, and not held back by Silver's watermark guard)",
    "valid_percentage": f"100 * event_count / (event_count + quarantined_count), rounded to {DECIMAL_PLACES} dp; "
                        "duplicates are not in the denominator",
}

#: Asserted in code before commit; recorded verbatim in every manifest.
RECONCILIATION_IDENTITIES: tuple[str, ...] = (
    "sum(rows.event_count) == clean records read == silver.valid_loaded",
    "quarantined_attributed + quarantined_unattributed == quarantine records read == silver.quarantined",
    "sum(rows.quarantined_count) == quarantined_attributed",
    "sum(unattributed_by_reason) == quarantined_unattributed",
    "sum(rows.late_event_count) == silver.late; sum(rows.out_of_order_event_count) == silver.out_of_order",
    "sum(rows.sum_delay_seconds) == sum(delay_seconds) over clean records read",
    "watermark_guarded_event_count (clean records read with _silver_watermark_guarded) == silver.watermark_guarded",
    "silver.extracted == silver.valid_loaded + silver.quarantined + silver.duplicates_excluded",
    "overall_valid_percentage == silver.quality_percentage",
    "lines in each written file == number of rows",
)

Clock = Callable[[], datetime]


@dataclass(frozen=True)
class GoldRunResult:
    status: str  # COMMITTED | ALREADY_COMMITTED
    run_id: str
    manifest: dict[str, Any]
    manifest_path: Path


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"clock must return a timezone-aware datetime, got {value!r}")
    return value.astimezone(UTC)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def reference_fingerprint(reference: ReferenceDataset) -> str:
    """Same fingerprint Silver records as `reference_sha256`."""
    return hashlib.sha256(reference.to_json(indent=None).encode("utf-8")).hexdigest()


def _fingerprint(config: GoldConfig, reference_sha: str, silver: SilverRun) -> str:
    return _sha256_json(
        {
            "gold": config.fingerprint_fields(),
            "reference_sha256": reference_sha,
            "silver_run": silver.run_id,
            "silver_input_fingerprint": silver.manifest.get("input_fingerprint"),
            "silver_tables": {role: silver.table(role)["sha256"] for role in (ROLE_CLEAN, ROLE_QUARANTINE, ROLE_DUPLICATES)},
        }
    )


def jsonl_bytes(rows: list[dict[str, Any]]) -> bytes:
    return "".join(
        json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n" for row in rows
    ).encode("utf-8")


def csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    """Header + one line per row, columns in `COLUMNS` order; null -> empty."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(COLUMNS)
    for row in rows:
        writer.writerow(["" if row[column] is None else row[column] for column in COLUMNS])
    return buffer.getvalue().encode("utf-8")


def _counters(agg: Aggregate, silver: SilverRun) -> dict[str, Any]:
    rows = agg.rows
    denominator = agg.clean_records + agg.quarantined_records
    return {
        "rows": len(rows),
        "routes": len({row["route_id"] for row in rows}),
        "periods": len({row["period_start"] for row in rows}),
        "event_count": sum(row["event_count"] for row in rows),
        "late_event_count": sum(row["late_event_count"] for row in rows),
        "out_of_order_event_count": sum(row["out_of_order_event_count"] for row in rows),
        "sum_delay_seconds": sum(row["sum_delay_seconds"] for row in rows),
        "quarantined_in_rows": sum(row["quarantined_count"] for row in rows),
        "quarantined_attributed": agg.quarantined_attributed,
        "quarantined_unattributed": agg.quarantined_unattributed,
        "unattributed_by_reason": {reason: agg.unattributed_by_reason[reason] for reason in UNATTRIBUTED_REASONS},
        "clean_records_read": agg.clean_records,
        "watermark_guarded_event_count": agg.clean_watermark_guarded,
        "clean_sum_delay_seconds": agg.clean_sum_delay_seconds,
        "quarantine_records_read": agg.quarantined_records,
        "duplicates_excluded": silver.counters["duplicates_excluded"],
        "overall_valid_percentage": (
            round(100.0 * agg.clean_records / denominator, DECIMAL_PLACES) if denominator else None
        ),
        "replay_sensitive": bool(silver.replayed_bronze_runs()),
    }


def reconcile(counters: dict[str, Any], silver: SilverRun, written: dict[str, int]) -> dict[str, Any]:
    """Evaluate every `RECONCILIATION_IDENTITIES` entry from independently
    obtained numbers: the row sums, the records actually read, the Silver
    manifest's counters, and the lines actually written to each staged file."""
    c, s = counters, silver.counters
    checks = {
        "event_count_equals_clean_read": c["event_count"] == c["clean_records_read"],
        "event_count_equals_silver_valid_loaded": c["event_count"] == s["valid_loaded"],
        "quarantine_read_equals_silver_quarantined": c["quarantine_records_read"] == s["quarantined"],
        "quarantine_attribution_complete": (
            c["quarantined_attributed"] + c["quarantined_unattributed"] == s["quarantined"]
        ),
        "rows_quarantined_equals_attributed": c["quarantined_in_rows"] == c["quarantined_attributed"],
        "unattributed_reasons_sum": sum(c["unattributed_by_reason"].values()) == c["quarantined_unattributed"],
        "late_equals_silver_late": c["late_event_count"] == s[LATE],
        "out_of_order_equals_silver": c["out_of_order_event_count"] == s[OUT_OF_ORDER],
        "delay_sum_preserved": c["sum_delay_seconds"] == c["clean_sum_delay_seconds"],
        "watermark_guarded_equals_silver": c["watermark_guarded_event_count"] == s.get("watermark_guarded"),
        "silver_identity": s["extracted"] == s["valid_loaded"] + s["quarantined"] + s["duplicates_excluded"],
        "overall_valid_percentage_equals_silver_quality": c["overall_valid_percentage"] == s["quality_percentage"],
        **{f"{name}_lines_on_disk": lines == c["rows"] for name, lines in written.items()},
    }
    return {
        "identities": list(RECONCILIATION_IDENTITIES),
        "checks": checks,
        "holds": all(checks.values()),
        "formula": (
            f"{c['event_count']} event_count == {s['valid_loaded']} silver valid_loaded; "
            f"{c['quarantined_attributed']} attributed + {c['quarantined_unattributed']} unattributed == "
            f"{s['quarantined']} silver quarantined; {s['duplicates_excluded']} duplicates excluded"
        ),
    }


def run_gold(
    *,
    run_id: str,
    silver_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    silver_run_id: str | None = None,
    config: GoldConfig | None = None,
    scale_config: ScaleConfig | None = None,
    clock: Clock = system_clock,
    logger: logging.Logger | None = None,
) -> GoldRunResult:
    """Aggregate one committed Silver run (`silver_run_id`, default the
    latest committed one) into `route_performance` and commit it as `run_id`.

    Idempotency: same run_id + same input fingerprint already committed ->
    no-op `ALREADY_COMMITTED`; same run_id, different fingerprint ->
    `GoldRunIdConflictError`; same run_id after a FAILED/crashed attempt ->
    retried from scratch (the failed attempt is kept in `previous_attempts`)."""
    validate_run_id(run_id)
    config = config if config is not None else load_gold_config()
    scale_config = scale_config if scale_config is not None else load_scale_config()
    log = logger if logger is not None else logging.getLogger(LOGGER_NAME)
    reference = generate_reference_dataset(scale_config)
    reference_sha = reference_fingerprint(reference)

    silver_root = Path(silver_dir) if silver_dir is not None else config.silver_dir
    silver, selection = select_silver_run(silver_root, silver_run_id)
    if silver.manifest.get("reference_sha256") != reference_sha:
        raise GoldInputError(
            f"Gold reference data (sha256 {reference_sha}) is not the reference data Silver run "
            f"{silver.run_id!r} used ({silver.manifest.get('reference_sha256')}); pass the same --scale-config"
        )
    # DE-08: the event-time rules Gold re-applies come from the Silver run's
    # own manifest (already covered by its input_fingerprint); a Silver run
    # that predates them is refused rather than guessed at (fail closed).
    event_time_check = EventTimeCheck.from_silver_manifest(silver.manifest)
    fingerprint = _fingerprint(config, reference_sha, silver)

    store = BronzeStore(Path(output_dir) if output_dir is not None else config.output_dir)
    log_ctx = {"gold_run_id": run_id}
    existing = store.read_manifest(run_id)
    previous_attempts: list[dict[str, Any]] = []
    if existing is not None:
        if existing.get("status") == STATUS_COMMITTED:
            if existing.get("input_fingerprint") == fingerprint:
                log.info("gold run already committed with identical input; no-op", extra=log_ctx)
                return GoldRunResult(STATUS_ALREADY_COMMITTED, run_id, existing, store.manifest_path(run_id))
            raise GoldRunIdConflictError(
                f"gold run_id {run_id!r} is already committed with different input/config "
                f"(committed fingerprint {existing.get('input_fingerprint')}, new {fingerprint}); "
                "committed runs are never overwritten - use a new run_id"
            )
        previous_attempts = [
            *existing.get("previous_attempts", []),
            {key: existing.get(key) for key in ("started_at", "ended_at", "input_fingerprint", "error")},
        ]

    started = _aware_utc(clock())
    processing_time = format_timestamp(started)
    table = config.route_performance_table
    manifest: dict[str, Any] = {
        "gold_manifest_version": GOLD_MANIFEST_VERSION,
        "layer": "gold",
        "run_id": run_id,
        "status": STATUS_FAILED,
        "processing_time": processing_time,
        "started_at": processing_time,
        "ended_at": None,
        "duration_seconds": None,
        "input_fingerprint": fingerprint,
        "previous_attempts": previous_attempts,
        "silver_selection": selection,
        "silver_runs_consumed": [
            {
                **silver.lineage(),
                "silver_to_gold_latency_seconds": (
                    started - _aware_utc(datetime.fromisoformat(silver.processing_time))
                ).total_seconds(),
            }
        ],
        "config": config.fingerprint_fields(),
        "reference_sha256": reference_sha,
        "aggregate_definition": AGGREGATE_DEFINITION,
        "attribution": event_time_check.as_dict(),
        "tables": {},
        "counters": None,
        "reconciliation": None,
        "removed_uncommitted_paths": [],
        "error": None,
    }
    log.info("gold run started", extra={**log_ctx, "silver_run_id": silver.run_id, "silver_selection": selection})
    if silver.replayed_bronze_runs():
        log.warning(
            "silver lineage contains DE-05 replay runs; quarantined counts include repeat deliveries (FU-07)",
            extra={**log_ctx, "replayed_bronze_runs": silver.replayed_bronze_runs()},
        )

    try:
        manifest["removed_uncommitted_paths"] = store.reset_uncommitted(run_id)
        if manifest["removed_uncommitted_paths"]:
            log.warning(
                "removed uncommitted output of an earlier attempt",
                extra={**log_ctx, "paths": manifest["removed_uncommitted_paths"]},
            )
        clean = read_silver_table(silver_root, silver, ROLE_CLEAN)
        quarantine = read_silver_table(silver_root, silver, ROLE_QUARANTINE)
        read_silver_table(silver_root, silver, ROLE_DUPLICATES)  # verified, never aggregated

        agg = aggregate_route_performance(
            clean,
            quarantine,
            periods=Periods(config.period_minutes, config.utc_offset_hours),
            reference_routes={route.route_id: (route.route_number, route.name) for route in reference.routes},
            lineage={"gold_run_id": run_id, "silver_run_id": silver.run_id, "processing_time": processing_time},
            event_time_check=event_time_check,
        )
        outputs = {PART_FILE_NAME: jsonl_bytes(agg.rows)}
        if config.write_csv:
            outputs[CSV_FILE_NAME] = csv_bytes(agg.rows)
        staged = store.staging_dir(run_id) / table
        staged.mkdir(parents=True, exist_ok=True)
        for name, data in outputs.items():
            (staged / name).write_bytes(data)
        written = {"jsonl": (staged / PART_FILE_NAME).read_bytes().count(b"\n")}
        if config.write_csv:
            written["csv"] = (staged / CSV_FILE_NAME).read_bytes().count(b"\n") - 1  # minus header

        counters = _counters(agg, silver)
        reconciliation = reconcile(counters, silver, written)
        manifest["counters"] = counters
        manifest["reconciliation"] = reconciliation
        if not reconciliation["holds"]:
            failed = [name for name, ok in reconciliation["checks"].items() if not ok]
            raise GoldAccountingError(f"reconciliation failed: {failed}")
        partition = f"{table}/run_id={run_id}"
        manifest["tables"] = {
            "route_performance": {
                "table": table,
                "path": f"{partition}/{PART_FILE_NAME}",
                "sha256": hashlib.sha256(outputs[PART_FILE_NAME]).hexdigest(),
                "records": len(agg.rows),
                "csv_path": f"{partition}/{CSV_FILE_NAME}" if config.write_csv else None,
                "csv_sha256": hashlib.sha256(outputs[CSV_FILE_NAME]).hexdigest() if config.write_csv else None,
                "columns": list(COLUMNS),
            }
        }

        store.publish_table(table, run_id)
        store.discard_staging(run_id)
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
        manifest["error"] = {"type": type(exc).__name__, "message": str(exc)}
        store.write_manifest(manifest)
        log.error("gold run failed; nothing committed", extra={**log_ctx, "error": manifest["error"]})
        raise

    log.info(
        "gold run committed",
        extra={
            **log_ctx,
            "manifest": path.name,
            **{k: counters[k] for k in ("rows", "event_count", "quarantined_attributed", "quarantined_unattributed")},
        },
    )
    return GoldRunResult(STATUS_COMMITTED, run_id, manifest, path)
