"""DE-11 local incremental batch loader (PROJECT.md section 11).

Reloads DE-01 reference entities in full, loads DE-04 transactional datasets
incrementally by their catalogue watermark column (`updated_at`), lands each
extract in Bronze through `simulator.bronze.ingest_batch`, and advances a
dataset's watermark only after Bronze returns COMMITTED/ALREADY_COMMITTED.
Every attempt is audited in an immutable record.

IMPLEMENTED - NOT CONNECTED: local files only; no Fabric Data Factory,
Lakehouse or any other Azure/Fabric resource is read or written. See
`simulator/README.md` "DE-11".
"""

from simulator.incremental.clock import (
    Clock,
    ManualClock,
    SystemClock,
    compact_ts,
    format_ts,
    parse_aware,
)
from simulator.incremental.config import (
    BadSourcePolicy,
    DatasetPlan,
    IncrementalConfig,
    LateArrivalPolicy,
    LoadMode,
    RetryConfig,
    load_incremental_config,
    resolve_plan,
)
from simulator.incremental.extract import (
    SourceError,
    SourceManifestMismatchError,
    write_extract,
)
from simulator.incremental.loader import (
    PIPELINE_FAILED,
    PIPELINE_PARTIAL,
    PIPELINE_SUCCEEDED,
    RECONCILIATION_IDENTITY,
    STAGE_BRONZE_COMMITTED,
    STAGE_EXTRACT_WRITTEN,
    STAGE_WATERMARK_WRITTEN,
    STAGES,
    STATUS_ALREADY_COMMITTED,
    STATUS_COMMITTED,
    STATUS_FAILED,
    STATUS_SKIPPED_UNCHANGED,
    STATUS_UP_TO_DATE,
    WATERMARK_SEMANTICS,
    AsOfError,
    BadSourceError,
    ControlStateMismatchError,
    DatasetResult,
    IncrementalError,
    LateArrivalError,
    PipelineResult,
    ReconciliationError,
    TransientError,
    bronze_run_id_for,
    run_incremental_load,
)
from simulator.incremental.state import AuditStore, ControlStore, StateError

__all__ = [
    "PIPELINE_FAILED",
    "PIPELINE_PARTIAL",
    "PIPELINE_SUCCEEDED",
    "RECONCILIATION_IDENTITY",
    "STAGES",
    "STAGE_BRONZE_COMMITTED",
    "STAGE_EXTRACT_WRITTEN",
    "STAGE_WATERMARK_WRITTEN",
    "STATUS_ALREADY_COMMITTED",
    "STATUS_COMMITTED",
    "STATUS_FAILED",
    "STATUS_SKIPPED_UNCHANGED",
    "STATUS_UP_TO_DATE",
    "WATERMARK_SEMANTICS",
    "AsOfError",
    "AuditStore",
    "BadSourceError",
    "BadSourcePolicy",
    "Clock",
    "ControlStateMismatchError",
    "ControlStore",
    "DatasetPlan",
    "DatasetResult",
    "IncrementalConfig",
    "IncrementalError",
    "LateArrivalError",
    "LateArrivalPolicy",
    "LoadMode",
    "ManualClock",
    "PipelineResult",
    "ReconciliationError",
    "RetryConfig",
    "SourceError",
    "SourceManifestMismatchError",
    "StateError",
    "SystemClock",
    "TransientError",
    "bronze_run_id_for",
    "compact_ts",
    "format_ts",
    "load_incremental_config",
    "parse_aware",
    "resolve_plan",
    "run_incremental_load",
    "write_extract",
]
