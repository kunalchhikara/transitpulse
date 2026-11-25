"""DE-06 local Silver layer for the telemetry stream: validation against every
PROJECT.md section 12 rule, quarantine with lossless raw payload,
deterministic `event_id` deduplication, event-time watermark (late /
out-of-order flagging) with the DE-08 stream-relative watermark guard, DE-01
reference enrichment and section 15 counters with an asserted reconciliation
identity.

Scope: the DE-02/DE-03 telemetry stream in Bronze only; Silver for the DE-04
batch datasets is out of scope for DE-06.

IMPLEMENTED - NOT CONNECTED: local files only; no Fabric Lakehouse/OneLake
resource is read or written. See `simulator/README.md` "DE-06".
"""

from simulator.silver.config import (
    ArrivalTimeReference,
    ArrivalTimeSkewConfig,
    ArrivalTimeSkewMode,
    FutureTimeReference,
    SilverConfig,
    WatermarkGuardConfig,
    WatermarkGuardMode,
    load_silver_config,
)
from simulator.silver.errors import (
    SilverAccountingError,
    SilverError,
    SilverInputError,
    SilverRunIdConflictError,
    ValidationContractError,
)
from simulator.silver.pipeline import (
    RECONCILIATION_IDENTITIES,
    SILVER_MANIFEST_VERSION,
    SilverRunResult,
    run_silver,
)
from simulator.silver.rules import (
    RULE_CATALOGUE,
    RULE_IDS,
    ArrivalTimeSkew,
    TelemetryValidator,
)
from simulator.silver.watermark import (
    IN_ORDER,
    LATE,
    OUT_OF_ORDER,
    EventTimeSequencer,
    EventTimeTracker,
    WatermarkGuard,
)

__all__ = [
    "IN_ORDER",
    "LATE",
    "OUT_OF_ORDER",
    "RECONCILIATION_IDENTITIES",
    "RULE_CATALOGUE",
    "RULE_IDS",
    "SILVER_MANIFEST_VERSION",
    "ArrivalTimeReference",
    "ArrivalTimeSkew",
    "ArrivalTimeSkewConfig",
    "ArrivalTimeSkewMode",
    "EventTimeSequencer",
    "EventTimeTracker",
    "FutureTimeReference",
    "SilverAccountingError",
    "SilverConfig",
    "SilverError",
    "SilverInputError",
    "SilverRunIdConflictError",
    "SilverRunResult",
    "TelemetryValidator",
    "ValidationContractError",
    "WatermarkGuard",
    "WatermarkGuardConfig",
    "WatermarkGuardMode",
    "load_silver_config",
    "run_silver",
]
