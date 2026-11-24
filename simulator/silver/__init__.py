"""DE-06 local Silver layer for the telemetry stream: validation against every
PROJECT.md section 12 rule, quarantine with lossless raw payload,
deterministic `event_id` deduplication, event-time watermark (late /
out-of-order flagging), DE-01 reference enrichment and section 15 counters
with an asserted reconciliation identity.

Scope: the DE-02/DE-03 telemetry stream in Bronze only; Silver for the DE-04
batch datasets is out of scope for DE-06.

IMPLEMENTED - NOT CONNECTED: local files only; no Fabric Lakehouse/OneLake
resource is read or written. See `simulator/README.md` "DE-06".
"""

from simulator.silver.config import (
    FutureTimeReference,
    SilverConfig,
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
    IN_ORDER,
    LATE,
    OUT_OF_ORDER,
    RECONCILIATION_IDENTITIES,
    EventTimeTracker,
    SilverRunResult,
    run_silver,
)
from simulator.silver.rules import RULE_CATALOGUE, RULE_IDS, TelemetryValidator

__all__ = [
    "IN_ORDER",
    "LATE",
    "OUT_OF_ORDER",
    "RECONCILIATION_IDENTITIES",
    "RULE_CATALOGUE",
    "RULE_IDS",
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
    "load_silver_config",
    "run_silver",
]
