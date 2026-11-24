"""DE-07 local Gold layer GROUNDWORK: one minimal route-performance
aggregate (per route per period: valid-event count, average delay, valid
percentage) over one committed DE-06 Silver run, closing the local
Bronze -> Silver -> Gold chain.

Not the PROJECT.md section 10 dimensional model: no dim_*/fact_* tables, no
SCD Type 2, no Warehouse SQL, no Power BI - that is the later Warehouse story.

IMPLEMENTED - NOT CONNECTED: local files only; no Fabric Lakehouse,
Warehouse or OneLake resource is read or written. See `simulator/README.md`
"DE-07".
"""

from simulator.gold.aggregate import (
    COLUMNS,
    UNATTRIBUTED_REASONS,
    Periods,
    aggregate_route_performance,
    attribute_quarantined,
)
from simulator.gold.config import GoldConfig, load_gold_config
from simulator.gold.errors import (
    GoldAccountingError,
    GoldError,
    GoldInputError,
    GoldRunIdConflictError,
)
from simulator.gold.pipeline import (
    AGGREGATE_DEFINITION,
    RECONCILIATION_IDENTITIES,
    GoldRunResult,
    run_gold,
)

__all__ = [
    "AGGREGATE_DEFINITION",
    "COLUMNS",
    "RECONCILIATION_IDENTITIES",
    "UNATTRIBUTED_REASONS",
    "GoldAccountingError",
    "GoldConfig",
    "GoldError",
    "GoldInputError",
    "GoldRunIdConflictError",
    "GoldRunResult",
    "Periods",
    "aggregate_route_performance",
    "attribute_quarantined",
    "load_gold_config",
    "run_gold",
]
