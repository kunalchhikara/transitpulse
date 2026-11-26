"""DE-12 local batch Silver: validate committed Bronze batch runs strictly
against the DE-04/DE-01 models, quarantine losslessly, merge valid rows into
current-state tables by catalogue primary key, and reconcile every run.

IMPLEMENTED - NOT CONNECTED: local files only; no Fabric Lakehouse/OneLake
resource is read or written. See `simulator/README.md` "DE-12".

Consumers (DE-13) read current-state tables only through committed
manifests: `head_manifest(output_dir)` and `read_current_table(output_dir,
dataset, run_id=...)`.
"""

from simulator.silver_batch.config import SilverBatchConfig, load_silver_batch_config
from simulator.silver_batch.datasets import (
    CATALOGUE_DATASETS,
    REFERENCE_DATASETS,
    SILVER_DATASETS,
    LoadMode,
    SilverDataset,
)
from simulator.silver_batch.errors import (
    BronzeContractError,
    SilverBatchAccountingError,
    SilverBatchError,
    SilverBatchInputError,
    SilverBatchRefusedError,
    SilverBatchRunIdConflictError,
    SilverBatchStateError,
    SnapshotDeleteGuardError,
)
from simulator.silver_batch.pipeline import (
    RECONCILIATION_IDENTITIES,
    STAGE_LOADED,
    STAGE_PUBLISHED,
    STAGE_STAGED,
    STATUS_ALREADY_COMMITTED,
    SilverBatchResult,
    run_silver_batch,
)
from simulator.silver_batch.rules import RULE_CATALOGUE, RULE_IDS
from simulator.silver_batch.store import (
    LINEAGE_COLUMNS,
    SILVER_COLUMNS,
    head_manifest,
    read_current_table,
)

__all__ = [
    "CATALOGUE_DATASETS",
    "LINEAGE_COLUMNS",
    "RECONCILIATION_IDENTITIES",
    "REFERENCE_DATASETS",
    "RULE_CATALOGUE",
    "RULE_IDS",
    "SILVER_COLUMNS",
    "SILVER_DATASETS",
    "STAGE_LOADED",
    "STAGE_PUBLISHED",
    "STAGE_STAGED",
    "STATUS_ALREADY_COMMITTED",
    "BronzeContractError",
    "LoadMode",
    "SilverBatchAccountingError",
    "SilverBatchConfig",
    "SilverBatchError",
    "SilverBatchInputError",
    "SilverBatchRefusedError",
    "SilverBatchResult",
    "SilverBatchRunIdConflictError",
    "SilverBatchStateError",
    "SilverDataset",
    "SnapshotDeleteGuardError",
    "head_manifest",
    "load_silver_batch_config",
    "read_current_table",
    "run_silver_batch",
]
