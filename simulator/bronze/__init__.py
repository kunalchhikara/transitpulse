"""DE-05 local Bronze layer: ingest streaming-shaped (DE-02/DE-03 NDJSON) and
batch-shaped (DE-04) inputs, preserving every input field unmodified plus
ingestion metadata, with an audited, idempotent commit per run.

IMPLEMENTED - NOT CONNECTED: local files only; no Fabric Lakehouse/OneLake
resource is read or written. See `simulator/README.md` "DE-05".
"""

from simulator.bronze.config import BronzeConfig, ParseErrorPolicy, load_bronze_config
from simulator.bronze.ingest import (
    AccountingError,
    BronzeError,
    DuplicateInputError,
    InputError,
    MalformedInputError,
    RunIdConflictError,
    RunResult,
    SourceManifestError,
    fixed_clock,
    ingest_batch,
    ingest_stream,
    system_clock,
)
from simulator.bronze.store import BronzeStore, InvalidRunIdError

__all__ = [
    "AccountingError",
    "BronzeConfig",
    "BronzeError",
    "BronzeStore",
    "DuplicateInputError",
    "InputError",
    "InvalidRunIdError",
    "MalformedInputError",
    "ParseErrorPolicy",
    "RunIdConflictError",
    "RunResult",
    "SourceManifestError",
    "fixed_clock",
    "ingest_batch",
    "ingest_stream",
    "load_bronze_config",
    "system_clock",
]
