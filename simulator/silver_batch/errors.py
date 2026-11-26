"""Loud DE-12 batch Silver failures. None of them leaves anything committed."""

from __future__ import annotations


class SilverBatchError(Exception):
    """Base class for every loud batch-Silver failure."""


class SilverBatchRefusedError(SilverBatchError):
    """Refused before anything was written (exit code 2): run-id conflict,
    unknown explicit Bronze run id, inconsistent Silver-batch store."""


class SilverBatchRunIdConflictError(SilverBatchRefusedError):
    """run_id already committed with different input/config; never overwritten."""


class SilverBatchInputError(SilverBatchRefusedError):
    """Requested Bronze input is unknown or not a committed batch run."""


class SilverBatchStateError(SilverBatchRefusedError):
    """The Silver-batch store itself is inconsistent (two commits claim the
    same sequence number, a base run is missing, ...)."""


class BronzeContractError(SilverBatchError):
    """A committed Bronze run disagrees with its own manifest (sha256, line
    count, lineage), or a DE-11 extract manifest disagrees with the Bronze
    run it describes."""


class SnapshotDeleteGuardError(SilverBatchError):
    """A full reference snapshot would soft-delete more than the configured
    fraction of the active rows (a truncated/empty snapshot is far more likely
    than a real mass deletion)."""


class SilverBatchAccountingError(SilverBatchError):
    """Internal self-check failed: per-dataset counters, row counts or lines
    on disk do not reconcile."""
