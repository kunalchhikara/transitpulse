"""Loud DE-07 Gold failures. Every one of them leaves nothing committed."""

from __future__ import annotations


class GoldError(Exception):
    """Base class for every loud Gold failure."""


class GoldInputError(GoldError):
    """Requested Silver input is missing, uncommitted, or disagrees with its
    committed manifest (sha256 / record count / run id / counters), or Gold's
    reference data is not the reference data Silver used."""


class GoldRunIdConflictError(GoldError):
    """run_id already committed with different input/config; never overwritten."""


class GoldAccountingError(GoldError):
    """Internal self-check failed: the aggregate does not reconcile to Silver."""
