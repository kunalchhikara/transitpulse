"""Loud DE-06 Silver failures. Every one of them leaves nothing committed."""

from __future__ import annotations


class SilverError(Exception):
    """Base class for every loud Silver failure."""


class SilverInputError(SilverError):
    """Requested Bronze input is missing, uncommitted, or disagrees with its
    committed manifest (sha256 / record count / lineage)."""


class SilverRunIdConflictError(SilverError):
    """run_id already committed with different input/config; never overwritten."""


class SilverAccountingError(SilverError):
    """Internal self-check failed: the buckets do not reconcile to the input."""


class ValidationContractError(SilverError):
    """DE-01 returned a rejection reason Silver cannot map to a rule."""
