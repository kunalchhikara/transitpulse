"""Deterministic reference-entity generation (vehicles, routes, stops,
depots) driven by `simulator/config/` and a seed. See
`simulator/README.md` for the determinism contract.
"""

from simulator.reference.generator import ReferenceDataset, generate_reference_dataset

__all__ = ["ReferenceDataset", "generate_reference_dataset"]
