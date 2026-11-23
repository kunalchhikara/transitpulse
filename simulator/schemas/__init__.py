"""Pydantic schema definitions for TransitPulse reference entities and
telemetry events. See `simulator/README.md` for the field contract and
`schema_version` policy.
"""

from simulator.schemas.entities import Depot, Route, Stop, Vehicle
from simulator.schemas.telemetry import (
    CURRENT_SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
    TelemetryEvent,
    VehicleStatus,
)

__all__ = [
    "CURRENT_SCHEMA_VERSION",
    "SUPPORTED_SCHEMA_VERSIONS",
    "Depot",
    "Route",
    "Stop",
    "TelemetryEvent",
    "Vehicle",
    "VehicleStatus",
]
