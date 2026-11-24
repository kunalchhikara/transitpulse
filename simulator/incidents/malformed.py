"""PROJECT.md section 13 item 1: bad/malformed data.

`TelemetryEvent` (`simulator/schemas/telemetry.py`) structurally *cannot*
hold most of these values -- it is `extra="forbid"` with field-level
constraints (`vehicle_id: str` is required/non-null, `latitude`/`longitude`
are range-bound, `speed_kmh >= 0`, etc.), by design (DE-01/DE-02). That is
exactly why malformed-data injection operates on the event's *raw, JSON-
serializable dict representation* (`TelemetryEvent.model_dump(mode="json")`)
rather than on a `TelemetryEvent` instance: a malformed payload models what
a real malformed message looks like *on the wire*, before a downstream
Silver-layer validator (DE-06) parses and rejects/quarantines it -- it is
never a valid in-process `TelemetryEvent`.
"""

from __future__ import annotations

from typing import Any

EventPayload = dict[str, Any]

#: The six malformed-data sub-anomalies this module can produce, matching
#: `simulator.incidents.config.MALFORMED_MODES`.
MALFORMED_MODES: tuple[str, ...] = (
    "null_vehicle_id",
    "invalid_coordinates",
    "negative_speed",
    "malformed_timestamp",
    "unknown_route",
    "missing_field",
)

#: A route_id that `generate_reference_dataset` can never produce (route IDs
#: are always `ROUTE-\d+`) -- used by the `unknown_route` mode so the
#: anomaly is unambiguous regardless of scale config.
_UNKNOWN_ROUTE_ID = "ROUTE-UNKNOWN-9999"


def corrupt_payload(payload: EventPayload, mode: str) -> EventPayload:
    """Return a *new* dict: `payload` with exactly the one named malformation
    applied. Never mutates `payload` in place (callers may still hold/log
    the original)."""
    corrupted = dict(payload)
    if mode == "null_vehicle_id":
        corrupted["vehicle_id"] = None
    elif mode == "invalid_coordinates":
        corrupted["latitude"] = 999.0
        corrupted["longitude"] = -999.0
    elif mode == "negative_speed":
        corrupted["speed_kmh"] = -42.0
    elif mode == "malformed_timestamp":
        corrupted["event_time"] = "not-a-timestamp"
    elif mode == "unknown_route":
        corrupted["route_id"] = _UNKNOWN_ROUTE_ID
    elif mode == "missing_field":
        corrupted.pop("occupancy", None)
    else:
        raise ValueError(f"unknown malformed mode: {mode!r}")
    return corrupted
