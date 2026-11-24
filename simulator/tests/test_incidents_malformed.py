"""PROJECT.md section 13 item 1: bad/malformed data -- null vehicle IDs,
invalid coordinates, negative speeds, malformed timestamps, unknown routes,
missing fields."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from simulator.incidents.config import MalformedDataConfig
from simulator.schemas.telemetry import TelemetryEvent
from simulator.tests._incident_helpers import build_injector


def test_malformed_disabled_produces_structurally_valid_events():
    injector = build_injector()
    for tick in range(10):
        payload = injector.produce_event(tick)
        TelemetryEvent.model_validate(payload)  # does not raise


def test_malformed_enabled_round_robins_every_configured_mode_deterministically():
    """probability_per_tick=1.0 makes every tick malformed, deterministically
    cycling through `modes` in order -- proving each of the six anomalies
    this scenario covers is individually producible."""
    injector = build_injector(
        malformed=MalformedDataConfig(enabled=True, probability_per_tick=1.0)
    )

    payloads = [injector.produce_event(tick) for tick in range(6)]

    assert payloads[0]["vehicle_id"] is None

    assert payloads[1]["latitude"] == 999.0
    assert payloads[1]["longitude"] == -999.0
    with pytest.raises(ValidationError):
        TelemetryEvent.model_validate(payloads[1])

    assert payloads[2]["speed_kmh"] == -42.0
    with pytest.raises(ValidationError):
        TelemetryEvent.model_validate(payloads[2])

    assert payloads[3]["event_time"] == "not-a-timestamp"
    with pytest.raises(ValidationError):
        TelemetryEvent.model_validate(payloads[3])

    assert payloads[4]["route_id"] == "ROUTE-UNKNOWN-9999"
    # Structurally valid (route_id is just a str) -- "unknown route" is a
    # business-threshold rejection, not a structural one; validated instead
    # against the known-route set, mirroring validate_against_thresholds.
    TelemetryEvent.model_validate(payloads[4])
    known_route_ids = {f"ROUTE-{i:02d}" for i in range(1, 11)}
    assert payloads[4]["route_id"] not in known_route_ids

    assert "occupancy" not in payloads[5]
    with pytest.raises(ValidationError):
        TelemetryEvent.model_validate(payloads[5])


def test_malformed_each_mode_individually_configurable():
    """Each mode can be exercised on its own via a single-element `modes`
    list -- scenarios are independently configurable, not all-or-nothing."""
    for mode, check in [
        ("null_vehicle_id", lambda p: p["vehicle_id"] is None),
        ("invalid_coordinates", lambda p: p["latitude"] == 999.0 and p["longitude"] == -999.0),
        ("negative_speed", lambda p: p["speed_kmh"] == -42.0),
        ("malformed_timestamp", lambda p: p["event_time"] == "not-a-timestamp"),
        ("unknown_route", lambda p: p["route_id"] == "ROUTE-UNKNOWN-9999"),
        ("missing_field", lambda p: "occupancy" not in p),
    ]:
        injector = build_injector(
            malformed=MalformedDataConfig(enabled=True, probability_per_tick=1.0, modes=[mode])
        )
        payload = injector.produce_event(0)
        assert check(payload), f"mode {mode!r} did not produce the expected anomaly"


def test_malformed_rejects_unknown_mode_name():
    with pytest.raises(ValidationError):
        MalformedDataConfig(enabled=True, modes=["not_a_real_mode"])
