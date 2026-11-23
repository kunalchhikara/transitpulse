"""Basic schema tests for the Vehicle/Route/Stop/Depot reference entities."""

import pytest
from pydantic import ValidationError

from simulator.schemas.entities import (
    ENTITY_SCHEMA_VERSION,
    Depot,
    Route,
    Stop,
    Vehicle,
)


def test_depot_requires_core_fields_and_stamps_schema_version():
    depot = Depot(depot_id="DEPOT-01", name="Test Depot", latitude=-33.8, longitude=151.2, capacity=20)
    assert depot.schema_version == ENTITY_SCHEMA_VERSION


def test_depot_missing_required_field_fails_validation():
    with pytest.raises(ValidationError):
        Depot(name="Test Depot", latitude=-33.8, longitude=151.2, capacity=20)  # missing depot_id


def test_stop_invalid_longitude_fails_validation():
    with pytest.raises(ValidationError):
        Stop(stop_id="STOP-0001", name="Test Stop", latitude=-33.8, longitude=999.0)


def test_route_requires_at_least_two_stops():
    with pytest.raises(ValidationError):
        Route(
            route_id="ROUTE-01",
            route_number="100",
            name="Test Route",
            home_depot_id="DEPOT-01",
            stop_ids=["STOP-0001"],
        )


def test_vehicle_negative_capacity_fails_validation():
    with pytest.raises(ValidationError):
        Vehicle(
            vehicle_id="VEH-0001",
            fleet_number="0001",
            home_depot_id="DEPOT-01",
            assigned_route_id="ROUTE-01",
            capacity=-1,
        )
