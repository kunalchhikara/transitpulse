"""Reference (static/slowly-changing) domain entities: PROJECT.md section 5/7.

Vehicle, Route, Stop and Depot are the synthetic domain model that every
other simulator story (telemetry generation, incidents, batch datasets,
Bronze/Silver/Gold) is built on top of. Each carries `schema_version` so a
later shape change can be detected/migrated explicitly rather than silently.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

ENTITY_SCHEMA_VERSION = "1.0.0"


class Depot(BaseModel):
    """A physical depot/garage where vehicles are based overnight."""

    model_config = {"extra": "forbid"}

    schema_version: str = ENTITY_SCHEMA_VERSION
    depot_id: str
    name: str
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    capacity: int = Field(ge=0, description="Number of vehicle bays.")


class Stop(BaseModel):
    """A passenger stop that one or more routes may visit."""

    model_config = {"extra": "forbid"}

    schema_version: str = ENTITY_SCHEMA_VERSION
    stop_id: str
    name: str
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)


class Route(BaseModel):
    """A bus route: an ordered sequence of stops, based out of one depot."""

    model_config = {"extra": "forbid"}

    schema_version: str = ENTITY_SCHEMA_VERSION
    route_id: str
    route_number: str
    name: str
    home_depot_id: str
    stop_ids: list[str] = Field(min_length=2)


class Vehicle(BaseModel):
    """A physical vehicle, based out of one depot and nominally assigned to
    one route. (A real operator reassigns vehicles over time — that
    history is deliberately out of DE-01's scope; see `dim_vehicle` SCD2 in
    `PROJECT.md` section 10 for the later, full treatment.)
    """

    model_config = {"extra": "forbid"}

    schema_version: str = ENTITY_SCHEMA_VERSION
    vehicle_id: str
    fleet_number: str
    vehicle_type: str = "BUS"
    home_depot_id: str
    assigned_route_id: str | None = None
    capacity: int = Field(ge=0, description="Passenger capacity.")
