"""Pydantic models for simulator scale and validation-threshold configuration.

These models exist so that `simulator/config/*.toml` is validated on load
(fail fast on a malformed config) and so every consumer gets typed,
documented access to configuration instead of raw dicts.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import AwareDatetime, BaseModel, Field, model_validator

#: Absolute timestamp bounds must leave every in-bounds instant representable
#: in UTC and at any local offset with a day of headroom (Silver standardises
#: to UTC; Gold labels periods of up to a day in local time). datetime.min/max
#: themselves are not: e.g. 0001-01-01T00:00+14:00 has no UTC equivalent.
REPRESENTABLE_MIN_TIMESTAMP = datetime(1, 1, 3, tzinfo=UTC)
REPRESENTABLE_MAX_TIMESTAMP = datetime(9999, 12, 29, tzinfo=UTC)


class BoundingBox(BaseModel):
    """A geographic bounding box used to constrain synthetic coordinates."""

    model_config = {"extra": "forbid"}

    min_latitude: float = Field(ge=-90.0, le=90.0)
    max_latitude: float = Field(ge=-90.0, le=90.0)
    min_longitude: float = Field(ge=-180.0, le=180.0)
    max_longitude: float = Field(ge=-180.0, le=180.0)

    @model_validator(mode="after")
    def _check_bounds(self) -> BoundingBox:
        if self.min_latitude >= self.max_latitude:
            raise ValueError("min_latitude must be strictly less than max_latitude")
        if self.min_longitude >= self.max_longitude:
            raise ValueError("min_longitude must be strictly less than max_longitude")
        return self

    def contains(self, latitude: float, longitude: float) -> bool:
        return (
            self.min_latitude <= latitude <= self.max_latitude
            and self.min_longitude <= longitude <= self.max_longitude
        )


class CountsConfig(BaseModel):
    """Reference-entity counts. Drives DE-01's count-matches-config criterion."""

    model_config = {"extra": "forbid"}

    vehicle_count: int = Field(gt=0)
    route_count: int = Field(gt=0)
    stop_count: int = Field(gt=0)
    stop_count_min: int = Field(gt=0)
    stop_count_max: int = Field(gt=0)
    depot_count: int = Field(gt=0)

    @model_validator(mode="after")
    def _check_stop_bounds(self) -> CountsConfig:
        if self.stop_count_min > self.stop_count_max:
            raise ValueError("stop_count_min must be <= stop_count_max")
        if not (self.stop_count_min <= self.stop_count <= self.stop_count_max):
            raise ValueError(
                "stop_count must fall within [stop_count_min, stop_count_max]"
            )
        return self


class FleetConfig(BaseModel):
    """Vehicle passenger-capacity range used when generating reference vehicles."""

    model_config = {"extra": "forbid"}

    min_vehicle_capacity: int = Field(gt=0)
    max_vehicle_capacity: int = Field(gt=0)

    @model_validator(mode="after")
    def _check_range(self) -> FleetConfig:
        if self.min_vehicle_capacity > self.max_vehicle_capacity:
            raise ValueError("min_vehicle_capacity must be <= max_vehicle_capacity")
        return self


class RouteShapeConfig(BaseModel):
    """How many stops a generated route visits."""

    model_config = {"extra": "forbid"}

    min_stops_per_route: int = Field(gt=1)
    max_stops_per_route: int = Field(gt=1)

    @model_validator(mode="after")
    def _check_range(self) -> RouteShapeConfig:
        if self.min_stops_per_route > self.max_stops_per_route:
            raise ValueError("min_stops_per_route must be <= max_stops_per_route")
        return self


class DepotShapeConfig(BaseModel):
    """Depot vehicle-bay capacity range."""

    model_config = {"extra": "forbid"}

    min_capacity: int = Field(gt=0)
    max_capacity: int = Field(gt=0)

    @model_validator(mode="after")
    def _check_range(self) -> DepotShapeConfig:
        if self.min_capacity > self.max_capacity:
            raise ValueError("min_capacity must be <= max_capacity")
        return self


class ScaleConfig(BaseModel):
    """Top-level scale configuration: `PROJECT.md` section 5."""

    model_config = {"extra": "forbid"}

    seed: int
    counts: CountsConfig
    sydney_bounding_box: BoundingBox
    fleet: FleetConfig
    route_shape: RouteShapeConfig
    depot: DepotShapeConfig


class ValidationThresholds(BaseModel):
    """Data-quality validation thresholds: `PROJECT.md` section 12.

    These are intentionally separate from the structural field constraints
    declared on `TelemetryEvent` itself (type, basic range) — these are the
    *business/operational* thresholds a later Silver-layer validator applies,
    and must stay config-driven so they can be tuned without code changes.
    """

    model_config = {"extra": "forbid"}

    max_speed_kmh: float = Field(gt=0)
    min_speed_kmh: float = Field(ge=0)
    max_occupancy: int = Field(gt=0)
    min_occupancy: int = Field(ge=0)
    min_heading_degrees: float
    max_heading_degrees: float
    future_time_tolerance_seconds: int = Field(ge=0)
    require_known_route: bool
    supported_schema_versions: list[str]
    required_fields: list[str]
    # DE-08 / BACKLOG FU-08: inclusive delay_seconds bounds.
    min_delay_seconds: int
    max_delay_seconds: int
    # DE-08 / BACKLOG FU-08: service area = ScaleConfig.sydney_bounding_box
    # expanded by this margin on every side (the box itself stays in
    # default_scale.toml - one source of truth for "Sydney").
    require_service_area: bool
    service_area_margin_degrees: float = Field(ge=0)
    # DE-08 / BACKLOG FU-10: inclusive absolute bounds on event_time and
    # generated_at (timezone-aware).
    min_timestamp: AwareDatetime
    max_timestamp: AwareDatetime

    @model_validator(mode="after")
    def _check_ranges(self) -> ValidationThresholds:
        if self.min_speed_kmh > self.max_speed_kmh:
            raise ValueError("min_speed_kmh must be <= max_speed_kmh")
        if self.min_occupancy > self.max_occupancy:
            raise ValueError("min_occupancy must be <= max_occupancy")
        if self.min_heading_degrees > self.max_heading_degrees:
            raise ValueError("min_heading_degrees must be <= max_heading_degrees")
        if not self.supported_schema_versions:
            raise ValueError("supported_schema_versions must not be empty")
        if self.min_delay_seconds > self.max_delay_seconds:
            raise ValueError("min_delay_seconds must be <= max_delay_seconds")
        if self.min_timestamp >= self.max_timestamp:
            raise ValueError("min_timestamp must be strictly before max_timestamp")
        if self.min_timestamp < REPRESENTABLE_MIN_TIMESTAMP or self.max_timestamp > REPRESENTABLE_MAX_TIMESTAMP:
            raise ValueError(
                f"timestamp bounds must lie within [{REPRESENTABLE_MIN_TIMESTAMP.isoformat()}, "
                f"{REPRESENTABLE_MAX_TIMESTAMP.isoformat()}] so every in-bounds instant is representable"
            )
        return self
