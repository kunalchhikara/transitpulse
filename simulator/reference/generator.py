"""Deterministic generation of TransitPulse reference entities.

Given the same `ScaleConfig` (and therefore the same `seed`), calling
`generate_reference_dataset` twice — in the same process or two separate
process runs — produces byte-identical serialized output. This is achieved
by:

1. Driving all randomness from a single `random.Random(seed)` instance.
2. Generating entity kinds in a fixed order (depots -> stops -> routes ->
   vehicles), each as a fixed-size loop over a config-driven count, so the
   sequence of calls made against the RNG is always identical for a given
   config.
3. Never reading wall-clock time, environment state, dict/set iteration
   order of non-deterministic collections, or anything else outside the
   seeded RNG and the config.

Display names (suburb-style labels) are flavor text drawn from a fixed
local list purely for readability; they do not affect any
operational/validation behaviour and are not a "scattered magic constant"
in the sense PROJECT.md section 19 warns against — only thresholds/counts
that affect behaviour are required to be externalized to config.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field

from simulator.config.models import ScaleConfig
from simulator.schemas.entities import Depot, Route, Stop, Vehicle

# Flavor-text suburb names for realistic-sounding Sydney-area entity names.
# Cycled deterministically if a count exceeds the list length. Not a
# behavioural/operational constant (see module docstring).
_SYDNEY_SUBURBS: tuple[str, ...] = (
    "Parramatta",
    "Chatswood",
    "Bondi",
    "Burwood",
    "Randwick",
    "Leichhardt",
    "Hurstville",
    "Manly",
    "Liverpool",
    "Blacktown",
    "Strathfield",
    "Ryde",
    "Penrith",
    "Sutherland",
    "Bankstown",
    "Castle Hill",
    "Mascot",
    "Marrickville",
    "North Sydney",
    "Rockdale",
)


def _suburb(index: int) -> str:
    return _SYDNEY_SUBURBS[index % len(_SYDNEY_SUBURBS)]


@dataclass
class ReferenceDataset:
    """The full generated set of reference entities for one config/seed."""

    depots: list[Depot] = field(default_factory=list)
    stops: list[Stop] = field(default_factory=list)
    routes: list[Route] = field(default_factory=list)
    vehicles: list[Vehicle] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "depots": [d.model_dump(mode="json") for d in self.depots],
            "stops": [s.model_dump(mode="json") for s in self.stops],
            "routes": [r.model_dump(mode="json") for r in self.routes],
            "vehicles": [v.model_dump(mode="json") for v in self.vehicles],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        """Stable, deterministic JSON serialization (sorted keys) suitable
        for the byte-identical-across-runs determinism check."""
        return json.dumps(self.to_dict(), sort_keys=True, indent=indent)


def _generate_depots(rng: random.Random, config: ScaleConfig) -> list[Depot]:
    bbox = config.sydney_bounding_box
    depots: list[Depot] = []
    for i in range(config.counts.depot_count):
        latitude = rng.uniform(bbox.min_latitude, bbox.max_latitude)
        longitude = rng.uniform(bbox.min_longitude, bbox.max_longitude)
        capacity = rng.randint(config.depot.min_capacity, config.depot.max_capacity)
        depots.append(
            Depot(
                depot_id=f"DEPOT-{i + 1:02d}",
                name=f"{_suburb(i)} Depot",
                latitude=latitude,
                longitude=longitude,
                capacity=capacity,
            )
        )
    return depots


def _generate_stops(rng: random.Random, config: ScaleConfig) -> list[Stop]:
    bbox = config.sydney_bounding_box
    stops: list[Stop] = []
    for i in range(config.counts.stop_count):
        latitude = rng.uniform(bbox.min_latitude, bbox.max_latitude)
        longitude = rng.uniform(bbox.min_longitude, bbox.max_longitude)
        stops.append(
            Stop(
                stop_id=f"STOP-{i + 1:04d}",
                name=f"{_suburb(i)} Stop {i + 1}",
                latitude=latitude,
                longitude=longitude,
            )
        )
    return stops


def _generate_routes(
    rng: random.Random,
    config: ScaleConfig,
    depots: list[Depot],
    stops: list[Stop],
) -> list[Route]:
    depot_ids = [d.depot_id for d in depots]
    stop_ids = [s.stop_id for s in stops]
    routes: list[Route] = []
    for i in range(config.counts.route_count):
        home_depot_id = rng.choice(depot_ids)
        num_stops = rng.randint(
            config.route_shape.min_stops_per_route,
            min(config.route_shape.max_stops_per_route, len(stop_ids)),
        )
        route_stop_ids = rng.sample(stop_ids, k=num_stops)
        route_number = f"{100 + i}"
        routes.append(
            Route(
                route_id=f"ROUTE-{i + 1:02d}",
                route_number=route_number,
                name=f"Route {route_number} ({_suburb(i)} line)",
                home_depot_id=home_depot_id,
                stop_ids=route_stop_ids,
            )
        )
    return routes


def _generate_vehicles(
    rng: random.Random,
    config: ScaleConfig,
    depots: list[Depot],
    routes: list[Route],
) -> list[Vehicle]:
    depot_ids = [d.depot_id for d in depots]
    route_ids = [r.route_id for r in routes]
    vehicles: list[Vehicle] = []
    for i in range(config.counts.vehicle_count):
        home_depot_id = rng.choice(depot_ids)
        assigned_route_id = rng.choice(route_ids)
        capacity = rng.randint(
            config.fleet.min_vehicle_capacity, config.fleet.max_vehicle_capacity
        )
        vehicles.append(
            Vehicle(
                vehicle_id=f"VEH-{i + 1:04d}",
                fleet_number=f"{i + 1:04d}",
                home_depot_id=home_depot_id,
                assigned_route_id=assigned_route_id,
                capacity=capacity,
            )
        )
    return vehicles


def generate_reference_dataset(config: ScaleConfig) -> ReferenceDataset:
    """Deterministically generate the full reference dataset for `config`.

    Same `config` (in particular the same `seed`) -> byte-identical
    `ReferenceDataset.to_json()` output, across separate process runs.
    """
    rng = random.Random(config.seed)
    depots = _generate_depots(rng, config)
    stops = _generate_stops(rng, config)
    routes = _generate_routes(rng, config, depots, stops)
    vehicles = _generate_vehicles(rng, config, depots, routes)
    return ReferenceDataset(depots=depots, stops=stops, routes=routes, vehicles=vehicles)
