"""Movement-model tests: vehicle positions must progress plausibly along
their route -- no implausible teleporting between consecutive events for
the same vehicle -- and must actually advance over time (not stay frozen
forever)."""

import itertools
import random
from collections import defaultdict

from simulator.generator.config import load_generator_config
from simulator.generator.movement import advance_vehicle, haversine_km, initial_state
from simulator.schemas.entities import Route, Stop
from simulator.tests._generator_helpers import build_generator

_EVENT_COUNT = 4000


def _positions_by_vehicle(generator, count):
    positions: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    for event in generator.events(count):
        positions[event.vehicle_id].append(
            (event.latitude, event.longitude, event.speed_kmh)
        )
    return positions


def test_no_implausible_teleport_between_consecutive_events_same_vehicle():
    generator = build_generator()
    positions = _positions_by_vehicle(generator, _EVENT_COUNT)

    # Per-vehicle simulated seconds between successive updates, and the
    # maximum distance physically reachable in that time at the configured
    # cruise-speed ceiling (plus slack for interpolation/rounding).
    dt_hours = generator._dt_seconds() / 3600.0
    max_speed_kmh = generator.generator_config.cruise_speed_max_kmh
    max_plausible_km = max_speed_kmh * dt_hours * 1.05  # 5% slack for rounding

    violations = []
    for vehicle_id, samples in positions.items():
        for (lat1, lon1, _), (lat2, lon2, _) in itertools.pairwise(samples):
            a = Stop(stop_id="a", name="a", latitude=lat1, longitude=lon1)
            b = Stop(stop_id="b", name="b", latitude=lat2, longitude=lon2)
            distance_km = haversine_km(a, b)
            if distance_km > max_plausible_km:
                violations.append((vehicle_id, distance_km, max_plausible_km))

    assert violations == [], f"implausible teleport(s) detected: {violations[:5]}"


def test_vehicle_positions_actually_progress_over_time():
    generator = build_generator()
    positions = _positions_by_vehicle(generator, _EVENT_COUNT)

    moved_vehicle_count = 0
    for samples in positions.values():
        distinct_positions = {(round(lat, 6), round(lon, 6)) for lat, lon, _ in samples}
        if len(distinct_positions) > 1:
            moved_vehicle_count += 1

    assert moved_vehicle_count > 0, "no vehicle's position changed across the run"
    # Expect the overwhelming majority of vehicles to have moved given 4000
    # events spread round-robin across ~50 vehicles.
    assert moved_vehicle_count >= len(positions) * 0.9


def test_speed_never_negative_and_within_configured_cruise_ceiling():
    generator = build_generator()
    max_speed = generator.generator_config.cruise_speed_max_kmh
    for event in generator.events(_EVENT_COUNT):
        assert event.speed_kmh >= 0.0
        assert event.speed_kmh <= max_speed + 1e-6


def test_lap_number_increments_as_vehicle_completes_route_loops():
    """Unit-level test of the movement model itself (not the full
    default-scale generator, whose DE-01 reference stops are scattered
    randomly across the whole Sydney bounding box and can be tens of km
    apart -- unsuitable for a short, fast lap-completion test): a vehicle
    on a short two-stop loop must complete multiple laps within a bounded
    number of ticks, proving stop-sequence progression/looping is real,
    not just interpolation that never arrives."""
    stop_a = Stop(stop_id="A", name="A", latitude=-33.870, longitude=151.210)
    stop_b = Stop(stop_id="B", name="B", latitude=-33.871, longitude=151.211)
    route = Route(
        route_id="ROUTE-TEST",
        route_number="1",
        name="Test loop",
        home_depot_id="DEPOT-01",
        stop_ids=["A", "B"],
    )
    stops_by_id = {"A": stop_a, "B": stop_b}
    config = load_generator_config()
    rng = random.Random(123)

    state = initial_state(route)
    for _ in range(2000):
        state, _sample = advance_vehicle(
            state=state,
            route=route,
            stops_by_id=stops_by_id,
            rng=rng,
            dt_seconds=5.0,
            config=config,
        )
        if state.lap_number >= 3:
            break

    assert state.lap_number >= 3, "vehicle did not complete multiple laps of a short route"
