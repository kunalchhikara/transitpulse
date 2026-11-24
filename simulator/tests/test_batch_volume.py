"""DE-04 volume is config-driven: row counts follow exact formulas derived
from the batch config and DE-01 scale config, and change when (only when)
the config changes."""

import pytest

from simulator.batch.config import load_batch_config
from simulator.batch.operations import InfeasibleDisruptionConfigError
from simulator.batch.schemas import DATASET_CATALOGUE
from simulator.config import load_scale_config
from simulator.tests._batch_helpers import (
    default_datasets,
    default_reference,
    generate,
    with_overrides,
)


def _expected_counts(datasets, reference, cfg):
    days = cfg.batch.service_days
    trips = len(reference.routes) * days * cfg.schedules.trips_per_route_per_day
    stops_by_route = {r.route_id: len(r.stop_ids) for r in reference.routes}
    return {
        "schedules": trips,
        "schedule_stop_times": sum(stops_by_route[s.route_id] for s in datasets.schedules),
        "historical_trips": trips,
        # One stop-arrival snapshot per stop actually served.
        "historical_telemetry": sum(t.stops_served for t in datasets.historical_trips),
        "service_disruptions": cfg.disruptions.per_day * days,
        "weather_observations": (
            len(reference.depots) * days * cfg.weather.observations_per_day
            if cfg.weather.enabled
            else 0
        ),
        # DE-10: one INITIAL row per vehicle + transfers_per_day per later date.
        "vehicle_depot_assignments": len(reference.vehicles)
        + (
            cfg.fleet_changes.transfers_per_day * (days - 1)
            if cfg.fleet_changes.enabled
            else 0
        ),
    }


def _assert_journey_bounds(datasets, reference, cfg):
    capacity = {v.vehicle_id: v.capacity for v in reference.vehicles}
    low = high = 0
    for t in datasets.historical_trips:
        if t.stops_served >= 2:
            cap = capacity[t.vehicle_id]
            low += min(cfg.passengers.min_journeys_per_trip, cap)
            high += min(cfg.passengers.max_journeys_per_trip, cap)
    assert low <= len(datasets.passenger_journeys) <= high


def test_default_config_volume_matches_formula():
    cfg = load_batch_config()
    datasets, reference = default_datasets(), default_reference()
    counts = datasets.counts()
    assert set(counts) == set(DATASET_CATALOGUE)
    for name, expected in _expected_counts(datasets, reference, cfg).items():
        assert counts[name] == expected, name
    # Concrete default numbers (10 routes x 7 days x 17 trips; 4 depots x 7 x 24).
    assert counts["schedules"] == 1190
    assert counts["service_disruptions"] == 21
    assert counts["weather_observations"] == 672
    assert counts["vehicle_depot_assignments"] == 56  # 50 vehicles + 1 x 6 later days
    _assert_journey_bounds(datasets, reference, cfg)
    assert all(count > 0 for count in counts.values())


@pytest.mark.parametrize(
    "overrides",
    [
        {"batch": {"service_days": 2}},
        {"batch": {"service_days": 3}, "schedules": {"headway_minutes": 30}},
        {"batch": {"service_days": 1}, "disruptions": {"per_day": 6}},
        {"batch": {"service_days": 2}, "weather": {"observation_interval_minutes": 180}},
        {"batch": {"service_days": 1}, "weather": {"enabled": False}},
        {"batch": {"service_days": 1}, "disruptions": {"per_day": 0}},
        {
            "batch": {"service_days": 1},
            "passengers": {"min_journeys_per_trip": 40, "max_journeys_per_trip": 60},
        },
    ],
)
def test_volume_follows_config(overrides):
    cfg = with_overrides(load_batch_config(), **overrides)
    datasets, reference = generate(cfg)
    counts = datasets.counts()
    for name, expected in _expected_counts(datasets, reference, cfg).items():
        assert counts[name] == expected, name
    _assert_journey_bounds(datasets, reference, cfg)


def test_zero_journeys_configurable():
    cfg = with_overrides(
        load_batch_config(),
        batch={"service_days": 1},
        passengers={"min_journeys_per_trip": 0, "max_journeys_per_trip": 0},
    )
    datasets, _ = generate(cfg)
    assert datasets.passenger_journeys == []
    assert all(t.passenger_boardings == 0 for t in datasets.historical_trips)
    assert all(e.occupancy == 0 for e in datasets.historical_telemetry)


def test_volume_scales_with_de01_scale_config():
    scale = load_scale_config()
    bigger = scale.model_copy(
        update={"counts": scale.counts.model_copy(update={"route_count": 12, "depot_count": 5})}
    )
    cfg = with_overrides(load_batch_config(), batch={"service_days": 1})
    datasets, reference = generate(cfg, bigger)
    assert len(reference.routes) == 12
    assert len(datasets.schedules) == 12 * cfg.schedules.trips_per_route_per_day
    assert len(datasets.weather_observations) == 5 * cfg.weather.observations_per_day


def test_infeasible_trip_disruption_volume_fails_loudly():
    # More trip-scoped disruptions per day than vehicles in the fleet.
    types = [t.model_dump(mode="python") for t in load_batch_config().disruptions.types]
    cancel_only = [dict(t, weight=1) for t in types if t["effect"].value == "CANCEL"]
    cfg = with_overrides(
        load_batch_config(),
        batch={"service_days": 1},
        disruptions={"per_day": 51, "types": cancel_only},
    )
    with pytest.raises(InfeasibleDisruptionConfigError):
        generate(cfg)
