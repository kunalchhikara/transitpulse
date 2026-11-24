"""Reference-entity generation tests: determinism, config-driven counts,
coordinate bounding box, and referential integrity between entities."""

from simulator.config import load_scale_config
from simulator.reference.generator import generate_reference_dataset


def test_determinism_same_seed_byte_identical_output():
    config = load_scale_config()
    first = generate_reference_dataset(config).to_json()
    second = generate_reference_dataset(config).to_json()
    assert first == second
    # Explicit diff-is-empty framing per the story's test requirements.
    diff = [
        (a, b)
        for a, b in zip(first.splitlines(), second.splitlines())
        if a != b
    ]
    assert diff == []


def test_different_seed_produces_different_output():
    config = load_scale_config()
    baseline = generate_reference_dataset(config).to_json()
    other_config = config.model_copy(update={"seed": config.seed + 1})
    varied = generate_reference_dataset(other_config).to_json()
    assert baseline != varied


def test_counts_match_config():
    config = load_scale_config()
    dataset = generate_reference_dataset(config)
    assert len(dataset.vehicles) == config.counts.vehicle_count == 50
    assert len(dataset.routes) == config.counts.route_count == 10
    assert len(dataset.depots) == config.counts.depot_count == 4
    assert len(dataset.stops) == config.counts.stop_count
    assert 100 <= len(dataset.stops) <= 200


def test_counts_are_config_driven_not_hardcoded():
    config = load_scale_config()
    small_config = config.model_copy(
        update={
            "counts": config.counts.model_copy(
                update={
                    "vehicle_count": 5,
                    "route_count": 2,
                    "stop_count": 12,
                    "stop_count_min": 10,
                    "stop_count_max": 20,
                    "depot_count": 1,
                }
            )
        }
    )
    dataset = generate_reference_dataset(small_config)
    assert len(dataset.vehicles) == 5
    assert len(dataset.routes) == 2
    assert len(dataset.stops) == 12
    assert len(dataset.depots) == 1


def test_coordinates_within_sydney_bounding_box():
    config = load_scale_config()
    dataset = generate_reference_dataset(config)
    bbox = config.sydney_bounding_box

    for depot in dataset.depots:
        assert bbox.contains(depot.latitude, depot.longitude), depot
    for stop in dataset.stops:
        assert bbox.contains(stop.latitude, stop.longitude), stop


def test_referential_integrity_between_entities():
    config = load_scale_config()
    dataset = generate_reference_dataset(config)
    depot_ids = {d.depot_id for d in dataset.depots}
    stop_ids = {s.stop_id for s in dataset.stops}
    route_ids = {r.route_id for r in dataset.routes}

    for route in dataset.routes:
        assert route.home_depot_id in depot_ids
        assert set(route.stop_ids).issubset(stop_ids)
        assert len(route.stop_ids) == len(set(route.stop_ids)), "no duplicate stops on a route"

    for vehicle in dataset.vehicles:
        assert vehicle.home_depot_id in depot_ids
        assert vehicle.assigned_route_id in route_ids


def test_entity_ids_are_unique():
    config = load_scale_config()
    dataset = generate_reference_dataset(config)
    assert len({d.depot_id for d in dataset.depots}) == len(dataset.depots)
    assert len({s.stop_id for s in dataset.stops}) == len(dataset.stops)
    assert len({r.route_id for r in dataset.routes}) == len(dataset.routes)
    assert len({v.vehicle_id for v in dataset.vehicles}) == len(dataset.vehicles)
