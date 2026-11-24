"""DE-04 referential integrity: every foreign key in every batch dataset
resolves to a real DE-01 reference entity or a real batch row, and the batch
datasets are internally consistent with each other (trips <-> schedules <->
stop times <-> telemetry <-> journeys <-> disruptions).

Checks run on dict rows so the same assertions cover both the in-memory
default-config dataset and the committed on-disk sample fixture."""

import pytest

from simulator.batch.config import load_batch_config
from simulator.batch.schemas import DATASET_CATALOGUE
from simulator.config import load_scale_config
from simulator.reference.generator import generate_reference_dataset
from simulator.tests._batch_helpers import (
    SAMPLE_OUTPUT_DIR,
    SAMPLE_SCALE_CONFIG,
    default_datasets,
    default_reference,
    generate,
    read_jsonl,
    rows_as_dicts,
    with_overrides,
)


def _default_tables():
    datasets = default_datasets()
    tables = {name: rows_as_dicts(datasets, name) for name in DATASET_CATALOGUE}
    return tables, default_reference().to_dict()


def _sample_tables():
    tables = {
        name: read_jsonl(SAMPLE_OUTPUT_DIR / spec.file_name)
        for name, spec in DATASET_CATALOGUE.items()
    }
    reference = generate_reference_dataset(load_scale_config(SAMPLE_SCALE_CONFIG))
    return tables, reference.to_dict()


def _delay_only_tables():
    base = load_batch_config()
    types = [t.model_dump(mode="python") for t in base.disruptions.types]
    cfg = with_overrides(
        base,
        batch={"service_days": 2},
        disruptions={"per_day": 8, "types": [t for t in types if t["effect"].value == "DELAY"]},
    )
    datasets, reference = generate(cfg)
    return {n: rows_as_dicts(datasets, n) for n in DATASET_CATALOGUE}, reference.to_dict()


SOURCES = {"default": _default_tables, "committed_sample": _sample_tables, "delay_only": _delay_only_tables}


@pytest.fixture(scope="module", params=sorted(SOURCES))
def tables(request):
    return SOURCES[request.param]()


def _target_keys(reference_target: str, batch_tables, reference) -> set:
    """`"<dataset>.<column>"` -> set of values, where dataset is a DE-01
    reference entity list (depots/stops/routes/vehicles) or a batch dataset."""
    dataset, column = reference_target.split(".")
    rows = reference[dataset] if dataset in reference else batch_tables[dataset]
    assert rows, f"FK target {reference_target} is empty"
    return {r[column] for r in rows}


def test_every_catalogued_foreign_key_resolves(tables):
    batch_tables, reference = tables
    assert set(reference) == {"depots", "stops", "routes", "vehicles"}
    checked = 0
    for name, spec in DATASET_CATALOGUE.items():
        for fk in spec.foreign_keys:
            target = _target_keys(fk.references, batch_tables, reference)
            for row in batch_tables[name]:
                value = row[fk.column]
                if value is None:
                    assert fk.nullable, f"{name}.{fk.column} is null but not nullable"
                    continue
                assert value in target, f"{name}.{fk.column}={value!r} not in {fk.references}"
                checked += 1
    assert checked > 0


def test_primary_keys_unique(tables):
    batch_tables, _ = tables
    for name, spec in DATASET_CATALOGUE.items():
        keys = [tuple(r[c] for c in spec.primary_key) for r in batch_tables[name]]
        assert len(keys) == len(set(keys)), f"duplicate primary key in {name}"


def test_historical_trips_are_one_to_one_with_scheduled_trips(tables):
    batch_tables, _ = tables
    schedules = {r["trip_id"]: r for r in batch_tables["schedules"]}
    trips = batch_tables["historical_trips"]
    assert {t["trip_id"] for t in trips} == set(schedules)
    for t in trips:
        s = schedules[t["trip_id"]]
        for column in (
            "route_id", "vehicle_id", "depot_id", "direction",
            "scheduled_departure", "scheduled_arrival", "event_date",
        ):
            assert t[column] == s[column], (t["trip_id"], column)
        assert t["stops_scheduled"] == s["stop_count"]


def test_schedule_stop_times_follow_the_route_stop_sequence(tables):
    batch_tables, reference = tables
    routes = {r["route_id"]: r for r in reference["routes"]}
    vehicles = {v["vehicle_id"]: v for v in reference["vehicles"]}
    by_trip: dict[str, list[dict]] = {}
    for st in batch_tables["schedule_stop_times"]:
        by_trip.setdefault(st["trip_id"], []).append(st)
    for s in batch_tables["schedules"]:
        stop_times = sorted(by_trip[s["trip_id"]], key=lambda r: r["stop_sequence"])
        expected = list(routes[s["route_id"]]["stop_ids"])
        if s["direction"] == "INBOUND":
            expected.reverse()
        assert [st["stop_id"] for st in stop_times] == expected
        assert [st["stop_sequence"] for st in stop_times] == list(range(1, len(expected) + 1))
        assert s["origin_stop_id"] == expected[0]
        assert s["destination_stop_id"] == expected[-1]
        assert s["stop_count"] == len(expected)
        assert stop_times[0]["scheduled_departure"] == s["scheduled_departure"]
        assert stop_times[-1]["scheduled_arrival"] == s["scheduled_arrival"]
        # Operating depot is the assigned vehicle's home depot.
        assert s["depot_id"] == vehicles[s["vehicle_id"]]["home_depot_id"]


def test_telemetry_snapshots_reference_real_trip_stops(tables):
    batch_tables, reference = tables
    stops = {s["stop_id"]: s for s in reference["stops"]}
    stop_times = {(st["trip_id"], st["stop_sequence"]): st for st in batch_tables["schedule_stop_times"]}
    schedules = {s["trip_id"]: s for s in batch_tables["schedules"]}
    trips = {t["trip_id"]: t for t in batch_tables["historical_trips"]}
    for e in batch_tables["historical_telemetry"]:
        st = stop_times[(e["trip_id"], e["stop_sequence"])]
        assert e["stop_id"] == st["stop_id"]
        assert (e["latitude"], e["longitude"]) == (
            stops[e["stop_id"]]["latitude"],
            stops[e["stop_id"]]["longitude"],
        )
        assert e["vehicle_id"] == schedules[e["trip_id"]]["vehicle_id"]
        assert e["route_id"] == schedules[e["trip_id"]]["route_id"]
        assert e["event_date"] == schedules[e["trip_id"]]["event_date"]
        assert trips[e["trip_id"]]["trip_status"] != "CANCELLED"
        assert e["stop_sequence"] <= trips[e["trip_id"]]["stops_served"]


def test_passenger_journeys_reference_real_operated_trips_and_stops(tables):
    batch_tables, _ = tables
    stop_times = {(st["trip_id"], st["stop_sequence"]): st for st in batch_tables["schedule_stop_times"]}
    trips = {t["trip_id"]: t for t in batch_tables["historical_trips"]}
    for j in batch_tables["passenger_journeys"]:
        trip = trips[j["trip_id"]]
        assert trip["trip_status"] != "CANCELLED"
        assert j["route_id"] == trip["route_id"]
        assert j["vehicle_id"] == trip["vehicle_id"]
        assert j["event_date"] == trip["event_date"]
        assert stop_times[(j["trip_id"], j["boarding_stop_sequence"])]["stop_id"] == j["boarding_stop_id"]
        assert stop_times[(j["trip_id"], j["alighting_stop_sequence"])]["stop_id"] == j["alighting_stop_id"]
        assert j["boarding_stop_sequence"] < j["alighting_stop_sequence"] <= trip["stops_served"]


def test_disruptions_are_consistent_with_trips_they_reference(tables):
    batch_tables, reference = tables
    routes = {r["route_id"]: r for r in reference["routes"]}
    vehicles = {v["vehicle_id"]: v for v in reference["vehicles"]}
    schedules = {s["trip_id"]: s for s in batch_tables["schedules"]}
    stop_times = {(st["trip_id"], st["stop_id"]) for st in batch_tables["schedule_stop_times"]}
    for d in batch_tables["service_disruptions"]:
        if d["scope"] == "TRIP":
            s = schedules[d["trip_id"]]
            assert d["vehicle_id"] == s["vehicle_id"]
            assert d["route_id"] == s["route_id"]
            assert d["depot_id"] == vehicles[d["vehicle_id"]]["home_depot_id"]
            assert (d["trip_id"], d["stop_id"]) in stop_times
        else:
            assert d["vehicle_id"] is None and d["trip_id"] is None and d["stop_id"] is None
            assert d["depot_id"] == routes[d["route_id"]]["home_depot_id"]


def test_every_non_completed_trip_is_explained_by_a_disruption(tables):
    batch_tables, _ = tables
    disruptions = {d["disruption_id"]: d for d in batch_tables["service_disruptions"]}
    for t in batch_tables["historical_trips"]:
        if t["trip_status"] == "COMPLETED":
            continue
        d = disruptions[t["disruption_id"]]
        assert d["scope"] == "TRIP"
        assert d["vehicle_id"] == t["vehicle_id"]
        if t["trip_status"] == "PARTIAL":
            assert d["effect"] == "BREAKDOWN" and d["trip_id"] == t["trip_id"]
