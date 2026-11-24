"""DE-04 semantic consistency / reconciliation between batch datasets:
counts that must agree across datasets, time ordering, occupancy derivation,
disruption effects and watermark semantics."""

import itertools
from collections import Counter, defaultdict
from datetime import timedelta

from simulator.batch.config import load_batch_config
from simulator.batch.schemas import DisruptionScope, TripStatus
from simulator.schemas.telemetry import VehicleStatus
from simulator.tests._batch_helpers import (
    default_datasets,
    default_reference,
    generate,
    with_overrides,
)


def _only_effect(effect: str, **extra):
    base = load_batch_config()
    types = [t.model_dump(mode="python") for t in base.disruptions.types]
    selected = [t for t in types if t["effect"].value == effect]
    return with_overrides(
        base,
        batch={"service_days": 2},
        disruptions={"types": selected, **extra},
    )


def test_trip_level_aggregates_reconcile_with_detail_datasets():
    ds = default_datasets()
    journeys = Counter(j.trip_id for j in ds.passenger_journeys)
    snapshots: dict[str, list] = defaultdict(list)
    for e in ds.historical_telemetry:
        snapshots[e.trip_id].append(e)
    for t in ds.historical_trips:
        assert t.passenger_boardings == journeys.get(t.trip_id, 0)
        trip_snaps = snapshots.get(t.trip_id, [])
        assert len(trip_snaps) == t.stops_served
        assert t.peak_occupancy == max((e.occupancy for e in trip_snaps), default=0)
        if trip_snaps:
            assert t.departure_delay_seconds == trip_snaps[0].delay_seconds
            assert t.actual_departure >= trip_snaps[0].event_time
    # Network-level identity: every telemetry snapshot belongs to an operated trip.
    assert sum(t.stops_served for t in ds.historical_trips) == len(ds.historical_telemetry)


def test_occupancy_is_derived_from_journeys_and_never_exceeds_capacity():
    ds = default_datasets()
    capacity = {v.vehicle_id: v.capacity for v in default_reference().vehicles}
    legs: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for j in ds.passenger_journeys:
        legs[j.trip_id].append((j.boarding_stop_sequence, j.alighting_stop_sequence))
    for e in ds.historical_telemetry:
        expected = sum(1 for b, a in legs[e.trip_id] if b < e.stop_sequence <= a)
        assert e.occupancy == expected
        assert e.occupancy <= capacity[e.vehicle_id]


def test_scheduled_and_actual_times_are_strictly_increasing():
    ds = default_datasets()
    stop_times: dict[str, list] = defaultdict(list)
    for st in ds.schedule_stop_times:
        stop_times[st.trip_id].append(st)
    scheduled_arrival = {(st.trip_id, st.stop_sequence): st.scheduled_arrival for st in ds.schedule_stop_times}
    for rows in stop_times.values():
        rows.sort(key=lambda r: r.stop_sequence)
        for prev, cur in itertools.pairwise(rows):
            assert cur.scheduled_arrival > prev.scheduled_departure
    snapshots: dict[str, list] = defaultdict(list)
    for e in ds.historical_telemetry:
        snapshots[e.trip_id].append(e)
        # delay_seconds is exactly actual - scheduled arrival at that stop.
        delta = e.event_time - scheduled_arrival[(e.trip_id, e.stop_sequence)]
        assert int(delta.total_seconds()) == e.delay_seconds
    for rows in snapshots.values():
        rows.sort(key=lambda r: r.stop_sequence)
        for prev, cur in itertools.pairwise(rows):
            assert cur.event_time > prev.event_time
    for j in ds.passenger_journeys:
        assert j.alighting_time > j.boarding_time


def test_no_vehicle_is_double_booked_at_default_scale():
    ds = default_datasets()
    assert ds.vehicle_overlap_count == 0
    layover = timedelta(minutes=load_batch_config().schedules.min_layover_minutes)
    by_vehicle: dict[tuple, list] = defaultdict(list)
    for s in ds.schedules:
        by_vehicle[(s.vehicle_id, s.event_date)].append(s)
    for trips in by_vehicle.values():
        trips.sort(key=lambda s: s.scheduled_departure)
        for prev, cur in itertools.pairwise(trips):
            assert cur.scheduled_departure >= prev.scheduled_arrival + layover


def test_affected_trip_counts_reconcile():
    ds = default_datasets()
    trips = ds.historical_trips
    for d in ds.service_disruptions:
        if d.scope == DisruptionScope.TRIP:
            expected = sum(1 for t in trips if t.disruption_id == d.disruption_id)
        else:
            expected = sum(
                1
                for t in trips
                if t.route_id == d.route_id
                and t.trip_status != TripStatus.CANCELLED
                and d.start_time <= t.scheduled_departure < d.end_time
            )
        assert d.affected_trip_count == expected, d.disruption_id


def test_breakdown_effect_produces_partial_trip_breakdown_status_and_withdrawal():
    cfg = _only_effect("BREAKDOWN", per_day=4)
    ds, _ = generate(cfg)
    assert len(ds.service_disruptions) == 8
    partial = [t for t in ds.historical_trips if t.trip_status == TripStatus.PARTIAL]
    assert len(partial) == 8
    breakdown_snaps = [e for e in ds.historical_telemetry if e.status == VehicleStatus.BREAKDOWN]
    assert len(breakdown_snaps) == 8
    disruptions = {d.disruption_id: d for d in ds.service_disruptions}
    last_snap = {}
    for e in ds.historical_telemetry:
        if e.trip_id not in last_snap or e.stop_sequence > last_snap[e.trip_id].stop_sequence:
            last_snap[e.trip_id] = e
    for t in partial:
        d = disruptions[t.disruption_id]
        snap = last_snap[t.trip_id]
        assert snap.status == VehicleStatus.BREAKDOWN
        assert snap.stop_id == d.stop_id
        assert d.start_time == snap.event_time
        assert t.actual_arrival is None and t.stops_served < t.stops_scheduled
    # Withdrawal: the vehicle's later trips departing inside the window are cancelled.
    for t in ds.historical_trips:
        if t.trip_status == TripStatus.CANCELLED:
            d = disruptions[t.disruption_id]
            assert d.effect.value == "BREAKDOWN" and d.vehicle_id == t.vehicle_id
            assert d.start_time <= t.scheduled_departure < d.end_time


def test_cancel_effect_cancels_trip_without_actuals():
    ds, _ = generate(_only_effect("CANCEL", per_day=3))
    cancelled = [t for t in ds.historical_trips if t.trip_status == TripStatus.CANCELLED]
    targets = {d.trip_id for d in ds.service_disruptions}
    assert targets <= {t.trip_id for t in cancelled}
    cancelled_ids = {t.trip_id for t in cancelled}
    assert not any(e.trip_id in cancelled_ids for e in ds.historical_telemetry)
    assert not any(j.trip_id in cancelled_ids for j in ds.passenger_journeys)
    assert not any(t.trip_status == TripStatus.PARTIAL for t in ds.historical_trips)


def test_delay_effect_adds_impact_to_trips_departing_in_window():
    ds, _ = generate(_only_effect("DELAY", per_day=6))
    assert all(t.trip_status == TripStatus.COMPLETED for t in ds.historical_trips)
    for t in ds.historical_trips:
        active = [
            d
            for d in ds.service_disruptions
            if d.route_id == t.route_id and d.start_time <= t.scheduled_departure < d.end_time
        ]
        assert t.disruption_delay_seconds == sum(d.estimated_delay_seconds for d in active)
        if active:
            assert t.disruption_id == min(active, key=lambda d: (d.start_time, d.disruption_id)).disruption_id
        else:
            assert t.disruption_id is None
    assert any(t.disruption_delay_seconds > 0 for t in ds.historical_trips)


def test_watermark_columns_have_source_system_semantics():
    """`updated_at` = when the record became final in the source system: a
    timetable is published before service; operated records settle after
    the facts they describe."""
    ds = default_datasets()
    for s in ds.schedules:
        assert s.updated_at < s.scheduled_departure
    for t in ds.historical_trips:
        assert t.updated_at > (t.actual_arrival or t.scheduled_departure)
    for e in ds.historical_telemetry:
        assert e.updated_at == e.event_time
    for j in ds.passenger_journeys:
        assert j.updated_at > j.alighting_time
    for d in ds.service_disruptions:
        assert d.updated_at > d.end_time
    for w in ds.weather_observations:
        assert w.updated_at == w.observed_at
    # Partition column spans exactly the configured service dates.
    cfg = load_batch_config()
    for name in ("schedules", "historical_trips", "historical_telemetry", "passenger_journeys",
                 "service_disruptions", "weather_observations"):
        assert {r.event_date for r in ds.rows(name)} == set(cfg.batch.service_dates), name
    # DE-10 assignments: recorded `notice_days` before they take effect at
    # local midnight of their (service-date) partition.
    for a in ds.vehicle_depot_assignments:
        assert a.updated_at < a.effective_from
        assert a.event_date in cfg.batch.service_dates
