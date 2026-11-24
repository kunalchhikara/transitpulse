"""DE-10: `vehicle_depot_assignments` - effective-dated vehicle-to-depot
assignment history (SCD Type 2 source for `dim_vehicle`).

Covers the DE-10 acceptance criteria:
1. catalogue entry (PK, watermark, partition, FKs, change types);
2. one INITIAL row per DE-01 vehicle; TRANSFER rows deterministic,
   config-driven, always to a different depot, at a local service-date
   boundary, never two per vehicle per date; default has >= 3 transfers incl.
   a vehicle with >= 2; `updated_at` semantics;
3. the volume formula;
4. trips / schedules / trip-scoped disruptions follow the vehicle's
   effective depot on their `event_date` (100% of rows);
5. fleet changes disabled -> every pre-existing dataset file is
   byte-identical to the pre-DE-10 output (pinned SHA-256 digests).
(6 - determinism / prefix stability / committed-sample reproducibility /
referential integrity - is covered for every catalogued dataset, including
this one, by the existing `test_batch_determinism.py` and
`test_batch_referential_integrity.py`.)
"""

from __future__ import annotations

import hashlib
import itertools
from collections import Counter, defaultdict
from datetime import timedelta

import pytest
from pydantic import ValidationError

from simulator.batch.config import (
    FLEET_CHANGES_DISABLED,
    BatchConfig,
    load_batch_config,
)
from simulator.batch.fleet import InfeasibleFleetChangeConfigError
from simulator.batch.generator import serialize_row
from simulator.batch.schemas import (
    DATASET_CATALOGUE,
    AssignmentChangeType,
    VehicleDepotAssignment,
)
from simulator.batch.timetable import local_midnight_utc
from simulator.batch.writer import write_batch_datasets
from simulator.config import load_scale_config
from simulator.tests._batch_helpers import (
    SAMPLE_BATCH_CONFIG,
    SAMPLE_OUTPUT_DIR,
    SAMPLE_SCALE_CONFIG,
    default_datasets,
    default_reference,
    effective_depot_lookup,
    generate,
    read_jsonl,
    rows_as_dicts,
    with_overrides,
)

DATASET = "vehicle_depot_assignments"
#: Datasets carrying a vehicle-attributed `depot_id` that must follow transfers.
DEPOT_ATTRIBUTED = ("schedules", "historical_trips", "service_disruptions")
PRE_DE10_DATASETS = [name for name in DATASET_CATALOGUE if name != DATASET]

#: SHA-256 of every dataset file written by `python -m simulator.batch` with the
#: default batch + scale configs at commit 295f7db (main, before DE-10).
PRE_DE10_DEFAULT_SHA256 = {
    "historical_telemetry": "cbb0f74955cf21349cacec6ea0136cf4ab6adfeaaba4a365e404370b03141144",
    "historical_trips": "1914a48f6ae94b07cc5693575996eb3468b3876005d84d8b571fb450bf5c7b20",
    "passenger_journeys": "790b2893c9c8e16401db85573ce0be0c01470ef9d58bc83cfb2aab6d2f87d4c8",
    "schedule_stop_times": "a144c601fffa1dd7c965852bec11dcdbbbc7cd884eebf74e557332e9d6714a32",
    "schedules": "4c8d593b2241209bfc911c68a56072d8cbcc881cbf40d243835b01c16f9383ec",
    "service_disruptions": "9d7d61484afb8a71b5175a84333c8b4e22de0751fe7f09f2e48fe2717296b010",
    "weather_observations": "c77466ec140c3114a6dd2c641d9c60de7d02e6e06f54911bdd4bc9eff586763f",
}
#: SHA-256 of the committed 1-day sample files at 295f7db (pre-DE-10
#: `sample/output/manifest.json`), generated from the sample configs with
#: `service_days = 1`.
PRE_DE10_SAMPLE_SHA256 = {
    "historical_telemetry": "3af8823ff3a82fa40a4604dbfd2e5dc8b4f9b95c157183c959f498217e5857b3",
    "historical_trips": "c8976d196fd66fa9ad38d7fc97e51c60375b5f12e90cbaa7058582bada3f4b28",
    "passenger_journeys": "5f4e105a9df728c8781151bdb19a6ae6a784ff7518157d21806c0e790b4f7580",
    "schedule_stop_times": "2b164bbc6bb6ca5313a37e1ef073868c59ed6f587b54dfe5a3632814a1d3b69d",
    "schedules": "3f50489b0f786c06a3a9b600361ca9f489144862785c3f71f1d679c2e524c3a4",
    "service_disruptions": "67f62c188b7b96c59c298a96cfb377bbfe316d04cc75e8c95988460461fd507b",
    "weather_observations": "c728a5c0354ca6092fc66765b3249e25ad9205e5e09d7d089724713403c912d4",
}


def _disabled(cfg: BatchConfig) -> BatchConfig:
    return with_overrides(cfg, fleet_changes={"enabled": False})


def _heavy_transfer_config() -> BatchConfig:
    """Many transfers drawn from the whole default fleet plus many trip-scoped
    (BREAKDOWN/CANCEL) disruptions, so transferred vehicles certainly operate
    trips and are hit by trip-scoped disruptions after their transfer."""
    base = load_batch_config()
    types = [t.model_dump(mode="python") for t in base.disruptions.types]
    return with_overrides(
        base,
        batch={"service_days": 4},
        disruptions={"per_day": 12, "types": types},
        fleet_changes={"transfers_per_day": 15, "relief_pool_size": 50},
    )


def _tables_from_datasets(datasets) -> dict[str, list[dict]]:
    return {name: rows_as_dicts(datasets, name) for name in DATASET_CATALOGUE}


def _default_tables():
    return _tables_from_datasets(default_datasets()), default_reference().to_dict()


def _sample_tables():
    from simulator.reference.generator import generate_reference_dataset

    tables = {
        name: read_jsonl(SAMPLE_OUTPUT_DIR / spec.file_name)
        for name, spec in DATASET_CATALOGUE.items()
    }
    return tables, generate_reference_dataset(load_scale_config(SAMPLE_SCALE_CONFIG)).to_dict()


def _heavy_tables():
    datasets, reference = generate(_heavy_transfer_config())
    return _tables_from_datasets(datasets), reference.to_dict()


SOURCES = {"default": _default_tables, "committed_sample": _sample_tables, "heavy": _heavy_tables}


@pytest.fixture(scope="module", params=sorted(SOURCES))
def tables(request):
    return SOURCES[request.param]()


# --- Criterion 1: catalogue entry ---------------------------------------------


def test_catalogue_entry():
    spec = DATASET_CATALOGUE[DATASET]
    assert spec.model is VehicleDepotAssignment
    assert spec.file_name == "vehicle_depot_assignments.jsonl"
    assert spec.primary_key == ("assignment_id",)
    assert spec.watermark_column == "updated_at"
    assert spec.partition_column == "event_date"
    fks = {(fk.column, fk.references, fk.nullable) for fk in spec.foreign_keys}
    assert fks == {
        ("vehicle_id", "vehicles.vehicle_id", False),
        ("depot_id", "depots.depot_id", False),
        ("previous_depot_id", "depots.depot_id", True),
    }
    assert {t.value for t in AssignmentChangeType} == {"INITIAL", "TRANSFER"}
    assert any("dim_vehicle" in target for target in spec.supports)


# --- Criterion 2: INITIAL / TRANSFER semantics ---------------------------------


def test_exactly_one_initial_row_per_vehicle_with_home_depot(tables):
    batch_tables, reference = tables
    rows = batch_tables[DATASET]
    start = min(r["event_date"] for r in batch_tables["schedules"])
    initial = [r for r in rows if r["change_type"] == "INITIAL"]
    assert Counter(r["vehicle_id"] for r in initial) == Counter(
        v["vehicle_id"] for v in reference["vehicles"]
    )
    homes = {v["vehicle_id"]: v["home_depot_id"] for v in reference["vehicles"]}
    for r in initial:
        assert r["event_date"] == start
        assert r["depot_id"] == homes[r["vehicle_id"]]
        assert r["previous_depot_id"] is None
        assert r["change_sequence"] == 1


def test_change_history_is_well_formed(tables):
    batch_tables, _ = tables
    service_dates = {r["event_date"] for r in batch_tables["schedules"]}
    by_vehicle: dict[str, list[dict]] = defaultdict(list)
    for r in batch_tables[DATASET]:
        by_vehicle[r["vehicle_id"]].append(r)
        assert r["event_date"] in service_dates
        assert r["assignment_id"] == f"VDA-{r['vehicle_id']}-{r['event_date'].replace('-', '')}"
    for vehicle_id, history in by_vehicle.items():
        history.sort(key=lambda r: r["event_date"])
        dates = [r["event_date"] for r in history]
        assert len(dates) == len(set(dates)), f"{vehicle_id}: two changes on one date"
        assert history[0]["change_type"] == "INITIAL"
        assert [r["change_sequence"] for r in history] == list(range(1, len(history) + 1))
        for prev, cur in itertools.pairwise(history):
            assert cur["change_type"] == "TRANSFER"
            assert cur["previous_depot_id"] == prev["depot_id"]
            assert cur["depot_id"] != cur["previous_depot_id"]
            # Per vehicle, the record order (watermark) follows effective order:
            # no backdated changes in the generated log.
            assert cur["updated_at"] > prev["updated_at"]


def test_effective_from_is_local_midnight_and_updated_at_follows_notice():
    cfg = load_batch_config()
    offset = cfg.batch.utc_offset_hours
    notice = timedelta(days=cfg.fleet_changes.notice_days)
    publish_lead = timedelta(days=cfg.schedules.publish_lead_days)
    for r in default_datasets().vehicle_depot_assignments:
        midnight = local_midnight_utc(r.event_date, offset)
        assert r.effective_from == midnight
        assert r.updated_at == midnight - notice
        # Recorded no later than the timetable for its effective date is published.
        assert r.updated_at <= midnight - publish_lead


def test_default_config_has_multi_transfer_history():
    rows = default_datasets().vehicle_depot_assignments
    transfers = [r for r in rows if r.change_type == AssignmentChangeType.TRANSFER]
    assert len(transfers) >= 3
    per_vehicle = Counter(r.vehicle_id for r in transfers)
    assert max(per_vehicle.values()) >= 2
    # Transfers fall on service-date boundaries after start_date.
    cfg = load_batch_config()
    assert {r.event_date for r in transfers} <= set(cfg.batch.service_dates[1:])


def test_transfers_are_deterministic_and_seed_driven():
    cfg = with_overrides(load_batch_config(), batch={"service_days": 3})
    a, _ = generate(cfg)
    b, _ = generate(cfg)
    c, _ = generate(cfg.with_seed(cfg.batch.seed + 1))
    serial = [serialize_row(r) for r in a.vehicle_depot_assignments]
    assert serial == [serialize_row(r) for r in b.vehicle_depot_assignments]
    assert serial != [serialize_row(r) for r in c.vehicle_depot_assignments]


def test_rows_are_ordered_by_effective_date_then_vehicle():
    rows = default_datasets().vehicle_depot_assignments
    keys = [(r.event_date, r.vehicle_id) for r in rows]
    assert keys == sorted(keys)


# --- Criterion 3: volume formula -----------------------------------------------


def _expected_rows(cfg: BatchConfig, vehicle_count: int) -> int:
    fleet = cfg.fleet_changes
    transfers = fleet.transfers_per_day * (cfg.batch.service_days - 1) if fleet.enabled else 0
    return vehicle_count + transfers


def test_default_volume():
    cfg = load_batch_config()
    rows = default_datasets().vehicle_depot_assignments
    assert len(rows) == _expected_rows(cfg, len(default_reference().vehicles))
    assert len(rows) == 56  # 50 vehicles + 1 transfer x 6 later service dates


@pytest.mark.parametrize(
    "overrides",
    [
        {"batch": {"service_days": 1}},
        {"batch": {"service_days": 2}},
        {"batch": {"service_days": 5}, "fleet_changes": {"transfers_per_day": 4, "relief_pool_size": 4}},
        {"batch": {"service_days": 3}, "fleet_changes": {"transfers_per_day": 0}},
        {"batch": {"service_days": 3}, "fleet_changes": {"enabled": False}},
        {"batch": {"service_days": 3}, "fleet_changes": {"transfers_per_day": 10, "relief_pool_size": 50}},
    ],
)
def test_volume_follows_config(overrides):
    cfg = with_overrides(load_batch_config(), **overrides)
    datasets, reference = generate(cfg)
    rows = datasets.vehicle_depot_assignments
    assert len(rows) == _expected_rows(cfg, len(reference.vehicles))
    transfers = [r for r in rows if r.change_type == AssignmentChangeType.TRANSFER]
    if cfg.fleet_changes.enabled:
        # Exactly transfers_per_day per later service date.
        per_date = Counter(r.event_date for r in transfers)
        for d in cfg.batch.service_dates[1:]:
            assert per_date.get(d, 0) == cfg.fleet_changes.transfers_per_day
    else:
        assert transfers == []


def test_volume_scales_with_de01_fleet():
    scale = load_scale_config()
    bigger = scale.model_copy(update={"counts": scale.counts.model_copy(update={"vehicle_count": 60})})
    cfg = with_overrides(load_batch_config(), batch={"service_days": 2})
    datasets, reference = generate(cfg, bigger)
    assert len(reference.vehicles) == 60
    assert len(datasets.vehicle_depot_assignments) == _expected_rows(cfg, 60)


# --- Criterion 4: depot attribution follows transfers ---------------------------


def test_depot_attribution_follows_effective_assignment(tables):
    batch_tables, _ = tables
    depot = effective_depot_lookup(batch_tables[DATASET])
    checked = Counter()
    for name in ("schedules", "historical_trips"):
        for r in batch_tables[name]:
            assert r["depot_id"] == depot(r["vehicle_id"], r["event_date"]), (name, r["trip_id"])
            checked[name] += 1
    for d in batch_tables["service_disruptions"]:
        if d["scope"] == "TRIP":
            assert d["depot_id"] == depot(d["vehicle_id"], d["event_date"]), d["disruption_id"]
            checked["service_disruptions"] += 1
    assert checked["schedules"] == len(batch_tables["schedules"])
    assert checked["historical_trips"] == len(batch_tables["historical_trips"])
    assert checked["service_disruptions"] == sum(
        1 for d in batch_tables["service_disruptions"] if d["scope"] == "TRIP"
    )


def test_depot_attribution_check_is_not_vacuous(tables):
    """Each source really contains vehicle-attributed rows whose depot differs
    from the DE-01 home depot because of a transfer."""
    batch_tables, reference = tables
    homes = {v["vehicle_id"]: v["home_depot_id"] for v in reference["vehicles"]}
    for name in ("schedules", "historical_trips"):
        moved = [r for r in batch_tables[name] if r["depot_id"] != homes[r["vehicle_id"]]]
        assert moved, f"{name}: no row attributed to a transferred depot"
    moved_trip_scoped = [
        d
        for d in batch_tables["service_disruptions"]
        if d["scope"] == "TRIP" and d["depot_id"] != homes[d["vehicle_id"]]
    ]
    assert moved_trip_scoped, "no trip-scoped disruption on a transferred vehicle"


def test_same_vehicle_changes_depot_across_a_transfer_boundary():
    """A vehicle's trips before and after its transfer resolve to different
    depots (the point-in-time behaviour DE-13's fact_trip lookup relies on)."""
    ds = default_datasets()
    transfers = [r for r in ds.vehicle_depot_assignments if r.change_type == AssignmentChangeType.TRANSFER]
    boundaries_seen = 0
    for t in transfers:
        day_before = t.event_date - timedelta(days=1)
        before = {s.depot_id for s in ds.schedules if s.vehicle_id == t.vehicle_id and s.event_date == day_before}
        on = {s.depot_id for s in ds.schedules if s.vehicle_id == t.vehicle_id and s.event_date == t.event_date}
        if before:
            assert before == {t.previous_depot_id}, t.assignment_id
        if on:
            assert on == {t.depot_id}, t.assignment_id
        boundaries_seen += bool(before and on)
    assert boundaries_seen > 0


def test_transfers_change_only_depot_attribution():
    """Enabling transfers changes no RNG draw anywhere else: every other
    dataset is identical, and schedules / trips / disruptions differ only in
    `depot_id`."""
    cfg = with_overrides(load_batch_config(), batch={"service_days": 4})
    enabled, _ = generate(cfg)
    disabled, _ = generate(_disabled(cfg))
    changed_rows = 0
    for name in PRE_DE10_DATASETS:
        a = [r.model_dump(mode="json") for r in enabled.rows(name)]
        b = [r.model_dump(mode="json") for r in disabled.rows(name)]
        if name not in DEPOT_ATTRIBUTED:
            assert a == b, name
            continue
        assert len(a) == len(b), name
        for ra, rb in zip(a, b, strict=True):
            changed_rows += ra["depot_id"] != rb["depot_id"]
            ra.pop("depot_id"), rb.pop("depot_id")
            assert ra == rb, name
    assert changed_rows > 0


# --- Criterion 5: disabled -> byte-identical to pre-DE-10 ----------------------


def _written_sha256(datasets, cfg, scale, tmp_path) -> dict[str, str]:
    manifest = write_batch_datasets(datasets, tmp_path, batch_config=cfg, scale_config=scale)
    digests = {}
    for name in PRE_DE10_DATASETS:
        data = (tmp_path / DATASET_CATALOGUE[name].file_name).read_bytes()
        digests[name] = hashlib.sha256(data).hexdigest()
        assert manifest["datasets"][name]["sha256"] == digests[name]
    return digests


def test_disabled_default_output_is_byte_identical_to_pre_de10(tmp_path):
    cfg = _disabled(load_batch_config())
    scale = load_scale_config()
    datasets, reference = generate(cfg, scale)
    assert set(PRE_DE10_DEFAULT_SHA256) == set(PRE_DE10_DATASETS)
    assert _written_sha256(datasets, cfg, scale, tmp_path) == PRE_DE10_DEFAULT_SHA256
    # The new dataset still carries the baseline (INITIAL rows only).
    assert len(datasets.vehicle_depot_assignments) == len(reference.vehicles)


def test_absent_fleet_changes_section_means_disabled(tmp_path):
    data = load_batch_config().model_dump(mode="python")
    del data["fleet_changes"]
    cfg = BatchConfig.model_validate(data)
    assert cfg.fleet_changes == FLEET_CHANGES_DISABLED
    scale = load_scale_config()
    datasets, _ = generate(cfg, scale)
    assert _written_sha256(datasets, cfg, scale, tmp_path) == PRE_DE10_DEFAULT_SHA256


@pytest.mark.parametrize("enabled", [False, True])
def test_one_day_sample_reproduces_pre_de10_sample(tmp_path, enabled):
    """The pre-DE-10 committed sample was 1 service day. With 1 day no
    transfer can take effect, so it is byte-identical whether or not fleet
    changes are enabled."""
    cfg = with_overrides(
        load_batch_config(SAMPLE_BATCH_CONFIG),
        batch={"service_days": 1},
        fleet_changes={"enabled": enabled},
    )
    scale = load_scale_config(SAMPLE_SCALE_CONFIG)
    datasets, _ = generate(cfg, scale)
    assert _written_sha256(datasets, cfg, scale, tmp_path) == PRE_DE10_SAMPLE_SHA256


# --- Config validation and infeasible configs ----------------------------------


def test_default_and_sample_fleet_config():
    fleet = load_batch_config().fleet_changes
    assert fleet.enabled and fleet.transfers_per_day == 1 and fleet.relief_pool_size == 3
    sample = load_batch_config(SAMPLE_BATCH_CONFIG).fleet_changes
    assert sample.enabled and sample.transfers_per_day >= 1


@pytest.mark.parametrize(
    "fleet",
    [
        {"transfers_per_day": 4, "relief_pool_size": 3},  # > pool
        {"transfers_per_day": -1},
        {"relief_pool_size": -1},
        {"notice_days": -1},
        {"notice_days": 13},  # < publish_lead_days (14) while transfers are generated
        {"unexpected": 1},
    ],
)
def test_inconsistent_fleet_config_rejected(fleet):
    with pytest.raises(ValidationError):
        with_overrides(load_batch_config(), fleet_changes=fleet)


def test_notice_rule_only_applies_when_transfers_are_generated():
    with_overrides(load_batch_config(), fleet_changes={"enabled": False, "notice_days": 0})
    with_overrides(load_batch_config(), fleet_changes={"transfers_per_day": 0, "notice_days": 0})


def test_pool_larger_than_fleet_fails_loudly():
    cfg = with_overrides(
        load_batch_config(), batch={"service_days": 2}, fleet_changes={"relief_pool_size": 51}
    )
    with pytest.raises(InfeasibleFleetChangeConfigError):
        generate(cfg)


def test_single_depot_fleet_cannot_transfer():
    scale = load_scale_config()
    one_depot = scale.model_copy(update={"counts": scale.counts.model_copy(update={"depot_count": 1})})
    cfg = with_overrides(load_batch_config(), batch={"service_days": 2})
    with pytest.raises(InfeasibleFleetChangeConfigError):
        generate(cfg, one_depot)
    # Without transfers a single-depot network is fine.
    datasets, _ = generate(_disabled(cfg), one_depot)
    assert all(r.change_type == AssignmentChangeType.INITIAL for r in datasets.vehicle_depot_assignments)


# --- Row model negatives --------------------------------------------------------


def _transfer_payload() -> dict:
    row = next(
        r for r in default_datasets().vehicle_depot_assignments
        if r.change_type == AssignmentChangeType.TRANSFER
    )
    return row.model_dump(mode="json")


@pytest.mark.parametrize(
    "patch",
    [
        {"previous_depot_id": None},  # TRANSFER needs a previous depot
        {"change_sequence": 1},  # TRANSFER cannot be the first version
        {"change_type": "INITIAL"},  # INITIAL cannot have a previous depot / seq > 1
        {"change_type": "MOVE"},
        {"change_sequence": 0},
        {"effective_from": "2025-01-07T00:00:00"},  # naive datetime
    ],
)
def test_invalid_assignment_rows_rejected(patch):
    payload = _transfer_payload()
    payload.update(patch)
    with pytest.raises(ValidationError):
        VehicleDepotAssignment.model_validate(payload)


def test_transfer_to_same_depot_rejected():
    payload = _transfer_payload()
    payload["depot_id"] = payload["previous_depot_id"]
    with pytest.raises(ValidationError):
        VehicleDepotAssignment.model_validate(payload)
