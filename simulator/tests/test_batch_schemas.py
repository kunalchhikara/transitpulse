"""Per-dataset DE-04 schema tests: every written row validates against its
catalogued model, carries schema_version + watermark (`updated_at`) +
partition (`event_date`) columns, rejects missing/unknown fields, and each
dataset's schema is documented in `simulator/README.md`."""

import json
import re
from datetime import UTC, timedelta

import pytest
from pydantic import ValidationError

from simulator.batch.generator import serialize_row
from simulator.batch.schemas import (
    BATCH_SCHEMA_VERSION,
    DATASET_CATALOGUE,
    HistoricalTelemetrySnapshot,
    ServiceDisruption,
)
from simulator.config import load_validation_thresholds
from simulator.schemas.telemetry import (
    CURRENT_SCHEMA_VERSION,
    TelemetryEvent,
    validate_against_thresholds,
)
from simulator.tests._batch_helpers import (
    REPO_ROOT,
    SAMPLE_OUTPUT_DIR,
    default_datasets,
    default_reference,
)

DATASET_NAMES = list(DATASET_CATALOGUE)
EXPECTED_SCHEMA_VERSION = {
    name: (CURRENT_SCHEMA_VERSION if name == "historical_telemetry" else BATCH_SCHEMA_VERSION)
    for name in DATASET_NAMES
}
_UTC_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def test_catalogue_covers_project_section_7_batch_sources():
    assert set(DATASET_NAMES) == {
        "schedules",
        "schedule_stop_times",
        "historical_trips",
        "historical_telemetry",
        "passenger_journeys",
        "service_disruptions",
        "weather_observations",
        # DE-10: vehicle-to-depot change history (PROJECT.md section 10 SCD2 source).
        "vehicle_depot_assignments",
    }


@pytest.mark.parametrize("name", DATASET_NAMES)
def test_catalogue_columns_exist_on_model(name):
    spec = DATASET_CATALOGUE[name]
    fields = spec.model.model_fields
    for column in (*spec.primary_key, spec.watermark_column, spec.partition_column, "schema_version"):
        assert column in fields, f"{name}: {column} missing from model"
    for fk in spec.foreign_keys:
        assert fk.column in fields, f"{name}: FK column {fk.column} missing"
    assert spec.watermark_column == "updated_at"
    assert spec.partition_column == "event_date"
    assert spec.supports, f"{name}: no downstream Gold/Warehouse target documented"


@pytest.mark.parametrize("name", DATASET_NAMES)
def test_every_generated_row_round_trips_through_its_schema(name):
    spec = DATASET_CATALOGUE[name]
    rows = default_datasets().rows(name)
    assert rows, f"{name} is empty at default config"
    for row in rows:
        line = serialize_row(row)
        parsed = spec.model.model_validate_json(line)
        assert serialize_row(parsed) == line
        payload = json.loads(line)
        assert set(payload) == set(spec.model.model_fields)
        assert payload["schema_version"] == EXPECTED_SCHEMA_VERSION[name]
        assert _UTC_Z.match(payload["updated_at"]), payload["updated_at"]
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", payload["event_date"])


@pytest.mark.parametrize("name", DATASET_NAMES)
def test_committed_sample_rows_validate(name):
    spec = DATASET_CATALOGUE[name]
    lines = (SAMPLE_OUTPUT_DIR / spec.file_name).read_text(encoding="utf-8").splitlines()
    assert lines, f"sample {name} is empty"
    for line in lines:
        spec.model.model_validate_json(line)


@pytest.mark.parametrize("name", DATASET_NAMES)
def test_missing_required_field_fails_validation(name):
    spec = DATASET_CATALOGUE[name]
    payload = default_datasets().rows(name)[0].model_dump(mode="json")
    for column in (*spec.primary_key, spec.watermark_column, spec.partition_column):
        broken = dict(payload)
        del broken[column]
        with pytest.raises(ValidationError):
            spec.model.model_validate(broken)


@pytest.mark.parametrize("name", DATASET_NAMES)
def test_unknown_field_is_rejected(name):
    spec = DATASET_CATALOGUE[name]
    payload = default_datasets().rows(name)[0].model_dump(mode="json")
    payload["unexpected_column"] = "x"
    with pytest.raises(ValidationError):
        spec.model.model_validate(payload)


def test_naive_watermark_is_rejected():
    payload = default_datasets().schedules[0].model_dump(mode="json")
    payload["updated_at"] = "2025-01-01T00:00:00"  # no timezone
    with pytest.raises(ValidationError):
        DATASET_CATALOGUE["schedules"].model.model_validate(payload)


def test_historical_telemetry_conforms_to_streaming_telemetry_contract():
    """Historical snapshots are a superset of DE-01's TelemetryEvent and pass
    the configurable section-12 validation thresholds, so DE-06 Silver rules
    apply unchanged to history."""
    assert issubclass(HistoricalTelemetrySnapshot, TelemetryEvent)
    thresholds = load_validation_thresholds()
    known_routes = {r.route_id for r in default_reference().routes}
    snapshots = default_datasets().historical_telemetry
    now = max(e.event_time for e in snapshots).astimezone(UTC) + timedelta(seconds=1)
    for e in snapshots:
        projected = TelemetryEvent.model_validate(
            e.model_dump(include=set(TelemetryEvent.model_fields))
        )
        assert validate_against_thresholds(
            projected, thresholds, known_route_ids=known_routes, now=now
        ) == [], e.event_id


def test_service_disruptions_can_populate_fact_service_incident():
    """`fact_service_incident` (PROJECT.md section 10) needs a natural key,
    date + time keys, route/depot dimension keys (vehicle/stop/trip when
    trip-scoped), classification attributes and duration/impact measures."""
    always = (
        "disruption_id", "event_date", "start_time", "end_time", "route_id", "depot_id",
        "disruption_type", "effect", "scope", "severity", "status", "description",
        "duration_minutes", "affected_trip_count", "updated_at",
    )
    disruptions = default_datasets().service_disruptions
    assert disruptions
    for d in disruptions:
        for column in always:
            assert getattr(d, column) not in (None, ""), (d.disruption_id, column)
        assert d.duration_minutes == round((d.end_time - d.start_time).total_seconds() / 60)
        if d.scope.value == "TRIP":
            assert d.vehicle_id and d.trip_id and d.stop_id
        if d.effect.value == "DELAY":
            assert d.estimated_delay_seconds is not None and d.estimated_delay_seconds >= 0
    assert set(ServiceDisruption.model_fields) >= set(always) | {"vehicle_id", "trip_id", "stop_id"}


def test_every_dataset_schema_is_documented_in_readme():
    readme = (REPO_ROOT / "simulator" / "README.md").read_text(encoding="utf-8")
    marker = "## DE-04 batch datasets"
    assert marker in readme
    section = readme.split(marker, 1)[1]
    for name, spec in DATASET_CATALOGUE.items():
        heading = f"### `{name}`"
        assert heading in section, f"README missing {heading}"
        body = section.split(heading, 1)[1].split("\n### ", 1)[0]
        for column in spec.model.model_fields:
            assert f"`{column}`" in body, f"README {name} catalogue missing column {column}"
