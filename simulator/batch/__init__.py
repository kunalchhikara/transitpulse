"""DE-04: deterministic, config-driven synthetic batch/historical datasets
(PROJECT.md section 7): schedules, schedule_stop_times, historical_trips,
historical_telemetry, passenger_journeys, service_disruptions, optional
weather_observations and (DE-10) vehicle_depot_assignments, all
referentially consistent with DE-01's reference entities. See the "DE-04 batch datasets" section of `simulator/README.md`.
"""

from simulator.batch.config import BatchConfig, load_batch_config
from simulator.batch.generator import (
    BatchDatasets,
    generate_batch_datasets,
    serialize_row,
)
from simulator.batch.schemas import BATCH_SCHEMA_VERSION, DATASET_CATALOGUE
from simulator.batch.writer import write_batch_datasets

__all__ = [
    "BATCH_SCHEMA_VERSION",
    "DATASET_CATALOGUE",
    "BatchConfig",
    "BatchDatasets",
    "generate_batch_datasets",
    "load_batch_config",
    "serialize_row",
    "write_batch_datasets",
]
