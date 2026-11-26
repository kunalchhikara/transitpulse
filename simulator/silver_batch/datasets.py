"""The batch-Silver dataset registry: every DE-04 `DATASET_CATALOGUE` dataset
(merge mode, keyed by the catalogue primary key, guarded by the catalogue
watermark column) plus the four DE-01 reference entities that DE-11 loads in
`full` mode (snapshot mode).

Nothing here is configured per dataset: models, primary keys, watermark
columns and foreign keys come from DE-04's catalogue and DE-01's entity
models, so a new catalogue dataset (e.g. DE-10's `vehicle_depot_assignments`)
goes through the same code path with no change.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel

from simulator.batch import DATASET_CATALOGUE
from simulator.batch.schemas import BATCH_SCHEMA_VERSION, HistoricalTelemetrySnapshot
from simulator.schemas.entities import (
    ENTITY_SCHEMA_VERSION,
    Depot,
    Route,
    Stop,
    Vehicle,
)
from simulator.schemas.telemetry import SUPPORTED_SCHEMA_VERSIONS


class LoadMode(str, Enum):
    #: Incremental DE-04 datasets: merged by primary key with an
    #: `updated_at` guard (inserted / updated / unchanged / stale / conflict).
    MERGE = "merge"
    #: Full DE-01 reference snapshots: current state := the latest valid
    #: snapshot (inserted / updated / unchanged / deleted).
    SNAPSHOT = "snapshot"


@dataclass(frozen=True)
class ForeignKeySpec:
    """`column` references `<parent>.<parent_column>`. `is_list`: the column
    holds a list of references (e.g. `routes.stop_ids`)."""

    column: str
    parent: str
    parent_column: str
    nullable: bool = False
    is_list: bool = False

    @property
    def name(self) -> str:
        return f"{self.column}->{self.parent}.{self.parent_column}"


@dataclass(frozen=True)
class SilverDataset:
    name: str
    model: type[BaseModel]
    mode: LoadMode
    primary_key: tuple[str, ...]
    #: merge mode: the catalogue watermark column (`updated_at`); snapshot: None.
    watermark_column: str | None
    foreign_keys: tuple[ForeignKeySpec, ...]
    supported_schema_versions: tuple[str, ...]

    @property
    def fields(self) -> tuple[str, ...]:
        return tuple(self.model.model_fields)

    @property
    def current_table(self) -> str:
        return f"current_{self.name}"

    @property
    def quarantine_table(self) -> str:
        return f"quarantine_{self.name}"

    @property
    def stale_table(self) -> str:
        return f"stale_{self.name}"


def _supported_versions(model: type[BaseModel]) -> tuple[str, ...]:
    if issubclass(model, HistoricalTelemetrySnapshot):
        # Extends DE-01's streaming TelemetryEvent contract (DE-04 docstring).
        return tuple(SUPPORTED_SCHEMA_VERSIONS)
    return (BATCH_SCHEMA_VERSION,)


def _catalogue_dataset(name: str) -> SilverDataset:
    spec = DATASET_CATALOGUE[name]
    foreign_keys = []
    for fk in spec.foreign_keys:
        parent, parent_column = fk.references.split(".", 1)
        foreign_keys.append(ForeignKeySpec(fk.column, parent, parent_column, nullable=fk.nullable))
    return SilverDataset(
        name=name,
        model=spec.model,
        mode=LoadMode.MERGE,
        primary_key=tuple(spec.primary_key),
        watermark_column=spec.watermark_column,
        foreign_keys=tuple(foreign_keys),
        supported_schema_versions=_supported_versions(spec.model),
    )


#: DE-01 reference entities, as DE-11 loads them in `full` mode
#: (`default_incremental.toml`: name, primary_key). Their foreign keys follow
#: the DE-01 entity docstrings (DE-04's catalogue only covers DE-04 datasets).
_REFERENCE: tuple[SilverDataset, ...] = (
    SilverDataset("depots", Depot, LoadMode.SNAPSHOT, ("depot_id",), None, (), (ENTITY_SCHEMA_VERSION,)),
    SilverDataset("stops", Stop, LoadMode.SNAPSHOT, ("stop_id",), None, (), (ENTITY_SCHEMA_VERSION,)),
    SilverDataset(
        "routes",
        Route,
        LoadMode.SNAPSHOT,
        ("route_id",),
        None,
        (
            ForeignKeySpec("home_depot_id", "depots", "depot_id"),
            ForeignKeySpec("stop_ids", "stops", "stop_id", is_list=True),
        ),
        (ENTITY_SCHEMA_VERSION,),
    ),
    SilverDataset(
        "vehicles",
        Vehicle,
        LoadMode.SNAPSHOT,
        ("vehicle_id",),
        None,
        (
            ForeignKeySpec("home_depot_id", "depots", "depot_id"),
            ForeignKeySpec("assigned_route_id", "routes", "route_id", nullable=True),
        ),
        (ENTITY_SCHEMA_VERSION,),
    ),
)

REFERENCE_DATASETS: tuple[str, ...] = tuple(ds.name for ds in _REFERENCE)
CATALOGUE_DATASETS: tuple[str, ...] = tuple(DATASET_CATALOGUE)

#: Registry order = the order datasets are reported in manifests (reference
#: entities first, then the DE-04 catalogue order). Processing order is always
#: Bronze commit order (see pipeline).
SILVER_DATASETS: dict[str, SilverDataset] = {
    ds.name: ds for ds in (*_REFERENCE, *(_catalogue_dataset(name) for name in DATASET_CATALOGUE))
}
