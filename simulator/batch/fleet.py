"""DE-10: effective-dated vehicle-to-depot assignment history
(`vehicle_depot_assignments`), the change source for an SCD Type 2
`dim_vehicle`.

Model:

- Every DE-01 vehicle gets exactly one `INITIAL` row with its DE-01
  `home_depot_id`, effective from local midnight of `start_date`.
- When `[fleet_changes] enabled`, a relief pool of `relief_pool_size`
  vehicles is drawn once per seed from the fleet (stream
  `fleet-changes|relief-pool`). On each later service date `d`
  (`start_date + 1 .. start_date + service_days - 1`) exactly
  `transfers_per_day` distinct pool vehicles are drawn (stream
  `fleet-changes|<d>`), and each moves to a depot drawn uniformly from the
  depots other than its current one, effective from local midnight of `d`.
  So a vehicle never has two changes on one effective date, and a transfer
  always changes the depot.
- `updated_at` (watermark) = when the assignment was recorded in the source
  fleet system = local midnight of the effective date minus `notice_days`
  (config validation keeps that no later than the timetable publication for
  the date). Per vehicle, `updated_at` therefore increases with the effective
  date: the generated log contains no backdated changes.

Prefix stability: the pool depends only on the seed and fleet, and day `d`'s
transfers only on day `d`'s stream and the state after earlier days, so
growing `service_days` appends rows without changing existing ones.

Trips do not change vehicle (route assignment / vehicle selection are out of
scope): a transfer changes only the depot a vehicle's trips and trip-scoped
disruptions are attributed to from its effective date. `depot_on(...)` is the
lookup the timetable uses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from simulator.batch.config import BatchConfig
from simulator.batch.rng import derive_rng
from simulator.batch.schemas import AssignmentChangeType, VehicleDepotAssignment
from simulator.batch.timetable import compact_date, local_midnight_utc
from simulator.reference.generator import ReferenceDataset


class InfeasibleFleetChangeConfigError(ValueError):
    """Raised when `[fleet_changes]` asks for transfers the DE-01 reference
    data cannot satisfy (pool larger than the fleet, or fewer than 2 depots)."""


@dataclass(frozen=True)
class FleetHistory:
    """Generated assignment rows plus the per-service-date depot lookup."""

    assignments: list[VehicleDepotAssignment]
    #: service date -> vehicle_id -> effective depot_id on that date.
    depots_by_date: dict[date, dict[str, str]]

    def depot_on(self, service_date: date) -> dict[str, str]:
        return self.depots_by_date[service_date]


def assignment_id(vehicle_id: str, effective_date: date) -> str:
    return f"VDA-{vehicle_id}-{compact_date(effective_date)}"


def generate_fleet_history(reference: ReferenceDataset, config: BatchConfig) -> FleetHistory:
    """Build the `vehicle_depot_assignments` rows and per-date depot lookup."""
    fleet_cfg = config.fleet_changes
    seed = config.batch.seed
    offset = config.batch.utc_offset_hours
    notice = timedelta(days=fleet_cfg.notice_days)
    service_dates = config.batch.service_dates
    vehicles = sorted(reference.vehicles, key=lambda v: v.vehicle_id)
    depot_ids = sorted(d.depot_id for d in reference.depots)

    transfers_enabled = fleet_cfg.enabled and fleet_cfg.transfers_per_day > 0
    pool: list[str] = []
    if transfers_enabled:
        if fleet_cfg.relief_pool_size > len(vehicles):
            raise InfeasibleFleetChangeConfigError(
                f"fleet_changes.relief_pool_size={fleet_cfg.relief_pool_size} exceeds "
                f"the DE-01 fleet of {len(vehicles)} vehicles"
            )
        if len(depot_ids) < 2:
            raise InfeasibleFleetChangeConfigError(
                "fleet_changes needs at least 2 depots to transfer a vehicle"
            )
        pool_rng = derive_rng(seed, "fleet-changes", "relief-pool")
        pool = sorted(
            pool_rng.sample([v.vehicle_id for v in vehicles], fleet_cfg.relief_pool_size)
        )

    def row(
        vehicle_id: str,
        effective_date: date,
        depot_id: str,
        previous: str | None,
        sequence: int,
    ) -> VehicleDepotAssignment:
        effective_from = local_midnight_utc(effective_date, offset)
        return VehicleDepotAssignment(
            event_date=effective_date,
            updated_at=effective_from - notice,
            assignment_id=assignment_id(vehicle_id, effective_date),
            vehicle_id=vehicle_id,
            depot_id=depot_id,
            previous_depot_id=previous,
            change_type=(
                AssignmentChangeType.INITIAL if previous is None else AssignmentChangeType.TRANSFER
            ),
            change_sequence=sequence,
            effective_from=effective_from,
        )

    start = config.batch.start_date
    current = {v.vehicle_id: v.home_depot_id for v in vehicles}
    sequence = {v.vehicle_id: 1 for v in vehicles}
    rows = [row(v.vehicle_id, start, v.home_depot_id, None, 1) for v in vehicles]
    depots_by_date: dict[date, dict[str, str]] = {start: dict(current)}

    for service_date in service_dates[1:]:
        if transfers_enabled:
            rng = derive_rng(seed, "fleet-changes", service_date.isoformat())
            movers = sorted(rng.sample(pool, fleet_cfg.transfers_per_day))
            for vehicle_id in movers:
                previous = current[vehicle_id]
                target = rng.choice([d for d in depot_ids if d != previous])
                sequence[vehicle_id] += 1
                rows.append(row(vehicle_id, service_date, target, previous, sequence[vehicle_id]))
                current[vehicle_id] = target
        depots_by_date[service_date] = dict(current)

    return FleetHistory(assignments=rows, depots_by_date=depots_by_date)
