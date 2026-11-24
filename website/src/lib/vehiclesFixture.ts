/**
 * Type contract + runtime guard for `website/src/fixtures/vehicles.json`.
 *
 * WE-03 ("Live Operations Page — Demo Mode Fleet View") follows the same
 * fixture convention WE-02 established in `homeFixture.ts`: a small,
 * hand-authored static JSON fixture, a TypeScript contract for it, and a
 * runtime guard that fails the build loudly if the fixture is shaped wrong.
 * See that module's docs for the full rationale; this file repeats the
 * pattern rather than inventing a new one.
 *
 * `VehicleSnapshot`'s fields are loosely based on the `Vehicle` and
 * `TelemetryEvent` Pydantic models in `simulator/schemas/entities.py` and
 * `simulator/schemas/telemetry.py` (see `website/README.md` "Live
 * Operations fixture (WE-03)" for the exact field-by-field mapping) —
 * this is a documented *contract*, not a code or network dependency on the
 * simulator. There is no import of simulator code and no fetch anywhere in
 * this module or the page that uses it.
 */

/**
 * Mirrors `simulator.schemas.telemetry.VehicleStatus`: IN_SERVICE, DELAYED,
 * BREAKDOWN, OUT_OF_SERVICE. Kept as a literal union (not importing Python)
 * so a typo in the fixture is caught by `isVehicleStatus` below.
 */
export type VehicleStatus =
  | "IN_SERVICE"
  | "DELAYED"
  | "BREAKDOWN"
  | "OUT_OF_SERVICE";

const VEHICLE_STATUSES: readonly VehicleStatus[] = [
  "IN_SERVICE",
  "DELAYED",
  "BREAKDOWN",
  "OUT_OF_SERVICE",
];

export interface VehicleSnapshot {
  /** Stable identifier; mirrors `Vehicle.vehicle_id` / `TelemetryEvent.vehicle_id`. */
  vehicleId: string;
  /** Operator-facing fleet number; mirrors `Vehicle.fleet_number`. */
  fleetNumber: string;
  /** Mirrors `Route.route_id` / `TelemetryEvent.route_id`. */
  routeId: string;
  /** Operator-facing route number, e.g. "M70"; mirrors `Route.route_number`. */
  routeNumber: string;
  /** Mirrors `TelemetryEvent.status` (`VehicleStatus` enum). */
  status: VehicleStatus;
  /** Current passenger count; mirrors `TelemetryEvent.occupancy` (>= 0). */
  occupancy: number;
  /** Passenger capacity; mirrors `Vehicle.capacity` (>= 0). */
  capacity: number;
  /** Mirrors `TelemetryEvent.delay_seconds` (negative means ahead of schedule). */
  delaySeconds: number;
  /** Mirrors `TelemetryEvent.speed_kmh` (>= 0). */
  speedKmh: number;
}

export interface VehiclesFixture {
  /** Illustrative snapshot timestamp (ISO 8601) — not a live clock reading. */
  asOf: string;
  /** At least one vehicle (WE-03 acceptance criteria). */
  vehicles: VehicleSnapshot[];
  /** Explicit disclosure that this fleet snapshot is static demo data. */
  fixtureNote: string;
}

function isVehicleStatus(value: unknown): value is VehicleStatus {
  return (
    typeof value === "string" &&
    (VEHICLE_STATUSES as readonly string[]).includes(value)
  );
}

function isVehicleSnapshot(value: unknown): value is VehicleSnapshot {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.vehicleId === "string" &&
    candidate.vehicleId.length > 0 &&
    typeof candidate.fleetNumber === "string" &&
    candidate.fleetNumber.length > 0 &&
    typeof candidate.routeId === "string" &&
    candidate.routeId.length > 0 &&
    typeof candidate.routeNumber === "string" &&
    candidate.routeNumber.length > 0 &&
    isVehicleStatus(candidate.status) &&
    typeof candidate.occupancy === "number" &&
    Number.isFinite(candidate.occupancy) &&
    candidate.occupancy >= 0 &&
    typeof candidate.capacity === "number" &&
    Number.isFinite(candidate.capacity) &&
    candidate.capacity >= 0 &&
    typeof candidate.delaySeconds === "number" &&
    Number.isFinite(candidate.delaySeconds) &&
    typeof candidate.speedKmh === "number" &&
    Number.isFinite(candidate.speedKmh) &&
    candidate.speedKmh >= 0
  );
}

/** Runtime structural check — see module docs. */
export function isVehiclesFixture(value: unknown): value is VehiclesFixture {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const candidate = value as Record<string, unknown>;

  return (
    typeof candidate.asOf === "string" &&
    candidate.asOf.length > 0 &&
    Array.isArray(candidate.vehicles) &&
    candidate.vehicles.length > 0 &&
    candidate.vehicles.every(isVehicleSnapshot) &&
    typeof candidate.fixtureNote === "string" &&
    candidate.fixtureNote.length > 0
  );
}

/**
 * Validates `value` against the `VehiclesFixture` contract and returns it
 * typed, or throws with a message pointing at the fixture file and this
 * contract. Used by `src/app/live-operations/page.tsx` so a malformed
 * `vehicles.json` fails the build fast and legibly.
 */
export function assertVehiclesFixture(value: unknown): VehiclesFixture {
  if (!isVehiclesFixture(value)) {
    throw new Error(
      "website/src/fixtures/vehicles.json does not match the " +
        "VehiclesFixture contract defined in website/src/lib/vehiclesFixture.ts.",
    );
  }
  return value;
}
