import vehiclesFixtureData from "@/fixtures/vehicles.json";
import {
  assertVehiclesFixture,
  isVehiclesFixture,
  type VehiclesFixture,
} from "./vehiclesFixture";

/**
 * Fixture-shape test (WE-03 test requirement, mirroring
 * `homeFixture.test.ts`): proves the actual `vehicles.json` fixture matches
 * the `VehiclesFixture` contract at runtime, and that the guard genuinely
 * rejects malformed shapes rather than always returning true.
 */
describe("VehiclesFixture contract", () => {
  it("accepts the real vehicles.json fixture", () => {
    expect(isVehiclesFixture(vehiclesFixtureData)).toBe(true);
    expect(() => assertVehiclesFixture(vehiclesFixtureData)).not.toThrow();
  });

  it("exposes at least one vehicle, as required by WE-03 acceptance criteria", () => {
    const fixture = assertVehiclesFixture(vehiclesFixtureData);
    expect(fixture.vehicles.length).toBeGreaterThanOrEqual(1);
  });

  const validFixture: VehiclesFixture = {
    asOf: "2026-11-24T08:00:00Z",
    vehicles: [
      {
        vehicleId: "VEH-001",
        fleetNumber: "1001",
        routeId: "RT-01",
        routeNumber: "M50",
        status: "IN_SERVICE",
        occupancy: 10,
        capacity: 60,
        delaySeconds: 0,
        speedKmh: 40,
      },
    ],
    fixtureNote: "note",
  };

  it("accepts a minimal well-formed fixture", () => {
    expect(isVehiclesFixture(validFixture)).toBe(true);
  });

  it.each<[string, unknown]>([
    ["null", null],
    ["a non-object", "not an object"],
    ["missing asOf", { ...validFixture, asOf: undefined }],
    ["missing vehicles array", { ...validFixture, vehicles: undefined }],
    ["empty vehicles array", { ...validFixture, vehicles: [] }],
    ["missing fixtureNote", { ...validFixture, fixtureNote: undefined }],
    [
      "a vehicle with an invalid status",
      {
        ...validFixture,
        vehicles: [{ ...validFixture.vehicles[0], status: "FLYING" }],
      },
    ],
    [
      "a vehicle with a negative occupancy",
      {
        ...validFixture,
        vehicles: [{ ...validFixture.vehicles[0], occupancy: -1 }],
      },
    ],
    [
      "a vehicle with a non-numeric delaySeconds",
      {
        ...validFixture,
        vehicles: [{ ...validFixture.vehicles[0], delaySeconds: "420" }],
      },
    ],
    [
      "a vehicle with a negative speedKmh",
      {
        ...validFixture,
        vehicles: [{ ...validFixture.vehicles[0], speedKmh: -5 }],
      },
    ],
    [
      "a vehicle missing a routeId",
      {
        ...validFixture,
        vehicles: [
          {
            vehicleId: "VEH-001",
            fleetNumber: "1001",
            routeNumber: "M50",
            status: "IN_SERVICE",
            occupancy: 10,
            capacity: 60,
            delaySeconds: 0,
            speedKmh: 40,
          },
        ],
      },
    ],
  ])("rejects a fixture with %s", (_name, malformed) => {
    expect(isVehiclesFixture(malformed)).toBe(false);
    expect(() => assertVehiclesFixture(malformed)).toThrow(
      /does not match the VehiclesFixture contract/,
    );
  });
});
