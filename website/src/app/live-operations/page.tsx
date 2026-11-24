import type { Metadata } from "next";
import { ModeIndicator } from "@/components/ModeIndicator";
import {
  assertVehiclesFixture,
  type VehicleStatus,
} from "@/lib/vehiclesFixture";
import vehiclesFixtureData from "@/fixtures/vehicles.json";

export const metadata: Metadata = { title: "Live Operations" };

/**
 * Live Operations page (WE-03: "Live Operations Page — Demo Mode Fleet
 * View"). All vehicle data below comes from the hand-authored static
 * fixture at `src/fixtures/vehicles.json`, validated against the
 * `VehiclesFixture` contract in `src/lib/vehiclesFixture.ts` — the same
 * fixture + type-contract + runtime-guard convention WE-02 established in
 * `homeFixture.ts`. There is no fetch, no simulator invocation and no
 * Fabric/Eventstream connection behind this page; `ModeIndicator` stays
 * `DEMO` accordingly, and the fixture's own `fixtureNote` says so in-page.
 *
 * A plain, accessible table is used deliberately instead of a map or chart
 * — the backlog (WE-03) explicitly defers any mapping/charting library
 * choice to a later, separately-approved story. See `website/README.md`
 * "Live Operations fixture (WE-03)" for the list-over-map rationale.
 */
const vehiclesFixture = assertVehiclesFixture(vehiclesFixtureData);

const STATUS_LABEL: Record<VehicleStatus, string> = {
  IN_SERVICE: "In service",
  DELAYED: "Delayed",
  BREAKDOWN: "Breakdown",
  OUT_OF_SERVICE: "Out of service",
};

const STATUS_BADGE_CLASSNAME: Record<VehicleStatus, string> = {
  IN_SERVICE:
    "bg-emerald-50 text-emerald-800 ring-emerald-600/30 dark:bg-emerald-950 dark:text-emerald-300 dark:ring-emerald-400/40",
  DELAYED:
    "bg-amber-50 text-amber-800 ring-amber-600/30 dark:bg-amber-950 dark:text-amber-300 dark:ring-amber-400/40",
  BREAKDOWN:
    "bg-red-50 text-red-800 ring-red-600/30 dark:bg-red-950 dark:text-red-300 dark:ring-red-400/40",
  OUT_OF_SERVICE:
    "bg-slate-100 text-slate-700 ring-slate-500/30 dark:bg-slate-800 dark:text-slate-300 dark:ring-slate-400/30",
};

function formatDelay(delaySeconds: number): string {
  if (delaySeconds === 0) {
    return "On time";
  }
  const minutes = Math.round(Math.abs(delaySeconds) / 60);
  const unit = minutes === 1 ? "min" : "mins";
  return delaySeconds > 0 ? `+${minutes} ${unit} late` : `${minutes} ${unit} early`;
}

function formatAsOf(asOf: string): string {
  const parsed = new Date(asOf);
  if (Number.isNaN(parsed.getTime())) {
    return asOf;
  }
  return parsed.toLocaleString("en-AU", {
    dateStyle: "medium",
    timeStyle: "short",
  });
}

export default function LiveOperationsPage() {
  return (
    <section className="mx-auto w-full max-w-5xl px-4 py-12 sm:px-6 lg:px-8">
      <ModeIndicator mode="DEMO" />

      <h1 className="mt-4 text-3xl font-bold tracking-tight text-foreground">
        Live Operations
      </h1>
      <p className="mt-4 max-w-2xl text-base leading-relaxed text-foreground/80">
        Fleet status, route delays and occupancy across the TransitPulse
        network.
      </p>
      <p className="mt-2 max-w-2xl text-sm text-foreground/60">
        {vehiclesFixture.fixtureNote}
      </p>
      <p className="mt-1 text-xs text-foreground/50">
        Snapshot as of {formatAsOf(vehiclesFixture.asOf)}.
      </p>

      <h2 className="sr-only">Fleet vehicle list</h2>
      <div className="mt-8 overflow-x-auto rounded-lg border border-foreground/10">
        <table className="w-full min-w-[640px] border-collapse text-left text-sm">
          <caption className="sr-only">
            Current status, route, occupancy and delay for each vehicle in
            the demo fleet snapshot.
          </caption>
          <thead>
            <tr className="border-b border-foreground/10 bg-foreground/5 text-xs font-semibold uppercase tracking-wide text-foreground/60">
              <th scope="col" className="px-4 py-3">
                Vehicle
              </th>
              <th scope="col" className="px-4 py-3">
                Route
              </th>
              <th scope="col" className="px-4 py-3">
                Status
              </th>
              <th scope="col" className="px-4 py-3">
                Occupancy
              </th>
              <th scope="col" className="px-4 py-3">
                Delay
              </th>
              <th scope="col" className="px-4 py-3">
                Speed
              </th>
            </tr>
          </thead>
          <tbody>
            {vehiclesFixture.vehicles.map((vehicle) => (
              <tr
                key={vehicle.vehicleId}
                className="border-b border-foreground/5 last:border-b-0"
              >
                <td className="px-4 py-3">
                  <span className="font-semibold text-foreground">
                    Fleet {vehicle.fleetNumber}
                  </span>
                  <span className="block text-xs text-foreground/50">
                    {vehicle.vehicleId}
                  </span>
                </td>
                <td className="px-4 py-3">
                  <span className="font-medium text-foreground">
                    {vehicle.routeNumber}
                  </span>
                  <span className="block text-xs text-foreground/50">
                    {vehicle.routeId}
                  </span>
                </td>
                <td className="px-4 py-3">
                  <span
                    className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold ring-1 ring-inset ${STATUS_BADGE_CLASSNAME[vehicle.status]}`}
                  >
                    {STATUS_LABEL[vehicle.status]}
                  </span>
                </td>
                <td className="px-4 py-3 text-foreground/80">
                  {vehicle.occupancy} / {vehicle.capacity}
                </td>
                <td className="px-4 py-3 text-foreground/80">
                  {formatDelay(vehicle.delaySeconds)}
                </td>
                <td className="px-4 py-3 text-foreground/80">
                  {vehicle.speedKmh} km/h
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
