import { render, screen, within } from "@testing-library/react";
import LiveOperationsPage from "./page";
import vehiclesFixture from "@/fixtures/vehicles.json";

/**
 * Live Operations page render test (WE-03 test requirement, mirroring
 * `app/page.test.tsx`): asserts the page renders every fixture vehicle with
 * its route, status, occupancy and delay fields, shows an unambiguous DEMO
 * label, and never claims a live/connected backend.
 */
describe("LiveOperationsPage", () => {
  it("renders a single top-level heading", () => {
    render(<LiveOperationsPage />);
    const heading = screen.getByRole("heading", { level: 1 });
    expect(heading).toHaveTextContent("Live Operations");
  });

  it("renders a visible, unambiguous DEMO mode indicator", () => {
    render(<LiveOperationsPage />);
    const indicator = screen.getByRole("status");
    expect(indicator).toHaveAttribute("data-mode", "DEMO");
    expect(indicator).toHaveTextContent("DEMO");
  });

  it("never claims a LIVE or CONNECTED TEST backend", () => {
    render(<LiveOperationsPage />);
    expect(screen.queryByText("LIVE")).not.toBeInTheDocument();
    expect(screen.queryByText("CONNECTED TEST")).not.toBeInTheDocument();
  });

  it("renders the fixture's disclosure note", () => {
    render(<LiveOperationsPage />);
    expect(screen.getByText(vehiclesFixture.fixtureNote)).toBeInTheDocument();
  });

  it("renders exactly as many vehicle rows as the fixture has vehicles", () => {
    render(<LiveOperationsPage />);
    const table = screen.getByRole("table");
    const rows = within(table).getAllByRole("row");
    // header row + one row per fixture vehicle
    expect(rows).toHaveLength(vehiclesFixture.vehicles.length + 1);
  });

  /** Mirrors `formatDelay` in `page.tsx` so the test asserts real rendered text. */
  function expectedDelayText(delaySeconds: number): string {
    if (delaySeconds === 0) {
      return "On time";
    }
    const minutes = Math.round(Math.abs(delaySeconds) / 60);
    const unit = minutes === 1 ? "min" : "mins";
    return delaySeconds > 0
      ? `+${minutes} ${unit} late`
      : `${minutes} ${unit} early`;
  }

  it("renders route, status, occupancy and delay for every fixture vehicle", () => {
    render(<LiveOperationsPage />);
    const table = screen.getByRole("table");

    for (const vehicle of vehiclesFixture.vehicles) {
      const row = within(table).getByText(vehicle.vehicleId).closest("tr");
      expect(row).not.toBeNull();
      const withinRow = within(row as HTMLElement);

      expect(withinRow.getByText(vehicle.routeNumber)).toBeInTheDocument();
      expect(withinRow.getByText(vehicle.routeId)).toBeInTheDocument();
      expect(
        withinRow.getByText(`${vehicle.occupancy} / ${vehicle.capacity}`),
      ).toBeInTheDocument();
      expect(
        withinRow.getByText(expectedDelayText(vehicle.delaySeconds)),
      ).toBeInTheDocument();

      // Every status value in the fixture must render as its human label.
      const statusLabels: Record<string, string> = {
        IN_SERVICE: "In service",
        DELAYED: "Delayed",
        BREAKDOWN: "Breakdown",
        OUT_OF_SERVICE: "Out of service",
      };
      expect(
        withinRow.getByText(statusLabels[vehicle.status]),
      ).toBeInTheDocument();
    }
  });

  it("makes no fetch/network calls while rendering", () => {
    const originalFetch = global.fetch;
    const fetchSpy = jest.fn();
    global.fetch = fetchSpy as unknown as typeof global.fetch;

    render(<LiveOperationsPage />);

    expect(fetchSpy).not.toHaveBeenCalled();
    global.fetch = originalFetch;
  });
});
