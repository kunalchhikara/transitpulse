import type { ReactElement } from "react";
import { render, screen } from "@testing-library/react";
import { NAV_ITEMS } from "@/lib/navigation";

import HomePage from "@/app/page";
import LiveOperationsPage from "@/app/live-operations/page";
import ArchitecturePage from "@/app/architecture/page";
import DataPipelinePage from "@/app/data-pipeline/page";
import StreamingLabPage from "@/app/streaming-lab/page";
import IncidentControlCentrePage from "@/app/incident-control-centre/page";
import DataQualityPage from "@/app/data-quality/page";
import ObservabilityPage from "@/app/observability/page";
import AnalyticsPage from "@/app/analytics/page";
import EngineeringPage from "@/app/engineering/page";

/**
 * Route-smoke test (WE-01 acceptance criteria: "all 10 nav routes
 * resolve"). `npm run build` already proves Next.js can statically
 * generate every route; this test proves each page module renders without
 * throwing, shows exactly one ModeIndicator, and that indicator is never
 * LIVE or CONNECTED TEST — because WE-01 ships with no backend at all, so
 * nothing on this site is allowed to claim otherwise.
 */
const ROUTE_MODULES: Record<string, () => ReactElement> = {
  "/": HomePage,
  "/live-operations": LiveOperationsPage,
  "/architecture": ArchitecturePage,
  "/data-pipeline": DataPipelinePage,
  "/streaming-lab": StreamingLabPage,
  "/incident-control-centre": IncidentControlCentrePage,
  "/data-quality": DataQualityPage,
  "/observability": ObservabilityPage,
  "/analytics": AnalyticsPage,
  "/engineering": EngineeringPage,
};

describe("site routes", () => {
  it("has a page module registered for every nav item, and vice versa", () => {
    const navHrefs = NAV_ITEMS.map((item) => item.href).sort();
    const routeHrefs = Object.keys(ROUTE_MODULES).sort();
    expect(routeHrefs).toEqual(navHrefs);
  });

  it.each(NAV_ITEMS.map((item) => [item.href, item.label] as const))(
    "renders %s (%s) with a DEMO-only ModeIndicator and no throw",
    (href) => {
      const Page = ROUTE_MODULES[href];
      render(<Page />);

      const indicators = screen.getAllByRole("status");
      expect(indicators).toHaveLength(1);
      expect(indicators[0]).toHaveAttribute("data-mode", "DEMO");

      // Never claim a connection this story doesn't have.
      expect(screen.queryByText("LIVE")).not.toBeInTheDocument();
      expect(screen.queryByText("CONNECTED TEST")).not.toBeInTheDocument();
    },
  );

  it("renders exactly one top-level heading (h1) per page", () => {
    for (const Page of Object.values(ROUTE_MODULES)) {
      const { unmount } = render(<Page />);
      const headings = screen.getAllByRole("heading", { level: 1 });
      expect(headings).toHaveLength(1);
      unmount();
    }
  });
});
