import { render, screen } from "@testing-library/react";
import { axe, toHaveNoViolations } from "jest-axe";
import { SiteHeader } from "@/components/SiteHeader";
import { SiteFooter } from "@/components/SiteFooter";
import HomePage from "@/app/page";
import LiveOperationsPage from "@/app/live-operations/page";

expect.extend(toHaveNoViolations);

jest.mock("next/navigation", () => ({
  usePathname: () => "/",
}));

/**
 * Accessibility smoke check (WE-01 test requirement). We use jest-axe
 * rather than a hand-rolled heuristic: it is the standard, well-maintained
 * axe-core binding for RTL/Jest, it is a single additional dependency (not
 * a heavy framework), and it catches a materially broader class of real
 * issues (landmark/role/contrast/label problems) than a manual
 * heading-order check alone would. We still assert heading order
 * explicitly below, since axe does not check document outline by itself.
 */
function renderShell(children: React.ReactNode) {
  return render(
    <>
      <SiteHeader />
      <main id="main-content">{children}</main>
      <SiteFooter />
    </>,
  );
}

describe("accessibility smoke check", () => {
  it("has no axe violations on the full page shell (Home)", async () => {
    const { container } = renderShell(<HomePage />);
    const results = await axe(container);
    expect(results).toHaveNoViolations();
  });

  it("has no axe violations on the Live Operations page", async () => {
    const { container } = renderShell(<LiveOperationsPage />);
    const results = await axe(container);
    expect(results).toHaveNoViolations();
  });

  it("exposes the four standard landmarks: banner, navigation, main, contentinfo", () => {
    renderShell(<HomePage />);
    expect(screen.getByRole("banner")).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Primary" })).toBeInTheDocument();
    expect(screen.getByRole("main")).toBeInTheDocument();
    expect(screen.getByRole("contentinfo")).toBeInTheDocument();
  });

  it("keeps a sane heading order (single h1, no h3 before an h2)", () => {
    renderShell(<HomePage />);
    const headings = screen.getAllByRole("heading");
    const levels = headings.map((h) => Number(h.tagName.slice(1)));

    expect(levels.filter((level) => level === 1)).toHaveLength(1);
    expect(levels[0]).toBe(1);

    let previous = levels[0];
    for (const level of levels.slice(1)) {
      expect(level).toBeLessThanOrEqual(previous + 1);
      previous = level;
    }
  });
});
