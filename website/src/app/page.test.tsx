import { render, screen } from "@testing-library/react";
import HomePage from "./page";
import { NAV_ITEMS } from "@/lib/navigation";
import homeFixture from "@/fixtures/home.json";

/**
 * Home page render test (WE-02 test requirement): asserts summary content,
 * nav links sourced from `navigation.ts`, and that stat values on the page
 * match the static fixture exactly.
 */
describe("HomePage", () => {
  it("renders a single top-level heading with the site name", () => {
    render(<HomePage />);
    const heading = screen.getByRole("heading", { level: 1 });
    expect(heading).toHaveTextContent("TransitPulse");
  });

  it("renders a visible DEMO mode indicator", () => {
    render(<HomePage />);
    const indicator = screen.getByRole("status");
    expect(indicator).toHaveAttribute("data-mode", "DEMO");
    expect(indicator).toHaveTextContent("DEMO");
  });

  it("renders the fixture's summary paragraphs", () => {
    render(<HomePage />);
    for (const paragraph of homeFixture.summary) {
      expect(screen.getByText(paragraph)).toBeInTheDocument();
    }
  });

  it("renders a link to every other nav section, sourced from navigation.ts", () => {
    render(<HomePage />);
    const otherSections = NAV_ITEMS.filter((item) => item.href !== "/");
    expect(otherSections.length).toBeGreaterThan(0);

    for (const item of otherSections) {
      const link = screen.getByRole("link", { name: item.label });
      expect(link).toHaveAttribute("href", item.href);
    }
  });

  it("renders at least one fixture-sourced stat block with matching values", () => {
    render(<HomePage />);
    expect(homeFixture.stats.length).toBeGreaterThanOrEqual(1);

    for (const stat of homeFixture.stats) {
      expect(screen.getByText(stat.label)).toBeInTheDocument();
      expect(screen.getByText(String(stat.value))).toBeInTheDocument();
      expect(screen.getByText(stat.description)).toBeInTheDocument();
    }
  });

  it("never claims a LIVE or CONNECTED TEST backend", () => {
    render(<HomePage />);
    expect(screen.queryByText("LIVE")).not.toBeInTheDocument();
    expect(screen.queryByText("CONNECTED TEST")).not.toBeInTheDocument();
  });
});
