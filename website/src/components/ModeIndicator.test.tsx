import { render, screen } from "@testing-library/react";
import { ModeIndicator, type SiteMode } from "./ModeIndicator";

describe("ModeIndicator", () => {
  it("defaults to DEMO when no mode prop is given", () => {
    render(<ModeIndicator />);
    const indicator = screen.getByRole("status");
    expect(indicator).toHaveAttribute("data-mode", "DEMO");
    expect(indicator).toHaveTextContent("DEMO");
  });

  it.each<[SiteMode, string]>([
    ["LIVE", "LIVE"],
    ["CONNECTED_TEST", "CONNECTED TEST"],
    ["RECORDED", "RECORDED"],
    ["DEMO", "DEMO"],
  ])("renders the %s mode with label %p", (mode, expectedLabel) => {
    render(<ModeIndicator mode={mode} />);
    const indicator = screen.getByRole("status");
    expect(indicator).toHaveAttribute("data-mode", mode);
    expect(indicator).toHaveTextContent(expectedLabel);
  });

  it("renders exactly one of the four supported states at a time", () => {
    const allLabels = ["LIVE", "CONNECTED TEST", "RECORDED", "DEMO"];

    render(<ModeIndicator mode="RECORDED" />);
    const indicator = screen.getByRole("status");

    const matchedLabels = allLabels.filter((label) =>
      indicator.textContent?.includes(label),
    );

    // "RECORDED" only — guards against e.g. CONNECTED_TEST's description
    // text leaking the word "backend" into another state's label by
    // accident, or two states being shown simultaneously.
    expect(matchedLabels).toEqual(["RECORDED"]);
  });

  it("exposes a human-readable description for assistive tech", () => {
    render(<ModeIndicator mode="DEMO" />);
    const indicator = screen.getByRole("status");
    expect(indicator).toHaveAttribute(
      "title",
      expect.stringContaining("No backend was executed"),
    );
  });
});
