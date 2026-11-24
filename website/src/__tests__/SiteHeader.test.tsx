import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SiteHeader } from "@/components/SiteHeader";
import { NAV_ITEMS } from "@/lib/navigation";

jest.mock("next/navigation", () => ({
  usePathname: () => "/",
}));

describe("SiteHeader", () => {
  it("renders a Primary navigation landmark with a link for every nav item", () => {
    render(<SiteHeader />);
    const nav = screen.getByRole("navigation", { name: "Primary" });

    for (const item of NAV_ITEMS) {
      const link = screen.getByRole("link", { name: item.label });
      expect(nav).toContainElement(link);
      expect(link).toHaveAttribute("href", item.href);
    }
  });

  it("marks the current page link with aria-current", () => {
    render(<SiteHeader />);
    const homeLink = screen.getByRole("link", { name: "Home" });
    expect(homeLink).toHaveAttribute("aria-current", "page");
  });

  it("toggles the mobile navigation disclosure button state on click", async () => {
    const user = userEvent.setup();
    render(<SiteHeader />);
    const toggle = screen.getByRole("button", { name: /toggle site navigation/i });

    expect(toggle).toHaveAttribute("aria-expanded", "false");
    await user.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
  });
});
