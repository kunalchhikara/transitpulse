/**
 * Single source of truth for TransitPulse site navigation.
 *
 * PROJECT.md section 17 requires exactly these 10 pages. SiteHeader renders
 * this list, and the route-smoke test walks it to confirm every route
 * resolves to a real page module. Add a page here and in `src/app/<href>`
 * together so navigation and routes never drift apart.
 */
export interface NavItem {
  /** Label shown in the navigation UI. */
  label: string;
  /** App Router path, always starting with "/". */
  href: string;
}

export const NAV_ITEMS: readonly NavItem[] = [
  { label: "Home", href: "/" },
  { label: "Live Operations", href: "/live-operations" },
  { label: "Architecture", href: "/architecture" },
  { label: "Data Pipeline", href: "/data-pipeline" },
  { label: "Streaming Lab", href: "/streaming-lab" },
  { label: "Incident Control Centre", href: "/incident-control-centre" },
  { label: "Data Quality", href: "/data-quality" },
  { label: "Observability", href: "/observability" },
  { label: "Analytics", href: "/analytics" },
  { label: "Engineering", href: "/engineering" },
] as const;
