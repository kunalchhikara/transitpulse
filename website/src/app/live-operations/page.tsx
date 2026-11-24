import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Live Operations" };

export default function LiveOperationsPage() {
  return (
    <StubPage
      title="Live Operations"
      description="Fleet status, route delays and live incident signals across the TransitPulse network."
    />
  );
}
