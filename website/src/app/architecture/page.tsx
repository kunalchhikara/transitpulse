import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Architecture" };

export default function ArchitecturePage() {
  return (
    <StubPage
      title="Architecture"
      description="Batch, streaming and combined architecture diagrams showing how data moves through TransitPulse."
    />
  );
}
