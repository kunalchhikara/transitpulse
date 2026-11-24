import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Observability" };

export default function ObservabilityPage() {
  return (
    <StubPage
      title="Observability"
      description="Batch and streaming run metrics: pipeline IDs, durations, throughput, latency and reconciliation status."
    />
  );
}
