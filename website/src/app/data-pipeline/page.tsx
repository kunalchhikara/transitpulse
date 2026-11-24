import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Data Pipeline" };

export default function DataPipelinePage() {
  return (
    <StubPage
      title="Data Pipeline"
      description="Batch ingestion, Bronze/Silver/Gold medallion processing and incremental-load runs."
    />
  );
}
