import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Data Quality" };

export default function DataQualityPage() {
  return (
    <StubPage
      title="Data Quality"
      description="Validation rules, quarantine outcomes, and quality/reconciliation counters across pipeline runs."
    />
  );
}
