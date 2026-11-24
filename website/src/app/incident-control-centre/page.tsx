import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Incident Control Centre" };

export default function IncidentControlCentrePage() {
  return (
    <StubPage
      title="Incident Control Centre"
      description="Trigger supported incident scenarios and observe detection, engineering response, metric effect and recovery."
    />
  );
}
