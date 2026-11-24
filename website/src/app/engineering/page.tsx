import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Engineering" };

export default function EngineeringPage() {
  return (
    <StubPage
      title="Engineering"
      description="How TransitPulse is built: architecture decisions, CI/CD, testing strategy and the autonomous agent workflow."
    />
  );
}
