import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Analytics" };

export default function AnalyticsPage() {
  return (
    <StubPage
      title="Analytics"
      description="Route performance, passenger demand and historical reliability trends from the Gold/Warehouse layer."
    />
  );
}
