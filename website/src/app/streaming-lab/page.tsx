import type { Metadata } from "next";
import { StubPage } from "@/components/StubPage";

export const metadata: Metadata = { title: "Streaming Lab" };

export default function StreamingLabPage() {
  return (
    <StubPage
      title="Streaming Lab"
      description="Event-time vs processing-time behaviour, windowing, watermarks, and out-of-order/duplicate handling."
    />
  );
}
