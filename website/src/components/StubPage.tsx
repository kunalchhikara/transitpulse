import { ModeIndicator } from "@/components/ModeIndicator";

export interface StubPageProps {
  title: string;
  description: string;
}

/**
 * Shared shell for the 9 pages that are stubs in WE-01 (every page except
 * Home). Always defaults ModeIndicator to DEMO and says plainly that the
 * page is a placeholder — the real content lands in a later, dedicated
 * story, and this file must never be mistaken for that content.
 */
export function StubPage({ title, description }: StubPageProps) {
  return (
    <section className="mx-auto w-full max-w-3xl px-4 py-12 sm:px-6 lg:px-8">
      <ModeIndicator mode="DEMO" />
      <h1 className="mt-4 text-3xl font-bold tracking-tight text-foreground">
        {title}
      </h1>
      <p className="mt-4 text-base leading-relaxed text-foreground/80">
        {description}
      </p>
      <p className="mt-8 rounded-lg border border-dashed border-foreground/20 bg-foreground/5 p-4 text-sm text-foreground/70">
        This page is a placeholder for the website shell story (WE-01). Full{" "}
        {title.toLowerCase()} content, backed by real fixtures or a connected
        source where available, is built in a later story.
      </p>
    </section>
  );
}
