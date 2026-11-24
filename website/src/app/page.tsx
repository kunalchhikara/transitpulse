import Link from "next/link";
import { ModeIndicator } from "@/components/ModeIndicator";
import { NAV_ITEMS } from "@/lib/navigation";

/**
 * Home page. WE-01 only needs the route to exist and the nav to work — full
 * home content (fixture-backed summary stats, etc.) is WE-02's job. This is
 * deliberately a minimal placeholder that still links everywhere.
 */
export default function HomePage() {
  const sections = NAV_ITEMS.filter((item) => item.href !== "/");

  return (
    <section className="mx-auto w-full max-w-5xl px-4 py-12 sm:px-6 lg:px-8">
      <ModeIndicator mode="DEMO" />
      <h1 className="mt-4 text-4xl font-bold tracking-tight text-foreground">
        TransitPulse
      </h1>
      <p className="mt-4 max-w-2xl text-base leading-relaxed text-foreground/80">
        A portfolio-grade, Azure/Fabric-focused real-time transport data
        engineering platform for a fictional Sydney public transport
        operator, built on synthetic data. This shell is the navigation
        foundation for the full site; each section below becomes a real,
        content-complete page in its own story.
      </p>

      <h2 className="sr-only">Site sections</h2>
      <ul className="mt-10 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {sections.map((item) => (
          <li key={item.href}>
            <Link
              href={item.href}
              className="block h-full rounded-lg border border-foreground/10 p-4 transition-colors hover:border-foreground/30 hover:bg-foreground/5"
            >
              <span className="font-semibold text-foreground">
                {item.label}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </section>
  );
}
