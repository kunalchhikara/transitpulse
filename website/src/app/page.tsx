import Link from "next/link";
import { ModeIndicator } from "@/components/ModeIndicator";
import { NAV_ITEMS } from "@/lib/navigation";
import { assertHomeFixture } from "@/lib/homeFixture";
import homeFixtureData from "@/fixtures/home.json";

/**
 * Home page (WE-02: "Home Page Content with Static Demo Fixtures").
 *
 * All content below the title comes from the hand-authored static fixture
 * at `src/fixtures/home.json`, validated against the `HomeFixture`
 * contract in `src/lib/homeFixture.ts` (see that module for the
 * compile-time + runtime check rationale). Nav links are read from
 * `NAV_ITEMS`, never hand-duplicated, so this page can't drift from the
 * site's actual routes.
 */
const homeFixture = assertHomeFixture(homeFixtureData);

export default function HomePage() {
  const sections = NAV_ITEMS.filter((item) => item.href !== "/");

  return (
    <section className="mx-auto w-full max-w-5xl px-4 py-12 sm:px-6 lg:px-8">
      <ModeIndicator mode="DEMO" />

      <h1 className="mt-4 text-4xl font-bold tracking-tight text-foreground">
        TransitPulse
      </h1>
      <p className="mt-2 max-w-2xl text-lg text-foreground/70">
        {homeFixture.tagline}
      </p>

      <div className="mt-6 max-w-2xl space-y-4">
        {homeFixture.summary.map((paragraph) => (
          <p
            key={paragraph.slice(0, 40)}
            className="text-base leading-relaxed text-foreground/80"
          >
            {paragraph}
          </p>
        ))}
      </div>

      <h2 className="mt-12 text-xl font-semibold tracking-tight text-foreground">
        By the numbers
      </h2>
      <p className="mt-1 max-w-2xl text-sm text-foreground/60">
        {homeFixture.fixtureNote}
      </p>
      <dl className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
        {homeFixture.stats.map((stat) => (
          <div
            key={stat.id}
            className="rounded-lg border border-foreground/10 p-4"
          >
            <dt className="text-sm font-medium text-foreground/60">
              {stat.label}
            </dt>
            <dd className="mt-1 text-3xl font-bold text-foreground">
              {stat.value}
            </dd>
            <dd className="mt-1 text-xs leading-relaxed text-foreground/60">
              {stat.description}
            </dd>
          </div>
        ))}
      </dl>

      <h2 className="mt-12 text-xl font-semibold tracking-tight text-foreground">
        Architecture at a glance
      </h2>
      <p className="mt-2 max-w-2xl text-base leading-relaxed text-foreground/80">
        {homeFixture.architectureTeaser}
      </p>
      <Link
        href="/architecture"
        className="mt-3 inline-block text-sm font-semibold text-sky-700 hover:underline dark:text-sky-400"
      >
        View the Architecture page →
      </Link>

      <h2 className="mt-12 text-xl font-semibold tracking-tight text-foreground">
        Explore the platform
      </h2>
      <ul className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
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
