/**
 * Type contract + runtime guard for `website/src/fixtures/home.json`.
 *
 * WE-02 ("Home Page Content with Static Demo Fixtures") deliberately keeps
 * Home's data a small, hand-authored static fixture — see
 * `website/README.md` "Home page fixture (WE-02)". This module is the
 * single source of truth for what a well-formed home fixture looks like:
 *
 *  - `HomeFixture`/`HomeStat` give the JSON import a compile-time
 *    structural check (see `src/app/page.tsx`, which assigns the imported
 *    JSON literal to this interface).
 *  - `isHomeFixture`/`assertHomeFixture` give a runtime check, so a
 *    corrupted or hand-edited-wrong fixture fails loudly and specifically
 *    instead of rendering `undefined` deep inside JSX or silently showing
 *    wrong numbers.
 *
 * `value` on a stat is a plain number (not a pre-formatted string) so the
 * page can render it consistently and a future connected data source could
 * populate the same shape without a template change.
 */

export interface HomeStat {
  /** Stable identifier, used as the React list key. */
  id: string;
  /** Short label shown above the number, e.g. "Fleet vehicles". */
  label: string;
  /** The statistic itself, e.g. 50. */
  value: number;
  /** Optional unit suffix, e.g. "vehicles" or "km". */
  unit?: string;
  /** One sentence of context, including where the number comes from. */
  description: string;
}

export interface HomeFixture {
  /** One-line subtitle shown directly under the page title. */
  tagline: string;
  /** Summary paragraphs introducing TransitPulse. */
  summary: string[];
  /** Short paragraph teasing the medallion/batch+streaming architecture. */
  architectureTeaser: string;
  /** At least one stat block (WE-02 acceptance criteria). */
  stats: HomeStat[];
  /** Explicit disclosure that these figures are illustrative demo data. */
  fixtureNote: string;
}

function isHomeStat(value: unknown): value is HomeStat {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.id === "string" &&
    candidate.id.length > 0 &&
    typeof candidate.label === "string" &&
    candidate.label.length > 0 &&
    typeof candidate.value === "number" &&
    Number.isFinite(candidate.value) &&
    (candidate.unit === undefined || typeof candidate.unit === "string") &&
    typeof candidate.description === "string" &&
    candidate.description.length > 0
  );
}

/** Runtime structural check — see module docs. */
export function isHomeFixture(value: unknown): value is HomeFixture {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const candidate = value as Record<string, unknown>;

  return (
    typeof candidate.tagline === "string" &&
    candidate.tagline.length > 0 &&
    Array.isArray(candidate.summary) &&
    candidate.summary.length > 0 &&
    candidate.summary.every((paragraph) => typeof paragraph === "string") &&
    typeof candidate.architectureTeaser === "string" &&
    candidate.architectureTeaser.length > 0 &&
    Array.isArray(candidate.stats) &&
    candidate.stats.length > 0 &&
    candidate.stats.every(isHomeStat) &&
    typeof candidate.fixtureNote === "string" &&
    candidate.fixtureNote.length > 0
  );
}

/**
 * Validates `value` against the `HomeFixture` contract and returns it typed,
 * or throws with a message pointing at the fixture file and this contract.
 * Used by `src/app/page.tsx` so a malformed `home.json` fails fast and
 * legibly rather than rendering broken content.
 */
export function assertHomeFixture(value: unknown): HomeFixture {
  if (!isHomeFixture(value)) {
    throw new Error(
      "website/src/fixtures/home.json does not match the HomeFixture " +
        "contract defined in website/src/lib/homeFixture.ts.",
    );
  }
  return value;
}
