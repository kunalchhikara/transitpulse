import homeFixtureData from "@/fixtures/home.json";
import {
  assertHomeFixture,
  isHomeFixture,
  type HomeFixture,
} from "./homeFixture";

/**
 * Fixture-shape test (WE-02 test requirement): proves the actual
 * `home.json` fixture matches the `HomeFixture` contract at runtime, and
 * that the guard genuinely rejects malformed shapes rather than always
 * returning true.
 */
describe("HomeFixture contract", () => {
  it("accepts the real home.json fixture", () => {
    expect(isHomeFixture(homeFixtureData)).toBe(true);
    expect(() => assertHomeFixture(homeFixtureData)).not.toThrow();
  });

  it("exposes at least one stat, as required by WE-02 acceptance criteria", () => {
    const fixture = assertHomeFixture(homeFixtureData);
    expect(fixture.stats.length).toBeGreaterThanOrEqual(1);
  });

  const validFixture: HomeFixture = {
    tagline: "tagline",
    summary: ["paragraph one"],
    architectureTeaser: "teaser",
    stats: [
      { id: "a", label: "A", value: 1, description: "description" },
    ],
    fixtureNote: "note",
  };

  it("accepts a minimal well-formed fixture", () => {
    expect(isHomeFixture(validFixture)).toBe(true);
  });

  it.each<[string, unknown]>([
    ["null", null],
    ["a non-object", "not an object"],
    ["missing tagline", { ...validFixture, tagline: undefined }],
    ["empty summary array", { ...validFixture, summary: [] }],
    ["summary with a non-string entry", { ...validFixture, summary: [42] }],
    ["missing stats array", { ...validFixture, stats: undefined }],
    ["empty stats array", { ...validFixture, stats: [] }],
    [
      "a stat with a non-numeric value",
      {
        ...validFixture,
        stats: [{ id: "a", label: "A", value: "50", description: "d" }],
      },
    ],
    [
      "a stat missing a label",
      { ...validFixture, stats: [{ id: "a", value: 1, description: "d" }] },
    ],
    ["missing fixtureNote", { ...validFixture, fixtureNote: undefined }],
  ])("rejects a fixture with %s", (_name, malformed) => {
    expect(isHomeFixture(malformed)).toBe(false);
    expect(() => assertHomeFixture(malformed)).toThrow(
      /does not match the HomeFixture contract/,
    );
  });
});
