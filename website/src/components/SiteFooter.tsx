/**
 * Site-wide footer. Carries the project's standing truthfulness disclosure
 * (CLAUDE.md "Website integrity") so it is visible from every page, not
 * just repeated per-page.
 */
export function SiteFooter() {
  return (
    <footer className="border-t border-foreground/10">
      <div className="mx-auto flex max-w-6xl flex-col gap-2 px-4 py-6 text-xs text-foreground/60 sm:px-6 lg:px-8">
        <p>
          TransitPulse is a personal, portfolio-grade Azure/Fabric data
          engineering project using synthetic data only. Every page is
          labelled LIVE, CONNECTED TEST, RECORDED or DEMO — animation or UI
          state alone is never presented as proof that a backend ran.
        </p>
        <p>
          &copy; {new Date().getFullYear()} TransitPulse. Source on{" "}
          <a
            href="https://github.com/kunalchhikara/transitpulse"
            className="underline hover:text-foreground"
            target="_blank"
            rel="noopener noreferrer"
          >
            GitHub
          </a>
          .
        </p>
      </div>
    </footer>
  );
}
