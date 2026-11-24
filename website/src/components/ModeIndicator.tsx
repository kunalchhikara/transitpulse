/**
 * ModeIndicator — the compliance mechanism required by PROJECT.md section 14
 * and 18, and CLAUDE.md "Website integrity": every operational surface must
 * say, unambiguously, whether what it shows is real.
 *
 * Exactly four states are supported. There is no fifth "it's complicated"
 * state — a page must pick one honestly:
 *   - LIVE            Real-time data from a connected, running backend.
 *   - CONNECTED_TEST   A real backend/pipeline, but a test/non-production run.
 *   - RECORDED         A recorded snapshot of real output, replayed.
 *   - DEMO             Synthetic/static data with no backend behind it at all.
 *
 * `mode` defaults to "DEMO" because during WE-01 there is no backend to be
 * live or connected to — defaulting to anything else would misrepresent a
 * static stub page as doing real work.
 */
export type SiteMode = "LIVE" | "CONNECTED_TEST" | "RECORDED" | "DEMO";

interface ModeConfig {
  label: string;
  description: string;
  badgeClassName: string;
  dotClassName: string;
}

const MODE_CONFIG: Record<SiteMode, ModeConfig> = {
  LIVE: {
    label: "LIVE",
    description:
      "Live data from a connected, currently running backend or pipeline.",
    badgeClassName:
      "bg-emerald-50 text-emerald-800 ring-emerald-600/30 dark:bg-emerald-950 dark:text-emerald-300 dark:ring-emerald-400/40",
    dotClassName: "bg-emerald-500",
  },
  CONNECTED_TEST: {
    label: "CONNECTED TEST",
    description:
      "Connected to a real backend or pipeline run in a test/non-production configuration.",
    badgeClassName:
      "bg-sky-50 text-sky-800 ring-sky-600/30 dark:bg-sky-950 dark:text-sky-300 dark:ring-sky-400/40",
    dotClassName: "bg-sky-500",
  },
  RECORDED: {
    label: "RECORDED",
    description:
      "A recorded snapshot of real output, replayed — not a live connection.",
    badgeClassName:
      "bg-amber-50 text-amber-800 ring-amber-600/30 dark:bg-amber-950 dark:text-amber-300 dark:ring-amber-400/40",
    dotClassName: "bg-amber-500",
  },
  DEMO: {
    label: "DEMO",
    description:
      "Simulated/static data shown for demonstration only. No backend was executed.",
    badgeClassName:
      "bg-slate-100 text-slate-700 ring-slate-500/30 dark:bg-slate-800 dark:text-slate-300 dark:ring-slate-400/30",
    dotClassName: "bg-slate-500",
  },
};

export interface ModeIndicatorProps {
  /** One of the 4 supported modes. Defaults to "DEMO" — see module docs. */
  mode?: SiteMode;
  className?: string;
}

export function ModeIndicator({ mode = "DEMO", className = "" }: ModeIndicatorProps) {
  const config = MODE_CONFIG[mode];

  return (
    <span
      role="status"
      data-mode={mode}
      title={config.description}
      className={`inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-wide ring-1 ring-inset ${config.badgeClassName} ${className}`.trim()}
    >
      <span aria-hidden="true" className={`h-2 w-2 shrink-0 rounded-full ${config.dotClassName}`} />
      <span>{config.label}</span>
      <span className="sr-only"> — {config.description}</span>
    </span>
  );
}
