# website/

Owned by: Full-Stack Engineer (`.claude/agents/web-engineer.md`).

Purpose: public operational portfolio website (Next.js, React, TypeScript, Tailwind
CSS) — Home, Live Operations, Architecture, Data Pipeline, Streaming Lab, Incident
Control Centre, Data Quality, Observability, Analytics and Engineering pages, per
`PROJECT.md` section 17. Must clearly distinguish LIVE / CONNECTED TEST / RECORDED /
DEMO and remain useful after the Fabric trial expires.

Status: WE-01 (Website Shell, Navigation & Mode-Indicator Foundation) implemented.
Remaining scope (page content, live/connected data) is not yet implemented — see
`docs/BACKLOG.md` (WE-02, WE-03).

This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `src/app/page.tsx`. The page auto-updates as you edit the file.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.

---

## TransitPulse conventions

This directory (`website/`) is owned by the Full-Stack Engineer role (see
`.claude/agents/web-engineer.md`) — frontend code and frontend tests only.
No Azure/Fabric hosting is provisioned for this site; it must remain fully
usable as a static/local build after the Fabric trial expires
(`PROJECT.md` section 17, `CLAUDE.md` "Website integrity").

### Stack

- Next.js 16 (App Router), React 19, TypeScript, Tailwind CSS 3.
- ESLint 9 with flat config (`eslint.config.mjs`), built directly on
  `eslint-config-next`'s own flat `core-web-vitals`/`typescript` exports —
  `next lint` was removed in Next.js 16, so `npm run lint` runs `eslint .`
  directly.
- Jest 30 + `next/jest` + React Testing Library + `@testing-library/user-event`
  for component/unit tests, `jest-axe` for accessibility smoke checks.

### Scripts

- `npm run dev` — local dev server.
- `npm run build` — production build (also the source of truth that every
  route in `src/app/**/page.tsx` actually resolves).
- `npm run lint` — ESLint over the whole project.
- `npm run test` — Jest once; `npm run test:watch` — Jest in watch mode.

### Mode indicator (LIVE / CONNECTED TEST / RECORDED / DEMO)

`src/components/ModeIndicator.tsx` is the single place that renders the
four states required by `PROJECT.md` sections 14 and 18. It defaults to
`DEMO` — every page must either pass no `mode` prop (get DEMO for free) or
explicitly justify a different mode with a real, verifiable data source
behind it. `CLAUDE.md`'s "Website integrity" rule means:

- Never pass `mode="LIVE"` or `mode="CONNECTED_TEST"` unless the page is
  genuinely talking to a running backend/pipeline at that moment.
- `RECORDED` is for replaying a real, previously captured snapshot — not a
  synonym for "looks realistic".
- Everything else — static fixtures, hand-authored demo data, placeholder
  stub pages — is `DEMO`, no exceptions.

As of WE-01 there is no backend at all, so every page on the site uses
`DEMO` and nothing else; this is enforced by the route-smoke test in
`src/__tests__/routes.test.tsx`, which asserts no page ever renders a
`LIVE` or `CONNECTED TEST` label.

### Navigation / routes

`src/lib/navigation.ts` is the single source of truth for the primary nav
(`SiteHeader`) and for the route-smoke test — add a page and a nav entry
together so they can't drift apart. Stub pages (everything except Home for
now) share the `StubPage` component so the "this is a placeholder" framing
stays consistent until each page's real content story lands.

### Testing conventions

- Co-locate simple component tests next to the component
  (`ModeIndicator.test.tsx`); put cross-cutting tests (routes, header,
  accessibility) under `src/__tests__/`.
- Prefer React Testing Library's accessible queries (`getByRole`,
  `getByText`) over snapshot tests or querying by CSS class/test-id.
- `jest-axe` is used deliberately sparingly — on the page shell, not on
  every stub — because it's meant as a smoke check for landmark/label/
  contrast regressions, not a substitute for a real accessibility audit.
- Mock `next/navigation` hooks (e.g. `usePathname`) in tests that render
  client components using them; page components rendered directly in
  `routes.test.tsx` don't need this since they don't call router hooks
  themselves.

### Styling conventions

- Tailwind utility classes in JSX; no CSS-in-JS, no separate stylesheet
  per component. Shared design tokens (`--background`, `--foreground`)
  live in `src/app/globals.css` and respect `prefers-color-scheme`.
- No mapping/charting library is selected yet (deliberately deferred per
  the backlog — a later, separately-approved story). Pages that will
  eventually need one use a plain placeholder in the meantime.
