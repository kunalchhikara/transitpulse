# 0003 - Repository structure and branching strategy

Status: Accepted

## Context
Multiple concurrent agents (PM, Data, Web, QA, Lead) and the human owner work in
the same repository. `PROJECT.md` section 22 requires a protected `main`, no
self-approval, and isolated feature branches/worktrees per role to prevent
concurrent agents editing overlapping paths.

## Decision
Top-level ownership by directory:
- `simulator/`, `fabric/`, `sql/` - Data Platform Engineer
- `website/` - Full-Stack Engineer
- `docs/adr/` - Lead (any role may propose, Lead accepts)
- `.github/workflows/` - Lead/CI, changed via PR like any other path

Branch naming: `agent/<role>/<short-topic>`, e.g. `agent/data/telemetry-schema`,
`agent/web/home-page`, `agent/docs/day1-foundation`. Branches are short-lived and
scoped to one story or one cohesive piece of foundational work.

`main` is protected: pull request required, 1 approving review required, no
force-push, no deletion, enforced for admins too. No agent pushes directly to
`main` or approves its own PR. Concurrent agents only run on genuinely
independent, non-overlapping paths (per `PROJECT.md` section 24).

The GitHub repository `kunalchhikara/transitpulse` is public. This was a
deliberate owner decision: GitHub's branch-protection rules (classic and
rulesets) require either a public repository or a paid GitHub Pro plan on the
free tier, and a public repository carries no recurring cost and fits a
portfolio project's end goal. No secrets are ever committed (`.env` is
git-ignored; `.env.example` documents required variables with placeholder
values only).

## Consequences
- Every change except this initial governance commit goes through a PR with
  at least one human-approved review before merging to `main`.
- Directory ownership keeps concurrent Data/Web agent work non-overlapping.
- Because the repo is public, no secrets, employer data or employer logic may
  ever be committed, per `CLAUDE.md`/`PROJECT.md` section 20.
