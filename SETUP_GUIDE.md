# TransitPulse Azure Autonomous Setup Guide

## Where to use the prompts
Use Claude Code as the primary development environment. Open a terminal at the repository root and start Claude Code there.

- Paste `prompts/MASTER_CLAUDE_CODE_PROMPT.md` into the MAIN Claude Code session first.
- Later, after basic agent/governance behaviour is working, enable Claude Code agent-team functionality according to the current official documentation for your installed version and paste `prompts/AGENT_TEAM_PROMPT.md` into the MAIN Claude Code session.
- Only when local foundations are mature and you are ready to use the Fabric trial, paste `prompts/FABRIC_READINESS_PROMPT.md` into the MAIN Claude Code session.

Ordinary Claude web/app chat is optional for separate explanations/brainstorming. Do not use it as the main autonomous repository/build environment.

## Files
- PROJECT.md: authoritative project, architecture, cost, timeline and automation specification.
- CLAUDE.md: repository-wide agent rules.
- .claude/agents/*.md: PM, Data, Web and QA roles.
- prompts/: operational prompts for the main Lead session.
- docs/6-7_DAY_SPRINT.md: aggressive MVP schedule and reality check.

## Initial local prerequisites
Install supported/current releases of Git, GitHub CLI, Python, Node.js/npm, VS Code if desired, and Claude Code.

## Sequence
1. Extract the starter pack.
2. Open terminal at repo root.
3. Initialise Git and connect a personal GitHub repository.
4. Review PROJECT.md and CLAUDE.md.
5. Start Claude Code.
6. Paste MASTER_CLAUDE_CODE_PROMPT.md.
7. Let the Lead execute local Phase 0/1 with specialised agents and QA.
8. Do not activate Fabric immediately. Preserve the 60-day trial until local foundations are mature.
9. Once governance is reliable, add agent-team mode and the AGENT_TEAM_PROMPT.
10. When Fabric readiness is reached, intentionally activate/use the Fabric trial, then run FABRIC_READINESS_PROMPT.md.
11. During the first 60 days after Fabric-trial activation, use no paid Fabric capacity or paid Azure infrastructure unless you explicitly override the rule.
12. Build Fabric integration, Power BI, end-to-end incident tests and evidence.
13. Ensure public website DEMO/RECORDED mode remains usable after the trial.

## Git/GitHub automation
Claude Code can manage local Git/branches/commits/worktrees and, once GitHub tooling/authentication is configured, help manage issues/PRs/workflows. Use GitHub as durable state and CI as deterministic evidence.

## Fabric automation
Claude can orchestrate Fabric only after you configure supported authenticated Fabric APIs/Git/CLI/MCP/tooling. Never paste raw access tokens into prompts. Start with controlled interactive authentication; later use a dedicated least-privilege automation identity where appropriate.

## Cost model
Cloud platform target for first Fabric trial period: $0 by using the free trial and avoiding paid Azure resources.
Claude Code/model usage is a separate potential cost and depends on your Claude subscription/API arrangement. Agent concurrency increases model usage, so use concurrency for independent work rather than keeping agents busy artificially.
