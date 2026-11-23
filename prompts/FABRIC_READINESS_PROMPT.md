# Paste into MAIN Claude Code only when local foundations are mature and you are ready to start/use the Fabric trial

Perform a Fabric readiness review before any Fabric provisioning.

Read PROJECT.md, CLAUDE.md, current ADRs, tests, local simulator, schemas and CI results.

Confirm with evidence:
- synthetic domain/schema is stable enough for integration
- simulator normal and incident modes run locally
- data contracts are versioned
- relevant local tests pass
- secrets are not committed
- Git repository and branch/PR approach are ready
- batch transformation logic has local coverage where practical
- website has a DEMO/RECORDED fallback path
- likely Fabric artifacts and responsibilities are documented
- no AWS dependency exists

Then provide:
1. exact Fabric artifacts to create
2. dependency order
3. which can be automated using authenticated APIs/Git/tooling and which require manual steps
4. expected trial-cost status
5. security/authentication requirements
6. validation evidence required after each create/deploy operation
7. rollback/cleanup approach

Do not create or deploy anything until confirming the work fits the no-paid-Fabric/Azure rule. If an operation may incur cost, stop and ask me.
