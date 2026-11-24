# 0004 - Local-first development and Fabric trial activation gate

Status: Accepted

## Context
`PROJECT.md` sections 3 and 30 impose a hard cost rule: no paid Fabric capacity
or paid Azure infrastructure during the first 60 days after the Fabric trial is
activated, and the trial itself must not be activated prematurely, since it is
a time-boxed resource.

## Decision
Phase A (current): build and test everything that does not require a live
Fabric workspace, locally and in CI -
repo/agent governance, synthetic domain model and schemas, deterministic
simulator, incident-generation logic, local Bronze/Silver/Gold transformation
logic, unit/data-quality tests, website shell in DEMO mode, and CI foundations.

Phase B: activate the 60-day Fabric trial only once Phase A is sufficiently
mature to make productive use of the trial window (readiness gate owned by the
Lead, requiring explicit owner approval to activate, per `PROJECT.md` section
27). During the 60 days, use the trial aggressively for Fabric-native
implementation and evidence collection, provisioning no paid Fabric capacity or
paid Azure infrastructure unless the owner explicitly overrides this rule.

Before trial expiry, export/commit all source-controllable Fabric artifacts,
capture architecture/operational evidence and screenshots/recordings, and
ensure the website's DEMO/RECORDED mode works fully independently of any live
Fabric connection.

## Consequences
- No agent may activate the Fabric trial or provision Fabric/Azure resources
  without the Lead confirming the readiness gate and the owner approving.
- Work is sequenced so Phase A artifacts (schemas, simulator, tests, website
  shell) are reusable once Fabric-native work begins, avoiding rework.
- Any claim of Fabric execution without evidence is prohibited; unavailable
  Fabric work is labelled `IMPLEMENTED - NOT CONNECTED` or `DEMO MODE`.
