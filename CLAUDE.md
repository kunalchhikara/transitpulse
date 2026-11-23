# TransitPulse Claude Development Instructions

Read PROJECT.md first. It is authoritative.

## Mission
Operate as a professional Azure/Fabric-focused engineering organisation. Maximise routine automation while preserving correctness, security, evidence, test independence and cost control.

## Non-negotiable scope
- Microsoft Azure / Microsoft Fabric focus.
- AWS is out of scope.
- Synthetic/public-safe data only.
- First Fabric trial window: no paid Fabric capacity or paid Azure infrastructure unless the owner explicitly overrides.
- Delay Fabric trial activation until local foundations are ready.

## Development loop
1. Understand story/criteria.
2. Read relevant code/tests/ADRs.
3. Identify dependencies/interfaces.
4. Make smallest complete change in owned paths.
5. Add/update tests.
6. Execute validation.
7. Update docs/ADR where material.
8. Provide evidence-based handover.

Do not modify unrelated code.

## Architecture
Do not add frameworks, services, dependencies or patterns without a concrete reason. Material architecture changes require an ADR and may require owner approval.

## Git
Never push directly to main. Never force-push main. Use isolated feature branches/worktrees. Developers do not approve their own work. Prevent concurrent agents editing overlapping paths.

## Quality
Generated code is not considered working. Run deterministic tests wherever possible. Never disable tests, weaken assertions merely to pass, hide errors or fabricate output.

## Fabric truthfulness
Claude may only operate Fabric after authenticated access/tools/APIs are configured. Never claim a resource was created, pipeline ran, data loaded, stream processed or deployment succeeded without evidence.

If unavailable, label IMPLEMENTED - NOT CONNECTED or DEMO MODE.

## Security
Never expose or commit .env, credentials, tokens, connection strings or private keys. Use least privilege and secure authentication.

## Cost
Before cloud work, check whether it consumes paid capacity. During the first 60-day Fabric trial, do not provision paid Fabric/Azure resources. Escalate any unavoidable cost to owner before action.

## Website integrity
Operational UIs must distinguish LIVE, CONNECTED TEST, RECORDED and DEMO. Animation does not prove backend execution.

## Autonomy
Resolve routine technical choices without asking the owner. Escalate only real blockers or approval-boundary actions.

## Handover
Story:
Branch:
Files changed:
Implementation:
Tests executed:
Results:
Known limitations:
Security impact:
Cost impact:
Documentation:
Next action:
