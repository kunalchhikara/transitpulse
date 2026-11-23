# 6-7 Day Intensive MVP Sprint

## Reality check
A capable multi-agent team can substantially accelerate implementation, testing and documentation, especially because the specification is already detailed and Data/Web work can overlap. A strong portfolio MVP in 6-7 focused days is plausible if authentication, Fabric APIs/features and owner approvals do not block progress.

It is not realistic to guarantee a fully production-hardened platform in 6-7 days. Production hardening includes soak/load testing, security review, accessibility/cross-browser validation, deployment hardening, operational tuning and iterative UX refinement.

## Suggested execution
### Day 1
Repo/agent governance, architecture and ADRs, synthetic domain/schema, simulator skeleton, website shell/demo mode, CI foundations.

### Day 2
Deterministic generator, batch/reference data, incidents at generator level, local Bronze/Silver/Gold logic, tests.

### Day 3
If readiness gate passes, start Fabric trial. Create Lakehouse/pipelines/notebooks, batch/incremental loads, quality/quarantine and reconciliation.

### Day 4
Streaming/Eventstream/Eventhouse, event-time processing, windows, watermarks, duplicates, late and out-of-order events.

### Day 5
Incident end-to-end behaviour, observability, Warehouse/star schema/SCD2, integration tests.

### Day 6
Power BI and website connected views, architecture animations, Incident Control Centre, analytics/health views.

### Day 7
End-to-end incident/recovery suite, security/secret checks, accessibility/build tests, CI/CD, evidence capture, recorded demo, README and deployment polish.

## Definition of success for Day 7
Prefer a smaller end-to-end vertical slice with executed evidence over many partially implemented features. Everything unfinished is recorded transparently as backlog.
