# TransitPulse
## Azure/Fabric-Focused Production-Style Real-Time Transport Data Engineering Platform

## 1. Vision
TransitPulse is a personal, portfolio-grade, Azure/Fabric-focused data engineering project. It demonstrates production-style batch and streaming ingestion, Lakehouse/Warehouse engineering, real-time processing, data quality, observability, incident simulation, analytics, and an interactive public website.

It uses only synthetic data or explicitly reusable public data. It must never use employer data, code, schemas, credentials, architecture, business logic, or organisational cloud resources.

The primary career objective is Microsoft Azure / Microsoft Fabric data engineering. AWS is explicitly out of scope for this project unless the owner later changes the scope.

## 2. Core Technology Direction
Primary platform:
- Microsoft Fabric
- OneLake
- Fabric Data Factory
- Fabric Lakehouse
- Delta Lake
- PySpark
- Eventstream
- Eventhouse / Real-Time Intelligence where appropriate
- Fabric Warehouse
- SQL
- Power BI

Engineering/development:
- Python
- Git / GitHub
- GitHub Actions
- Next.js
- React
- TypeScript
- Tailwind CSS
- automated testing, security checks and CI/CD
- Claude Code as the main autonomous engineering orchestrator

## 3. Hard Cost Rule
### First 60 days: no paid Fabric/Azure infrastructure
The project must use the Microsoft Fabric free trial for cloud platform work during the first 60-day Fabric window. Do not purchase a Fabric capacity or create paid Azure infrastructure during this window unless the owner explicitly changes this rule.

Delay activation of the Fabric trial until local foundations are sufficiently mature to make productive use of the full trial window.

Before activating the Fabric trial, complete as much as practical locally:
- repository/governance
- synthetic transport domain model
- event/data schemas
- deterministic simulator
- incident generator logic
- batch transformation logic that can be tested locally
- test framework
- website shell/demo mode
- CI foundations
- architecture and ADRs

Once the Fabric trial starts, use the free trial aggressively for Fabric-native implementation, end-to-end testing, evidence collection, dashboards and integration.

Do not require paid Fabric after trial expiry for the portfolio to remain usable. The public website must retain a clearly labelled DEMO/RECORDED mode.

AI tooling such as Claude Code may have a separate subscription/API cost. Do not confuse AI-agent cost with cloud-platform cost.

## 4. Business Scenario
TransitPulse represents a fictional Sydney public transport operator managing:
- buses
- routes
- stops
- depots
- scheduled trips
- passenger journeys
- service incidents

Synthetic vehicles continuously transmit telemetry. Operations teams need live fleet status, route delays, incidents and quality indicators. Management needs historical route/reliability analysis. Engineers need pipeline, streaming, quality and reconciliation telemetry.

## 5. Initial Configurable Scale
Start around:
- 50 vehicles
- 10 routes
- 100-200 stops
- 4 depots
- realistic synthetic Sydney-area coordinates
- scheduled trips
- passenger journeys

Scale must be configuration-driven rather than requiring redesign.

## 6. Streaming Telemetry
Representative event fields:
- event_id
- schema_version
- generated_at
- event_time
- vehicle_id
- route_id
- trip_id
- latitude
- longitude
- speed_kmh
- heading
- occupancy
- delay_seconds
- status

Downstream layers should capture ingestion_time and processing_time.

Streaming must demonstrate:
- event time vs processing time
- windowing
- watermarks
- late-arriving events
- out-of-order events
- duplicate events and deduplication
- malformed-event validation
- quarantine
- enrichment
- throughput and latency metrics
- recovery from incidents

## 7. Batch Sources
Synthetic/reference datasets:
- vehicles
- routes
- stops
- depots
- schedules
- historical trips
- historical telemetry
- passenger journeys
- service disruptions
- optional synthetic weather observations

Demonstrate both full reference loads and incremental transactional loads.

## 8. Medallion Architecture
### Bronze
Retain source representation with minimum transformation and ingestion metadata for replay, traceability and troubleshooting.

### Silver
Schema/type enforcement, standardisation, GPS/timestamp validation, deduplication, quality checks, reference enrichment, late-data handling and quarantine routing.

### Gold
Business/analytics models supporting route performance, vehicle performance, trip performance, passenger demand, reliability, streaming operations and data-quality metrics.

Do not create layers merely to say the project uses medallion architecture. Every copy/transformation must have a reason.

## 9. Lakehouse vs Warehouse
Lakehouse:
- Bronze
- Silver
- detailed event history
- Delta-based engineering
- Spark workloads

Warehouse:
- dimensional models
- curated business facts/dimensions
- BI-oriented SQL access
- Power BI semantic models

Document why each dataset belongs in its selected destination.

## 10. Dimensional Model
Candidate dimensions:
- dim_vehicle
- dim_route
- dim_stop
- dim_depot
- dim_date
- dim_time

Candidate facts:
- fact_vehicle_telemetry
- fact_trip
- fact_passenger_journey
- fact_route_performance
- fact_service_incident

At least one dimension must demonstrate SCD Type 2, such as historical vehicle-to-depot assignment.

## 11. Incremental Batch Ingestion
Use watermarks. Advance a watermark only after successful processing. Demonstrate:
- initial/full load
- incremental load
- retries
- partial-failure behaviour
- reruns
- idempotency
- duplicate prevention
- reconciliation
- audit logging

## 12. Data Quality and Quarantine
Configurable validation rules include:
- required event_id/vehicle_id
- known route where applicable
- valid latitude/longitude
- nonnegative speed
- configurable upper-speed threshold
- nonnegative occupancy
- parsable timestamps
- supported schema version
- future-time tolerance

Invalid events must not silently disappear. Quarantine should preserve raw payload, rejection reason/rule, ingestion time, source and run ID.

Track total, valid, quarantined, duplicate, late and schema-failure counts and quality percentage.

## 13. Incident Simulation
Incident simulation is first-class, not an add-on.

Scenarios:
1. Bad/malformed data: null vehicle IDs, invalid coordinates, negative speeds, malformed timestamps, unknown routes and missing fields.
2. Duplicate-event injection: replay event IDs and prove no downstream double-counting.
3. Late-event injection: event_time significantly earlier than arrival.
4. Out-of-order delivery.
5. Route congestion: lower speed and raise delays so route KPIs visibly degrade.
6. Volume spike: temporarily increase configurable event rate (for example 5x-10x in controlled tests) and measure throughput/latency/lag where available.
7. Vehicle breakdown: status becomes BREAKDOWN and a service incident is created.
8. Recovery: restore normal state and demonstrate metric recovery.

Incidents must produce real processing consequences when in connected mode.

## 14. Incident Control Centre
Website controls should safely trigger supported scenarios. It must show:
Incident -> detection -> engineering response -> metric effect -> recovery.

Every operational surface must distinguish appropriately between:
- LIVE
- CONNECTED TEST
- RECORDED
- DEMO

Never present frontend-only animation as proof that Fabric processed something.

## 15. Observability and Reconciliation
Batch metrics:
- pipeline/run ID
- start/end/duration
- source/target/rejected counts
- inserted/updated counts where relevant
- status/error
- watermark before/after

Streaming metrics where available:
- received
- processed
- quarantined
- duplicates
- late events
- throughput
- processing latency

Critical batch reconciliation should satisfy a clearly documented accounting identity, for example source extracted = valid loaded + quarantined + intentionally excluded.

## 16. Power BI
Suggested report pages:
- Network Overview
- Live Operations
- Route Performance
- Passenger Analytics
- Historical Trends
- Data Engineering Health

Do not weaken security to embed a public Power BI report. If secure/licensed embedding is unsuitable, use safe screenshots, recorded views or purpose-built web analytics.

## 17. Public Website
The public website is the portfolio experience, not a static README.

Suggested stack:
- Next.js
- React
- TypeScript
- Tailwind CSS
- maintainable chart/mapping libraries selected at implementation time

Pages:
- Home
- Live Operations
- Architecture (Batch / Streaming / Combined)
- Data Pipeline
- Streaming Lab
- Incident Control Centre
- Data Quality
- Observability
- Analytics
- Engineering

Architecture animations must communicate real data flow and engineering concepts. Site must remain useful when Fabric is offline or the trial expires.

## 18. Website Modes
### LIVE / CONNECTED TEST
Uses actual current project outputs where safely available.

### RECORDED / DEMO
Uses recorded/synthetic snapshots and local simulation after cloud resources are unavailable. Mode must be clearly labelled.

## 19. Synthetic Event Generator
Python generator requirements:
- deterministic seed
- configurable vehicle count
- configurable event rate
- realistic route movement
- unique normal event IDs
- schema version
- incident controls
- structured logs
- graceful shutdown
- externally stored configuration

Do not scatter magic operational constants throughout source files.

## 20. Security
Never commit passwords, tokens, connection strings or private keys.
Use:
- environment variables locally
- .env excluded from Git
- .env.example committed safely
- appropriate managed/workload identity for deployed automation
- least privilege
- secure cloud secret mechanisms when necessary

Agents must never print or paste live secrets into prompts/logs unnecessarily.

## 21. Testing
Implement continuously, not at the end.

Unit tests:
- validation rules
- generator
- transformations
- deduplication
- SCD logic

Batch tests:
- initial load
- incremental load
- rerun/idempotency
- empty source
- bad source
- partial failure
- reconciliation

Streaming/incident tests:
- duplicate
- late event
- malformed event
- out-of-order event
- congestion
- breakdown
- volume spike
- recovery

Website:
- lint
- unit/integration tests
- build
- accessibility checks
- offline/demo fallback

## 22. GitHub and CI/CD
GitHub is the source of truth.

Protected main branch. No agent pushes directly to main or self-approves.

Use feature branches/worktrees for concurrent developers:
- agent/data/*
- agent/web/*
- agent/test/*
- agent/docs/*

Candidate required checks:
- Python formatting/lint
- Python unit tests
- simulator/data-quality tests
- SQL validation
- website lint/tests/build
- accessibility
- secret scanning
- dependency/security scanning

## 23. Fabric Automation
Claude Code may orchestrate Fabric work only after authenticated tooling/API access is intentionally configured.

Preferred model:
Claude Code / GitHub Actions -> authenticated Fabric APIs/Git integration -> Fabric workspace.

Automation may create/update supported Fabric artifacts, synchronize source-controlled items, run validation and inspect outcomes only within explicitly granted permissions.

Do not assume Claude can control Fabric merely because it can edit the repo. Authentication and supported APIs/tools are required.

Use interactive authentication during controlled development where appropriate; move to a dedicated least-privilege automation identity/workload identity for repeatable CI/CD when supported and justified.

Never place access tokens directly in prompts or committed files.

## 24. Autonomous Agent Team
Primary roles:
1. Technical Lead / Orchestrator
2. Product Manager
3. Data Platform Engineer
4. Full-Stack Engineer
5. QA / Security Engineer

Use the main Claude Code session as Lead initially. Specialised agents/subagents handle role work. Use concurrent agents only for genuinely independent tasks.

### Lead
Owns architecture, allocation, dependencies, integration, conflicts, evidence and release readiness. Coordinates more than it codes.

### PM
Owns epics/stories, measurable acceptance criteria, sequence, dependencies and requirement traceability. No production code.

### Data Engineer
Owns simulator/, fabric/, sql/ and data tests.

### Web Engineer
Owns website/ and frontend tests.

### QA/Security
Independently validates implementation. Verdict: PASS, PASS WITH CONDITIONS, FAIL. Does not alter product code just to force a pass.

## 25. Agent Workflow
PM story -> Lead review -> isolated developer work -> developer tests -> draft PR -> deterministic CI -> QA -> defect loop -> QA retest -> Lead architecture/integration review -> human approval if required -> merge.

No agent statement substitutes for test/execution evidence.

## 26. Agent Handovers
Developer:
Story; Branch; Commit; Paths changed; Implementation summary; Criteria addressed; Commands executed; Tests passed/failed; Limitations; Security impact; Cost impact; Documentation; QA questions.

QA:
Story; PR; Environment; Criteria tested; Commands; Automated results; Manual checks; Defects; Regression risk; Security findings; Evidence; Verdict.

## 27. Human Approval Boundaries
Agents may autonomously read/edit owned files, create branches, run tests/lint/formatters, generate synthetic data, update docs, create draft PRs, review code and loop on defects.

Human approval is initially required for:
- merging to main
- paid cloud resources
- IAM/RBAC/authentication changes
- public endpoints/deployments
- GitHub secret changes
- destructive cloud/database operations
- material architecture decisions
- changes that create recurring cost

As governance proves reliable, low-risk merge approvals may be relaxed deliberately.

## 28. Evidence-Based Completion
Generated code is not working code. Completion requires actual evidence appropriate to the component.

If cloud infrastructure is unavailable, status must be labelled IMPLEMENTED - NOT CONNECTED or DEMO MODE. Never claim deployed/successful Fabric execution without evidence.

## 29. Development Timeline Target
A 6-7 day intensive sprint is an aggressive MVP target, not a guarantee of a fully production-hardened system.

With the repository specification already defined, parallel agents, fast owner approvals and no major Fabric/authentication blockers, target a strong portfolio MVP in 6-7 focused days:

Day 1: repo governance, architecture/ADRs, synthetic domain model, simulator skeleton, website foundation.
Day 2: deterministic generator, batch datasets, local Bronze/Silver/Gold logic, unit/data-quality tests.
Day 3: activate Fabric trial if local foundation is ready; build Lakehouse/pipelines/notebooks and validate batch/incremental flow.
Day 4: streaming/Eventstream/Eventhouse path, windowing, late/duplicate/out-of-order handling.
Day 5: incident simulations, quarantine, observability, Warehouse/dimensional model/SCD2.
Day 6: Power BI/analytics, website connected/demo experiences, architecture animations and control centre.
Day 7: end-to-end tests, failure/recovery tests, security/readme/docs, screenshots/recorded demo, deployment and portfolio polish.

Treat anything that cannot be evidenced by Day 7 as a truthful limitation/backlog item, not as completed work.

A genuinely production-hardened platform normally requires longer for security review, operational soak testing, performance/load tuning, cross-browser/accessibility validation, deployment hardening and refinement. Do not sacrifice correctness to meet 7 days.

## 30. Fabric Trial Strategy
Do not automatically activate the Fabric trial on Day 1.

Phase A - Local/free foundation first:
- Git/repo/agent governance
- simulator
- schemas
- incident logic
- local transformations/tests
- website/demo mode
- CI

Phase B - Start 60-day Fabric trial only when ready for Fabric-native work.

During the first 60 days after activation:
- use no paid Fabric capacity
- use no paid Azure infrastructure unless the owner explicitly overrides the rule
- complete Fabric pipelines, Lakehouse, Warehouse, streaming, Power BI, integration tests and evidence collection using the trial

Before expiry:
- export/commit all source-controlled assets possible
- capture architecture and operational evidence
- capture screenshots/recorded demo assets
- ensure website DEMO/RECORDED mode works independently
- document how to reactivate/deploy if paid or another eligible environment is available later

## 31. Out of Scope
- AWS implementation
- multi-cloud comparison implementation
- employer/organisational resources
- uncontrolled autonomous paid-resource creation

## 32. Definition of Done
A reviewer can understand the problem/architecture; observe batch and streaming concepts; inspect incremental loading, medallion, Warehouse modelling and SCD2; understand event time/windows/watermarks/late/duplicate handling; trigger incidents and recovery; see quality/quarantine/observability/reconciliation; explore analytics; inspect tests/CI/security/cost controls; review GitHub code; and use the public website without access to employer systems or permanent paid Fabric infrastructure.
