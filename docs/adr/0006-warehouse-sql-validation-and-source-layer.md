# 0006 - Local validation of Warehouse SQL, and which layer feeds the Warehouse

Status: Accepted (2026-11-24, Lead; owner approved the local Phase 1b)

## Context
Phase 1b (`docs/BACKLOG_PHASE_1B.md`) builds the Fabric Warehouse dimensional
model and an SCD Type 2 `dim_vehicle` (PROJECT.md sections 9-10) before the
60-day Fabric trial is activated (ADR-0004). Fabric Warehouse speaks T-SQL,
and no Fabric Warehouse exists locally. Two questions must be settled before
story DE-13 starts:

1. How is the Warehouse SQL validated locally and in CI?
2. Which layer feeds the Warehouse? ADR-0002 says "fed from Gold", but the
   batch datasets now get a Silver current-state layer (DE-12), and an extra
   Lakehouse Gold copy of the same rows would have no reason to exist
   (PROJECT.md section 8: every copy must have a reason).

## Options considered (question 1)
- **A. Python reference implementation + static T-SQL.** Dimensional/SCD2
  logic implemented and fully tested in Python; T-SQL DDL/load scripts kept in
  `sql/` and checked statically. No SQL engine needed; the logic is proven by
  executable tests. Cost: two expressions of the same logic, and the T-SQL
  does not execute until the trial.
- **B. DuckDB (optionally with sqlglot transpiling T-SQL).** SQL executes,
  but adds a native dependency, and dialect gaps (`MERGE`, identity columns,
  `datetime2`, `NOT ENFORCED` constraints) mean the SQL that was tested is not
  the SQL that would be deployed. Rejected: false confidence.
- **C. SQLite (stdlib).** No dependency, but the weakest T-SQL fidelity; the
  SQL would have to be dialect-neutral rather than Fabric T-SQL. Rejected.
- **D. T-SQL static check with sqlfluff (`tsql` dialect).** Catches syntax
  errors; checks no semantics. Dev/CI-only dependency.
- **E. SQL Server container in CI.** Highest T-SQL fidelity, free licence,
  no Azure resource. Rejected for now: Docker in CI and slower runs, and
  Fabric Warehouse supports only a subset of SQL Server T-SQL, so passing on
  SQL Server would not prove the SQL runs on Fabric. Can be revisited.

## Decision (question 1): A + D
- **The Python reference implementation is the executable source of truth**
  for dimensional and SCD2 behaviour. It lives in `simulator/warehouse/`
  (Data Platform Engineer), with tests in `simulator/tests/test_warehouse_*.py`,
  so the existing CI `python` job runs them.
- **The T-SQL in `sql/`** (`sql/ddl/`, `sql/load/`) is the deployable artefact
  for Fabric Warehouse. It is syntax-checked in CI by a new `sql` job running
  `sqlfluff parse --dialect tsql sql` with **`sqlfluff==4.4.0` pinned**
  (released 2026-10-02). sqlfluff is a CI/dev-only dependency; it is not added
  to `simulator/requirements.txt`.
- Every T-SQL script is labelled **IMPLEMENTED - NOT CONNECTED, NOT
  EXECUTED** until it has run on a Fabric Warehouse with evidence.
- DE-13/DE-14 must make the Python implementation and the T-SQL load logic
  describe the same rules (grain, keys, SCD2 policy, unknown member), and list
  every behaviour that can only be confirmed on Fabric under "verify in
  Fabric" in `sql/README.md`; that list feeds the Fabric plan (ADR-0005).

### Evidence gathered for this decision (Lead, local, sqlfluff 4.4.0)
- A plain `CREATE TABLE` in T-SQL: `sqlfluff parse --dialect tsql` exits 0.
- A syntactically broken statement: exits 1 (`PRS` unparsable section).
- **Known gap:** Fabric Warehouse primary-key/unique constraints use
  `... PRIMARY KEY NONCLUSTERED (...) NOT ENFORCED`, and sqlfluff 4.4.0's tsql
  dialect reports `NOT ENFORCED` as unparsable. Statements that use
  Fabric-only syntax the parser does not support must be kept in clearly named
  files listed in `sql/.sqlfluffignore`, each with a comment explaining why,
  and added to the "verify in Fabric" list. Nothing else may be ignored.

## Decision (question 2): Silver batch current-state tables feed the Warehouse
- Warehouse dimensions and batch facts (`fact_trip`, `fact_service_incident`)
  are loaded from the **Silver batch current-state tables** produced by
  DE-12. The Warehouse dimensional model *is* the curated, business-facing
  (Gold) representation of the batch data; no separate Lakehouse Gold copy of
  those rows is made.
- `fact_route_performance` is loaded from the committed **Gold**
  `route_performance` output (DE-07), because that aggregate already lives in
  Gold.
- ADR-0002 is amended accordingly (see its "Amendment" section).

## Consequences
- The SCD2 and point-in-time logic is proven locally by executable tests;
  the T-SQL is proven only syntactically until the trial. The Fabric plan must
  include executing every script on the Warehouse and comparing results with
  the Python implementation on the same fixture.
- CI gains a `sql` job, gated on `sql/**/*.sql` existing.
- If the two implementations ever disagree, the Python tests define intended
  behaviour and the T-SQL is fixed, until Fabric execution evidence exists.
