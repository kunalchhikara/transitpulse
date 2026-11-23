# 0002 - Medallion architecture with a Lakehouse/Warehouse split

Status: Accepted

## Context
TransitPulse ingests both streaming vehicle telemetry and batch reference/
transactional data, and must demonstrate quality enforcement, reconciliation,
deduplication and BI-ready dimensional modelling. `PROJECT.md` sections 8-10
require medallion layers with a stated reason for each copy/transformation,
and a documented reason for each dataset's placement in Lakehouse vs Warehouse.

## Decision
Adopt Bronze/Silver/Gold inside the Fabric Lakehouse:
- **Bronze**: raw/minimally-transformed source representation plus ingestion
  metadata (source, ingestion time, run ID), kept for replay and traceability.
- **Silver**: schema/type enforcement, GPS/timestamp validation, deduplication,
  late-data handling, quarantine routing and reference enrichment.
- **Gold**: business/analytics models — route, vehicle, trip, passenger demand,
  reliability, streaming-ops and data-quality metrics.

Route dimensional modelling (`dim_*`/`fact_*`), SCD Type 2 history and BI-facing
SQL access live in the Fabric **Warehouse**, fed from Gold, because Power BI
semantic models and ad-hoc SQL analytics are better served by a curated
dimensional store than by querying Delta tables directly.

Detailed event history and Spark-native engineering stay in the **Lakehouse**
because they need Delta/PySpark workloads, not warehouse SQL.

Every layer/copy must have a stated reason (quality enforcement, replay,
BI-performance, history) — layers are never added merely to claim medallion
architecture.

## Consequences
- Each new dataset's design doc/story must state which layer(s) it lands in
  and why, and whether it belongs in Lakehouse, Warehouse, or both.
- Reconciliation must satisfy: source extracted = valid loaded + quarantined +
  intentionally excluded (`PROJECT.md` section 15).
