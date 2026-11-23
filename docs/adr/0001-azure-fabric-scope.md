# 0001 - Azure/Microsoft Fabric scope, AWS excluded

Status: Accepted

## Context
TransitPulse is a portfolio project whose primary career objective is Microsoft
Azure / Microsoft Fabric data engineering. Supporting two cloud ecosystems would
roughly double infrastructure, tooling and documentation surface area without
adding proportional portfolio value for that objective.

## Decision
Build exclusively on Microsoft Fabric (OneLake, Data Factory, Lakehouse, Delta
Lake, Eventstream, Eventhouse, Warehouse, Power BI) and standard Azure identity
primitives where unavoidable. AWS implementation and multi-cloud comparison
implementation are explicitly out of scope, per `PROJECT.md` sections 2 and 31.

## Consequences
- All cloud-native code, IaC and documentation target Fabric/Azure only.
- No AWS SDKs, services or credentials are introduced at any point.
- If the owner changes this scope later, a new ADR supersedes this one before
  any AWS work begins.
