# ADR 0005: Hybrid Storage Strategy (SQLite for Local Dev, ClickHouse for Scale)

## Status
Accepted

## Context
Developers require an instant, zero-docker local experience (`pip install swarmwatch && swarmwatch ui`), while production workloads require ingesting millions of agent spans without high query latency.

## Decision
1. Default to embedded SQLite + WAL mode for single-developer local installations.
2. Provide a pluggable ClickHouse driver for production / team multi-tenant environments.
