# Architecture Decision Records

This directory holds ADRs — short write-ups of significant technical
decisions, including decisions to **not** do something. The primary
audience is future-us: when a similar question comes up again, the ADR
should carry enough context to decide whether to revisit or move on.

Format: numbered files (`NNNN-<slug>.md`). Keep each one self-contained;
prefer being precise over being exhaustive. If the situation changes
(upstream library adds a feature, workload profile shifts, ...),
supersede the ADR with a new one that references the previous.

## Index

- [0001 — vtprotobuf unmarshal and unsafe](0001-vtproto-unmarshal-unsafe.md)
- [0002 — FSM throughput ceiling](0002-fsm-throughput-ceiling.md)
- [0003 — JSON toolchain](0003-json-toolchain-v2.md)
- [0004 — Logs and projections integrity](0004-logs-and-projections-integrity.md)
