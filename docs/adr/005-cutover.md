# ADR 005: Gated short-freeze cutover

**Decision:** Require complete copy, drained catch-up, validation, sequence sync, and readiness state. Then freeze application writes, drain again, validate, resync, switch the router, and reopen writes.

Automatic switching after copy and ungated manual switching were rejected: neither proves convergence at the switching instant.
