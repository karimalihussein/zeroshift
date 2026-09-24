# Explaining the migration

A short script for explaining this approach aloud, plus the questions that usually follow.

> "I would use a near-zero-downtime migration. Establish change capture, record a committed boundary, copy historical data in bounded keyset batches with bulk loading, let the target catch up with concurrent changes, validate, briefly fence writes, drain the final changes and cut over to PostgreSQL."

## The pieces

- **Change capture:** every committed insert, update and delete after an ordered boundary. This lab uses SQL Server Change Tracking ([ADR 001](decisions/001-change-capture.md)).
- **Batching:** bounds memory, transaction length and source load. Tune throughput against source latency instead of maximizing it blindly.
- **Keyset pagination:** `id > last_id` uses the index at a stable cost. `OFFSET 700000` repeatedly scans and skips earlier rows, and shifts under concurrent writes.
- **Bulk loading:** PostgreSQL `COPY` spreads network and parsing overhead across many rows. Merging from a staging table keeps retries idempotent.
- **Checkpointing:** commit data and its position together. A retry may repeat work but must never skip it.
- **Catch-up:** replay everything after the boundary until the backlog is small enough for a short write freeze.
- **Sequence sync:** move every identity sequence past the largest migrated key before the target accepts writes.
- **ANALYZE:** bulk loads change table statistics. Refreshing them lets the PostgreSQL planner choose sensible plans.
- **Validation:** counts alone miss changed values. This lab compares ordered SHA-256 fingerprints of every column behind a write fence and validates constraints.
- **Cutover:** pass the readiness gates, fence writes, drain to zero, validate again, switch routing, then watch closely.
- **Rollback:** once the target accepts its own writes, a naive switch back loses data. Use reverse replication or reconcile those writes explicitly ([ADR 006](decisions/006-rollback.md)).

## Common follow-ups

**How do you prevent a gap between the snapshot and CDC?** Enable capture first and record a committed boundary. The copy plus a replay of every change after that boundary covers every commit.

**Why not order changes by timestamp?** Resolution collisions, clock assumptions, and the difference between write order and commit order. Use an LSN, a change version or a commit-ordered sequence.

**Why not a trigger-written change log with an identity column?** Identity values are allocated when a row is written, not when its transaction commits. A long transaction can commit a lower ID after a watermark has already passed it. Change Tracking versions are commit-ordered.

**What if an update races with copying that row?** The copy may hold the old or the new image. Replaying the change idempotently after the boundary converges it to the latest committed image. The **Live Data Changes** panel shows this happening ([live-changes.md](live-changes.md)).

**How do deletes work?** The capture feed keeps the deleted primary key. Replay deletes it on the target, and a row that is already missing is the desired result.

**How do you control risk?** Rehearse at production scale, throttle and monitor source latency, keep resumable checkpoints, define abort criteria, gate cutover on validation, keep the freeze short, and have a rollback or reconciliation plan written down.
