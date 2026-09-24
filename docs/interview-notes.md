# Interview notes

“I would use a near-zero-downtime migration. I would establish change capture, take a consistent initial snapshot, migrate historical data in controlled batches using keyset pagination and bulk loading, let the target catch up with concurrent changes, validate it, briefly freeze writes, drain the final changes, and cut over to PostgreSQL.”

## Explain the pieces

- **CDC:** capture every committed insert, update, and delete from an ordered boundary. In this lab a trigger-written transactional log replaces native CDC for portability.
- **Batching:** bound memory, transaction duration, and load. Tune throughput against source latency rather than maximizing it blindly.
- **Keyset pagination:** `id > last_id` uses an index and has stable cost; `OFFSET 700000` repeatedly scans/skips earlier rows and changes under concurrent writes.
- **Bulk loading:** PostgreSQL `COPY` amortizes network and parsing overhead for snapshot rows. CDC needs upserts/deletes, so it uses ordered SQL operations instead.
- **Checkpointing:** commit data and its position atomically. A retry may repeat work but must never skip it.
- **Catch-up:** replay everything after the boundary until lag is low enough for a short write freeze.
- **Sequence sync:** advance every identity generator beyond the largest migrated key before target writes begin.
- **ANALYZE:** bulk loads change table distributions; refreshed statistics let PostgreSQL’s planner choose sane plans.
- **Validation:** combine counts, exact aggregates, normalized chunk hashes, referential checks, samples, and business queries. Counts alone miss changed values.
- **Cutover:** pass readiness gates, freeze writes, drain to zero, validate again, switch the application, observe closely.
- **Rollback:** once the target accepts unique writes, naive failback loses data. Use reverse replication or reconcile those writes explicitly.

## Common follow-ups

**How do you prevent a snapshot/CDC gap?** Establish capture first and record an ordered committed boundary. The initial copy plus replay of all events after that boundary covers every commit. Verify the exact database snapshot semantics in the production design.

**Why not timestamp ordering?** Resolution collisions, clock assumptions, and commit/order differences. Use an LSN, change version, or transactional sequence.

**What happens if an update races with copying that row?** The copy may contain the old or new image. Ordered idempotent replay after the boundary converges it to the newest committed image.

**How do deletes work?** Capture the primary key in the source transaction and replay a target delete. A missing target row is already the desired result.

**How do you control risk?** Rehearse at production scale, throttle and monitor source latency, keep resumable checkpoints, define abort criteria, require validation gates, freeze briefly, and keep an explicit rollback/reconciliation plan.

**Would you use this trigger log in production?** Only after load and failure analysis. Native CDC, transaction-log products, or managed replication are often better operational choices; the invariant matters more than the product name.
