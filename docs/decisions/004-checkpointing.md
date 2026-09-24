# ADR 004: Checkpoints commit with the rows they describe

**Status:** Accepted

## Decision

Store all migration state in PostgreSQL's `migration_state` table: stage, status, current table, `last_id`, capture version, counters, primary database and CDC-pause flag. Each snapshot batch commits its rows and its new `last_id` in **one PostgreSQL transaction**. Each change replay commits its row changes, its per-key `replay_receipt` versions and the new capture version together.

## Why

- A checkpoint committed separately from its rows could describe rows that were rolled back, and skip them for good. With a shared transaction, a retry may repeat work but can never skip it.
- Replay receipts record which Change Tracking version of each key has been applied. They make redelivery safe, and they let the dashboard show a specific change as *pending* or *replayed*.
- No recovery cursor lives only in a Java field. After a backend restart, a `RUNNING` migration becomes `PAUSED` and resumes from the last commit.

## Consequences

- **Simulate Crash** arms a flag that throws after target writes but before the checkpoint. The whole batch rolls back and is copied again on resume.
- A large capture backlog is applied as one large target transaction. Java memory stays bounded by paging, but the transaction does not. This is an accepted trade-off for a teaching lab.
