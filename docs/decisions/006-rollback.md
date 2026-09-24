# ADR 006: No automatic failback after cutover

**Status:** Superseded by [ADR 007](007-reverse-sync-rollback.md)

## Decision

Before cutover, aborting is simple: SQL Server stays primary and Reset clears the target. After cutover, PostgreSQL accepts new writes and SQL Server **stays fenced**. The lab does not pretend that pointing the application back at SQL Server is a recovery.

## Why

As soon as the target accepts writes that the source does not have, a naive switch back loses them. A real failback plan needs reverse replication from PostgreSQL to SQL Server, or an explicit reconciliation of target-only writes. Either would roughly double the size of this lab and hide its main lesson.

## Consequences

- The dashboard does not offer a rollback button after cutover. Reset is lab administration, not a production rollback.
- A production plan should choose reverse capture or a reconciliation procedure *before* cutover, and rehearse it.
