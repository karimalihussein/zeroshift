# ADR 001: SQL Server Change Tracking for change capture

**Status:** Accepted

## Decision

Capture source changes with native SQL Server **Change Tracking**, enabled on `dbo.customers` and `dbo.orders` when the source schema is created. `ChangeTrackingCapture` reads `CHANGETABLE(CHANGES …)` joined to the base table inside one SQL Server SNAPSHOT transaction.

## Why

- Change Tracking versions are assigned at **commit**. The earlier TypeScript prototype of this lab used a trigger-written change log with an identity column. Identity values are allocated when a row is inserted, not when its transaction commits, so a long transaction can commit an *earlier* ID after a later one has already been read. That can drop a change past a watermark. Commit-ordered versions avoid that.
- It needs no agent or job, so it works the same in a local Developer container and in tests.
- It captures inserts, updates and deletes, including deletes that have no row left to read.

## Consequences

- Change Tracking is a **net-change** feed: several modifications of one key between reads merge into one change with the current row values. Intermediate before/after images are not retained. The dashboard labels counts as "net changed keys" for that reason.
- Retention is seven days. `ChangeTrackingCapture` checks `CHANGE_TRACKING_MIN_VALID_VERSION` inside the same snapshot as the reads. If needed history has been cleaned up, the migration fails closed and needs a new snapshot.
- Timestamp polling was rejected: clock resolution, clock skew and commit order make it unable to guarantee no gaps.
- A production system should also consider log-based CDC or managed replication. The invariant (ordered, committed, complete from a boundary) matters more than the product.
