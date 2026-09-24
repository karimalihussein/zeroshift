# ADR 007: Rollback after cutover through reverse sync

**Status:** Accepted. Supersedes [ADR 006](006-rollback.md).

## Decision

After cutover, PostgreSQL accepts writes that SQL Server does not have, so switching back is only a rollback if those writes come too. ZeroShift replicates them in reverse before it switches:

1. **Capture from the moment of cutover.** The cutover transaction attaches a row trigger to `customers` and `orders` that appends `(table, key, operation, seq)` to `reverse_change`. Routing reads the primary under the same row lock, so no PostgreSQL write can precede its capture. Capture is part of the write's own transaction: an acknowledged write is always captured.
2. **Replay net images.** Each pass reads one `REPEATABLE READ` snapshot. Every captured key maps to its current row, or to a delete when the row is gone. SQL Server receives customer upserts, then order upserts and deletes, then customer deletes, which satisfies its immediate foreign key. Upserts keep PostgreSQL's keys (`IDENTITY_INSERT`).
3. **Commit, then acknowledge.** A page commits in SQL Server first. Then `reverse_change` rows up to each key's seq are deleted in PostgreSQL. A crash in between replays the same images, which is idempotent. Per key, seq order is commit order, because a later write to the same key waits on the earlier one's row lock.
4. **Refuse conflicts.** SQL Server is fenced after cutover. Reverse sync is the only writer allowed past the fence, and it tags every write with a Change Tracking context. Before each page, it locks the page's keys (`UPDLOCK, HOLDLOCK`) and checks Change Tracking since the cutover version. A key changed by anything else is a conflict: nothing is written, the conflict is recorded, and the rollback stops. The same check runs over the whole table before replay starts and again inside the freeze.
5. **Validate while PostgreSQL is live.** Counts and SHA-256 fingerprints of every row except keys still pending replay, read from one snapshot. A mismatch stops the rollback before any write is frozen.
6. **Freeze, drain, prove.** PostgreSQL's `write_gate` is set. A statement trigger reads it `FOR SHARE`, so setting the gate waits for in-flight writes. The final pass drains, `reverse_change` must be empty, and a full validation of both databases must match.
7. **Switch.** SQL Server identities are reseeded past both its own maximum and the highest key PostgreSQL's sequences ever issued, so a key handed out and deleted after cutover is never reused. SQL Server's fence opens. Only then does the transaction that makes SQL Server primary commit. PostgreSQL stays fenced as the secondary, and capture is removed.

Every stage is persisted before its work runs. Pause, simulated crash and process restart resume from the stage reached. Until the switch, **Abort rollback** lifts PostgreSQL's fence and leaves it primary, with capture still running.

## Why

- Trigger capture needs no `wal_level=logical`, replication slots or extensions, and it shares the write's transaction: no captured change can belong to a write that rolled back, and no committed write can escape.
- Replaying current images per key instead of every event matches the forward direction's net-change model (SQL Server Change Tracking), and makes replay naturally idempotent.
- The failure mode that matters is silently overwriting data. Conflicts and validation failures therefore stop the rollback and leave PostgreSQL as the primary.

## Consequences

- Row triggers add a small cost to every PostgreSQL write while it is primary.
- A conflict cannot be resolved inside the lab. The safe path is to abort. Reconciling SQL Server is an operator decision, not something the tool guesses.
- After rollback, PostgreSQL is a fenced, validated copy as of the switch. Forward replication does not resume; migrating again starts from Reset.
- The fences guard ordinary DML only. Disabling triggers or privileged DDL can bypass them, as with ADR 005. A write that escapes capture is still caught by validation, which is tested.
