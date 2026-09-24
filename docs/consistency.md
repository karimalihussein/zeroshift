# Following one migration

## 1. Capture before copying

`SqlServerReader.boundary()` opens a SQL Server SNAPSHOT transaction. It reads the Change Tracking version, each table's maximum identity, and the initial row count. PostgreSQL persists these before any batch is copied. Capture is enabled when the source schema is created, so concurrent writes cannot fall between capture setup and snapshot start.

## 2. Bounded, resumable initial copy

`SnapshotBatch` asks for `TOP (batchSize) ... WHERE id > lastId AND id <= initialMax ORDER BY id`. Gaps from deletes are harmless. The upper bound prevents an ever-growing source from making the snapshot endless.

The source read is a normal committed read. It is intentionally not a globally consistent snapshot across all initial batches: a row can be updated or deleted between batches. All changes since the initial version will be replayed afterward. There is no target business traffic until cutover.

`PostgresBulkLoader` writes CSV through the PostgreSQL JDBC COPY protocol into a temporary table. It merges by primary key. CSV quotes, embedded newlines, empty strings, nulls, Unicode and exact decimal values retain their meanings. The rows and `last_id` checkpoint commit in the **same PostgreSQL transaction**. Re-reading a batch is safe; a rolled-back checkpoint cannot describe committed rows.

## 3. A consistent change window

`ChangeTrackingCapture` opens one SQL Server SNAPSHOT transaction and first reads the gate table to establish its data snapshot. Metadata functions alone do not establish that snapshot. It then uses the same transaction for:

- the retention safety check;
- the high version watermark;
- all pages of changed keys and their current row values in both tables.

The capture reader joins `CHANGETABLE(CHANGES ...)` with the base table. Deletes have no row payload. Reads page by changed primary key within the fixed snapshot, so a moving key cannot slip across pages. Changes committed after this snapshot are read in the next window.

Both tables' changes and the high version are committed as one target transaction. Inserts/updates use COPY and idempotent merge; deletes use batched statements. Deferred foreign keys permit a parent delete and its child deletes within the same transaction. Java keeps only a bounded page in memory. The target transaction itself can be large if the source backlog is large; this is a deliberate educational tradeoff.

Change Tracking coalesces multiple modifications of a key. It records commit-based versions, unlike an identity-valued trigger log whose identity allocation order can differ from transaction commit order. If retention has removed needed changes, migration fails instead of advancing a corrupt checkpoint.

## 4. Prepare the target

After the first catch-up, create the secondary index and deferred FK/check constraint, sync identities and run ANALYZE. The primary key exists before COPY to enable safe upserts. The other constraints are introduced with NOT VALID, then explicitly validated when source and target are fenced and equal. Catch-up continues in Ready.

## 5. Pause, crash and backend restart

Pause changes only `status`; `stage` and all cursors remain durable. Simulate Crash persists an armed flag. The next copy/capture transaction throws after writing rows, before checkpointing. PostgreSQL rolls it all back, and a separate transaction marks CRASHED.

On a process restart, RUNNING becomes PAUSED and requires Resume. No recovery cursor lives only in a Java field. If a process dies during a source read, COPY or checkpoint update, PostgreSQL releases the connection and rolls back the target transaction. If it dies after commit, the committed cursor is the restart point.

The two runtime workers have only a readiness flag; it gates scheduling until recovery completes and is not migration progress. All operational state, routing, traffic enablement and logs are persisted in PostgreSQL.

## 6. Fence and cut over

Cutover first commits stage FREEZE. Simulator ticks observe that state and do not write. The next migration transaction locks the PostgreSQL routing row and sets the SQL Server gate to frozen.

SQL Server AFTER triggers read the gate using HOLDLOCK. Their shared locks last until each source transaction commits. Updating the gate therefore waits for existing guarded writes to finish. New DML sees the frozen gate and rolls back. This establishes a real source fence, not just a disabled UI button.

Behind that fence, drain the remaining change window, compare counts and canonical ordered SHA-256 fingerprints for every column, validate constraints, and sync sequences to the final maxima. Only then commit PostgreSQL as primary. The gate stays frozen permanently until Reset.

A failure after the source fence commits cannot silently reopen source writes. FREEZE remains durable, and Resume retries the final catch-up and validation. If validation fails, the primary remains SQL Server and writes remain fenced until repaired or reset. If the route commit succeeded but the process died immediately afterward, restart reads PostgreSQL as primary.

For the standalone Validate action, the routing row lock blocks simulator writes and the same source fence provides stable hashes. Its `finally` unfreezes the source; startup also clears a leftover standalone-validation fence when stage is not FREEZE or COMPLETE.

## What the UI means

The dashboard polls typed backend state. It has no timer-generated progress. The percentage is stage-weighted, copied rows count actual committed snapshot rows, batches are committed batches, and checkpoint time comes from PostgreSQL. Net change counts may be smaller than the number of source operations. Independent live counts are informational; only fenced validation proves equality.
