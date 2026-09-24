# ADR 003: Keyset batches loaded with PostgreSQL COPY

**Status:** Accepted

## Decision

Copy one table at a time in dependency order (customers, then orders). Each batch reads `TOP (batchSize) … WHERE id > lastId AND id <= bound ORDER BY id`. `PostgresBulkLoader` streams the rows through the JDBC `COPY` protocol into a temporary table and merges them by primary key.

## Why

- `OFFSET` pagination gets slower as the offset grows and shifts when rows are inserted or deleted concurrently. A keyset range uses the primary-key index at a stable cost, and gaps left by deletes are harmless.
- `COPY` avoids a network round trip and statement parse per row. Merging from a staging table keeps retries idempotent.
- A single worker with a bounded `BATCH_SIZE` keeps source load and memory predictable and easy to tune.

## Consequences

- One batch is copied per scheduler tick (`TICK_MS`, default 250 ms). Multi-million-row demos should raise `BATCH_SIZE` (maximum 10,000).
- Change replay uses the same COPY-and-merge path for inserts and updates, and batched statements for deletes.
- CSV encoding preserves quotes, embedded newlines, empty strings, `NULL`, Unicode and exact decimals. The integration tests cover each of these.
