# ADR 002: Version and key bounds before copying

**Status:** Accepted

## Decision

`SqlServerReader.boundary()` opens one SQL Server SNAPSHOT transaction and reads the current Change Tracking version, each table's maximum ID, and the row counts. PostgreSQL stores them before the first batch is copied. The snapshot copies only `id <= bound`. Catch-up replays every change after the stored version.

## Why

- Capture is enabled together with the schema, so no commit can fall between "capture starts" and "copy starts".
- The upper key bound stops a busy source from making the snapshot endless. Rows inserted later arrive through change capture.
- A row copied in its old state and then changed is replayed later. A row copied in its new state and then replayed again is merged idempotently. Either way, the target converges on the latest committed row.

## Consequences

- The initial copy is a *fuzzy* keyset snapshot: individual batches are ordinary committed reads, not one multi-hour transaction. Correctness comes from replaying everything after the boundary version.
- While the snapshot is running, `ChangeCatchUp.drainCopied` replays only keys already behind the copy frontier. It does not advance the baseline version, so changes to rows that aren't copied yet are still replayed after those rows arrive.
- Enabling capture after the copy starts was rejected because it creates a window in which changes are lost.
