# ADR 003: Keyset batches and COPY

**Decision:** Read one dependency-ordered table at a time with `id > last_id`, use bounded configurable batches, and load through PostgreSQL `COPY`.

OFFSET pagination degrades as the offset grows and behaves poorly under mutation. Per-row inserts waste network round trips. Controlled single-worker batches make source impact visible and tunable.
