# ADR 004: Target-side atomic checkpoints

**Decision:** Persist table position, row count, and batch number in PostgreSQL in the same transaction as target rows.

A separate checkpoint commit could claim rows that rolled back. On failure this design resumes after the last fully visible batch. CDC also records applied IDs to make redelivery safe.
