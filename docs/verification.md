# Verification

Last verified on 2026-09-24 with Java 26 running the Java 25 build (`--release 25`), Spring Boot 4.1.1, SQL Server 2022 and PostgreSQL 17.

## Automated tests

`mvn verify` passes with **42 tests, no failures and none skipped**.

| Suite | Tests | Covers |
|---|---|---|
| Unit | 13 | domain rules, input validation, row fingerprints, catch-up filtering, configuration limits, HTTP error mapping |
| `MigrationIT` | 16 | COPY fidelity (Unicode, quotes, `NULL`, decimals), capture of every DML type, crash rollback, restart recovery, concurrent writes, cutover fence, retention expiry, chunked seeding past 100,000 rows |
| `LiveChangesIT` | 13 | live INSERT, UPDATE and DELETE before and after a row is copied, paused replay with the snapshot still advancing, catch-up, restart persistence |

The integration tests use Testcontainers with real SQL Server and PostgreSQL containers.

## End-to-end runs

Both scripts drive the running Compose stack only through its public HTTP API.

**`scripts/verify_demo.py`** ([evidence](verification.json)) runs 10,000 customers and 10,000 orders with traffic running throughout, a simulated crash with rollback and resume, and a SIGKILL of the app container with recovery. It then runs full validation and cutover, writes a new row to the target, and confirms the source is unchanged after cutover. Result: `COMPLETE`, PostgreSQL primary, 40 snapshot batches, 226 net changes applied.

**`scripts/verify_live_changes.py`** ([evidence](live-verification.json)) checks, in order:
- At 50% of the snapshot, it pauses replay and updates Order #100 from `PENDING` to `COMPLETED` in SQL Server. SQL Server shows the new value, PostgreSQL the old one, and exactly one key is pending while the snapshot keeps copying.
- It resumes replay, and the inspector reaches `IN SYNC`.
- A new customer and order insert is captured and replayed.
- A delete is held through an app container kill while replay is paused, then replays after resume.
- Final validation and cutover pass.

## Scale run

Run with `BATCH_SIZE=10000` and 2,000,000 customers plus 2,000,000 orders on a laptop:

| Step | Time |
|---|---|
| Seed | 48 s |
| Snapshot to Ready | about 4.5 min |
| Validation (counts, SHA-256, constraints) | 15 s, passed |
| Cutover | completed; counts equal |
| Reset | 68 s |

These are observations from one machine, not performance guarantees.
