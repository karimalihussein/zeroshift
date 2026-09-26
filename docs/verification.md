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

## Phase 3: resilience lab

Run on 2026-09-26 against the full Compose stack plus the chaos overlay, on Docker Desktop with 8
CPUs and a 9.7 GB VM shared with the rest of the lab.

**`scripts/verify_resilience_lab.py`**: all 9 drills passed in one run (about 8 minutes). Each
experiment ran automatically; every claim was decided from measured samples and then the lab's
reset was checked directly against the services and Toxiproxy. Observed values:

| Drill | Injected | Observed | Mitigation | After |
|---|---|---|---|---|
| load | 5 clients/s for 20 s | 101 arrivals; order-service admitted 114 for 100 successes in 114 attempts | — | stop halts arrivals, reset zeroes counters |
| chaos | order-db +300 ms, then down | a list read 13 ms → 1526 ms; proxy disabled | — | healed, reads fast |
| slow-consumer | 700 ms per payment command at 6/s | lag 47 and growing, API 100 % accepted | rate limit 1/s per replica: lag 49 → 36 | lag 0, 105 % of arrivals completed |
| poll-interval | max.poll.records=100, interval 6 s, 20 s backlog | 3 rebalances, lag 62, 3 duplicates skipped | max.poll.records=5: none for 20 s | lag 0, no rebalance for 15 s |
| gateway-breaker | gateway answers in 3 s | lag 42, 0.9 gateway calls/s, no dead letters | 800 ms timeout, breaker, pause on open: breaker OPEN, consumer paused | breaker CLOSED, lag 5 |
| bulkhead | order-db +100 ms | read p99 1245 ms, up to 8 threads waiting for a connection | bulkhead 4: 5 refused/s, read p99 608 ms | read p99 176 ms, write p99 162 ms |
| retry-storm | order-db down, immediate retries ×8 | 5.0 attempts per client, 64 requests in flight | backoff + jitter + budget: 1.07 per client | 117 % succeed (backlog), 1.2 per client, 0 in flight |
| kafka-partition | payment ↔ Kafka partitioned | lag 102, 123 unfinished (baseline 24), API 100 % accepted | shed above 80: 6.3 shed/s, growth stopped | lag 0, shedding off by itself, 116 % completed |
| cdc-degraded | Debezium link at 4 KB/s | relay p95 2.7 → 4.5 s in 5 s, API 100 % accepted | shed above 40: growth stopped at 70 | relay p95 380 ms, 0 dead letters, 26 unfinished |

The event-lab drills (`scripts/verify_event_lab.py`, 18/18) also passed against the Phase 3 service
builds, so the new endpoints and levers left Phase 1 behaviour unchanged.

**Tests.** `mvn spotless:check test` passes for every module (including the new `LoadGeneratorTest`,
`ExperimentTest`, `EdgeGuardsTest`, `GatewayPolicyTest`, `LabPressureTest`). Integration tests run
for the changed code: `ToxiproxyIT` 5/5, `OrderServiceIT` 13/13 (with the new edge-guard test),
`PaymentServiceIT` 5/5. `InventoryServiceIT` could not start in this session: its Kafka Connect
test container was OOM-killed at VM level twice while the shared stack was running, before any
test ran. The new restock endpoint it would cover was exercised live by every load drill.

**Resources during the run** (docker stats every 10 s, 41 samples): containers used 7.2 GB on
average, 7.5 GB at peak. Toxiproxy: 25 MB, 8 % CPU on average. The control plane with the load
generator and sampler: 400–430 MB (a second instance was used for the run; in the normal stack this
runs inside `app`), 14 % CPU on average, about 3 % when idle (the sampler stops a minute after the
page is closed). The busiest containers under the experiments' 3–20 clients/s were Kafka (34 %
average, 188 % peak), payment-service (140 % peak while working off a backlog) and the order-service
replicas (up to 98 %). Earlier in the session, with the 3-node kafka-lab cluster also running
(about 1.5 GB), the VM ran out of memory once: SQL Server was OOM-killed and Tempo later hit its
own 900 MB limit. Running Phase 3's experiments and the kafka-lab profile at the same time needs
more than 9.7 GB.

