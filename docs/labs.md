# Experiments: learning labs

The **Experiments** section of `/events` holds labs that each break one guarantee on the running
system, show why, then fix and recover it. Each follows the same path:
**Learn → Trigger → Observe → Break → Understand → Fix → Recover.**

Nothing is simulated in the browser. The control plane acts as a real HTTP client or calls the
services' lab APIs, and every result is read back from the services, Kafka or the databases.
Experiment runs are kept in the control plane's `lab_run` table; carrier scans are kept per scan in
shipping-service's `tracking_scan`.

Automated coverage: `python3 scripts/verify_event_lab.py lab_idempotency lab_ordering lab_repartition lab_read_your_writes`,
plus the service integration tests named below.

## Client retries and idempotency keys

| | |
|---|---|
| **Failure** | order-service commits the order, then holds its answer 2.5 s (the `slow-response` fault). The control plane, as a client with a 1 s timeout, retries. Without a key the retry is a new request: **one logical order becomes two orders and two card charges**. With every answer slow, the client even gives up believing the order failed while three exist. |
| **Why** | A timeout cannot tell a lost request from a lost answer. payment-service is idempotent *per order*, but each retry created a different order. |
| **Fix** | `Idempotency-Key` header. `PlaceOrder` claims the key in `idempotency_key` in the same transaction as `OrderPlaced`; a retry finds it and gets the first order back (`Idempotent-Replayed: true`). A concurrent retry waits on the key's primary key, then finds it. The same key with a different body is refused with 422. |
| **Recover** | *Refund duplicate* sends a real `RefundPayment` through the outbox; payment-service refunds once. A shipped order cannot be undone (the saga's pivot): prevention beats cure. |
| **Evidence** | The client's attempts (timeouts, 202, replayed) next to what exists for that request: orders, saga states, charges, refunds. |
| **Tests** | `OrderServiceIT.aRetryWithTheSameIdempotencyKeyGetsTheFirstAnswerInsteadOfASecondOrder` (replay, conflict, 4 concurrent retries → 1 order); drill `lab_idempotency`. |

## Ordering and partitioning

A carrier publishes scan events (`shipping.carrier-scans`: picked up → in transit → out for delivery
→ delivered) for real shipped parcels. shipping-service projects them into `tracking`. Its consumer
honours a `carrier-slow` fault (`"0,1:700"`: partitions 0 and 1, 700 ms per scan), standing in for a
lagging shard.

| | |
|---|---|
| **Failure 1: wrong key** | Scans keyed by their own id spread over partitions. With one slow partition, later scans overtake earlier ones, and the last-write-wins projection moves parcels **backwards** (a delivered parcel shows "in transit"). |
| **Failure 2: repartition in flight** | Keys are correct, but partitions are added (3 → 6) while scans are unconsumed. Parcels whose key now hashes to a new partition get scans 3–4 before 1–2. |
| **Failure 3: hot key** | Every scan keyed by its hub: order is safe, but one partition, and so one consumer, does all the work. |
| **Why** | Kafka orders records only within a partition, and a key maps to a stable partition only while the partition count stays the same. |
| **Fix** | Key by tracking number, and a **sequence guard**: the projection refuses a scan older than the one it applied (`STALE_SKIPPED`, logged as an `IGNORED` decision), so it is correct in any arrival order. |
| **Recover** | *Replay tracking* rewinds the consumer to offset 0 into an empty projection; with the guard on, it comes out right. *Recreate topic* returns to 3 partitions: partitions can never be removed, and it also resets the producer, whose cached metadata still points at the deleted partitions. |
| **Evidence** | Per-scan log (partition@offset, key, outcome: applied / regressed / refused), parcel statuses with regression counts, scans per partition, consumer lag. |
| **Tests** | `ShippingServiceIT.anOutOfOrderScanRegressesTheNaiveProjectionAndTheGuardRefusesIt`, `…theCarrierLabKeysScansAsChosen`; drills `lab_ordering`, `lab_repartition`. |

Found while building it: a partition nobody has committed on (a new group, a recreated topic, an
added partition) used to report no lag. The control plane now counts its lag from the earliest
offset, since every lab consumer starts there.

## Read your writes (CQRS)

| | |
|---|---|
| **Failure** | Place an order and read it back at once from the read model: **404, your own order not found**, because the projection follows the write through the outbox, Debezium and Kafka. Pause the projection and it stays that way. |
| **Why** | The read model is eventually consistent, and without knowing which write to wait for it can only answer with what it has. |
| **Fix** | The write returns the order's `version` as a **consistency token**. `GET /orders/{id}?minVersion=N&waitMs=…` on order-query-service waits (at most 5 s, server side) until the projection has applied that version. If it is still behind it answers **409** with both versions, never stale data as if it were current. Alternatively, read the write model. |
| **Recover** | Resume the projection: the backlog drains and token reads succeed after a short wait. |
| **Evidence** | Per run: write latency and version; read status, latency and how long it waited; the verdict. |
| **Tests** | `OrderQueryIT.aConsistencyTokenReadWaitsForTheProjectionInsteadOfServingAStaleAnswer`; drill `lab_read_your_writes`. |
