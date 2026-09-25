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
| **Fix** | The write returns the order's `version` as a **consistency token**. `GET /orders/{id}?minVersion=N&waitMs=…` on order-query-service waits (at most 5 s, server side) until the projection has applied that version. If it is still behind it answers **409** `READ_MODEL_BEHIND`, with `requiredVersion`, `projectedVersion` and `waitedMs` in the error's `context`, never stale data as if it were current. The logic lives in `ReadYourWrites`, not the controller. Alternatively, read the write model. |
| **Recover** | Resume the projection: the backlog drains and token reads succeed after a short wait. |
| **Evidence** | Per run: write latency and version; read status, latency and how long it waited; the verdict. |
| **Tests** | `OrderQueryIT.aConsistencyTokenReadWaitsForTheProjectionInsteadOfServingAStaleAnswer`; drill `lab_read_your_writes`. |

---

# Phase 2: Kafka internals

The **Kafka internals** section of `/events` runs on a separate 3-node KRaft cluster, behind a
Compose profile so the everyday stack stays small:

```sh
docker compose --profile kafka-lab up -d      # the stack plus kafka-lab-1..3 and a Docker API proxy
```

Each node is both broker and controller, so the controller quorum has 3 voters. Failures are real:
**Kill** is a SIGKILL of the node's container, **Stop** a SIGTERM (Kafka's controlled shutdown),
**Freeze** a `docker pause` (every process stops; the kernel still accepts TCP). The control plane
does this through a Docker API proxy that only exposes container endpoints, and it only touches
containers labelled as this lab's nodes ([ADR 017](decisions/017-kafka-lab-cluster.md)).

Everything shown is read back: node states from Docker; quorum leader, epoch and voter lag from
`describeMetadataQuorum`; leaders, replicas, ISR and ELR from `describeTopics`; high watermark and
last stable offset from `listOffsets`; groups and committed offsets from the admin API; truncations
from the broker's own log. An observer reads all of it once a second and records every change it
sees (a node dying, a new quorum leader, a leader moving, an ISR shrinking). Runs are kept in
`lab_run`.

Lab settings that differ from Kafka's defaults, to make failures visible in seconds:
`broker.session.timeout.ms=6000` (9000), `broker.heartbeat.interval.ms=1000` (2000),
`replica.lag.time.max.ms=10000` (30000), `leader.imbalance.check.interval.seconds=30` (300),
`min.insync.replicas=2`, replication factor 3 for every internal topic.

Automated coverage: `python3 scripts/verify_event_lab.py kafka` (7 drills against the live
profile) and `KafkaLabIT` (9 scenarios on a 3-node cluster started by Testcontainers, failed through
the same Docker API proxy and the same classes).

## Cluster and controller quorum

| | |
|---|---|
| **Learn** | Three voters replicate the metadata log (topics, leaders, ISRs); a change needs a majority, 2 of 3. |
| **Trigger** | *Create lab topics*: `lab.replicated`, 3 partitions × 3 replicas, `min.insync.replicas=2`. |
| **Break** | Kill the quorum leader: the other two elect another (epoch goes up). Stop two nodes: one voter has no majority, so nothing can change. |
| **Understand** | A majority survives one failure, never two. Without a quorum, brokers keep serving partitions they lead, but no dead leader is replaced. |
| **Fix / Recover** | An odd number of voters in separate failure domains. *Recover all nodes*. |
| **Tests** | Drill `kafka_quorum`; `KafkaLabIT.killingTheControllerQuorumLeaderElectsAnother`. |

## Leader failure under load

| | |
|---|---|
| **Trigger** | Traffic: 20 records/s into `lab.replicated`, acks=all, idempotent; a consumer group reads them back and counts every sequence number. |
| **Break** | Kill the leader of `lab.replicated-0`: writes to its partitions stall until the broker session expires (≈6 s), then an in-sync follower leads. A graceful stop hands leadership over first: no stall. |
| **Observe** | Acknowledged and failed writes per second, the slowest acknowledgement, producer retries, every leader/ISR change. |
| **Understand** | The stall is failure detection, not the election. Nothing acknowledged is lost (2 in-sync copies, the new leader is one of them); retries are deduplicated. |
| **Recover** | Start the node: it truncates, catches up, rejoins the ISR; leadership returns to preferred replicas (30 s, or *Elect preferred leaders*). *Stop traffic and count*: acknowledged-but-never-consumed must be 0. |
| **Measured** | 557–1252 records acknowledged per run, **0 lost, 0 duplicated**, 11–162 producer retries, slowest acknowledgement 8.2–9.5 s. |
| **Tests** | Drill `kafka_failover`; `KafkaLabIT.aLeaderCrashUnderTrafficLosesAndDuplicatesNothing`. |

## Durability: acks, min.insync.replicas, retries

| | |
|---|---|
| **acks=1** | `lab.acks`, 2 replicas. The follower is frozen, then 1 s passes so a fetch already in flight comes back empty (otherwise it is applied when the follower thaws and hides the loss). Records 6–10 are written with acks=1, the leader is killed, the follower (still in the ISR) takes over: **records 6–10 acknowledged, then lost**. The old leader logs `Truncating to offset 5` when it returns. |
| **acks=all** | Same crash: nothing is acknowledged while the follower is frozen; the producer retries against the new leader; **nothing acknowledged is lost**. |
| **min.insync.replicas** | Stop a follower of `lab.durability` (ISR 2): acks=all still written. Require 3: acks=all is refused with `NotEnoughReplicasException`, **acks=1 is still written**: min ISR only guards acks=all. |
| **Retries** | A follower frozen 4 s, request timeout 800 ms: the producer retries while its first request waits in the leader's purgatory. Plain producer: **1 send, 2–3 retries, 3–4 copies in the log**. Idempotent: **1 copy**. |
| **Fix** | acks=all + `enable.idempotence=true` + `min.insync.replicas=2` on 3 replicas. |
| **Tests** | Drills `kafka_acks`, `kafka_min_isr`, `kafka_retries`; `KafkaLabIT.acksOne…`, `…minInsyncReplicas…`, `…retries…`. |

## Unclean leader election (explicitly dangerous)

| | |
|---|---|
| **Trigger** | `lab.unclean`, 2 replicas, `min.insync.replicas=1`. The follower is stopped, 3 records are acknowledged by the leader alone, the leader is killed, the stale follower returns. |
| **Observe** | The partition is **offline**: no leader, ISR empty, ELR = the dead leader (Kafka 4.1's eligible leader replicas: the ones safe to wait for). |
| **Break** | *Unclean election* (`Admin.electLeaders(UNCLEAN)`, behind a confirmation): available again, **3 acknowledged records lost for good**; the old leader truncates them when it rejoins. |
| **Fix** | Wait for the replica that has everything: start it and it leads again (it is in ELR), **all 4 records kept**. Keep `unclean.leader.election.enable=false` and `min.insync.replicas` ≥ 2. |
| **Tests** | Drill `kafka_unclean`; `KafkaLabIT.theLastInSyncReplicaDying…`, `…waitingForTheInSyncReplicaLosesNothing`. |

## Delivery semantics

A consume-transform-produce worker runs as **its own JVM** so that a crash is a real one: at the
chosen record it calls `Runtime.halt`, skipping every commit, close and shutdown hook. Each run uses
fresh `lab.delivery.in`/`.out` topics and a fresh group, writes records 1–30, runs the worker (it
crashes at record 12), restarts it, then reads the output as read_committed and read_uncommitted
and counts every record.

| Mode | Order | Crash at record 12 | read_committed | read_uncommitted |
|---|---|---|---|---|
| At most once | commit offsets, then write | after the commit, before the write | **gaps** (e.g. 12, 14) | same |
| At least once | write, then commit | after the write, before the commit | **duplicates** (e.g. 1, 7, 9, 12) | same |
| Exactly once | one Kafka transaction: outputs + offsets (`sendOffsetsToTransaction`) | mid-transaction, output flushed | **every record once** | the aborted copies are visible |

The restarted exactly-once worker uses the same `transactional.id`: `initTransactions` fences the
dead one and aborts its open transaction. Static group membership lets it take over the crashed
worker's partitions at once. Tests: drill `kafka_delivery`;
`KafkaLabIT.atMostOnceLeavesGapsAtLeastOnceDuplicatesAndTransactionsDoNeither`.
