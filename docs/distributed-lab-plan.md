# Distributed-systems lab: audit and plan

## 1. Audit of what exists

| Area | Today | Keep / change |
|---|---|---|
| Build | One Maven module, Java 25, Spring Boot 4.1, Testcontainers ITs | Becomes a multi-module build; the migration lab moves unchanged into `migration-lab/` (`git mv`, history kept) |
| Layering | `domain` ← `application` (ports) ← `infrastructure`/`web`; `ApplicationWiring` composes use cases | Same shape per service. No shared "framework" layer beyond a thin `platform` module |
| State | One PostgreSQL row + log table, every transition committed | Same rule: every screen value is a row, a Kafka offset or a connector status |
| Change capture | SQL Server Change Tracking, polled by the app | Stays for the migration lab (it *is* the lesson). The new lab uses Debezium on WAL instead of app polling |
| UI | Thymeleaf + plain JS, polls `/api/status` every second, self-hosted assets | Same stack. New `/events` page in the same app; migration page untouched |
| Observability | Actuator + Micrometer/Prometheus endpoint, durable operator log | OpenTelemetry agent everywhere; Prometheus, Tempo, Loki, Grafana |
| Runtime | Compose: SQL Server, PostgreSQL, app; dev override with hot reload | New services added to the same Compose project; heavy tooling behind profiles |

Resource budget measured on this machine: Docker has 16.7 GB; the current lab uses ~2.8 GB.
Estimated full stack ≈ 7 GB (see §5).

## 2. Architecture

```
                              ┌──────────────────── control plane (migration-lab app, /events) ───────────────────┐
                              │ reads: Kafka AdminClient (offsets, lag, groups) · Connect REST · service admin APIs│
                              │        · event tap (own consumer group) · Tempo/Loki links                          │
                              └───────────────▲───────────────────────────────▲──────────────────────────────────┘
                                              │                               │
 HTTP POST /orders                            │                               │
      │                                       │                               │
      ▼                                       │                               │
┌───────────────┐  one tx   ┌──────────────┐  │ WAL  ┌──────────┐  outbox   ┌───────────────────────────────┐
│ order-service │──────────►│ orders DB    │──┼─────►│ Debezium │──router──►│ Kafka (KRaft, 1 broker)        │
│ commands,     │           │ event_store  │  │      │ Connect  │           │ order.events      (3 partitions)│
│ event-sourced │           │ snapshots    │  │      └──────────┘           │ payment.commands  (3)          │
│ Order, saga   │◄──────────│ outbox       │  │           ▲                 │ payment.events    (3)          │
│ orchestrator  │  replies  │ saga_state   │  │           │ WAL             │ inventory.* shipping.* (3 each)│
└───────────────┘           │ processed_msg│  │           │                 │ *.DLT dead-letter topics       │
      ▲                     └──────────────┘  │   ┌───────┴───────────┐     └──────┬───────────┬────────────┘
      │ consumes *.events                     │   │ payments/inventory│            │           │
      └───────────────────────────────────────┼───│ /shipping DBs     │◄───────────┘           │
                                              │   │ (each: own tables,│  payment-, inventory-,  │
                                              │   │  outbox, processed│  shipping-service       │
                                              │   │  _message)        │  consume *.commands     │
                                              │   └───────────────────┘                         ▼
                                              │                                     ┌─────────────────────┐
                                              └─────────────────────────────────────│ order-query-service │
                                                                                    │ CQRS read model:    │
                                                                                    │ order_view, rebuild │
                                                                                    └─────────────────────┘
 OTel Java agent in every JVM ─► otel-collector ─► Tempo (traces) · Prometheus (metrics) · Loki (logs) ─► Grafana
```

Trace context crosses Kafka because the outbox row stores `traceparent`, and Debezium's outbox
router places it in a Kafka header that the consumer's OTel agent continues.

### Modules

| Module | Responsibility |
|---|---|
| `contracts` | Event and command records, the envelope (eventId, correlationId, causationId, type, schemaVersion, occurredAt), topic names, upcasters for old schema versions |
| `platform` | Only what every service repeats: outbox writer, idempotent-consumer gate, Kafka error handler (backoff → DLT), JSON codec. No DI magic |
| `migration-lab` | Existing ZeroShift app + the new control-plane page |
| `order-service` | Command API, event-sourced `Order` aggregate (event store, snapshots, optimistic concurrency), saga orchestrator with timeouts |
| `payment-service` | Payment authorisation/refund; calls a fault-injectable gateway through Resilience4j (timeout, retry, circuit breaker) |
| `inventory-service` | Stock reservation/release with optimistic locking on the SKU row |
| `shipping-service` | Shipment scheduling; last saga step, so its failure exercises full compensation |
| `order-query-service` | Projection of `order.events` into `order_view`; rebuild = truncate + replay from offset 0 |

Databases: one extra PostgreSQL instance (`wal_level=logical`) with a database per service, so
the migration lab's PostgreSQL stays untouched.

## 3. Concept → where you see it

| Concept | Real mechanism | Demo action |
|---|---|---|
| Dual-write problem | A deliberately naive endpoint writes DB then publishes directly; a crash between the two loses the event | "Naive dual write + crash" shows an order with no event |
| Transactional outbox | Event store row + outbox row in one tx; Debezium relays | Stop Connect, create orders, restart: nothing lost |
| Kafka partitions/ordering | Key = orderId; per-order events stay ordered on one partition | Event tap shows partition/offset per event |
| Consumer groups/rebalancing | 2 replicas of a participant service; group members & assignments from AdminClient | Crash a replica, watch reassignment |
| Offsets/retention/replay | Committed vs end offsets, lag; short-retention topic; reset group offsets | "Replay from offset" / "Reset group to earliest" |
| At-least-once + idempotency | `processed_message(consumer, event_id)` insert in the handler's tx; decisions recorded | "Duplicate event" re-publishes an existing record; UI shows `SKIPPED_DUPLICATE` |
| Event sourcing | `event_store(stream_id, version)` unique; snapshot every N; rebuild aggregate | "Rebuild aggregate" shows events folded vs snapshot |
| CQRS | Separate query service and DB; eventual consistency visible as lag | "Rebuild projection" |
| Saga + compensation | Durable `saga_state`; orchestrator commands; refund/release on failure; deadline timeout | "Fail payment / inventory / shipping" |
| Retries/backoff/DLQ | Spring Kafka `DefaultErrorHandler` exponential backoff → `*.DLT`; retry attempts recorded | "Poison message", "Inspect DLQ", "Redrive" |
| Timeouts/circuit breaker | Resilience4j around the payment gateway call; breaker state exported | "Gateway slow / down" |
| Optimistic locking | Event store version conflict; SKU `version` column | Concurrent reservations for last unit |
| Distributed locking | ShedLock (PostgreSQL) guards the saga-timeout scanner across 2 order-service replicas | Lock owner shown; kill owner, other takes over |
| Observability | OTel agent, Tempo trace per order, Loki logs by traceId, Grafana dashboards | Trace link on every order |

## 4. Increments (each ends green: unit + Testcontainers ITs, lab still runs)

1. **Restructure + infrastructure.** Multi-module build, migration lab moved and verified unchanged; Kafka (KRaft), Connect/Debezium, commerce PostgreSQL in Compose.
2. **Order write side.** Contracts, event-sourced `Order`, outbox, Debezium outbox connector, topics. IT: create order → record on `order.events` with correct key/partition and `traceparent`.
3. **Participants + saga.** Payment, inventory, shipping; orchestrator with compensation and timeout; idempotent gate; DLT. ITs for happy path, each failure, duplicates, poison message.
4. **CQRS read side.** Query service, projection, rebuild via replay.
5. **Control plane.** `/events` page: order journey timeline, topics/partitions/offsets/lag, consumer groups, idempotency decisions, DLQ, saga states, projection, demo actions.
6. **Resilience + locking.** Resilience4j gateway, optimistic-lock contention demo, ShedLock with 2 replicas, consumer crash/rebalance.
7. **Observability.** OTel collector, Tempo, Prometheus, Loki, Grafana with provisioned dashboards; trace/log links in the UI.
8. **Docs + full verification.** README, ADRs per pattern, a failure-drill guide, end-to-end verification script.

## 5. Estimated memory

Existing lab 2.8 GB · Kafka 0.7 · Connect 0.8 · 5 service JVMs 1.8 · commerce PostgreSQL 0.1 ·
Prometheus/Tempo/Loki/Grafana/collector 0.8 → **≈ 7 GB**. OpenSearch + Dashboards would add ≈ 2 GB,
which is why Loki is the default log store.

## 6. Deliberately not doing

- No service mesh, API gateway, Kubernetes or Schema Registry. Contracts are versioned Java
  records with upcasters; a registry adds a service without a new lesson at this size.
- No Kafka UI tool: the control plane shows the same facts, tied to the order being followed.
- No event-sourcing framework (Axon etc.): the store is two tables so the mechanism stays visible.
