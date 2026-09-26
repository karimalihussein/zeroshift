package io.zeroshift.docs;

import io.zeroshift.docs.Model.Page;
import java.util.List;

/** Narrative pages. Facts point at the code and the ADRs; they do not restate a second API. */
final class Guides {
  private Guides() {}

  static List<Page> all() {
    return List.of(
        page(
            "overview",
            "Overview",
            "Getting started",
            "What ZeroShift runs, and where each fact in this portal comes from.",
            """
            ZeroShift is a local lab for two systems you can watch and break.

            The **migration lab** copies customers and orders from SQL Server to PostgreSQL while traffic keeps writing, then validates, cuts over and can roll back. The **event lab** is an order system: an event-sourced order service orchestrates payment, inventory and shipping through Kafka, and a query service projects a read model.

            This portal is served by the control plane at `/docs`. Endpoint paths, event fields and topic settings are taken from the controllers, the `Contracts` registry and `infra/kafka/topics.sh`. Where a page describes a mechanism, it names the class that implements it.

            There is no gRPC API and no WebSocket or server-sent stream. The dashboards poll HTTP. There is no authentication and no rate limit: both are absent on purpose in a local lab, not features waiting to be turned on.

            ## Two labs

            | Lab | UI | API |
            |---|---|---|
            | Migration | [http://localhost:8080](/) | `/api/status`, `/api/live`, `/api/actions` |
            | Event-driven | [http://localhost:8080/events](/events) | the service ports below, plus `/api/events` |
            | Kafka internals | the Event-driven page, profile `kafka-lab` | `/api/kafka-lab` |
            | Developer portal | `/docs` | `GET /api/docs/catalog` |

            ## Services

            | Service | Port | Database |
            |---|---|---|
            | migration-lab | 8080 | PostgreSQL `zeroshift`, SQL Server source |
            | order-service | 18081, replica 18088 | `orders` |
            | payment-service | 18082 | `payments` |
            | inventory-service | 18084 | `inventory` |
            | shipping-service | 18085 | `shipping` |
            | order-query-service | 18086 | `order_query` |

            Commerce databases share the PostgreSQL instance on port 55434. The migration target is a different instance on 55433.
            """),
        page(
            "architecture",
            "Architecture",
            "Getting started",
            "How a placed order becomes a shipment, and how a row moves between databases.",
            """
            ## Place an order

            :::flow
            Client → POST /orders → order-service
            order-service → PostgreSQL transaction → order event + outbox row
            outbox → Debezium EventRouter → Kafka
            Kafka → payment-service → inventory-service → shipping-service
            payment-service → order-saga
            inventory-service → order-saga
            shipping-service → order-saga
            order.events → order-query-service
            :::

            Before that transaction, `PlaceOrder` checks the customer and prices every item from inventory-service (`GET /products?sku=…`). That call is synchronous: when the catalog is down, placing fails with **503** `CATALOG_UNAVAILABLE`. `PlaceOrder` then writes the event, the outbox row, the `orders`, `order_item` and `invoice` rows and the voucher's use in one transaction. Debezium reads the WAL and the outbox event router publishes to the topic stored on the row. Participants answer on their own event topics. `SagaReplies` advances `Saga`. `OrderEvents` projects `OrderView`.

            The dual-write endpoint skips this. It writes the database and Kafka separately so you can see an event lost or invented. See [ADR 008](docs/decisions/008-transactional-outbox.md) in the repository.

            ## Patterns in this path

            - **Transactional outbox** — `Outbox`, published by Debezium rather than a second write. [microservices.io](https://microservices.io/patterns/data/transactional-outbox.html)
            - **Event sourcing** — `Order` is the fold of `event_store`. [Martin Fowler](https://martinfowler.com/eaaDev/EventSourcing.html)
            - **CQRS** — the query service has its own database and does not read the write model. [Martin Fowler](https://martinfowler.com/bliki/CQRS.html)
            - **Orchestrated saga** — `OrderSaga` decides the next command and the compensation. [microservices.io](https://microservices.io/patterns/data/saga.html)
            - **Idempotent consumer** — `Inbox` inserts `processed_message` in the handler's transaction. [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html)
            - **Circuit breaker** — Resilience4j around `HttpPaymentGateway` only. [Martin Fowler](https://martinfowler.com/bliki/CircuitBreaker.html)

            ## Commerce model

            Seven entities, each owned by one service and stored in that service's database ([ADR 021](docs/decisions/021-commerce-model.md)):

            | Entity | Owner | Tables |
            |---|---|---|
            | Customer, Voucher | order-service | `customer`, `voucher` |
            | Order, OrderItem, Invoice | order-service | `orders`, `order_item`, `invoice` (the `event_store` stays the source of truth) |
            | Product (catalog and stock) | inventory-service | `product`, `reservation`, `reservation_item` |
            | Payment | payment-service | `payment`, unique on `idempotency_key` |
            | Shipment | shipping-service | `shipment` |

            Ids are UUIDs. Money is `NUMERIC(12,2)` and `BigDecimal` at scale 2, rounded `HALF_EVEN`, always with a three-letter currency (`commerce.currency`, default `USD`). An order snapshots each item's product id, SKU, name and unit price, the voucher and the tax rate when it is placed, so later catalog or voucher changes never alter it. Across services a reference is a plain UUID: no foreign key and no join.

            ## Migrate a table

            :::flow
            SQL Server → Change Tracking → ChangeCatchUp
            SQL Server → keyset snapshot → PostgreSQL COPY
            Validation → fence → cutover → PostgreSQL primary
            PostgreSQL → reverse capture → SQL Server
            :::

            Change capture is SQL Server Change Tracking, not Debezium. Debezium is only the commerce outbox. The decision is [ADR 001](docs/decisions/001-change-capture.md). Cutover is [ADR 005](docs/decisions/005-cutover.md). Rollback replays PostgreSQL writes back and refuses out-of-band SQL Server writes as conflicts ([ADR 007](docs/decisions/007-reverse-sync-rollback.md)).

            ## Decisions

            The repository keeps one ADR per decision in `docs/decisions/`, numbered 001 through 021: change capture, snapshot boundary, batching, checkpointing, cutover, rollback, reverse sync, outbox, idempotent consumer, saga, event sourcing and CQRS, retries and dead letters, the fencing lease, observability, jOOQ, HTTP conventions, the Kafka lab cluster, the race condition lab, the resilience lab, events over time, and the commerce model.
            """),
        page(
            "quickstart",
            "Quick start",
            "Getting started",
            "Run the stack and place one order that you can see in the portal.",
            """
            Docker needs about 8–10 GB. SQL Server is the largest piece.

            ```sh
            docker compose up
            ```

            Open [the migration dashboard](/) and [the event lab](/events). This portal is [Docs](/docs/overview).

            Orders are placed for a real customer. Compose seeds customers, vouchers and the product catalog; the customer ids are random, so pick one:

            ```sh
            curl -sS "http://localhost:18081/customers?limit=5"
            ```

            Place an order against the write model with that `id`. `SKU-CABLE` is a real product of the inventory catalog (`GET http://localhost:18084/products`) and `WELCOME10` a real voucher (`GET /vouchers`).

            ```sh
            curl -sS -D - http://localhost:18081/orders \\
              -H 'content-type: application/json' \\
              -H 'Idempotency-Key: docs-demo-1' \\
              -d '{"customerId":"<customer id>","items":[{"sku":"SKU-CABLE","quantity":2}],"voucherCode":"WELCOME10"}'
            ```

            The status is **202**. The body carries the invoice number and the amounts in `currency` at 2 decimals, for example `"currency": "USD"`, `"subtotal": 25.98`, `"discount": 2.60`, `"tax": 1.87`, `"total": 25.25`. Keep `version` from the body and read your write:

            ```sh
            curl -sS -D - "http://localhost:18086/orders/<orderId>?minVersion=<version>&waitMs=2000"
            ```

            A projection that has caught up answers **200** with `X-Projected-Version`. One that has not answers **409** `READ_MODEL_BEHIND`.

            Try it on [Create order](/docs/api/order-post-orders) sends that POST from this control plane, and only that write. Replace its placeholder `customerId` with one from [List customers](/docs/api/order-get-customers). Crash, cutover, faults and dual-write stay on the lab pages.
            """),
        page(
            "local",
            "Running locally",
            "Getting started",
            "Ports, health and what is running when a lab profile is off.",
            """
            `docker compose up` is the development stack: the control plane runs from source with hot reload. Service images are packaged jars. After changing a service, rebuild that image.

            Each commerce service exposes Spring Boot actuator `health`, `info`, `metrics` and `prometheus` on its own port, for example `http://localhost:18081/actuator/health`. Compose uses that health URL inside the container.

            | Port | Process |
            |---|---|
            | 8080 | Control plane |
            | 18081 / 18088 | order-service A and B |
            | 18082 | payment-service |
            | 18084 | inventory-service |
            | 18085 | shipping-service |
            | 18086 | order-query-service |
            | 18083 | Kafka Connect |
            | 18090 | WireMock payment gateway |
            | 9094 | Kafka |
            | 3000 | Grafana |
            | 9090 | Prometheus |
            | 3200 | Tempo |
            | 3100 | Loki |

            The Kafka internals cluster is a Compose profile:

            ```sh
            docker compose --profile kafka-lab up -d
            ```

            Without it, `/api/kafka-lab` answers `KAFKA_LAB_NOT_CONFIGURED`. OpenSearch is a separate compose file and is not required.

            `race-lab` is a library on the control plane's classpath. `experiments.Catalog` currently registers one experiment, `oversell` (several reservations of the last item). There is no HTTP mapping for it in this repository, so this portal does not describe a `/race` API.
            """),
        page(
            "migration",
            "Migration",
            "Core concepts",
            "Snapshot, catch-up, validation, cutover and rollback.",
            """
            The unit of progress is a checkpoint committed with its rows. A crash resumes from the last one. `SnapshotBatch` loads a keyset range with PostgreSQL `COPY`. `ChangeCatchUp` applies SQL Server Change Tracking versions newer than the checkpoint.

            Stages, from `Stage`: idle, snapshot, catch-up, prepare, ready, freeze, validation, cutover, then the rollback stages. Progress is weighted by stage (snapshot up to 80%, then fixed points). It is not an estimate of remaining time.

            Validation fences the source and compares ordered SHA-256 fingerprints of every column (`RowFingerprint`). Cutover keeps that fence, drains, syncs sequences and switches the primary. Traffic follows the primary.

            Rollback after a successful cutover captures PostgreSQL writes, replays them into SQL Server, validates and fences. Abort is available until the switch and leaves PostgreSQL primary.

            The HTTP levers are [Migration status](/docs/api/lab-get-status) and [Migration action](/docs/api/lab-post-action). Live row edits are under `/api/live`.
            """),
        page(
            "cdc",
            "CDC",
            "Core concepts",
            "Two capture mechanisms, used for different jobs.",
            """
            ## Commerce outbox

            Each of `orders`, `payments`, `inventory` and `shipping` has a Debezium PostgreSQL connector named `{db}-outbox`. The connector reads `public.outbox` through the `outbox_inserts` publication and the `{db}_outbox` replication slot. `snapshot.mode` is `no_data`. The outbox event router routes by the row's `topic` column and copies `type`, `schema_version`, `correlation_id`, `causation_id` and `traceparent` onto Kafka headers.

            `order-query-service` sets `zeroshift.outbox-slot: false`. It consumes. It does not publish.

            Header names and the router settings are read from `infra/debezium/outbox-connector.json` when this portal starts. The register script's database list is `orders payments inventory shipping`.

            ## Migration changes

            The migration does not use Debezium. `ChangeTrackingCapture` reads SQL Server Change Tracking. Pause and resume (`/api/live/cdc/pause` and `/resume`) stop and start replay into PostgreSQL. Source writes continue, which is how the two databases are held apart on purpose.

            [Debezium outbox event router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
            """),
        page(
            "eda",
            "Event-driven architecture",
            "Core concepts",
            "Commands and events, and who publishes them.",
            """
            A message on Kafka is an `Envelope`: `eventId`, `type`, `schemaVersion`, `correlationId`, `causationId`, `occurredAt`, `payload`. `eventId` is the idempotency key. `correlationId` ties one order request together. `causationId` is the event that caused this message, and is null for the first.

            The order service publishes `OrderPlaced` and the commands `AuthorizePayment`, `ReserveStock` and `ScheduleShipment`. Each participant publishes its own events. The saga consumes those events. The query service consumes `order.events` only.

            The partition key is the order id, except carrier scans, whose key is chosen by the lab. One order stays ordered inside a partition. Two topics are not ordered against each other; the saga's version and the event-stream version reject the loser of a race.

            Schema version is `upcasters + 1` in `Contracts`. The only upcaster is `OrderPlaced` v1 to v2, which sets `currency` to `USD`.

            Adding a field is backward compatible, so the commerce model's additions did not change a version: `OrderPlaced` (still v2) gained `customerName`, `subtotal`, `discount`, `taxRate`, `tax`, `voucherCode` and `invoiceNumber`; `OrderLine` gained `productId`, `name`, `subtotal`, `discount` and `total`; `AuthorizePayment` gained `idempotencyKey`; `PaymentAuthorized` and `PaymentRefunded` gained `currency`; `ReserveStock`'s `StockLine` gained `productId`. A payload written before them decodes with neutral values (no discount, no tax, `subtotal = total`, the customer id as the name).

            :::flow
            OrderPlaced v1 → upcaster → OrderPlaced v2
            :::
            """),
        page(
            "cqrs",
            "CQRS",
            "Core concepts",
            "The write model and the read model are different APIs.",
            """
            `GET http://localhost:18081/orders/{id}` folds the event stream. `GET http://localhost:18086/orders/{id}` reads `order_view`. They can disagree until the projection catches up.

            The projection's version is `eventsApplied`. `POST /orders` returns that version on the write side as `version`. Pass it as `minVersion` to wait. The wait is capped at 5000 ms. The failure is `READ_MODEL_BEHIND` with both versions in `context`, not a stale 200.

            Rebuilding the projection rewinds the `order-projection` group's offsets, then deletes the read models and that consumer's inbox rows. Offsets move first so a failed delete cannot leave empty tables with the consumer already at the end.

            [Martin Fowler on CQRS](https://martinfowler.com/bliki/CQRS.html)
            """),
        page(
            "event-sourcing",
            "Event sourcing",
            "Core concepts",
            "The order is the fold of its events.",
            """
            `Order` is never updated in place. `Order.place` returns an `OrderPlaced` event. Later methods return the next event or refuse with `OrderRuleViolation`. `OrderRepository` folds the stream, optionally from a snapshot.

            `GET /orders/{id}/fold` returns every intermediate state. `DELETE /orders/{id}/snapshot` drops the snapshot only.

            Statuses, from `OrderStatus`: `NEW` (empty), `PLACED`, `PAID`, `RESERVED`, `SHIPPED`, `CANCELLED`.

            [Martin Fowler on event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html). The lab's version of the decision is [ADR 011](docs/decisions/011-event-sourcing-and-cqrs.md).
            """),
        page(
            "saga",
            "Saga",
            "Core concepts",
            "One orchestrator, durable steps, compensations.",
            """
            `OrderSaga` is the orchestrator. Payment and stock can be compensated. Shipping is the pivot: once a shipment is requested the saga waits for the answer instead of timing out.

            States in `SagaState`: `AWAITING_PAYMENT`, `AWAITING_STOCK`, `AWAITING_SHIPMENT`, `COMPENSATING`, `COMPLETED`, `CANCELLED`. The first two time out. A timeout scanner runs on one replica at a time, guarded by a PostgreSQL lease and a fencing token ([ADR 013](docs/decisions/013-lease-with-fencing-tokens.md)). `GET /lab/lease` on the order service shows the holder.

            Two order-service replicas share the `order-saga` group. Replies for one order can still be handled together after a rebalance. The saga version and the event-stream version make the loser retry.

            A decline or rejection moves the saga to compensation: `RefundPayment` and `ReleaseStock`. Refunding a charge that never happened, and releasing a reservation that does not exist, are successful no-ops in those handlers.

            [Saga pattern](https://microservices.io/patterns/data/saga.html). [ADR 010](docs/decisions/010-orchestrated-saga.md).
            """),
        page(
            "consistency",
            "Consistency",
            "Core concepts",
            "Versions, fences and read-your-writes.",
            """
            Four different versions show up in the lab:

            - The order event-stream version, returned as `version` from `POST /orders` and checked on every append (`CONCURRENT_UPDATE`).
            - The saga version, checked on every save.
            - The product row's stock, taken by a conditional update (`stock >= quantity`) when two reservations race, so the last item is sold once.
            - The read-model `eventsApplied`, compared with `minVersion`.

            The migration has a different kind of consistency: a source fence during validation and cutover, and conflict detection on rollback so an out-of-band SQL Server write is not overwritten.

            The long form is `docs/consistency.md` in the repository.
            """),
        page(
            "idempotency",
            "Idempotency",
            "Core concepts",
            "HTTP keys and consumer inbox rows do different jobs.",
            """
            ## At the HTTP edge

            `POST /orders` stores the first response under `Idempotency-Key`. The same key and the same body return that response with `Idempotent-Replayed: true`. The same key and a different body is **422** `IDEMPOTENCY_KEY_REUSED`. Without a key, a timed-out client that retries creates a second order. That difference is the point of the client-retries experiment.

            ## At the consumer

            `Inbox` inserts `eventId` into `processed_message` in the same transaction as the handler. Kafka is at least once, so a redelivery is normal. A poison message is not retried forever: after four attempts it is published to `{topic}.dlt`. `MalformedMessageException` goes there on the first failure.

            [Idempotent receiver](https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html). [ADR 009](docs/decisions/009-idempotent-consumer.md).
            """),
        page(
            "grpc",
            "gRPC",
            "gRPC",
            "There is no gRPC surface in this repository.",
            """
            ZeroShift has no `.proto` files and no gRPC server or client. Service boundaries are HTTP for commands and queries, and Kafka for everything that must survive the caller.

            If a gRPC API is added later, its contract belongs next to `contracts/` and this page should be generated from those descriptors the way events are generated from `Contracts`.
            """),
        page(
            "retries",
            "Retries",
            "Reliability",
            "Kafka deliveries and the payment gateway retry on different clocks.",
            """
            ## Consumers

            `PlatformConfiguration` installs a `DefaultErrorHandler` with four retries. The backoff starts at 500 ms, doubles, and caps at 4 s, so the waits are 500 ms, 1 s, 2 s and 4 s. The record is then published to `{topic}.dlt`. The delivery-attempt header is enabled on the listener, and each attempt is written to the decision log.

            ## Payment gateway

            `HttpPaymentGateway` retries `PaymentGateway.Unavailable` three times, exponential from 200 ms. Every attempt sends the command's idempotency key (`AuthorizePayment.idempotencyKey`, `order:<orderId>:authorize` for the saga) as the gateway's `Idempotency-Key`. The key is claimed in the `payment` table before the first call, so a redelivered command answers with the recorded outcome instead of charging twice. An open circuit breaker is not retried. The per-attempt timeout is `payment.gateway-timeout`, default 1500 ms.

            HTTP clients of the control plane use an 800 ms connect timeout and a 4 s read timeout (`LabServices`). The portal's Try it uses the same limits.
            """),
        page(
            "timeouts",
            "Timeouts",
            "Reliability",
            "Where a call gives up.",
            """
            | Call | Limit | Where |
            |---|---|---|
            | Payment gateway attempt | 1500 ms default | `payment.gateway-timeout` |
            | Gateway open breaker | 15 s, then half-open | `PaymentWiring` |
            | Control-plane HTTP connect | 800 ms | `LabServices` |
            | Control-plane HTTP read | 4 s | `LabServices` |
            | Query consistency wait | 0–5000 ms | `waitMs` |
            | Kafka consumer retries | 500 ms to 4 s, four times | `PlatformConfiguration` |
            | Saga step deadline | payment and stock only | `SagaState.timesOut` |
            | Kafka lab worker | 90 s | `WORKER_TIMED_OUT` |
            | Lab cluster recovery | 90 s | `CLUSTER_NOT_RECOVERED` |

            Shipping does not time out once the saga has asked for a shipment. It waits for `ShipmentScheduled` or `ShipmentFailed`.
            """),
        page(
            "breakers",
            "Circuit breakers",
            "Reliability",
            "One breaker, around the card gateway.",
            """
            The breaker is named `payment-gateway`. It is built in `PaymentWiring`, not with an annotation.

            - Sliding window of 10 calls
            - At least 4 calls before the failure rate counts
            - Opens at a 50% failure rate
            - Stays open 15 seconds, then allows 2 calls in half-open
            - Records `PaymentGateway.Unavailable` only

            `GET /lab/gateway` returns the mode and the breaker's live counters. `POST /lab/gateway/breaker/reset` closes it. `POST /lab/gateway/{mode}` makes WireMock healthy, slow, down or declining so the breaker has something real to see.

            [Circuit breaker](https://martinfowler.com/bliki/CircuitBreaker.html). [ADR 012](docs/decisions/012-retries-dead-letters-breaker.md).
            """),
        page(
            "concurrency",
            "Concurrency",
            "Reliability",
            "Optimistic versions, a lease, and consumer groups.",
            """
            Appends to an order stream and saga saves check a version. The loser gets `CONCURRENT_UPDATE` or the domain's equivalent and retries against the winner's state.

            Stock does not read and then write. A reservation takes each product with one conditional update, `UPDATE product SET stock = stock - :q … WHERE id = :id AND active AND stock >= :q`, in product-id order so two reservations never deadlock. If any item fails the transaction rolls back and the reservation is `REJECTED`. A limited voucher's last use is taken the same way, inside the order's transaction; the loser gets `VOUCHER_EXHAUSTED`.

            The timeout scanner takes a PostgreSQL lease. The fencing token is checked inside the transaction the lease guards, so a scanner that lost the lease cannot still write.

            `order-saga` runs one consumer per replica (`order.saga-consumers`, default 1). Two replicas split the partitions of the three reply topics. Stopping one replica moves those partitions.

            The other listeners use concurrency 3, matching the three partitions of their topic: `payment-service`, `inventory-service`, `shipping-service`, `order-projection`.

            `race-lab` is the start of a separate concurrency lesson (isolation levels, optimistic locking, retries). It is not wired to an HTTP API in this tree.
            """),
        page(
            "failures",
            "Failure handling",
            "Reliability",
            "What happens to a record that cannot be processed.",
            """
            A handler exception is retried, then the record is published to `{topic}.dlt` with the original key. The decision log stores `DEAD_LETTERED` and the cause. The dead-letter topics have one partition and unlimited retention so they can be inspected and redriven in arrival order.

            The event lab can redrive and can also inject a poison message or a duplicate. Those levers are `POST /api/events/actions/{action}` (`redrive`, `poison`, `duplicate`). They are not offered as Try it.

            `POST /lab/crash` accepts the request and then exits the JVM. Use it from the event lab when you want to watch a rebalance, not from this portal.

            Errors themselves use `application/problem+json`. The shape is `ApiError`: RFC 9457 fields plus `code`, `requestId`, `traceId`, `errors` and `context`. Clients branch on `code`. [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457.html). [ADR 016](docs/decisions/016-http-api-conventions.md).
            """),
        page(
            "errors",
            "Errors",
            "Reliability",
            "The problem+json body and the codes this lab actually returns.",
            """
            Every error is `application/problem+json`:

            ```json
            {
              "type": "about:blank",
              "title": "Unprocessable Content",
              "status": 422,
              "detail": "Unknown SKU SKU-NOPE",
              "instance": "/orders",
              "code": "ORDER_RULE_VIOLATION",
              "requestId": "…",
              "traceId": "…",
              "errors": [],
              "context": {}
            }
            ```

            Empty `errors` and `context` are omitted. `errors[]` is `{field, message}` for validation. `context` carries machine-readable facts, such as `requiredVersion` on `READ_MODEL_BEHIND`.

            **401, 403 and 429 are not produced.** Nothing in the lab checks an identity or a quota.

            The table on this page is the code list from the services. A test fails if a new code appears in the source and is missing here.
            """),
        page(
            "labs-migration",
            "Migration lab",
            "Labs",
            "Copy, catch up, validate, cut over, roll back.",
            """
            The page is [/](/). Generate data, start traffic, start the migration, and use pause, crash and resume to see checkpoints. Live data changes insert, update or delete a real SQL Server row and show it beside the PostgreSQL copy. Pause CDC replay to hold them apart.

            Actions and the status document are in the [Migration](/docs/api/lab-get-status) API group. The narrative of each stage is [Migration](/docs/migration).
            """),
        page(
            "labs-kafka",
            "Kafka lab",
            "Labs",
            "A 3-node cluster for acks, ISR, unclean election and delivery.",
            """
            `docker compose --profile kafka-lab up -d` starts three KRaft brokers. Topics declared by `LabTopics` are replicated, unlike the single-node commerce cluster (replication factor 1 in `topics.sh`).

            The HTTP surface is `/api/kafka-lab`. Reads (`GET /state`, log truncations) can be sent from Try it. Killing a broker, changing ISR and running scenarios are documented and left to the event lab, because they stop processes.

            [ADR 017](docs/decisions/017-kafka-lab-cluster.md).
            """),
        page(
            "labs-ordering",
            "Ordering lab",
            "Labs",
            "What the partition key does to carrier scans.",
            """
            `shipping.carrier-scans` is a separate topic so the lab can add partitions and recreate it without touching shipment commands.

            `POST /lab/carrier/scans` publishes real scans for real shipped parcels. `keying=tracking` keeps one parcel in one partition. `scan` gives every scan its own key. `hub` puts a hub's scans on one hot key (`hub-AMS`).

            The tracking projection can ignore a scan whose `seq` is older than the one it already applied. `PUT /lab/tracking/guard?on=true` is that switch. Replay rebuilds the projection from the topic.
            """),
        page(
            "labs-cqrs",
            "CQRS lab",
            "Labs",
            "Read your own write, then rebuild the projection.",
            """
            Place an order, then immediately `GET` it on port 18086 with no `minVersion`. It may be missing. Repeat with `minVersion` set to the `version` you were given and `waitMs` above zero.

            `POST /lab/projection/rebuild` on the query service rewinds `order-projection` and empties the read models. Lag draining back to zero is the rebuild. The endpoint is documented and not executable here.
            """),
        page(
            "labs-saga",
            "Saga lab",
            "Labs",
            "Compensation, the lease, and two replicas.",
            """
            Set the gateway to `declining` or `down` from the event lab and place an order. The saga compensates. `GET /lab/lease` shows which order-service replica holds the timeout scanner.

            `order-service-b` on port 18088 is the same program with the same database. Crash one replica from the event lab and the `order-saga` partitions move.
            """),
        page(
            "labs-chaos",
            "Chaos lab",
            "Labs",
            "Faults, crashes and poison messages.",
            """
            Each commerce service can arm a named fault (`PUT /lab/faults/{name}`), pause a listener, or exit after acknowledging `POST /lab/crash`. The event lab is the place to do that, because the effect is the thing you came to see and the portal will not send those calls.

            The decision log (`GET /lab/decisions`) and the outbox (`GET /lab/outbox`) are safe to read from Try it. Pick the service in the host list: the same routes exist on every commerce process.
            """),
        page(
            "labs-race",
            "Race lab",
            "Labs",
            "A concurrency engine without an HTTP API yet.",
            """
            `race-lab` runs experiments against PostgreSQL. The one registered in `experiments.Catalog` is `oversell`: each request reads stock, checks it in Java, then writes stock minus one. The strategies are unsafe, serializable, pessimistic (`FOR UPDATE`), optimistic (`WHERE version = :read`) and atomic (the conditional update decides). The engine records the interleaving. Nothing in this repository maps that engine to HTTP.

            Concurrency you can call over HTTP today is the conditional stock update on `product`, the voucher's limited uses, the saga version, the event-stream version, and the carrier-scan ordering lab.
            """));
  }

  private static Page page(
      String slug, String title, String group, String summary, String markdown) {
    return new Page(slug, title, group, summary, markdown.strip());
  }
}
