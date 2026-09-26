# ADR 020: Events over time: replay from the real stores, schema evolution without a registry

**Status:** Accepted

## Decision

Phase 4 adds `/history` to the control plane: an order's history explorer and four six-step labs
(History → Inspect → Change / rebuild → Replay → Compare → Understand).

- **Time travel reads the source of truth.** order-service exposes `GET /orders/{id}/history`
  (every stored event as stored and as read today, with the state after it) and
  `GET /orders/{id}/rebuild?version=|at=`, which folds the event store up to a version or up to what
  was recorded by an instant. No snapshot is used, so the answer depends on the event store alone.
- **Kafka replay uses Kafka's own index.** A timestamp becomes one offset per partition through
  `ListOffsets` with `OffsetSpec.forTimestamp` (the broker's time index). Replays are consumers with
  no group (assign + seek, no commits), so looking at history never moves a service's position.
  An order's records are found on the one partition its key hashes to (murmur2, as the producer's
  partitioner), not by scanning the topic.
- **A new projection over old history.** Sales per SKU, built by the control plane from
  order.events in two versions: v1 counts lines when an order is placed (wrong: cancelled orders
  count), v2 when it ships. Each keeps its Kafka offsets in the same transaction as its rows, so a
  rebuild is "delete rows and offsets, read again" and a catch-up reads only what is new. The judge
  is independent of Kafka and of both versions: shipped orders' lines read from the event store.
- **Old events are real.** The contracts gain downcasters and `MessageCodec.writingAs(version, …)`:
  inside it, encoding writes the older schema where the contract can express it. A lab endpoint
  places an order through the normal `PlaceOrder` path inside `writingAs(1, …)`, so OrderPlaced v1
  lands in the event store and the outbox (and on Kafka) exactly as the previous release wrote it,
  and every reader, in every service, reads it through the existing v1 → v2 upcaster. The event
  store's and the outbox's `schema_version` columns now record the version actually written.
- **v3 is a proposal, not a production contract.** A meaningful v3 exists in the lab: money as
  integer minor units (`unitPriceMinor`, `totalMinor`), the usual fix when readers in other
  languages parse JSON numbers as floats. It is breaking (fields renamed and retyped), so it needs a
  version bump and a v2 → v3 upcaster. It is not added to the production contracts because every
  reader today is Java with `BigDecimal`: the change would force every service to redeploy for no
  product reason. The lab writes a v3 event to the real order.events; the deployed readers refuse
  it and dead-letter it on the first delivery with the reason in the dead letter's headers.
- **No Schema Registry.** The contracts are versioned Java records with upcasters in one shared
  module; every producer and consumer is built from it, the envelope carries `schemaVersion`, and
  compatibility is enforced by `ContractCompatibilityTest` against payloads frozen exactly as
  earlier releases wrote them. A registry would add a service and a second source of truth for the
  same rules without enabling anything this lab does. It would earn its place with producers or
  consumers outside this build (other languages, other teams) or with Avro/Protobuf.
- **One compacted topic.** `lab.order-status`, keyed by order id, holds real orders' status
  changelogs plus a tombstone, with lab-sized cleaning (5 s segments, dirty ratio 0.01, tombstones
  kept 20 s). The lab recreates it on each run and waits for the real log cleaner.

## Findings from running it

- order.events held 30,893 records; the fixed projection was rebuilt from offset 0 in 2.9 s and
  matched the event store exactly. The wrong one overcounted every SKU (6,314 keyboards from
  cancelled orders alone).
- Every stored OrderPlaced was v2 before this phase: the v1 upcaster had never run against real
  data. It now does, in every service.
- Debezium relays the outbox's JSONB column, so the record text is PostgreSQL's canonical form
  (`"type": "OrderPlaced"`, with a space), not the writer's bytes: match records by parsing them.
- Spring's dead-letter headers carry offsets and partitions as raw big-endian bytes. Written into a
  JSONB column as text they contain NUL and PostgreSQL rejects them; they must be decoded as
  numbers.
- The log cleaner compacted 19 records to 6 about 18 s after the segment rolled (its default
  check interval is 15 s). Until then a consumer sees every value.

## Consequences

- New endpoints only; default behaviour of every service unchanged. The outbox and event store
  compute the written schema version from what they encoded (one extra JSON parse per write).
- The control-plane database gains four `history_*` tables (Flyway V10).
- Replaying the whole of order.events keeps a v3 test record dead-lettering again on every
  read-model rebuild; that is what a poison event does, and the dead letter says why.
