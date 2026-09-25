# ADR 009: Idempotent consumers and a durable decision log

**Status:** Accepted

## Decision

Every consumer handles a record inside one database transaction that also inserts
`processed_message(consumer, event_id)`. If that row already exists the record is skipped. Every
outcome (`PROCESSED`, `DUPLICATE_SKIPPED`, `IGNORED`, `RETRY_SCHEDULED`, `DEAD_LETTERED`) is written to
`consumer_decision` together with the partition, offset, attempt, trace id and the replica that
handled it. Offsets are committed per record after the transaction (`ack-mode: record`).

## Why

- Kafka redelivers whatever was not committed: after a crash, a rebalance, a replay, or a producer
  retry. The effect and its idempotency record must commit together; checking first and inserting
  later leaves a window where a redelivery runs twice.
- A decision log turns "exactly-once effects" from a claim into something visible: the
  "crash after commit" drill shows the redelivery arrive and get skipped.

## Consequences

- Deduplication is per consumer group and forever; the table grows with traffic (fine for a lab,
  a production system would expire rows older than the longest possible redelivery).
- Offsets are never the source of truth for "done"; they only bound how much comes back.
- Kafka transactions (exactly-once semantics) were not used: they cover Kafka-to-Kafka flows, not the
  database write each handler makes.
