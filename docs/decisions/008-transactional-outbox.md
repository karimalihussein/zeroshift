# ADR 008: Transactional outbox relayed by Debezium

**Status:** Accepted

## Decision

A service never publishes to Kafka from its request or handler code. It inserts the message into its
own `outbox` table in the same transaction as the state change. Debezium reads committed inserts
from the PostgreSQL WAL (publication `outbox_inserts`, slot `<db>_outbox`) and its outbox event
router publishes each row to `row.topic`, keyed by `aggregate_id`, with `type`, `correlation_id`,
`causation_id` and `traceparent` as Kafka headers.

## Why

- Writing the database and then Kafka is a dual write: a crash between the two loses the event, and
  publishing first then rolling back announces something that never happened. The lab keeps both
  failures reachable (`POST /orders/dual-write`) so the difference can be seen.
- Reading the WAL instead of polling the table means no polling interval, no `published` flag to
  update, and no ordering questions between pollers. Only committed rows ever leave.
- Keying by aggregate id keeps every message about one order on one partition, in commit order.

## Consequences

- Delivery is at-least-once (Connect can resend after a restart), so every consumer is idempotent
  ([ADR 009](009-idempotent-consumer.md)).
- A replication slot pins WAL until its connector confirms it. A slot nobody reads grows forever, so
  only services with an outbox connector create one (`zeroshift.outbox-slot`), and the dashboard
  shows each slot's lag in bytes.
- The outbox is append-only here; deleting old rows is safe (the publication ignores deletes) but not
  automated.
