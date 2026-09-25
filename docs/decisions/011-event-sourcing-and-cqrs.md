# ADR 011: Event-sourced order, separate read model

**Status:** Accepted

## Decision

The order aggregate is stored only as events in `event_store(stream_id, version)` with a unique
constraint on the pair; its state is rebuilt by folding them, starting from a snapshot taken every
three events. `order-query-service` consumes `order.events` into its own database (`order_view`).
Rebuilding the projection truncates it and replays the topic from offset 0.

## Why

- Two tables keep the mechanism visible: append with the expected version, fold, snapshot. A
  framework would hide exactly what the lab is meant to show.
- The unique `(stream_id, version)` constraint is the optimistic lock: two writers that loaded the
  same version cannot both append.
- A separate read side makes eventual consistency observable (consumer lag) instead of theoretical.

## Consequences

- `order.events` has infinite retention: it is the read model's source of truth for rebuilds.
- The read model can lag the write model; the journey view shows both side by side.
- Event schemas are versioned records with upcasters in `contracts`; there is no schema registry.
