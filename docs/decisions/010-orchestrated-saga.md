# ADR 010: Orchestrated saga with compensation and step deadlines

**Status:** Accepted

## Decision

The order service orchestrates: it sends `AuthorizePayment`, then `ReserveStock`, then
`ScheduleShipment` as commands and advances a durable `saga_state` row on each reply. A failure
sends compensating commands (`RefundPayment`, `ReleaseStock`) for the steps already done and waits
for their confirmations before the saga is `CANCELLED`. Each compensatable step has a deadline; a
scanner compensates sagas whose deadline passed. Shipping is the pivot: once requested, the saga
waits for its answer rather than timing out.

## Why

- Orchestration keeps the whole flow and its state in one place that the dashboard can show;
  choreography would spread it across three services' event handlers.
- A deadline is the only way to notice a participant that never answers (paused, crashed, stuck in
  retries).

## Consequences

- The saga row and the event-sourced order are updated in the same transaction as the outbox rows
  for the next commands, so the saga never advances without its commands being sent.
- Replies can race each other (different topics, different replicas); optimistic concurrency on the
  saga row and the event stream makes the loser retry against the new state.
- A late success reply after a timeout is `IGNORED` and its effect is undone by the compensation
  already in flight.
