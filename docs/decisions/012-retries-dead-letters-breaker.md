# ADR 012: Retries with backoff, dead-letter topics, and a circuit breaker

**Status:** Accepted

## Decision

A failing handler is retried by Spring Kafka's `DefaultErrorHandler` four times with exponential
backoff (0.5, 1, 2, 4 s), then the record goes to `<topic>.dlt` with the cause in its headers. A
record that cannot be decoded (`MalformedMessageException`) goes to the DLT at once. The payment
gateway call is wrapped in Resilience4j: a time limit, in-process retries, and a circuit breaker that
opens when most recent calls fail. The dashboard can inspect and redrive DLT records.

## Why

- Retrying in place (blocking the partition) keeps per-order ordering; a retry topic would not.
- Poison messages must not block a partition forever, and must not be dropped silently either.
- The breaker stops hammering a gateway that is down and fails fast, which turns into Kafka retries
  and finally a saga timeout with compensation rather than a stuck consumer.

## Consequences

- A partition is blocked for up to ~7.5 s per failing record while retries run.
- Redriving republishes the original record; the idempotent consumer makes a redrive of an
  already-applied record harmless.
