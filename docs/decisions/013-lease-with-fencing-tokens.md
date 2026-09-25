# ADR 013: A PostgreSQL lease with fencing tokens for the timeout scanner

**Status:** Accepted (replaces the ShedLock idea in the original plan)

## Decision

Two order-service replicas run. The saga-timeout scanner must run on one of them at a time, so it
holds a lease: a row in `lease(name, owner, token, expires_at)`. One upsert both acquires and renews;
it succeeds when the lease is free, expired, or already ours, and increments `token` whenever the
owner changes. The TTL is 5 s and the scanner renews every second.

Before doing any work, the scan's transaction runs
`SELECT 1 FROM lease WHERE name=? AND owner=? AND token=? FOR SHARE`. If the token was superseded,
the scan writes nothing. Each due saga row is also claimed with `FOR UPDATE SKIP LOCKED`.

## Why

- A lock that dies with its holder needs an expiry, and an expiry means a slow holder (GC pause,
  CPU starvation, network partition) can lose the lock without knowing. The fencing token is how the
  protected resource tells the old holder from the new one. Checking it inside the same transaction
  as the work, under a share lock, means a takeover waits for an in-flight scan to commit and a
  stale holder that wakes up later is rejected.
- ShedLock would give mutual exclusion but hides the token; the lab exists to show it.
- `SKIP LOCKED` is a second, independent guard: even overlapping scanners could not compensate one
  saga twice.

## Consequences

- The "Stall timeout scanner" fault reproduces the pause: the holder sleeps 8 s holding token n, the
  other replica takes over with n+1, and the woken holder is fenced (`zeroshift_lease_fenced_total`).
- Lease time comes from PostgreSQL's clock, so replica clock skew does not matter.
- Kafka consumer work is not guarded by the lease: the consumer group already gives each partition
  one owner, and the inbox handles the redeliveries a rebalance causes.
