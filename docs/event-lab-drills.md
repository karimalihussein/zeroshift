# Event lab: failure drills

Each drill breaks one thing on purpose and names what to watch. Everything shown comes from the
system itself: service tables, Kafka offsets and group membership, the lease row, Tempo, Loki and
Prometheus. Run them by hand from **http://localhost:8080/events**, or all at once:

```sh
python3 scripts/verify_event_lab.py                 # every drill, ~10 minutes
python3 scripts/verify_event_lab.py failover lease  # just some
```

The script only adds orders and arms one-shot faults; it does not reset anything. Drills are listed
in the order the script runs them.

| Drill | How to trigger | What you should see | Where |
|---|---|---|---|
| **happy** | Place an order | Outbox row → Kafka (partition, offset) → each consumer `PROCESSED` → saga `COMPLETED`, read model in sync | Journey; Grafana trace |
| **replicas** | Nothing: start the stack | `order-saga` members `order-service/order-saga-0` and `order-service-b/order-saga-0` split the 3 partitions of each reply topic 2 + 1 | Consumer groups |
| **duplicate** | *Duplicate* on any journey row | Same event id at a new offset; consumer records `DUPLICATE_SKIPPED`; nothing changes | Journey deliveries |
| **replay** | *Replay from 0* on `order-projection` | Consumers stop on every replica, offsets move to 0, lag jumps, drains as `DUPLICATE_SKIPPED`; read model unchanged | Consumer groups; *Consumer decisions / s* panel |
| **compensation** | *Decline next payment*, *Reject next reservation*, *Fail next shipment* | Saga `CANCELLED`; compensations none, `payment refunded`, `payment refunded` + `stock released` | Journey saga column |
| **poison** | *Poison message* | Unreadable record goes straight to `payment.commands.dlt` (no retries); the partition keeps flowing | DLQ panel |
| **observability** | Place an order, open *Trace in Tempo* | One trace across all five services, through Kafka; *Logs in Loki* shows its log lines; service graph | Grafana |
| **failover** | `docker compose kill order-service-b` (then `start` it) | No LeaveGroup is sent, so Kafka notices only when B's 10 s session times out; then the group rebalances and `order-service` owns all 9 reply partitions; orders placed meanwhile still complete; B rejoins when started. *Crash service* in the dashboard is gentler to watch but subtler: Docker restarts B in a few seconds, usually before the timeout, so B's old member expires and its new one takes the partitions back | Consumer groups; *Consumer lag* panel |
| **lease** | *Crash service* on the replica showing *scanner lease* | Within the 5 s TTL the other replica holds `saga-timeout-scanner` with a higher token | Services panel |
| **fencing** | *Stall timeout scanner 8 s* on `order-service` | The holder sleeps holding token n; the other replica takes over with n+1; the sleeper wakes, fails the fence check, writes nothing. Log line `fenced off`, counter `zeroshift_lease_fenced_total` | Services panel; Loki; *Saga timeouts and fenced scanners* panel |
| **crash_after_commit** | *Crash after commit* on `payment-service` | Payment committed, JVM halts before the offset commit, Docker restarts it, Kafka redelivers, inbox records `DUPLICATE_SKIPPED`: charged once | Journey deliveries |
| **timeout** | *Pause* the `payment-service` group, place an order | After `SAGA_STEP_TIMEOUT` (30 s) the lease holder times the saga out: `COMPENSATING`, "Timed out". The refund is a command to the paused consumer, so the saga stays there until you *Resume*: the late charge is `IGNORED` by the saga, then refunded, then `CANCELLED` | Journey saga column |
| **breaker** | *Gateway down*, place orders | In-process retries fail, the circuit breaker opens (calls rejected without touching the gateway), Kafka retries, then compensation or DLT | Gateway panel; *Payment gateway circuit breaker* panel |

## Reading the fencing drill

The lease alone would not be safe. Timeline of the drill:

1. `order-service` holds the lease with token 7 and starts a scan, then stalls for 8 s (a stand-in
   for a GC pause or a frozen VM).
2. After 5 s the lease expires. `order-service-b` acquires it: the owner changes, so the token
   becomes 8. B scans and compensates what is due.
3. `order-service` wakes up. It still believes it holds token 7. Its scan transaction runs
   `SELECT … FROM lease WHERE owner='order-service' AND token=7 FOR SHARE`, finds nothing, and
   returns without touching a saga.

Without step 3, both replicas could compensate the same saga. `FOR UPDATE SKIP LOCKED` on the saga
rows would still stop an exact double compensation, but only for overlapping scans; the fence also
rejects a stale holder that acts after the new holder has committed. See
[ADR 013](decisions/013-lease-with-fencing-tokens.md).

## Grafana

**http://localhost:3000** opens the *ZeroShift event lab* dashboard. No login.

- *Consumer decisions / s*: every delivery outcome by consumer. Replays and duplicates show up here.
- *Consumer lag*: from Kafka client metrics per service and topic.
- *Payment gateway circuit breaker* and *Gateway calls / s*: Resilience4j.
- *Saga timeouts and fenced scanners*: `zeroshift_saga_timeouts_total`, `zeroshift_lease_fenced_total`.
- *Service graph*: built by Tempo from spans, including the Kafka hops.
- *Warnings and errors*: Loki. Each line links to its trace; each trace links back to its logs.

In the control plane, the journey's *Trace in Tempo* and *Logs in Loki* links open Grafana Explore
for the order being followed.
