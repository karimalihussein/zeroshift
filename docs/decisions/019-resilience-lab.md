# ADR 019: The resilience lab: real load, real network faults, claims checked against samples

**Status:** Accepted

## Decision

Phase 3 adds `/resilience` to the control plane: a load generator, network chaos, backpressure and
resilience levers on the running services, and seven experiments that each follow Hypothesis →
Inject → Observe → Explain → Apply mitigation → Recover → Verify.

- **Measurement, not animation.** A sampler in the control plane takes one sample a second from the
  load generator's clients, both order-service replicas (`/lab/edge`, `/lab/throughput`), every
  service's `/lab/pressure` (CPU, heap, Hikari pool, rebalances, decisions, `zeroshift.*` meters,
  read straight from its Micrometer registry), the gateway's breaker and policy, the brokers'
  committed offsets (lag for five consumer groups) and a group-less consumer tailing the
  outbox-fed topics (outbox → Kafka relay delay = record timestamp − `occurredAt`). The browser
  only draws samples; a null means "not measured", never zero. It samples only while the page is
  open or something runs.
- **Checks decide the claims.** Each step's claims are functions of the samples (this step's, the
  baseline's, earlier phases'). A claim is re-evaluated every second until it holds or its deadline
  passes; claims about a whole step ("no dead letters", "the API kept accepting") are judged once,
  at the end. A claim that does not hold is reported, the run continues to Recover and Verify, and
  the lab is reset either way. Runs are stored in `lab_run`.
- **Open-loop load.** Clients arrive at a fixed rate whether or not earlier ones were answered, so
  slowness shows up as waiting clients and latency instead of silently slowing the generator
  (coordinated omission). A concurrency cap counts excess arrivals as turned away. Every write
  carries an Idempotency-Key; retries (none, immediate, exponential, full jitter, jitter with a
  gRPC-style retry-throttling budget) reuse it. One client in twenty is traced; the rest send an
  unsampled `traceparent`, so Tempo keeps a sample instead of every order.
- **Network chaos is Toxiproxy, as an overlay.** `docker-compose.chaos.yml` adds Toxiproxy (~15 MB)
  and routes five real connections through it: order-service → PostgreSQL, payment-service →
  PostgreSQL, payment-service → Kafka, Debezium → PostgreSQL, payment-service → the card gateway.
  Faults: latency, bandwidth, reset, partition (timeout toxics both ways: bytes vanish, sockets stay
  open) and down (listener closed). An overlay, not a profile, because a profile can add Toxiproxy
  but cannot re-point other services' connection strings at it. Kafka needs its own listener
  (`CHAOS://…:9095` advertised as `toxiproxy:19095`): a client bootstrapped through a proxy is
  otherwise told to reconnect to the broker's real address and bypasses the proxy.
- **Levers live in the services.** order-service gains edge guards on `POST /orders`
  (Resilience4j rate limiter, a load shedder on unfinished sagas, a Resilience4j semaphore
  bulkhead), each refusing at once with 429/503 and `Retry-After`. payment-service's gateway call
  policy (timeout, retry schedule, breaker on/off, pause the consumer while the breaker is open) is
  switchable at runtime. The platform gains a `slow-processing` fault (a slow consumer), consumer
  `max.poll.records` / `max.poll.interval.ms` overrides that restart the listener container, and
  `/lab/pressure`. inventory-service can be restocked, so sustained load does not turn into a run
  of out-of-stock cancellations. None of these change behaviour until switched on.

## Why not k6, Gatling or a chaos platform

A load tool would need its own container (the stack already uses ~7.5 GB of a 9.7 GB VM) and would
report its numbers to itself: correlating them with Kafka lag and saga completions would mean
exporting to Prometheus and back. The generator is ~500 lines of plain Java with virtual threads
in the control plane, measures what the lab needs (per-client latency including retries, attempts
per client, outcomes by cause) and shares the sampler's clock. Toxiproxy is the smallest tool that
injects faults on a TCP link without root, iptables or kernel modules, and its state is readable,
so the UI shows the links as they are.

## Findings from running it (September 2026, Docker Desktop, 8 CPUs, 9.7 GB)

- **Capacity.** With every service, Debezium, the observability stack and a busy 3-node kafka-lab
  sharing 8 CPUs, the saga pipeline sustained 8 orders/s (lag 0, end-to-end p50 3.3 s) once the
  JVMs were warm; cold, it fell behind at 8/s. Experiments therefore run at 3–6 orders/s.
- **Timeouts turn delay into work.** Twice a first calibration failed for the same real reason: a
  queue whose wait passed the saga's 30 s step timeout. Timed-out sagas compensate, compensation
  sends RefundPayment commands to the same slow consumer, and the backlog feeds itself: 813 sagas
  sat in COMPENSATING after the Debezium link had been healed. The experiments are sized to keep
  waits under 30 s, and the CDC experiment keeps load shedding on through recovery; lifting it at
  once recreated the metastable state.
- **A static limit cannot match an unknown capacity.** Throttling Debezium's link to 4 KB/s let
  only ~0.5 orders/s through, below the rate limiter's minimum of 1/s per replica. Shedding on
  unfinished orders works for any slow hop, so the CDC and Kafka-partition experiments both use it.
- **Restarting Kafka strands Connect for ~4 minutes.** After the broker restarts, the Connect
  worker's incremental cooperative rebalancing waits `scheduled.rebalance.max.delay.ms` (5 min
  default) before reassigning connectors; they show UNASSIGNED and no outbox row is relayed. The
  slots keep the WAL, so nothing is lost.
- **Resilience4j's `changeLimitForPeriod`** applies from the next refresh period, so a lowered
  limit let the current period's permits through; the guard now replaces the limiter.

## Consequences

- Default stack: unchanged apart from new endpoints and the idle sampler. With the overlay:
  Toxiproxy, one extra Kafka listener, and those five connections through a proxy (sub-millisecond
  while healthy).
- `scripts/verify_resilience_lab.py` runs every experiment automatically and fails on any claim
  that did not hold, then checks the reset independently (services, Toxiproxy). Unit tests cover
  the generator (against a real HTTP server), guards, gateway policy and pressure readings;
  `ToxiproxyIT` checks each fault on a real PostgreSQL connection; `OrderServiceIT` checks the
  guards over HTTP.
- Settings are in memory. Restarting a service returns it to defaults, which is what "reset"
  means anyway.
