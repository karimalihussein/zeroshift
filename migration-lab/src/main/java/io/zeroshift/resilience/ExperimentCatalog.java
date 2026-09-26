package io.zeroshift.resilience;

import static io.zeroshift.resilience.Experiment.Phase.*;
import static io.zeroshift.resilience.Metrics.*;

import io.zeroshift.resilience.Experiment.Check;
import io.zeroshift.resilience.Experiment.Observation;
import io.zeroshift.resilience.Experiment.Phase;
import io.zeroshift.resilience.Experiment.Step;
import io.zeroshift.resilience.Experiment.Window;
import io.zeroshift.resilience.LoadGenerator.Profile;
import io.zeroshift.resilience.Toxiproxy.Fault;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.springframework.stereotype.Component;

/**
 * The resilience lab's experiments. Each one breaks something real (a network link through
 * Toxiproxy, a consumer's speed, the gateway, the consumer's poll settings), states beforehand what
 * should happen, and checks it against measured samples. The numbers in the claims are sized for
 * the lab's two small order-service replicas and three-partition topics.
 */
@Component
public class ExperimentCatalog {
  private static final String PAYMENT = Levers.PAYMENT;

  private final Levers levers;
  private final LoadGenerator load;
  private final Sampler sampler;
  private final Map<String, Experiment> experiments = new LinkedHashMap<>();

  public ExperimentCatalog(Levers levers, LoadGenerator load, Sampler sampler) {
    this.levers = levers;
    this.load = load;
    this.sampler = sampler;
    for (var e :
        List.of(
            slowConsumer(),
            pollInterval(),
            gatewayBreaker(),
            bulkhead(),
            retryStorm(),
            kafkaPartition(),
            cdcDegraded())) experiments.put(e.id(), e);
  }

  public List<Experiment> all() {
    return List.copyOf(experiments.values());
  }

  public Experiment get(String id) {
    return experiments.get(id);
  }

  // ---- Backpressure --------------------------------------------------------------------------

  private Experiment slowConsumer() {
    var load = new Profile(6, 200, 0, "jitter", 3, 3000);
    return new Experiment(
        "slow-consumer",
        "Slow consumer: lag grows when processing is slower than traffic",
        "The payment consumer is made to take 700 ms per command while 6 orders/s keep arriving.",
        List.of("slow consumer", "Kafka lag", "Little's law", "rate limiting"),
        false,
        load,
        List.of(
            hypothesis(
                load,
                "Three payment consumer threads (one per partition) at 700 ms a command can process"
                    + " at most 3 × 1.43 = 4.3 commands/s. Orders arrive at 6/s, so the"
                    + " payment-service group's lag should grow by roughly 1.7 records every second,"
                    + " with no errors anywhere: the API keeps answering 202 while every new order"
                    + " waits longer for its payment. Capping intake below 4.3/s should make the lag"
                    + " shrink again."),
            step(
                INJECT,
                "Make the payment consumer slow",
                "Arms the slow-processing fault on payment-service: every delivery sleeps 700 ms on"
                    + " the consumer thread before it is handled.",
                () -> {
                  levers.slowConsumer(PAYMENT, 700);
                  return "payment-service: slow-processing 700 ms per delivery";
                },
                5),
            observe(
                "Lag builds up on payment.commands",
                check(
                    "payment-service lag passes 45 records",
                    60,
                    w -> above(w.latest(lag(PAYMENT)), 45, "payment-service lag %.0f")),
                check(
                    "…and is still growing (higher than 10 s ago)",
                    60,
                    w -> {
                      var now = w.latest(lag(PAYMENT));
                      var before = w.ago(lag(PAYMENT), 10);
                      return now == null || before == null
                          ? Observation.unmeasured("lag")
                          : Observation.of(
                              now > before, "lag %.0f now, %.0f 10 s ago", now, before);
                    }),
                throughout(
                    "…while the API keeps accepting orders (≥ 80 % of arrivals succeed over the step)",
                    10,
                    w ->
                        ratio(
                            w.recent(SUCCEEDED, w.seconds()),
                            w.recent(OFFERED, w.seconds()),
                            0.8,
                            "%.0f %% of arrivals succeed"))),
            explain(
                "Kafka is the buffer between a fast producer and a slow consumer. The broker does"
                    + " not push back on the producer (Debezium), so nothing upstream notices: the"
                    + " cost shows up only as lag, and by Little's law as waiting time. Each record"
                    + " behind is 1/4.3 s of extra delay, so lag of 86 is 20 s. If it reaches the"
                    + " saga's 30 s step timeout, orders start being cancelled even though nothing"
                    + " failed."),
            step(
                MITIGATE,
                "Rate-limit intake below consumer capacity",
                "Turns on the edge rate limiter at 1 order/s on each order-service replica (2/s in"
                    + " total, below the 4.3/s the consumer can do). The excess gets 429 with"
                    + " Retry-After, and clients back off with jitter.",
                () -> {
                  levers.edge(new Levers.Edge(true, 1, false, 200, false, 6));
                  return "edge rate limit: 1/s per replica";
                },
                3,
                check(
                    "requests are being rate-limited",
                    30,
                    w -> above(w.recent(RATE_LIMITED, 5), 0, "%.1f rate-limited/s")),
                check(
                    "payment-service lag falls at least 25 % below its peak",
                    120,
                    w -> {
                      var peak = w.peak(lag(PAYMENT), OBSERVE, MITIGATE);
                      var now = w.latest(lag(PAYMENT));
                      return now == null || peak == null
                          ? Observation.unmeasured("lag")
                          : Observation.of(now <= peak * 0.75, "lag %.0f, peak %.0f", now, peak);
                    })),
            step(
                RECOVER,
                "Restore the consumer, lift the limit",
                "Clears the slow-processing fault and turns the rate limiter off.",
                () -> {
                  levers.clearFault(PAYMENT, "slow-processing");
                  levers.edge(Levers.Edge.OFF);
                  return "slow-processing cleared, edge guards off";
                },
                3),
            verify(
                check(
                    "payment-service lag drains to ≤ 5",
                    120,
                    w -> below(w.latest(lag(PAYMENT)), 5, "lag %.0f")),
                check(
                    "orders complete at the arrival rate again (≥ 70 %)",
                    60,
                    w ->
                        ratio(
                            w.recent(COMPLETED, 5),
                            w.recent(OFFERED, 5),
                            0.7,
                            "%.0f %% of arrivals completed")))));
  }

  private Experiment pollInterval() {
    var load = new Profile(3, 100, 0, "jitter", 3, 3000);
    return new Experiment(
        "poll-interval",
        "max.poll.interval.ms exceeded: a working consumer thrown out of its group",
        "A backlog meets max.poll.records=100 and a 6 s max.poll.interval.ms: each poll hands the"
            + " consumer more work than it can finish in time.",
        List.of("max.poll.interval.ms", "rebalance storm", "redelivery", "pause/resume"),
        false,
        load,
        List.of(
            hypothesis(
                load,
                "Payment commands take 600 ms each (slow-processing), so the consumer handles"
                    + " about 5/s against 3/s arriving: fine while it keeps up. But Kafka only"
                    + " believes a consumer is alive if it calls poll() at least every"
                    + " max.poll.interval.ms. With a backlog, one poll returns up to"
                    + " max.poll.records records: 30 of them take 18 s, far past a 6 s interval."
                    + " The broker should evict the consumer while it is still working, rebalance,"
                    + " and hand the same uncommitted records out again: repeated rebalances,"
                    + " redeliveries, and lag that grows although capacity exceeds demand.",
                () -> {
                  levers.slowConsumer(PAYMENT, 600);
                  return "payment-service: slow-processing 600 ms per delivery";
                }),
            step(
                INJECT,
                "Build a backlog, then poll it with max.poll.records=100",
                "Sets max.poll.records=100 and max.poll.interval.ms=6000 on the payment consumer"
                    + " (restarting it), pauses it for 20 s so a backlog builds, then resumes it.",
                () -> {
                  levers.consumerConfig(PAYMENT, PAYMENT, 100, 6000);
                  levers.consumer(PAYMENT, PAYMENT, "pause");
                  Thread.sleep(20_000);
                  levers.consumer(PAYMENT, PAYMENT, "resume");
                  return "max.poll.records=100, max.poll.interval.ms=6000; paused 20 s, resumed";
                },
                2),
            observe(
                "The group rebalances again and again",
                check(
                    "at least 2 rebalances",
                    120,
                    w -> above(w.sum(REBALANCES), 1, "%.0f rebalances in this step")),
                check(
                    "payment-service lag stays above 40",
                    60,
                    w -> above(w.latest(lag(PAYMENT)), 40, "lag %.0f")),
                check(
                    "records handled before an eviction come back and are skipped as duplicates",
                    120,
                    w -> above(w.sum(DUPLICATES), 0, "%.0f duplicates skipped"))),
            explain(
                "max.poll.interval.ms is a liveness check on the processing loop, not on the"
                    + " network: heartbeats come from a background thread, but a consumer that"
                    + " does not return to poll() in time leaves the group on its own. Its"
                    + " partitions move, its offset commits are refused (CommitFailedException),"
                    + " and the records it already handled come back. The idempotent inbox keeps"
                    + " the effects single (see duplicates skipped), but the work is repeated and"
                    + " every rebalance stops the whole group. The fix is to make one poll's work"
                    + " fit the interval: fewer records per poll, or more time."),
            step(
                MITIGATE,
                "Poll fewer records at a time",
                "Sets max.poll.records=5 (5 × 600 ms = 3 s, inside the 6 s interval), restarting"
                    + " the consumer.",
                () -> {
                  levers.consumerConfig(PAYMENT, PAYMENT, 5, 6000);
                  return "max.poll.records=5, max.poll.interval.ms=6000";
                },
                2,
                check(
                    "no rebalance for 20 consecutive seconds",
                    120,
                    w -> {
                      double recent = w.recentSum(REBALANCES, 20);
                      return Observation.of(
                          w.seconds() >= 20 && recent == 0,
                          "%.0f rebalances in the last 20 s",
                          recent);
                    }),
                check(
                    "payment-service lag falls below its peak",
                    120,
                    w -> {
                      var peak = w.peak(lag(PAYMENT), OBSERVE, MITIGATE);
                      var now = w.latest(lag(PAYMENT));
                      return now == null || peak == null
                          ? Observation.unmeasured("lag")
                          : Observation.of(now < peak * 0.8, "lag %.0f, peak %.0f", now, peak);
                    })),
            step(
                RECOVER,
                "Default consumer settings, normal speed",
                "Clears the consumer overrides (restart) and the slow-processing fault.",
                () -> {
                  levers.clearFault(PAYMENT, "slow-processing");
                  levers.consumerConfig(PAYMENT, PAYMENT, null, null);
                  return "consumer defaults, slow-processing cleared";
                },
                3),
            verify(
                check(
                    "payment-service lag drains to ≤ 5",
                    120,
                    w -> below(w.latest(lag(PAYMENT)), 5, "lag %.0f")),
                check(
                    "no rebalance for 15 s",
                    60,
                    w -> {
                      double recent = w.recentSum(REBALANCES, 15);
                      return Observation.of(
                          w.seconds() >= 15 && recent == 0,
                          "%.0f rebalances in the last 15 s",
                          recent);
                    }))));
  }

  // ---- Resilience ----------------------------------------------------------------------------

  private Experiment gatewayBreaker() {
    var load = new Profile(4, 200, 0, "jitter", 3, 3000);
    var generous = new Levers.Policy(5000, "exponential", 3, false, false);
    return new Experiment(
        "gateway-breaker",
        "Slow dependency: timeout, circuit breaker and pausing the consumer",
        "The card gateway starts answering in 3 s. A generous timeout lets it hold every payment"
            + " thread; a short timeout with a breaker fails fast and parks the work in Kafka.",
        List.of("timeout", "circuit breaker", "pause/resume", "backpressure"),
        false,
        load,
        List.of(
            hypothesis(
                load,
                "With a 5 s timeout and no breaker, a 3 s gateway is merely slow, so every charge"
                    + " waits for it: three consumer threads manage 1 charge/s against 4 orders/s."
                    + " Lag should grow and throughput collapse without a single error. With an 800"
                    + " ms timeout the calls fail fast, the breaker opens after half of the last"
                    + " calls failed, and the consumer pauses: the backlog waits in Kafka instead"
                    + " of burning retries into the dead-letter topic. When the gateway recovers,"
                    + " the breaker's half-open probes succeed and the backlog drains.",
                () -> {
                  levers.policy(generous);
                  return "gateway policy: timeout 5000 ms, exponential retry ×3, breaker off";
                }),
            step(
                INJECT,
                "Make the gateway slow",
                "Puts the simulated card gateway (WireMock) in slow mode: every charge answers after"
                    + " 3 s.",
                () -> {
                  levers.gateway("slow");
                  return "gateway: slow (3 s per charge)";
                },
                5),
            observe(
                "Throughput collapses, lag grows, nothing fails",
                check(
                    "payment-service lag passes 40",
                    90,
                    w -> above(w.latest(lag(PAYMENT)), 40, "lag %.0f")),
                check(
                    "gateway calls between 0.2 and 1.5/s over 10 s (three threads, 3 s each)",
                    60,
                    w -> {
                      var rate = w.recent(GATEWAY_CALLS, 10);
                      return rate == null
                          ? Observation.unmeasured("gateway calls")
                          : Observation.of(
                              w.seconds() >= 10 && rate > 0.2 && rate <= 1.5,
                              "%.2f gateway calls/s",
                              rate);
                    }),
                throughout(
                    "no dead letters during the step",
                    5,
                    w ->
                        Observation.of(
                            w.sum(DEAD_LETTERED) == 0,
                            "%.0f dead-lettered",
                            w.sum(DEAD_LETTERED)))),
            explain(
                "A slow dependency is worse than a dead one. Nothing times out, so nothing is"
                    + " counted as a failure: the breaker (if it were on) would stay closed, and"
                    + " each consumer thread spends 3 s per command. The system's capacity is now"
                    + " the dependency's latency divided into the thread count. A timeout turns"
                    + " slowness into failure the breaker can count; the breaker then stops calling"
                    + " for a while; and pausing the consumer while it is open stops Kafka from"
                    + " delivering commands that could only fail, so they are not retried into the"
                    + " dead-letter topic."),
            step(
                MITIGATE,
                "Short timeout, breaker on, pause while open",
                "Gateway policy: 800 ms timeout, up to 2 attempts with jittered backoff, breaker"
                    + " on, and pause the payment consumer while the breaker is open (resume on"
                    + " half-open to let probes through).",
                () -> {
                  levers.policy(new Levers.Policy(800, "jitter", 2, true, true));
                  return "gateway policy: timeout 800 ms, jitter retry ×2, breaker on, pause on open";
                },
                3,
                check(
                    "the breaker opens",
                    60,
                    w ->
                        Observation.of(
                            w.max(BREAKER_OPEN) != null && w.max(BREAKER_OPEN) == 1,
                            "breaker %s",
                            latestBreaker(w))),
                check(
                    "the payment consumer is paused while open",
                    60,
                    w ->
                        Observation.of(
                            w.max(PAYMENT_PAUSED) != null && w.max(PAYMENT_PAUSED) == 1,
                            "paused seen: %s",
                            w.max(PAYMENT_PAUSED) != null && w.max(PAYMENT_PAUSED) == 1))),
            step(
                RECOVER,
                "Heal the gateway",
                "Puts the gateway back to healthy. The breaker's next half-open probes succeed and"
                    + " close it; the consumer resumes.",
                () -> {
                  levers.gateway("healthy");
                  return "gateway: healthy";
                },
                3),
            verify(
                check(
                    "the breaker closes",
                    120,
                    w ->
                        Observation.of(
                            w.latest(BREAKER_CLOSED) != null && w.latest(BREAKER_CLOSED) == 1,
                            "breaker %s",
                            latestBreaker(w))),
                check(
                    "the payment consumer runs",
                    60,
                    w ->
                        Observation.of(
                            w.latest(PAYMENT_PAUSED) != null && w.latest(PAYMENT_PAUSED) == 0,
                            "paused: %s",
                            w.latest(PAYMENT_PAUSED))),
                check(
                    "payment-service lag drains to ≤ 5",
                    180,
                    w -> below(w.latest(lag(PAYMENT)), 5, "lag %.0f")))));
  }

  private Experiment bulkhead() {
    var load = new Profile(20, 600, 40, "none", 1, 5000);
    return new Experiment(
        "bulkhead",
        "Slow database: a bulkhead keeps reads alive",
        "100 ms is added to every answer on the order-service → PostgreSQL link while 20 clients/s"
            + " place orders and read them back.",
        List.of("network latency", "connection pool", "bulkhead"),
        true,
        load,
        List.of(
            hypothesis(
                load,
                "Placing an order is several round trips inside one transaction, each now 100 ms"
                    + " slower, so a placement holds one of the replica's 10 pool connections for"
                    + " most of a second. At 12 placements/s the pools saturate; placements queue"
                    + " for connections, and reads (GET /orders/{id}, 40 % of clients) queue behind"
                    + " them though each needs a connection only briefly. Read p99 should jump from"
                    + " milliseconds to seconds. Capping concurrent placements (a bulkhead) should"
                    + " leave connections for reads: read p99 recovers while excess placements are"
                    + " refused fast with 503."),
            step(
                INJECT,
                "Add 100 ms latency to the order database link",
                "Toxiproxy latency toxic on order-db, downstream: every answer PostgreSQL sends the"
                    + " order-service replicas is held back 100 ms (± 20).",
                () -> {
                  levers.chaos().inject("order-db", Fault.LATENCY, 100, 20);
                  return "order-db: +100 ms ± 20 on every answer";
                },
                3),
            observe(
                "Reads starve behind placements",
                check(
                    "read p99 above 1 s",
                    90,
                    w -> above(w.latest(READ_P99), 1000, "read p99 %.0f ms")),
                check(
                    "threads wait for order-service connections",
                    60,
                    w ->
                        above(
                            w.max(poolPending("order-service")),
                            0,
                            "up to %.0f waiting for a connection"))),
            explain(
                "Latency multiplies inside a transaction: a connection is held for every round trip"
                    + " it makes, so a 100 ms network delay becomes most of a second of pool time"
                    + " per placement. Little's law: 12 placements/s × ~0.8 s = ~10 connections,"
                    + " the whole pool. Every other use of the pool, including cheap reads and the"
                    + " health check, now waits in the same queue. Nothing is broken, it is just"
                    + " full."),
            step(
                MITIGATE,
                "Bulkhead: at most 4 concurrent placements per replica",
                "Turns on the edge bulkhead: a fifth concurrent POST /orders on a replica is refused"
                    + " at once with 503 BULKHEAD_FULL, so placements can never hold more than 4 of"
                    + " the 10 connections.",
                () -> {
                  levers.edge(new Levers.Edge(false, 20, false, 200, true, 4));
                  return "edge bulkhead: 4 concurrent placements per replica";
                },
                3,
                check(
                    "placements are refused by the bulkhead",
                    30,
                    w -> above(w.recent(BULKHEAD_FULL, 5), 0, "%.1f refused/s")),
                check(
                    "read p99 falls below half its peak",
                    90,
                    w -> {
                      var peak = w.peak(READ_P99, OBSERVE);
                      var now = w.latest(READ_P99);
                      return now == null || peak == null
                          ? Observation.unmeasured("read p99")
                          : Observation.of(
                              now < peak / 2, "read p99 %.0f ms, peak %.0f ms", now, peak);
                    })),
            step(
                RECOVER,
                "Heal the link, remove the bulkhead",
                "Removes the latency toxic and turns the edge guards off.",
                () -> {
                  levers.chaos().heal("order-db");
                  levers.edge(Levers.Edge.OFF);
                  return "order-db healed, edge guards off";
                },
                3),
            verify(
                check(
                    "read p99 back under 3 × baseline + 100 ms",
                    90,
                    w -> nearBaseline(w, READ_P99, 3, 100, "read p99")),
                check(
                    "write p99 back under 3 × baseline + 200 ms",
                    90,
                    w -> nearBaseline(w, WRITE_P99, 3, 200, "write p99")),
                check(
                    "no failed requests for 5 s",
                    60,
                    w ->
                        Observation.of(
                            w.recentSum(FAILED, 5) == 0 && w.seconds() >= 5,
                            "%.0f failed in the last 5 s",
                            w.recentSum(FAILED, 5))))));
  }

  private Experiment retryStorm() {
    var load = new Profile(6, 2000, 0, "immediate", 8, 1000);
    return new Experiment(
        "retry-storm",
        "Database outage: a retry storm, then backoff with jitter and a retry budget",
        "The order-service → PostgreSQL link is cut while clients retry immediately, up to 8 times.",
        List.of("connection loss", "retry storm", "exponential backoff", "jitter", "retry budget"),
        true,
        load,
        List.of(
            hypothesis(
                load,
                "Clients give up after 1 s and retry at once, up to 8 times. When the database"
                    + " link goes down, placements hang waiting for a connection, every attempt"
                    + " times out, and each arriving client becomes 8 requests: the order service"
                    + " should see several times the real demand, with hundreds of requests piled"
                    + " up inside it, exactly when it can serve none. Retrying with exponential"
                    + " backoff, full jitter and a retry budget should bring requests back to"
                    + " about one per client, and recovery should not be a thundering herd."),
            step(
                INJECT,
                "Cut the order database link",
                "Toxiproxy disables order-db: open connections close and new ones are refused.",
                () -> {
                  levers.chaos().inject("order-db", Fault.DOWN, 0, 0);
                  return "order-db: down";
                },
                3),
            observe(
                "Requests multiply while nothing succeeds",
                check(
                    "requests sent ≥ 4 × clients arriving",
                    60,
                    w -> above(w.recent(AMPLIFICATION, 5), 4, "%.1f attempts per client")),
                check(
                    "requests pile up inside order-service (≥ 20 in flight)",
                    60,
                    w -> above(w.max(EDGE_IN_FLIGHT), 19, "up to %.0f in flight"))),
            explain(
                "A retry is extra load sent exactly when the server is least able to take it. With"
                    + " immediate retries the load multiplies by the attempt count, and because"
                    + " the server holds each request until its own timeout (a pool waiting 30 s"
                    + " for a connection), abandoned requests keep consuming threads after the"
                    + " client has left. Backoff spaces a client's attempts; jitter keeps clients"
                    + " that failed together from retrying together; a retry budget (gRPC-style"
                    + " token bucket) stops retrying altogether while most attempts fail."),
            step(
                MITIGATE,
                "Exponential backoff, full jitter, retry budget",
                "Switches the clients to budget mode: waits drawn from 0 … 100 ms × 2^attempt (or"
                    + " around Retry-After), and no retries while the token bucket is below half.",
                () -> {
                  this.load.update(new Profile(6, 2000, 0, "budget", 8, 1000));
                  return "clients: exponential backoff + full jitter + retry budget";
                },
                3,
                check(
                    "requests sent ≤ 1.5 × clients arriving",
                    60,
                    w -> below(w.recent(AMPLIFICATION, 5), 1.5, "%.2f attempts per client")),
                check(
                    "the budget is withholding retries",
                    30,
                    w -> above(w.recent(THROTTLED, 5), 0, "%.1f retries withheld/s"))),
            step(
                RECOVER,
                "Restore the database link",
                "Re-enables order-db. Hikari reconnects and the requests still inside the service"
                    + " finish.",
                () -> {
                  levers.chaos().heal("order-db");
                  return "order-db healed";
                },
                3),
            verify(
                check(
                    "≥ 90 % of clients succeed again",
                    120,
                    w ->
                        ratio(
                            w.recent(SUCCEEDED, 5), w.recent(OFFERED, 5), 0.9, "%.0f %% succeed")),
                check(
                    "requests back to ≤ 1.2 per client",
                    60,
                    w -> below(w.recent(AMPLIFICATION, 5), 1.2, "%.2f attempts per client")),
                check(
                    "order-service drains (≤ 10 in flight)",
                    90,
                    w -> below(w.latest(EDGE_IN_FLIGHT), 10, "%.0f in flight")))));
  }

  private Experiment kafkaPartition() {
    var load = new Profile(5, 300, 0, "jitter", 3, 3000);
    return new Experiment(
        "kafka-partition",
        "Kafka partition: the backlog grows silently; load shedding bounds it",
        "payment-service loses its network to Kafka (packets dropped both ways, no error) while"
            + " orders keep arriving.",
        List.of("network partition", "Kafka connectivity", "async decoupling", "load shedding"),
        true,
        load,
        List.of(
            hypothesis(
                load,
                "Placing an order only needs order-service and its database; payment happens later"
                    + " through Kafka. So when payment-service is partitioned from Kafka the API"
                    + " should look perfectly healthy while payment commands pile up unread and"
                    + " unfinished sagas grow by the arrival rate, until the saga's 30 s step"
                    + " timeout starts cancelling them. Shedding load once too many orders are"
                    + " unfinished should bound that backlog and tell clients the truth"
                    + " immediately (503) instead of accepting orders that will be cancelled."),
            step(
                INJECT,
                "Partition payment-service from Kafka",
                "Toxiproxy timeout toxics on payment-kafka, both directions, with no timeout: bytes"
                    + " are dropped and connections stay open, like a partition. The consumer is"
                    + " left to notice through its own timeouts.",
                () -> {
                  levers.chaos().inject("payment-kafka", Fault.PARTITION, 0, 0);
                  return "payment-kafka: partitioned";
                },
                5),
            observe(
                "Work piles up behind a healthy-looking API",
                check(
                    "payment-service lag passes 100",
                    90,
                    w -> above(w.latest(lag(PAYMENT)), 100, "lag %.0f")),
                check(
                    "unfinished sagas pass 3 × baseline + 50",
                    90,
                    w -> {
                      var base = w.baseline(ACTIVE_SAGAS);
                      var now = w.latest(ACTIVE_SAGAS);
                      return now == null || base == null
                          ? Observation.unmeasured("active sagas")
                          : Observation.of(
                              now > base * 3 + 50, "%.0f unfinished (baseline %.0f)", now, base);
                    }),
                throughout(
                    "…while the API keeps accepting ≥ 80 % of orders over the step",
                    10,
                    w ->
                        ratio(
                            w.recent(SUCCEEDED, w.seconds()),
                            w.recent(OFFERED, w.seconds()),
                            0.8,
                            "%.0f %% accepted"))),
            explain(
                "Asynchronous decoupling hides failures from the caller: the order is accepted"
                    + " because its first step (outbox → Kafka) works; only the next consumer"
                    + " cannot read. Health checks stay green. The only honest signals are lag and"
                    + " the amount of unfinished work. Load shedding uses the second one: when more"
                    + " orders are in progress than the system can finish in time, refusing new"
                    + " ones is kinder than accepting and cancelling them 30 s later."),
            step(
                MITIGATE,
                "Shed load above 80 unfinished orders",
                "Turns on the edge load shedder on both replicas: while more than 80 sagas are"
                    + " unfinished, POST /orders answers 503 LOAD_SHED with Retry-After.",
                () -> {
                  levers.edge(new Levers.Edge(false, 20, true, 80, false, 6));
                  return "edge load shedding above 80 unfinished orders";
                },
                3,
                check(
                    "new orders are being shed",
                    30,
                    w -> above(w.recent(SHED, 5), 0, "%.1f shed/s")),
                check(
                    "unfinished sagas stop growing (≤ their level when shedding began)",
                    90,
                    w -> {
                      var start = w.ago(ACTIVE_SAGAS, w.seconds());
                      var now = w.latest(ACTIVE_SAGAS);
                      return now == null || start == null
                          ? Observation.unmeasured("active sagas")
                          : Observation.of(
                              w.seconds() >= 15 && now <= start,
                              "%.0f unfinished, %.0f when shedding began",
                              now,
                              start);
                    })),
            step(
                RECOVER,
                "Heal the partition",
                "Removes the toxics. The consumer reconnects, rejoins its group and reads the"
                    + " backlog; shedding stays on and lets orders in as the backlog shrinks.",
                () -> {
                  levers.chaos().heal("payment-kafka");
                  return "payment-kafka healed";
                },
                3),
            verify(
                check(
                    "payment-service lag drains to ≤ 5",
                    180,
                    w -> below(w.latest(lag(PAYMENT)), 5, "lag %.0f")),
                check(
                    "shedding stops by itself",
                    120,
                    w ->
                        Observation.of(
                            w.seconds() >= 5 && w.recentSum(SHED, 5) == 0,
                            "%.1f shed in the last 5 s",
                            w.recentSum(SHED, 5))),
                check(
                    "orders complete at ≥ 70 % of the arrival rate",
                    120,
                    w ->
                        ratio(
                            w.recent(COMPLETED, 5),
                            w.recent(OFFERED, 5),
                            0.7,
                            "%.0f %% completed")))));
  }

  private Experiment cdcDegraded() {
    var load = new Profile(5, 300, 0, "jitter", 3, 3000);
    return new Experiment(
        "cdc-degraded",
        "Degraded Debezium: CDC falls behind, then catches up with nothing lost",
        "The link Debezium reads PostgreSQL's WAL through is throttled to a trickle while orders"
            + " keep being written.",
        List.of("degraded CDC", "replication slot", "relay delay", "load shedding"),
        true,
        load,
        List.of(
            hypothesis(
                load,
                "Every outbox row reaches Kafka through Debezium reading the WAL over one"
                    + " replication connection. Throttle that connection below the rate outbox"
                    + " rows are written and the delay between an order's outbox write and its"
                    + " Kafka record should grow steadily, while the API and every consumer look"
                    + " healthy. Nothing should be lost: the replication slot keeps the WAL until"
                    + " Debezium confirms it, so once the link is healed the delay should drain to"
                    + " normal."),
            step(
                INJECT,
                "Throttle the Debezium → PostgreSQL link",
                "Toxiproxy bandwidth toxic on cdc-db: the WAL stream to all four outbox connectors"
                    + " trickles at 4 KB/s.",
                () -> {
                  levers.chaos().inject("cdc-db", Fault.BANDWIDTH, 4, 0);
                  return "cdc-db: 4 KB/s";
                },
                3),
            observe(
                "The relay falls behind",
                check(
                    "outbox → Kafka delay p95 passes 3 s",
                    120,
                    w -> above(w.latest(RELAY_P95), 3000, "relay p95 %.0f ms")),
                check(
                    "…and keeps growing (higher than 5 s ago)",
                    120,
                    w -> {
                      var now = w.latest(RELAY_P95);
                      var before = w.ago(RELAY_P95, 5);
                      return now == null || before == null
                          ? Observation.unmeasured("relay delay")
                          : Observation.of(
                              now > before + 1000, "%.0f ms now, %.0f ms 5 s ago", now, before);
                    }),
                throughout(
                    "…while the API keeps accepting ≥ 80 % of orders over the step",
                    10,
                    w ->
                        ratio(
                            w.recent(SUCCEEDED, w.seconds()),
                            w.recent(OFFERED, w.seconds()),
                            0.8,
                            "%.0f %% accepted"))),
            explain(
                "CDC turns the database's own log into the message stream, so it inherits the"
                    + " log's guarantees: an outbox row is relayed exactly when (and only if) its"
                    + " transaction committed, and nothing is dropped while the reader is slow."
                    + " The cost is visibility: lag lives in the replication slot (WAL PostgreSQL"
                    + " must keep, disk that grows) and in end-to-end latency, not in any error."
                    + " Long delays also push sagas past their 30 s step timeout, and every timeout"
                    + " turns into compensation commands: delay becomes extra work. A fixed rate"
                    + " limit"
                    + " cannot be set right for a link whose capacity you do not know; the number"
                    + " of unfinished orders can. It rises whichever hop is slow, so the load"
                    + " shedder from the Kafka partition experiment protects against this failure"
                    + " too."),
            step(
                MITIGATE,
                "Shed load above 40 unfinished orders",
                "Turns on the edge load shedder on both replicas: while more than 40 orders are"
                    + " unfinished, POST /orders answers 503 LOAD_SHED with Retry-After.",
                () -> {
                  levers.edge(new Levers.Edge(false, 20, true, 40, false, 6));
                  return "edge load shedding above 40 unfinished orders";
                },
                3,
                check(
                    "new orders are being shed",
                    30,
                    w -> above(w.recent(SHED, 5), 0, "%.1f shed/s")),
                check(
                    "unfinished orders stop growing for 20 s (≤ their level when shedding began + 5)",
                    60,
                    w -> {
                      var start = w.ago(ACTIVE_SAGAS, w.seconds());
                      var max = w.max(ACTIVE_SAGAS);
                      return max == null || start == null
                          ? Observation.unmeasured("active sagas")
                          : Observation.of(
                              w.seconds() >= 20 && max <= start + 5,
                              "at most %.0f unfinished, %.0f when shedding began",
                              max,
                              start);
                    })),
            step(
                RECOVER,
                "Heal the CDC link, keep shedding",
                "Removes the bandwidth toxic and leaves the shedder on. The healed link flushes"
                    + " the backlog at once, and sagas that timed out while their commands were"
                    + " stuck now send compensation commands too: lifting the guard now would"
                    + " let new orders queue behind all of that, time out in turn and create yet"
                    + " more work (a metastable failure that outlives its trigger). Shedding"
                    + " admits new orders again by itself as the backlog drains.",
                () -> {
                  levers.chaos().heal("cdc-db");
                  return "cdc-db healed; load shedding stays on";
                },
                3),
            verify(
                check(
                    "relay delay p95 back under 2 s",
                    180,
                    w -> below(w.latest(RELAY_P95), 2000, "relay p95 %.0f ms")),
                throughout(
                    "no dead letters during the run",
                    5,
                    w -> {
                      var dead =
                          w.sumOver(
                              DEAD_LETTERED, INJECT, OBSERVE, EXPLAIN, MITIGATE, RECOVER, VERIFY);
                      return Observation.of(dead == 0, "%.0f dead-lettered", dead);
                    }),
                check(
                    "shedding stops by itself",
                    180,
                    w ->
                        Observation.of(
                            w.seconds() >= 5 && w.recentSum(SHED, 5) == 0,
                            "%.1f shed in the last 5 s",
                            w.recentSum(SHED, 5))),
                check(
                    "unfinished orders drain back under 3 × baseline + 30",
                    180,
                    w -> {
                      var base = w.baseline(ACTIVE_SAGAS);
                      var now = w.latest(ACTIVE_SAGAS);
                      return now == null || base == null
                          ? Observation.unmeasured("active sagas")
                          : Observation.of(
                              now <= base * 3 + 30, "%.0f unfinished (baseline %.0f)", now, base);
                    }))));
  }

  // ---- Step builders -------------------------------------------------------------------------

  private Step hypothesis(Profile profile, String text) {
    return hypothesis(profile, text, () -> "no extra setup");
  }

  /**
   * Resets the lab, applies the experiment's own setup, starts its load and measures a baseline.
   */
  private Step hypothesis(Profile profile, String text, Experiment.Action setup) {
    return new Step(
        HYPOTHESIS,
        "Hypothesis",
        text,
        () -> {
          var failures = levers.resetAll();
          if (!failures.isEmpty()) throw new IllegalStateException("Reset failed: " + failures);
          load.stop();
          var quiet = awaitQuiet();
          var extra = setup.run();
          load.reset();
          load.start(profile);
          return "lab reset; "
              + quiet
              + "; "
              + extra
              + "; load "
              + profile.ratePerSecond()
              + " clients/s — baseline for 15 s";
        },
        List.of(
            check(
                "baseline: ≥ 80 % of clients succeed",
                45,
                w ->
                    w.seconds() < 15
                        ? new Observation(false, "measuring baseline (" + w.seconds() + " s)")
                        : ratio(
                            w.recent(SUCCEEDED, 10),
                            w.recent(OFFERED, 10),
                            0.8,
                            "%.0f %% succeed"))),
        15);
  }

  /**
   * Waits (up to two minutes) until the previous run's work has drained: no consumer lag and few
   * unfinished orders. A baseline taken while an earlier backlog is still being worked off would
   * make every later comparison meaningless.
   */
  private String awaitQuiet() throws InterruptedException {
    long since = System.currentTimeMillis();
    long deadline = since + 120_000;
    while (System.currentTimeMillis() < deadline) {
      var recent = sampler.since(since);
      if (!recent.isEmpty()) {
        var last = recent.getLast();
        long lag =
            last.lag() == null
                ? Long.MAX_VALUE
                : last.lag().values().stream().mapToLong(Long::longValue).sum();
        var active = last.sagas().active();
        if (lag <= 5 && active != null && active <= 20)
          return "system quiet (lag " + lag + ", " + active + " unfinished orders)";
      }
      Thread.sleep(1000);
    }
    throw new IllegalStateException("The previous run's work did not drain within 2 minutes");
  }

  private static Step step(
      Phase phase,
      String title,
      String text,
      Experiment.Action action,
      int settle,
      Check... checks) {
    return new Step(phase, title, text, action, List.of(checks), settle);
  }

  private static Step observe(String title, Check... checks) {
    return new Step(
        OBSERVE,
        title,
        "Measured, not assumed: each claim below is decided from the samples.",
        null,
        List.of(checks),
        0);
  }

  private static Step explain(String text) {
    return new Step(EXPLAIN, "Why", text, null, List.of(), 0);
  }

  private static Step verify(Check... checks) {
    return new Step(
        VERIFY,
        "Verify recovery",
        "The system is back to its baseline, by measurement.",
        null,
        List.of(checks),
        0);
  }

  private static Check check(String claim, int within, Function<Window, Observation> probe) {
    return new Check(claim, within, probe);
  }

  /** A claim about the whole step, judged once the step's other claims are decided. */
  private static Check throughout(
      String claim, int minSeconds, Function<Window, Observation> probe) {
    return new Check(claim, minSeconds, probe, true);
  }

  // ---- Claim helpers -------------------------------------------------------------------------

  static Observation above(Double value, double threshold, String format) {
    return value == null
        ? Observation.unmeasured("value")
        : Observation.of(value > threshold, format, value);
  }

  static Observation below(Double value, double threshold, String format) {
    return value == null
        ? Observation.unmeasured("value")
        : Observation.of(value <= threshold, format, value);
  }

  static Observation ratio(Double part, Double whole, double atLeast, String format) {
    if (part == null || whole == null || whole == 0) return Observation.unmeasured("rate");
    double r = part / whole;
    return Observation.of(r >= atLeast, format, r * 100);
  }

  static Observation nearBaseline(
      Window w, Experiment.Metric m, double factor, double slack, String what) {
    var base = w.baseline(m);
    var now = w.recent(m, 5);
    if (base == null || now == null) return Observation.unmeasured(what);
    return Observation.of(
        now <= base * factor + slack, "%s %.0f ms (baseline %.0f ms)", what, now, base);
  }

  private static String latestBreaker(Window w) {
    var open = w.latest(BREAKER_OPEN);
    var closed = w.latest(BREAKER_CLOSED);
    if (open == null) return "unknown";
    return open == 1 ? "OPEN" : closed != null && closed == 1 ? "CLOSED" : "HALF_OPEN";
  }
}
