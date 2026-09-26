package io.zeroshift.resilience;

import io.zeroshift.eventlab.EventLabSettings;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

/**
 * Takes one {@link Sample} a second, just after the second ends, and keeps the last 15 minutes.
 * Every service is asked in parallel with a short deadline: a service that is down or stuck shows
 * as unreachable in that second instead of stalling the timeline. It only samples while someone is
 * watching (the page polled in the last minute) or something is running (load, an experiment), so
 * an idle lab costs nothing.
 */
@Component
public class Sampler implements SmartLifecycle {
  static final int KEEP = 900;
  static final List<String> SERVICES =
      List.of(
          "order-service",
          "order-service-b",
          "payment-service",
          "inventory-service",
          "shipping-service",
          "order-query-service");
  private static final Logger log = LoggerFactory.getLogger(Sampler.class);

  /** Same unsampled W3C parent as the event lab's polling, so Tempo records none of this. */
  private static final String UNSAMPLED = "00-5a3057b3f1f7a1e0c0ffee0000000002-00f067aa0ba902b7-00";

  private final EventLabSettings settings;
  private final LoadGenerator load;
  private final KafkaProbe kafka;
  private final RestClient http;
  private final ArrayDeque<Sample> samples = new ArrayDeque<>();
  private final Map<String, JsonNode> previous = new ConcurrentHashMap<>();
  private final Map<String, Long> previousAt = new ConcurrentHashMap<>();
  private volatile long watchedUntil;
  private volatile boolean running;
  private volatile java.util.function.BooleanSupplier busy = () -> false;
  private Thread thread;

  public Sampler(EventLabSettings settings, LoadGenerator load, KafkaProbe kafka) {
    this.settings = settings;
    this.load = load;
    this.kafka = kafka;
    var factory =
        new JdkClientHttpRequestFactory(
            HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build());
    factory.setReadTimeout(Duration.ofMillis(800));
    http = RestClient.builder().requestFactory(factory).build();
  }

  /** Something other than the load generator that needs samples (a running experiment). */
  void keepSamplingWhile(java.util.function.BooleanSupplier condition) {
    busy = condition;
  }

  /** The page is open: keep sampling for another minute. */
  public void watched() {
    watchedUntil = System.currentTimeMillis() + 60_000;
  }

  public List<Sample> since(long t) {
    synchronized (samples) {
      return samples.stream().filter(s -> s.t() > t).toList();
    }
  }

  public List<Sample> last(int n) {
    synchronized (samples) {
      var all = new ArrayList<>(samples);
      return all.subList(Math.max(0, all.size() - n), all.size());
    }
  }

  @Override
  public void start() {
    running = true;
    thread = Thread.ofPlatform().name("resilience-sampler").daemon().start(this::loop);
  }

  private void loop() {
    while (running) {
      // 150 ms after each second boundary, so the load generator's last second is complete.
      long now = System.currentTimeMillis();
      long wait = 1000 - now % 1000 + 150;
      try {
        Thread.sleep(wait);
      } catch (InterruptedException e) {
        return;
      }
      if (!(System.currentTimeMillis() < watchedUntil || load.running() || busy.getAsBoolean()))
        continue;
      try {
        var sample = sample(System.currentTimeMillis() / 1000 * 1000);
        synchronized (samples) {
          samples.addLast(sample);
          while (samples.size() > KEEP) samples.removeFirst();
        }
      } catch (RuntimeException e) {
        log.warn("resilience sample failed", e);
      }
    }
  }

  Sample sample(long t) {
    try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
      var pressure = new HashMap<String, Future<JsonNode>>();
      for (var s : SERVICES) pressure.put(s, pool.submit(() -> get(s, "/lab/pressure")));
      var edgeA = pool.submit(() -> get("order-service", "/lab/edge"));
      var edgeB = pool.submit(() -> get("order-service-b", "/lab/edge"));
      var throughput =
          pool.submit(
              () -> {
                var a = get("order-service", "/lab/throughput?seconds=5");
                return a != null ? a : get("order-service-b", "/lab/throughput?seconds=5");
              });
      var gateway = pool.submit(() -> get("payment-service", "/lab/gateway"));
      var paymentState = pool.submit(() -> get("payment-service", "/lab/state"));
      var lag = pool.submit(kafka::lag);

      var unreachable = new ArrayList<String>();
      var readings = new TreeMap<String, JsonNode>();
      for (var e : pressure.entrySet()) {
        var node = await(e.getValue());
        if (node == null) unreachable.add(e.getKey());
        else readings.put(e.getKey(), node);
      }
      var lagReading = await(lag);
      return new Sample(
          t,
          traffic(),
          latency(),
          edge(await(edgeA), await(edgeB)),
          sagas(await(throughput)),
          lagReading == null || lagReading.error() != null ? null : lagReading.byGroup(),
          consumers(readings),
          gateway(await(gateway), await(paymentState)),
          relay(),
          resources(readings),
          unreachable);
    }
  }

  private Sample.Traffic traffic() {
    var s = load.seconds(1).getFirst();
    var outcomes = new TreeMap<String, Integer>();
    s.outcomes().forEach((k, v) -> outcomes.put(k.name(), v));
    return new Sample.Traffic(
        s.arrivals(),
        s.dropped(),
        s.attempts(),
        s.retries(),
        s.succeeded(),
        s.failed(),
        s.throttled(),
        outcomes,
        s.attemptsPerTenth(),
        load.totals().inFlight());
  }

  private Sample.Latency latency() {
    var w = load.percentiles(false, 5);
    var r = load.percentiles(true, 5);
    return new Sample.Latency(w.p50(), w.p95(), w.p99(), r.p50(), r.p95(), r.p99());
  }

  private Sample.Edge edge(JsonNode a, JsonNode b) {
    if (a == null && b == null) return new Sample.Edge(null, null, null, null, null);
    return new Sample.Edge(
        edgeRate(a, b, "admitted"),
        edgeRate(a, b, "rateLimited"),
        edgeRate(a, b, "shed"),
        edgeRate(a, b, "bulkheadFull"),
        (a == null ? 0 : a.path("inFlight").asInt())
            + (b == null ? 0 : b.path("inFlight").asInt()));
  }

  private Double edgeRate(JsonNode a, JsonNode b, String counter) {
    Double total = null;
    for (var pair : List.of(Map.entry("order-service", a), Map.entry("order-service-b", b))) {
      if (pair.getValue() == null) continue;
      var rate =
          rate(
              "edge:" + pair.getKey() + ":" + counter,
              pair.getValue().path("counters").path(counter).asDouble());
      if (rate != null) total = (total == null ? 0 : total) + rate;
    }
    return total;
  }

  private Sample.Sagas sagas(JsonNode throughput) {
    if (throughput == null) return new Sample.Sagas(null, null, null, null, null, null);
    var c = throughput.path("completions");
    double window = Math.max(1, c.path("windowSeconds").asInt(5));
    return new Sample.Sagas(
        c.path("completed").asDouble() / window,
        c.path("cancelled").asDouble() / window,
        throughput.path("active").path("active").asLong(),
        number(c.path("p50Ms")),
        number(c.path("p95Ms")),
        number(c.path("p99Ms")));
  }

  private Sample.Consumers consumers(Map<String, JsonNode> readings) {
    Double processed = null, retries = null, dead = null, duplicates = null, rebalances = null;
    for (var e : readings.entrySet()) {
      var d = e.getValue().path("decisions");
      var s = e.getKey();
      processed =
          add(
              processed,
              rate(
                  s + ":processed", d.path("PROCESSED").asDouble() + d.path("IGNORED").asDouble()));
      retries = add(retries, rate(s + ":retry", d.path("RETRY_SCHEDULED").asDouble()));
      dead = add(dead, rate(s + ":dlt", d.path("DEAD_LETTERED").asDouble()));
      duplicates = add(duplicates, rate(s + ":dup", d.path("DUPLICATE_SKIPPED").asDouble()));
      rebalances =
          add(rebalances, delta(s + ":rebalances", e.getValue().path("rebalances").asDouble()));
    }
    return new Sample.Consumers(processed, retries, dead, duplicates, rebalances);
  }

  private Sample.Gateway gateway(JsonNode gateway, JsonNode paymentState) {
    var attempts = new TreeMap<String, Double>();
    Double charges = null;
    var pressure = previous.get("pressure:payment-service");
    if (pressure != null) {
      var counters = pressure.path("counters");
      for (var name : counters.propertyNames()) {
        if (name.startsWith("zeroshift.gateway.attempts{outcome=")) {
          var outcome =
              name.substring("zeroshift.gateway.attempts{outcome=".length(), name.length() - 1);
          var r = rate("gw:" + outcome, counters.path(name).asDouble());
          if (r != null) attempts.put(outcome, r);
        } else if (name.startsWith("zeroshift.gateway.charges{")) {
          charges = add(charges, rate("gw:" + name, counters.path(name).asDouble()));
        }
      }
    }
    Boolean paused = null;
    if (paymentState != null)
      for (var c : paymentState.path("consumers"))
        if (c.path("id").asString().equals("payment-service"))
          paused = c.path("pauseRequested").asBoolean() || c.path("paused").asBoolean();
    return new Sample.Gateway(
        attempts,
        charges,
        gateway == null ? null : gateway.path("breakerState").asString(null),
        paused,
        gateway == null ? null : gateway.path("policy").path("timeoutMs").asInt(),
        gateway == null ? null : gateway.path("policy").path("retry").asString(null));
  }

  private Sample.Relay relay() {
    var delays = new ArrayList<>(kafka.drainRelayDelays());
    if (delays.isEmpty()) return new Sample.Relay(0, null, null);
    delays.sort(null);
    int index = Math.max(0, (int) Math.ceil(0.95 * delays.size()) - 1);
    return new Sample.Relay(delays.size(), delays.get(index), delays.getLast());
  }

  private Map<String, Sample.Resource> resources(Map<String, JsonNode> readings) {
    var result = new TreeMap<String, Sample.Resource>();
    readings.forEach(
        (service, p) -> {
          var pool = p.path("pool");
          result.put(
              service,
              new Sample.Resource(
                  Math.round(p.path("processCpu").asDouble() * 1000) / 10.0,
                  p.path("heapUsedBytes").asLong() / 1_048_576,
                  p.path("heapMaxBytes").asLong() / 1_048_576,
                  p.path("liveThreads").asInt(),
                  pool.path("active").asInt(),
                  pool.path("pending").asInt(),
                  pool.path("max").asInt()));
        });
    return result;
  }

  private JsonNode get(String service, String path) {
    var base = settings.services().get(service);
    if (base == null) return null;
    try {
      var node =
          http.get()
              .uri(base + path)
              .header("traceparent", UNSAMPLED)
              .retrieve()
              .body(JsonNode.class);
      if (path.equals("/lab/pressure") && node != null) previous.put("pressure:" + service, node);
      return node;
    } catch (RuntimeException e) {
      if (path.equals("/lab/pressure")) previous.remove("pressure:" + service);
      return null;
    }
  }

  /** Per-second rate of a running total since its previous reading; null on the first one. */
  private Double rate(String key, double total) {
    long now = System.nanoTime();
    var before = previousTotals.put(key, total);
    var at = previousAt.put(key, now);
    if (before == null || at == null || total < before) return null; // first reading, or a restart
    double seconds = (now - at) / 1e9;
    return seconds <= 0 ? null : (total - before) / seconds;
  }

  private Double delta(String key, double total) {
    var before = previousTotals.put(key, total);
    return before == null || total < before ? null : total - before;
  }

  private final Map<String, Double> previousTotals = new ConcurrentHashMap<>();

  private static Double add(Double sum, Double value) {
    return value == null ? sum : (sum == null ? 0 : sum) + value;
  }

  private static Double number(JsonNode node) {
    return node == null || node.isNull() || node.isMissingNode() ? null : node.asDouble();
  }

  private static <T> T await(Future<T> f) {
    try {
      return f.get(900, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      f.cancel(true);
      return null;
    }
  }

  @Override
  public void stop() {
    running = false;
    if (thread != null) thread.interrupt();
  }

  @Override
  public boolean isRunning() {
    return running;
  }
}
