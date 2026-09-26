package io.zeroshift.resilience;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.zeroshift.eventlab.EventLabSettings;
import io.zeroshift.eventlab.LabServices;
import io.zeroshift.platform.web.ApiException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Real clients placing real orders. Arrivals are open-loop: a new client arrives every 1/rate
 * seconds whether or not earlier ones got an answer, as users do. A slow system therefore piles up
 * clients instead of quietly slowing the generator down (which would hide the latency it causes).
 * When {@code concurrency} clients are already waiting, a new arrival is counted as dropped rather
 * than queued.
 *
 * <p>Each client sends POST /orders (or, for {@code readPercent} of them, GET /orders/{id} for an
 * order placed earlier) to the two order-service replicas in turn, with a timeout, and on a
 * retryable failure (429, 503, 5xx, timeout, refused connection) retries according to its retry
 * mode. Every write carries an Idempotency-Key, so a retried write never doubles an order.
 */
@Component
public class LoadGenerator {
  /** How a client retries. See {@link #backoff}. */
  public enum RetryMode {
    NONE,
    IMMEDIATE,
    EXPONENTIAL,
    JITTER,
    BUDGET;

    static RetryMode parse(String value) {
      try {
        return valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException | NullPointerException e) {
        throw new ApiException(
            HttpStatus.BAD_REQUEST,
            "UNKNOWN_RETRY_MODE",
            "Client retry is none, immediate, exponential, jitter or budget, not " + value);
      }
    }
  }

  public enum Outcome {
    OK,
    RATE_LIMITED,
    SHED,
    BULKHEAD_FULL,
    SERVER_ERROR,
    CLIENT_ERROR,
    TIMEOUT,
    CONNECTION;

    boolean retryable() {
      return this != OK && this != CLIENT_ERROR;
    }
  }

  public record Profile(
      int ratePerSecond,
      int concurrency,
      int readPercent,
      String retry,
      int maxAttempts,
      int timeoutMs) {
    public static final Profile DEFAULT = new Profile(8, 64, 0, "jitter", 3, 2000);

    public Profile {
      RetryMode.parse(retry);
      if (ratePerSecond < 1
          || ratePerSecond > 500
          || concurrency < 1
          || concurrency > 2000
          || readPercent < 0
          || readPercent > 100
          || maxAttempts < 1
          || maxAttempts > 10
          || timeoutMs < 100
          || timeoutMs > 30_000)
        throw new ApiException(
            HttpStatus.BAD_REQUEST,
            "INVALID_LOAD_PROFILE",
            "Rate 1–500/s, concurrency 1–2000, reads 0–100 %, attempts 1–10, timeout 100–30000 ms");
    }

    RetryMode mode() {
      return RetryMode.parse(retry);
    }
  }

  /** Totals since the last reset, and the current state. */
  public record Totals(
      boolean running,
      Instant startedAt,
      Profile profile,
      int inFlight,
      long arrivals,
      long dropped,
      long attempts,
      long retries,
      long succeeded,
      long failed,
      long throttledRetries,
      Map<Outcome, Long> outcomes,
      double retryTokens) {}

  static final List<String> SKUS = List.of("SKU-CABLE", "SKU-MOUSE", "SKU-KEYBOARD");

  private final EventLabSettings settings;
  private final LabServices services;
  private final Stats stats = new Stats();
  private final RetryThrottle throttle = new RetryThrottle();
  private final AtomicInteger inFlight = new AtomicInteger();
  private final AtomicInteger nextTarget = new AtomicInteger();
  private final AtomicLong logical = new AtomicLong();
  private final RecentOrders recent = new RecentOrders(500);
  private final JsonMapper json = JsonMapper.builder().build();
  private final HttpClient http =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .connectTimeout(Duration.ofSeconds(1))
          .build();
  private final MeterRegistry meters;
  private final Map<String, Timer> latency = new java.util.concurrent.ConcurrentHashMap<>();
  private final Counter droppedCounter;

  private volatile Profile profile = Profile.DEFAULT;
  private volatile boolean running;
  private volatile Instant startedAt;
  private Thread arrivals;

  public LoadGenerator(EventLabSettings settings, LabServices services, MeterRegistry meters) {
    this.settings = settings;
    this.services = services;
    this.meters = meters;
    droppedCounter =
        Counter.builder("zeroshift.load.dropped")
            .description("Arrivals turned away because the concurrency cap was reached")
            .register(meters);
  }

  /**
   * Restocks the SKUs the generator orders, then starts arrivals. Restocking is a real inventory
   * delivery: without it, sustained load would mostly measure out-of-stock cancellations.
   */
  public synchronized Totals start(Profile next) {
    profile = next;
    if (running) return totals();
    for (var sku : SKUS)
      services.post("inventory-service", "/lab/stock/" + sku + "/restock?available=1000000", null);
    running = true;
    startedAt = Instant.now();
    arrivals = Thread.ofVirtual().name("load-arrivals").start(this::arrive);
    return totals();
  }

  /** Changes rate, concurrency, mix or retries without stopping. */
  public Totals update(Profile next) {
    profile = next;
    return totals();
  }

  /** Stops arrivals; clients already waiting finish (or give up) on their own. */
  public synchronized Totals stop() {
    running = false;
    if (arrivals != null) arrivals.interrupt();
    return totals();
  }

  public synchronized Totals reset() {
    stop();
    stats.clear();
    throttle.reset();
    return totals();
  }

  public boolean running() {
    return running;
  }

  public Profile profile() {
    return profile;
  }

  public Totals totals() {
    var t = stats.totals();
    return new Totals(
        running,
        startedAt,
        profile,
        inFlight.get(),
        t.arrivals(),
        t.dropped(),
        t.attempts(),
        t.retries(),
        t.succeeded(),
        t.failed(),
        t.throttled(),
        t.outcomes(),
        throttle.tokens());
  }

  /** The last {@code seconds} complete seconds, oldest first. */
  public List<Stats.SecondView> seconds(int seconds) {
    return stats.lastSeconds(seconds);
  }

  public Stats.Percentiles percentiles(boolean reads, int windowSeconds) {
    return stats.percentiles(reads, windowSeconds);
  }

  private void arrive() {
    long next = System.nanoTime();
    while (running) {
      next += 1_000_000_000L / profile.ratePerSecond();
      long now = System.nanoTime();
      // Fallen more than a second behind (a long GC pause): start again from now rather than
      // firing the missed arrivals in one burst.
      if (next < now - 1_000_000_000L) next = now;
      if (next > now) LockSupport.parkNanos(next - now);
      if (!running) break;
      stats.arrival();
      if (inFlight.get() >= profile.concurrency()) {
        stats.dropped();
        droppedCounter.increment();
        continue;
      }
      inFlight.incrementAndGet();
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  client();
                } finally {
                  inFlight.decrementAndGet();
                }
              });
    }
  }

  private void client() {
    var p = profile;
    var mode = p.mode();
    var readId = ThreadLocalRandom.current().nextInt(100) < p.readPercent() ? recent.pick() : null;
    boolean read = readId != null;
    var body = read ? null : order();
    var key = "load-" + UUID.randomUUID();
    long started = System.nanoTime();
    // One in twenty clients is traced end to end; the rest send an unsampled parent, so Tempo
    // keeps a representative sample instead of every order under load.
    boolean traced = logical.incrementAndGet() % 20 == 0;
    int maxAttempts = mode == RetryMode.NONE ? 1 : p.maxAttempts();
    for (int attempt = 1; ; attempt++) {
      var answer = send(read, readId, body, key, traced, p.timeoutMs());
      stats.attempt(attempt > 1, answer.outcome());
      meters
          .counter(
              "zeroshift.load.attempts",
              "kind",
              read ? "read" : "write",
              "outcome",
              answer.outcome().name())
          .increment();
      if (answer.outcome() == Outcome.OK) {
        long ms = (System.nanoTime() - started) / 1_000_000;
        stats.succeeded(read, ms);
        timer(read).record(Duration.ofMillis(ms));
        if (!read && answer.orderId() != null) recent.add(answer.orderId());
        throttle.onSuccess();
        return;
      }
      if (answer.outcome().retryable()) throttle.onFailure();
      if (!answer.outcome().retryable() || attempt >= maxAttempts || !running) {
        stats.failed();
        return;
      }
      if (mode == RetryMode.BUDGET && !throttle.allowRetry()) {
        stats.throttled();
        stats.failed();
        return;
      }
      sleep(backoff(mode, attempt, answer.retryAfterSeconds()));
    }
  }

  /**
   * How long a client waits before retry {@code attempt + 1}.
   *
   * <ul>
   *   <li>{@code IMMEDIATE}: not at all, and ignores Retry-After. Failed clients come straight
   *       back, multiplying the load on whatever is already failing: a retry storm.
   *   <li>{@code EXPONENTIAL}: 100 ms, 200, 400 … capped at 5 s, or the server's Retry-After if
   *       longer. Spreads retries over time, but clients that failed together retry together.
   *   <li>{@code JITTER}, {@code BUDGET}: "full jitter", a random wait between 0 and the
   *       exponential value (or around Retry-After), so synchronized failures come back spread out.
   *       {@code BUDGET} also stops retrying while most recent attempts fail (see {@link
   *       RetryThrottle}).
   * </ul>
   */
  static long backoff(RetryMode mode, int attempt, Integer retryAfterSeconds) {
    var random = ThreadLocalRandom.current();
    long exponential = Math.min(5_000, 100L << Math.min(attempt - 1, 10));
    long floor = retryAfterSeconds == null ? 0 : retryAfterSeconds * 1000L;
    return switch (mode) {
      case NONE, IMMEDIATE -> 0;
      case EXPONENTIAL -> Math.max(exponential, floor);
      case JITTER, BUDGET ->
          floor > 0 ? floor / 2 + random.nextLong(floor + 1) : random.nextLong(exponential + 1);
    };
  }

  record Answer(Outcome outcome, UUID orderId, Integer retryAfterSeconds) {}

  private Answer send(
      boolean read, UUID readId, String body, String key, boolean traced, int timeoutMs) {
    var target = targets().get(Math.floorMod(nextTarget.getAndIncrement(), targets().size()));
    var request =
        HttpRequest.newBuilder(URI.create(target + (read ? "/orders/" + readId : "/orders")))
            .timeout(Duration.ofMillis(timeoutMs));
    if (!traced) request.header("traceparent", unsampledParent());
    if (read) request.GET();
    else
      request
          .header("Content-Type", "application/json")
          .header("Idempotency-Key", key)
          .POST(HttpRequest.BodyPublishers.ofString(body));
    try {
      var response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
      int status = response.statusCode();
      var retryAfter =
          response.headers().firstValue("Retry-After").map(LoadGenerator::seconds).orElse(null);
      if (status / 100 == 2)
        return new Answer(Outcome.OK, read ? readId : orderId(response.body()), null);
      if (status == 429) return new Answer(Outcome.RATE_LIMITED, null, retryAfter);
      if (status == 503 && response.body().contains("LOAD_SHED"))
        return new Answer(Outcome.SHED, null, retryAfter);
      if (status == 503 && response.body().contains("BULKHEAD_FULL"))
        return new Answer(Outcome.BULKHEAD_FULL, null, retryAfter);
      return new Answer(
          status >= 500 ? Outcome.SERVER_ERROR : Outcome.CLIENT_ERROR, null, retryAfter);
    } catch (HttpTimeoutException e) {
      return new Answer(Outcome.TIMEOUT, null, null);
    } catch (IOException e) {
      return new Answer(Outcome.CONNECTION, null, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Answer(Outcome.CONNECTION, null, null);
    }
  }

  private List<String> targets() {
    var s = settings.services();
    return List.of(s.get("order-service"), s.get("order-service-b"));
  }

  /** A realistic basket: one to three distinct items, mostly cheap, now and then a bulk order. */
  String order() {
    var random = ThreadLocalRandom.current();
    var items = new ArrayList<Map<String, Object>>();
    if (random.nextInt(100) < 2) {
      // Over the payment limit: declined by the payment service, a business outcome, not an error.
      items.add(Map.of("sku", "SKU-KEYBOARD", "quantity", 12));
    } else {
      var skus = new ArrayList<>(SKUS);
      java.util.Collections.shuffle(skus, random);
      for (var sku : skus.subList(0, 1 + random.nextInt(skus.size())))
        items.add(Map.of("sku", sku, "quantity", 1 + random.nextInt(3)));
    }
    return json.writeValueAsString(
        Map.of("customerId", "load-" + (1 + random.nextInt(500)), "items", items));
  }

  private UUID orderId(String body) {
    try {
      var id = json.readTree(body).path("orderId").asString(null);
      return id == null ? null : UUID.fromString(id);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private Timer timer(boolean read) {
    return latency.computeIfAbsent(
        read ? "read" : "write",
        kind ->
            Timer.builder("zeroshift.load.latency")
                .description("Client-observed latency of successful requests, retries included")
                .tag("kind", kind)
                .publishPercentileHistogram()
                .register(meters));
  }

  private static Integer seconds(String header) {
    try {
      return Integer.parseInt(header.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** A W3C parent with the sampled flag off: the services' agents record nothing for it. */
  private static String unsampledParent() {
    var random = ThreadLocalRandom.current();
    var hex = HexFormat.of();
    var trace = new byte[16];
    var span = new byte[8];
    random.nextBytes(trace);
    random.nextBytes(span);
    return "00-" + hex.formatHex(trace) + "-" + hex.formatHex(span) + "-00";
  }

  private static void sleep(long millis) {
    if (millis <= 0) return;
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Recently placed order ids, for the read share of the load. */
  static final class RecentOrders {
    private final UUID[] ids;
    private int size;
    private int next;

    RecentOrders(int capacity) {
      ids = new UUID[capacity];
    }

    synchronized void add(UUID id) {
      ids[next] = id;
      next = (next + 1) % ids.length;
      size = Math.min(size + 1, ids.length);
    }

    synchronized UUID pick() {
      return size == 0 ? null : ids[ThreadLocalRandom.current().nextInt(size)];
    }
  }

  /**
   * gRPC's retry throttling: a bucket of 10 tokens; each failed attempt takes one, each success
   * gives back 0.1; retries are allowed only while more than half the bucket is left. When most
   * attempts fail the clients stop retrying altogether, so retries can add at most about 10 % load.
   */
  static final class RetryThrottle {
    static final double MAX = 10;
    private double tokens = MAX;

    synchronized void onFailure() {
      tokens = Math.max(0, tokens - 1);
    }

    synchronized void onSuccess() {
      tokens = Math.min(MAX, tokens + 0.1);
    }

    synchronized boolean allowRetry() {
      return tokens > MAX / 2;
    }

    synchronized double tokens() {
      return tokens;
    }

    synchronized void reset() {
      tokens = MAX;
    }
  }

  /** Per-second counts and latencies, the last 15 minutes of them. */
  public static final class Stats {
    static final int KEEP_SECONDS = 900;

    public record SecondView(
        long at,
        int arrivals,
        int dropped,
        int attempts,
        int retries,
        int succeeded,
        int failed,
        int throttled,
        Map<Outcome, Integer> outcomes,
        int[] attemptsPerTenth) {}

    public record Percentiles(int count, Integer p50, Integer p95, Integer p99, Integer max) {}

    record Total(
        long arrivals,
        long dropped,
        long attempts,
        long retries,
        long succeeded,
        long failed,
        long throttled,
        Map<Outcome, Long> outcomes) {}

    private static final class Second {
      int arrivals, dropped, attempts, retries, succeeded, failed, throttled;
      final int[] outcomes = new int[Outcome.values().length];
      final int[] tenths = new int[10];
      final IntList writes = new IntList();
      final IntList reads = new IntList();
    }

    private final java.util.TreeMap<Long, Second> seconds = new java.util.TreeMap<>();
    private long arrivals, dropped, attempts, retries, succeeded, failed, throttled;
    private final long[] outcomes = new long[Outcome.values().length];

    private Second now() {
      long ms = System.currentTimeMillis();
      var s = seconds.computeIfAbsent(ms / 1000, k -> new Second());
      while (seconds.size() > KEEP_SECONDS) seconds.pollFirstEntry();
      return s;
    }

    synchronized void arrival() {
      now().arrivals++;
      arrivals++;
    }

    synchronized void dropped() {
      now().dropped++;
      dropped++;
    }

    synchronized void attempt(boolean retry, Outcome outcome) {
      var s = now();
      s.attempts++;
      s.tenths[(int) (System.currentTimeMillis() % 1000 / 100)]++;
      s.outcomes[outcome.ordinal()]++;
      attempts++;
      outcomes[outcome.ordinal()]++;
      if (retry) {
        s.retries++;
        retries++;
      }
    }

    synchronized void succeeded(boolean read, long ms) {
      var s = now();
      s.succeeded++;
      succeeded++;
      (read ? s.reads : s.writes).add((int) Math.min(ms, Integer.MAX_VALUE));
    }

    synchronized void failed() {
      now().failed++;
      failed++;
    }

    synchronized void throttled() {
      now().throttled++;
      throttled++;
    }

    synchronized void clear() {
      seconds.clear();
      arrivals = dropped = attempts = retries = succeeded = failed = throttled = 0;
      java.util.Arrays.fill(outcomes, 0);
    }

    synchronized Total totals() {
      var byOutcome = new EnumMap<Outcome, Long>(Outcome.class);
      for (var o : Outcome.values()) byOutcome.put(o, outcomes[o.ordinal()]);
      return new Total(
          arrivals, dropped, attempts, retries, succeeded, failed, throttled, byOutcome);
    }

    /** Complete seconds only: the current one is still filling. */
    synchronized List<SecondView> lastSeconds(int count) {
      long current = System.currentTimeMillis() / 1000;
      var result = new ArrayList<SecondView>();
      for (long at = current - count; at < current; at++) {
        var s = seconds.get(at);
        if (s == null) {
          result.add(new SecondView(at, 0, 0, 0, 0, 0, 0, 0, Map.of(), new int[10]));
          continue;
        }
        var byOutcome = new EnumMap<Outcome, Integer>(Outcome.class);
        for (var o : Outcome.values())
          if (s.outcomes[o.ordinal()] > 0) byOutcome.put(o, s.outcomes[o.ordinal()]);
        result.add(
            new SecondView(
                at,
                s.arrivals,
                s.dropped,
                s.attempts,
                s.retries,
                s.succeeded,
                s.failed,
                s.throttled,
                byOutcome,
                s.tenths.clone()));
      }
      return result;
    }

    /** Over the last {@code window} complete seconds, successful requests only. */
    synchronized Percentiles percentiles(boolean reads, int window) {
      long current = System.currentTimeMillis() / 1000;
      var all = new IntList();
      for (long at = current - window; at < current; at++) {
        var s = seconds.get(at);
        if (s != null) all.addAll(reads ? s.reads : s.writes);
      }
      return all.percentiles();
    }
  }

  /** A growable int array: latencies without boxing. */
  static final class IntList {
    private int[] values = new int[16];
    private int size;

    void add(int v) {
      if (size == values.length) values = java.util.Arrays.copyOf(values, size * 2);
      values[size++] = v;
    }

    void addAll(IntList other) {
      for (int i = 0; i < other.size; i++) add(other.values[i]);
    }

    /** Nearest-rank percentiles. */
    Stats.Percentiles percentiles() {
      if (size == 0) return new Stats.Percentiles(0, null, null, null, null);
      var sorted = java.util.Arrays.copyOf(values, size);
      java.util.Arrays.sort(sorted);
      return new Stats.Percentiles(
          size, rank(sorted, 50), rank(sorted, 95), rank(sorted, 99), sorted[size - 1]);
    }

    private static int rank(int[] sorted, int percentile) {
      int index = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
      return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }
  }
}
