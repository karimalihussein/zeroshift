package io.zeroshift.resilience;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.zeroshift.eventlab.EventLabSettings;
import io.zeroshift.eventlab.LabServices;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.resilience.LoadGenerator.Outcome;
import io.zeroshift.resilience.LoadGenerator.Profile;
import io.zeroshift.resilience.LoadGenerator.RetryMode;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The generator against a real HTTP server standing in for both order-service replicas. */
class LoadGeneratorTest {
  private HttpServer server;
  private LoadGenerator load;
  private final AtomicInteger placements = new AtomicInteger();
  private final Map<String, Integer> attemptsByKey = new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<String> restocked = new CopyOnWriteArrayList<>();
  private volatile int refuseFirst; // answer 503 LOAD_SHED to this many attempts per key
  private volatile long delayMs;

  @BeforeEach
  void start() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.createContext(
        "/orders",
        exchange -> {
          var key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
          int attempt = attemptsByKey.merge(key, 1, Integer::sum);
          try {
            Thread.sleep(delayMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          if (attempt <= refuseFirst) {
            var body = "{\"code\":\"LOAD_SHED\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Retry-After", "0");
            exchange.sendResponseHeaders(503, body.length);
            exchange.getResponseBody().write(body);
          } else {
            placements.incrementAndGet();
            var body =
                ("{\"orderId\":\"" + UUID.randomUUID() + "\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(202, body.length);
            exchange.getResponseBody().write(body);
          }
          exchange.close();
        });
    server.createContext(
        "/lab/stock",
        exchange -> {
          restocked.add(exchange.getRequestURI().getPath());
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, 2);
          exchange.getResponseBody().write("{}".getBytes(StandardCharsets.UTF_8));
          exchange.close();
        });
    server.start();
    var url = "http://127.0.0.1:" + server.getAddress().getPort();
    var settings =
        new EventLabSettings(
            false,
            "unused:9092",
            "http://unused",
            Map.of("order-service", url, "order-service-b", url, "inventory-service", url),
            "");
    load = new LoadGenerator(settings, new LabServices(settings), new SimpleMeterRegistry());
  }

  @AfterEach
  void stop() {
    load.stop();
    server.stop(0);
  }

  @Test
  void placesOrdersAtTheConfiguredRateAfterRestocking() {
    load.start(new Profile(20, 50, 0, "none", 1, 2000));
    await().atMost(Duration.ofSeconds(5)).until(() -> load.totals().arrivals() >= 40);
    load.stop();
    await().until(() -> load.totals().inFlight() == 0);
    var totals = load.totals();

    assertThat(restocked).hasSize(LoadGenerator.SKUS.size());
    assertThat(totals.succeeded()).isEqualTo(totals.arrivals()).isEqualTo(placements.get());
    assertThat(totals.retries()).isZero();
    // Percentiles cover complete seconds only: the current one (≤ 20 requests) is still filling.
    assertThat(load.percentiles(false, 30).count())
        .isBetween((int) totals.succeeded() - 20, (int) totals.succeeded());
  }

  @Test
  void aClientRetriesWithTheSameIdempotencyKeyUntilAdmitted() {
    refuseFirst = 2;
    load.start(new Profile(10, 50, 0, "exponential", 5, 2000));
    await().atMost(Duration.ofSeconds(5)).until(() -> load.totals().succeeded() >= 5);
    load.stop();
    await().until(() -> load.totals().inFlight() == 0);
    var totals = load.totals();

    // Every admitted client got there on its third attempt, with one key; a client caught
    // mid-retry by stop() gives up with fewer.
    assertThat(attemptsByKey.values()).allMatch(n -> n <= 3);
    assertThat(attemptsByKey.values().stream().filter(n -> n == 3).count())
        .isEqualTo(totals.succeeded());
    assertThat(placements.get()).isEqualTo((int) totals.succeeded());
    assertThat(totals.outcomes().get(Outcome.SHED)).isGreaterThanOrEqualTo(2 * totals.succeeded());
  }

  @Test
  void arrivalsBeyondTheConcurrencyCapAreCountedNotQueued() {
    delayMs = 1500;
    load.start(new Profile(20, 5, 0, "none", 1, 5000));
    await().atMost(Duration.ofSeconds(5)).until(() -> load.totals().arrivals() >= 20);
    var totals = load.totals();
    load.stop();

    assertThat(totals.inFlight()).isLessThanOrEqualTo(5);
    assertThat(totals.dropped()).isGreaterThanOrEqualTo(totals.arrivals() - 5 - 1);
  }

  @Test
  void backoffSchedules() {
    assertThat(LoadGenerator.backoff(RetryMode.IMMEDIATE, 3, 2)).isZero();
    assertThat(LoadGenerator.backoff(RetryMode.EXPONENTIAL, 1, null)).isEqualTo(100);
    assertThat(LoadGenerator.backoff(RetryMode.EXPONENTIAL, 4, null)).isEqualTo(800);
    assertThat(LoadGenerator.backoff(RetryMode.EXPONENTIAL, 20, null)).isEqualTo(5000);
    assertThat(LoadGenerator.backoff(RetryMode.EXPONENTIAL, 1, 2)).isEqualTo(2000);
    for (int i = 0; i < 200; i++) {
      assertThat(LoadGenerator.backoff(RetryMode.JITTER, 3, null)).isBetween(0L, 400L);
      assertThat(LoadGenerator.backoff(RetryMode.BUDGET, 1, 2)).isBetween(1000L, 3000L);
    }
  }

  @Test
  void theRetryBudgetClosesWhileMostAttemptsFailAndReopensOnSuccess() {
    var throttle = new LoadGenerator.RetryThrottle();
    assertThat(throttle.allowRetry()).isTrue();
    for (int i = 0; i < 5; i++) throttle.onFailure();
    assertThat(throttle.allowRetry()).isFalse(); // 5 tokens left: not more than half
    for (int i = 0; i < 10; i++) throttle.onSuccess();
    assertThat(throttle.allowRetry()).isTrue();
  }

  @Test
  void nearestRankPercentiles() {
    var list = new LoadGenerator.IntList();
    for (int i = 1; i <= 100; i++) list.add(i);
    var p = list.percentiles();
    assertThat(p.count()).isEqualTo(100);
    assertThat(p.p50()).isEqualTo(50);
    assertThat(p.p95()).isEqualTo(95);
    assertThat(p.p99()).isEqualTo(99);
    assertThat(p.max()).isEqualTo(100);
    assertThat(new LoadGenerator.IntList().percentiles().p99()).isNull();
  }

  @Test
  void rejectsProfilesOutsideTheLabsLimits() {
    assertThatThrownBy(() -> new Profile(0, 10, 0, "none", 1, 1000))
        .isInstanceOf(ApiException.class);
    assertThatThrownBy(() -> new Profile(10, 10, 0, "sometimes", 1, 1000))
        .isInstanceOf(ApiException.class);
    assertThatThrownBy(() -> new Profile(10, 10, 101, "none", 1, 1000))
        .isInstanceOf(ApiException.class);
  }

  @Test
  void generatedOrdersUseOnlyRestockedSkus() {
    for (int i = 0; i < 200; i++)
      assertThat(load.order()).contains("\"customerId\":\"load-").doesNotContain("SKU-MONITOR");
  }
}
