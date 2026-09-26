package io.zeroshift.eventlab;

import io.zeroshift.platform.web.ApiHeaders;
import io.zeroshift.platform.web.RequestContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * The learning labs where the control plane itself is the client. It sends real HTTP requests with
 * real timeouts to the services and records exactly what each request got back, so the dashboard
 * shows a client's view next to the system's (orders, charges, read model).
 */
@Component
public class Experiments {
  public static final String IDEMPOTENCY = "idempotency";
  public static final String READ_YOUR_WRITES = "read-your-writes";

  /** How long order-service holds the first answer, and how long the client waits for it. */
  static final int SLOW_RESPONSE_MS = 2500;

  static final int CLIENT_TIMEOUT_MS = 1000;
  static final int MAX_ATTEMPTS = 3;

  private final EventLabSettings settings;
  private final LabServices services;
  private final JdbcTemplate jdbc;
  private final JsonMapper json = JsonMapper.builder().build();
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofMillis(800)).build();

  public Experiments(EventLabSettings settings, LabServices services, JdbcTemplate jdbc) {
    this.settings = settings;
    this.services = services;
    this.jdbc = jdbc;
  }

  /**
   * A client with a 1 s timeout that retries, against an order-service whose first answer comes 2.5
   * s after the order has committed. Without an Idempotency-Key the retry is a second order; with
   * one, the retry gets the first order back.
   */
  public ObjectNode clientRetry(JsonNode order, boolean withKey, int slowAnswers) {
    var body = json.createObjectNode();
    var customer = order.path("customerId").asString("");
    body.put("customerId", customer);
    body.set("items", order.path("items"));
    if (order.hasNonNull("voucherCode") && !order.path("voucherCode").asString().isBlank())
      body.set("voucherCode", order.get("voucherCode"));
    // The customer's orders before the run: whatever appears besides them, this run caused.
    var before = json.createArrayNode();
    for (var existing : customerOrders(customer))
      before.add(existing.path("order").path("id").asString());
    services.send(
        url("order-service")
            + "/lab/faults/slow-response?mode="
            + SLOW_RESPONSE_MS
            + "&times="
            + Math.max(1, Math.min(slowAnswers, MAX_ATTEMPTS)),
        "PUT",
        null);
    var key = withKey ? "order-" + UUID.randomUUID() : null;
    var attempts = json.createArrayNode();
    String orderId = null;
    for (int n = 1; n <= MAX_ATTEMPTS && orderId == null; n++) {
      var attempt = attempts.addObject();
      attempt.put("attempt", n);
      attempt.put("at", Instant.now().toString());
      long started = System.nanoTime();
      try {
        var request =
            request(url("order-service") + "/orders")
                .timeout(Duration.ofMillis(CLIENT_TIMEOUT_MS))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
        if (key != null) request.header(ApiHeaders.IDEMPOTENCY_KEY, key);
        var response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        attempt.put("ms", elapsed(started));
        attempt.put("status", response.statusCode());
        if (response.statusCode() == 202) {
          var accepted = json.readTree(response.body());
          orderId = accepted.path("orderId").asString();
          attempt.put("orderId", orderId);
          attempt.put("replayed", accepted.path("replayed").asBoolean());
          attempt.put(
              "outcome",
              accepted.path("replayed").asBoolean()
                  ? "202, the first order replayed"
                  : "202, order created");
        } else attempt.put("outcome", "HTTP " + response.statusCode() + ": " + response.body());
      } catch (HttpTimeoutException timeout) {
        attempt.put("ms", elapsed(started));
        attempt.put(
            "outcome",
            "timed out after "
                + CLIENT_TIMEOUT_MS
                + " ms: the client cannot tell whether the order exists");
      } catch (Exception e) {
        attempt.put("ms", elapsed(started));
        attempt.put("outcome", "failed: " + e.getMessage());
      }
    }
    var result = json.createObjectNode();
    result.put("customerId", customer);
    result.set("ordersBefore", before);
    result.put("withKey", withKey);
    if (key != null) result.put("idempotencyKey", key);
    result.put("clientTimeoutMs", CLIENT_TIMEOUT_MS);
    result.put("serverDelayMs", SLOW_RESPONSE_MS);
    result.put("slowAnswers", slowAnswers);
    result.set("attempts", attempts);
    result.put("clientSawOrder", orderId);
    record(
        IDEMPOTENCY,
        (withKey ? "with key" : "no key") + (slowAnswers > 1 ? ", every answer slow" : ""),
        attempts.size() + " attempts, client saw " + (orderId == null ? "no order" : "one order"),
        result);
    return result;
  }

  /**
   * Writes an order, then reads it back at once: from the read model (naive), from the read model
   * with the write's version as a consistency token, or from the write model.
   */
  public ObjectNode readYourWrites(JsonNode order, String mode) throws Exception {
    var result = json.createObjectNode();
    result.put("mode", mode);
    long started = System.nanoTime();
    var write =
        http.send(
            request(url("order-service") + "/orders")
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(order.toString()))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    if (write.statusCode() != 202)
      throw new LabServices.ActionFailed(
          "Write refused: HTTP " + write.statusCode() + " " + write.body());
    var accepted = json.readTree(write.body());
    var orderId = accepted.path("orderId").asString();
    long version = accepted.path("version").asLong(1);
    var w = result.putObject("write");
    w.put("orderId", orderId);
    w.put("version", version);
    w.put("ms", elapsed(started));
    var readUrl =
        switch (mode) {
          case "token" ->
              url("order-query-service")
                  + "/orders/"
                  + orderId
                  + "?minVersion="
                  + version
                  + "&waitMs=3000";
          case "write-model" -> url("order-service") + "/orders/" + orderId;
          default -> url("order-query-service") + "/orders/" + orderId;
        };
    long readStarted = System.nanoTime();
    var read =
        http.send(
            request(readUrl).timeout(Duration.ofSeconds(5)).build(),
            HttpResponse.BodyHandlers.ofString());
    var r = result.putObject("read");
    r.put(
        "url",
        readUrl
            .replace(url("order-query-service"), "order-query-service")
            .replace(url("order-service"), "order-service"));
    r.put("status", read.statusCode());
    r.put("ms", elapsed(readStarted));
    JsonNode body = read.body().isBlank() ? json.createObjectNode() : json.readTree(read.body());
    // A token read says how long it waited: in headers when served, in the error's context when
    // the read model is still behind (409 READ_MODEL_BEHIND).
    read.headers()
        .firstValue(ApiHeaders.WAITED_MS)
        .map(Long::parseLong)
        .or(() -> number(body.path("context").path("waitedMs")))
        .ifPresent(ms -> r.put("waitedMs", ms));
    read.headers()
        .firstValue(ApiHeaders.PROJECTED_VERSION)
        .map(Long::parseLong)
        .or(() -> number(body.path("context").path("projectedVersion")))
        .ifPresent(v -> r.put("projectedVersion", v));
    var answer = mode.equals("write-model") ? body.path("order") : body;
    r.put("orderStatus", read.statusCode() == 200 ? answer.path("status").asString(null) : null);
    r.put("code", body.path("code").asString(null));
    r.put("detail", body.path("detail").asString(null));
    var verdict =
        read.statusCode() == 200
            ? "Read your write: "
                + answer.path("status").asString()
                + (r.has("waitedMs") ? " after waiting " + r.path("waitedMs").asLong() + " ms" : "")
            : read.statusCode() == 404
                ? "Your own order was not found: the read model had not projected it yet"
                : read.statusCode() == 409
                    ? "Not served stale: the read model is at version "
                        + r.path("projectedVersion").asLong()
                        + ", the write was version "
                        + version
                    : "HTTP " + read.statusCode();
    result.put("verdict", verdict);
    record(READ_YOUR_WRITES, mode, verdict, result);
    return result;
  }

  /** Recent runs per lab, and the orders and charges behind the newest client-retry run. */
  public ObjectNode view() {
    var result = json.createObjectNode();
    for (var lab : new String[] {IDEMPOTENCY, READ_YOUR_WRITES}) {
      var runs = result.putArray(lab);
      for (var row :
          jdbc.queryForList(
              "SELECT id, mode, summary, result::text AS result, at FROM lab_run WHERE lab=? ORDER BY id DESC LIMIT 6",
              lab)) {
        var run = runs.addObject();
        run.put("id", ((Number) row.get("id")).longValue());
        run.put("mode", (String) row.get("mode"));
        run.put("summary", (String) row.get("summary"));
        run.put("at", ((java.sql.Timestamp) row.get("at")).toInstant().toString());
        run.set("result", json.readTree((String) row.get("result")));
      }
    }
    var latest = result.path(IDEMPOTENCY).path(0).path("result");
    if (!latest.isMissingNode())
      result.set(
          "idempotencyOrders",
          ordersOf(latest.path("customerId").asString(), latest.path("ordersBefore")));
    return result;
  }

  private JsonNode customerOrders(String customerId) {
    var found =
        LabServices.data(
            services.tryGetAnyReplica(
                "order-service",
                // RestClient encodes the URI template itself: pass the raw value.
                "/orders?limit=50&customerId=" + customerId));
    return found.isArray() ? found : json.createArrayNode();
  }

  /** What really happened: every order the run created, and what payment-service charged. */
  private ArrayNode ordersOf(String customerId, JsonNode before) {
    var orders = json.createArrayNode();
    var known = new java.util.HashSet<String>();
    before.forEach(id -> known.add(id.asString()));
    for (var summary : customerOrders(customerId)) {
      var orderId = summary.path("order").path("id").asString();
      if (known.contains(orderId)) continue;
      var row = orders.addObject();
      row.put("orderId", orderId);
      row.put("total", summary.path("order").path("total").decimalValue());
      row.put("status", summary.path("order").path("status").asString());
      row.put("saga", summary.path("saga").path("state").asString());
      var calls =
          LabServices.data(
              services.tryGet("payment-service", "/lab/gateway/calls?orderId=" + orderId));
      int charged = 0;
      if (calls.isArray())
        for (var call : calls) if ("CHARGED".equals(call.path("outcome").asString())) charged++;
      row.put("charges", charged);
      var decisions =
          LabServices.data(services.tryGet("payment-service", "/lab/decisions?orderId=" + orderId));
      boolean refunded = false;
      if (decisions.isArray())
        for (var d : decisions)
          refunded |=
              "RefundPayment".equals(d.path("type").asString())
                  && "PROCESSED".equals(d.path("decision").asString());
      row.put("refunded", refunded);
    }
    return orders;
  }

  /** An outbound request that carries the operator action's request id, for the logs. */
  private static HttpRequest.Builder request(String url) {
    var builder = HttpRequest.newBuilder(URI.create(url));
    var requestId = RequestContext.requestId();
    if (requestId != null) builder.header(ApiHeaders.REQUEST_ID, requestId);
    return builder;
  }

  private static java.util.Optional<Long> number(JsonNode node) {
    return node.isNumber() ? java.util.Optional.of(node.asLong()) : java.util.Optional.empty();
  }

  private void record(String lab, String mode, String summary, ObjectNode result) {
    jdbc.update(
        "INSERT INTO lab_run(lab, mode, summary, result) VALUES(?,?,?,?::jsonb)",
        lab,
        mode,
        summary,
        result.toString());
  }

  private String url(String service) {
    return settings.services().get(service);
  }

  private static long elapsed(long started) {
    return (System.nanoTime() - started) / 1_000_000;
  }
}
