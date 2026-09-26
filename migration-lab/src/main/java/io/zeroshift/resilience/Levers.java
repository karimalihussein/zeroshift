package io.zeroshift.resilience;

import io.zeroshift.eventlab.EventLabSettings;
import io.zeroshift.eventlab.LabServices;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * Every change the resilience lab makes to the running system, through the services' own lab
 * endpoints and Toxiproxy. The UI's controls and the experiments use the same levers, and each
 * returns what the service reports back.
 */
@Component
public class Levers {
  public static final String PAYMENT = "payment-service";
  public static final List<String> ORDER_REPLICAS = List.of("order-service", "order-service-b");
  static final List<String> CONSUMER_SERVICES =
      List.of("order-service", "payment-service", "inventory-service", "shipping-service");

  /** Edge guards; see order-service's EdgeGuards. */
  public record Edge(
      boolean rateLimit,
      int ratePerSecond,
      boolean shedding,
      int maxActiveSagas,
      boolean bulkhead,
      int maxConcurrent) {
    public static final Edge OFF = new Edge(false, 20, false, 200, false, 6);
  }

  /** Payment → gateway call policy; see payment-service's GatewayPolicy. */
  public record Policy(
      int timeoutMs, String retry, int maxAttempts, boolean breaker, boolean pauseOnOpen) {
    public static final Policy DEFAULT = new Policy(1500, "exponential", 3, true, false);
  }

  private final LabServices services;
  private final EventLabSettings settings;
  private final Toxiproxy chaos;

  public Levers(LabServices services, EventLabSettings settings, Toxiproxy chaos) {
    this.services = services;
    this.settings = settings;
    this.chaos = chaos;
  }

  public Toxiproxy chaos() {
    return chaos;
  }

  /** The same guard settings on both replicas (each enforces its own share). */
  public Map<String, JsonNode> edge(Edge edge) {
    var result = new LinkedHashMap<String, JsonNode>();
    for (var r : ORDER_REPLICAS) result.put(r, send(r, "/lab/edge", "PUT", edge));
    return result;
  }

  public JsonNode policy(Policy policy) {
    return send(PAYMENT, "/lab/gateway/policy", "PUT", policy);
  }

  /** healthy, slow (3 s), down (503) or declining. */
  public JsonNode gateway(String mode) {
    return services.post(PAYMENT, "/lab/gateway/" + mode, null);
  }

  public JsonNode resetBreaker() {
    return services.post(PAYMENT, "/lab/gateway/breaker/reset", null);
  }

  /** A consumer that takes {@code millis} per delivery. */
  public JsonNode slowConsumer(String service, int millis) {
    return send(service, "/lab/faults/slow-processing?mode=" + millis, "PUT", null);
  }

  public JsonNode clearFault(String service, String fault) {
    return send(service, "/lab/faults/" + fault, "DELETE", null);
  }

  public JsonNode consumerConfig(
      String service, String consumer, Integer maxPollRecords, Integer maxPollIntervalMs) {
    var query = new ArrayList<String>();
    if (maxPollRecords != null) query.add("maxPollRecords=" + maxPollRecords);
    if (maxPollIntervalMs != null) query.add("maxPollIntervalMs=" + maxPollIntervalMs);
    return send(
        service,
        "/lab/consumers/"
            + consumer
            + "/config"
            + (query.isEmpty() ? "" : "?" + String.join("&", query)),
        "PUT",
        null);
  }

  /** pause or resume: pausing keeps the consumer in its group, it just stops fetching. */
  public JsonNode consumer(String service, String consumer, String action) {
    return services.post(service, "/lab/consumers/" + consumer + "/" + action, null);
  }

  /**
   * Back to how the stack starts: every link healed, guards off, the default gateway policy with a
   * healthy gateway and a closed breaker, no slow consumers, payment consumer at default settings
   * and running. Each step is attempted even if an earlier one failed; the failures are returned.
   */
  public List<String> resetAll() {
    var failures = new ArrayList<String>();
    attempt(failures, "heal links", chaos::healAll);
    attempt(failures, "edge guards off", () -> edge(Edge.OFF));
    attempt(failures, "gateway healthy", () -> gateway("healthy"));
    attempt(failures, "default gateway policy", () -> policy(Policy.DEFAULT));
    attempt(failures, "breaker reset", this::resetBreaker);
    for (var s : CONSUMER_SERVICES)
      attempt(failures, "clear slow consumer on " + s, () -> clearFault(s, "slow-processing"));
    attempt(
        failures, "payment consumer defaults", () -> consumerConfig(PAYMENT, PAYMENT, null, null));
    attempt(failures, "payment consumer running", () -> consumer(PAYMENT, PAYMENT, "resume"));
    return failures;
  }

  private static void attempt(List<String> failures, String what, Runnable step) {
    try {
      step.run();
    } catch (RuntimeException e) {
      failures.add(what + ": " + e.getMessage());
    }
  }

  private JsonNode send(String service, String path, String method, Object body) {
    var base = settings.services().get(service);
    if (base == null) throw new LabServices.ActionFailed("Unknown service " + service);
    return services.send(base + path, method, body);
  }
}
