package io.zeroshift.resilience;

import io.zeroshift.platform.web.ApiException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

/**
 * Toxiproxy's HTTP API: the network between a service and what it depends on. A fault is a set of
 * toxics on one proxy (or the proxy switched off); healing removes them all. Everything shown is
 * read back from Toxiproxy, never remembered here.
 */
@Component
public class Toxiproxy {
  /** Names of the proxies in infra/toxiproxy/toxiproxy.json and what each one carries. */
  public static final Map<String, String> LINKS =
      Map.of(
          "order-db", "order-service A and B → PostgreSQL",
          "payment-db", "payment-service → PostgreSQL",
          "payment-kafka", "payment-service → Kafka",
          "cdc-db", "Debezium → PostgreSQL (all outbox connectors)",
          "payment-gateway", "payment-service → card gateway");

  /**
   * What can go wrong on a link.
   *
   * <ul>
   *   <li>{@code LATENCY}: every chunk of the answer is held back {@code value} ms (± jitter).
   *   <li>{@code BANDWIDTH}: answers trickle at {@code value} KB/s.
   *   <li>{@code RESET}: connections are reset by the peer (TCP RST) as soon as data flows.
   *   <li>{@code PARTITION}: data is silently dropped both ways and connections stay open: the
   *       client sees nothing, only its own timeouts.
   *   <li>{@code DOWN}: the proxy stops listening; open connections close, new ones are refused.
   * </ul>
   */
  public enum Fault {
    LATENCY,
    BANDWIDTH,
    RESET,
    PARTITION,
    DOWN;

    public static Fault parse(String value) {
      try {
        return valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException | NullPointerException e) {
        throw new ApiException(
            HttpStatus.BAD_REQUEST,
            "UNKNOWN_FAULT",
            "Faults are latency, bandwidth, reset, partition and down, not " + value);
      }
    }
  }

  public record Toxic(String name, String type, String stream, Map<String, Object> attributes) {}

  public record Link(
      String name,
      String carries,
      String listen,
      String upstream,
      boolean enabled,
      List<Toxic> toxics) {
    public boolean healthy() {
      return enabled && toxics.isEmpty();
    }
  }

  private final ResilienceSettings settings;
  private final RestClient http;

  public Toxiproxy(ResilienceSettings settings) {
    this.settings = settings;
    var factory =
        new JdkClientHttpRequestFactory(
            HttpClient.newBuilder().connectTimeout(Duration.ofMillis(800)).build());
    factory.setReadTimeout(Duration.ofSeconds(3));
    http = RestClient.builder().requestFactory(factory).build();
  }

  public boolean configured() {
    return settings.chaosConfigured();
  }

  public List<Link> links() {
    var answer = http.get().uri(url("/proxies")).retrieve().body(JsonNode.class);
    var links = new ArrayList<Link>();
    // An object keyed by proxy name, not an array.
    for (var entry : answer.properties()) {
      var p = entry.getValue();
      var toxics = new ArrayList<Toxic>();
      for (var t : p.path("toxics"))
        toxics.add(
            new Toxic(
                t.path("name").asString(),
                t.path("type").asString(),
                t.path("stream").asString(),
                attributes(t.path("attributes"))));
      var name = p.path("name").asString();
      links.add(
          new Link(
              name,
              LINKS.getOrDefault(name, ""),
              p.path("listen").asString(),
              p.path("upstream").asString(),
              p.path("enabled").asBoolean(),
              toxics));
    }
    links.sort(Comparator.comparing(Link::name));
    return links;
  }

  /**
   * Replaces whatever is wrong with {@code link} by {@code fault}. {@code value} is the latency in
   * ms or the bandwidth in KB/s; {@code jitter} is ± ms on latency.
   */
  public void inject(String link, Fault fault, int value, int jitter) {
    known(link);
    heal(link);
    switch (fault) {
      case LATENCY ->
          toxic(
              link,
              "lab-latency",
              "latency",
              "downstream",
              Map.of("latency", value, "jitter", jitter));
      case BANDWIDTH ->
          toxic(link, "lab-bandwidth", "bandwidth", "downstream", Map.of("rate", value));
      case RESET -> toxic(link, "lab-reset", "reset_peer", "upstream", Map.of("timeout", 0));
      case PARTITION -> {
        // timeout 0: hold the data forever and never close, which is what a partition looks like.
        toxic(link, "lab-partition-up", "timeout", "upstream", Map.of("timeout", 0));
        toxic(link, "lab-partition-down", "timeout", "downstream", Map.of("timeout", 0));
      }
      case DOWN -> enabled(link, false);
    }
  }

  /** Removes every toxic and switches the proxy back on. */
  public void heal(String link) {
    known(link);
    var current =
        http.get().uri(url("/proxies/" + link + "/toxics")).retrieve().body(JsonNode.class);
    for (var t : current)
      http.delete()
          .uri(url("/proxies/" + link + "/toxics/" + t.path("name").asString()))
          .retrieve()
          .toBodilessEntity();
    enabled(link, true);
  }

  public void healAll() {
    if (!configured()) return;
    for (var link : LINKS.keySet()) heal(link);
  }

  private void toxic(
      String link, String name, String type, String stream, Map<String, Object> attributes) {
    http.post()
        .uri(url("/proxies/" + link + "/toxics"))
        .contentType(MediaType.APPLICATION_JSON)
        .body(
            Map.of(
                "name", name,
                "type", type,
                "stream", stream,
                "toxicity", 1.0,
                "attributes", attributes))
        .retrieve()
        .toBodilessEntity();
  }

  private void enabled(String link, boolean enabled) {
    http.post()
        .uri(url("/proxies/" + link))
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("enabled", enabled))
        .retrieve()
        .toBodilessEntity();
  }

  private static Map<String, Object> attributes(JsonNode node) {
    var result = new java.util.TreeMap<String, Object>();
    node.properties()
        .forEach(
            e ->
                result.put(
                    e.getKey(),
                    e.getValue().isNumber()
                        ? e.getValue().numberValue()
                        : e.getValue().asString()));
    return result;
  }

  private static void known(String link) {
    if (!LINKS.containsKey(link))
      throw new ApiException(
          HttpStatus.NOT_FOUND,
          "UNKNOWN_LINK",
          "No chaos link " + link + "; links are " + LINKS.keySet());
  }

  private String url(String path) {
    if (!configured())
      throw new ApiException(
          HttpStatus.CONFLICT,
          "CHAOS_NOT_RUNNING",
          "Network chaos needs the Toxiproxy overlay: docker compose -f docker-compose.yml"
              + " -f docker-compose.override.yml -f docker-compose.chaos.yml up -d");
    return settings.toxiproxyUrl() + path;
  }
}
