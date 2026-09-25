package io.zeroshift.eventlab;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * HTTP to the lab's services and to Kafka Connect, with short timeouts: a crashed service must show
 * up as unreachable within a poll, not hang the dashboard.
 */
@Component
public class LabServices {
  /**
   * Sent with every read: a W3C parent whose sampled flag is off, so each service's OpenTelemetry
   * agent (parent-based sampling) records nothing for the dashboard's once-a-second polling and
   * Tempo keeps only real traffic. Actions (POST) carry no parent and start a normal trace.
   */
  static final String UNSAMPLED = "00-5a3057b3f1f7a1e0c0ffee0000000001-00f067aa0ba902b7-00";

  private final EventLabSettings settings;
  private final RestClient http;
  private final JsonMapper json = JsonMapper.builder().build();

  public LabServices(EventLabSettings settings) {
    this.settings = settings;
    var factory =
        new JdkClientHttpRequestFactory(
            HttpClient.newBuilder().connectTimeout(Duration.ofMillis(800)).build());
    factory.setReadTimeout(Duration.ofSeconds(4));
    http = RestClient.builder().requestFactory(factory).build();
  }

  public Iterable<String> names() {
    return settings.services().keySet();
  }

  public JsonNode get(String service, String path) {
    return http.get()
        .uri(url(service) + path)
        .header("traceparent", UNSAMPLED)
        .retrieve()
        .body(JsonNode.class);
  }

  /** The body, or {"error": ...} when the service is down or refuses. */
  public JsonNode tryGet(String service, String path) {
    try {
      return get(service, path);
    } catch (RuntimeException e) {
      return error(e);
    }
  }

  /**
   * Replicas of a service share its database, so one answer is enough; try the next replica when
   * one is down (crashed on purpose, most likely). Replica names are the service name plus "-b".
   */
  public JsonNode tryGetAnyReplica(String service, String path) {
    var answer = tryGet(service, path);
    var replica = service + "-b";
    return answer.has("error") && settings.services().containsKey(replica)
        ? tryGet(replica, path)
        : answer;
  }

  public static boolean isReplica(String service) {
    return service.endsWith("-b");
  }

  public JsonNode post(String service, String path, Object body) {
    return send(url(service) + path, "POST", body);
  }

  public JsonNode send(String url, String method, Object body) {
    var request = http.method(org.springframework.http.HttpMethod.valueOf(method)).uri(url);
    if (body != null)
      request = request.contentType(org.springframework.http.MediaType.APPLICATION_JSON).body(body);
    try {
      var result = request.retrieve().body(JsonNode.class);
      return result == null ? json.createObjectNode().put("ok", true) : result;
    } catch (RestClientResponseException e) {
      throw new ActionFailed(
          method
              + " "
              + url
              + " → HTTP "
              + e.getStatusCode().value()
              + ": "
              + e.getResponseBodyAsString());
    }
  }

  public JsonNode connect(String path) {
    return http.get().uri(settings.connectUrl() + path).retrieve().body(JsonNode.class);
  }

  public String connectUrl() {
    return settings.connectUrl();
  }

  public ObjectNode error(Exception e) {
    var cause = e;
    while (cause.getCause() instanceof Exception inner) cause = inner;
    return json.createObjectNode()
        .put("error", cause.getClass().getSimpleName() + ": " + cause.getMessage());
  }

  private String url(String service) {
    var url = settings.services().get(service);
    if (url == null) throw new ActionFailed("Unknown service " + service);
    return url;
  }

  public static final class ActionFailed extends RuntimeException {
    public ActionFailed(String message) {
      super(message);
    }
  }
}
