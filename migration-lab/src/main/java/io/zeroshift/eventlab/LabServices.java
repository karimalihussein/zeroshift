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
    return http.get().uri(url(service) + path).retrieve().body(JsonNode.class);
  }

  /** The body, or {"error": ...} when the service is down or refuses. */
  public JsonNode tryGet(String service, String path) {
    try {
      return get(service, path);
    } catch (RuntimeException e) {
      return error(e);
    }
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
