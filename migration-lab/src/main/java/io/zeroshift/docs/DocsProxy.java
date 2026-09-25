package io.zeroshift.docs;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.FORBIDDEN;

import io.zeroshift.docs.Model.Operation;
import io.zeroshift.docs.Model.Portal;
import io.zeroshift.eventlab.EventLabSettings;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;

/**
 * Sends a Try it call only for an operation the catalog marks safe, and only to a configured lab
 * base URL or back to this control plane.
 */
@Component
public class DocsProxy {
  private static final int MAX_BODY = 16_384;
  private static final int MAX_RESPONSE = 262_144;
  private static final Set<String> HEADERS =
      Set.of("content-type", "accept", "x-request-id", "idempotency-key");

  private final Portal portal;
  private final EventLabSettings settings;
  private final RestClient http;

  public DocsProxy(Portal portal, EventLabSettings settings) {
    this.portal = portal;
    this.settings = settings;
    var factory =
        new JdkClientHttpRequestFactory(
            java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofMillis(800)).build());
    factory.setReadTimeout(Duration.ofSeconds(4));
    this.http = RestClient.builder().requestFactory(factory).build();
  }

  public TryResult execute(TryCall call, String origin) {
    var operation = find(call.operation());
    if (!operation.tryIt())
      throw new ResponseStatusException(
          FORBIDDEN, "This operation is documented and not executable from the portal");
    var base = base(operation, call.host(), origin);
    var path = fill(operation.path(), call.pathParams());
    var query = query(operation, call.query());
    var uri = URI.create(base + path + query);
    if (!uri.getScheme().equals("http") && !uri.getScheme().equals("https"))
      throw new ResponseStatusException(BAD_REQUEST, "Only http(s) lab URLs can be called");
    var body = body(operation, call.body());
    var started = System.nanoTime();
    try {
      var spec =
          http.method(HttpMethod.valueOf(operation.method()))
              .uri(uri)
              .headers(headers -> apply(headers, call.headers(), operation, body != null));
      if (body != null) spec.body(body);
      return spec.exchange(
          (request, response) -> {
            var bytes = response.getBody().readNBytes(MAX_RESPONSE + 1);
            var truncated = bytes.length > MAX_RESPONSE;
            var text =
                new String(
                    truncated ? java.util.Arrays.copyOf(bytes, MAX_RESPONSE) : bytes,
                    StandardCharsets.UTF_8);
            if (truncated) text = text + "\n… truncated";
            var headers = new LinkedHashMap<String, String>();
            response
                .getHeaders()
                .forEach((name, values) -> headers.put(name, String.join(", ", values)));
            return new TryResult(
                response.getStatusCode().value(),
                elapsed(started),
                headers,
                text,
                headers.get("X-Request-Id"),
                null);
          });
    } catch (RestClientException e) {
      return new TryResult(0, elapsed(started), Map.of(), "", null, root(e));
    }
  }

  private Operation find(String id) {
    return portal.operations().stream()
        .filter(operation -> operation.id().equals(id))
        .findFirst()
        .orElseThrow(() -> new ResponseStatusException(BAD_REQUEST, "Unknown operation"));
  }

  private String base(Operation operation, String host, String origin) {
    if (operation.hosts().isEmpty()) return trim(origin);
    var chosen = host == null || host.isBlank() ? operation.hosts().get(0) : host;
    if (!operation.hosts().contains(chosen))
      throw new ResponseStatusException(
          BAD_REQUEST, "That host is not one of this operation's services");
    var configured = settings.services().get(chosen);
    if (configured == null || configured.isBlank())
      throw new ResponseStatusException(BAD_REQUEST, "No URL is configured for " + chosen);
    return trim(configured);
  }

  private static String trim(String base) {
    return base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
  }

  private static String fill(String template, Map<String, String> values) {
    var path = template;
    if (values != null) {
      for (var entry : values.entrySet()) {
        var value = entry.getValue() == null ? "" : entry.getValue();
        if (value.contains("/") || value.contains("\\") || value.contains(".."))
          throw new ResponseStatusException(BAD_REQUEST, "Path parameters stay inside one segment");
        path =
            path.replace(
                "{" + entry.getKey() + "}", URLEncoder.encode(value, StandardCharsets.UTF_8));
      }
    }
    if (path.contains("{") || path.contains("}"))
      throw new ResponseStatusException(BAD_REQUEST, "A path parameter is missing");
    return path;
  }

  private static String query(Operation operation, Map<String, String> values) {
    if (values == null || values.isEmpty()) return "";
    var allowed = operation.query().stream().map(param -> param.name()).toList();
    var pairs = new StringBuilder();
    for (var entry : values.entrySet()) {
      if (entry.getValue() == null || entry.getValue().isBlank()) continue;
      if (!allowed.contains(entry.getKey()))
        throw new ResponseStatusException(
            BAD_REQUEST, "Unexpected query parameter " + entry.getKey());
      if (!pairs.isEmpty()) pairs.append('&');
      pairs.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
      pairs.append('=');
      pairs.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
    }
    return pairs.isEmpty() ? "" : "?" + pairs;
  }

  private static String body(Operation operation, String body) {
    if (operation.request() == null) return null;
    if (body == null || body.isBlank()) return null;
    if (body.length() > MAX_BODY)
      throw new ResponseStatusException(BAD_REQUEST, "The body is limited to 16 KB");
    return body;
  }

  private static void apply(
      org.springframework.http.HttpHeaders target,
      Map<String, String> incoming,
      Operation operation,
      boolean hasBody) {
    if (incoming != null) {
      incoming.forEach(
          (name, value) -> {
            if (value == null || value.isBlank()) return;
            if (!HEADERS.contains(name.toLowerCase(Locale.ROOT))) return;
            target.add(name, value);
          });
    }
    if (hasBody && !target.containsHeader("Content-Type"))
      target.set("Content-Type", "application/json");
    if (operation.method().equals("GET"))
      target.set("traceparent", "00-5a3057b3f1f7a1e0c0ffee0000000001-00f067aa0ba902b7-00");
  }

  private static long elapsed(long started) {
    return (System.nanoTime() - started) / 1_000_000;
  }

  private static String root(Throwable error) {
    var message = error.getMessage();
    return message == null ? error.getClass().getSimpleName() : message;
  }

  public record TryCall(
      String operation,
      String host,
      Map<String, String> pathParams,
      Map<String, String> query,
      Map<String, String> headers,
      String body) {}

  public record TryResult(
      int status,
      long durationMs,
      Map<String, String> headers,
      String body,
      String requestId,
      String error) {}
}
