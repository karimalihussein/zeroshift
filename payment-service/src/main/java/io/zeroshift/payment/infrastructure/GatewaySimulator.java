package io.zeroshift.payment.infrastructure;

import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ErrorCodes;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.Locale;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Makes the simulated card gateway (WireMock) really healthy, slow, down or declining, through its
 * admin API. The payment service only ever notices through its own calls.
 */
public final class GatewaySimulator {
  public enum Mode {
    HEALTHY,
    SLOW,
    DOWN,
    DECLINING;

    public static Mode parse(String value) {
      try {
        return valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new ApiException(
            HttpStatus.BAD_REQUEST,
            "UNKNOWN_GATEWAY_MODE",
            "Gateway mode is healthy, slow, down or declining, not " + value);
      }
    }

    public String label() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private static final Map<String, Object> CHARGES =
      Map.of("method", "POST", "urlPath", "/charges");

  private final RestClient admin;
  private volatile Mode mode = Mode.HEALTHY;

  public GatewaySimulator(URI gateway) {
    // HTTP/1.1: WireMock's server drops the JDK client's h2c upgrade on a request with a body.
    admin =
        RestClient.builder()
            .baseUrl(gateway.toString())
            .requestFactory(
                new JdkClientHttpRequestFactory(
                    HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()))
            .build();
  }

  public Mode mode() {
    return mode;
  }

  public void set(Mode mode) {
    post("/__admin/mappings/reset", Map.of());
    post("/__admin/settings", Map.of("fixedDelay", mode == Mode.SLOW ? 3000 : 0));
    switch (mode) {
      case DOWN -> stub(Map.of("status", 503, "body", "maintenance"));
      case DECLINING ->
          stub(
              Map.of(
                  "status", 402,
                  "headers", Map.of("Content-Type", "application/json"),
                  "jsonBody", Map.of("error", "card declined by issuer")));
      case HEALTHY, SLOW -> {}
    }
    this.mode = mode;
  }

  private void stub(Map<String, Object> response) {
    post("/__admin/mappings", Map.of("priority", 1, "request", CHARGES, "response", response));
  }

  private void post(String path, Map<String, Object> body) {
    try {
      admin.post().uri(path).body(body).retrieve().toBodilessEntity();
    } catch (RestClientException e) {
      throw new ApiException(
          HttpStatus.BAD_GATEWAY,
          ErrorCodes.BAD_GATEWAY,
          "Gateway admin call failed: " + e.getMessage());
    }
  }
}
