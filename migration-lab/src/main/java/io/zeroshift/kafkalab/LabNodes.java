package io.zeroshift.kafkalab;

import io.zeroshift.platform.web.ApiException;
import java.io.ByteArrayOutputStream;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The lab's Kafka nodes as Docker containers. Failures are real: SIGKILL, a graceful stop (Kafka's
 * controlled shutdown), or a freeze (cgroup pause: the process is alive but does nothing). Only
 * containers labelled as this lab cluster's nodes can be touched.
 */
@Component
public class LabNodes {
  static final String NODE_LABEL = "zeroshift.kafka-lab.node";
  static final String CLUSTER_LABEL = "zeroshift.kafka-lab.cluster";

  public enum Action {
    /** SIGKILL: a crash. No controlled shutdown, leadership moves only once the session expires. */
    KILL("kill"),
    /** SIGTERM: controlled shutdown. Leadership is handed over before the process exits. */
    STOP("stop?t=30"),
    START("start"),
    /** Freezes every process in the container: alive to TCP, silent to Kafka. */
    PAUSE("pause"),
    UNPAUSE("unpause");

    private final String path;

    Action(String path) {
      this.path = path;
    }

    public static Action parse(String value) {
      try {
        return valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new ApiException(
            HttpStatus.BAD_REQUEST,
            "UNKNOWN_NODE_ACTION",
            "A node can be killed, stopped, started, paused or unpaused, not " + value);
      }
    }
  }

  /**
   * One node's container.
   *
   * @param state Docker's state: running, paused, exited, restarting, created
   */
  public record Node(int id, String container, String state, String status) {
    public boolean running() {
      return "running".equals(state);
    }
  }

  private final KafkaLabSettings settings;
  private final RestClient docker;
  private final JsonMapper json = JsonMapper.builder().build();

  public LabNodes(KafkaLabSettings settings) {
    this.settings = settings;
    var factory =
        new JdkClientHttpRequestFactory(
            HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(1))
                .build());
    // A graceful stop waits for Kafka's controlled shutdown.
    factory.setReadTimeout(Duration.ofSeconds(40));
    docker =
        RestClient.builder()
            .requestFactory(factory)
            .baseUrl(settings.configured() ? settings.dockerUrl() : "http://unconfigured")
            .build();
  }

  public List<Node> nodes() {
    var filters =
        json.createObjectNode()
            .set(
                "label",
                json.createArrayNode()
                    .add(NODE_LABEL)
                    .add(CLUSTER_LABEL + "=" + settings.cluster()));
    var found =
        docker
            .get()
            .uri(
                b ->
                    b.path("/containers/json")
                        .queryParam("all", "true")
                        .queryParam("filters", "{filters}")
                        .build(filters.toString()))
            .retrieve()
            .body(JsonNode.class);
    var nodes = new ArrayList<Node>();
    for (var c : found)
      nodes.add(
          new Node(
              Integer.parseInt(c.path("Labels").path(NODE_LABEL).asString()),
              c.path("Names").path(0).asString().replaceFirst("^/", ""),
              c.path("State").asString(),
              c.path("Status").asString()));
    nodes.sort(Comparator.comparingInt(Node::id));
    return nodes;
  }

  public Node node(int id) {
    return nodes().stream()
        .filter(n -> n.id() == id)
        .findFirst()
        .orElseThrow(
            () ->
                new ApiException(HttpStatus.NOT_FOUND, "UNKNOWN_NODE", "No Kafka lab node " + id));
  }

  public void act(int id, Action action) {
    var node = node(id);
    try {
      docker
          .post()
          .uri("/containers/{id}/" + action.path, node.container())
          .retrieve()
          .toBodilessEntity();
    } catch (RestClientException e) {
      throw new ApiException(
          HttpStatus.CONFLICT,
          "NODE_ACTION_REFUSED",
          "Docker refused to "
              + action.name().toLowerCase(Locale.ROOT)
              + " node "
              + id
              + ": "
              + e.getMessage());
    }
  }

  /**
   * The node's own log lines since {@code since} that contain every one of {@code parts}: what the
   * broker says it did (a truncation, an election), not what the lab infers.
   */
  public List<String> logLines(int id, Instant since, String... parts) {
    byte[] raw =
        docker
            .get()
            .uri(
                "/containers/{id}/logs?stdout=true&stderr=true&since={since}",
                node(id).container(),
                since.getEpochSecond())
            .retrieve()
            .body(byte[].class);
    var lines = new ArrayList<String>();
    for (var line : demultiplex(raw).split("\n")) {
      boolean all = true;
      for (var part : parts) all &= line.contains(part);
      if (all && !line.isBlank()) lines.add(line.strip());
    }
    return lines;
  }

  /** Docker frames a non-TTY container's output: 1 byte stream, 3 zero bytes, 4 bytes length. */
  static String demultiplex(byte[] raw) {
    if (raw == null) return "";
    var out = new ByteArrayOutputStream();
    var buffer = ByteBuffer.wrap(raw);
    while (buffer.remaining() >= 8) {
      int stream = buffer.get(buffer.position());
      if (stream < 0 || stream > 2) return new String(raw, StandardCharsets.UTF_8); // not framed
      buffer.position(buffer.position() + 4);
      int length = buffer.getInt();
      length = Math.min(length, buffer.remaining());
      out.write(raw, buffer.position(), length);
      buffer.position(buffer.position() + length);
    }
    return out.toString(StandardCharsets.UTF_8);
  }
}
