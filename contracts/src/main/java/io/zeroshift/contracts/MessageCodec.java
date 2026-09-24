package io.zeroshift.contracts;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.cfg.JsonNodeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** Envelope ⇄ JSON. Decoding upcasts old schema versions and rejects what it cannot read. */
public final class MessageCodec {
  // Tolerant reader: a producer may add fields before every consumer knows them. Money keeps its
  // exact decimal and scale through the JSON tree used for upcasting (21.00 stays 21.00).
  private static final JsonMapper JSON =
      JsonMapper.builder()
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
          .build();

  public static JsonMapper json() {
    return JSON;
  }

  public static String encode(Envelope envelope) {
    return JSON.writeValueAsString(envelope);
  }

  public static String encodePayload(Message payload) {
    return JSON.writeValueAsString(payload);
  }

  public static Envelope decode(String json) {
    JsonNode root;
    try {
      root = JSON.readTree(json);
    } catch (JacksonException e) {
      throw new MalformedMessageException("Not JSON: " + e.getOriginalMessage(), e);
    }
    if (root == null || !root.isObject()) throw new MalformedMessageException("Not a JSON object");
    var contract = Contracts.of(text(root, "type"));
    int version = root.path("schemaVersion").asInt(1);
    if (version > contract.version())
      throw new MalformedMessageException(
          contract.type()
              + " v"
              + version
              + " is newer than this consumer understands (v"
              + contract.version()
              + ")");
    if (!(root.get("payload") instanceof ObjectNode payload))
      throw new MalformedMessageException(contract.type() + " has no payload object");
    for (int v = version; v < contract.version(); v++)
      payload = contract.upcasters().get(v).apply(payload);
    try {
      return new Envelope(
          UUID.fromString(text(root, "eventId")),
          contract.type(),
          contract.version(),
          uuid(root, "correlationId"),
          uuid(root, "causationId"),
          Instant.parse(text(root, "occurredAt")),
          JSON.treeToValue(payload, contract.javaType()));
    } catch (JacksonException | IllegalArgumentException | java.time.DateTimeException e) {
      throw new MalformedMessageException(contract.type() + " is malformed: " + e.getMessage(), e);
    }
  }

  private static String text(JsonNode root, String field) {
    var value = root.get(field);
    if (value == null || !value.isString())
      throw new MalformedMessageException("Missing field: " + field);
    return value.asString();
  }

  private static UUID uuid(JsonNode root, String field) {
    var value = root.get(field);
    return value == null || value.isNull() ? null : UUID.fromString(value.asString());
  }

  private MessageCodec() {}
}
