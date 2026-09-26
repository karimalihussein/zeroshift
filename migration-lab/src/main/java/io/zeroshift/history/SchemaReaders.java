package io.zeroshift.history;

import io.zeroshift.contracts.Contracts;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.OrderEvent;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * OrderPlaced as read by different deployments, applied to real records from order.events.
 *
 * <ul>
 *   <li><b>v1 reader</b>: the previous release. Knows only v1: no currency (always USD).
 *   <li><b>v2 reader</b>: the deployed code, {@link MessageCodec#decode}: upcasts v1, refuses
 *       anything newer than it understands.
 *   <li><b>v2 without the version check</b>: the same model, but a "tolerant" reader that trusts
 *       the field names instead of the version number.
 *   <li><b>v3 reader</b>: a proposed next release in which money travels as integer minor units
 *       ({@code unitPriceMinor}, {@code totalMinor}), so readers in any language get exact amounts.
 *       It reads v1 and v2 through the production upcaster and then {@link #v2ToV3}.
 * </ul>
 *
 * The v3 contract is not part of the production contracts: nothing reads JSON numbers lossily
 * today, so the change is not justified yet (ADR 020). It exists here to show what a breaking
 * change does to readers that have not been deployed first.
 */
public final class SchemaReaders {
  public static final int PROPOSED = 3;

  /** One reader's verdict on one record. */
  public record Reading(String reader, boolean read, String outcome, JsonNode understood) {}

  static final Map<String, Function<JsonNode, JsonNode>> READERS = new LinkedHashMap<>();

  static {
    READERS.put("v1 reader (previous release)", SchemaReaders::v1Reader);
    READERS.put("v2 reader (deployed)", SchemaReaders::v2Reader);
    READERS.put("v2 without the version check", SchemaReaders::v2Tolerant);
    READERS.put("v3 reader (proposed)", SchemaReaders::v3Reader);
  }

  public static List<Reading> readAll(String record) {
    var json = MessageCodec.json();
    JsonNode root;
    try {
      root = json.readTree(record);
    } catch (RuntimeException e) {
      return READERS.keySet().stream().map(r -> new Reading(r, false, "not JSON", null)).toList();
    }
    return READERS.entrySet().stream()
        .map(
            e -> {
              try {
                return new Reading(e.getKey(), true, "read", e.getValue().apply(root));
              } catch (RuntimeException failure) {
                return new Reading(e.getKey(), false, rootMessage(failure), null);
              }
            })
        .toList();
  }

  /** A v2 OrderPlaced envelope written by the proposed v3 writer instead. */
  public static ObjectNode asV3(ObjectNode envelope) {
    var copy = envelope.deepCopy();
    copy.set("payload", v2ToV3((ObjectNode) copy.get("payload")));
    copy.put("schemaVersion", PROPOSED);
    return copy;
  }

  /** v2 → v3: prices and total become integer cents. Exact, because USD has two decimals. */
  static ObjectNode v2ToV3(ObjectNode v2) {
    var v3 = v2.deepCopy();
    for (var line : v3.withArray("lines")) {
      var l = (ObjectNode) line;
      l.put("unitPriceMinor", minor(l.path("unitPrice").decimalValue()));
      l.remove("unitPrice");
    }
    v3.put("totalMinor", minor(v3.path("total").decimalValue()));
    v3.remove("total");
    return v3;
  }

  private static long minor(BigDecimal amount) {
    return amount.movePointRight(2).longValueExact();
  }

  private static JsonNode v1Reader(JsonNode root) {
    gate(root, 1);
    var payload = root.path("payload");
    for (var line : payload.path("lines"))
      if (!line.path("unitPrice").isNumber())
        throw new IllegalArgumentException("line has no unitPrice");
    if (!payload.path("total").isNumber()) throw new IllegalArgumentException("no total");
    var understood = MessageCodec.json().createObjectNode();
    understood.set("orderId", payload.path("orderId"));
    understood.set("customerId", payload.path("customerId"));
    understood.set("lines", payload.path("lines"));
    understood.set("total", payload.path("total"));
    understood.put("currency", "USD (assumed: v1 has no currency)");
    return understood;
  }

  private static JsonNode v2Reader(JsonNode root) {
    var envelope = MessageCodec.decode(root.toString());
    return MessageCodec.json().valueToTree(envelope.payload());
  }

  private static JsonNode v2Tolerant(JsonNode root) {
    var payload = (ObjectNode) root.path("payload").deepCopy();
    int version = root.path("schemaVersion").asInt(1);
    if (version < 2) payload = Contracts.of("OrderPlaced").upcasters().get(1).apply(payload);
    return MessageCodec.json()
        .valueToTree(MessageCodec.json().treeToValue(payload, OrderEvent.OrderPlaced.class));
  }

  private static JsonNode v3Reader(JsonNode root) {
    int version = gate(root, PROPOSED);
    var payload = (ObjectNode) root.path("payload").deepCopy();
    if (version < 2) payload = Contracts.of("OrderPlaced").upcasters().get(1).apply(payload);
    if (version < 3) payload = v2ToV3(payload);
    for (var line : payload.path("lines"))
      if (!line.path("unitPriceMinor").canConvertToLong())
        throw new IllegalArgumentException("line has no unitPriceMinor");
    return payload;
  }

  private static int gate(JsonNode root, int understands) {
    if (!"OrderPlaced".equals(root.path("type").asString(null)))
      throw new IllegalArgumentException("not an OrderPlaced event");
    int version = root.path("schemaVersion").asInt(1);
    if (version > understands)
      throw new IllegalArgumentException(
          "OrderPlaced v"
              + version
              + " is newer than this reader understands (v"
              + understands
              + ")");
    return version;
  }

  private static String rootMessage(Throwable e) {
    var root = e;
    while (root.getCause() != null && root.getCause() != root) root = root.getCause();
    var message = root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
    return root.getClass().getSimpleName() + ": " + message.lines().findFirst().orElse(message);
  }

  private SchemaReaders() {}
}
