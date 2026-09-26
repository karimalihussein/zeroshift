package io.zeroshift.contracts;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * The registry of every message type: its wire name, topic and current schema version, plus the
 * upcasters that turn older payloads into the current shape. Adding a field is backward compatible
 * (readers ignore what they do not know); renaming or re-meaning one needs a version bump and an
 * upcaster here.
 */
public final class Contracts {
  public record Contract(
      String type,
      Class<? extends Message> javaType,
      String topic,
      int version,
      Map<Integer, UnaryOperator<ObjectNode>> upcasters,
      Map<Integer, UnaryOperator<ObjectNode>> downcasters) {}

  private static final Map<Class<? extends Message>, String> FAMILIES =
      Map.of(
          OrderEvent.class, Topics.ORDER_EVENTS,
          PaymentCommand.class, Topics.PAYMENT_COMMANDS,
          PaymentEvent.class, Topics.PAYMENT_EVENTS,
          InventoryCommand.class, Topics.INVENTORY_COMMANDS,
          InventoryEvent.class, Topics.INVENTORY_EVENTS,
          ShippingCommand.class, Topics.SHIPPING_COMMANDS,
          ShippingEvent.class, Topics.SHIPPING_EVENTS,
          CarrierEvent.class, Topics.CARRIER_SCANS);

  /** OrderPlaced's additions from the commerce model (ADR 021): absent in older payloads. */
  private static final List<String> COMMERCE_FIELDS =
      List.of(
          "customerName", "subtotal", "discount", "taxRate", "tax", "voucherCode", "invoiceNumber");

  private static final List<String> LINE_FIELDS =
      List.of("productId", "name", "subtotal", "discount", "total");

  /** Upcasters keyed by the version they read: entry n turns a vn payload into v(n+1). */
  private static final Map<Class<? extends Message>, Map<Integer, UnaryOperator<ObjectNode>>>
      UPCASTERS = Map.of(OrderEvent.OrderPlaced.class, Map.of(1, v1 -> v1.put("currency", "USD")));

  /**
   * Downcasters keyed by the version they read: entry n turns a vn payload into v(n-1). Only for
   * reproducing what an older writer produced (the event history lab's "previous deployment"): a v1
   * OrderPlaced could only say USD, so a payload in any other currency cannot be written as v1.
   */
  private static final Map<Class<? extends Message>, Map<Integer, UnaryOperator<ObjectNode>>>
      DOWNCASTERS =
          Map.of(
              OrderEvent.OrderPlaced.class,
              Map.of(
                  2,
                  v2 -> {
                    var currency = v2.path("currency").asString("USD");
                    if (!currency.equals("USD"))
                      throw new IllegalArgumentException(
                          "OrderPlaced v1 has no currency field and means USD, not " + currency);
                    v2.remove("currency");
                    // Nor did it know the commerce model's additions: it wrote the total only.
                    COMMERCE_FIELDS.forEach(v2::remove);
                    if (v2.get("lines") instanceof ArrayNode lines)
                      for (var line : lines)
                        if (line instanceof ObjectNode l) LINE_FIELDS.forEach(l::remove);
                    return v2;
                  }));

  private static final Map<String, Contract> BY_TYPE = new LinkedHashMap<>();
  private static final Map<Class<?>, Contract> BY_CLASS = new LinkedHashMap<>();

  static {
    FAMILIES.forEach(
        (family, topic) ->
            Arrays.stream(family.getPermittedSubclasses())
                .map(Contracts::messageType)
                .forEach(
                    type -> {
                      var upcasters = UPCASTERS.getOrDefault(type, Map.of());
                      var contract =
                          new Contract(
                              type.getSimpleName(),
                              type,
                              topic,
                              upcasters.size() + 1,
                              upcasters,
                              DOWNCASTERS.getOrDefault(type, Map.of()));
                      BY_TYPE.put(contract.type(), contract);
                      BY_CLASS.put(type, contract);
                    }));
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends Message> messageType(Class<?> type) {
    return (Class<? extends Message>) type;
  }

  public static Contract of(Class<?> type) {
    var contract = BY_CLASS.get(type);
    if (contract == null) throw new IllegalArgumentException("Not a message type: " + type);
    return contract;
  }

  public static Contract of(String type) {
    var contract = BY_TYPE.get(type);
    if (contract == null) throw new MalformedMessageException("Unknown message type: " + type);
    return contract;
  }

  public static Iterable<Contract> all() {
    return BY_TYPE.values();
  }

  private Contracts() {}
}
