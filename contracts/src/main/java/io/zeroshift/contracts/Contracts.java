package io.zeroshift.contracts;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
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
      Map<Integer, UnaryOperator<ObjectNode>> upcasters) {}

  private static final Map<Class<? extends Message>, String> FAMILIES =
      Map.of(
          OrderEvent.class, Topics.ORDER_EVENTS,
          PaymentCommand.class, Topics.PAYMENT_COMMANDS,
          PaymentEvent.class, Topics.PAYMENT_EVENTS,
          InventoryCommand.class, Topics.INVENTORY_COMMANDS,
          InventoryEvent.class, Topics.INVENTORY_EVENTS,
          ShippingCommand.class, Topics.SHIPPING_COMMANDS,
          ShippingEvent.class, Topics.SHIPPING_EVENTS);

  /** Upcasters keyed by the version they read: entry n turns a vn payload into v(n+1). */
  private static final Map<Class<? extends Message>, Map<Integer, UnaryOperator<ObjectNode>>>
      UPCASTERS = Map.of(OrderEvent.OrderPlaced.class, Map.of(1, v1 -> v1.put("currency", "USD")));

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
                              type.getSimpleName(), type, topic, upcasters.size() + 1, upcasters);
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
