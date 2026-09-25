package io.zeroshift.contracts;

/** Topic names, declared with their partitions and retention in infra/kafka/topics.sh. */
public final class Topics {
  public static final String ORDER_EVENTS = "order.events";
  public static final String PAYMENT_COMMANDS = "payment.commands";
  public static final String PAYMENT_EVENTS = "payment.events";
  public static final String INVENTORY_COMMANDS = "inventory.commands";
  public static final String INVENTORY_EVENTS = "inventory.events";
  public static final String SHIPPING_COMMANDS = "shipping.commands";
  public static final String SHIPPING_EVENTS = "shipping.events";

  /** The carrier's scan events, consumed by shipping-service's tracking projection. */
  public static final String CARRIER_SCANS = "shipping.carrier-scans";

  /** Where a consumer parks a record it cannot process. */
  public static String deadLetter(String topic) {
    return topic + ".dlt";
  }

  private Topics() {}
}
