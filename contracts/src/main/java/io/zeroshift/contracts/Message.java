package io.zeroshift.contracts;

import java.util.UUID;

/**
 * Every message the services exchange. Each belongs to exactly one topic family, and every one
 * concerns a single order, which is also its Kafka key: all messages about one order land on the
 * same partition and are consumed in the order they were published. (The ordering lab breaks this
 * rule on purpose for carrier scans, to show what it protects.)
 */
public sealed interface Message
    permits OrderEvent,
        PaymentCommand,
        PaymentEvent,
        InventoryCommand,
        InventoryEvent,
        ShippingCommand,
        ShippingEvent,
        CarrierEvent {
  UUID orderId();
}
