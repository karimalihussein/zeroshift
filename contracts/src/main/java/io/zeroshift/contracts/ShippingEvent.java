package io.zeroshift.contracts;

import java.util.UUID;

public sealed interface ShippingEvent extends Message {
  record ShipmentScheduled(UUID orderId, String trackingNumber, String carrier)
      implements ShippingEvent {}

  record ShipmentFailed(UUID orderId, String reason) implements ShippingEvent {}
}
