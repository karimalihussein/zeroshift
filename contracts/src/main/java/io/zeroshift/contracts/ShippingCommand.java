package io.zeroshift.contracts;

import java.util.UUID;

public sealed interface ShippingCommand extends Message {
  record ScheduleShipment(UUID orderId, String customerId) implements ShippingCommand {}
}
