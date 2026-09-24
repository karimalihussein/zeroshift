package io.zeroshift.contracts;

import java.util.UUID;

public sealed interface InventoryEvent extends Message {
  record StockReserved(UUID orderId, UUID reservationId) implements InventoryEvent {}

  record StockRejected(UUID orderId, String reason) implements InventoryEvent {}

  record StockReleased(UUID orderId) implements InventoryEvent {}
}
