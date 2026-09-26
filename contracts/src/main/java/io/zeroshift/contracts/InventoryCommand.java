package io.zeroshift.contracts;

import java.util.List;
import java.util.UUID;

public sealed interface InventoryCommand extends Message {
  record ReserveStock(UUID orderId, List<StockLine> lines) implements InventoryCommand {}

  /** Compensation. Releasing an order with no reservation is a successful no-op. */
  record ReleaseStock(UUID orderId) implements InventoryCommand {}

  /** {@code productId} is null only in commands written before products had ids. */
  record StockLine(UUID productId, String sku, int quantity) {}
}
