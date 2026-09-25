package io.zeroshift.inventory.application;

import io.zeroshift.contracts.InventoryCommand.StockLine;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface Stock {
  record Item(String sku, int onHand, int reserved, int version) {
    public int available() {
      return onHand - reserved;
    }
  }

  enum Status {
    RESERVED,
    REJECTED,
    RELEASED
  }

  record Reservation(
      UUID orderId, UUID reservationId, Status status, List<StockLine> lines, String reason) {}

  Optional<Item> find(String sku);

  /** Adds {@code delta} to reserved if the row is still at {@code item.version()}. */
  boolean adjust(Item item, int delta);

  Optional<Reservation> reservation(UUID orderId);

  void save(Reservation reservation);
}
