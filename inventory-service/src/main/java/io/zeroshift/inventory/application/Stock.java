package io.zeroshift.inventory.application;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Each product's sellable stock, and one reservation per order. */
public interface Stock {
  record Item(UUID productId, String sku, int quantity) {}

  enum Status {
    RESERVED,
    REJECTED,
    RELEASED
  }

  /** {@code items} is empty unless the reservation is (or was) {@code RESERVED}. */
  record Reservation(UUID id, UUID orderId, Status status, List<Item> items, String reason) {}

  Optional<UUID> productId(String sku);

  /**
   * Takes every item's quantity from stock, all or none. Returns why not when some product is
   * unknown, inactive or short; then nothing was taken.
   */
  Optional<String> take(List<Item> items);

  /** Returns the quantities to stock. */
  void giveBack(List<Item> items);

  Optional<Reservation> reservation(UUID orderId);

  /** Records a new reservation with its items. */
  void insert(Reservation reservation);

  /** Marks the order's reservation released. */
  void released(UUID orderId, String reason);
}
