package io.zeroshift.contracts;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * One sold item, as it was when the order was placed: the product's id, SKU, name and unit price
 * are snapshots, so later catalog changes never alter a placed order. {@code discount} is this
 * line's share of the order's discount; {@code total = subtotal - discount}.
 *
 * <p>{@code productId} is null only in events written before products had ids (schema v1/v2).
 */
public record OrderLine(
    UUID productId,
    String sku,
    String name,
    int quantity,
    BigDecimal unitPrice,
    BigDecimal subtotal,
    BigDecimal discount,
    BigDecimal total) {
  public OrderLine {
    if (sku == null || sku.isBlank()) throw new IllegalArgumentException("sku is required");
    if (quantity < 1) throw new IllegalArgumentException("quantity must be at least 1");
    if (unitPrice == null || unitPrice.signum() < 0)
      throw new IllegalArgumentException("unitPrice must not be negative");
    if (name == null || name.isBlank()) name = sku;
    unitPrice = Money.exact(unitPrice);
    var expected = unitPrice.multiply(BigDecimal.valueOf(quantity));
    subtotal = subtotal == null ? expected : Money.exact(subtotal);
    if (subtotal.compareTo(expected) != 0)
      throw new IllegalArgumentException("subtotal must be unitPrice × quantity");
    discount = discount == null ? Money.ZERO : Money.exact(discount);
    if (discount.signum() < 0 || discount.compareTo(subtotal) > 0)
      throw new IllegalArgumentException("discount must be between 0 and the subtotal");
    total = total == null ? subtotal.subtract(discount) : Money.exact(total);
    if (total.compareTo(subtotal.subtract(discount)) != 0)
      throw new IllegalArgumentException("total must be subtotal - discount");
  }

  /** A line without product id or name (tests, and events from before products had ids). */
  public OrderLine(String sku, int quantity, BigDecimal unitPrice) {
    this(null, sku, null, quantity, unitPrice, null, null, null);
  }

  /** An undiscounted line (subtotal and total computed). */
  public static OrderLine of(
      UUID productId, String sku, String name, int quantity, BigDecimal unitPrice) {
    return new OrderLine(productId, sku, name, quantity, unitPrice, null, null, null);
  }

  /** The same line with its share of the order's discount. */
  public OrderLine withDiscount(BigDecimal share) {
    return new OrderLine(productId, sku, name, quantity, unitPrice, subtotal, share, null);
  }
}
