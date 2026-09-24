package io.zeroshift.contracts;

import java.math.BigDecimal;

public record OrderLine(String sku, int quantity, BigDecimal unitPrice) {
  public OrderLine {
    if (sku == null || sku.isBlank()) throw new IllegalArgumentException("sku is required");
    if (quantity < 1) throw new IllegalArgumentException("quantity must be at least 1");
    if (unitPrice == null || unitPrice.signum() < 0)
      throw new IllegalArgumentException("unitPrice must not be negative");
  }

  public BigDecimal subtotal() {
    return unitPrice.multiply(BigDecimal.valueOf(quantity));
  }
}
