package io.zeroshift.domain;

import java.math.BigDecimal;

public record OrderEdit(String customerName, BigDecimal amount, String status) {
  public OrderEdit {
    if (customerName == null || customerName.isBlank() || customerName.length() > 180)
      throw new InvalidAction("Customer name must contain 1–180 characters");
    if (status == null || !status.matches("[A-Za-z][A-Za-z0-9_ -]{0,23}"))
      throw new InvalidAction(
          "Status must contain 1–24 letters, numbers, spaces, underscores or hyphens");
    if (amount == null
        || amount.signum() < 0
        || amount.scale() > 4
        || amount.precision() - amount.scale() > 15)
      throw new InvalidAction(
          "Amount must be nonnegative with at most 15 integer and 4 decimal digits");
  }
}
