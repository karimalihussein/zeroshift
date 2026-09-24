package io.zeroshift.domain;

import java.math.BigDecimal;

/** A deliberately small inspection projection, not a persistence entity. */
public record OrderRecord(
    long id, long customerId, String customerName, BigDecimal amount, String status) {
  public boolean sameValues(OrderRecord other) {
    return other != null
        && id == other.id
        && customerId == other.customerId
        && customerName.equals(other.customerName)
        && amount.compareTo(other.amount) == 0
        && status.equals(other.status);
  }
}
