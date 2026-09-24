package io.zeroshift.order.domain;

/** A command the order's current state does not allow. */
public final class OrderRuleViolation extends RuntimeException {
  public OrderRuleViolation(String message) {
    super(message);
  }
}
