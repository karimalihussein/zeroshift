package io.zeroshift.order.application;

import java.util.UUID;

/** An order for a customer this service does not know. */
public final class UnknownCustomer extends RuntimeException {
  public UnknownCustomer(UUID customerId) {
    super("No customer " + customerId);
  }
}
