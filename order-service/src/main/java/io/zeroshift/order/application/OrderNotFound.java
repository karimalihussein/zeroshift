package io.zeroshift.order.application;

import java.util.UUID;

/** No event stream for this order id: never placed, or a ghost from the dual-write demo. */
public final class OrderNotFound extends RuntimeException {
  public OrderNotFound(UUID orderId) {
    super("No order " + orderId);
  }
}
