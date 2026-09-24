package io.zeroshift.payment.application;

import java.math.BigDecimal;
import java.util.UUID;

/** The external card processor. Charges carry the order id as idempotency key. */
public interface PaymentGateway {
  sealed interface Result permits Charged, Declined {}

  record Charged(String reference) implements Result {}

  record Declined(String reason) implements Result {}

  /** Throws {@link Unavailable} when no answer can be trusted: timeout, 5xx, connection refused. */
  Result charge(UUID orderId, BigDecimal amount, String currency);

  final class Unavailable extends RuntimeException {
    public Unavailable(String message) {
      super(message);
    }
  }
}
