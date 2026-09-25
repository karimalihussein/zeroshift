package io.zeroshift.contracts;

import java.math.BigDecimal;
import java.util.UUID;

public sealed interface PaymentCommand extends Message {
  record AuthorizePayment(UUID orderId, BigDecimal amount, String currency)
      implements PaymentCommand {}

  /** Compensation. Refunding an order that was never charged is a successful no-op. */
  record RefundPayment(UUID orderId, String reason) implements PaymentCommand {}
}
