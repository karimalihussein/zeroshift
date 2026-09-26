package io.zeroshift.contracts;

import java.math.BigDecimal;
import java.util.UUID;

public sealed interface PaymentEvent extends Message {
  record PaymentAuthorized(UUID orderId, UUID paymentId, BigDecimal amount, String currency)
      implements PaymentEvent {}

  record PaymentDeclined(UUID orderId, String reason) implements PaymentEvent {}

  /** {@code paymentId} is null when there was nothing to refund. */
  record PaymentRefunded(UUID orderId, UUID paymentId, BigDecimal amount, String currency)
      implements PaymentEvent {}
}
