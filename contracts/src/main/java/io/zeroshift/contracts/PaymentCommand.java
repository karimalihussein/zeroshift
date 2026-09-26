package io.zeroshift.contracts;

import java.math.BigDecimal;
import java.util.UUID;

public sealed interface PaymentCommand extends Message {
  /**
   * {@code idempotencyKey} identifies this charge attempt: the same key never charges twice. Null
   * in commands written before it existed; the payment service then uses {@link #keyFor}.
   */
  record AuthorizePayment(UUID orderId, BigDecimal amount, String currency, String idempotencyKey)
      implements PaymentCommand {
    public AuthorizePayment {
      if (idempotencyKey == null || idempotencyKey.isBlank()) idempotencyKey = keyFor(orderId);
    }

    /** The key the order saga uses: one authorization per order. */
    public static String keyFor(UUID orderId) {
      return "order:" + orderId + ":authorize";
    }
  }

  /** Compensation. Refunding an order that was never charged is a successful no-op. */
  record RefundPayment(UUID orderId, String reason) implements PaymentCommand {}
}
