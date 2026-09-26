package io.zeroshift.payment.application;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Payments by idempotency key: one row per charge attempt, never two for the same key. */
public interface Payments {
  enum Status {
    PENDING,
    AUTHORIZED,
    DECLINED,
    REFUNDED,
    VOIDED
  }

  enum Method {
    CARD
  }

  /** {@code amount} and {@code currency} are null only for an order voided before any charge. */
  record Payment(
      UUID id,
      UUID orderId,
      String idempotencyKey,
      Status status,
      BigDecimal amount,
      String currency,
      String providerReference,
      String failureReason) {}

  /**
   * Claims {@code idempotencyKey} with a new PENDING payment, committed on its own so a crash after
   * calling the gateway leaves it visible, and returns the payment that holds the key: the new one,
   * or the one that claimed it first.
   */
  Payment claim(String idempotencyKey, UUID orderId, BigDecimal amount, String currency);

  /** PENDING to AUTHORIZED. */
  void authorize(UUID id, String providerReference);

  /** PENDING to DECLINED. */
  void decline(UUID id, String reason);

  /** AUTHORIZED to REFUNDED. */
  void refund(UUID id);

  /** PENDING to VOIDED: the claimed charge is never sent (again). */
  void voidPending(UUID id);

  /** Records {@code idempotencyKey} as VOIDED unless a payment already holds it. */
  void voidUnclaimed(String idempotencyKey, UUID orderId, String reason);

  /** The order's payments, oldest first. */
  List<Payment> forOrder(UUID orderId);
}
