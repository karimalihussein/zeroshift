package io.zeroshift.order.application;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

/** Idempotency-Key → the answer its first request got. */
public interface IdempotencyKeys {
  record Claim(
      String key,
      String requestHash,
      UUID orderId,
      UUID correlationId,
      UUID eventId,
      BigDecimal total) {}

  Optional<Claim> find(String key);

  /**
   * Records the key in the caller's transaction. False if another request holds it: a concurrent
   * holder makes this wait until it commits, so false always means "already answered".
   */
  boolean claim(Claim claim);
}
