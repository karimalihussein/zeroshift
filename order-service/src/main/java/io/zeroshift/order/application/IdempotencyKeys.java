package io.zeroshift.order.application;

import java.util.Optional;
import java.util.UUID;

/** Idempotency-Key → the order its first request created. */
public interface IdempotencyKeys {
  record Claim(String key, String requestHash, UUID orderId, UUID correlationId, UUID eventId) {}

  Optional<Claim> find(String key);

  /**
   * Records the key in the caller's transaction. False if another request holds it: a concurrent
   * holder makes this wait until it commits, so false always means "already answered".
   */
  boolean claim(Claim claim);
}
