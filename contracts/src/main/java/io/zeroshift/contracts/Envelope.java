package io.zeroshift.contracts;

import java.time.Instant;
import java.util.UUID;

/**
 * What travels on Kafka. {@code eventId} identifies this message everywhere (outbox row id,
 * idempotency key, log field). {@code correlationId} is shared by everything one order request
 * causes; {@code causationId} is the eventId of the message that caused this one (null for the
 * first).
 */
public record Envelope(
    UUID eventId,
    String type,
    int schemaVersion,
    UUID correlationId,
    UUID causationId,
    Instant occurredAt,
    Message payload) {

  /** A new message at its current schema version. */
  public static Envelope of(Message payload, UUID correlationId, UUID causationId) {
    var contract = Contracts.of(payload.getClass());
    return new Envelope(
        UUID.randomUUID(),
        contract.type(),
        contract.version(),
        correlationId,
        causationId,
        Instant.now(),
        payload);
  }

  /** A message caused by this one, in the same conversation. */
  public Envelope reply(Message payload) {
    return of(payload, correlationId, eventId);
  }

  public String topic() {
    return Contracts.of(payload.getClass()).topic();
  }

  public UUID orderId() {
    return payload.orderId();
  }
}
