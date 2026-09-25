package io.zeroshift.order.domain;

import java.time.Instant;
import java.util.*;

/**
 * Durable orchestration state for one order. {@code pending} names the compensation replies still
 * awaited; {@code compensations} the steps already undone. {@code version} is checked on every save
 * (optimistic locking), so two replies handled at once cannot both advance the same saga.
 */
public record Saga(
    UUID orderId,
    UUID correlationId,
    SagaState state,
    Instant deadline,
    Set<String> pending,
    String failureReason,
    List<String> compensations,
    int version,
    Instant startedAt,
    Instant updatedAt) {

  public static Saga start(UUID orderId, UUID correlationId, Instant now, Instant deadline) {
    return new Saga(
        orderId,
        correlationId,
        SagaState.AWAITING_PAYMENT,
        deadline,
        Set.of(),
        null,
        List.of(),
        1,
        now,
        now);
  }

  public Saga advance(SagaState next, Instant deadline) {
    return new Saga(
        orderId,
        correlationId,
        next,
        deadline,
        pending,
        failureReason,
        compensations,
        version,
        startedAt,
        updatedAt);
  }

  public Saga compensate(String reason, Set<String> awaiting) {
    return new Saga(
        orderId,
        correlationId,
        SagaState.COMPENSATING,
        null,
        Set.copyOf(awaiting),
        reason,
        compensations,
        version,
        startedAt,
        updatedAt);
  }

  /** One awaited compensation reply arrived. */
  public Saga compensated(String replyType, String undone) {
    var stillPending = new TreeSet<>(pending);
    stillPending.remove(replyType);
    var done = new ArrayList<>(compensations);
    done.add(undone);
    return new Saga(
        orderId,
        correlationId,
        state,
        deadline,
        stillPending,
        failureReason,
        done,
        version,
        startedAt,
        updatedAt);
  }

  public Saga fail(String reason) {
    return new Saga(
        orderId,
        correlationId,
        SagaState.CANCELLED,
        null,
        Set.of(),
        reason,
        compensations,
        version,
        startedAt,
        updatedAt);
  }

  public Saga cancelled() {
    return advance(SagaState.CANCELLED, null);
  }
}
