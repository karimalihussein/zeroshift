package io.zeroshift.order.application;

import io.zeroshift.order.domain.Saga;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface SagaStore {
  void start(Saga saga);

  Optional<Saga> find(UUID orderId);

  /**
   * Saves if still at {@code saga.version()}, logging the transition; otherwise throws {@link
   * ConcurrencyConflict}.
   */
  void save(Saga saga, String from, String trigger, UUID triggerEventId, String detail);

  /** Sagas past their deadline, locked and skipped by any other scanner (SKIP LOCKED). */
  List<Saga> due(Instant now, int limit);

  List<Saga> recent(int limit);

  List<Map<String, Object>> transitions(UUID orderId);
}
