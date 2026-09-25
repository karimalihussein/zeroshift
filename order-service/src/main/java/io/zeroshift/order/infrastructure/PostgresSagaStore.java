package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.SAGA;
import static io.zeroshift.order.db.Tables.SAGA_TRANSITION;

import io.zeroshift.order.application.ConcurrencyConflict;
import io.zeroshift.order.application.SagaStore;
import io.zeroshift.order.db.tables.records.SagaRecord;
import io.zeroshift.order.domain.Saga;
import io.zeroshift.order.domain.SagaState;
import io.zeroshift.platform.PostgresClock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import org.jooq.DSLContext;

public final class PostgresSagaStore implements SagaStore {
  private final DSLContext db;

  public PostgresSagaStore(DSLContext db) {
    this.db = db;
  }

  @Override
  public void start(Saga saga) {
    db.insertInto(SAGA)
        .set(SAGA.ORDER_ID, saga.orderId())
        .set(SAGA.CORRELATION_ID, saga.correlationId())
        .set(SAGA.STATE, saga.state().name())
        .set(SAGA.DEADLINE, utc(saga.deadline()))
        .set(SAGA.VERSION, 1)
        .set(SAGA.STARTED_AT, utc(saga.startedAt()))
        .set(SAGA.UPDATED_AT, utc(saga.startedAt()))
        .execute();
    transition(saga, null, "PlaceOrder", null, "order placed → AuthorizePayment sent");
  }

  @Override
  public Optional<Saga> find(UUID orderId) {
    return db.selectFrom(SAGA)
        .where(SAGA.ORDER_ID.eq(orderId))
        .fetchOptional(PostgresSagaStore::saga);
  }

  /** Optimistic lock: the update matches only if nobody saved since this saga was read. */
  @Override
  public void save(Saga saga, String from, String trigger, UUID triggerEventId, String detail) {
    int saved =
        db.update(SAGA)
            .set(SAGA.STATE, saga.state().name())
            .set(SAGA.DEADLINE, utc(saga.deadline()))
            .set(SAGA.PENDING, saga.pending().toArray(String[]::new))
            .set(SAGA.FAILURE_REASON, saga.failureReason())
            .set(SAGA.COMPENSATIONS, saga.compensations().toArray(String[]::new))
            .set(SAGA.VERSION, SAGA.VERSION.plus(1))
            .set(SAGA.UPDATED_AT, PostgresClock.NOW)
            .where(SAGA.ORDER_ID.eq(saga.orderId()).and(SAGA.VERSION.eq(saga.version())))
            .execute();
    if (saved != 1)
      throw new ConcurrencyConflict(
          "Saga " + saga.orderId() + " changed after version " + saga.version() + " was read");
    transition(saga, from, trigger, triggerEventId, detail);
  }

  private void transition(
      Saga saga, String from, String trigger, UUID triggerEventId, String detail) {
    db.insertInto(SAGA_TRANSITION)
        .set(SAGA_TRANSITION.ORDER_ID, saga.orderId())
        .set(SAGA_TRANSITION.FROM_STATE, from)
        .set(SAGA_TRANSITION.TO_STATE, saga.state().name())
        .set(SAGA_TRANSITION.TRIGGER_TYPE, trigger)
        .set(SAGA_TRANSITION.TRIGGER_EVENT_ID, triggerEventId)
        .set(SAGA_TRANSITION.DETAIL, detail)
        .execute();
  }

  /** Locks what it returns; rows another scanner has locked are skipped, not waited for. */
  @Override
  public List<Saga> due(Instant now, int limit) {
    return db.selectFrom(SAGA)
        .where(
            SAGA.DEADLINE
                .lt(utc(now))
                .and(
                    SAGA.STATE.in(
                        Arrays.stream(SagaState.values())
                            .filter(SagaState::timesOut)
                            .map(Enum::name)
                            .toList())))
        .orderBy(SAGA.DEADLINE)
        .limit(limit)
        .forUpdate()
        .skipLocked()
        .fetch(PostgresSagaStore::saga);
  }

  @Override
  public List<Saga> recent(int limit) {
    return db.selectFrom(SAGA)
        .orderBy(SAGA.STARTED_AT.desc())
        .limit(limit)
        .fetch(PostgresSagaStore::saga);
  }

  @Override
  public List<Transition> transitions(UUID orderId) {
    var t = SAGA_TRANSITION;
    return db.select(t.FROM_STATE, t.TO_STATE, t.TRIGGER_TYPE, t.TRIGGER_EVENT_ID, t.DETAIL, t.AT)
        .from(t)
        .where(t.ORDER_ID.eq(orderId))
        .orderBy(t.ID)
        .fetch(
            r ->
                new Transition(
                    r.value1(),
                    r.value2(),
                    r.value3(),
                    r.value4(),
                    r.value5(),
                    r.value6().toInstant()));
  }

  private static Saga saga(SagaRecord r) {
    return new Saga(
        r.getOrderId(),
        r.getCorrelationId(),
        SagaState.valueOf(r.getState()),
        instant(r.getDeadline()),
        new TreeSet<>(Arrays.asList(r.getPending())),
        r.getFailureReason(),
        List.of(r.getCompensations()),
        r.getVersion(),
        instant(r.getStartedAt()),
        instant(r.getUpdatedAt()));
  }

  private static OffsetDateTime utc(Instant instant) {
    return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
  }

  private static Instant instant(OffsetDateTime time) {
    return time == null ? null : time.toInstant();
  }
}
