package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.SAGA;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.percentileCont;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.order.domain.SagaState;
import io.zeroshift.platform.PostgresClock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * How much work is inside the system and how long it takes to get through: sagas not yet finished,
 * and the time from placement to the saga's end for recently finished ones. Read from the saga
 * table, so it covers both replicas and every consumer between them.
 */
public class SagaPressure {
  /** Sagas that finished in the window, and how long they took end to end. */
  public record Completions(
      int windowSeconds,
      int completed,
      int cancelled,
      int started,
      Double p50Ms,
      Double p95Ms,
      Double p99Ms,
      Double maxMs) {}

  /**
   * @param refreshedAt when {@code active} was last read; stale when the database is unreachable
   */
  public record Active(long active, Instant refreshedAt, String error) {}

  private static final List<String> FINISHED =
      Arrays.stream(SagaState.values()).filter(SagaState::finished).map(Enum::name).toList();

  /** Seconds between placement and the last saga transition: plain SQL for the interval. */
  private static final Field<Double> DURATION_MS =
      DSL.field(
          "extract(epoch from ({0} - {1})) * 1000", Double.class, SAGA.UPDATED_AT, SAGA.STARTED_AT);

  private final DSLContext db;
  private volatile Active active = new Active(0, null, "not read yet");

  public SagaPressure(DSLContext db, MeterRegistry meters) {
    this.db = db;
    Gauge.builder("zeroshift.saga.active", () -> active.active())
        .description("Sagas placed but not yet completed or cancelled")
        .register(meters);
  }

  /**
   * Refreshed in the background, so a request deciding whether to shed load reads a number instead
   * of waiting on the database it may be protecting.
   */
  @Scheduled(fixedDelay = 500)
  public void refresh() {
    try {
      active = new Active(db.fetchCount(SAGA, SAGA.STATE.notIn(FINISHED)), Instant.now(), null);
    } catch (RuntimeException e) {
      active = new Active(active.active(), active.refreshedAt(), e.getClass().getSimpleName());
    }
  }

  public Active active() {
    return active;
  }

  public Completions completions(Duration window) {
    var since = PostgresClock.nowPlus(window.negated());
    var finished = SAGA.STATE.in(FINISHED).and(SAGA.UPDATED_AT.gt(since));
    var r =
        db.select(
                count().filterWhere(finished.and(SAGA.STATE.eq(SagaState.COMPLETED.name()))),
                count().filterWhere(finished.and(SAGA.STATE.eq(SagaState.CANCELLED.name()))),
                count().filterWhere(SAGA.STARTED_AT.gt(since)),
                percentileCont(0.5).withinGroupOrderBy(DURATION_MS).filterWhere(finished),
                percentileCont(0.95).withinGroupOrderBy(DURATION_MS).filterWhere(finished),
                percentileCont(0.99).withinGroupOrderBy(DURATION_MS).filterWhere(finished),
                DSL.max(DURATION_MS).filterWhere(finished))
            .from(SAGA)
            .where(SAGA.UPDATED_AT.gt(since).or(SAGA.STARTED_AT.gt(since)))
            .fetchSingle();
    return new Completions(
        (int) window.toSeconds(),
        r.value1(),
        r.value2(),
        r.value3(),
        number(r.value4()),
        number(r.value5()),
        number(r.value6()),
        number(r.value7()));
  }

  private static Double number(Number n) {
    return n == null ? null : n.doubleValue();
  }
}
