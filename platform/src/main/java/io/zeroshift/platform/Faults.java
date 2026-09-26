package io.zeroshift.platform;

import static io.zeroshift.platform.db.Tables.LAB_FAULT;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Operator-armed failures, stored in the service's own database. Each trigger is committed in its
 * own transaction, so a fault that makes the handler roll back is still counted as used: "fail 3
 * times" fails three deliveries, not the same one forever.
 */
public final class Faults {
  /** Every consumer: halt the JVM after the handler commits but before the offset commit. */
  public static final String CRASH_AFTER_COMMIT = "crash-after-commit";

  /** Every consumer: throw before processing, exercising retry with backoff and then the DLT. */
  public static final String TRANSIENT_ERROR = "transient-error";

  /**
   * Every consumer: sleep this many milliseconds (the mode) before handling each delivery, on the
   * consumer's own thread and outside any transaction. A slow consumer, for the backpressure labs.
   */
  public static final String SLOW_PROCESSING = "slow-processing";

  private final DSLContext db;
  private final TransactionTemplate separate;

  public Faults(DSLContext db, TransactionTemplate transactions) {
    this.db = db;
    separate = new TransactionTemplate(transactions.getTransactionManager());
    separate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  /** Arms {@code name}; {@code times} null means until cleared. */
  public void arm(String name, String mode, Integer times) {
    db.insertInto(LAB_FAULT)
        .set(LAB_FAULT.NAME, name)
        .set(LAB_FAULT.MODE, mode)
        .set(LAB_FAULT.REMAINING, times)
        .onConflict(LAB_FAULT.NAME)
        .doUpdate()
        .set(LAB_FAULT.MODE, mode)
        .set(LAB_FAULT.REMAINING, times)
        .set(LAB_FAULT.ARMED_AT, PostgresClock.NOW)
        .execute();
  }

  public void clear(String name) {
    db.deleteFrom(LAB_FAULT).where(LAB_FAULT.NAME.eq(name)).execute();
  }

  /** The armed mode, if any, using up one trigger. */
  public Optional<String> trigger(String name) {
    return separate.execute(
        s -> {
          var mode =
              db.update(LAB_FAULT)
                  .set(LAB_FAULT.REMAINING, LAB_FAULT.REMAINING.minus(1))
                  .where(
                      LAB_FAULT
                          .NAME
                          .eq(name)
                          .and(LAB_FAULT.REMAINING.isNull().or(LAB_FAULT.REMAINING.gt(0))))
                  .returning(LAB_FAULT.MODE)
                  .fetchOptional(LAB_FAULT.MODE);
          db.deleteFrom(LAB_FAULT)
              .where(LAB_FAULT.NAME.eq(name).and(LAB_FAULT.REMAINING.le(0)))
              .execute();
          return mode;
        });
  }

  /** An armed fault; {@code remaining} null means armed until cleared. */
  public record Fault(String name, String mode, Integer remaining, OffsetDateTime armedAt) {}

  public List<Fault> armed() {
    return db.selectFrom(LAB_FAULT)
        .orderBy(LAB_FAULT.NAME)
        .fetch(r -> new Fault(r.getName(), r.getMode(), r.getRemaining(), r.getArmedAt()));
  }
}
