package io.zeroshift.platform;

import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
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

  private final JdbcTemplate jdbc;
  private final TransactionTemplate separate;

  public Faults(JdbcTemplate jdbc, TransactionTemplate transactions) {
    this.jdbc = jdbc;
    separate = new TransactionTemplate(transactions.getTransactionManager());
    separate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  public void arm(String name, String mode, Integer times) {
    jdbc.update(
        "INSERT INTO lab_fault(name,mode,remaining) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE"
            + " SET mode=EXCLUDED.mode,remaining=EXCLUDED.remaining,armed_at=clock_timestamp()",
        name,
        mode,
        times);
  }

  public void clear(String name) {
    jdbc.update("DELETE FROM lab_fault WHERE name=?", name);
  }

  /** The armed mode, if any, using up one trigger. */
  public Optional<String> trigger(String name) {
    return separate.execute(
        s -> {
          var mode =
              jdbc
                  .query(
                      "UPDATE lab_fault SET remaining=remaining-1 WHERE name=?"
                          + " AND (remaining IS NULL OR remaining>0) RETURNING mode",
                      (r, n) -> r.getString(1),
                      name)
                  .stream()
                  .findFirst();
          jdbc.update("DELETE FROM lab_fault WHERE name=? AND remaining<=0", name);
          return mode;
        });
  }

  public java.util.List<java.util.Map<String, Object>> armed() {
    return jdbc.queryForList("SELECT name,mode,remaining,armed_at FROM lab_fault ORDER BY name");
  }
}
