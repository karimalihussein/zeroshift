package io.zeroshift.payment.infrastructure;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Durable log of gateway attempts, committed independently of the delivery's transaction. */
public final class GatewayCalls {
  private final JdbcTemplate jdbc;
  private final TransactionTemplate separate;

  public GatewayCalls(JdbcTemplate jdbc, TransactionTemplate transactions) {
    this.jdbc = jdbc;
    separate = new TransactionTemplate(transactions.getTransactionManager());
    separate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  public void record(
      UUID orderId, String outcome, long latencyMillis, String breaker, String detail) {
    separate.executeWithoutResult(
        s ->
            jdbc.update(
                "INSERT INTO gateway_call(order_id,outcome,latency_ms,breaker_state,detail) VALUES(?,?,?,?,?)",
                orderId,
                outcome,
                latencyMillis,
                breaker,
                detail));
  }

  public List<Map<String, Object>> recent(UUID orderId, int limit) {
    return jdbc.queryForList(
        "SELECT * FROM gateway_call WHERE (?::uuid IS NULL OR order_id=?) ORDER BY id DESC LIMIT ?",
        orderId,
        orderId,
        limit);
  }
}
