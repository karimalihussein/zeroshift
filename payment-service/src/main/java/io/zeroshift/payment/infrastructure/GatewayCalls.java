package io.zeroshift.payment.infrastructure;

import static io.zeroshift.payment.db.Tables.GATEWAY_CALL;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Durable log of gateway attempts, committed independently of the delivery's transaction. */
public final class GatewayCalls {
  private final DSLContext db;
  private final TransactionTemplate separate;

  public GatewayCalls(DSLContext db, TransactionTemplate transactions) {
    this.db = db;
    separate = new TransactionTemplate(transactions.getTransactionManager());
    separate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  /**
   * Committed on its own: a call the gateway saw stays logged even if the delivery that made it
   * rolls back and is retried.
   */
  public void record(
      UUID orderId, String outcome, long latencyMillis, String breaker, String detail) {
    separate.executeWithoutResult(
        s ->
            db.insertInto(GATEWAY_CALL)
                .set(GATEWAY_CALL.ORDER_ID, orderId)
                .set(GATEWAY_CALL.OUTCOME, outcome)
                .set(GATEWAY_CALL.LATENCY_MS, latencyMillis)
                .set(GATEWAY_CALL.BREAKER_STATE, breaker)
                .set(GATEWAY_CALL.DETAIL, detail)
                .execute());
  }

  /** For the control plane, newest first; every order's calls when {@code orderId} is null. */
  public List<Map<String, Object>> recent(UUID orderId, int limit) {
    return db.selectFrom(GATEWAY_CALL)
        .where(orderId == null ? DSL.noCondition() : GATEWAY_CALL.ORDER_ID.eq(orderId))
        .orderBy(GATEWAY_CALL.ID.desc())
        .limit(limit)
        .fetchMaps();
  }
}
