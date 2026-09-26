package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.IDEMPOTENCY_KEY;

import io.zeroshift.order.application.IdempotencyKeys;
import java.util.Optional;
import org.jooq.DSLContext;

public final class PostgresIdempotencyKeys implements IdempotencyKeys {
  private final DSLContext db;

  public PostgresIdempotencyKeys(DSLContext db) {
    this.db = db;
  }

  @Override
  public Optional<Claim> find(String key) {
    return db.selectFrom(IDEMPOTENCY_KEY)
        .where(IDEMPOTENCY_KEY.KEY.eq(key))
        .fetchOptional(
            r ->
                new Claim(
                    r.getKey(),
                    r.getRequestHash(),
                    r.getOrderId(),
                    r.getCorrelationId(),
                    r.getEventId()));
  }

  @Override
  public boolean claim(Claim claim) {
    return db.insertInto(IDEMPOTENCY_KEY)
            .set(IDEMPOTENCY_KEY.KEY, claim.key())
            .set(IDEMPOTENCY_KEY.REQUEST_HASH, claim.requestHash())
            .set(IDEMPOTENCY_KEY.ORDER_ID, claim.orderId())
            .set(IDEMPOTENCY_KEY.CORRELATION_ID, claim.correlationId())
            .set(IDEMPOTENCY_KEY.EVENT_ID, claim.eventId())
            .onConflictDoNothing()
            .execute()
        == 1;
  }
}
