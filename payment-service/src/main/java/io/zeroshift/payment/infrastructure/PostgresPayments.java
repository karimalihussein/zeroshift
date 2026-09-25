package io.zeroshift.payment.infrastructure;

import static io.zeroshift.payment.db.Tables.PAYMENT;

import io.zeroshift.payment.application.Payments;
import io.zeroshift.platform.PostgresClock;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;

public final class PostgresPayments implements Payments {
  private final DSLContext db;

  public PostgresPayments(DSLContext db) {
    this.db = db;
  }

  @Override
  public Optional<Payment> find(UUID orderId) {
    return db.select(
            PAYMENT.ORDER_ID, PAYMENT.PAYMENT_ID, PAYMENT.STATUS, PAYMENT.AMOUNT, PAYMENT.REASON)
        .from(PAYMENT)
        .where(PAYMENT.ORDER_ID.eq(orderId))
        .fetchOptional(
            r ->
                new Payment(
                    r.value1(), r.value2(), Status.valueOf(r.value3()), r.value4(), r.value5()));
  }

  /** One row per order. Amount, currency and gateway reference are kept from the first write. */
  @Override
  public void save(Payment payment, String currency, String gatewayReference) {
    db.insertInto(PAYMENT)
        .set(PAYMENT.ORDER_ID, payment.orderId())
        .set(PAYMENT.PAYMENT_ID, payment.paymentId())
        .set(PAYMENT.STATUS, payment.status().name())
        .set(PAYMENT.AMOUNT, payment.amount())
        .set(PAYMENT.CURRENCY, currency)
        .set(PAYMENT.GATEWAY_REFERENCE, gatewayReference)
        .set(PAYMENT.REASON, payment.reason())
        .onConflict(PAYMENT.ORDER_ID)
        .doUpdate()
        .set(PAYMENT.STATUS, payment.status().name())
        .set(PAYMENT.REASON, payment.reason())
        .set(PAYMENT.UPDATED_AT, PostgresClock.NOW)
        .execute();
  }
}
