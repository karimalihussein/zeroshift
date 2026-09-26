package io.zeroshift.payment.infrastructure;

import static io.zeroshift.payment.db.Tables.PAYMENT;

import io.zeroshift.payment.application.Payments;
import io.zeroshift.payment.db.tables.records.PaymentRecord;
import io.zeroshift.platform.PostgresClock;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.TableField;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The payment table. Every status change is conditional on the status it leaves, so a transition
 * applies once however often it is asked for.
 */
public final class PostgresPayments implements Payments {
  /** One payment as the read API shows it. */
  public record PaymentView(
      UUID id,
      UUID orderId,
      String idempotencyKey,
      Status status,
      Method method,
      BigDecimal amount,
      String currency,
      String provider,
      String providerReference,
      String failureReason,
      OffsetDateTime createdAt,
      OffsetDateTime authorizedAt,
      OffsetDateTime declinedAt,
      OffsetDateTime refundedAt,
      OffsetDateTime voidedAt) {}

  private final DSLContext db;
  private final TransactionTemplate separate;
  private final String provider;

  public PostgresPayments(DSLContext db, TransactionTemplate transactions, String provider) {
    this.db = db;
    this.provider = provider;
    separate = new TransactionTemplate(transactions.getTransactionManager());
    separate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  @Override
  public Payment claim(String idempotencyKey, UUID orderId, BigDecimal amount, String currency) {
    return separate.execute(
        s -> {
          db.insertInto(PAYMENT)
              .set(PAYMENT.ID, UUID.randomUUID())
              .set(PAYMENT.ORDER_ID, orderId)
              .set(PAYMENT.IDEMPOTENCY_KEY, idempotencyKey)
              .set(PAYMENT.STATUS, Status.PENDING.name())
              .set(PAYMENT.METHOD, Method.CARD.name())
              .set(PAYMENT.AMOUNT, amount)
              .set(PAYMENT.CURRENCY, currency)
              .set(PAYMENT.PROVIDER, provider)
              .onConflict(PAYMENT.IDEMPOTENCY_KEY)
              .doNothing()
              .execute();
          return db.selectFrom(PAYMENT)
              .where(PAYMENT.IDEMPOTENCY_KEY.eq(idempotencyKey))
              .fetchSingle(PostgresPayments::payment);
        });
  }

  @Override
  public void authorize(UUID id, String providerReference) {
    db.update(PAYMENT)
        .set(PAYMENT.STATUS, Status.AUTHORIZED.name())
        .set(PAYMENT.PROVIDER_REFERENCE, providerReference)
        .set(PAYMENT.AUTHORIZED_AT, PostgresClock.NOW)
        .set(PAYMENT.UPDATED_AT, PostgresClock.NOW)
        .where(PAYMENT.ID.eq(id), PAYMENT.STATUS.eq(Status.PENDING.name()))
        .execute();
  }

  @Override
  public void decline(UUID id, String reason) {
    db.update(PAYMENT)
        .set(PAYMENT.STATUS, Status.DECLINED.name())
        .set(PAYMENT.FAILURE_REASON, reason)
        .set(PAYMENT.DECLINED_AT, PostgresClock.NOW)
        .set(PAYMENT.UPDATED_AT, PostgresClock.NOW)
        .where(PAYMENT.ID.eq(id), PAYMENT.STATUS.eq(Status.PENDING.name()))
        .execute();
  }

  @Override
  public void refund(UUID id) {
    transition(id, Status.AUTHORIZED, Status.REFUNDED, PAYMENT.REFUNDED_AT);
  }

  @Override
  public void voidPending(UUID id) {
    transition(id, Status.PENDING, Status.VOIDED, PAYMENT.VOIDED_AT);
  }

  @Override
  public void voidUnclaimed(String idempotencyKey, UUID orderId, String reason) {
    db.insertInto(PAYMENT)
        .set(PAYMENT.ID, UUID.randomUUID())
        .set(PAYMENT.ORDER_ID, orderId)
        .set(PAYMENT.IDEMPOTENCY_KEY, idempotencyKey)
        .set(PAYMENT.STATUS, Status.VOIDED.name())
        .set(PAYMENT.METHOD, Method.CARD.name())
        .set(PAYMENT.PROVIDER, provider)
        .set(PAYMENT.FAILURE_REASON, reason)
        .set(PAYMENT.VOIDED_AT, PostgresClock.NOW)
        .onConflict(PAYMENT.IDEMPOTENCY_KEY)
        .doNothing()
        .execute();
  }

  @Override
  public List<Payment> forOrder(UUID orderId) {
    return db.selectFrom(PAYMENT)
        .where(PAYMENT.ORDER_ID.eq(orderId))
        .orderBy(PAYMENT.CREATED_AT, PAYMENT.ID)
        .fetch(PostgresPayments::payment);
  }

  public List<PaymentView> views(UUID orderId) {
    return db.selectFrom(PAYMENT)
        .where(PAYMENT.ORDER_ID.eq(orderId))
        .orderBy(PAYMENT.CREATED_AT, PAYMENT.ID)
        .fetch(PostgresPayments::view);
  }

  public Optional<PaymentView> view(UUID id) {
    return db.selectFrom(PAYMENT).where(PAYMENT.ID.eq(id)).fetchOptional(PostgresPayments::view);
  }

  private void transition(
      UUID id, Status from, Status to, TableField<PaymentRecord, OffsetDateTime> at) {
    db.update(PAYMENT)
        .set(PAYMENT.STATUS, to.name())
        .set(at, PostgresClock.NOW)
        .set(PAYMENT.UPDATED_AT, PostgresClock.NOW)
        .where(PAYMENT.ID.eq(id), PAYMENT.STATUS.eq(from.name()))
        .execute();
  }

  private static Payment payment(PaymentRecord r) {
    return new Payment(
        r.getId(),
        r.getOrderId(),
        r.getIdempotencyKey(),
        Status.valueOf(r.getStatus()),
        r.getAmount(),
        r.getCurrency(),
        r.getProviderReference(),
        r.getFailureReason());
  }

  private static PaymentView view(PaymentRecord r) {
    return new PaymentView(
        r.getId(),
        r.getOrderId(),
        r.getIdempotencyKey(),
        Status.valueOf(r.getStatus()),
        Method.valueOf(r.getMethod()),
        r.getAmount(),
        r.getCurrency(),
        r.getProvider(),
        r.getProviderReference(),
        r.getFailureReason(),
        r.getCreatedAt(),
        r.getAuthorizedAt(),
        r.getDeclinedAt(),
        r.getRefundedAt(),
        r.getVoidedAt());
  }
}
