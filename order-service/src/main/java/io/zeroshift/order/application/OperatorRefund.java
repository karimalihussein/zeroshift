package io.zeroshift.order.application;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.PaymentCommand.RefundPayment;
import io.zeroshift.platform.Outbox;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Recovery after the fact: an operator refunds an order's payment, for example a duplicate order
 * created by a client retry. It is a real compensating command through the outbox, in the order's
 * own conversation, and payment-service refunds it at most once. The order itself is not undone:
 * once shipped, the saga has passed its pivot, which is why preventing the duplicate beats fixing
 * it.
 */
public final class OperatorRefund {
  private final SagaStore sagas;
  private final Outbox outbox;
  private final TransactionOperations transactions;

  public OperatorRefund(SagaStore sagas, Outbox outbox, TransactionOperations transactions) {
    this.sagas = sagas;
    this.outbox = outbox;
    this.transactions = transactions;
  }

  public Envelope refund(UUID orderId, String reason) {
    return transactions.execute(
        tx -> {
          var saga =
              sagas
                  .find(orderId)
                  .orElseThrow(() -> new NoSuchElementException("No order " + orderId));
          return outbox.append(
              Envelope.of(new RefundPayment(orderId, reason), saga.correlationId(), null));
        });
  }
}
