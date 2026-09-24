package io.zeroshift.order.infrastructure;

import io.zeroshift.order.application.OrderSaga;
import io.zeroshift.order.application.SagaStore;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Compensates sagas whose current step missed its deadline. Due sagas are claimed with FOR UPDATE
 * SKIP LOCKED, so several order-service replicas can scan at once without handling one twice.
 */
public class SagaTimeouts {
  private static final Logger log = LoggerFactory.getLogger(SagaTimeouts.class);
  private final SagaStore sagas;
  private final OrderSaga saga;
  private final TransactionOperations transactions;
  private final Clock clock;

  public SagaTimeouts(
      SagaStore sagas, OrderSaga saga, TransactionOperations transactions, Clock clock) {
    this.sagas = sagas;
    this.saga = saga;
    this.transactions = transactions;
    this.clock = clock;
  }

  @Scheduled(fixedDelayString = "${order.timeout-scan:1000}")
  public void scan() {
    transactions.executeWithoutResult(
        tx ->
            sagas
                .due(clock.instant(), 20)
                .forEach(
                    due -> {
                      log.warn("Saga {} timed out in {}", due.orderId(), due.state());
                      saga.timeout(due);
                    }));
  }
}
