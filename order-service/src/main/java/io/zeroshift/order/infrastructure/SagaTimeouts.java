package io.zeroshift.order.infrastructure;

import io.zeroshift.order.application.OrderSaga;
import io.zeroshift.order.application.SagaStore;
import io.zeroshift.platform.Faults;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Compensates sagas whose current step missed its deadline. Two mechanisms, two jobs: a lease makes
 * one replica the scanner at a time (job-level mutual exclusion that survives the holder dying),
 * and FOR UPDATE SKIP LOCKED claims each due saga row so that even overlapping scans can never
 * compensate one saga twice.
 *
 * <p>A lease alone is not enough: a holder can stall (GC pause, CPU starvation) past its expiry,
 * lose the lease to another replica, then wake up still believing it is the scanner. So the scan's
 * transaction first checks, under a share lock on the lease row, that its fencing token is still
 * the current one; a superseded holder is fenced off and writes nothing. The share lock also makes
 * a takeover wait for an in-flight scan to commit, so two scanners never act at once.
 */
public class SagaTimeouts {
  public static final String LEASE = "saga-timeout-scanner";

  /** Operator fault: the scanner stalls this many milliseconds after acquiring the lease. */
  public static final String STALL = "scanner-stall";

  static final Duration TTL = Duration.ofSeconds(5);
  private static final Logger log = LoggerFactory.getLogger(SagaTimeouts.class);
  private final SagaStore sagas;
  private final OrderSaga saga;
  private final TransactionOperations transactions;
  private final Clock clock;
  private final PostgresLease lease;
  private final Faults faults;

  public SagaTimeouts(
      SagaStore sagas,
      OrderSaga saga,
      TransactionOperations transactions,
      Clock clock,
      PostgresLease lease,
      Faults faults) {
    this.lease = lease;
    this.faults = faults;
    this.sagas = sagas;
    this.saga = saga;
    this.transactions = transactions;
    this.clock = clock;
  }

  @Scheduled(fixedDelayString = "${order.timeout-scan:1000}")
  public void scan() throws InterruptedException {
    var acquired = lease.acquire(LEASE, TTL);
    if (acquired.isEmpty()) return; // another replica is the scanner
    long token = acquired.getAsLong();
    var stall = faults.trigger(STALL);
    if (stall.isPresent()) {
      log.warn("Scanner {} stalls {} ms holding token {}", lease.owner(), stall.get(), token);
      Thread.sleep(Long.parseLong(stall.get()));
    }
    transactions.executeWithoutResult(
        tx -> {
          if (!lease.fence(LEASE, token)) {
            log.warn(
                "Scanner {} fenced off: token {} was superseded while it stalled",
                lease.owner(),
                token);
            return;
          }
          sagas
              .due(clock.instant(), 20)
              .forEach(
                  due -> {
                    log.warn(
                        "Saga {} timed out in {} (scanner {}, fencing token {})",
                        due.orderId(),
                        due.state(),
                        lease.owner(),
                        token);
                    saga.timeout(due);
                  });
        });
  }
}
