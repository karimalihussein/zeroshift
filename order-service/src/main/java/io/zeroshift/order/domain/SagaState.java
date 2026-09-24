package io.zeroshift.order.domain;

/**
 * Order saga steps. Payment and stock are compensatable; shipping is the pivot: once a shipment is
 * requested the saga waits for its answer instead of timing out.
 */
public enum SagaState {
  AWAITING_PAYMENT(true),
  AWAITING_STOCK(true),
  AWAITING_SHIPMENT(false),
  COMPENSATING(false),
  COMPLETED(false),
  CANCELLED(false);

  private final boolean timesOut;

  SagaState(boolean timesOut) {
    this.timesOut = timesOut;
  }

  public boolean timesOut() {
    return timesOut;
  }

  public boolean finished() {
    return this == COMPLETED || this == CANCELLED;
  }
}
