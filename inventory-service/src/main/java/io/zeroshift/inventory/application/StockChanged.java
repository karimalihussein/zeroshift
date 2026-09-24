package io.zeroshift.inventory.application;

/** Optimistic lock lost: the stock row changed between read and write. Retrying re-reads it. */
public final class StockChanged extends RuntimeException {
  public StockChanged(String message) {
    super(message);
  }
}
