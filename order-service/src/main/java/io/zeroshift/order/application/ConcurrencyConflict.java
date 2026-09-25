package io.zeroshift.order.application;

/**
 * Someone else wrote first: the stream or saga is no longer at the version this write was based on.
 * Nothing was written. Retrying reloads the new state and decides again.
 */
public final class ConcurrencyConflict extends RuntimeException {
  public ConcurrencyConflict(String message) {
    super(message);
  }
}
