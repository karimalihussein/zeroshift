package io.zeroshift.order.application;

/** The catalog did not answer in time or at all: no price, so no order. */
public final class CatalogUnavailable extends RuntimeException {
  public CatalogUnavailable(String message, Throwable cause) {
    super(message, cause);
  }
}
