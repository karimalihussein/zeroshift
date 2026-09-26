package io.zeroshift.order.domain;

/** The voucher exists (or not) but cannot discount this order: unknown, off, expired, too small. */
public final class VoucherNotApplicable extends RuntimeException {
  public VoucherNotApplicable(String message) {
    super(message);
  }
}
