package io.zeroshift.order.application;

/** Every use of a limited voucher is taken, possibly by an order placed a moment earlier. */
public final class VoucherExhausted extends RuntimeException {
  public VoucherExhausted(String code) {
    super("Voucher " + code + " has no uses left");
  }
}
