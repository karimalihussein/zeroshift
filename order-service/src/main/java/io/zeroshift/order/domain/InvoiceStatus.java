package io.zeroshift.order.domain;

/** Issued with the order, paid when its payment is authorized, voided when it is cancelled. */
public enum InvoiceStatus {
  ISSUED,
  PAID,
  VOIDED
}
