package io.zeroshift.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * A discount code. {@code usageCount} is how many placed orders hold one of its {@code usageLimit}
 * uses (null: unlimited); a cancelled order gives its use back. What a voucher takes off an order
 * is {@link Pricing}'s business; this only says whether it can be used at all.
 */
public record Voucher(
    UUID id,
    String code,
    DiscountType discountType,
    BigDecimal value,
    BigDecimal minimumAmount,
    BigDecimal maximumDiscount,
    Integer usageLimit,
    int usageCount,
    Instant validFrom,
    Instant validUntil,
    boolean active) {
  public enum DiscountType {
    FIXED,
    PERCENTAGE
  }

  /** Codes are stored upper case; a customer may type them in any case. */
  public static String normalize(String code) {
    return code == null || code.isBlank() ? null : code.strip().toUpperCase(java.util.Locale.ROOT);
  }

  /** Throws unless the voucher is switched on and {@code now} is inside its validity. */
  public void requireUsableAt(Instant now) {
    if (!active) throw new VoucherNotApplicable("Voucher " + code + " is not active");
    if (now.isBefore(validFrom) || (validUntil != null && !now.isBefore(validUntil)))
      throw new VoucherNotApplicable("Voucher " + code + " is not valid now");
  }

  public boolean exhausted() {
    return usageLimit != null && usageCount >= usageLimit;
  }
}
