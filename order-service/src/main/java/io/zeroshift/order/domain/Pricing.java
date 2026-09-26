package io.zeroshift.order.domain;

import io.zeroshift.contracts.Money;
import io.zeroshift.contracts.OrderLine;
import java.math.BigDecimal;
import java.util.List;

/**
 * An order's amounts, exactly as ADR 021 defines them, at scale 2 rounded half-even:
 *
 * <pre>
 * subtotal = Σ unit price × quantity
 * discount = FIXED: min(value, subtotal) | PERCENTAGE: round(subtotal × value / 100),
 *            then capped by the voucher's maximum discount
 * tax      = round((subtotal − discount) × tax rate)
 * total    = subtotal − discount + tax
 * </pre>
 *
 * The discount is split across the lines in proportion to their subtotals, in whole cents, so the
 * lines always add up to the order. Pure: the same lines, voucher and rate always price the same.
 */
public final class Pricing {
  public record Priced(
      List<OrderLine> lines,
      String currency,
      BigDecimal subtotal,
      BigDecimal discount,
      BigDecimal taxRate,
      BigDecimal tax,
      BigDecimal total) {}

  private final String currency;
  private final BigDecimal taxRate;

  public Pricing(String currency, BigDecimal taxRate) {
    if (!Money.isCurrency(currency)) throw new IllegalArgumentException("Bad currency " + currency);
    if (taxRate.signum() < 0 || taxRate.compareTo(BigDecimal.ONE) >= 0)
      throw new IllegalArgumentException("Tax rate must be in [0, 1): " + taxRate);
    this.currency = currency;
    this.taxRate = taxRate.setScale(4, Money.ROUNDING);
  }

  public String currency() {
    return currency;
  }

  /**
   * @param lines undiscounted lines, priced from the catalog
   * @param voucher null for none; its validity is checked by the caller, its minimum amount here
   */
  public Priced price(List<OrderLine> lines, Voucher voucher) {
    if (lines == null || lines.isEmpty()) throw new OrderRuleViolation("An order needs a line");
    var subtotal =
        Money.of(lines.stream().map(OrderLine::subtotal).reduce(Money.ZERO, BigDecimal::add));
    var discount = discount(subtotal, voucher);
    var shares = Money.allocate(discount, lines.stream().map(OrderLine::subtotal).toList());
    var discounted = new java.util.ArrayList<OrderLine>(lines.size());
    for (int i = 0; i < lines.size(); i++) discounted.add(lines.get(i).withDiscount(shares.get(i)));
    var tax = Money.of(subtotal.subtract(discount).multiply(taxRate));
    return new Priced(
        List.copyOf(discounted),
        currency,
        subtotal,
        discount,
        taxRate,
        tax,
        subtotal.subtract(discount).add(tax));
  }

  static BigDecimal discount(BigDecimal subtotal, Voucher voucher) {
    if (voucher == null) return Money.ZERO;
    if (subtotal.compareTo(voucher.minimumAmount()) < 0)
      throw new VoucherNotApplicable(
          "Voucher "
              + voucher.code()
              + " needs a subtotal of at least "
              + Money.of(voucher.minimumAmount()));
    var discount =
        switch (voucher.discountType()) {
          case FIXED -> voucher.value().min(subtotal);
          case PERCENTAGE -> subtotal.multiply(voucher.value()).movePointLeft(2);
        };
    discount = Money.of(discount);
    if (voucher.maximumDiscount() != null) discount = discount.min(voucher.maximumDiscount());
    return Money.of(discount.min(subtotal));
  }
}
