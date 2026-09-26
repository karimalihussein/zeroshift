package io.zeroshift.order.domain;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.contracts.OrderLine;
import io.zeroshift.order.domain.Voucher.DiscountType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PricingTest {
  private final Pricing pricing = new Pricing("USD", new BigDecimal("0.0800"));

  private static OrderLine line(String sku, int quantity, String price) {
    return OrderLine.of(UUID.randomUUID(), sku, sku + " name", quantity, new BigDecimal(price));
  }

  private static Voucher voucher(DiscountType type, String value, String minimum, String max) {
    return new Voucher(
        UUID.randomUUID(),
        "CODE",
        type,
        new BigDecimal(value),
        new BigDecimal(minimum),
        max == null ? null : new BigDecimal(max),
        null,
        0,
        Instant.EPOCH,
        null,
        true);
  }

  private final List<OrderLine> cart = List.of(line("MOUSE", 2, "29.50"), line("CABLE", 1, "9.99"));

  @Test
  void withoutAVoucherTaxIsChargedOnTheSubtotal() {
    var p = pricing.price(cart, null);
    assertThat(p.subtotal()).isEqualByComparingTo("68.99");
    assertThat(p.discount()).isEqualByComparingTo("0.00");
    assertThat(p.tax()).isEqualByComparingTo("5.52"); // 5.5192
    assertThat(p.total()).isEqualByComparingTo("74.51");
    assertThat(p.taxRate()).isEqualByComparingTo("0.08");
    assertThat(p.currency()).isEqualTo("USD");
    assertThat(p.lines()).extracting(OrderLine::discount).allMatch(d -> d.signum() == 0);
  }

  @Test
  void aPercentageVoucherIsRoundedThenSplitAcrossTheLinesByTheirSubtotals() {
    var p = pricing.price(cart, voucher(DiscountType.PERCENTAGE, "10", "0", null));
    assertThat(p.discount()).isEqualByComparingTo("6.90"); // 6.899
    // 690 cents in proportion 59.00 : 9.99 → 590.09 and 99.91 → 590 and 100 (largest remainder).
    assertThat(p.lines()).extracting(OrderLine::discount).containsExactly(bd("5.90"), bd("1.00"));
    assertThat(p.lines()).extracting(OrderLine::total).containsExactly(bd("53.10"), bd("8.99"));
    assertThat(p.tax()).isEqualByComparingTo("4.97"); // 62.09 × 0.08 = 4.9672
    assertThat(p.total()).isEqualByComparingTo("67.06");
  }

  @Test
  void aFixedVoucherNeedsItsMinimumAmount() {
    var save15 = voucher(DiscountType.FIXED, "15.00", "100.00", null);
    assertThatThrownBy(() -> pricing.price(cart, save15))
        .isInstanceOf(VoucherNotApplicable.class)
        .hasMessageContaining("100.00");

    var p = pricing.price(List.of(line("MOUSE", 4, "29.50")), save15);
    assertThat(p.discount()).isEqualByComparingTo("15.00");
    assertThat(p.tax()).isEqualByComparingTo("8.24"); // 103.00 × 0.08
    assertThat(p.total()).isEqualByComparingTo("111.24");
  }

  @Test
  void aFixedVoucherNeverTakesMoreThanTheSubtotal() {
    var p =
        pricing.price(
            List.of(line("CABLE", 1, "9.99")), voucher(DiscountType.FIXED, "100", "0", null));
    assertThat(p.discount()).isEqualByComparingTo("9.99");
    assertThat(p.tax()).isEqualByComparingTo("0.00");
    assertThat(p.total()).isEqualByComparingTo("0.00");
  }

  @Test
  void theMaximumDiscountCapsAPercentage() {
    var big20 = voucher(DiscountType.PERCENTAGE, "20", "0", "50.00");
    var p = pricing.price(List.of(line("MONITOR", 2, "329.00")), big20);
    assertThat(p.discount()).isEqualByComparingTo("50.00"); // 20% would be 131.60
    assertThat(pricing.price(List.of(line("MOUSE", 1, "29.50")), big20).discount())
        .isEqualByComparingTo("5.90"); // under the cap
  }

  @Test
  void roundingIsHalfEven() {
    var tenPercent = voucher(DiscountType.PERCENTAGE, "10", "0", null);
    assertThat(pricing.price(List.of(line("A", 1, "0.25")), tenPercent).discount())
        .isEqualByComparingTo("0.02"); // 0.025
    assertThat(pricing.price(List.of(line("A", 1, "0.35")), tenPercent).discount())
        .isEqualByComparingTo("0.04"); // 0.035
    var fivePercentTax = new Pricing("EUR", new BigDecimal("0.05"));
    assertThat(fivePercentTax.price(List.of(line("A", 1, "0.10")), null).tax())
        .isEqualByComparingTo("0.00"); // 0.005
    assertThat(fivePercentTax.price(List.of(line("A", 1, "0.30")), null).tax())
        .isEqualByComparingTo("0.02"); // 0.015
  }

  @Test
  void aTieInTheSplitGoesToTheEarlierLine() {
    var three = List.of(line("A", 1, "1.00"), line("B", 1, "1.00"), line("C", 1, "1.00"));
    var p = pricing.price(three, voucher(DiscountType.FIXED, "0.10", "0", null));
    assertThat(p.lines())
        .extracting(OrderLine::discount)
        .containsExactly(bd("0.04"), bd("0.03"), bd("0.03"));
  }

  @Test
  void theLinesAlwaysAddUpToTheOrder() {
    var random = new Random(19);
    for (int run = 0; run < 2_000; run++) {
      var lines = new ArrayList<OrderLine>();
      for (int i = 0, n = 1 + random.nextInt(5); i < n; i++)
        lines.add(
            line(
                "SKU" + i,
                1 + random.nextInt(9),
                BigDecimal.valueOf(random.nextInt(50_000), 2).toPlainString()));
      var voucher =
          random.nextBoolean()
              ? voucher(DiscountType.PERCENTAGE, String.valueOf(1 + random.nextInt(100)), "0", null)
              : voucher(
                  DiscountType.FIXED,
                  BigDecimal.valueOf(1 + random.nextInt(20_000), 2).toPlainString(),
                  "0",
                  null);

      var p = pricing.price(lines, voucher);

      assertThat(sum(p.lines().stream().map(OrderLine::discount).toList()))
          .isEqualByComparingTo(p.discount());
      assertThat(sum(p.lines().stream().map(OrderLine::subtotal).toList()))
          .isEqualByComparingTo(p.subtotal());
      assertThat(p.discount()).isBetween(BigDecimal.ZERO, p.subtotal());
      assertThat(p.total()).isEqualByComparingTo(p.subtotal().subtract(p.discount()).add(p.tax()));
      assertThat(p.total().scale()).isEqualTo(2);
    }
  }

  @Test
  void anOrderNeedsALine() {
    assertThatThrownBy(() -> pricing.price(List.of(), null)).isInstanceOf(OrderRuleViolation.class);
  }

  private static BigDecimal bd(String value) {
    return new BigDecimal(value);
  }

  private static BigDecimal sum(List<BigDecimal> values) {
    return values.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
  }
}
