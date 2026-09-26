package io.zeroshift.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.zeroshift.contracts.PaymentCommand.AuthorizePayment;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MoneyTest {
  private static List<BigDecimal> amounts(String... values) {
    return java.util.Arrays.stream(values).map(BigDecimal::new).toList();
  }

  @Test
  void roundsHalfEvenToCents() {
    assertThat(Money.of("2.345")).isEqualByComparingTo("2.34");
    assertThat(Money.of("2.355")).isEqualByComparingTo("2.36");
    assertThat(Money.of("2.3").scale()).isEqualTo(2);
  }

  @Test
  void anExactAmountRefusesFractionsOfACent() {
    assertThatThrownBy(() -> Money.exact(new BigDecimal("1.005")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void allocationAlwaysAddsUpAndIsDeterministic() {
    var parts = Money.allocate(new BigDecimal("10.00"), amounts("1.00", "1.00", "1.00"));
    assertThat(parts)
        .containsExactly(new BigDecimal("3.34"), new BigDecimal("3.33"), new BigDecimal("3.33"));
    var weighted = Money.allocate(new BigDecimal("15.00"), amounts("89.00", "29.50", "9.99"));
    assertThat(weighted.stream().reduce(BigDecimal.ZERO, BigDecimal::add))
        .isEqualByComparingTo("15.00");
    assertThat(weighted)
        .isEqualTo(Money.allocate(new BigDecimal("15.00"), amounts("89.00", "29.50", "9.99")));
    assertThat(Money.allocate(new BigDecimal("0.00"), amounts("5.00", "7.00")))
        .containsExactly(new BigDecimal("0.00"), new BigDecimal("0.00"));
  }

  @Test
  void theSagaKeyAuthorizesAnOrderOnce() {
    var order = UUID.randomUUID();
    assertThat(new AuthorizePayment(order, BigDecimal.TEN, "USD", null).idempotencyKey())
        .isEqualTo("order:" + order + ":authorize");
  }
}
