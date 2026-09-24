package io.zeroshift.domain;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class OrderEditTest {
  @Test
  void rejectsInvalidAmountsAndTextBeforeDatabaseWrites() {
    assertThatThrownBy(() -> new OrderEdit("", BigDecimal.ONE, "PENDING"))
        .isInstanceOf(InvalidAction.class);
    assertThatThrownBy(() -> new OrderEdit("Customer", new BigDecimal("-1"), "PENDING"))
        .isInstanceOf(InvalidAction.class);
    assertThatThrownBy(() -> new OrderEdit("Customer", new BigDecimal("0.00001"), "PENDING"))
        .isInstanceOf(InvalidAction.class);
    assertThatThrownBy(
            () -> new OrderEdit("Customer", new BigDecimal("1000000000000000"), "PENDING"))
        .isInstanceOf(InvalidAction.class);
    assertThatThrownBy(() -> new OrderEdit("Customer", BigDecimal.ONE, "<script>"))
        .isInstanceOf(InvalidAction.class);
  }

  @Test
  void comparesExactDecimalValuesWithoutScaleFalsePositives() {
    var a = new OrderRecord(1, 2, "Customer", new BigDecimal("500.0000"), "COMPLETED");
    assertThat(a.sameValues(new OrderRecord(1, 2, "Customer", new BigDecimal("500"), "COMPLETED")))
        .isTrue();
    assertThat(
            a.sameValues(
                new OrderRecord(1, 2, "Customer", new BigDecimal("500.0001"), "COMPLETED")))
        .isFalse();
  }
}
