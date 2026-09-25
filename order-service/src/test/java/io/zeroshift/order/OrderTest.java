package io.zeroshift.order;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.contracts.OrderLine;
import io.zeroshift.order.domain.*;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OrderTest {
  private final UUID id = UUID.randomUUID();
  private final List<OrderLine> lines =
      List.of(
          new OrderLine("SKU-MOUSE", 2, new BigDecimal("29.50")),
          new OrderLine("SKU-CABLE", 1, new BigDecimal("9.99")));

  @Test
  void stateIsTheFoldOfItsEvents() {
    var order = Order.empty(id).apply(Order.place(id, "c-1", lines));
    order = order.apply(order.authorizePayment(UUID.randomUUID()));
    order = order.apply(order.reserveStock(UUID.randomUUID()));
    order = order.apply(order.ship("TRK-1", "DHL"));

    assertThat(order.status()).isEqualTo(OrderStatus.SHIPPED);
    assertThat(order.version()).isEqualTo(4);
    assertThat(order.total()).isEqualByComparingTo("68.99");
    assertThat(order.trackingNumber()).isEqualTo("TRK-1");
  }

  @Test
  void commandsAreCheckedAgainstTheCurrentState() {
    var placed = Order.empty(id).apply(Order.place(id, "c-1", lines));

    assertThatThrownBy(() -> placed.reserveStock(UUID.randomUUID()))
        .isInstanceOf(OrderRuleViolation.class)
        .hasMessageContaining("PLACED");
    var shipped = placed.apply(placed.authorizePayment(UUID.randomUUID()));
    var reserved = shipped.apply(shipped.reserveStock(UUID.randomUUID()));
    var done = reserved.apply(reserved.ship("T", "C"));
    assertThatThrownBy(() -> done.cancel("too late", List.of()))
        .isInstanceOf(OrderRuleViolation.class);
    assertThatThrownBy(() -> Order.place(id, "c-1", List.of()))
        .isInstanceOf(OrderRuleViolation.class);
  }

  @Test
  void eventsForAnotherOrderAreRefused() {
    assertThatThrownBy(() -> Order.empty(id).apply(Order.place(UUID.randomUUID(), "c", lines)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
