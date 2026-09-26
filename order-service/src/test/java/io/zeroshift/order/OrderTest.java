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
  private final Pricing.Priced priced =
      new Pricing("USD", new BigDecimal("0.0800"))
          .price(
              List.of(
                  OrderLine.of(
                      UUID.randomUUID(), "SKU-MOUSE", "Wireless mouse", 2, new BigDecimal("29.50")),
                  OrderLine.of(
                      UUID.randomUUID(), "SKU-CABLE", "USB-C cable", 1, new BigDecimal("9.99"))),
              null);

  private Order placed() {
    return Order.empty(id)
        .apply(Order.place(id, "c-1", "Ada Lovelace", priced, null, "INV-2026-000001"));
  }

  @Test
  void stateIsTheFoldOfItsEvents() {
    var order = placed();
    order = order.apply(order.authorizePayment(UUID.randomUUID()));
    order = order.apply(order.reserveStock(UUID.randomUUID()));
    order = order.apply(order.ship("TRK-1", "DHL"));

    assertThat(order.status()).isEqualTo(OrderStatus.SHIPPED);
    assertThat(order.version()).isEqualTo(4);
    assertThat(order.subtotal()).isEqualByComparingTo("68.99");
    assertThat(order.tax()).isEqualByComparingTo("5.52");
    assertThat(order.total()).isEqualByComparingTo("74.51");
    assertThat(order.customerName()).isEqualTo("Ada Lovelace");
    assertThat(order.invoiceNumber()).isEqualTo("INV-2026-000001");
    assertThat(order.lines())
        .extracting(OrderLine::name)
        .containsExactly("Wireless mouse", "USB-C cable");
    assertThat(order.trackingNumber()).isEqualTo("TRK-1");
  }

  @Test
  void commandsAreCheckedAgainstTheCurrentState() {
    var placed = placed();

    assertThatThrownBy(() -> placed.reserveStock(UUID.randomUUID()))
        .isInstanceOf(OrderRuleViolation.class)
        .hasMessageContaining("PLACED");
    var paid = placed.apply(placed.authorizePayment(UUID.randomUUID()));
    var reserved = paid.apply(paid.reserveStock(UUID.randomUUID()));
    var done = reserved.apply(reserved.ship("T", "C"));
    assertThatThrownBy(() -> done.cancel("too late", List.of()))
        .isInstanceOf(OrderRuleViolation.class);
    assertThatThrownBy(() -> Order.place(id, " ", "x", priced, null, null))
        .isInstanceOf(OrderRuleViolation.class);
  }

  @Test
  void eventsForAnotherOrderAreRefused() {
    assertThatThrownBy(
            () ->
                Order.empty(id).apply(Order.place(UUID.randomUUID(), "c", "C", priced, null, null)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
