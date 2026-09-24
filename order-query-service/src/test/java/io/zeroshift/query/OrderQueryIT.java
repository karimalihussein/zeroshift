package io.zeroshift.query;

import static io.zeroshift.platform.testing.ServiceTestSupport.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zeroshift.contracts.*;
import io.zeroshift.contracts.OrderEvent.*;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderQueryIT {
  @DynamicPropertySource
  static void stack(DynamicPropertyRegistry registry) {
    configure(registry, "order_query");
  }

  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired JdbcTemplate jdbc;
  @Autowired ProjectionRebuild rebuild;

  @Test
  void projectsTheLifecycleAndRebuildsTheSameViewByReplaying() throws Exception {
    var id = UUID.randomUUID();
    var placed =
        Envelope.of(
            new OrderPlaced(
                id,
                "cust-9",
                List.of(new OrderLine("SKU-MOUSE", 2, new BigDecimal("29.50"))),
                new BigDecimal("59.00"),
                "USD"),
            UUID.randomUUID(),
            null);
    send(kafka, placed);
    send(kafka, placed.reply(new OrderPaymentAuthorized(id, UUID.randomUUID())));
    send(kafka, placed.reply(new OrderStockReserved(id, UUID.randomUUID())));
    var shipped = placed.reply(new OrderShipped(id, "ZS-1", "ZeroShift Express"));
    send(kafka, shipped);
    send(kafka, shipped); // duplicate delivery must not double-count

    await().atMost(WAIT).until(() -> "SHIPPED".equals(status(id)));
    await().atMost(WAIT).until(() -> decisions("DUPLICATE_SKIPPED") >= 1);
    assertThat(shippedValue("cust-9")).isEqualByComparingTo("59.00");

    rebuild.rebuild();
    assertThat(status(id)).isNull(); // truncated, now replaying from offset 0
    await().atMost(WAIT).until(() -> "SHIPPED".equals(status(id)));
    assertThat(shippedValue("cust-9")).isEqualByComparingTo("59.00");
    assertThat(
            jdbc.queryForObject(
                "SELECT events_applied FROM order_view WHERE order_id=?", Integer.class, id))
        .isEqualTo(4);
  }

  private String status(UUID id) {
    return jdbc
        .queryForList("SELECT status FROM order_view WHERE order_id=?", String.class, id)
        .stream()
        .findFirst()
        .orElse(null);
  }

  private BigDecimal shippedValue(String customer) {
    return jdbc.queryForObject(
        "SELECT shipped_value FROM customer_summary WHERE customer_id=?",
        BigDecimal.class,
        customer);
  }

  private int decisions(String decision) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM consumer_decision WHERE decision=?", Integer.class, decision);
  }
}
