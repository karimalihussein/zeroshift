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
  void generatedJooqClassesMatchTheMigratedSchema(@Autowired org.jooq.DSLContext db) {
    io.zeroshift.platform.testing.GeneratedSchema.assertMatchesDatabase(
        db, io.zeroshift.platform.db.Public.PUBLIC, io.zeroshift.query.db.Public.PUBLIC);
  }

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

  @Test
  void projectsACancellationWithItsCompensationsAndIgnoresAnUnknownOrder() {
    var id = UUID.randomUUID();
    var placed =
        Envelope.of(
            new OrderPlaced(
                id,
                "cust-cancel",
                List.of(new OrderLine("SKU-CABLE", 3, new BigDecimal("9.99"))),
                new BigDecimal("29.97"),
                "USD"),
            UUID.randomUUID(),
            null);
    send(kafka, placed);
    send(kafka, placed.reply(new OrderPaymentAuthorized(id, UUID.randomUUID())));
    // A '|' inside a compensation must survive: the array is bound as an array, not a string.
    var compensations = List.of("payment refunded | card ending 4242", "stock released");
    send(kafka, placed.reply(new OrderCancelled(id, "Stock rejected: none left", compensations)));
    var unknown = UUID.randomUUID();
    send(kafka, Envelope.of(new OrderShipped(unknown, "ZS-X", "ZeroShift Express"), unknown, null));

    await().atMost(WAIT).until(() -> "CANCELLED".equals(status(id)));
    var row =
        jdbc.queryForMap(
            "SELECT cancel_reason, compensations::text AS compensations, item_count, events_applied"
                + " FROM order_view WHERE order_id=?",
            id);
    assertThat(row.get("cancel_reason")).isEqualTo("Stock rejected: none left");
    assertThat(row.get("compensations"))
        .isEqualTo("{\"payment refunded | card ending 4242\",\"stock released\"}");
    assertThat(row).containsEntry("item_count", 3).containsEntry("events_applied", 3);
    assertThat(
            jdbc.queryForMap(
                "SELECT orders_placed, orders_shipped, orders_cancelled, shipped_value"
                    + " FROM customer_summary WHERE customer_id='cust-cancel'"))
        .containsEntry("orders_placed", 1)
        .containsEntry("orders_shipped", 0)
        .containsEntry("orders_cancelled", 1)
        .hasEntrySatisfying("shipped_value", v -> assertThat((BigDecimal) v).isZero());

    await()
        .atMost(WAIT)
        .until(
            () ->
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM consumer_decision WHERE order_id=? AND decision='IGNORED'",
                        Integer.class,
                        unknown.toString())
                    == 1);
    assertThat(status(unknown)).isNull();
  }

  @org.springframework.boot.test.web.server.LocalServerPort int port;

  @Test
  void aConsistencyTokenReadWaitsForTheProjectionInsteadOfServingAStaleAnswer() throws Exception {
    var id = UUID.randomUUID();
    var placed =
        Envelope.of(
            new OrderPlaced(
                id,
                "cust-token",
                List.of(new OrderLine("SKU-CABLE", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                "USD"),
            UUID.randomUUID(),
            null);
    send(kafka, placed);
    await().atMost(WAIT).until(() -> "PLACED".equals(status(id)));
    var http = java.net.http.HttpClient.newHttpClient();
    java.util.function.Function<String, java.net.http.HttpResponse<String>> get =
        query -> {
          try {
            return http.send(
                java.net.http.HttpRequest.newBuilder(
                        java.net.URI.create("http://localhost:" + port + "/orders/" + id + query))
                    .build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        };

    // The write side is at version 2 but the projection has applied 1: the token read says so.
    var behind = get.apply("?minVersion=2&waitMs=300");
    assertThat(behind.statusCode()).isEqualTo(409);
    assertThat(behind.headers().firstValue("X-Projected-Version")).hasValue("1");
    assertThat(behind.body()).contains("\"requiredVersion\":2", "\"projectedVersion\":1");

    // A token read waiting for version 2 returns as soon as the projection gets there.
    var waiting =
        java.util.concurrent.CompletableFuture.supplyAsync(
            () -> get.apply("?minVersion=2&waitMs=5000"));
    Thread.sleep(300);
    send(kafka, placed.reply(new OrderPaymentAuthorized(id, UUID.randomUUID())));
    var answered = waiting.get(10, java.util.concurrent.TimeUnit.SECONDS);
    assertThat(answered.statusCode()).isEqualTo(200);
    assertThat(answered.body()).contains("\"status\":\"PAID\"", "\"events_applied\":2");
    assertThat(Long.parseLong(answered.headers().firstValue("X-Waited-Ms").orElseThrow()))
        .isGreaterThan(0);

    // A token that is already satisfied answers at once.
    assertThat(get.apply("?minVersion=1&waitMs=5000").headers().firstValue("X-Waited-Ms"))
        .hasValueSatisfying(ms -> assertThat(Long.parseLong(ms)).isLessThan(100));
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
