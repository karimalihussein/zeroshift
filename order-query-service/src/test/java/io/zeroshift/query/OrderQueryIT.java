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
  @Autowired ReadModels readModels;

  @Test
  void generatedJooqClassesMatchTheMigratedSchema(@Autowired org.jooq.DSLContext db) {
    io.zeroshift.platform.testing.GeneratedSchema.assertMatchesDatabase(
        db, io.zeroshift.platform.db.Public.PUBLIC, io.zeroshift.query.db.Public.PUBLIC);
  }

  @Test
  void projectsTheLifecycleAndRebuildsTheSameViewByReplaying() throws Exception {
    var id = UUID.randomUUID();
    var customer = UUID.randomUUID().toString();
    // 59.00 + 9.99 = 68.99, voucher SAVE10 takes 10.00 (split 8.55 / 1.45 by subtotal),
    // tax 8% of 58.99 = 4.72, total 63.71.
    var lines =
        List.of(
            OrderLine.of(UUID.randomUUID(), "SKU-MOUSE", "Mouse", 2, new BigDecimal("29.50"))
                .withDiscount(new BigDecimal("8.55")),
            OrderLine.of(UUID.randomUUID(), "SKU-CABLE", "Cable", 1, new BigDecimal("9.99"))
                .withDiscount(new BigDecimal("1.45")));
    var placed =
        Envelope.of(
            new OrderPlaced(
                id,
                customer,
                "Ada Lovelace",
                lines,
                "USD",
                new BigDecimal("68.99"),
                new BigDecimal("10.00"),
                new BigDecimal("0.0800"),
                new BigDecimal("4.72"),
                new BigDecimal("63.71"),
                "SAVE10",
                "INV-2026-000001"),
            UUID.randomUUID(),
            null);
    send(kafka, placed);
    await().atMost(WAIT).until(() -> "PLACED".equals(status(id)));
    var view = readModels.order(id).orElseThrow();
    assertThat(view.customerName()).isEqualTo("Ada Lovelace");
    assertThat(view.currency()).isEqualTo("USD");
    assertThat(view.subtotal()).isEqualByComparingTo("68.99");
    assertThat(view.discount()).isEqualByComparingTo("10.00");
    assertThat(view.taxRate()).isEqualByComparingTo("0.0800");
    assertThat(view.tax()).isEqualByComparingTo("4.72");
    assertThat(view.total()).isEqualByComparingTo("63.71");
    assertThat(view.voucherCode()).isEqualTo("SAVE10");
    assertThat(view.invoiceNumber()).isEqualTo("INV-2026-000001");
    assertThat(view.invoiceStatus()).isEqualTo("ISSUED");
    assertThat(view.itemCount()).isEqualTo(3);
    assertThat(view.items()).isEqualTo(lines);

    send(kafka, placed.reply(new OrderPaymentAuthorized(id, UUID.randomUUID())));
    await().atMost(WAIT).until(() -> "PAID".equals(status(id)));
    assertThat(invoiceStatus(id)).isEqualTo("PAID");
    send(kafka, placed.reply(new OrderStockReserved(id, UUID.randomUUID())));
    var shipped = placed.reply(new OrderShipped(id, "ZS-1", "ZeroShift Express"));
    send(kafka, shipped);
    send(kafka, shipped); // duplicate delivery must not double-count

    await().atMost(WAIT).until(() -> "SHIPPED".equals(status(id)));
    await().atMost(WAIT).until(() -> decisions("DUPLICATE_SKIPPED") >= 1);
    assertThat(readModels.order(id).orElseThrow().carrier()).isEqualTo("ZeroShift Express");
    assertThat(shippedValue(customer)).isEqualByComparingTo("63.71");
    assertThat(readModels.customers())
        .filteredOn(c -> c.customerId().equals(customer))
        .singleElement()
        .satisfies(
            c -> {
              assertThat(c.customerName()).isEqualTo("Ada Lovelace");
              assertThat(c.currency()).isEqualTo("USD");
              assertThat(c.ordersPlaced()).isEqualTo(1);
              assertThat(c.ordersShipped()).isEqualTo(1);
            });

    rebuild.rebuild();
    assertThat(status(id)).isNull(); // truncated, now replaying from offset 0
    await().atMost(WAIT).until(() -> "SHIPPED".equals(status(id)));
    assertThat(shippedValue(customer)).isEqualByComparingTo("63.71");
    assertThat(invoiceStatus(id)).isEqualTo("PAID");
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
            undiscounted(
                id,
                "cust-cancel",
                "INV-2026-000002",
                new OrderLine("SKU-CABLE", 3, new BigDecimal("9.99"))),
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
            "SELECT cancel_reason, compensations::text AS compensations, item_count, events_applied,"
                + " invoice_status FROM order_view WHERE order_id=?",
            id);
    assertThat(row.get("cancel_reason")).isEqualTo("Stock rejected: none left");
    assertThat(row.get("compensations"))
        .isEqualTo("{\"payment refunded | card ending 4242\",\"stock released\"}");
    assertThat(row)
        .containsEntry("item_count", 3)
        .containsEntry("events_applied", 3)
        .containsEntry("invoice_status", "VOIDED");
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

  @Test
  void projectsAnOrderPlacedWrittenBeforeTheCommerceModel() {
    var id = UUID.randomUUID();
    // Schema v2, as the event store holds it: a free-text customer, bare lines, only a total.
    var v2 =
        """
        {"eventId":"%s","type":"OrderPlaced","schemaVersion":2,"correlationId":"%s",\
        "causationId":null,"occurredAt":"2026-01-10T09:00:00Z","payload":{"orderId":"%s",\
        "customerId":"old-customer","lines":[{"sku":"SKU-1","quantity":2,"unitPrice":10.50}],\
        "total":21.00,"currency":"USD"}}"""
            .formatted(UUID.randomUUID(), UUID.randomUUID(), id);
    kafka.send(Topics.ORDER_EVENTS, id.toString(), v2).join();
    send(kafka, Envelope.of(new OrderPaymentAuthorized(id, UUID.randomUUID()), id, null));

    await().atMost(WAIT).until(() -> "PAID".equals(status(id)));
    var view = readModels.order(id).orElseThrow();
    assertThat(view.customerId()).isEqualTo("old-customer");
    assertThat(view.customerName()).isEqualTo("old-customer");
    assertThat(view.subtotal()).isEqualByComparingTo("21.00");
    assertThat(view.discount()).isZero();
    assertThat(view.tax()).isZero();
    assertThat(view.total()).isEqualByComparingTo("21.00");
    assertThat(view.invoiceNumber()).isNull();
    assertThat(view.invoiceStatus()).isNull(); // no invoice existed, so none was paid
    assertThat(view.items())
        .singleElement()
        .satisfies(
            line -> {
              assertThat(line.name()).isEqualTo("SKU-1");
              assertThat(line.total()).isEqualByComparingTo("21.00");
            });
  }

  @org.springframework.boot.test.web.server.LocalServerPort int port;

  @Test
  void aConsistencyTokenReadWaitsForTheProjectionInsteadOfServingAStaleAnswer() throws Exception {
    var id = UUID.randomUUID();
    var placed =
        Envelope.of(
            undiscounted(
                id, "cust-token", null, new OrderLine("SKU-CABLE", 1, new BigDecimal("9.99"))),
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
    assertThat(behind.headers().firstValue("Content-Type")).hasValue("application/problem+json");
    assertThat(behind.body())
        .contains(
            "\"code\":\"READ_MODEL_BEHIND\"", "\"requiredVersion\":2", "\"projectedVersion\":1");

    // A token read waiting for version 2 returns as soon as the projection gets there.
    var waiting =
        java.util.concurrent.CompletableFuture.supplyAsync(
            () -> get.apply("?minVersion=2&waitMs=5000"));
    Thread.sleep(300);
    send(kafka, placed.reply(new OrderPaymentAuthorized(id, UUID.randomUUID())));
    var answered = waiting.get(10, java.util.concurrent.TimeUnit.SECONDS);
    assertThat(answered.statusCode()).isEqualTo(200);
    assertThat(answered.body()).contains("\"status\":\"PAID\"", "\"eventsApplied\":2");
    assertThat(Long.parseLong(answered.headers().firstValue("X-Waited-Ms").orElseThrow()))
        .isGreaterThan(0);

    // A token that is already satisfied answers at once.
    assertThat(get.apply("?minVersion=1&waitMs=5000").headers().firstValue("X-Waited-Ms"))
        .hasValueSatisfying(ms -> assertThat(Long.parseLong(ms)).isLessThan(100));
  }

  @Test
  void theQueryApiUsesTheSharedContract() throws Exception {
    var http = java.net.http.HttpClient.newHttpClient();
    java.util.function.Function<String, java.net.http.HttpResponse<String>> get =
        path -> {
          try {
            return http.send(
                java.net.http.HttpRequest.newBuilder(
                        java.net.URI.create("http://localhost:" + port + path))
                    .header("X-Request-Id", "it-query-1")
                    .build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        };
    var missing = get.apply("/orders/" + UUID.randomUUID());
    assertThat(missing.statusCode()).isEqualTo(404);
    assertThat(missing.body())
        .contains("\"code\":\"ORDER_NOT_PROJECTED\"", "\"requestId\":\"it-query-1\"");
    assertThat(get.apply("/orders?limit=0").body()).contains("\"code\":\"VALIDATION_FAILED\"");
    assertThat(get.apply("/orders?limit=5").body())
        .startsWith("{\"data\":[")
        .contains("\"meta\":{");
    assertThat(get.apply("/customers").statusCode()).isEqualTo(200);
  }

  /** A v3 order without voucher or tax. */
  private static OrderPlaced undiscounted(
      UUID id, String customer, String invoiceNumber, OrderLine... lines) {
    var subtotal =
        java.util.Arrays.stream(lines)
            .map(OrderLine::subtotal)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
    return new OrderPlaced(
        id,
        customer,
        customer,
        List.of(lines),
        "USD",
        subtotal,
        BigDecimal.ZERO,
        BigDecimal.ZERO,
        BigDecimal.ZERO,
        subtotal,
        null,
        invoiceNumber);
  }

  private String invoiceStatus(UUID id) {
    return jdbc.queryForObject(
        "SELECT invoice_status FROM order_view WHERE order_id=?", String.class, id);
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
