package io.zeroshift.order;

import static io.zeroshift.platform.testing.CommerceStack.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zeroshift.contracts.*;
import io.zeroshift.contracts.InventoryEvent.*;
import io.zeroshift.contracts.PaymentEvent.*;
import io.zeroshift.contracts.ShippingEvent.*;
import io.zeroshift.order.application.*;
import io.zeroshift.order.domain.*;
import io.zeroshift.order.infrastructure.PostgresLease;
import io.zeroshift.platform.Outbox;
import io.zeroshift.platform.testing.CommerceStack;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import org.jooq.DSLContext;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The order service against real PostgreSQL, Kafka and Debezium. Participants are played by the
 * test: it publishes their replies to Kafka and reads the commands Debezium relays.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderServiceIT {
  private static final Duration WAIT = Duration.ofSeconds(60);

  /** 2 × 29.50 + 9.99 = 68.99, plus 8 % tax (5.52) = 74.51. */
  private static final List<PlaceOrder.Item> ITEMS =
      List.of(new PlaceOrder.Item("SKU-MOUSE", 2), new PlaceOrder.Item("SKU-CABLE", 1));

  private static final TestCatalog CATALOG = new TestCatalog();

  @DynamicPropertySource
  static void stack(DynamicPropertyRegistry registry) {
    CommerceStack.start();
    registry.add("spring.datasource.url", () -> createDatabase("orders"));
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("order.step-timeout", () -> "6s");
    registry.add("commerce.catalog-url", CATALOG::url);
  }

  @Autowired PlaceOrder placeOrder;
  @Autowired OrderRepository orders;
  @Autowired SagaStore sagas;
  @Autowired JdbcTemplate jdbc;
  @Autowired DSLContext db;
  @Autowired EventStore eventStore;
  @Autowired Outbox outbox;
  @Autowired TransactionTemplate transactions;
  @Autowired KafkaTemplate<String, String> kafka;

  /** A customer of its own for each test, so no test sees another's orders. */
  private UUID customer;

  @BeforeEach
  void aCustomer() {
    customer = customer("Test Customer " + UUID.randomUUID());
  }

  @Test
  void generatedJooqClassesMatchTheMigratedSchema(@Autowired org.jooq.DSLContext db) {
    io.zeroshift.platform.testing.GeneratedSchema.assertMatchesDatabase(
        db, io.zeroshift.platform.db.Public.PUBLIC, io.zeroshift.order.db.Public.PUBLIC);
  }

  @Test
  @org.junit.jupiter.api.Order(0)
  void demoDataSeedsCustomersAndVouchersOnceIntoAnEmptyDatabase() throws Exception {
    jdbc.update("DELETE FROM customer");
    var seeder = new io.zeroshift.order.infrastructure.DemoData(db, transactions, 7L);
    seeder.run(null);
    seeder.run(null); // not empty any more: nothing is added

    assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM customer", Integer.class)).isEqualTo(40);
    assertThat(
            jdbc.queryForObject("SELECT COUNT(DISTINCT lower(email)) FROM customer", Integer.class))
        .isEqualTo(40);
    assertThat(jdbc.queryForList("SELECT code FROM voucher ORDER BY code", String.class))
        .containsExactly(
            "BIG20", "FLASH5", "LAUNCH30", "SAVE15", "SPRING25", "SUMMER24", "VIP50", "WELCOME10");
  }

  @Test
  @org.junit.jupiter.api.Order(1)
  void anOrderPlacedWhileConnectIsDownIsPublishedWhenItStarts() {
    var placed = placeOrder.place(customer, ITEMS, null);
    assertThat(read(Topics.ORDER_EVENTS, forOrder(placed.orderId()), 1, Duration.ofSeconds(3)))
        .as("no connector yet: nothing can reach Kafka")
        .isEmpty();

    registerOutboxConnector("orders");

    // The replication slot was created by the service's migration, so WAL was retained.
    assertThat(read(Topics.ORDER_EVENTS, forOrder(placed.orderId()), 1, WAIT)).hasSize(1);
  }

  @Test
  @org.junit.jupiter.api.Order(2)
  void placingAnOrderPublishesTheEventAndTheFirstCommandOnTheOrdersPartition() {
    var placed = placeOrder.place(customer, ITEMS, null);

    var event = read(Topics.ORDER_EVENTS, forOrder(placed.orderId()), 1, WAIT).getFirst();
    var command = read(Topics.PAYMENT_COMMANDS, forOrder(placed.orderId()), 1, WAIT).getFirst();

    assertThat(event.key()).isEqualTo(placed.orderId().toString());
    assertThat(header(event, "eventType")).isEqualTo("OrderPlaced");
    assertThat(header(event, "schemaVersion")).isEqualTo("2");
    var placedEnvelope = MessageCodec.decode(event.value());
    assertThat(placedEnvelope.eventId()).isEqualTo(placed.eventId());
    var orderPlaced = (OrderEvent.OrderPlaced) placedEnvelope.payload();
    assertThat(orderPlaced.subtotal()).isEqualByComparingTo("68.99");
    assertThat(orderPlaced.tax()).isEqualByComparingTo("5.52");
    assertThat(orderPlaced.total()).isEqualByComparingTo("74.51");
    assertThat(orderPlaced.customerId()).isEqualTo(customer.toString());
    assertThat(orderPlaced.invoiceNumber()).isEqualTo(placed.invoiceNumber());
    var authorize = MessageCodec.decode(command.value());
    assertThat(authorize.payload())
        .isEqualTo(
            new PaymentCommand.AuthorizePayment(
                placed.orderId(),
                new BigDecimal("74.51"),
                "USD",
                PaymentCommand.AuthorizePayment.keyFor(placed.orderId())));
    assertThat(authorize.correlationId()).isEqualTo(placed.correlationId());
    assertThat(authorize.causationId()).isEqualTo(placed.eventId());
    // Same key, same partition count: every message about this order shares a partition number.
    assertThat(command.partition()).isEqualTo(event.partition());
  }

  @Test
  @org.junit.jupiter.api.Order(3)
  void happyPathCompletesTheSagaAndSnapshotsTheOrder() {
    var id = placeOrder.place(customer, ITEMS, null).orderId();

    reply(id, new PaymentAuthorized(id, UUID.randomUUID(), new BigDecimal("74.51"), "USD"));
    awaitSaga(id, SagaState.AWAITING_STOCK);
    assertThat(read(Topics.INVENTORY_COMMANDS, forOrder(id), 1, WAIT)).hasSize(1);
    reply(id, new StockReserved(id, UUID.randomUUID()));
    awaitSaga(id, SagaState.AWAITING_SHIPMENT);
    reply(id, new ShipmentScheduled(id, "TRK-42", "DHL"));
    awaitSaga(id, SagaState.COMPLETED);

    var fromSnapshot = orders.load(id, true);
    var fullReplay = orders.load(id, false);
    assertThat(fromSnapshot.order().status()).isEqualTo(OrderStatus.SHIPPED);
    assertThat(fromSnapshot.snapshot().version()).isEqualTo(3);
    assertThat(fromSnapshot.replayed()).hasSize(1);
    assertThat(fullReplay.replayed()).hasSize(4);
    assertThat(fromSnapshot.order()).isEqualTo(fullReplay.order());
    // The orders row caches the fold, at the same version.
    var row = orderRow(id);
    assertThat(row)
        .containsEntry("status", "SHIPPED")
        .containsEntry("tracking_number", "TRK-42")
        .containsEntry("version", 4L);
    assertThat(row.get("shipped_at")).isNotNull();
    assertThat(invoiceStatus(id)).isEqualTo("PAID");

    // A slower writer saving an older state never moves the snapshot backwards.
    var older = io.zeroshift.order.domain.Order.empty(id);
    for (var recorded : fullReplay.replayed().subList(0, 2))
      older = older.apply((OrderEvent) recorded.envelope().payload());
    eventStore.saveSnapshot(older);
    assertThat(eventStore.snapshot(id).orElseThrow().version()).isEqualTo(3);

    // A snapshot is only a cache: discarding it changes nothing but how the order is rebuilt.
    eventStore.discardSnapshot(id);
    var withoutSnapshot = orders.load(id, true);
    assertThat(withoutSnapshot.snapshot()).isNull();
    assertThat(withoutSnapshot.replayed()).hasSize(4);
    assertThat(withoutSnapshot.order()).isEqualTo(fullReplay.order());
  }

  @Test
  @org.junit.jupiter.api.Order(4)
  void rejectedStockIsCompensatedByRefundingThePayment() {
    var code = voucher("PAYBACK", "PERCENTAGE", "10", 5);
    var id = placeOrder.place(customer, ITEMS, code).orderId();
    assertThat(usageCount(code)).isOne();
    reply(id, new PaymentAuthorized(id, UUID.randomUUID(), new BigDecimal("67.06"), "USD"));
    awaitSaga(id, SagaState.AWAITING_STOCK);
    assertThat(orderRow(id)).containsEntry("status", "PAID").containsEntry("version", 2L);
    assertThat(orderRow(id).get("payment_id")).isNotNull();
    assertThat(orderRow(id).get("paid_at")).isNotNull();
    assertThat(invoiceStatus(id)).isEqualTo("PAID");

    reply(id, new StockRejected(id, "SKU-MOUSE: 0 left"));
    awaitSaga(id, SagaState.COMPENSATING);
    var refund = read(Topics.PAYMENT_COMMANDS, r -> isType(r, "RefundPayment", id), 1, WAIT);
    assertThat(refund).hasSize(1);
    reply(id, new PaymentRefunded(id, UUID.randomUUID(), new BigDecimal("67.06"), "USD"));
    awaitSaga(id, SagaState.CANCELLED);

    var order = orders.load(id);
    assertThat(order.status()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(order.cancelReason()).contains("Stock rejected");
    assertThat(sagas.find(id).orElseThrow().compensations()).containsExactly("payment refunded");
    // Cancelling voids the invoice and gives the voucher's use back, in the same transaction.
    assertThat(orderRow(id)).containsEntry("status", "CANCELLED").containsEntry("version", 3L);
    assertThat(orderRow(id).get("cancelled_at")).isNotNull();
    assertThat(invoiceStatus(id)).isEqualTo("VOIDED");
    assertThat(usageCount(code)).isZero();
  }

  @Test
  @org.junit.jupiter.api.Order(5)
  void aRedeliveredReplyChangesNothing() {
    var id = placeOrder.place(customer, ITEMS, null).orderId();
    var authorized =
        Envelope.of(
            new PaymentAuthorized(id, UUID.randomUUID(), BigDecimal.ONE, "USD"),
            UUID.randomUUID(),
            null);

    publish(authorized);
    publish(authorized); // the same event id: a redelivery, a replay or a duplicate publish

    await().atMost(WAIT).until(() -> decisions(id, "DUPLICATE_SKIPPED") == 1);
    assertThat(decisions(id, "PROCESSED")).isEqualTo(1);
    assertThat(sagas.transitions(id)).hasSize(2); // started, payment authorized
  }

  @Test
  @org.junit.jupiter.api.Order(6)
  void aPoisonMessageIsDeadLetteredWithoutRetries() {
    var key = "poison-" + UUID.randomUUID();
    kafka.send(Topics.PAYMENT_EVENTS, key, "{this is not an envelope");

    var parked = read(Topics.deadLetter(Topics.PAYMENT_EVENTS), r -> key.equals(r.key()), 1, WAIT);
    assertThat(parked).hasSize(1);
    assertThat(header(parked.getFirst(), "kafka_dlt-exception-message")).contains("Not JSON");
    await()
        .atMost(WAIT)
        .until(
            () ->
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM consumer_decision WHERE order_id=? AND decision='DEAD_LETTERED'",
                        Integer.class,
                        key)
                    == 1);
  }

  @Test
  @org.junit.jupiter.api.Order(7)
  void aMissedDeadlineCompensatesAndALateReplyIsIgnored() {
    var id = placeOrder.place(customer, ITEMS, null).orderId();

    awaitSaga(id, SagaState.COMPENSATING); // nobody answers AuthorizePayment within 6 s
    assertThat(read(Topics.PAYMENT_COMMANDS, r -> isType(r, "RefundPayment", id), 1, WAIT))
        .hasSize(1);

    reply(id, new PaymentAuthorized(id, UUID.randomUUID(), BigDecimal.ONE, "USD")); // too late
    reply(
        id,
        new PaymentRefunded(id, UUID.randomUUID(), new BigDecimal("0.00"), null)); // never charged
    awaitSaga(id, SagaState.CANCELLED);
    assertThat(decisions(id, "IGNORED")).isEqualTo(1);
    assertThat(orders.load(id).cancelReason()).contains("Timed out");
  }

  @Test
  @org.junit.jupiter.api.Order(8)
  void aLeaseExcludesOtherReplicasUntilItExpiresThenFencesTheOldHolder() throws Exception {
    var a = new PostgresLease(db, "replica-a");
    var b = new PostgresLease(db, "replica-b");
    var ttl = Duration.ofMillis(800);

    long first = a.acquire("test-lease", ttl).orElseThrow();
    assertThat(b.acquire("test-lease", ttl)).isEmpty();
    assertThat(a.acquire("test-lease", ttl)).hasValue(first); // renewal keeps the token

    Thread.sleep(1000); // a stops renewing: crashed, paused or partitioned
    long second = b.acquire("test-lease", ttl).orElseThrow();
    assertThat(second).isGreaterThan(first);
    assertThat(a.acquire("test-lease", ttl)).isEmpty(); // the old holder is out
    // Woken from its stall, a still has token `first`: the fence rejects it, b passes.
    assertThat(a.fence("test-lease", first)).isFalse();
    assertThat(b.fence("test-lease", second)).isTrue();
  }

  @Test
  @org.junit.jupiter.api.Order(9)
  void theEventAndItsOutboxRowCommitOrRollBackTogether() {
    var id = UUID.randomUUID();
    var placed =
        Envelope.of(
            new OrderEvent.OrderPlaced(
                id,
                "atomic",
                null,
                List.of(new OrderLine("SKU-CABLE", 1, new BigDecimal("9.99"))),
                "USD",
                null,
                null,
                null,
                null,
                new BigDecimal("9.99"),
                null,
                null),
            UUID.randomUUID(),
            null);
    // jOOQ joins Spring's transaction: a failure after both writes undoes both.
    assertThatThrownBy(
            () ->
                transactions.executeWithoutResult(
                    tx -> {
                      eventStore.append(id, 0, List.of(placed));
                      outbox.append(placed);
                      throw new IllegalStateException("failure after both writes");
                    }))
        .hasMessage("failure after both writes");
    assertThat(eventStore.load(id, 0)).isEmpty();
    assertThat(outboxRows(id)).isZero();

    // An outbox write outside any transaction is refused: it could commit without its change.
    assertThatThrownBy(() -> outbox.append(placed)).isInstanceOf(IllegalStateException.class);

    transactions.executeWithoutResult(
        tx -> {
          eventStore.append(id, 0, List.of(placed));
          outbox.append(placed);
        });
    assertThat(eventStore.load(id, 0)).hasSize(1);
    assertThat(outboxRows(id)).isOne();
  }

  @Test
  @org.junit.jupiter.api.Order(10)
  void aRetryWithTheSameIdempotencyKeyGetsTheFirstAnswerInsteadOfASecondOrder() throws Exception {
    // Without a key, a client retry is a second order: the failure the key exists to prevent.
    var first = placeOrder.place(customer, ITEMS, null, null);
    var retry = placeOrder.place(customer, ITEMS, null, null);
    assertThat(retry.orderId()).isNotEqualTo(first.orderId());

    var key = "key-" + UUID.randomUUID();
    var original = placeOrder.place(customer, ITEMS, null, key);
    var replayed = placeOrder.place(customer, ITEMS, null, key);
    assertThat(original.replayed()).isFalse();
    assertThat(original.version()).isEqualTo(1);
    assertThat(replayed.replayed()).isTrue();
    assertThat(replayed.orderId()).isEqualTo(original.orderId());
    assertThat(replayed.eventId()).isEqualTo(original.eventId());
    // The replay answers with the order's own amounts and its current version.
    assertThat(replayed.total()).isEqualByComparingTo("74.51");
    assertThat(replayed.invoiceNumber()).isEqualTo(original.invoiceNumber());
    assertThat(replayed.version()).isEqualTo(1);
    // A retry is answered even while the catalog is down: it needs no price.
    CATALOG.down = true;
    try {
      assertThat(placeOrder.place(customer, ITEMS, null, key).orderId())
          .isEqualTo(original.orderId());
    } finally {
      CATALOG.down = false;
    }
    // Exactly one order and one AuthorizePayment behind the key.
    assertThat(outboxRows(original.orderId())).isEqualTo(2);
    assertThat(
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM idempotency_key WHERE key=?", Integer.class, key))
        .isOne();

    // The same key for a different order is refused, never answered with the first order.
    assertThatThrownBy(
            () ->
                placeOrder.place(customer, List.of(new PlaceOrder.Item("SKU-MOUSE", 7)), null, key))
        .isInstanceOf(PlaceOrder.IdempotencyConflict.class);
    // So is the same body with a voucher: the voucher is part of what makes it the same order.
    var welcome = voucher("KEYED", "PERCENTAGE", "10", null);
    assertThatThrownBy(() -> placeOrder.place(customer, ITEMS, welcome, key))
        .isInstanceOf(PlaceOrder.IdempotencyConflict.class);

    // Two requests with one key at once: the loser waits on the key, then gets the winner's order.
    var concurrentKey = "key-" + UUID.randomUUID();
    try (var pool = java.util.concurrent.Executors.newFixedThreadPool(4)) {
      var answers =
          pool.invokeAll(
              java.util.Collections.nCopies(
                  4,
                  (java.util.concurrent.Callable<PlaceOrder.Placed>)
                      () -> placeOrder.place(customer, ITEMS, null, concurrentKey)));
      var ids = new HashSet<UUID>();
      for (var answer : answers) ids.add(answer.get().orderId());
      assertThat(ids).hasSize(1);
      assertThat(answers.stream().filter(a -> !a.resultNow().replayed())).hasSize(1);
    }
  }

  @org.springframework.boot.test.web.server.LocalServerPort int port;

  @Test
  @org.junit.jupiter.api.Order(11)
  void theHttpApiUsesTheSharedContract() throws Exception {
    var invalid =
        post("/orders", "{\"customerId\":null,\"items\":[{\"sku\":\"SKU-CABLE\",\"quantity\":0}]}");
    assertThat(invalid.statusCode()).isEqualTo(400);
    assertThat(invalid.headers().firstValue("Content-Type"))
        .hasValueSatisfying(t -> assertThat(t).startsWith("application/problem+json"));
    assertThat(invalid.headers().firstValue("X-Request-Id")).hasValue("it-orders-1");
    assertThat(invalid.body())
        .contains(
            "\"code\":\"VALIDATION_FAILED\"",
            "\"requestId\":\"it-orders-1\"",
            "\"field\":\"customerId\"",
            "\"field\":\"items[0].quantity\"");

    for (var notForSale : List.of("SKU-NOPE", "SKU-RETIRED")) {
      var refused = post("/orders", order(customer, notForSale, 1, null));
      assertThat(refused.statusCode()).isEqualTo(422);
      assertThat(refused.body()).contains("\"code\":\"ORDER_RULE_VIOLATION\"");
    }
    var nobody = post("/orders", order(UUID.randomUUID(), "SKU-CABLE", 1, null));
    assertThat(nobody.statusCode()).isEqualTo(422);
    assertThat(nobody.body()).contains("\"code\":\"UNKNOWN_CUSTOMER\"");
    for (var unusable :
        List.of(
            "NOSUCHCODE",
            voucher("OFF", "FIXED", "5", null, false, false),
            voucher("OVER", "FIXED", "5", null, true, true))) {
      var refused = post("/orders", order(customer, "SKU-CABLE", 1, unusable));
      assertThat(refused.statusCode()).as(unusable).isEqualTo(422);
      assertThat(refused.body()).contains("\"code\":\"VOUCHER_NOT_APPLICABLE\"");
    }

    var key = "api-" + UUID.randomUUID();
    var first = post("/orders", key, order(customer, "SKU-CABLE", 1, null));
    assertThat(first.statusCode()).isEqualTo(202);
    assertThat(first.headers().firstValue("Idempotent-Replayed")).hasValue("false");
    assertThat(first.body())
        .contains(
            "\"subtotal\":9.99",
            "\"discount\":0.00",
            "\"tax\":0.80",
            "\"total\":10.79",
            "\"currency\":\"USD\"",
            "\"invoiceNumber\":\"INV-");
    var reused = post("/orders", key, order(customer, "SKU-CABLE", 2, null));
    assertThat(reused.statusCode()).isEqualTo(422);
    assertThat(reused.body()).contains("\"code\":\"IDEMPOTENCY_KEY_REUSED\"");

    var missing = get("/orders/" + UUID.randomUUID());
    assertThat(missing.statusCode()).isEqualTo(404);
    assertThat(missing.body()).contains("\"code\":\"ORDER_NOT_FOUND\"");
    assertThat(get("/orders/not-a-uuid").body()).contains("\"code\":\"INVALID_PARAMETER\"");
    assertThat(get("/orders?limit=0").body())
        .contains("\"code\":\"VALIDATION_FAILED\"", "\"field\":\"limit\"");

    var page = get("/orders?limit=1");
    assertThat(page.statusCode()).isEqualTo(200);
    assertThat(page.body())
        .contains("\"data\":[", "\"meta\":{\"count\":1,\"limit\":1,\"hasMore\":true}");
    var orderId = MessageCodec.json().readTree(first.body()).path("orderId").asString();
    assertThat(get("/orders/" + orderId).body())
        .contains("\"invoice\":{", "\"status\":\"ISSUED\"", "\"customerName\":\"Test Customer");

    assertThat(get("/customers?limit=2").body()).contains("\"meta\":{\"count\":2,\"limit\":2");
    assertThat(get("/customers/" + customer).body()).contains("\"id\":\"" + customer + "\"");
    var noCustomer = get("/customers/" + UUID.randomUUID());
    assertThat(noCustomer.statusCode()).isEqualTo(404);
    assertThat(noCustomer.body()).contains("\"code\":\"CUSTOMER_NOT_FOUND\"");
    assertThat(get("/vouchers").body()).contains("\"code\":\"OFF", "\"usageCount\":");
    assertThat(get("/catalog").statusCode()).as("the catalog is inventory's now").isEqualTo(404);
  }

  @Test
  @org.junit.jupiter.api.Order(12)
  void aVoucherIsRedeemedAndEverythingSoldIsSnapshottedWithTheInvoice() {
    var code = voucher("TENOFF", "PERCENTAGE", "10", null);
    var placed = placeOrder.place(customer, ITEMS, code.toLowerCase());

    assertThat(placed.subtotal()).isEqualByComparingTo("68.99");
    assertThat(placed.discount()).isEqualByComparingTo("6.90");
    assertThat(placed.tax()).isEqualByComparingTo("4.97");
    assertThat(placed.total()).isEqualByComparingTo("67.06");
    assertThat(placed.invoiceNumber()).matches("INV-\\d{4}-\\d{6}");
    assertThat(usageCount(code)).isOne();

    var id = placed.orderId();
    assertThat(orderRow(id))
        .containsEntry("status", "PLACED")
        .containsEntry("customer_id", customer)
        .containsEntry("voucher_code", code)
        .containsEntry("voucher_discount_type", "PERCENTAGE")
        .containsEntry("version", 1L);
    assertThat((BigDecimal) orderRow(id).get("total")).isEqualByComparingTo("67.06");
    assertThat((BigDecimal) orderRow(id).get("voucher_value")).isEqualByComparingTo("10.00");
    assertThat(items(id))
        .containsExactly(
            "1 SKU-MOUSE Wireless mouse 29.50 x2 = 59.00 - 5.90 = 53.10",
            "2 SKU-CABLE USB-C cable 9.99 x1 = 9.99 - 1.00 = 8.99");
    assertThat(
            jdbc.queryForMap(
                "SELECT number, status, subtotal, discount, tax, total FROM invoice WHERE order_id=?",
                id))
        .containsEntry("number", placed.invoiceNumber())
        .containsEntry("status", "ISSUED")
        .containsEntry("total", new BigDecimal("67.06"));

    // A later price change reaches new orders only: this one keeps what it was sold at.
    CATALOG.price("SKU-MOUSE", "99.00");
    try {
      var later = placeOrder.place(customer, ITEMS, null);
      assertThat(later.subtotal()).isEqualByComparingTo("207.99");
      var order = orders.load(id, false).order();
      assertThat(order.lines().getFirst().unitPrice()).isEqualByComparingTo("29.50");
      assertThat(order.total()).isEqualByComparingTo("67.06");
      assertThat(items(id).getFirst()).contains("29.50");
    } finally {
      CATALOG.price("SKU-MOUSE", "29.50");
    }
  }

  @Test
  @org.junit.jupiter.api.Order(13)
  void twoPlacementsRaceForAVouchersLastUseAndExactlyOneGetsIt() throws Exception {
    var code = voucher("LASTONE", "FIXED", "5.00", 1);
    var start = new java.util.concurrent.CountDownLatch(1);
    try (var pool = java.util.concurrent.Executors.newFixedThreadPool(2)) {
      var attempts = new ArrayList<java.util.concurrent.Future<PlaceOrder.Placed>>();
      for (int i = 0; i < 2; i++)
        attempts.add(
            pool.submit(
                () -> {
                  start.await();
                  return placeOrder.place(customer, ITEMS, code);
                }));
      start.countDown();
      int won = 0;
      int exhausted = 0;
      for (var attempt : attempts)
        try {
          assertThat(attempt.get().discount()).isEqualByComparingTo("5.00");
          won++;
        } catch (java.util.concurrent.ExecutionException e) {
          assertThat(e.getCause()).isInstanceOf(VoucherExhausted.class);
          exhausted++;
        }
      assertThat(won).isOne();
      assertThat(exhausted).isOne();
    }
    assertThat(usageCount(code)).isOne();
    assertThat(
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE voucher_code=?", Integer.class, code))
        .isOne();
    var third = post("/orders", order(customer, "SKU-CABLE", 1, code));
    assertThat(third.statusCode()).isEqualTo(409);
    assertThat(third.body()).contains("\"code\":\"VOUCHER_EXHAUSTED\"");
  }

  @Test
  @org.junit.jupiter.api.Order(14)
  void anUnavailableCatalogFailsThePlacementWith503AndWritesNothing() throws Exception {
    int before = jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class);
    CATALOG.down = true;
    try {
      assertThatThrownBy(() -> placeOrder.place(customer, ITEMS, null))
          .isInstanceOf(CatalogUnavailable.class);
      var refused = post("/orders", order(customer, "SKU-CABLE", 1, null));
      assertThat(refused.statusCode()).isEqualTo(503);
      assertThat(refused.body()).contains("\"code\":\"CATALOG_UNAVAILABLE\"");
      assertThat(refused.headers().firstValue("Retry-After")).hasValue("2");
    } finally {
      CATALOG.down = false;
    }
    assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class)).isEqualTo(before);
    assertThatThrownBy(() -> placeOrder.place(UUID.randomUUID(), ITEMS, null))
        .isInstanceOf(UnknownCustomer.class);
  }

  private UUID customer(String name) {
    var id = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO customer(id, name, email) VALUES (?, ?, ?)", id, name, id + "@example.com");
    return id;
  }

  /** A voucher valid since yesterday; returns its (unique) code. */
  private String voucher(String prefix, String type, String value, Integer usageLimit) {
    return voucher(prefix, type, value, usageLimit, true, false);
  }

  private String voucher(
      String prefix,
      String type,
      String value,
      Integer usageLimit,
      boolean active,
      boolean expired) {
    var code = prefix + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    jdbc.update(
        "INSERT INTO voucher(code, discount_type, value, usage_limit, active, valid_from, valid_until)"
            + " VALUES (?, ?, ?, ?, ?, now() - interval '2 days',"
            + " CASE WHEN ? THEN now() - interval '1 day' END)",
        code,
        type,
        new BigDecimal(value),
        usageLimit,
        active,
        expired);
    return code;
  }

  private int usageCount(String code) {
    return jdbc.queryForObject("SELECT usage_count FROM voucher WHERE code=?", Integer.class, code);
  }

  private Map<String, Object> orderRow(UUID id) {
    return jdbc.queryForMap("SELECT * FROM orders WHERE id=?", id);
  }

  private String invoiceStatus(UUID id) {
    return jdbc.queryForObject("SELECT status FROM invoice WHERE order_id=?", String.class, id);
  }

  private List<String> items(UUID id) {
    return jdbc.query(
        "SELECT * FROM order_item WHERE order_id=? ORDER BY line_no",
        (r, n) ->
            "%d %s %s %s x%d = %s - %s = %s"
                .formatted(
                    r.getInt("line_no"),
                    r.getString("sku"),
                    r.getString("product_name"),
                    r.getBigDecimal("unit_price"),
                    r.getInt("quantity"),
                    r.getBigDecimal("subtotal"),
                    r.getBigDecimal("discount"),
                    r.getBigDecimal("total")),
        id);
  }

  private static String order(UUID customerId, String sku, int quantity, String voucherCode) {
    return "{\"customerId\":\"%s\",\"items\":[{\"sku\":\"%s\",\"quantity\":%d}]%s}"
        .formatted(
            customerId,
            sku,
            quantity,
            voucherCode == null ? "" : ",\"voucherCode\":\"" + voucherCode + "\"");
  }

  private java.net.http.HttpResponse<String> post(String path, String body) throws Exception {
    return post(path, null, body);
  }

  /** A JSON POST with a fixed request id, and an Idempotency-Key when {@code key} is given. */
  private java.net.http.HttpResponse<String> post(String path, String key, String body)
      throws Exception {
    var request =
        java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://localhost:" + port + path))
            .header("Content-Type", "application/json")
            .header("X-Request-Id", "it-orders-1")
            .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body));
    if (key != null) request.header("Idempotency-Key", key);
    return java.net.http.HttpClient.newHttpClient()
        .send(request.build(), java.net.http.HttpResponse.BodyHandlers.ofString());
  }

  private java.net.http.HttpResponse<String> get(String path) throws Exception {
    return java.net.http.HttpClient.newHttpClient()
        .send(
            java.net.http.HttpRequest.newBuilder(
                    java.net.URI.create("http://localhost:" + port + path))
                .build(),
            java.net.http.HttpResponse.BodyHandlers.ofString());
  }

  private int outboxRows(UUID orderId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM outbox WHERE aggregate_id=?", Integer.class, orderId.toString());
  }

  private void reply(UUID orderId, Message payload) {
    var saga = sagas.find(orderId).orElseThrow();
    publish(Envelope.of(payload, saga.correlationId(), null));
  }

  @Test
  @org.junit.jupiter.api.Order(20)
  void edgeGuardsRefuseOverHttpWithRetryAfterAndTheirOwnCodes(
      @org.springframework.boot.test.web.server.LocalServerPort int port) throws Exception {
    var base = "http://localhost:" + port;
    var http = java.net.http.HttpClient.newHttpClient();
    var body = order(customer, "SKU-CABLE", 1, null);
    java.util.function.Function<String, java.net.http.HttpRequest> put =
        settings ->
            java.net.http.HttpRequest.newBuilder(java.net.URI.create(base + "/lab/edge"))
                .header("Content-Type", "application/json")
                .PUT(java.net.http.HttpRequest.BodyPublishers.ofString(settings))
                .build();
    var post =
        java.net.http.HttpRequest.newBuilder(java.net.URI.create(base + "/orders"))
            .header("Content-Type", "application/json")
            .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
            .build();
    var text = java.net.http.HttpResponse.BodyHandlers.ofString();
    try {
      assertThat(
              http.send(
                      put.apply(
                          "{\"rateLimit\":true,\"ratePerSecond\":1,\"shedding\":false,"
                              + "\"maxActiveSagas\":200,\"bulkhead\":false,\"maxConcurrent\":6}"),
                      text)
                  .statusCode())
          .isEqualTo(200);
      assertThat(http.send(post, text).statusCode()).isEqualTo(202);
      var limited = http.send(post, text);
      assertThat(limited.statusCode()).isEqualTo(429);
      assertThat(limited.headers().firstValue("Retry-After")).hasValue("1");
      assertThat(limited.body()).contains("\"code\":\"RATE_LIMITED\"");

      // Shedding above one unfinished order: nobody answers these sagas in this test, so each
      // order placed stays unfinished and the shedder soon refuses the next.
      http.send(
          put.apply(
              "{\"rateLimit\":false,\"ratePerSecond\":1,\"shedding\":true,"
                  + "\"maxActiveSagas\":1,\"bulkhead\":false,\"maxConcurrent\":6}"),
          text);
      await().atMost(Duration.ofSeconds(5)).until(() -> http.send(post, text).statusCode() == 503);
      var shed = http.send(post, text);
      assertThat(shed.body()).contains("\"code\":\"LOAD_SHED\"");
      assertThat(shed.headers().firstValue("Retry-After")).hasValue("2");

      var throughput =
          http.send(
              java.net.http.HttpRequest.newBuilder(
                      java.net.URI.create(base + "/lab/throughput?seconds=300"))
                  .build(),
              text);
      assertThat(throughput.body()).contains("\"completions\"").contains("\"active\"");
    } finally {
      http.send(
          put.apply(
              "{\"rateLimit\":false,\"ratePerSecond\":20,\"shedding\":false,"
                  + "\"maxActiveSagas\":200,\"bulkhead\":false,\"maxConcurrent\":6}"),
          text);
    }
  }

  private void publish(Envelope envelope) {
    kafka
        .send(envelope.topic(), envelope.orderId().toString(), MessageCodec.encode(envelope))
        .join();
  }

  private void awaitSaga(UUID id, SagaState state) {
    await().atMost(WAIT).until(() -> sagas.find(id).orElseThrow().state() == state);
  }

  private int decisions(UUID id, String decision) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM consumer_decision WHERE order_id=? AND decision=?",
        Integer.class,
        id.toString(),
        decision);
  }

  private static java.util.function.Predicate<
          org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>
      forOrder(UUID id) {
    return r -> id.toString().equals(r.key());
  }

  private static boolean isType(
      org.apache.kafka.clients.consumer.ConsumerRecord<String, String> r, String type, UUID id) {
    return id.toString().equals(r.key()) && type.equals(header(r, "eventType"));
  }

  /**
   * inventory-service's catalog, played by the test: GET /products?sku=… answers the known SKUs in
   * the shape inventory returns. {@code down} makes it answer 503; prices can change mid-test.
   */
  static final class TestCatalog {
    volatile boolean down;
    private final Map<String, String[]> products = new java.util.concurrent.ConcurrentHashMap<>();
    private final com.sun.net.httpserver.HttpServer server;

    TestCatalog() {
      product("SKU-MOUSE", "Wireless mouse", "29.50", true);
      product("SKU-CABLE", "USB-C cable", "9.99", true);
      product("SKU-MONITOR", "27\" monitor", "329.00", true);
      product("SKU-RETIRED", "Retired gadget", "5.00", false);
      try {
        server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(0), 0);
      } catch (java.io.IOException e) {
        throw new IllegalStateException(e);
      }
      server.createContext("/products", this::answer);
      server.start();
    }

    String url() {
      return "http://localhost:" + server.getAddress().getPort();
    }

    void price(String sku, String price) {
      var p = products.get(sku);
      products.put(sku, new String[] {p[0], p[1], price, p[3]});
    }

    private void product(String sku, String name, String price, boolean active) {
      products.put(
          sku,
          new String[] {
            UUID.nameUUIDFromBytes(sku.getBytes()).toString(), name, price, "" + active
          });
    }

    private void answer(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException {
      var query = exchange.getRequestURI().getQuery();
      var data = new ArrayList<String>();
      for (var param : query == null ? new String[0] : query.split("&")) {
        var sku = java.net.URLDecoder.decode(param.substring(param.indexOf('=') + 1), UTF_8);
        var p = products.get(sku);
        if (param.startsWith("sku=") && p != null)
          data.add(
              "{\"id\":\"%s\",\"sku\":\"%s\",\"name\":\"%s\",\"description\":\"\",\"price\":%s,"
                      .formatted(p[0], sku, p[1].replace("\"", "\\\""), p[2])
                  + "\"currency\":\"USD\",\"stock\":10,\"version\":1,\"active\":%s}"
                      .formatted(p[3]));
      }
      var body =
          down
              ? "{\"code\":\"MAINTENANCE\"}"
              : "{\"data\":["
                  + String.join(",", data)
                  + "],\"meta\":{\"count\":"
                  + data.size()
                  + "}}";
      var bytes = body.getBytes(UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(down ? 503 : 200, bytes.length);
      try (var out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    }
  }
}
