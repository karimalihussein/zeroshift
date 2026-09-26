package io.zeroshift.order;

import static io.zeroshift.platform.testing.CommerceStack.*;
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
  private static final List<PlaceOrder.Item> ITEMS =
      List.of(new PlaceOrder.Item("SKU-MOUSE", 2), new PlaceOrder.Item("SKU-CABLE", 1));

  @DynamicPropertySource
  static void stack(DynamicPropertyRegistry registry) {
    CommerceStack.start();
    registry.add("spring.datasource.url", () -> createDatabase("orders"));
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("order.step-timeout", () -> "6s");
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

  @Test
  void generatedJooqClassesMatchTheMigratedSchema(@Autowired org.jooq.DSLContext db) {
    io.zeroshift.platform.testing.GeneratedSchema.assertMatchesDatabase(
        db, io.zeroshift.platform.db.Public.PUBLIC, io.zeroshift.order.db.Public.PUBLIC);
  }

  @Test
  @org.junit.jupiter.api.Order(1)
  void anOrderPlacedWhileConnectIsDownIsPublishedWhenItStarts() {
    var placed = placeOrder.place("c-early", ITEMS);
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
    var placed = placeOrder.place("c-1", ITEMS);

    var event = read(Topics.ORDER_EVENTS, forOrder(placed.orderId()), 1, WAIT).getFirst();
    var command = read(Topics.PAYMENT_COMMANDS, forOrder(placed.orderId()), 1, WAIT).getFirst();

    assertThat(event.key()).isEqualTo(placed.orderId().toString());
    assertThat(header(event, "eventType")).isEqualTo("OrderPlaced");
    assertThat(header(event, "schemaVersion")).isEqualTo("2");
    var placedEnvelope = MessageCodec.decode(event.value());
    assertThat(placedEnvelope.eventId()).isEqualTo(placed.eventId());
    assertThat(((OrderEvent.OrderPlaced) placedEnvelope.payload()).total())
        .isEqualByComparingTo("68.99");
    var authorize = MessageCodec.decode(command.value());
    assertThat(authorize.payload()).isInstanceOf(PaymentCommand.AuthorizePayment.class);
    assertThat(authorize.correlationId()).isEqualTo(placed.correlationId());
    assertThat(authorize.causationId()).isEqualTo(placed.eventId());
    // Same key, same partition count: every message about this order shares a partition number.
    assertThat(command.partition()).isEqualTo(event.partition());
  }

  @Test
  @org.junit.jupiter.api.Order(3)
  void happyPathCompletesTheSagaAndSnapshotsTheOrder() {
    var id = placeOrder.place("c-2", ITEMS).orderId();

    reply(id, new PaymentAuthorized(id, UUID.randomUUID(), new BigDecimal("68.99")));
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
    var id = placeOrder.place("c-3", ITEMS).orderId();
    reply(id, new PaymentAuthorized(id, UUID.randomUUID(), new BigDecimal("68.99")));
    awaitSaga(id, SagaState.AWAITING_STOCK);

    reply(id, new StockRejected(id, "SKU-MOUSE: 0 left"));
    awaitSaga(id, SagaState.COMPENSATING);
    var refund = read(Topics.PAYMENT_COMMANDS, r -> isType(r, "RefundPayment", id), 1, WAIT);
    assertThat(refund).hasSize(1);
    reply(id, new PaymentRefunded(id, UUID.randomUUID(), new BigDecimal("68.99")));
    awaitSaga(id, SagaState.CANCELLED);

    var order = orders.load(id);
    assertThat(order.status()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(order.cancelReason()).contains("Stock rejected");
    assertThat(sagas.find(id).orElseThrow().compensations()).containsExactly("payment refunded");
  }

  @Test
  @org.junit.jupiter.api.Order(5)
  void aRedeliveredReplyChangesNothing() {
    var id = placeOrder.place("c-4", ITEMS).orderId();
    var authorized =
        Envelope.of(
            new PaymentAuthorized(id, UUID.randomUUID(), BigDecimal.ONE), UUID.randomUUID(), null);

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
    var id = placeOrder.place("c-5", ITEMS).orderId();

    awaitSaga(id, SagaState.COMPENSATING); // nobody answers AuthorizePayment within 6 s
    assertThat(read(Topics.PAYMENT_COMMANDS, r -> isType(r, "RefundPayment", id), 1, WAIT))
        .hasSize(1);

    reply(id, new PaymentAuthorized(id, UUID.randomUUID(), BigDecimal.ONE)); // arrives too late
    reply(id, new PaymentRefunded(id, UUID.randomUUID(), BigDecimal.ONE));
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
                List.of(new OrderLine("SKU-CABLE", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                "USD"),
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
    var customer = "retry-" + UUID.randomUUID();
    // Without a key, a client retry is a second order: the failure the key exists to prevent.
    var first = placeOrder.place(customer, ITEMS, null);
    var retry = placeOrder.place(customer, ITEMS, null);
    assertThat(retry.orderId()).isNotEqualTo(first.orderId());

    var key = "key-" + UUID.randomUUID();
    var original = placeOrder.place(customer, ITEMS, key);
    var replayed = placeOrder.place(customer, ITEMS, key);
    assertThat(original.replayed()).isFalse();
    assertThat(original.version()).isEqualTo(1);
    assertThat(replayed.replayed()).isTrue();
    assertThat(replayed.orderId()).isEqualTo(original.orderId());
    assertThat(replayed.eventId()).isEqualTo(original.eventId());
    // Exactly one order and one AuthorizePayment behind the key.
    assertThat(outboxRows(original.orderId())).isEqualTo(2);
    assertThat(
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM idempotency_key WHERE key=?", Integer.class, key))
        .isOne();

    // The same key for a different order is refused, never answered with the first order.
    assertThatThrownBy(
            () -> placeOrder.place(customer, List.of(new PlaceOrder.Item("SKU-MOUSE", 7)), key))
        .isInstanceOf(PlaceOrder.IdempotencyConflict.class);

    // Two requests with one key at once: the loser waits on the key, then gets the winner's order.
    var concurrentKey = "key-" + UUID.randomUUID();
    try (var pool = java.util.concurrent.Executors.newFixedThreadPool(4)) {
      var answers =
          pool.invokeAll(
              java.util.Collections.nCopies(
                  4,
                  (java.util.concurrent.Callable<PlaceOrder.Placed>)
                      () -> placeOrder.place(customer, ITEMS, concurrentKey)));
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
        post("/orders", "{\"customerId\":\"\",\"items\":[{\"sku\":\"SKU-CABLE\",\"quantity\":0}]}");
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

    var unknownSku =
        post(
            "/orders",
            "{\"customerId\":\"api\",\"items\":[{\"sku\":\"SKU-NOPE\",\"quantity\":1}]}");
    assertThat(unknownSku.statusCode()).isEqualTo(422);
    assertThat(unknownSku.body()).contains("\"code\":\"ORDER_RULE_VIOLATION\"");

    var key = "api-" + UUID.randomUUID();
    var first =
        post(
            "/orders",
            key,
            "{\"customerId\":\"api\",\"items\":[{\"sku\":\"SKU-CABLE\",\"quantity\":1}]}");
    assertThat(first.statusCode()).isEqualTo(202);
    assertThat(first.headers().firstValue("Idempotent-Replayed")).hasValue("false");
    var reused =
        post(
            "/orders",
            key,
            "{\"customerId\":\"api\",\"items\":[{\"sku\":\"SKU-CABLE\",\"quantity\":2}]}");
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
    var body = "{\"customerId\":\"edge\",\"items\":[{\"sku\":\"SKU-CABLE\",\"quantity\":1}]}";
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
}
