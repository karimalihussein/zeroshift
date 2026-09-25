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
import io.zeroshift.platform.testing.CommerceStack;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;

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
  @Autowired KafkaTemplate<String, String> kafka;

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
    var a = new io.zeroshift.order.infrastructure.PostgresLease(jdbc, "replica-a");
    var b = new io.zeroshift.order.infrastructure.PostgresLease(jdbc, "replica-b");
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

  private void reply(UUID orderId, Message payload) {
    var saga = sagas.find(orderId).orElseThrow();
    publish(Envelope.of(payload, saga.correlationId(), null));
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
