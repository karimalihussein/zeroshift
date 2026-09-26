package io.zeroshift.inventory;

import static io.zeroshift.platform.testing.ServiceTestSupport.*;
import static org.assertj.core.api.Assertions.*;

import io.zeroshift.contracts.InventoryCommand.*;
import io.zeroshift.contracts.InventoryEvent.*;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.Topics;
import io.zeroshift.inventory.application.InventoryHandler;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.testing.CommerceStack;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;
import tools.jackson.databind.JsonNode;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class InventoryServiceIT {
  @DynamicPropertySource
  static void stack(DynamicPropertyRegistry registry) {
    configure(registry, "inventory");
  }

  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired JdbcTemplate jdbc;
  @Autowired Faults faults;
  @LocalServerPort int port;

  @BeforeEach
  void connector() { // after the service's migrations created the publication and slot
    CommerceStack.registerOutboxConnector("inventory");
  }

  @Test
  void generatedJooqClassesMatchTheMigratedSchema(@Autowired org.jooq.DSLContext db) {
    io.zeroshift.platform.testing.GeneratedSchema.assertMatchesDatabase(
        db, io.zeroshift.platform.db.Public.PUBLIC, io.zeroshift.inventory.db.Public.PUBLIC);
  }

  @Test
  void reservesAndReleasesStock() {
    var keyboard = product(UUID.randomUUID(), 10);
    var order = UUID.randomUUID();
    send(kafka, new ReserveStock(order, List.of(new StockLine(keyboard.id, keyboard.sku, 2))));
    var reserved = (StockReserved) replies(Topics.INVENTORY_EVENTS, order, 1).getFirst().payload();
    assertThat(stock(keyboard)).isEqualTo(8);
    assertThat(
            jdbc.queryForObject(
                "SELECT quantity FROM reservation_item WHERE reservation_id=? AND product_id=?",
                Integer.class,
                reserved.reservationId(),
                keyboard.id))
        .isEqualTo(2);

    send(kafka, new ReleaseStock(order));
    assertThat(replies(Topics.INVENTORY_EVENTS, order, 2).get(1).payload())
        .isInstanceOf(StockReleased.class);
    assertThat(stock(keyboard)).isEqualTo(10);
    assertThat(
            jdbc.queryForObject(
                "SELECT status || ' ' || (released_at IS NOT NULL) FROM reservation WHERE order_id=?",
                String.class,
                order))
        .isEqualTo("RELEASED true");
  }

  @Test
  void findsTheProductBySkuForCommandsWrittenBeforeProductIds() {
    var mouse = product(UUID.randomUUID(), 5);
    var order = UUID.randomUUID();
    send(kafka, new ReserveStock(order, List.of(new StockLine(null, mouse.sku, 3))));

    assertThat(replies(Topics.INVENTORY_EVENTS, order, 1).getFirst().payload())
        .isInstanceOf(StockReserved.class);
    assertThat(stock(mouse)).isEqualTo(2);
  }

  @Test
  void rejectsEveryItemWhenOneIsShortAndRecordsNoItems() {
    // Product-id order takes the plentiful one first, so its decrement must be rolled back.
    var cable = product(UUID.fromString("00000000-0000-4000-8000-" + hex(12)), 100);
    var monitor = product(UUID.fromString("ffffffff-0000-4000-8000-" + hex(12)), 3);
    var order = UUID.randomUUID();
    send(
        kafka,
        new ReserveStock(
            order,
            List.of(
                new StockLine(cable.id, cable.sku, 1),
                new StockLine(monitor.id, monitor.sku, 99))));

    var reply = (StockRejected) replies(Topics.INVENTORY_EVENTS, order, 1).getFirst().payload();
    assertThat(reply.reason()).isEqualTo(monitor.sku + ": 3 available, 99 requested");
    assertThat(stock(cable)).isEqualTo(100);
    assertThat(stock(monitor)).isEqualTo(3);
    assertThat(
            jdbc.queryForObject(
                "SELECT status || ' ' || (SELECT COUNT(*) FROM reservation_item i"
                    + " WHERE i.reservation_id = r.id) FROM reservation r WHERE order_id=?",
                String.class,
                order))
        .isEqualTo("REJECTED 0");
  }

  @Test
  void redeliveredCommandsAnswerAgainWithoutReservingOrReleasingTwice() {
    var lamp = product(UUID.randomUUID(), 10);
    var order = UUID.randomUUID();
    var reserve = new ReserveStock(order, List.of(new StockLine(lamp.id, lamp.sku, 4)));
    send(kafka, reserve);
    send(kafka, reserve); // a new event id: past the inbox, caught by the one reservation per order

    var answers = replies(Topics.INVENTORY_EVENTS, order, 2);
    assertThat(answers.get(0).payload()).isEqualTo(answers.get(1).payload());
    assertThat(stock(lamp)).isEqualTo(6);

    send(kafka, new ReleaseStock(order));
    send(kafka, new ReleaseStock(order));
    assertThat(replies(Topics.INVENTORY_EVENTS, order, 4).subList(2, 4))
        .allSatisfy(e -> assertThat(e.payload()).isInstanceOf(StockReleased.class));
    assertThat(stock(lamp)).isEqualTo(10);
  }

  @Test
  void twoConcurrentReservationsForTheLastUnitNeverOversell() {
    var last = product(UUID.randomUUID(), 1);
    faults.arm(InventoryHandler.SLOW_FAULT, "700", 1); // the winner holds the row lock for 700 ms
    var first = orderOnPartition(0);
    var second = orderOnPartition(1); // different partitions: handled by different threads

    send(kafka, new ReserveStock(first, List.of(new StockLine(last.id, last.sku, 1))));
    send(kafka, new ReserveStock(second, List.of(new StockLine(last.id, last.sku, 1))));

    var outcomes =
        List.of(
            replies(Topics.INVENTORY_EVENTS, first, 1).getFirst().payload(),
            replies(Topics.INVENTORY_EVENTS, second, 1).getFirst().payload());
    assertThat(outcomes).filteredOn(StockReserved.class::isInstance).hasSize(1);
    assertThat(outcomes)
        .filteredOn(StockRejected.class::isInstance)
        .singleElement()
        .extracting(m -> ((StockRejected) m).reason())
        .isEqualTo(last.sku + ": 0 available, 1 requested");
    assertThat(stock(last)).isZero();
  }

  @Test
  void servesTheCatalog() throws Exception {
    var desk = product(UUID.randomUUID(), 7);
    var chair = product(UUID.randomUUID(), 2);

    var bySku = get("/products?sku=" + desk.sku + "&sku=" + chair.sku + "&sku=SKU-NOT-SOLD");
    assertThat(bySku.statusCode()).isEqualTo(200);
    var data = json(bySku).path("data");
    assertThat(data.size()).isEqualTo(2);
    assertThat(data.findValuesAsString("sku")).containsExactlyInAnyOrder(desk.sku, chair.sku);

    var one = json(get("/products/" + desk.id));
    assertThat(one.path("sku").asString()).isEqualTo(desk.sku);
    assertThat(one.path("price").decimalValue()).isEqualByComparingTo("12.50");
    assertThat(one.path("currency").asString()).isEqualTo("USD");
    assertThat(one.path("stock").asInt()).isEqualTo(7);
    assertThat(one.path("active").asBoolean()).isTrue();

    var missing = get("/products/" + UUID.randomUUID());
    assertThat(missing.statusCode()).isEqualTo(404);
    assertThat(json(missing).path("code").asString()).isEqualTo("PRODUCT_NOT_FOUND");

    var page = json(get("/products?limit=1&active=true"));
    assertThat(page.path("data").size()).isEqualTo(1);
    assertThat(page.path("meta").path("hasMore").asBoolean()).isTrue();

    var level =
        json(get("/stock"))
            .path("data")
            .valueStream()
            .filter(l -> l.path("sku").asString().equals(chair.sku))
            .findFirst()
            .orElseThrow();
    assertThat(level.path("available").asInt()).isEqualTo(2);
    assertThat(level.path("reserved").asInt()).isZero();
  }

  private record Product(UUID id, String sku) {}

  private Product product(UUID id, int stock) {
    var sku = "T-" + hex(10).toUpperCase();
    jdbc.update(
        "INSERT INTO product(id, sku, name, price, stock) VALUES (?,?,?,?,?)",
        id,
        sku,
        "Test " + sku,
        new BigDecimal("12.50"),
        stock);
    return new Product(id, sku);
  }

  private int stock(Product product) {
    return jdbc.queryForObject("SELECT stock FROM product WHERE id=?", Integer.class, product.id);
  }

  private static String hex(int length) {
    return UUID.randomUUID().toString().replace("-", "").substring(0, length);
  }

  private HttpResponse<String> get(String path) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
            HttpResponse.BodyHandlers.ofString());
  }

  private static JsonNode json(HttpResponse<String> response) {
    return MessageCodec.json().readTree(response.body());
  }
}
