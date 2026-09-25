package io.zeroshift.inventory;

import static io.zeroshift.platform.testing.ServiceTestSupport.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zeroshift.contracts.InventoryCommand.*;
import io.zeroshift.contracts.InventoryEvent.*;
import io.zeroshift.contracts.Topics;
import io.zeroshift.inventory.application.InventoryHandler;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.testing.CommerceStack;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class InventoryServiceIT {
  @DynamicPropertySource
  static void stack(DynamicPropertyRegistry registry) {
    configure(registry, "inventory");
  }

  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired JdbcTemplate jdbc;
  @Autowired Faults faults;

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
    int before = reserved("SKU-KEYBOARD");
    var order = UUID.randomUUID();
    send(kafka, new ReserveStock(order, List.of(new StockLine("SKU-KEYBOARD", 2))));
    assertThat(replies(Topics.INVENTORY_EVENTS, order, 1).getFirst().payload())
        .isInstanceOf(StockReserved.class);
    assertThat(reserved("SKU-KEYBOARD")).isEqualTo(before + 2);

    send(kafka, new ReleaseStock(order));
    assertThat(replies(Topics.INVENTORY_EVENTS, order, 2).get(1).payload())
        .isInstanceOf(StockReleased.class);
    assertThat(reserved("SKU-KEYBOARD")).isEqualTo(before);
  }

  @Test
  void rejectsAllLinesWhenOneIsShort() {
    int cables = reserved("SKU-CABLE");
    var order = UUID.randomUUID();
    send(
        kafka,
        new ReserveStock(
            order, List.of(new StockLine("SKU-CABLE", 1), new StockLine("SKU-MONITOR", 99))));

    var reply = (StockRejected) replies(Topics.INVENTORY_EVENTS, order, 1).getFirst().payload();
    assertThat(reply.reason()).contains("SKU-MONITOR");
    assertThat(reserved("SKU-CABLE")).isEqualTo(cables);
  }

  @Test
  void concurrentReservationsOfOneSkuConflictAndTheLoserRetries() {
    faults.arm(InventoryHandler.SLOW_FAULT, "700", 2); // both read before either writes
    var first = orderOnPartition(0);
    var second = orderOnPartition(1); // different partitions: handled by different threads
    int before = reserved("SKU-MOUSE");

    send(kafka, new ReserveStock(first, List.of(new StockLine("SKU-MOUSE", 1))));
    send(kafka, new ReserveStock(second, List.of(new StockLine("SKU-MOUSE", 1))));

    replies(Topics.INVENTORY_EVENTS, first, 1);
    replies(Topics.INVENTORY_EVENTS, second, 1);
    assertThat(reserved("SKU-MOUSE")).isEqualTo(before + 2); // neither update was lost
    await()
        .atMost(WAIT)
        .until(
            () ->
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM consumer_decision WHERE decision='RETRY_SCHEDULED'"
                            + " AND detail LIKE 'StockChanged%'",
                        Integer.class)
                    >= 1);
  }

  private int reserved(String sku) {
    return jdbc.queryForObject("SELECT reserved FROM stock WHERE sku=?", Integer.class, sku);
  }
}
