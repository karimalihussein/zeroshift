package io.zeroshift.shipping;

import static io.zeroshift.platform.testing.ServiceTestSupport.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zeroshift.contracts.CarrierEvent.ParcelScanned;
import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.ShippingCommand.ScheduleShipment;
import io.zeroshift.contracts.ShippingEvent.*;
import io.zeroshift.contracts.Topics;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.testing.CommerceStack;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
class ShippingServiceIT {
  @DynamicPropertySource
  static void stack(DynamicPropertyRegistry registry) {
    configure(registry, "shipping");
  }

  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired Faults faults;

  @BeforeEach
  void connector() { // after the service's migrations created the publication and slot
    CommerceStack.registerOutboxConnector("shipping");
  }

  @Test
  void generatedJooqClassesMatchTheMigratedSchema(@Autowired org.jooq.DSLContext db) {
    io.zeroshift.platform.testing.GeneratedSchema.assertMatchesDatabase(
        db, io.zeroshift.platform.db.Public.PUBLIC, io.zeroshift.shipping.db.Public.PUBLIC);
  }

  @Test
  void schedulesOnceAndAnswersARepeatWithTheSameTrackingNumber() {
    var order = UUID.randomUUID();
    send(kafka, new ScheduleShipment(order, "c-1"));
    send(kafka, new ScheduleShipment(order, "c-1"));

    var replies = replies(Topics.SHIPPING_EVENTS, order, 2);
    var first = (ShipmentScheduled) replies.get(0).payload();
    assertThat(((ShipmentScheduled) replies.get(1).payload()).trackingNumber())
        .isEqualTo(first.trackingNumber());
  }

  @Test
  void anArmedFaultFailsTheShipment() {
    faults.arm(ShippingHandler.FAIL_FAULT, "fail", 1);
    var order = UUID.randomUUID();
    send(kafka, new ScheduleShipment(order, "c-2"));

    assertThat(replies(Topics.SHIPPING_EVENTS, order, 1).getFirst().payload())
        .isInstanceOf(ShipmentFailed.class);
  }

  @Autowired Tracking tracking;
  @Autowired JdbcTemplate jdbc;
  @LocalServerPort int port;

  @Test
  void anOutOfOrderScanRegressesTheNaiveProjectionAndTheGuardRefusesIt() {
    tracking.guard(false);
    var naive = UUID.randomUUID();
    scan(naive, "ZS-NAIVE-" + naive, 4, "DELIVERED");
    scan(naive, "ZS-NAIVE-" + naive, 2, "IN_TRANSIT"); // arrives late
    await().atMost(WAIT).until(() -> row("ZS-NAIVE-" + naive, "scans_applied").equals(2));
    assertThat(row("ZS-NAIVE-" + naive, "status"))
        .isEqualTo("IN_TRANSIT"); // wrong: it was delivered
    assertThat(row("ZS-NAIVE-" + naive, "regressions")).isEqualTo(1);

    tracking.guard(true);
    try {
      var guarded = UUID.randomUUID();
      scan(guarded, "ZS-GUARD-" + guarded, 4, "DELIVERED");
      scan(guarded, "ZS-GUARD-" + guarded, 2, "IN_TRANSIT");
      await().atMost(WAIT).until(() -> row("ZS-GUARD-" + guarded, "stale_skipped").equals(1));
      assertThat(row("ZS-GUARD-" + guarded, "status")).isEqualTo("DELIVERED");
      assertThat(
              jdbc.queryForObject(
                  "SELECT decision FROM consumer_decision WHERE consumer=? AND order_id=? ORDER BY id DESC LIMIT 1",
                  String.class,
                  Tracking.CONSUMER,
                  guarded.toString()))
          .isEqualTo("IGNORED");
    } finally {
      tracking.guard(false);
    }
  }

  @Test
  void theCarrierLabKeysScansAsChosen() throws Exception {
    for (int i = 0; i < 2; i++)
      send(kafka, new ScheduleShipment(UUID.randomUUID(), "carrier-" + i));
    await()
        .atMost(WAIT)
        .until(
            () ->
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM shipment WHERE status='SCHEDULED'", Integer.class)
                    >= 2);

    // Keyed by tracking number: each parcel's four scans share one partition.
    var byParcel = partitionsByParcel(lab("tracking"));
    assertThat(byParcel.values()).allSatisfy(partitions -> assertThat(partitions).hasSize(1));
    await()
        .atMost(WAIT)
        .until(
            () -> byParcel.keySet().stream().allMatch(tn -> "DELIVERED".equals(row(tn, "status"))));
    assertThat(byParcel.keySet()).allSatisfy(tn -> assertThat(row(tn, "regressions")).isEqualTo(0));

    // Keyed by scan id: one parcel's scans spread over partitions (all 8 on one is a 1-in-729
    // fluke).
    assertThat(partitionsByParcel(lab("scan")).values().stream().anyMatch(p -> p.size() > 1))
        .isTrue();

    // Keyed by hub: every scan on one partition, the hot one.
    assertThat(partitionsByParcel(lab("hub")).values().stream().flatMap(Set::stream).distinct())
        .hasSize(1);
  }

  private JsonNode lab(String keying) throws Exception {
    var response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(
                        URI.create(
                            "http://localhost:"
                                + port
                                + "/lab/carrier/scans?parcels=2&keying="
                                + keying))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return MessageCodec.json().readTree(response.body());
  }

  private static Map<String, Set<Integer>> partitionsByParcel(JsonNode result) {
    var byParcel = new HashMap<String, Set<Integer>>();
    for (var row : result.path("produced"))
      byParcel
          .computeIfAbsent(row.path("trackingNumber").asString(), k -> new HashSet<>())
          .add(row.path("partition").asInt());
    return byParcel;
  }

  private void scan(UUID orderId, String trackingNumber, int seq, String status) {
    send(
        kafka,
        Envelope.of(new ParcelScanned(orderId, trackingNumber, seq, status, "AMS"), orderId, null));
  }

  private Object row(String trackingNumber, String column) {
    return jdbc
        .queryForList("SELECT " + column + " FROM tracking WHERE tracking_number=?", trackingNumber)
        .stream()
        .findFirst()
        .map(r -> r.get(column))
        .orElse("");
  }
}
