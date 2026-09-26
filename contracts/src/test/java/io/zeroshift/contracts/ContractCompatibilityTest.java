package io.zeroshift.contracts;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.contracts.OrderEvent.OrderPlaced;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The compatibility promises of the message contracts, checked against payloads frozen in test
 * resources exactly as older releases wrote them. A change that breaks one of these breaks events
 * already stored in the event store and on order.events, which are kept forever.
 */
class ContractCompatibilityTest {
  /** The frozen order: v1 and v2 knew no discount or tax, so they read with neutral amounts. */
  private static final OrderPlaced EXPECTED =
      new OrderPlaced(
          UUID.fromString("00000000-0000-0000-0000-00000000000a"),
          "customer-1",
          "customer-1",
          List.of(new OrderLine("SKU-1", 2, new BigDecimal("10.50"))),
          "USD",
          new BigDecimal("21.00"),
          Money.ZERO,
          BigDecimal.ZERO,
          Money.ZERO,
          new BigDecimal("21.00"),
          null,
          null);

  @Test
  void everyStoredVersionOfOrderPlacedStillReadsAsTheSameOrder() throws IOException {
    for (int v = 1; v <= Contracts.of(OrderPlaced.class).version(); v++) {
      var decoded = MessageCodec.decode(fixture("OrderPlaced.v" + v + ".json"));
      assertThat(decoded.payload()).as("OrderPlaced v%d", v).isEqualTo(EXPECTED);
      assertThat(decoded.schemaVersion()).isEqualTo(Contracts.of(OrderPlaced.class).version());
    }
  }

  @Test
  void everyContractCanUpcastFromVersionOneWithoutGaps() {
    for (var c : Contracts.all()) {
      assertThat(c.version()).as(c.type()).isGreaterThanOrEqualTo(1);
      for (int v = 1; v < c.version(); v++)
        assertThat(c.upcasters()).as("%s needs an upcaster from v%d", c.type(), v).containsKey(v);
    }
  }

  @Test
  void aPreviousReleaseWriterProducesExactlyTheFrozenV1Payload() throws Exception {
    var envelope =
        new Envelope(
            UUID.fromString("11111111-1111-1111-1111-111111111111"),
            "OrderPlaced",
            2,
            UUID.fromString("22222222-2222-2222-2222-222222222222"),
            null,
            java.time.Instant.parse("2026-01-10T09:00:00Z"),
            EXPECTED);
    var written = MessageCodec.writingAs(1, () -> MessageCodec.encode(envelope));
    var json = MessageCodec.json();
    assertThat(json.readTree(written)).isEqualTo(json.readTree(fixture("OrderPlaced.v1.json")));
    assertThat(MessageCodec.decode(written).payload()).isEqualTo(EXPECTED);
    // Outside writingAs, the current version again.
    assertThat(json.readTree(MessageCodec.encode(envelope)).path("schemaVersion").asInt())
        .isEqualTo(2);
  }

  @Test
  void aV1WriterCannotExpressAnotherCurrency() {
    var eur =
        Envelope.of(
            new OrderPlaced(
                EXPECTED.orderId(),
                "c",
                "c",
                EXPECTED.lines(),
                "EUR",
                EXPECTED.subtotal(),
                EXPECTED.discount(),
                EXPECTED.taxRate(),
                EXPECTED.tax(),
                EXPECTED.total(),
                null,
                null),
            UUID.randomUUID(),
            null);
    assertThatThrownBy(() -> MessageCodec.writingAs(1, () -> MessageCodec.encode(eur)))
        .hasMessageContaining("means USD");
  }

  @Test
  void aNewerVersionIsRefusedAsMalformedSoItIsDeadLetteredNotRetried() throws IOException {
    var v3 = fixture("OrderPlaced.v2.json").replace("\"schemaVersion\":2", "\"schemaVersion\":3");
    assertThatThrownBy(() -> MessageCodec.decode(v3))
        .isInstanceOf(MalformedMessageException.class)
        .hasMessageContaining("v3 is newer than this consumer understands (v2)");
  }

  @Test
  void anAddedFieldIsIgnoredWithoutAVersionBump() throws IOException {
    var extended =
        fixture("OrderPlaced.v2.json")
            .replace("\"currency\":\"USD\"", "\"currency\":\"USD\",\"channel\":\"web\"");
    assertThat(MessageCodec.decode(extended).payload()).isEqualTo(EXPECTED);
  }

  @Test
  void aDiscountedTaxedOrderRoundTripsAndAnEarlierPayloadReadsWithNeutralAmounts()
      throws Exception {
    var line =
        OrderLine.of(UUID.randomUUID(), "SKU-1", "Cable", 3, new BigDecimal("10.00"))
            .withDiscount(new BigDecimal("3.00"));
    var sold =
        new OrderPlaced(
            UUID.randomUUID(),
            UUID.randomUUID().toString(),
            "Ada Lovelace",
            List.of(line),
            "USD",
            new BigDecimal("30.00"),
            new BigDecimal("3.00"),
            new BigDecimal("0.0800"),
            new BigDecimal("2.16"),
            new BigDecimal("29.16"),
            "SAVE10",
            "INV-2026-000001");
    var envelope = Envelope.of(sold, UUID.randomUUID(), null);
    assertThat(MessageCodec.decode(MessageCodec.encode(envelope)).payload()).isEqualTo(sold);
    // The commerce fields are additions (no version bump): a payload written before them, still
    // v2, reads with neutral amounts, and the total it carried is kept.
    var json = MessageCodec.json();
    var tree =
        (tools.jackson.databind.node.ObjectNode) json.readTree(MessageCodec.encode(envelope));
    assertThat(tree.path("schemaVersion").asInt()).isEqualTo(2);
    var payload = (tools.jackson.databind.node.ObjectNode) tree.get("payload");
    List.of("subtotal", "discount", "taxRate", "tax", "customerName").forEach(payload::remove);
    var earlier = (OrderPlaced) MessageCodec.decode(json.writeValueAsString(tree)).payload();
    assertThat(earlier.total()).isEqualByComparingTo("29.16");
    assertThat(earlier.subtotal()).isEqualByComparingTo("29.16");
    assertThat(earlier.discount()).isEqualByComparingTo("0");
    assertThat(earlier.customerName()).isEqualTo(sold.customerId());
  }

  @Test
  void aLineOrAnOrderWhoseAmountsDoNotAddUpIsRefused() {
    assertThatThrownBy(
            () ->
                new OrderLine(
                    null,
                    "SKU-1",
                    "x",
                    2,
                    new BigDecimal("1.00"),
                    new BigDecimal("3.00"),
                    null,
                    null))
        .hasMessageContaining("unitPrice × quantity");
    assertThatThrownBy(
            () ->
                new OrderPlaced(
                    UUID.randomUUID(),
                    "c",
                    "c",
                    EXPECTED.lines(),
                    "USD",
                    new BigDecimal("21.00"),
                    Money.ZERO,
                    BigDecimal.ZERO,
                    Money.ZERO,
                    new BigDecimal("20.00"),
                    null,
                    null))
        .hasMessageContaining("total must be");
  }

  private static String fixture(String name) throws IOException {
    try (InputStream in =
        ContractCompatibilityTest.class.getResourceAsStream("/contracts/" + name)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
