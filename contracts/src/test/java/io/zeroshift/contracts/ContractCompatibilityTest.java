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
  private static final OrderPlaced EXPECTED =
      new OrderPlaced(
          UUID.fromString("00000000-0000-0000-0000-00000000000a"),
          "customer-1",
          List.of(new OrderLine("SKU-1", 2, new BigDecimal("10.50"))),
          new BigDecimal("21.00"),
          "USD");

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
            new OrderPlaced(EXPECTED.orderId(), "c", EXPECTED.lines(), EXPECTED.total(), "EUR"),
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

  private static String fixture(String name) throws IOException {
    try (InputStream in =
        ContractCompatibilityTest.class.getResourceAsStream("/contracts/" + name)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
