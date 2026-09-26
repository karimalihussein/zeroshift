package io.zeroshift.contracts;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.contracts.OrderEvent.OrderPlaced;
import io.zeroshift.contracts.PaymentCommand.AuthorizePayment;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MessageCodecTest {
  private static final UUID ORDER = UUID.fromString("00000000-0000-0000-0000-00000000000a");

  private static OrderPlaced placed() {
    return new OrderPlaced(
        ORDER,
        "customer-1",
        "Customer One",
        List.of(new OrderLine("SKU-1", 2, new BigDecimal("10.50"))),
        "EUR",
        new BigDecimal("21.00"),
        Money.ZERO,
        BigDecimal.ZERO,
        Money.ZERO,
        new BigDecimal("21.00"),
        null,
        null);
  }

  @Test
  void roundTripsEveryFieldAndKeepsTheConversation() {
    var first = Envelope.of(placed(), UUID.randomUUID(), null);
    var reply = first.reply(new AuthorizePayment(ORDER, new BigDecimal("21.00"), "EUR", null));

    var decoded = MessageCodec.decode(MessageCodec.encode(reply));

    assertThat(decoded).isEqualTo(reply);
    assertThat(decoded.correlationId()).isEqualTo(first.correlationId());
    assertThat(decoded.causationId()).isEqualTo(first.eventId());
    assertThat(decoded.topic()).isEqualTo(Topics.PAYMENT_COMMANDS);
    assertThat(MessageCodec.decode(MessageCodec.encode(first)).payload()).isEqualTo(placed());
  }

  @Test
  void upcastsAVersionOneOrderPlacedWithoutCurrency() {
    String v1 =
        """
        {"eventId":"%s","type":"OrderPlaced","schemaVersion":1,"correlationId":"%s",
         "causationId":null,"occurredAt":"2026-01-01T00:00:00Z",
         "payload":{"orderId":"%s","customerId":"c","total":5,
                    "lines":[{"sku":"S","quantity":1,"unitPrice":5}]}}
        """
            .formatted(UUID.randomUUID(), UUID.randomUUID(), ORDER);

    var decoded = MessageCodec.decode(v1);

    assertThat(decoded.schemaVersion()).isEqualTo(2);
    assertThat(((OrderPlaced) decoded.payload()).currency()).isEqualTo("USD");
  }

  @Test
  void ignoresFieldsAddedByANewerProducerOfTheSameVersion() {
    String json =
        MessageCodec.encode(Envelope.of(placed(), UUID.randomUUID(), null))
            .replace("\"customerId\"", "\"loyaltyTier\":\"gold\",\"customerId\"");

    assertThat(MessageCodec.decode(json).payload()).isEqualTo(placed());
  }

  @Test
  void rejectsWhatNoRetryCanFix() {
    String valid = MessageCodec.encode(Envelope.of(placed(), UUID.randomUUID(), null));

    assertThatThrownBy(() -> MessageCodec.decode("{not json"))
        .isInstanceOf(MalformedMessageException.class);
    assertThatThrownBy(
            () -> MessageCodec.decode(valid.replace("\"OrderPlaced\"", "\"OrderTeleported\"")))
        .isInstanceOf(MalformedMessageException.class)
        .hasMessageContaining("Unknown message type");
    assertThatThrownBy(
            () -> MessageCodec.decode(valid.replace("\"schemaVersion\":2", "\"schemaVersion\":99")))
        .isInstanceOf(MalformedMessageException.class)
        .hasMessageContaining("newer than this consumer understands");
    assertThatThrownBy(() -> MessageCodec.decode(valid.replace("\"quantity\":2", "\"quantity\":0")))
        .isInstanceOf(MalformedMessageException.class);
  }

  @Test
  void everyMessageTypeHasOneTopicFamily() {
    assertThat(Contracts.all())
        .extracting(Contracts.Contract::type)
        .contains("OrderPlaced", "AuthorizePayment", "StockReserved", "ShipmentFailed")
        .doesNotHaveDuplicates();
    assertThat(Contracts.of(OrderPlaced.class).version()).isEqualTo(2);
    assertThat(Contracts.of(AuthorizePayment.class).version()).isEqualTo(1);
  }
}
