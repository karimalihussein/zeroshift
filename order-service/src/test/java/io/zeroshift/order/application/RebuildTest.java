package io.zeroshift.order.application;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.OrderEvent.*;
import io.zeroshift.contracts.OrderLine;
import io.zeroshift.order.domain.Order;
import io.zeroshift.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RebuildTest {
  static final UUID ID = UUID.randomUUID();
  static final Instant T0 = Instant.parse("2026-09-26T10:00:00Z");

  /** Three events one minute apart; the first stored as schema v1, as an older release wrote it. */
  final EventStore store =
      new EventStore() {
        final List<Recorded> events = new ArrayList<>();

        {
          var placed =
              Envelope.of(
                  new OrderPlaced(
                      ID,
                      "c",
                      List.of(new OrderLine("SKU-1", 1, new BigDecimal("9.99"))),
                      new BigDecimal("9.99"),
                      "USD"),
                  UUID.randomUUID(),
                  null);
          add(placed, 1);
          add(placed.reply(new OrderPaymentAuthorized(ID, UUID.randomUUID())), 2);
          add(placed.reply(new OrderStockReserved(ID, UUID.randomUUID())), 2);
        }

        void add(Envelope e, int writer) {
          try {
            var stored = MessageCodec.writingAs(writer, () -> MessageCodec.encode(e));
            var decoded = MessageCodec.decode(stored);
            int storedVersion = MessageCodec.json().readTree(stored).path("schemaVersion").asInt();
            events.add(
                new Recorded(
                    events.size() + 1,
                    events.size() + 1,
                    decoded,
                    T0.plusSeconds(60L * events.size()),
                    storedVersion,
                    stored));
          } catch (Exception ex) {
            throw new IllegalStateException(ex);
          }
        }

        @Override
        public List<Recorded> load(UUID stream, long afterVersion) {
          return stream.equals(ID)
              ? events.stream().filter(r -> r.version() > afterVersion).toList()
              : List.of();
        }

        @Override
        public void append(UUID stream, long expectedVersion, List<Envelope> events) {}

        @Override
        public Optional<Snapshot> snapshot(UUID stream) {
          return Optional.empty();
        }

        @Override
        public void saveSnapshot(Order state) {}

        @Override
        public void discardSnapshot(UUID stream) {}
      };

  final OrderRepository orders = new OrderRepository(store, null, 3);

  @Test
  void rebuildsAtAVersion() {
    var r = orders.rebuild(ID, 2L, null);
    assertThat(r.state().status()).isEqualTo(OrderStatus.PAID);
    assertThat(r.state().version()).isEqualTo(2);
    assertThat(r.applied()).hasSize(2);
    assertThat(r.notYetApplied()).hasSize(1);
  }

  @Test
  void rebuildsAsOfAnInstantBetweenTwoEvents() {
    var r = orders.rebuild(ID, null, T0.plusSeconds(90));
    assertThat(r.state().status()).isEqualTo(OrderStatus.PAID);
    assertThat(orders.rebuild(ID, null, T0.minusSeconds(1)).state().status())
        .isEqualTo(OrderStatus.NEW);
    assertThat(orders.rebuild(ID, null, null).state().status()).isEqualTo(OrderStatus.RESERVED);
  }

  @Test
  void historyShowsTheV1RowAndTheUpcastReading() {
    var first = orders.history(ID).getFirst();
    assertThat(first.event().storedVersion()).isEqualTo(1);
    assertThat(first.event().stored()).doesNotContain("currency");
    assertThat(first.event().envelope().schemaVersion()).isEqualTo(2);
    assertThat(first.stateAfter().currency()).isEqualTo("USD");
  }

  @Test
  void anUnknownOrderIsNotFound() {
    assertThatThrownBy(() -> orders.rebuild(UUID.randomUUID(), null, null))
        .isInstanceOf(OrderNotFound.class);
  }
}
