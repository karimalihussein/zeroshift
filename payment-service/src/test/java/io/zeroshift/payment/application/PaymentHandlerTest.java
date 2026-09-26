package io.zeroshift.payment.application;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.PaymentCommand.AuthorizePayment;
import io.zeroshift.contracts.PaymentEvent.PaymentAuthorized;
import io.zeroshift.payment.application.Payments.Payment;
import io.zeroshift.payment.application.Payments.Status;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Outbox;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PaymentHandlerTest {
  private final Payments payments = mock(Payments.class);
  private final PaymentGateway gateway = mock(PaymentGateway.class);
  private final Outbox outbox = mock(Outbox.class);
  private final Faults faults = mock(Faults.class);
  private final PaymentHandler handler =
      new PaymentHandler(payments, gateway, outbox, faults, new BigDecimal("1000.00"));
  private final UUID order = UUID.randomUUID();
  private final String key = AuthorizePayment.keyFor(order);

  PaymentHandlerTest() {
    when(faults.trigger(any())).thenReturn(Optional.empty());
  }

  @Test
  void aPendingClaimIsChargedWithItsKeyAndAnsweredInItsCurrency() {
    var pending = payment(Status.PENDING);
    when(payments.claim(key, order, new BigDecimal("12.30"), "EUR")).thenReturn(pending);
    when(gateway.charge(order, key, pending.amount(), "EUR"))
        .thenReturn(new PaymentGateway.Charged("ch_1"));

    handler.handle(command(new BigDecimal("12.3")));

    verify(payments).authorize(pending.id(), "ch_1");
    assertThat(reply())
        .isEqualTo(new PaymentAuthorized(order, pending.id(), new BigDecimal("12.30"), "EUR"));
  }

  @Test
  void aKeyAlreadyAuthorizedIsAnsweredWithoutCallingTheGateway() {
    var authorized = payment(Status.AUTHORIZED);
    when(payments.claim(any(), any(), any(), any())).thenReturn(authorized);

    handler.handle(command(new BigDecimal("12.30")));

    verifyNoInteractions(gateway);
    assertThat(reply())
        .isEqualTo(new PaymentAuthorized(order, authorized.id(), authorized.amount(), "EUR"));
  }

  private Payment payment(Status status) {
    return new Payment(
        UUID.randomUUID(), order, key, status, new BigDecimal("12.30"), "EUR", null, null);
  }

  private Envelope command(BigDecimal amount) {
    return Envelope.of(new AuthorizePayment(order, amount, "EUR", null), UUID.randomUUID(), null);
  }

  private Object reply() {
    var sent = ArgumentCaptor.forClass(Envelope.class);
    verify(outbox).append(sent.capture());
    return sent.getValue().payload();
  }
}
