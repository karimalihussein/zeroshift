package io.zeroshift.payment.application;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.Message;
import io.zeroshift.contracts.PaymentCommand.*;
import io.zeroshift.contracts.PaymentEvent.*;
import io.zeroshift.payment.application.Payments.Payment;
import io.zeroshift.payment.application.Payments.Status;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Handled;
import io.zeroshift.platform.Outbox;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Answers payment commands. Every command about an order is answered from that order's payment row
 * when one exists, so a second AuthorizePayment never charges twice, whatever its event id.
 */
public final class PaymentHandler {
  /** Operator fault for this service: {@code decline} refuses the next authorizations. */
  public static final String DECLINE_FAULT = "payment-decline";

  private final Payments payments;
  private final PaymentGateway gateway;
  private final Outbox outbox;
  private final Faults faults;
  private final BigDecimal limit;

  public PaymentHandler(
      Payments payments, PaymentGateway gateway, Outbox outbox, Faults faults, BigDecimal limit) {
    this.payments = payments;
    this.gateway = gateway;
    this.outbox = outbox;
    this.faults = faults;
    this.limit = limit;
  }

  public Handled handle(Envelope command) {
    return switch (command.payload()) {
      case AuthorizePayment a -> authorize(command, a);
      case RefundPayment r -> refund(command, r);
      default -> Handled.ignored("Not a payment command: " + command.type());
    };
  }

  private Handled authorize(Envelope command, AuthorizePayment a) {
    var existing = payments.find(a.orderId());
    if (existing.isPresent()) {
      var p = existing.get();
      return switch (p.status()) {
        case AUTHORIZED ->
            reply(
                command,
                new PaymentAuthorized(p.orderId(), p.paymentId(), p.amount()),
                "Already authorized: answered from the payment row, no second charge");
        case DECLINED ->
            reply(command, new PaymentDeclined(p.orderId(), p.reason()), "Already declined");
        case REFUNDED, VOID ->
            Handled.ignored("Order was refunded or voided before this authorization");
      };
    }
    if (faults.trigger(DECLINE_FAULT).isPresent())
      return decline(command, a, "Declined by operator fault");
    if (a.amount().compareTo(limit) > 0)
      return decline(command, a, "Amount " + a.amount() + " exceeds the " + limit + " limit");
    return switch (gateway.charge(a.orderId(), a.amount(), a.currency())) {
      case PaymentGateway.Charged c -> {
        var paymentId = UUID.randomUUID();
        payments.save(
            new Payment(a.orderId(), paymentId, Status.AUTHORIZED, a.amount(), null),
            a.currency(),
            c.reference());
        yield reply(
            command,
            new PaymentAuthorized(a.orderId(), paymentId, a.amount()),
            "Charged " + a.amount() + " " + a.currency() + " (" + c.reference() + ")");
      }
      case PaymentGateway.Declined d -> decline(command, a, "Gateway declined: " + d.reason());
    };
  }

  private Handled decline(Envelope command, AuthorizePayment a, String reason) {
    payments.save(
        new Payment(a.orderId(), null, Status.DECLINED, a.amount(), reason), a.currency(), null);
    return reply(command, new PaymentDeclined(a.orderId(), reason), reason);
  }

  private Handled refund(Envelope command, RefundPayment r) {
    var existing = payments.find(r.orderId());
    if (existing.isEmpty()) {
      payments.save(
          new Payment(r.orderId(), null, Status.VOID, BigDecimal.ZERO, r.reason()), "USD", null);
      return reply(
          command,
          new PaymentRefunded(r.orderId(), null, BigDecimal.ZERO),
          "Nothing charged: order voided so a late authorization is refused");
    }
    var p = existing.get();
    if (p.status() == Status.AUTHORIZED)
      payments.save(
          new Payment(p.orderId(), p.paymentId(), Status.REFUNDED, p.amount(), r.reason()),
          "USD",
          null);
    var refunded =
        p.status() == Status.AUTHORIZED || p.status() == Status.REFUNDED
            ? p.amount()
            : BigDecimal.ZERO;
    return reply(
        command,
        new PaymentRefunded(p.orderId(), p.paymentId(), refunded),
        p.status() == Status.AUTHORIZED
            ? "Refunded " + refunded
            : "Nothing to refund (" + p.status() + ")");
  }

  private Handled reply(Envelope command, Message reply, String detail) {
    outbox.append(command.reply(reply));
    return Handled.processed(detail);
  }
}
