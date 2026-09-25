package io.zeroshift.shipping;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.Message;
import io.zeroshift.contracts.ShippingCommand.ScheduleShipment;
import io.zeroshift.contracts.ShippingEvent.*;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Handled;
import io.zeroshift.platform.Outbox;
import java.util.Locale;
import java.util.UUID;

/**
 * Schedules a shipment. The saga's pivot: once a shipment is requested the saga waits for this
 * answer instead of timing out, so every repeat of the command must get the same one.
 */
public final class ShippingHandler {
  /** {@code fail}: the carrier rejects the next shipments, forcing full compensation. */
  public static final String FAIL_FAULT = "shipping-fail";

  private static final String CARRIER = "ZeroShift Express";
  private final Shipments shipments;
  private final Outbox outbox;
  private final Faults faults;

  public ShippingHandler(Shipments shipments, Outbox outbox, Faults faults) {
    this.shipments = shipments;
    this.outbox = outbox;
    this.faults = faults;
  }

  public Handled handle(Envelope command) {
    if (!(command.payload() instanceof ScheduleShipment s))
      return Handled.ignored("Not a shipping command: " + command.type());
    var existing = shipments.find(s.orderId());
    if (existing.isPresent()) {
      var shipment = existing.get();
      return shipment.scheduled()
          ? reply(
              command,
              new ShipmentScheduled(s.orderId(), shipment.trackingNumber(), CARRIER),
              "Already scheduled " + shipment.trackingNumber())
          : reply(command, new ShipmentFailed(s.orderId(), shipment.reason()), "Already failed");
    }
    if (faults.trigger(FAIL_FAULT).isPresent()) {
      var reason = "Carrier rejected the shipment (operator fault)";
      shipments.failed(s.orderId(), reason);
      return reply(command, new ShipmentFailed(s.orderId(), reason), reason);
    }
    var tracking = "ZS" + UUID.randomUUID().toString().substring(0, 10).toUpperCase(Locale.ROOT);
    shipments.scheduled(s.orderId(), tracking, CARRIER);
    return reply(
        command, new ShipmentScheduled(s.orderId(), tracking, CARRIER), "Scheduled " + tracking);
  }

  private Handled reply(Envelope command, Message reply, String detail) {
    outbox.append(command.reply(reply));
    return Handled.processed(detail);
  }
}
