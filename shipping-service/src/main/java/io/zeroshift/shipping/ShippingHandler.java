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
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Schedules a shipment. Small enough to need no layers: one table, one command, and the shipment
 * row makes a repeated command answer the same way.
 */
public final class ShippingHandler {
  /** {@code fail}: the carrier rejects the next shipments, forcing full compensation. */
  public static final String FAIL_FAULT = "shipping-fail";

  private static final String CARRIER = "ZeroShift Express";
  private final JdbcTemplate jdbc;
  private final Outbox outbox;
  private final Faults faults;

  public ShippingHandler(JdbcTemplate jdbc, Outbox outbox, Faults faults) {
    this.jdbc = jdbc;
    this.outbox = outbox;
    this.faults = faults;
  }

  public Handled handle(Envelope command) {
    if (!(command.payload() instanceof ScheduleShipment s))
      return Handled.ignored("Not a shipping command: " + command.type());
    var existing =
        jdbc.query(
            "SELECT status,tracking_number,reason FROM shipment WHERE order_id=?",
            (r, n) -> new String[] {r.getString(1), r.getString(2), r.getString(3)},
            s.orderId());
    if (!existing.isEmpty()) {
      var row = existing.getFirst();
      return row[0].equals("SCHEDULED")
          ? reply(
              command,
              new ShipmentScheduled(s.orderId(), row[1], CARRIER),
              "Already scheduled " + row[1])
          : reply(command, new ShipmentFailed(s.orderId(), row[2]), "Already failed");
    }
    if (faults.trigger(FAIL_FAULT).isPresent()) {
      var reason = "Carrier rejected the shipment (operator fault)";
      jdbc.update(
          "INSERT INTO shipment(order_id,status,reason) VALUES(?,'FAILED',?)", s.orderId(), reason);
      return reply(command, new ShipmentFailed(s.orderId(), reason), reason);
    }
    var tracking = "ZS" + UUID.randomUUID().toString().substring(0, 10).toUpperCase(Locale.ROOT);
    jdbc.update(
        "INSERT INTO shipment(order_id,status,tracking_number,carrier) VALUES(?,'SCHEDULED',?,?)",
        s.orderId(),
        tracking,
        CARRIER);
    return reply(
        command, new ShipmentScheduled(s.orderId(), tracking, CARRIER), "Scheduled " + tracking);
  }

  private Handled reply(Envelope command, Message reply, String detail) {
    outbox.append(command.reply(reply));
    return Handled.processed(detail);
  }
}
