package io.zeroshift.inventory.application;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.InventoryCommand.*;
import io.zeroshift.contracts.InventoryEvent.*;
import io.zeroshift.contracts.Message;
import io.zeroshift.inventory.application.Stock.Reservation;
import io.zeroshift.inventory.application.Stock.Status;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Handled;
import io.zeroshift.platform.Outbox;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public final class InventoryHandler {
  /** {@code reject}: refuse the next reservations as if out of stock. */
  public static final String REJECT_FAULT = "inventory-reject";

  /** {@code <millis>}: pause between reading and writing stock, widening the race window. */
  public static final String SLOW_FAULT = "inventory-slow";

  private final Stock stock;
  private final Outbox outbox;
  private final Faults faults;

  public InventoryHandler(Stock stock, Outbox outbox, Faults faults) {
    this.stock = stock;
    this.outbox = outbox;
    this.faults = faults;
  }

  public Handled handle(Envelope command) {
    return switch (command.payload()) {
      case ReserveStock r -> reserve(command, r);
      case ReleaseStock r -> release(command, r);
      default -> Handled.ignored("Not an inventory command: " + command.type());
    };
  }

  private Handled reserve(Envelope command, ReserveStock r) {
    var existing = stock.reservation(r.orderId());
    if (existing.isPresent()) {
      var res = existing.get();
      return switch (res.status()) {
        case RESERVED ->
            reply(command, new StockReserved(r.orderId(), res.reservationId()), "Already reserved");
        case REJECTED ->
            reply(command, new StockRejected(r.orderId(), res.reason()), "Already rejected");
        case RELEASED -> Handled.ignored("Order was released before this reservation arrived");
      };
    }
    if (faults.trigger(REJECT_FAULT).isPresent())
      return reject(command, r, "Rejected by operator fault");
    // Read every line first, in SKU order, then write: all lines reserve or none do.
    var lines = r.lines().stream().sorted(Comparator.comparing(StockLine::sku)).toList();
    var items = lines.stream().map(l -> stock.find(l.sku())).toList();
    for (int i = 0; i < lines.size(); i++) {
      var line = lines.get(i);
      if (items.get(i).isEmpty()) return reject(command, r, "Unknown SKU " + line.sku());
      if (items.get(i).get().available() < line.quantity())
        return reject(
            command,
            r,
            line.sku()
                + ": "
                + items.get(i).get().available()
                + " available, "
                + line.quantity()
                + " requested");
    }
    faults.trigger(SLOW_FAULT).ifPresent(InventoryHandler::pause);
    for (int i = 0; i < lines.size(); i++)
      if (!stock.adjust(items.get(i).get(), lines.get(i).quantity()))
        throw new StockChanged(
            lines.get(i).sku()
                + " changed after it was read (version "
                + items.get(i).get().version()
                + "); retrying");
    var reservationId = UUID.randomUUID();
    stock.save(new Reservation(r.orderId(), reservationId, Status.RESERVED, r.lines(), null));
    return reply(
        command, new StockReserved(r.orderId(), reservationId), "Reserved " + describe(r.lines()));
  }

  private Handled reject(Envelope command, ReserveStock r, String reason) {
    stock.save(new Reservation(r.orderId(), null, Status.REJECTED, r.lines(), reason));
    return reply(command, new StockRejected(r.orderId(), reason), reason);
  }

  private Handled release(Envelope command, ReleaseStock r) {
    var existing = stock.reservation(r.orderId());
    if (existing.isEmpty()) {
      stock.save(
          new Reservation(
              r.orderId(), null, Status.RELEASED, List.of(), "released before reserved"));
      return reply(
          command,
          new StockReleased(r.orderId()),
          "Nothing reserved; late reservations will be refused");
    }
    var res = existing.get();
    if (res.status() != Status.RESERVED)
      return reply(
          command, new StockReleased(r.orderId()), "Nothing to release (" + res.status() + ")");
    for (var line : res.lines()) {
      var item = stock.find(line.sku()).orElseThrow();
      if (!stock.adjust(item, -line.quantity()))
        throw new StockChanged(line.sku() + " changed during release; retrying");
    }
    stock.save(
        new Reservation(
            r.orderId(), res.reservationId(), Status.RELEASED, res.lines(), "compensation"));
    return reply(command, new StockReleased(r.orderId()), "Released " + describe(res.lines()));
  }

  private Handled reply(Envelope command, Message reply, String detail) {
    outbox.append(command.reply(reply));
    return Handled.processed(detail);
  }

  private static String describe(List<StockLine> lines) {
    return String.join(", ", lines.stream().map(l -> l.quantity() + "× " + l.sku()).toList());
  }

  private static void pause(String millis) {
    try {
      Thread.sleep(Long.parseLong(millis));
    } catch (NumberFormatException ignored) {
      // A malformed fault value just means no pause.
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
