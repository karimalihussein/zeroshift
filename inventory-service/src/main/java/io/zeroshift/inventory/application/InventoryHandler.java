package io.zeroshift.inventory.application;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.InventoryCommand.*;
import io.zeroshift.contracts.InventoryEvent.*;
import io.zeroshift.contracts.Message;
import io.zeroshift.inventory.application.Stock.Item;
import io.zeroshift.inventory.application.Stock.Reservation;
import io.zeroshift.inventory.application.Stock.Status;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Handled;
import io.zeroshift.platform.Outbox;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

public final class InventoryHandler {
  /** {@code reject}: refuse the next reservations as if out of stock. */
  public static final String REJECT_FAULT = "inventory-reject";

  /**
   * {@code <millis>}: pause after taking stock, before committing. The taken products stay locked,
   * so a concurrent reservation of them waits, then sees what is left.
   */
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
            reply(command, new StockReserved(r.orderId(), res.id()), "Already reserved");
        case REJECTED ->
            reply(command, new StockRejected(r.orderId(), res.reason()), "Already rejected");
        case RELEASED -> Handled.ignored("Order was released before this reservation arrived");
      };
    }
    if (faults.trigger(REJECT_FAULT).isPresent())
      return reject(command, r, "Rejected by operator fault");
    // One item per product. Commands written before products had ids name only the SKU.
    var items = new LinkedHashMap<UUID, Item>();
    for (var line : r.lines()) {
      if (line.quantity() < 1)
        return reject(command, r, line.sku() + ": cannot reserve " + line.quantity());
      var productId =
          line.productId() != null ? line.productId() : stock.productId(line.sku()).orElse(null);
      if (productId == null) return reject(command, r, "Unknown SKU " + line.sku());
      items.merge(
          productId,
          new Item(productId, line.sku(), line.quantity()),
          (a, b) -> new Item(a.productId(), a.sku(), a.quantity() + b.quantity()));
    }
    var reserved = List.copyOf(items.values());
    var shortage = stock.take(reserved);
    if (shortage.isPresent()) return reject(command, r, shortage.get());
    faults.trigger(SLOW_FAULT).ifPresent(InventoryHandler::pause);
    var reservationId = UUID.randomUUID();
    stock.insert(new Reservation(reservationId, r.orderId(), Status.RESERVED, reserved, null));
    return reply(
        command, new StockReserved(r.orderId(), reservationId), "Reserved " + describe(reserved));
  }

  private Handled reject(Envelope command, ReserveStock r, String reason) {
    stock.insert(
        new Reservation(UUID.randomUUID(), r.orderId(), Status.REJECTED, List.of(), reason));
    return reply(command, new StockRejected(r.orderId(), reason), reason);
  }

  private Handled release(Envelope command, ReleaseStock r) {
    var existing = stock.reservation(r.orderId());
    if (existing.isEmpty()) {
      stock.insert(
          new Reservation(
              UUID.randomUUID(),
              r.orderId(),
              Status.RELEASED,
              List.of(),
              "released before reserved"));
      return reply(
          command,
          new StockReleased(r.orderId()),
          "Nothing reserved; late reservations will be refused");
    }
    var res = existing.get();
    if (res.status() != Status.RESERVED)
      return reply(
          command, new StockReleased(r.orderId()), "Nothing to release (" + res.status() + ")");
    stock.giveBack(res.items());
    stock.released(r.orderId(), "compensation");
    return reply(command, new StockReleased(r.orderId()), "Released " + describe(res.items()));
  }

  private Handled reply(Envelope command, Message reply, String detail) {
    outbox.append(command.reply(reply));
    return Handled.processed(detail);
  }

  private static String describe(List<Item> items) {
    return String.join(", ", items.stream().map(i -> i.quantity() + "× " + i.sku()).toList());
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
