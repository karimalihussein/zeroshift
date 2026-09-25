package io.zeroshift.inventory.infrastructure;

import static io.zeroshift.inventory.db.Tables.RESERVATION;
import static io.zeroshift.inventory.db.Tables.STOCK;

import io.zeroshift.contracts.InventoryCommand.StockLine;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.inventory.application.Stock;
import io.zeroshift.platform.PostgresClock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import tools.jackson.core.type.TypeReference;

public final class PostgresStock implements Stock {
  private static final TypeReference<List<StockLine>> LINES = new TypeReference<>() {};
  private final DSLContext db;

  public PostgresStock(DSLContext db) {
    this.db = db;
  }

  @Override
  public Optional<Item> find(String sku) {
    return db.select(STOCK.SKU, STOCK.ON_HAND, STOCK.RESERVED, STOCK.VERSION)
        .from(STOCK)
        .where(STOCK.SKU.eq(sku))
        .fetchOptional(r -> new Item(r.value1(), r.value2(), r.value3(), r.value4()));
  }

  /** Optimistic lock: matches only if the row is still at the version that was read. */
  @Override
  public boolean adjust(Item item, int delta) {
    return db.update(STOCK)
            .set(STOCK.RESERVED, STOCK.RESERVED.plus(delta))
            .set(STOCK.VERSION, STOCK.VERSION.plus(1))
            .where(STOCK.SKU.eq(item.sku()).and(STOCK.VERSION.eq(item.version())))
            .execute()
        == 1;
  }

  @Override
  public Optional<Reservation> reservation(UUID orderId) {
    var r = RESERVATION;
    return db.select(r.ORDER_ID, r.RESERVATION_ID, r.STATUS, r.LINES, r.REASON)
        .from(r)
        .where(r.ORDER_ID.eq(orderId))
        .fetchOptional(
            row ->
                new Reservation(
                    row.value1(),
                    row.value2(),
                    Status.valueOf(row.value3()),
                    MessageCodec.json().readValue(row.value4().data(), LINES),
                    row.value5()));
  }

  /** One row per order; later saves change only its status and reason. */
  @Override
  public void save(Reservation reservation) {
    var r = RESERVATION;
    db.insertInto(r)
        .set(r.ORDER_ID, reservation.orderId())
        .set(r.RESERVATION_ID, reservation.reservationId())
        .set(r.STATUS, reservation.status().name())
        .set(r.LINES, JSONB.valueOf(MessageCodec.json().writeValueAsString(reservation.lines())))
        .set(r.REASON, reservation.reason())
        .onConflict(r.ORDER_ID)
        .doUpdate()
        .set(r.STATUS, reservation.status().name())
        .set(r.REASON, reservation.reason())
        .set(r.UPDATED_AT, PostgresClock.NOW)
        .execute();
  }

  public record Level(
      String sku, String name, int onHand, int reserved, int available, int version) {}

  /** For the control plane, by sku. */
  public List<Level> levels() {
    return db.selectFrom(STOCK)
        .orderBy(STOCK.SKU)
        .fetch(
            r ->
                new Level(
                    r.getSku(),
                    r.getName(),
                    r.getOnHand(),
                    r.getReserved(),
                    r.getOnHand() - r.getReserved(),
                    r.getVersion()));
  }
}
