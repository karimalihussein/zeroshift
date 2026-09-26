package io.zeroshift.inventory.infrastructure;

import static io.zeroshift.inventory.db.Tables.PRODUCT;
import static io.zeroshift.inventory.db.Tables.RESERVATION;
import static io.zeroshift.inventory.db.Tables.RESERVATION_ITEM;

import io.zeroshift.inventory.application.Stock;
import io.zeroshift.platform.PostgresClock;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

public final class PostgresStock implements Stock {
  /** PostgreSQL's uuid order (Java's UUID.compareTo compares signed longs). */
  private static final Comparator<Item> PRODUCT_ORDER =
      Comparator.comparing(i -> i.productId().toString());

  private final DSLContext db;

  public PostgresStock(DSLContext db) {
    this.db = db;
  }

  @Override
  public Optional<UUID> productId(String sku) {
    return db.select(PRODUCT.ID).from(PRODUCT).where(PRODUCT.SKU.eq(sku)).fetchOptional(PRODUCT.ID);
  }

  /**
   * One conditional update per product, in product-id order so two reservations never wait on each
   * other's rows. The row lock each update takes is held until the delivery commits; a concurrent
   * reservation of the same product waits, then re-checks {@code stock >= q} against what is left.
   * A failed item rolls back to the savepoint: nothing was taken.
   */
  @Override
  public Optional<String> take(List<Item> items) {
    try {
      db.transaction(
          tx -> {
            for (var item : items.stream().sorted(PRODUCT_ORDER).toList()) {
              int updated =
                  tx.dsl()
                      .update(PRODUCT)
                      .set(PRODUCT.STOCK, PRODUCT.STOCK.minus(item.quantity()))
                      .set(PRODUCT.VERSION, PRODUCT.VERSION.plus(1))
                      .set(PRODUCT.UPDATED_AT, PostgresClock.NOW)
                      .where(PRODUCT.ID.eq(item.productId()))
                      .and(PRODUCT.ACTIVE)
                      .and(PRODUCT.STOCK.ge(item.quantity()))
                      .execute();
              if (updated == 0) throw new Shortage(why(tx.dsl(), item));
            }
          });
      return Optional.empty();
    } catch (Shortage s) {
      return Optional.of(s.getMessage());
    }
  }

  private static String why(DSLContext db, Item item) {
    var product =
        db.select(PRODUCT.SKU, PRODUCT.ACTIVE, PRODUCT.STOCK)
            .from(PRODUCT)
            .where(PRODUCT.ID.eq(item.productId()))
            .fetchOne();
    if (product == null) return "Unknown product " + item.productId() + " (" + item.sku() + ")";
    if (!product.value2()) return product.value1() + " is not for sale";
    return product.value1()
        + ": "
        + product.value3()
        + " available, "
        + item.quantity()
        + " requested";
  }

  private static final class Shortage extends RuntimeException {
    Shortage(String reason) {
      super(reason, null, false, false);
    }
  }

  @Override
  public void giveBack(List<Item> items) {
    for (var item : items.stream().sorted(PRODUCT_ORDER).toList())
      db.update(PRODUCT)
          .set(PRODUCT.STOCK, PRODUCT.STOCK.plus(item.quantity()))
          .set(PRODUCT.VERSION, PRODUCT.VERSION.plus(1))
          .set(PRODUCT.UPDATED_AT, PostgresClock.NOW)
          .where(PRODUCT.ID.eq(item.productId()))
          .execute();
  }

  @Override
  public Optional<Reservation> reservation(UUID orderId) {
    var r = RESERVATION;
    return db.select(r.ID, r.STATUS, r.REASON)
        .from(r)
        .where(r.ORDER_ID.eq(orderId))
        .fetchOptional(
            row ->
                new Reservation(
                    row.value1(),
                    orderId,
                    Status.valueOf(row.value2()),
                    items(row.value1()),
                    row.value3()));
  }

  private List<Item> items(UUID reservationId) {
    return db.select(RESERVATION_ITEM.PRODUCT_ID, PRODUCT.SKU, RESERVATION_ITEM.QUANTITY)
        .from(RESERVATION_ITEM)
        .join(PRODUCT)
        .on(PRODUCT.ID.eq(RESERVATION_ITEM.PRODUCT_ID))
        .where(RESERVATION_ITEM.RESERVATION_ID.eq(reservationId))
        .orderBy(PRODUCT.SKU)
        .fetch(row -> new Item(row.value1(), row.value2(), row.value3()));
  }

  @Override
  public void insert(Reservation reservation) {
    var r = RESERVATION;
    db.insertInto(r)
        .set(r.ID, reservation.id())
        .set(r.ORDER_ID, reservation.orderId())
        .set(r.STATUS, reservation.status().name())
        .set(r.REASON, reservation.reason())
        .set(
            r.RELEASED_AT,
            reservation.status() == Status.RELEASED
                ? PostgresClock.NOW
                : DSL.castNull(r.RELEASED_AT))
        .execute();
    for (var item : reservation.items())
      db.insertInto(RESERVATION_ITEM)
          .set(RESERVATION_ITEM.RESERVATION_ID, reservation.id())
          .set(RESERVATION_ITEM.PRODUCT_ID, item.productId())
          .set(RESERVATION_ITEM.QUANTITY, item.quantity())
          .execute();
  }

  /** The items stay: they record what was reserved, and were given back. */
  @Override
  public void released(UUID orderId, String reason) {
    var r = RESERVATION;
    db.update(r)
        .set(r.STATUS, Status.RELEASED.name())
        .set(r.REASON, reason)
        .set(r.UPDATED_AT, PostgresClock.NOW)
        .set(r.RELEASED_AT, PostgresClock.NOW)
        .where(r.ORDER_ID.eq(orderId))
        .execute();
  }

  /**
   * A delivery of new stock: raises {@code sku}'s stock to at least {@code available}. Never lowers
   * it. Returns whether the SKU exists.
   */
  public boolean restock(String sku, int available) {
    return db.update(PRODUCT)
            .set(PRODUCT.STOCK, DSL.greatest(PRODUCT.STOCK, DSL.val(available)))
            .set(PRODUCT.VERSION, PRODUCT.VERSION.plus(1))
            .set(PRODUCT.UPDATED_AT, PostgresClock.NOW)
            .where(PRODUCT.SKU.eq(sku))
            .execute()
        == 1;
  }
}
