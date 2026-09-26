package io.zeroshift.inventory.infrastructure;

import static io.zeroshift.inventory.db.Tables.PRODUCT;
import static io.zeroshift.inventory.db.Tables.RESERVATION;
import static io.zeroshift.inventory.db.Tables.RESERVATION_ITEM;

import io.zeroshift.contracts.Money;
import io.zeroshift.inventory.application.Stock;
import io.zeroshift.inventory.db.tables.records.ProductRecord;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

/** Reads of the catalog: products with their price and what is left to sell. */
public final class ProductCatalog {
  public record ProductView(
      UUID id,
      String sku,
      String name,
      String description,
      BigDecimal price,
      String currency,
      int stock,
      long version,
      boolean active,
      OffsetDateTime createdAt,
      OffsetDateTime updatedAt) {}

  /** {@code available} can still be sold; {@code reserved} is held by open reservations. */
  public record StockLevel(
      UUID productId, String sku, String name, int available, int reserved, long version) {}

  private final DSLContext db;
  private final String currency;

  public ProductCatalog(DSLContext db, String currency) {
    if (!Money.isCurrency(currency))
      throw new IllegalArgumentException("commerce.currency must be three letters: " + currency);
    this.db = db;
    this.currency = currency;
  }

  /** By SKU, up to {@code limit + 1} (see ApiResponse.page); {@code active} null means all. */
  public List<ProductView> list(int limit, Boolean active) {
    return db.selectFrom(PRODUCT)
        .where(active == null ? DSL.noCondition() : PRODUCT.ACTIVE.eq(active))
        .orderBy(PRODUCT.SKU)
        .limit(limit + 1)
        .fetch(this::view);
  }

  /** The products with these SKUs, by SKU. Unknown SKUs are simply absent. */
  public List<ProductView> bySku(Collection<String> skus, Boolean active) {
    var where = new ArrayList<Condition>(List.of(PRODUCT.SKU.in(skus)));
    if (active != null) where.add(PRODUCT.ACTIVE.eq(active));
    return db.selectFrom(PRODUCT).where(where).orderBy(PRODUCT.SKU).fetch(this::view);
  }

  public Optional<ProductView> find(UUID id) {
    return db.selectFrom(PRODUCT).where(PRODUCT.ID.eq(id)).fetchOptional(this::view);
  }

  /** For the control plane, by SKU. */
  public List<StockLevel> levels() {
    var held =
        db.select(RESERVATION_ITEM.PRODUCT_ID, DSL.sum(RESERVATION_ITEM.QUANTITY).as("reserved"))
            .from(RESERVATION_ITEM)
            .join(RESERVATION)
            .on(RESERVATION.ID.eq(RESERVATION_ITEM.RESERVATION_ID))
            .where(RESERVATION.STATUS.eq(Stock.Status.RESERVED.name()))
            .groupBy(RESERVATION_ITEM.PRODUCT_ID)
            .asTable("held");
    var reserved = DSL.coalesce(held.field("reserved", Integer.class), 0);
    return db.select(
            PRODUCT.ID, PRODUCT.SKU, PRODUCT.NAME, PRODUCT.STOCK, reserved, PRODUCT.VERSION)
        .from(PRODUCT)
        .leftJoin(held)
        .on(held.field(RESERVATION_ITEM.PRODUCT_ID).eq(PRODUCT.ID))
        .orderBy(PRODUCT.SKU)
        .fetch(
            r ->
                new StockLevel(
                    r.value1(), r.value2(), r.value3(), r.value4(), r.value5(), r.value6()));
  }

  private ProductView view(ProductRecord p) {
    return new ProductView(
        p.getId(),
        p.getSku(),
        p.getName(),
        p.getDescription(),
        p.getPrice(),
        currency,
        p.getStock(),
        p.getVersion(),
        p.getActive(),
        p.getCreatedAt(),
        p.getUpdatedAt());
  }
}
