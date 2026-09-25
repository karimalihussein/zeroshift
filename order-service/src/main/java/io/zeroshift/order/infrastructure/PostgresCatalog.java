package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.PRODUCT;

import io.zeroshift.order.application.Catalog;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;

public final class PostgresCatalog implements Catalog {
  private final DSLContext db;

  public PostgresCatalog(DSLContext db) {
    this.db = db;
  }

  @Override
  public Optional<Product> find(String sku) {
    return db.select(PRODUCT.SKU, PRODUCT.NAME, PRODUCT.PRICE)
        .from(PRODUCT)
        .where(PRODUCT.SKU.eq(sku))
        .fetchOptional(r -> new Product(r.value1(), r.value2(), r.value3()));
  }

  @Override
  public List<Product> all() {
    return db.select(PRODUCT.SKU, PRODUCT.NAME, PRODUCT.PRICE)
        .from(PRODUCT)
        .orderBy(PRODUCT.SKU)
        .fetch(r -> new Product(r.value1(), r.value2(), r.value3()));
  }
}
