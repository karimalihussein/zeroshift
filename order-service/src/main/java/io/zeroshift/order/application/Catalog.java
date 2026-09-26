package io.zeroshift.order.application;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * The product catalog, owned by inventory-service. Placing an order asks it for today's prices and
 * copies them into the order: later catalog changes never reach a placed order.
 */
public interface Catalog {
  record Product(
      UUID id, String sku, String name, BigDecimal price, String currency, boolean active) {}

  /**
   * The products with these SKUs; SKUs the catalog does not know are simply missing.
   *
   * @throws CatalogUnavailable when the catalog cannot answer
   */
  List<Product> find(Collection<String> skus);
}
