package io.zeroshift.order.application;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

public interface Catalog {
  record Product(String sku, String name, BigDecimal price) {}

  Optional<Product> find(String sku);

  List<Product> all();
}
