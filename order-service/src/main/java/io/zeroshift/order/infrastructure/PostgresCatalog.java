package io.zeroshift.order.infrastructure;

import io.zeroshift.order.application.Catalog;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;

public final class PostgresCatalog implements Catalog {
  private final JdbcTemplate jdbc;

  public PostgresCatalog(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Optional<Product> find(String sku) {
    return jdbc
        .query(
            "SELECT sku,name,price FROM product WHERE sku=?",
            (r, n) -> new Product(r.getString(1), r.getString(2), r.getBigDecimal(3)),
            sku)
        .stream()
        .findFirst();
  }

  @Override
  public List<Product> all() {
    return jdbc.query(
        "SELECT sku,name,price FROM product ORDER BY sku",
        (r, n) -> new Product(r.getString(1), r.getString(2), r.getBigDecimal(3)));
  }
}
