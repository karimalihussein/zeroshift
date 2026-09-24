package io.zeroshift.infrastructure;

import io.zeroshift.application.port.LiveTarget;
import io.zeroshift.domain.OrderRecord;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@DependsOn("schemaInitializer")
public class PostgresLiveTarget implements LiveTarget {
  private final JdbcTemplate jdbc;

  public PostgresLiveTarget(DataSource dataSource) {
    jdbc = new JdbcTemplate(dataSource);
  }

  @Override
  public Optional<OrderRecord> order(long id) {
    return jdbc
        .query(
            "SELECT o.id,o.customer_id,c.name,o.amount,o.status FROM orders o JOIN customers c ON c.id=o.customer_id WHERE o.id=?",
            (r, n) ->
                new OrderRecord(
                    r.getLong(1), r.getLong(2), r.getString(3), r.getBigDecimal(4), r.getString(5)),
            id)
        .stream()
        .findFirst();
  }

  @Override
  public List<Long> candidates() {
    return jdbc.query("SELECT id FROM orders ORDER BY id LIMIT 100", (r, n) -> r.getLong(1));
  }
}
