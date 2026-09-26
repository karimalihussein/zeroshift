package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.CUSTOMER;

import io.zeroshift.order.application.Customers;
import io.zeroshift.order.db.tables.records.CustomerRecord;
import io.zeroshift.order.domain.Customer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;

public final class PostgresCustomers implements Customers {
  private final DSLContext db;

  public PostgresCustomers(DSLContext db) {
    this.db = db;
  }

  @Override
  public Optional<Customer> find(UUID id) {
    return db.selectFrom(CUSTOMER)
        .where(CUSTOMER.ID.eq(id))
        .fetchOptional(PostgresCustomers::customer);
  }

  @Override
  public List<Customer> list(int limit) {
    return db.selectFrom(CUSTOMER)
        .orderBy(CUSTOMER.NAME, CUSTOMER.ID)
        .limit(limit)
        .fetch(PostgresCustomers::customer);
  }

  private static Customer customer(CustomerRecord r) {
    return new Customer(
        r.getId(), r.getName(), r.getEmail(), r.getPhone(), r.getCreatedAt().toInstant());
  }
}
