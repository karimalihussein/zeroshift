package io.zeroshift.order.application;

import io.zeroshift.order.domain.Customer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface Customers {
  Optional<Customer> find(UUID id);

  /** By name, up to {@code limit}. */
  List<Customer> list(int limit);
}
