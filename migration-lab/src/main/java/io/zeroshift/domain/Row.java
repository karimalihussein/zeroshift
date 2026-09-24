package io.zeroshift.domain;

import java.math.BigDecimal;

public sealed interface Row permits Row.Customer, Row.Order {
  long id();

  record Customer(long id, String name, String email, boolean active) implements Row {}

  record Order(long id, long customerId, BigDecimal amount, String status) implements Row {}
}
