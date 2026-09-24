package io.zeroshift.payment.application;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

public interface Payments {
  enum Status {
    AUTHORIZED,
    DECLINED,
    REFUNDED,
    VOID
  }

  record Payment(UUID orderId, UUID paymentId, Status status, BigDecimal amount, String reason) {}

  Optional<Payment> find(UUID orderId);

  void save(Payment payment, String currency, String gatewayReference);
}
