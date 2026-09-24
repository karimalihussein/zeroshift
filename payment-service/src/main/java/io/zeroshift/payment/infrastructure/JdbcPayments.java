package io.zeroshift.payment.infrastructure;

import io.zeroshift.payment.application.Payments;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcPayments implements Payments {
  private final JdbcTemplate jdbc;

  public JdbcPayments(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Optional<Payment> find(UUID orderId) {
    return jdbc
        .query(
            "SELECT order_id,payment_id,status,amount,reason FROM payment WHERE order_id=?",
            (r, n) ->
                new Payment(
                    r.getObject(1, UUID.class),
                    r.getObject(2, UUID.class),
                    Status.valueOf(r.getString(3)),
                    r.getBigDecimal(4),
                    r.getString(5)),
            orderId)
        .stream()
        .findFirst();
  }

  /** Amount, currency and gateway reference are kept from the first write. */
  @Override
  public void save(Payment payment, String currency, String gatewayReference) {
    jdbc.update(
        "INSERT INTO payment(order_id,payment_id,status,amount,currency,gateway_reference,reason)"
            + " VALUES(?,?,?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET status=EXCLUDED.status,"
            + "reason=EXCLUDED.reason,updated_at=clock_timestamp()",
        payment.orderId(),
        payment.paymentId(),
        payment.status().name(),
        payment.amount(),
        currency,
        gatewayReference,
        payment.reason());
  }
}
