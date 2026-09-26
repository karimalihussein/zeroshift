package io.zeroshift.payment.infrastructure;

import io.zeroshift.payment.infrastructure.PostgresPayments.PaymentView;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ApiResponse;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/** Read-only view of the payment rows, for the labs to see what was (or was not) charged. */
@RestController
@RequestMapping("/payments")
public class PaymentController {
  public static final String PAYMENT_NOT_FOUND = "PAYMENT_NOT_FOUND";

  private final PostgresPayments payments;

  public PaymentController(PostgresPayments payments) {
    this.payments = payments;
  }

  /** Every payment of the order, oldest first: more than one only when keys differed. */
  @GetMapping
  public ApiResponse<List<PaymentView>> forOrder(@RequestParam UUID orderId) {
    return ApiResponse.list(payments.views(orderId));
  }

  @GetMapping("/{id}")
  public PaymentView payment(@PathVariable UUID id) {
    return payments
        .view(id)
        .orElseThrow(
            () -> new ApiException(HttpStatus.NOT_FOUND, PAYMENT_NOT_FOUND, "No payment " + id));
  }
}
