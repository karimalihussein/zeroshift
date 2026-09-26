package io.zeroshift.order.web;

import io.zeroshift.order.application.Customers;
import io.zeroshift.order.application.Vouchers;
import io.zeroshift.order.domain.Customer;
import io.zeroshift.order.domain.Voucher;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ApiResponse;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/** The customers orders are placed for, and the vouchers they can use (with their usage). */
@RestController
public class CommerceController {
  public static final String CUSTOMER_NOT_FOUND = "CUSTOMER_NOT_FOUND";

  private final Customers customers;
  private final Vouchers vouchers;

  public CommerceController(Customers customers, Vouchers vouchers) {
    this.customers = customers;
    this.vouchers = vouchers;
  }

  /** By name. */
  @GetMapping("/customers")
  public ApiResponse<List<Customer>> customers(
      @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
    return ApiResponse.page(customers.list(limit + 1), limit);
  }

  @GetMapping("/customers/{id}")
  public Customer customer(@PathVariable UUID id) {
    return customers
        .find(id)
        .orElseThrow(
            () -> new ApiException(HttpStatus.NOT_FOUND, CUSTOMER_NOT_FOUND, "No customer " + id));
  }

  /** Every voucher, usable or not: usageCount against usageLimit shows what is left. */
  @GetMapping("/vouchers")
  public ApiResponse<List<Voucher>> vouchers() {
    return ApiResponse.list(vouchers.all());
  }
}
