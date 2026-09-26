package io.zeroshift.order.web;

import io.zeroshift.order.application.PlaceOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.UUID;

/**
 * POST /orders. Shape and bounds are checked here; business rules (a known customer, SKUs the
 * catalog sells, a usable voucher) in PlaceOrder.
 */
public record PlaceOrderRequest(
    @NotNull UUID customerId,
    @NotEmpty @Size(max = 20) List<@Valid Line> items,
    @Size(max = 40) String voucherCode) {

  public record Line(@NotBlank @Size(max = 40) String sku, @Min(1) @Max(99) int quantity) {}

  List<PlaceOrder.Item> toItems() {
    return items.stream().map(l -> new PlaceOrder.Item(l.sku(), l.quantity())).toList();
  }
}
