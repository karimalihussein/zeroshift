package io.zeroshift.order.web;

import io.zeroshift.order.application.PlaceOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.util.List;

/** POST /orders. Shape and bounds are checked here; business rules (known SKUs) in PlaceOrder. */
public record PlaceOrderRequest(
    @NotBlank @Size(max = 100) String customerId,
    @NotEmpty @Size(max = 20) List<@Valid Line> items) {

  public record Line(@NotBlank @Size(max = 40) String sku, @Min(1) @Max(99) int quantity) {}

  List<PlaceOrder.Item> toItems() {
    return items.stream().map(l -> new PlaceOrder.Item(l.sku(), l.quantity())).toList();
  }
}
