package io.zeroshift.inventory.infrastructure;

import io.zeroshift.inventory.infrastructure.ProductCatalog.ProductView;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ApiResponse;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/** The catalog: what is sold, at what price, and how many are left. */
@RestController
public class ProductController {
  public static final String PRODUCT_NOT_FOUND = "PRODUCT_NOT_FOUND";

  private final ProductCatalog catalog;

  public ProductController(ProductCatalog catalog) {
    this.catalog = catalog;
  }

  /**
   * Products by SKU. With {@code sku} (repeatable), exactly those products, unknown SKUs left out:
   * order-service prices an order this way. Without it, the first {@code limit}.
   */
  @GetMapping("/products")
  public ApiResponse<List<ProductView>> products(
      @RequestParam(required = false) @Size(max = 100) List<String> sku,
      @RequestParam(required = false) Boolean active,
      @RequestParam(defaultValue = "100") @Min(1) @Max(500) int limit) {
    if (sku != null && !sku.isEmpty()) return ApiResponse.list(catalog.bySku(sku, active));
    return ApiResponse.page(catalog.list(limit, active), limit);
  }

  @GetMapping("/products/{id}")
  public ProductView product(@PathVariable UUID id) {
    return catalog
        .find(id)
        .orElseThrow(
            () -> new ApiException(HttpStatus.NOT_FOUND, PRODUCT_NOT_FOUND, "No product " + id));
  }
}
