package io.zeroshift.inventory.infrastructure;

import io.zeroshift.inventory.infrastructure.ProductCatalog.StockLevel;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ApiResponse;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/** The control plane's view of stock, and its restock lever. */
@RestController
public class StockController {
  public static final String UNKNOWN_SKU = "UNKNOWN_SKU";

  private final PostgresStock stock;
  private final ProductCatalog catalog;

  public StockController(PostgresStock stock, ProductCatalog catalog) {
    this.stock = stock;
    this.catalog = catalog;
  }

  @GetMapping("/stock")
  public ApiResponse<List<StockLevel>> levels() {
    return ApiResponse.list(catalog.levels());
  }

  /**
   * A delivery of new stock: {@code sku} gets at least {@code available} units free to reserve. The
   * resilience lab's load generator restocks before a run so sustained load is not just a run of
   * out-of-stock cancellations.
   */
  @PostMapping("/lab/stock/{sku}/restock")
  public ApiResponse<List<StockLevel>> restock(
      @PathVariable String sku, @RequestParam @Min(1) @Max(10_000_000) int available) {
    if (!stock.restock(sku, available))
      throw new ApiException(HttpStatus.NOT_FOUND, UNKNOWN_SKU, "No SKU " + sku);
    return ApiResponse.list(catalog.levels());
  }
}
