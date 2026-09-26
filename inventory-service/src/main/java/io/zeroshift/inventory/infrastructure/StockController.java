package io.zeroshift.inventory.infrastructure;

import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ApiResponse;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
public class StockController {
  private final PostgresStock stock;

  public StockController(PostgresStock stock) {
    this.stock = stock;
  }

  @GetMapping("/stock")
  public ApiResponse<List<PostgresStock.Level>> levels() {
    return ApiResponse.list(stock.levels());
  }

  /**
   * A delivery of new stock: {@code sku} gets at least {@code available} units free to reserve. The
   * resilience lab's load generator restocks before a run so sustained load is not just a run of
   * out-of-stock cancellations.
   */
  @PostMapping("/lab/stock/{sku}/restock")
  public ApiResponse<List<PostgresStock.Level>> restock(
      @PathVariable String sku, @RequestParam @Min(1) @Max(10_000_000) int available) {
    if (!stock.restock(sku, available))
      throw new ApiException(HttpStatus.NOT_FOUND, "UNKNOWN_SKU", "No SKU " + sku);
    return ApiResponse.list(stock.levels());
  }
}
