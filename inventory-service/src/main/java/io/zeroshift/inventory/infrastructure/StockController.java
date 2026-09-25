package io.zeroshift.inventory.infrastructure;

import io.zeroshift.platform.web.ApiResponse;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

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
}
