package io.zeroshift.query;

import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ApiHeaders;
import io.zeroshift.platform.web.ApiResponse;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** The query API: reads only, straight from the read models. */
@RestController
public class QueryController {
  public static final String ORDER_NOT_PROJECTED = "ORDER_NOT_PROJECTED";
  public static final String READ_MODEL_BEHIND = "READ_MODEL_BEHIND";

  private final ReadModels readModels;
  private final ReadYourWrites readYourWrites;
  private final ProjectionRebuild rebuild;

  public QueryController(
      ReadModels readModels, ReadYourWrites readYourWrites, ProjectionRebuild rebuild) {
    this.readModels = readModels;
    this.readYourWrites = readYourWrites;
    this.rebuild = rebuild;
  }

  @GetMapping("/orders")
  public ApiResponse<List<ReadModels.OrderView>> orders(
      @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
    return ApiResponse.page(readModels.recentOrders(limit), limit);
  }

  /**
   * One order. Without {@code minVersion} this is a plain eventually consistent read: right after a
   * write it may be missing or stale. With the version the write returned (a consistency token), it
   * waits up to {@code waitMs} for the projection; still behind, it answers 409 {@code
   * READ_MODEL_BEHIND} with both versions rather than a stale answer.
   */
  @GetMapping("/orders/{id}")
  public ResponseEntity<ReadModels.OrderView> order(
      @PathVariable UUID id,
      @RequestParam(required = false) @Min(1) Integer minVersion,
      @RequestParam(defaultValue = "0") @Min(0) @Max(5000) long waitMs)
      throws InterruptedException {
    if (minVersion == null)
      return ResponseEntity.ok(
          readModels
              .order(id)
              .orElseThrow(
                  () ->
                      new ApiException(
                          HttpStatus.NOT_FOUND,
                          ORDER_NOT_PROJECTED,
                          "Not projected (yet): " + id)));
    var result = readYourWrites.await(id, minVersion, Duration.ofMillis(waitMs));
    if (!result.satisfied())
      throw new ApiException(
          HttpStatus.CONFLICT,
          READ_MODEL_BEHIND,
          "The read model has applied version "
              + result.projectedVersion()
              + " of this order, not "
              + minVersion
              + " yet",
          Map.of(
              "requiredVersion", result.requiredVersion(),
              "projectedVersion", result.projectedVersion(),
              "waitedMs", result.waitedMs()));
    return ResponseEntity.ok()
        .header(ApiHeaders.PROJECTED_VERSION, String.valueOf(result.projectedVersion()))
        .header(ApiHeaders.WAITED_MS, String.valueOf(result.waitedMs()))
        .body(result.order().orElseThrow());
  }

  @GetMapping("/customers")
  public ApiResponse<List<ReadModels.CustomerView>> customers() {
    return ApiResponse.list(readModels.customers());
  }

  @GetMapping("/lab/projection")
  public ReadModels.Stats projection() {
    return readModels.stats();
  }

  @PostMapping("/lab/projection/rebuild")
  public ProjectionRebuild.Started rebuild() throws Exception {
    return rebuild.rebuild();
  }
}
