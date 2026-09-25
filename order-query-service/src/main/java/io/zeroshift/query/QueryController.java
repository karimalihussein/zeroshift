package io.zeroshift.query;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** The query API: reads only, straight from the read models. */
@RestController
public class QueryController {
  private final ReadModels readModels;
  private final ProjectionRebuild rebuild;

  public QueryController(ReadModels readModels, ProjectionRebuild rebuild) {
    this.readModels = readModels;
    this.rebuild = rebuild;
  }

  @GetMapping(value = "/orders", produces = MediaType.APPLICATION_JSON_VALUE)
  public String orders(@RequestParam(defaultValue = "50") int limit) {
    return readModels.recentOrdersJson(Math.min(limit, 500));
  }

  /** Longest a read-your-writes request may wait for the projection. */
  static final long MAX_WAIT_MS = 5000;

  /**
   * One order from the read model. Without {@code minVersion} this is a plain eventually consistent
   * read: right after a write it may be missing or stale. With the version the write returned (a
   * consistency token), the read waits until the projection has applied at least that many of the
   * order's events, up to {@code waitMs}; if it is still behind it answers 409 with both versions
   * rather than serve a stale answer as if it were current.
   */
  @GetMapping(value = "/orders/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> order(
      @PathVariable UUID id,
      @RequestParam(required = false) Integer minVersion,
      @RequestParam(defaultValue = "0") long waitMs)
      throws InterruptedException {
    if (minVersion != null) {
      long started = System.nanoTime();
      long deadline = started + Math.min(Math.max(waitMs, 0), MAX_WAIT_MS) * 1_000_000;
      var projected = readModels.projectedVersion(id);
      while (projected.orElse(0) < minVersion && System.nanoTime() < deadline) {
        Thread.sleep(25);
        projected = readModels.projectedVersion(id);
      }
      long waited = (System.nanoTime() - started) / 1_000_000;
      if (projected.orElse(0) < minVersion)
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .header("X-Projected-Version", String.valueOf(projected.orElse(0)))
            .header("X-Waited-Ms", String.valueOf(waited))
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                "{\"error\":\"not yet projected\",\"requiredVersion\":%d,\"projectedVersion\":%d,\"waitedMs\":%d}"
                    .formatted(minVersion, projected.orElse(0), waited));
      return ResponseEntity.ok()
          .header("X-Projected-Version", String.valueOf(projected.get()))
          .header("X-Waited-Ms", String.valueOf(waited))
          .body(readModels.orderJson(id).orElseThrow());
    }
    return ResponseEntity.ok(
        readModels
            .orderJson(id)
            .orElseThrow(
                () ->
                    new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Not projected (yet): " + id)));
  }

  @GetMapping(value = "/customers", produces = MediaType.APPLICATION_JSON_VALUE)
  public String customers() {
    return readModels.customersJson();
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
