package io.zeroshift.query;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
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

  @GetMapping(value = "/orders/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
  public String order(@PathVariable UUID id) {
    return readModels
        .orderJson(id)
        .orElseThrow(
            () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Not projected (yet): " + id));
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
