package io.zeroshift.order.web;

import io.zeroshift.order.infrastructure.SagaPressure;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.Duration;
import org.springframework.web.bind.annotation.*;

/** The resilience lab's levers on this replica's edge, and the saga table's pressure readings. */
@RestController
@RequestMapping("/lab")
public class EdgeController {
  private final EdgeGuards guards;
  private final SagaPressure sagas;

  public EdgeController(EdgeGuards guards, SagaPressure sagas) {
    this.guards = guards;
    this.sagas = sagas;
  }

  @GetMapping("/edge")
  public EdgeGuards.State edge() {
    return guards.state();
  }

  /** Replaces all guard settings at once; the body is the whole {@link EdgeGuards.Settings}. */
  @PutMapping("/edge")
  public EdgeGuards.State configure(@RequestBody EdgeGuards.Settings settings) {
    return guards.configure(settings);
  }

  /** Sagas finished in the last {@code seconds}, with end-to-end percentiles, and those open. */
  @GetMapping("/throughput")
  public Throughput throughput(@RequestParam(defaultValue = "10") @Min(1) @Max(300) int seconds) {
    return new Throughput(sagas.completions(Duration.ofSeconds(seconds)), sagas.active());
  }

  public record Throughput(SagaPressure.Completions completions, SagaPressure.Active active) {}
}
