package io.zeroshift.history;

import io.zeroshift.eventlab.EventLabSettings;
import io.zeroshift.kafkalab.KafkaLabRuns;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.JsonNode;

/** Events over time: the page, an order's history in both stores, and the four labs. */
@Controller
@Validated
public class HistoryController {
  private final EventLabSettings settings;
  private final OrderHistory orders;
  private final HistoryLabs labs;
  private final KafkaLabRuns runs;

  public HistoryController(
      EventLabSettings settings, OrderHistory orders, HistoryLabs labs, KafkaLabRuns runs) {
    this.settings = settings;
    this.orders = orders;
    this.labs = labs;
    this.runs = runs;
  }

  @GetMapping("/history")
  public String page(Model model) {
    model.addAttribute("grafanaUrl", settings.grafanaUrl());
    return "history";
  }

  @GetMapping("/api/history/orders")
  @ResponseBody
  public JsonNode recent(
      @RequestParam(defaultValue = "30") @Min(1) @Max(100) int limit,
      @RequestParam(required = false) String customer) {
    return orders.recent(limit, customer);
  }

  /** Every stored event of the order, as stored and as read today, with the state after each. */
  @GetMapping("/api/history/orders/{id}")
  @ResponseBody
  public JsonNode history(@PathVariable UUID id) {
    return orders.history(id);
  }

  /** The order rebuilt from the event store up to a version, or as of an instant. */
  @GetMapping("/api/history/orders/{id}/rebuild")
  @ResponseBody
  public JsonNode rebuild(
      @PathVariable UUID id,
      @RequestParam(required = false) Long version,
      @RequestParam(required = false) Instant at) {
    return orders.rebuild(id, version, at);
  }

  /**
   * order.events from an instant: real offsets per partition, and this order's records after it.
   */
  @GetMapping("/api/history/orders/{id}/kafka")
  @ResponseBody
  public JsonNode kafka(@PathVariable UUID id, @RequestParam(required = false) Instant at)
      throws Exception {
    return orders.kafkaFrom(id, at);
  }

  @GetMapping("/api/history/labs")
  @ResponseBody
  public List<HistoryLabs.Info> labs() {
    return labs.all();
  }

  /** Runs step {@code n} (0-based) of a lab; 0 starts it over. */
  @PostMapping("/api/history/labs/{id}/steps/{n}")
  @ResponseBody
  public HistoryLabs.Run step(
      @PathVariable String id,
      @PathVariable @Min(0) @Max(5) int n,
      @RequestParam(required = false) String orderId) {
    return labs.step(id, n, orderId);
  }

  @GetMapping("/api/history/runs")
  @ResponseBody
  public List<KafkaLabRuns.Run> runs(
      @RequestParam(defaultValue = "10") @Min(1) @Max(50) int limit) {
    return runs.recent(HistoryLabs.LAB, limit);
  }
}
