package io.zeroshift.eventlab;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Controller
public class EventLabController {
  private final LabOverview overview;
  private final Journey journey;
  private final LabActions actions;
  private final EventLabSettings settings;
  private final LabServices services;

  public EventLabController(
      LabOverview overview,
      Journey journey,
      LabActions actions,
      EventLabSettings settings,
      LabServices services) {
    this.services = services;
    this.overview = overview;
    this.journey = journey;
    this.actions = actions;
    this.settings = settings;
  }

  @GetMapping("/events")
  public String page(org.springframework.ui.Model model) {
    model.addAttribute("grafanaUrl", settings.grafanaUrl());
    return "events";
  }

  @GetMapping("/api/events/state")
  @ResponseBody
  public JsonNode state() throws InterruptedException {
    return overview.snapshot();
  }

  @GetMapping("/api/events/orders/{id}")
  @ResponseBody
  public JsonNode order(@PathVariable UUID id) {
    return journey.of(id);
  }

  @GetMapping("/api/events/orders/{id}/fold")
  @ResponseBody
  public JsonNode fold(@PathVariable UUID id) {
    return services.get("order-service", "/orders/" + id + "/fold");
  }

  @PostMapping("/api/events/actions/{action}")
  @ResponseBody
  public JsonNode act(@PathVariable String action, @RequestBody(required = false) JsonNode body) {
    return actions.run(
        action, body == null ? JsonMapper.builder().build().createObjectNode() : body);
  }

  @ExceptionHandler(LabServices.ActionFailed.class)
  @ResponseBody
  ProblemDetail failed(LabServices.ActionFailed e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, e.getMessage());
  }
}
