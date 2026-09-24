package io.zeroshift.web;

import io.zeroshift.application.LiveChangesService;
import io.zeroshift.domain.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/live")
public final class LiveChangesController {
  private final LiveChangesService service;

  public LiveChangesController(LiveChangesService service) {
    this.service = service;
  }

  @GetMapping("/orders/{id}")
  public LiveChangesService.Inspection inspect(@PathVariable long id) {
    return service.inspect(id);
  }

  @GetMapping("/copied-order")
  public LiveChangesService.Inspection select() {
    return service.selectCopied();
  }

  @PostMapping("/orders")
  public LiveExperiment insert(@RequestBody OrderEdit edit) {
    return service.insert(edit);
  }

  @PutMapping("/orders/{id}")
  public LiveExperiment update(@PathVariable long id, @RequestBody OrderEdit edit) {
    return service.update(id, edit);
  }

  @DeleteMapping("/orders/{id}")
  public LiveExperiment delete(@PathVariable long id) {
    return service.delete(id);
  }

  @PostMapping("/cdc/pause")
  public DashboardController.ActionResult pause() {
    service.pauseReplay(true);
    return new DashboardController.ActionResult(
        "CDC replay paused; snapshot and source writes continue");
  }

  @PostMapping("/cdc/resume")
  public DashboardController.ActionResult resume() {
    service.pauseReplay(false);
    return new DashboardController.ActionResult("CDC replay resumed");
  }
}
