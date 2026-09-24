package io.zeroshift.infrastructure;

import io.zeroshift.application.*;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public final class LabWorkers {
  private final MigrationCoordinator migration;
  private final TrafficSimulator traffic;
  private volatile boolean ready;

  public LabWorkers(MigrationCoordinator migration, TrafficSimulator traffic) {
    this.migration = migration;
    this.traffic = traffic;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void start() {
    migration.recover();
    ready = true;
  }

  @Scheduled(fixedDelayString = "${lab.tick-ms}")
  public void migrate() {
    if (ready) migration.tick();
  }

  @Scheduled(fixedDelayString = "${lab.traffic-ms}")
  public void simulate() {
    if (ready) traffic.tick();
  }
}
