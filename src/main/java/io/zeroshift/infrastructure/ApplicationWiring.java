package io.zeroshift.infrastructure;

import io.zeroshift.application.*;
import io.zeroshift.application.port.*;
import org.springframework.context.annotation.*;

@Configuration
public class ApplicationWiring {
  @Bean
  LiveChangesService liveChangesService(
      MigrationStore store, SourceDatabase captureSource, LiveSource source, LiveTarget target) {
    return new LiveChangesService(store, captureSource, source, target);
  }

  @Bean
  MigrationMetrics migrationMetrics(
      io.micrometer.core.instrument.MeterRegistry registry,
      MigrationStore store,
      ChangeCatchUp catchUp) {
    return new MicrometerMigrationMetrics(registry, store, catchUp);
  }

  @Bean
  SnapshotBatch snapshotBatch(
      SourceDatabase source, LabSettings settings, MigrationMetrics metrics) {
    return new SnapshotBatch(source, settings.batchSize(), metrics);
  }

  @Bean
  ChangeCatchUp changeCatchUp(SourceDatabase source, LabSettings settings) {
    return new ChangeCatchUp(source, settings.batchSize());
  }

  @Bean
  ValidationService validationService(
      SourceDatabase source, MigrationStore store, LabSettings settings) {
    return new ValidationService(source, store, settings.batchSize());
  }

  @Bean
  CutoverService cutoverService(
      SourceDatabase source,
      MigrationStore store,
      ChangeCatchUp catchUp,
      ValidationService validation,
      MigrationMetrics metrics) {
    return new CutoverService(source, store, catchUp, validation, metrics);
  }

  @Bean
  MigrationCoordinator migrationCoordinator(
      MigrationStore store,
      SourceDatabase source,
      SnapshotBatch snapshot,
      ChangeCatchUp catchUp,
      CutoverService cutover) {
    return new MigrationCoordinator(store, source, snapshot, catchUp, cutover);
  }

  @Bean
  TrafficSimulator trafficSimulator(MigrationStore store, SourceDatabase source) {
    return new TrafficSimulator(store, source);
  }

  @Bean
  DemoDataService demoDataService(
      SourceDatabase source, MigrationStore store, LabSettings settings) {
    return new DemoDataService(source, store, settings.seedRows());
  }
}
