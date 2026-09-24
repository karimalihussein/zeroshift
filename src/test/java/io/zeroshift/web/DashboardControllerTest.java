package io.zeroshift.web;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.zeroshift.application.*;
import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class DashboardControllerTest {
  @Test
  void emptySourceDisablesMigrationStartInDashboardState() {
    var store = mock(MigrationStore.class);
    var source = mock(SourceDatabase.class);
    var traffic = mock(TrafficSimulator.class);
    var demo = mock(DemoDataService.class);
    var state = idleState();
    when(store.state()).thenReturn(state);
    when(store.logs()).thenReturn(List.of());
    when(source.count(Table.CUSTOMERS)).thenReturn(0L);
    when(source.count(Table.ORDERS)).thenReturn(0L);
    when(traffic.metrics())
        .thenReturn(new TrafficMetrics(false, Primary.SQL_SERVER, 0, 0, 0, 0, 0, 0, 0, 0, 0));

    var dashboard =
        new DashboardController(
                store,
                source,
                mock(MigrationCoordinator.class),
                traffic,
                demo,
                mock(CutoverService.class),
                mock(ChangeCatchUp.class))
            .status();

    assertThat(dashboard.migrationStartAllowed()).isFalse();
    assertThat(dashboard.progress()).isZero();
    assertThat(dashboard.migration().stage()).isEqualTo(Stage.IDLE);
  }

  @Test
  void browserUsesServerStartEligibilityAndStartsDisabled() throws IOException {
    String script = resource("/static/app.js");
    String page = resource("/templates/index.html");

    assertThat(script).contains("start: data.migrationStartAllowed");
    assertThat(page)
        .containsPattern("<button[^>]*data-action=\"start\"[^>]*\\sdisabled>");
  }

  @Test
  void completedDashboardReportsBackendSuccessSummary() {
    var store = mock(MigrationStore.class);
    var source = mock(SourceDatabase.class);
    var traffic = mock(TrafficSimulator.class);
    var started = Instant.parse("2026-01-01T00:00:00Z");
    when(store.state()).thenReturn(completedState(started, started.plusSeconds(12)));
    when(store.count(Table.CUSTOMERS)).thenReturn(20L);
    when(store.count(Table.ORDERS)).thenReturn(20L);
    when(store.logs()).thenReturn(List.of());
    when(source.count(any())).thenReturn(20L);
    when(traffic.metrics())
        .thenReturn(new TrafficMetrics(false, Primary.POSTGRESQL, 0, 0, 0, 0, 0, 0, 0, 0, 0));

    var dashboard =
        new DashboardController(
                store,
                source,
                mock(MigrationCoordinator.class),
                traffic,
                mock(DemoDataService.class),
                mock(CutoverService.class),
                mock(ChangeCatchUp.class))
            .status();

    assertThat(dashboard.progress()).isEqualTo(100);
    assertThat(dashboard.pending()).isZero();
    assertThat(dashboard.completion().successful()).isTrue();
    assertThat(dashboard.completion().validationPassed()).isTrue();
    assertThat(dashboard.completion().cdcPending()).isZero();
    assertThat(dashboard.completion().totalMigratedRows()).isEqualTo(40);
    assertThat(dashboard.completion().durationMillis()).isEqualTo(12_000);
  }

  @Test
  void browserMakesReadyAndCompletedStatesExplicit() throws IOException {
    String script = resource("/static/app.js");
    String page = resource("/templates/index.html");

    assertThat(script)
        .contains("Ready for Cutover — 95%")
        .contains("Migration completed successfully")
        .contains("PostgreSQL is now Primary")
        .contains("['IDLE', 'COMPLETED']");
    assertThat(page)
        .contains("id=\"completion\"")
        .contains("CDC fully caught up / 0 pending")
        .contains("Source/target validation passed");
  }

  @Test
  void directStartApiReturnsClearConflictForEmptySource() {
    var migration = mock(MigrationCoordinator.class);
    var failure =
        new InvalidAction(
            "Cannot start migration: SQL Server has no customers or orders to migrate");
    doThrow(failure).when(migration).start();
    var controller =
        new DashboardController(
            mock(MigrationStore.class),
            mock(SourceDatabase.class),
            migration,
            mock(TrafficSimulator.class),
            mock(DemoDataService.class),
            mock(CutoverService.class),
            mock(ChangeCatchUp.class));

    assertThatThrownBy(() -> controller.action("start", null)).isSameAs(failure);
    var response = new ApiExceptionHandler().invalid(failure);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    assertThat(response.getBody().getDetail()).isEqualTo(failure.getMessage());
  }

  private static MigrationState idleState() {
    return new MigrationState(
        Stage.IDLE,
        RunStatus.IDLE,
        Primary.SQL_SERVER,
        Table.CUSTOMERS,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        false,
        "Not checked",
        "",
        null,
        0,
        false,
        false,
        null,
        null,
        null,
        0);
  }

  private static MigrationState completedState(Instant started, Instant completed) {
    return new MigrationState(
        Stage.COMPLETED,
        RunStatus.SUCCESS,
        Primary.POSTGRESQL,
        Table.ORDERS,
        20,
        20,
        20,
        3,
        40,
        40,
        6,
        0,
        false,
        "Passed: counts + SHA-256 + constraints",
        "",
        completed,
        0,
        false,
        true,
        started,
        started.plusSeconds(10),
        completed,
        40);
  }

  private static String resource(String path) throws IOException {
    try (var stream = DashboardControllerTest.class.getResourceAsStream(path)) {
      assertThat(stream).as(path).isNotNull();
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
