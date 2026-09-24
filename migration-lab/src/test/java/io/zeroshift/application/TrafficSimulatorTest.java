package io.zeroshift.application;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.util.function.Function;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

class TrafficSimulatorTest {
  private final MigrationStore store = mock(MigrationStore.class);
  private final MigrationStore.Session session = mock(MigrationStore.Session.class);
  private final MigrationState state = mock(MigrationState.class);
  private final SourceDatabase source = mock(SourceDatabase.class);
  private final TrafficSimulator traffic = new TrafficSimulator(store, source);

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setup() {
    when(store.transaction(any()))
        .thenAnswer(i -> ((Function<Object, ?>) i.getArgument(0)).apply(session));
    when(session.state()).thenReturn(state);
    when(state.traffic()).thenReturn(true);
    when(state.stage()).thenReturn(Stage.SNAPSHOT);
    when(session.trafficStep()).thenReturn(1L); // INSERT
    when(source.writeTraffic(any()))
        .thenReturn(new TrafficOperationOutcome(Table.CUSTOMERS, 101L, "order #101 created"));
    when(session.writeTraffic(any()))
        .thenReturn(new TrafficOperationOutcome(Table.CUSTOMERS, 202L, "order #202 created"));
  }

  @Test
  void writesToSqlServerBeforeCutoverAndCountsTheOperationThere() {
    when(state.primary()).thenReturn(Primary.SQL_SERVER);
    traffic.tick();
    verify(source).writeTraffic(TrafficOperation.INSERT);
    verify(session, never()).writeTraffic(any());
    verify(session)
        .recordTraffic(
            new TrafficOperationResult(
                TrafficOperation.INSERT,
                Primary.SQL_SERVER,
                Table.CUSTOMERS,
                101L,
                "order #101 created",
                true));
  }

  @Test
  void writesToPostgresAfterCutoverAndNeverTouchesSqlServer() {
    when(state.primary()).thenReturn(Primary.POSTGRESQL);
    when(state.stage()).thenReturn(Stage.COMPLETED);
    traffic.tick();
    verify(session).writeTraffic(TrafficOperation.INSERT);
    verifyNoInteractions(source);
    verify(session)
        .recordTraffic(
            new TrafficOperationResult(
                TrafficOperation.INSERT,
                Primary.POSTGRESQL,
                Table.CUSTOMERS,
                202L,
                "order #202 created",
                true));
  }

  @Test
  void waitsWhileCutoverFencesTheSource() {
    when(state.stage()).thenReturn(Stage.FREEZE);
    traffic.tick();
    verify(session, never()).trafficStep();
    verifyNoInteractions(source);
  }

  @Test
  void countsFailuresAndStopsOnlyAfterAConsecutiveRun() {
    when(state.primary()).thenReturn(Primary.SQL_SERVER);
    doThrow(new MigrationException("connection reset")).when(source).writeTraffic(any());
    when(session.trafficError(any())).thenReturn(1, 2, 3, 4, 5);
    for (int i = 1; i < TrafficSimulator.MAX_CONSECUTIVE_ERRORS; i++) traffic.tick();
    verify(session, never()).traffic(false);
    verify(session, never()).recordTraffic(any());
    traffic.tick();
    verify(session).traffic(false);
    var failure = ArgumentCaptor.forClass(TrafficOperationResult.class);
    verify(session, times(TrafficSimulator.MAX_CONSECUTIVE_ERRORS)).trafficError(failure.capture());
    assertThat(failure.getAllValues())
        .allSatisfy(
            result -> {
              assertThat(result.operation()).isEqualTo(TrafficOperation.INSERT);
              assertThat(result.target()).isEqualTo(Primary.SQL_SERVER);
              assertThat(result.table()).isEqualTo(Table.CUSTOMERS);
              assertThat(result.recordId()).isNull();
              assertThat(result.details()).contains("connection reset");
              assertThat(result.success()).isFalse();
            });
  }

  @Test
  void doesNotMisreportACommittedSqlServerOperationWhenResultLoggingFails() {
    when(state.primary()).thenReturn(Primary.SQL_SERVER);
    doThrow(new MigrationException("log store unavailable")).when(session).recordTraffic(any());

    assertThatThrownBy(traffic::tick)
        .isInstanceOf(MigrationException.class)
        .hasMessageContaining("log store unavailable");

    verify(source).writeTraffic(TrafficOperation.INSERT);
    verify(session, never()).trafficError(any());
  }
}
