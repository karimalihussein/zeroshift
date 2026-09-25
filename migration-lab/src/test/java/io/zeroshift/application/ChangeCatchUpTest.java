package io.zeroshift.application;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.util.List;
import org.junit.jupiter.api.Test;

class ChangeCatchUpTest {
  @Test
  void closesCaptureAndDoesNotAdvanceCheckpointOnWriteFailure() {
    var source = mock(SourceDatabase.class);
    var capture = mock(SourceDatabase.Capture.class);
    var session = mock(MigrationStore.Session.class);
    var state = mock(MigrationState.class);
    when(session.state()).thenReturn(state);
    when(state.version()).thenReturn(8L);
    when(source.capture(8)).thenReturn(capture);
    var changes = List.of(new Change(1, Change.Operation.DELETE, null, 1));
    when(capture.read(Table.CUSTOMERS, 0, 10)).thenReturn(changes);
    doThrow(new MigrationException("disk full")).when(session).apply(Table.CUSTOMERS, changes);
    assertThatThrownBy(() -> new ChangeCatchUp(source, 10).drain(session)).hasMessage("disk full");
    verify(session, never()).captured(anyLong(), anyLong());
    verify(capture).close();
  }

  @Test
  void pagesByLastKeyAndCheckpointsOnlyAfterBothTables() {
    var source = mock(SourceDatabase.class);
    var capture = mock(SourceDatabase.Capture.class);
    var session = mock(MigrationStore.Session.class);
    var state = mock(MigrationState.class);
    when(session.state()).thenReturn(state);
    when(source.capture(0)).thenReturn(capture);
    when(capture.version()).thenReturn(9L);
    when(session.apply(eq(Table.CUSTOMERS), anyList())).thenReturn(1L);
    when(capture.read(Table.CUSTOMERS, 0, 1))
        .thenReturn(List.of(new Change(12, Change.Operation.DELETE, null, 1)));
    when(capture.read(Table.CUSTOMERS, 12, 1)).thenReturn(List.of());
    when(capture.read(Table.ORDERS, 0, 1)).thenReturn(List.of());
    assertThat(new ChangeCatchUp(source, 1).drain(session)).isEqualTo(1);
    var order = inOrder(session, capture);
    order.verify(session).apply(eq(Table.CUSTOMERS), anyList());
    order.verify(capture).read(Table.CUSTOMERS, 12, 1);
    order.verify(capture).read(Table.ORDERS, 0, 1);
    order.verify(session).captured(9, 1);
  }
}
