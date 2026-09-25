package io.zeroshift.integration;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.application.*;
import io.zeroshift.domain.*;
import io.zeroshift.infrastructure.*;
import java.math.BigDecimal;
import org.junit.jupiter.api.*;

class LiveChangesIT extends DatabaseIntegrationFixture {
  private SqlServerLiveSource liveSource;
  private LiveChangesService live;

  @BeforeEach
  void liveServices() {
    liveSource = new SqlServerLiveSource(sourceDataSource);
    live =
        new LiveChangesService(store, source, liveSource, new PostgresLiveTarget(targetDataSource));
  }

  private OrderEdit edit(String status) {
    return new OrderEdit("Manual customer", new BigDecimal("500.0000"), status);
  }

  private void halfCopied() {
    source.seed(140);
    coordinator.start();
    for (int i = 0;
        i < 100 && !(store.state().table() == Table.ORDERS && store.state().lastId() >= 35);
        i++) tick();
    assertThat(store.state().stage()).isEqualTo(Stage.SNAPSHOT);
    assertThat(store.state().progress()).isEqualTo(50.0);
  }

  private void tick() {
    coordinator.tick();
    assertThat(store.state().error()).isEmpty();
  }

  private void finishSnapshot() {
    for (int i = 0; i < 100 && store.state().stage() == Stage.SNAPSHOT; i++) tick();
    assertThat(store.state().stage()).isNotEqualTo(Stage.SNAPSHOT);
  }

  @Test
  void statusUpdateAtFiftyPercentStaysPendingUntilReplayResumes() {
    halfCopied();
    var before = live.inspect(20);
    assertThat(before.inSync()).isTrue();
    live.pauseReplay(true);
    var row = before.source();
    var experiment = live.update(20, new OrderEdit(row.customerName(), row.amount(), "COMPLETED"));
    assertThat(experiment.before().status()).isEqualTo("NEW");
    assertThat(experiment.after().status()).isEqualTo("COMPLETED");
    long batches = store.state().batches();
    tick();
    tick();
    assertThat(store.state().batches()).isGreaterThan(batches);
    var pending = live.inspect(20);
    assertThat(pending.source().status()).isEqualTo("COMPLETED");
    assertThat(pending.target().status()).isEqualTo("NEW");
    assertThat(pending.changes().stream().filter(LiveChangesService.CaptureInfo::pending))
        .hasSize(1);
    assertThat(new ChangeCatchUp(source, 7).pending(store)).isEqualTo(1);
    live.pauseReplay(false);
    tick();
    assertThat(store.state().stage()).isEqualTo(Stage.SNAPSHOT);
    assertThat(live.inspect(20).inSync()).isTrue();
    assertThat(live.inspect(20).target().status()).isEqualTo("COMPLETED");
    assertThat(new ChangeCatchUp(source, 7).pending(store)).isZero();
    long applied = store.state().applied();
    tick();
    assertThat(store.state().applied()).isEqualTo(applied);
  }

  @Test
  void insertCustomerAndOrderAfterBoundaryArrivesDuringSnapshot() {
    halfCopied();
    live.pauseReplay(true);
    var created = live.insert(edit("PENDING"));
    assertThat(created.orderId()).isGreaterThan(store.state().orderBound());
    assertThat(live.inspect(created.orderId()).target()).isNull();
    assertThat(live.inspect(created.orderId()).changes())
        .hasSize(2)
        .allMatch(LiveChangesService.CaptureInfo::pending);
    tick();
    assertThat(live.inspect(created.orderId()).target()).isNull();
    live.pauseReplay(false);
    tick();
    assertThat(store.state().stage()).isEqualTo(Stage.SNAPSHOT);
    assertThat(live.inspect(created.orderId()).inSync()).isTrue();
    assertThat(live.inspect(created.orderId()).target().amount()).isEqualByComparingTo("500");
  }

  @Test
  void deleteCopiedOrderPreservesCustomerAndShowsTombstoneUntilReplay() {
    halfCopied();
    live.pauseReplay(true);
    var before = live.inspect(20);
    live.delete(20);
    tick();
    var pending = live.inspect(20);
    assertThat(pending.source()).isNull();
    assertThat(pending.target()).isNotNull();
    assertThat(pending.changes()).anyMatch(c -> c.operation().equals("DELETE") && c.pending());
    assertThat(source.count(Table.CUSTOMERS)).isEqualTo(140);
    live.pauseReplay(false);
    tick();
    var applied = live.inspect(20);
    assertThat(applied.source()).isNull();
    assertThat(applied.target()).isNull();
    assertThat(applied.inSync()).isTrue();
    assertThat(applied.experiment().before()).isEqualTo(before.source());
  }

  @Test
  void changesBeforeCopyAreEventuallyCorrectWithoutAdvancingTheBaseline() {
    source.seed(70);
    coordinator.start();
    live.pauseReplay(true);
    long baseline = store.state().version();
    liveSource.update(60, edit("COMPLETED"));
    liveSource.delete(61);
    var inserted = live.insert(edit("PENDING"));
    tick();
    assertThat(live.inspect(60).target()).isNull();
    finishSnapshot();
    assertThat(store.state().stage()).isEqualTo(Stage.CATCH_UP);
    assertThat(store.state().version()).isEqualTo(baseline);
    assertThat(live.inspect(60).target().status()).isEqualTo("COMPLETED");
    assertThat(live.inspect(61).target()).isNull();
    assertThat(live.inspect(inserted.orderId()).target()).isNull();
    live.pauseReplay(false);
    while (store.state().stage() != Stage.READY) tick();
    assertThat(cutover.inspect().matches()).isTrue();
    assertThat(live.inspect(inserted.orderId()).inSync()).isTrue();
    assertThat(live.inspect(61).inSync()).isTrue();
  }

  @Test
  void liveActionsRejectUncopiedRecordsAndUnsafeStates() {
    assertThatThrownBy(() -> live.insert(edit("PENDING"))).isInstanceOf(InvalidAction.class);
    source.seed(40);
    coordinator.start();
    assertThatThrownBy(() -> live.update(20, edit("COMPLETED")))
        .hasMessageContaining("already exists");
    assertThatThrownBy(() -> live.delete(20)).hasMessageContaining("already exists");
    assertThatThrownBy(live::selectCopied).hasMessageContaining("No migrated order");
    while (store.state().stage() != Stage.READY) tick();
    live.pauseReplay(true);
    assertThatThrownBy(cutover::inspect).isInstanceOf(InvalidAction.class);
    assertThatThrownBy(cutover::request).isInstanceOf(InvalidAction.class);
    live.pauseReplay(false);
    cutover.request();
    assertThatThrownBy(() -> live.insert(edit("PENDING"))).isInstanceOf(InvalidAction.class);
    tick();
    assertThatThrownBy(() -> live.delete(20)).isInstanceOf(InvalidAction.class);
  }

  @Test
  void cdcPauseAndExperimentsSurviveBackendReconstruction() {
    halfCopied();
    live.pauseReplay(true);
    live.update(20, edit("COMPLETED"));
    store = new PostgresMigrationStore(targetDataSource);
    wire();
    coordinator.recover();
    live =
        new LiveChangesService(
            store,
            source,
            new SqlServerLiveSource(sourceDataSource),
            new PostgresLiveTarget(targetDataSource));
    assertThat(store.state().cdcPaused()).isTrue();
    assertThat(store.state().status()).isEqualTo(RunStatus.PAUSED);
    assertThat(live.inspect(20).experiment().after().status()).isEqualTo("COMPLETED");
    coordinator.resume();
    tick();
    assertThat(live.inspect(20).inSync()).isFalse();
    live.pauseReplay(false);
    tick();
    assertThat(live.inspect(20).inSync()).isTrue();
  }

  @Test
  void copiedCustomerNameAndAmountAreReplayedTogether() {
    halfCopied();
    live.pauseReplay(true);
    live.update(20, new OrderEdit("عميل, \"updated\"", new BigDecimal("999.1234"), "COMPLETED"));
    assertThat(live.inspect(20).changes().stream().filter(LiveChangesService.CaptureInfo::pending))
        .hasSize(2);
    live.pauseReplay(false);
    tick();
    var record = live.inspect(20);
    assertThat(record.inSync()).isTrue();
    assertThat(record.target().customerName()).isEqualTo("عميل, \"updated\"");
    assertThat(record.target().amount()).isEqualByComparingTo("999.1234");
  }

  @Test
  void insertUpdateDeleteBeforeReplayIsCoalescedWithoutInventedEvents() {
    halfCopied();
    live.pauseReplay(true);
    var created = live.insert(edit("PENDING"));
    liveSource.update(created.orderId(), edit("COMPLETED"));
    liveSource.delete(created.orderId());
    assertThat(live.inspect(created.orderId()).source()).isNull();
    live.pauseReplay(false);
    tick();
    assertThat(live.inspect(created.orderId()).inSync()).isTrue();
    assertThat(live.inspect(created.orderId()).target()).isNull();
  }

  @Test
  void snapshotReplayFailureRollsBackReceiptAndBatchAndCanResume() {
    halfCopied();
    live.pauseReplay(true);
    live.update(20, edit("COMPLETED"));
    long copied = store.state().copied();
    pg.execute("ALTER TABLE orders ADD CONSTRAINT reject_completed CHECK(status<>'COMPLETED')");
    live.pauseReplay(false);
    coordinator.tick();
    assertThat(store.state().status()).isEqualTo(RunStatus.FAILED);
    assertThat(store.state().copied()).isEqualTo(copied);
    assertThat(store.appliedVersion(Table.ORDERS, 20)).isZero();
    pg.execute("ALTER TABLE orders DROP CONSTRAINT reject_completed");
    coordinator.resume();
    tick();
    assertThat(live.inspect(20).inSync()).isTrue();
  }

  @Test
  void pauseReplayDoesNotStopSourceTraffic() {
    halfCopied();
    live.pauseReplay(true);
    traffic.toggle(true);
    long count = source.count(Table.CUSTOMERS);
    traffic.tick(); // READ
    traffic.tick(); // INSERT
    tick();
    assertThat(source.count(Table.CUSTOMERS)).isEqualTo(count + 1);
    assertThat(store.count(Table.CUSTOMERS)).isEqualTo(count);
    assertThat(store.state().traffic()).isTrue();
  }

  @Test
  void notYetCopiedUpdatesWaitForTheirFrontierAndCannotBeOverwritten() {
    source.seed(140);
    coordinator.start();
    liveSource.update(120, edit("COMPLETED"));
    tick();
    assertThat(store.appliedVersion(Table.ORDERS, 120)).isZero();
    assertThat(live.inspect(120).target()).isNull();
    while (store.state().stage() != Stage.READY) tick();
    assertThat(live.inspect(120).inSync()).isTrue();
    assertThat(cutover.inspect().matches()).isTrue();
  }

  @Test
  void repeatedEmptyResetStillGeneratesPositiveIdentityOne() {
    new DemoDataService(source, store, 10).reset();
    new DemoDataService(source, store, 10).reset();
    source.writeTraffic(TrafficOperation.INSERT);
    assertThat(sql.queryForObject("SELECT MIN(id) FROM dbo.orders", Long.class)).isEqualTo(1L);
    assertThat(sql.queryForObject("SELECT MIN(id) FROM dbo.customers", Long.class)).isEqualTo(1L);
  }

  @Test
  void resetClearsPauseReceiptsAndManualExperiments() {
    halfCopied();
    live.update(20, edit("COMPLETED"));
    tick();
    live.pauseReplay(true);
    new DemoDataService(source, store, 10).reset();
    assertThat(store.state().cdcPaused()).isFalse();
    assertThat(store.appliedVersion(Table.ORDERS, 20)).isZero();
    assertThat(liveSource.latest(20)).isEmpty();
    assertThat(live.inspect(20).inSync()).isFalse();
  }
}
