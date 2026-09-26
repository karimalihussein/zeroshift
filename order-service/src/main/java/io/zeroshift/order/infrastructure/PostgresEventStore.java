package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.EVENT_STORE;
import static io.zeroshift.order.db.Tables.ORDER_SNAPSHOT;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.order.application.ConcurrencyConflict;
import io.zeroshift.order.application.EventStore;
import io.zeroshift.order.domain.Order;
import io.zeroshift.platform.PostgresClock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.dao.DuplicateKeyException;

public final class PostgresEventStore implements EventStore {
  /** Bump when {@link Order}'s shape changes: older snapshots are then ignored, not misread. */
  static final int SNAPSHOT_FORMAT = 1;

  private final DSLContext db;

  public PostgresEventStore(DSLContext db) {
    this.db = db;
  }

  @Override
  public List<Recorded> load(UUID stream, long afterVersion) {
    return db.select(
            EVENT_STORE.GLOBAL_POSITION,
            EVENT_STORE.VERSION,
            EVENT_STORE.ENVELOPE,
            EVENT_STORE.RECORDED_AT,
            EVENT_STORE.SCHEMA_VERSION)
        .from(EVENT_STORE)
        .where(EVENT_STORE.STREAM_ID.eq(stream).and(EVENT_STORE.VERSION.gt(afterVersion)))
        .orderBy(EVENT_STORE.VERSION)
        .fetch(
            r ->
                new Recorded(
                    r.value1(),
                    r.value2(),
                    // Decoding upcasts events stored under an older schema version.
                    MessageCodec.decode(r.value3().data()),
                    r.value4().toInstant(),
                    r.value5(),
                    r.value3().data()));
  }

  /**
   * UNIQUE(stream_id, version) is the optimistic lock: a writer that loaded an older version tries
   * to insert a version that already exists, and the whole transaction is rolled back.
   */
  @Override
  public void append(UUID stream, long expectedVersion, List<Envelope> events) {
    long version = expectedVersion;
    try {
      for (var event : events) {
        // The version actually written: an older writer (see MessageCodec.writingAs) may have
        // encoded it below the current one.
        var encoded = MessageCodec.encode(event);
        db.insertInto(EVENT_STORE)
            .set(EVENT_STORE.STREAM_ID, stream)
            .set(EVENT_STORE.VERSION, ++version)
            .set(EVENT_STORE.EVENT_ID, event.eventId())
            .set(EVENT_STORE.TYPE, event.type())
            .set(
                EVENT_STORE.SCHEMA_VERSION,
                MessageCodec.json().readTree(encoded).path("schemaVersion").asInt())
            .set(EVENT_STORE.ENVELOPE, JSONB.valueOf(encoded))
            .execute();
      }
    } catch (DuplicateKeyException e) {
      throw new ConcurrencyConflict(
          "Order " + stream + " changed after version " + expectedVersion + " was read");
    }
  }

  @Override
  public Optional<Snapshot> snapshot(UUID stream) {
    return db.select(ORDER_SNAPSHOT.VERSION, ORDER_SNAPSHOT.STATE, ORDER_SNAPSHOT.TAKEN_AT)
        .from(ORDER_SNAPSHOT)
        .where(ORDER_SNAPSHOT.STREAM_ID.eq(stream).and(ORDER_SNAPSHOT.FORMAT.eq(SNAPSHOT_FORMAT)))
        .fetchOptional(
            r ->
                new Snapshot(
                    r.value1(),
                    MessageCodec.json().readValue(r.value2().data(), Order.class),
                    r.value3().toInstant()));
  }

  /** Replaces an older snapshot only: a slower writer never moves the snapshot backwards. */
  @Override
  public void saveSnapshot(Order state) {
    var json = JSONB.valueOf(MessageCodec.json().writeValueAsString(state));
    db.insertInto(ORDER_SNAPSHOT)
        .set(ORDER_SNAPSHOT.STREAM_ID, state.id())
        .set(ORDER_SNAPSHOT.VERSION, state.version())
        .set(ORDER_SNAPSHOT.FORMAT, SNAPSHOT_FORMAT)
        .set(ORDER_SNAPSHOT.STATE, json)
        .onConflict(ORDER_SNAPSHOT.STREAM_ID)
        .doUpdate()
        .set(ORDER_SNAPSHOT.VERSION, state.version())
        .set(ORDER_SNAPSHOT.FORMAT, SNAPSHOT_FORMAT)
        .set(ORDER_SNAPSHOT.STATE, json)
        .set(ORDER_SNAPSHOT.TAKEN_AT, PostgresClock.NOW)
        .where(ORDER_SNAPSHOT.VERSION.lt(state.version()))
        .execute();
  }

  @Override
  public void discardSnapshot(UUID stream) {
    db.deleteFrom(ORDER_SNAPSHOT).where(ORDER_SNAPSHOT.STREAM_ID.eq(stream)).execute();
  }
}
