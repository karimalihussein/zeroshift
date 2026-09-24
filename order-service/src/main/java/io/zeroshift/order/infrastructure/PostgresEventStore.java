package io.zeroshift.order.infrastructure;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.order.application.ConcurrencyConflict;
import io.zeroshift.order.application.EventStore;
import io.zeroshift.order.domain.Order;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

public final class PostgresEventStore implements EventStore {
  /** Bump when {@link Order}'s shape changes: older snapshots are then ignored, not misread. */
  static final int SNAPSHOT_FORMAT = 1;

  private final JdbcTemplate jdbc;

  public PostgresEventStore(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public List<Recorded> load(UUID stream, long afterVersion) {
    return jdbc.query(
        "SELECT global_position,version,envelope::text,recorded_at FROM event_store"
            + " WHERE stream_id=? AND version>? ORDER BY version",
        (r, n) ->
            new Recorded(
                r.getLong(1),
                r.getLong(2),
                // Decoding upcasts events stored under an older schema version.
                MessageCodec.decode(r.getString(3)),
                r.getTimestamp(4).toInstant()),
        stream,
        afterVersion);
  }

  @Override
  public void append(UUID stream, long expectedVersion, List<Envelope> events) {
    try {
      long version = expectedVersion;
      for (var event : events)
        jdbc.update(
            "INSERT INTO event_store(stream_id,version,event_id,type,schema_version,envelope)"
                + " VALUES(?,?,?,?,?,?::jsonb)",
            stream,
            ++version,
            event.eventId(),
            event.type(),
            event.schemaVersion(),
            MessageCodec.encode(event));
    } catch (DuplicateKeyException e) {
      throw new ConcurrencyConflict(
          "Order " + stream + " changed after version " + expectedVersion + " was read");
    }
  }

  @Override
  public Optional<Snapshot> snapshot(UUID stream) {
    return jdbc
        .query(
            "SELECT version,state::text,taken_at FROM order_snapshot WHERE stream_id=? AND format=?",
            (r, n) ->
                new Snapshot(
                    r.getLong(1),
                    MessageCodec.json().readValue(r.getString(2), Order.class),
                    r.getTimestamp(3).toInstant()),
            stream,
            SNAPSHOT_FORMAT)
        .stream()
        .findFirst();
  }

  @Override
  public void saveSnapshot(Order state) {
    jdbc.update(
        "INSERT INTO order_snapshot(stream_id,version,format,state) VALUES(?,?,?,?::jsonb)"
            + " ON CONFLICT(stream_id) DO UPDATE SET version=EXCLUDED.version,format=EXCLUDED.format,"
            + "state=EXCLUDED.state,taken_at=clock_timestamp() WHERE order_snapshot.version<EXCLUDED.version",
        state.id(),
        state.version(),
        SNAPSHOT_FORMAT,
        MessageCodec.json().writeValueAsString(state));
  }

  @Override
  public List<UUID> recentStreams(int limit) {
    return jdbc.queryForList(
        "SELECT stream_id FROM event_store WHERE version=1 ORDER BY global_position DESC LIMIT ?",
        UUID.class,
        limit);
  }
}
