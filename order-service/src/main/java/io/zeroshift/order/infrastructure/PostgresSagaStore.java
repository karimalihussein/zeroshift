package io.zeroshift.order.infrastructure;

import io.zeroshift.order.application.ConcurrencyConflict;
import io.zeroshift.order.application.SagaStore;
import io.zeroshift.order.domain.Saga;
import io.zeroshift.order.domain.SagaState;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public final class PostgresSagaStore implements SagaStore {
  private static final RowMapper<Saga> SAGA =
      (r, n) ->
          new Saga(
              r.getObject("order_id", UUID.class),
              r.getObject("correlation_id", UUID.class),
              SagaState.valueOf(r.getString("state")),
              instant(r, "deadline"),
              new TreeSet<>(Arrays.asList((String[]) r.getArray("pending").getArray())),
              r.getString("failure_reason"),
              List.of((String[]) r.getArray("compensations").getArray()),
              r.getInt("version"),
              instant(r, "started_at"),
              instant(r, "updated_at"));

  private final JdbcTemplate jdbc;

  public PostgresSagaStore(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private static Instant instant(ResultSet r, String column) throws SQLException {
    var value = r.getTimestamp(column);
    return value == null ? null : value.toInstant();
  }

  @Override
  public void start(Saga saga) {
    jdbc.update(
        "INSERT INTO saga(order_id,correlation_id,state,deadline,version,started_at,updated_at)"
            + " VALUES(?,?,?,?,1,?,?)",
        saga.orderId(),
        saga.correlationId(),
        saga.state().name(),
        Timestamp.from(saga.deadline()),
        Timestamp.from(saga.startedAt()),
        Timestamp.from(saga.startedAt()));
    transition(saga, null, "PlaceOrder", null, "order placed → AuthorizePayment sent");
  }

  @Override
  public Optional<Saga> find(UUID orderId) {
    return jdbc.query("SELECT * FROM saga WHERE order_id=?", SAGA, orderId).stream().findFirst();
  }

  @Override
  public void save(Saga saga, String from, String trigger, UUID triggerEventId, String detail) {
    int saved =
        jdbc.update(
            connection -> {
              var s =
                  connection.prepareStatement(
                      "UPDATE saga SET state=?,deadline=?,pending=?,failure_reason=?,compensations=?,"
                          + "version=version+1,updated_at=clock_timestamp() WHERE order_id=? AND version=?");
              s.setString(1, saga.state().name());
              s.setTimestamp(2, saga.deadline() == null ? null : Timestamp.from(saga.deadline()));
              s.setArray(3, connection.createArrayOf("text", saga.pending().toArray()));
              s.setString(4, saga.failureReason());
              s.setArray(5, connection.createArrayOf("text", saga.compensations().toArray()));
              s.setObject(6, saga.orderId());
              s.setInt(7, saga.version());
              return s;
            });
    if (saved != 1)
      throw new ConcurrencyConflict(
          "Saga " + saga.orderId() + " changed after version " + saga.version() + " was read");
    transition(saga, from, trigger, triggerEventId, detail);
  }

  private void transition(
      Saga saga, String from, String trigger, UUID triggerEventId, String detail) {
    jdbc.update(
        "INSERT INTO saga_transition(order_id,from_state,to_state,trigger_type,trigger_event_id,detail)"
            + " VALUES(?,?,?,?,?,?)",
        saga.orderId(),
        from,
        saga.state().name(),
        trigger,
        triggerEventId,
        detail);
  }

  @Override
  public List<Saga> due(Instant now, int limit) {
    return jdbc.query(
        "SELECT * FROM saga WHERE deadline<? AND state IN ('AWAITING_PAYMENT','AWAITING_STOCK')"
            + " ORDER BY deadline LIMIT ? FOR UPDATE SKIP LOCKED",
        SAGA,
        Timestamp.from(now),
        limit);
  }

  @Override
  public List<Saga> recent(int limit) {
    return jdbc.query("SELECT * FROM saga ORDER BY started_at DESC LIMIT ?", SAGA, limit);
  }

  @Override
  public List<Map<String, Object>> transitions(UUID orderId) {
    return jdbc.queryForList(
        "SELECT from_state,to_state,trigger_type,trigger_event_id,detail,at FROM saga_transition"
            + " WHERE order_id=? ORDER BY id",
        orderId);
  }
}
