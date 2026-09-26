package io.zeroshift.racelab.infrastructure.postgres;

import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.application.port.Session;
import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;

/** The lab's PostgreSQL, through its own pool whose connections work in the race_lab schema. */
public final class PostgresLabDatabase implements LabDatabase {
  /** Scenario tables, emptied by a reset. Run history is the repository's. */
  static final List<String> SCENARIO_TABLES =
      List.of(
          "reservation",
          "product",
          "deposit",
          "account",
          "payment",
          "order_effect",
          "orders",
          "document_revision",
          "document",
          "doctor");

  private final DataSource dataSource;
  private final JdbcTemplate jdbc;

  public PostgresLabDatabase(DataSource dataSource) {
    this.dataSource = dataSource;
    this.jdbc = new JdbcTemplate(dataSource);
  }

  @Override
  public Session openSession(String applicationName) {
    try {
      return new JdbcSession(dataSource.getConnection(), applicationName);
    } catch (SQLException e) {
      throw JdbcSession.failure(e);
    }
  }

  @Override
  public List<Map<String, Object>> query(String sql, Object... params) {
    return jdbc.queryForList(sql, params);
  }

  @Override
  public int update(String sql, Object... params) {
    return jdbc.update(sql, params);
  }

  @Override
  public long insert(String sql, Object... params) {
    Long id = jdbc.queryForObject(sql, Long.class, params);
    return id == null ? 0 : id;
  }

  @Override
  public Map<Integer, Waiting> waiting(Collection<Integer> pids) {
    var out = new HashMap<Integer, Waiting>();
    jdbc.query(
        "SELECT pid, pg_blocking_pids(pid) AS blockers, wait_event_type, wait_event"
            + " FROM pg_stat_activity WHERE pid = ANY(?)",
        ps -> ps.setArray(1, ps.getConnection().createArrayOf("int4", pids.toArray())),
        rs -> {
          Array blockers = rs.getArray("blockers");
          var by =
              blockers == null
                  ? List.<Integer>of()
                  : Arrays.stream((Integer[]) blockers.getArray()).toList();
          if (!by.isEmpty())
            out.put(
                rs.getInt("pid"),
                new Waiting(by, rs.getString("wait_event_type"), rs.getString("wait_event")));
        });
    return out;
  }

  @Override
  public String setting(String name) {
    return jdbc.queryForObject("SELECT current_setting(?)", String.class, name);
  }

  @Override
  public void cancel(int pid) {
    jdbc.queryForObject("SELECT pg_cancel_backend(?)", Boolean.class, pid);
  }

  @Override
  public Map<String, Integer> resetScenarios() {
    var counts = new LinkedHashMap<String, Integer>();
    for (var table : SCENARIO_TABLES)
      counts.put(table, jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class));
    jdbc.execute("TRUNCATE " + String.join(", ", SCENARIO_TABLES) + " RESTART IDENTITY");
    return counts;
  }
}
