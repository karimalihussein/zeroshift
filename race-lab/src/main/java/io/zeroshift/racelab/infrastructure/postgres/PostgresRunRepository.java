package io.zeroshift.racelab.infrastructure.postgres;

import io.zeroshift.racelab.application.port.RunRepository;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/** Runs and their events in race_lab.run and race_lab.run_event, as JSON documents. */
public final class PostgresRunRepository implements RunRepository {
  private static final String COLUMNS =
      "id, experiment, status, config::text AS config, requests::text AS requests,"
          + " result::text AS result, error, trace_id, created_at, started_at, completed_at";

  private final JdbcTemplate jdbc;
  private final JsonMapper json = JsonMapper.builder().findAndAddModules().build();

  public PostgresRunRepository(DataSource dataSource) {
    this.jdbc = new JdbcTemplate(dataSource);
  }

  @Override
  public Run create(RunConfig config, List<Run.Request> requests) {
    Long id =
        jdbc.queryForObject(
            "INSERT INTO run(experiment, mode, isolation, status, config, requests)"
                + " VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb) RETURNING id",
            Long.class,
            config.experiment(),
            config.mode().name(),
            config.isolation().name(),
            Run.Status.CREATED.name(),
            json.writeValueAsString(config),
            json.writeValueAsString(requests));
    return find(id).orElseThrow();
  }

  @Override
  public void save(Run run) {
    jdbc.update(
        "UPDATE run SET status = ?, trace_id = ?, started_at = ?, completed_at = ?,"
            + " result = ?::jsonb, error = ? WHERE id = ?",
        run.status().name(),
        run.traceId(),
        timestamp(run.startedAt()),
        timestamp(run.completedAt()),
        run.result() == null ? null : json.writeValueAsString(run.result()),
        run.error(),
        run.id());
  }

  @Override
  public Optional<Run> find(long id) {
    return jdbc.query("SELECT " + COLUMNS + " FROM run WHERE id = ?", this::run, id).stream()
        .findFirst();
  }

  @Override
  public List<Run> recent(String experiment, int limit) {
    return experiment == null
        ? jdbc.query("SELECT " + COLUMNS + " FROM run ORDER BY id DESC LIMIT ?", this::run, limit)
        : jdbc.query(
            "SELECT " + COLUMNS + " FROM run WHERE experiment = ? ORDER BY id DESC LIMIT ?",
            this::run,
            experiment,
            limit);
  }

  @Override
  public void append(List<RaceEvent> events) {
    jdbc.batchUpdate(
        "INSERT INTO run_event(run_id, seq, type, lane, at_micros, payload)"
            + " VALUES (?, ?, ?, ?, ?, ?::jsonb) ON CONFLICT DO NOTHING",
        events,
        200,
        (ps, e) -> {
          ps.setLong(1, e.runId());
          ps.setInt(2, e.seq());
          ps.setString(3, e.type().name());
          ps.setString(4, e.lane());
          ps.setLong(5, e.atMicros());
          ps.setString(6, json.writeValueAsString(e));
        });
  }

  @Override
  public List<RaceEvent> events(long runId, int afterSeq, int limit) {
    return jdbc.query(
        "SELECT payload::text FROM run_event WHERE run_id = ? AND seq > ? ORDER BY seq LIMIT ?",
        (rs, i) -> json.readValue(rs.getString(1), RaceEvent.class),
        runId,
        afterSeq,
        limit);
  }

  @Override
  public Map<String, Integer> deleteAll() {
    var counts = new LinkedHashMap<String, Integer>();
    counts.put("run", jdbc.queryForObject("SELECT count(*) FROM run", Integer.class));
    counts.put("run_event", jdbc.queryForObject("SELECT count(*) FROM run_event", Integer.class));
    jdbc.execute("TRUNCATE run_event, run RESTART IDENTITY");
    return counts;
  }

  private Run run(ResultSet rs, int row) throws SQLException {
    var result = rs.getString("result");
    return new Run(
        rs.getLong("id"),
        rs.getString("experiment"),
        json.readValue(rs.getString("config"), RunConfig.class),
        Run.Status.valueOf(rs.getString("status")),
        instant(rs.getTimestamp("created_at")),
        instant(rs.getTimestamp("started_at")),
        instant(rs.getTimestamp("completed_at")),
        rs.getString("trace_id"),
        json.readValue(rs.getString("requests"), new TypeReference<List<Run.Request>>() {}),
        result == null ? null : json.readValue(result, RunResult.class),
        rs.getString("error"));
  }

  private static Timestamp timestamp(Instant at) {
    return at == null ? null : Timestamp.from(at);
  }

  private static Instant instant(Timestamp t) {
    return t == null ? null : t.toInstant();
  }
}
