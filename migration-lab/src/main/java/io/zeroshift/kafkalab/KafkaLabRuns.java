package io.zeroshift.kafkalab;

import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Every Kafka lab run, kept in the control plane's lab_run table next to the Phase 1 labs. */
@Component
public class KafkaLabRuns {
  public static final String FAILOVER = "kafka-failover";
  public static final String ACKS = "kafka-acks";
  public static final String RETRIES = "kafka-retries";
  public static final String UNCLEAN = "kafka-unclean";
  public static final String DELIVERY = "kafka-delivery";

  public record Run(
      long id, String lab, String mode, String summary, JsonNode result, Instant at) {}

  private final JdbcTemplate jdbc;
  private final JsonMapper json = JsonMapper.builder().findAndAddModules().build();

  public KafkaLabRuns(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public Run record(String lab, String mode, String summary, Object result) {
    var id =
        jdbc.queryForObject(
            "INSERT INTO lab_run(lab, mode, summary, result) VALUES(?,?,?,?::jsonb) RETURNING id",
            Long.class,
            lab,
            mode,
            summary,
            json.writeValueAsString(result));
    return recent(lab, 1).stream().filter(r -> r.id() == id).findFirst().orElseThrow();
  }

  public List<Run> recent(String lab, int limit) {
    return jdbc.query(
        "SELECT id, lab, mode, summary, result::text AS result, at FROM lab_run WHERE lab=? ORDER BY id DESC LIMIT ?",
        (rs, i) ->
            new Run(
                rs.getLong("id"),
                rs.getString("lab"),
                rs.getString("mode"),
                rs.getString("summary"),
                json.readTree(rs.getString("result")),
                rs.getTimestamp("at").toInstant()),
        lab,
        limit);
  }
}
