package io.zeroshift.racelab.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * One recorded fact of a run. {@code lane} is a request's label (A, B, …), {@code DB} for committed
 * state read by the engine, or {@code LAB} for the experiment itself. Every value was observed:
 * {@code txId} and {@code pid} come from PostgreSQL ({@code pg_current_xact_id()}, {@code
 * pg_backend_pid()}), {@code blockedBy} from {@code pg_blocking_pids()}, versions and values from
 * the rows the statement returned. Only the fields that apply to the type are set.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RaceEvent(
    long runId,
    int seq,
    EventType type,
    String lane,
    Integer attempt,
    Instant at,
    long atMicros,
    Long durationMicros,
    String requestId,
    Long txId,
    Integer pid,
    String thread,
    String isolation,
    String sql,
    String target,
    String valueRead,
    String valueWritten,
    Long versionRead,
    Long versionWritten,
    Long expectedVersion,
    Long actualVersion,
    String lock,
    List<String> blockedBy,
    Long waitMicros,
    Integer rows,
    Boolean ok,
    String outcome,
    String sqlState,
    String message,
    String traceId,
    String spanId,
    Map<String, Object> data) {

  public static Builder of(EventType type, String lane) {
    return new Builder(type, lane);
  }

  /** Assembles an event; the recorder stamps run, sequence and time. */
  public static final class Builder {
    private final EventType type;
    private final String lane;
    private Integer attempt;
    private Instant at;
    private Long atMicros;
    private Long durationMicros;
    private String requestId;
    private Long txId;
    private Integer pid;
    private String thread;
    private String isolation;
    private String sql;
    private String target;
    private String valueRead;
    private String valueWritten;
    private Long versionRead;
    private Long versionWritten;
    private Long expectedVersion;
    private Long actualVersion;
    private String lock;
    private List<String> blockedBy;
    private Long waitMicros;
    private Integer rows;
    private Boolean ok;
    private String outcome;
    private String sqlState;
    private String message;
    private String traceId;
    private String spanId;
    private Map<String, Object> data;

    private Builder(EventType type, String lane) {
      this.type = type;
      this.lane = lane;
    }

    public EventType type() {
      return type;
    }

    public String lane() {
      return lane;
    }

    public Builder attempt(Integer v) {
      attempt = v;
      return this;
    }

    /** When the operation began, if earlier than the moment the event is recorded. */
    public Builder startedAt(Instant wall, long micros) {
      at = wall;
      atMicros = micros;
      return this;
    }

    public Builder durationMicros(Long v) {
      durationMicros = v;
      return this;
    }

    public Builder requestId(String v) {
      requestId = v;
      return this;
    }

    public Builder txId(Long v) {
      txId = v;
      return this;
    }

    public Builder pid(Integer v) {
      pid = v;
      return this;
    }

    public Builder thread(String v) {
      thread = v;
      return this;
    }

    public Builder isolation(String v) {
      isolation = v;
      return this;
    }

    public Builder sql(String v) {
      sql = v;
      return this;
    }

    public Builder target(String v) {
      target = v;
      return this;
    }

    public Builder valueRead(String v) {
      valueRead = v;
      return this;
    }

    public Builder valueWritten(String v) {
      valueWritten = v;
      return this;
    }

    public Builder versionRead(Long v) {
      versionRead = v;
      return this;
    }

    public Builder versionWritten(Long v) {
      versionWritten = v;
      return this;
    }

    public Builder expectedVersion(Long v) {
      expectedVersion = v;
      return this;
    }

    public Builder actualVersion(Long v) {
      actualVersion = v;
      return this;
    }

    public Builder lock(String v) {
      lock = v;
      return this;
    }

    public Builder blockedBy(List<String> v) {
      blockedBy = v == null ? null : List.copyOf(v);
      return this;
    }

    public Builder waitMicros(Long v) {
      waitMicros = v;
      return this;
    }

    public Builder rows(Integer v) {
      rows = v;
      return this;
    }

    public Builder ok(Boolean v) {
      ok = v;
      return this;
    }

    public Builder outcome(String v) {
      outcome = v;
      return this;
    }

    public Builder sqlState(String v) {
      sqlState = v;
      return this;
    }

    public Builder message(String v) {
      message = v;
      return this;
    }

    public Builder trace(String traceId, String spanId) {
      this.traceId = traceId;
      this.spanId = spanId;
      return this;
    }

    public Builder data(Map<String, Object> v) {
      data = v == null ? null : Map.copyOf(v);
      return this;
    }

    public RaceEvent build(long runId, int seq, Instant now, long nowMicros) {
      return new RaceEvent(
          runId,
          seq,
          type,
          lane,
          attempt,
          at != null ? at : now,
          atMicros != null ? atMicros : nowMicros,
          durationMicros,
          requestId,
          txId,
          pid,
          thread,
          isolation,
          sql,
          target,
          valueRead,
          valueWritten,
          versionRead,
          versionWritten,
          expectedVersion,
          actualVersion,
          lock,
          blockedBy,
          waitMicros,
          rows,
          ok,
          outcome,
          sqlState,
          message,
          traceId,
          spanId,
          data);
    }
  }
}
