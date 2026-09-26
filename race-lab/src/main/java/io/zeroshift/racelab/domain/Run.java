package io.zeroshift.racelab.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.List;

/** A run of one experiment: its configuration, its requests' identities and, once done, results. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Run(
    long id,
    String experiment,
    RunConfig config,
    Status status,
    Instant createdAt,
    Instant startedAt,
    Instant completedAt,
    String traceId,
    List<Request> requests,
    RunResult result,
    String error) {

  public enum Status {
    CREATED,
    RUNNING,
    COMPLETED,
    FAILED
  }

  /** A request's generated identities, fresh for every run. */
  public record Request(
      String lane, String requestId, String orderId, String customerId, String role) {}

  public Run withStatus(Status next, Instant started, Instant completed, String trace) {
    return new Run(
        id,
        experiment,
        config,
        next,
        createdAt,
        started,
        completed,
        trace,
        requests,
        result,
        error);
  }

  public Run completed(RunResult done, Instant at) {
    return new Run(
        id,
        experiment,
        config,
        Status.COMPLETED,
        createdAt,
        startedAt,
        at,
        traceId,
        requests,
        done,
        null);
  }

  public Run failed(String reason, Instant at) {
    return new Run(
        id,
        experiment,
        config,
        Status.FAILED,
        createdAt,
        startedAt,
        at,
        traceId,
        requests,
        result,
        reason);
  }
}
