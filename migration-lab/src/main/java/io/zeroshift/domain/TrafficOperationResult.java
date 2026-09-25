package io.zeroshift.domain;

/** A routed traffic attempt, suitable for durable logging after the database work finishes. */
public record TrafficOperationResult(
    TrafficOperation operation,
    Primary target,
    Table table,
    Long recordId,
    String details,
    boolean success) {
  public static TrafficOperationResult success(
      TrafficOperation operation, Primary target, TrafficOperationOutcome outcome) {
    return new TrafficOperationResult(
        operation, target, outcome.table(), outcome.recordId(), outcome.details(), true);
  }

  public static TrafficOperationResult failure(
      TrafficOperation operation, Primary target, String details) {
    return new TrafficOperationResult(operation, target, operation.table(), null, details, false);
  }
}
