package io.zeroshift.docs;

import io.zeroshift.docs.Model.CodeDoc;
import java.util.List;

/** Stable error codes. Statuses are the ones the throwing code uses. */
final class Codes {
  private Codes() {}

  static List<CodeDoc> all() {
    return List.of(
        code(
            "VALIDATION_FAILED",
            400,
            "shared",
            "A bean-validation or method-validation constraint failed. errors[] names the fields."),
        code(
            "MALFORMED_REQUEST",
            400,
            "shared",
            "The body could not be read as the declared record."),
        code(
            "INVALID_PARAMETER",
            400,
            "shared",
            "A path, query or header parameter is missing or the wrong type."),
        code("NOT_FOUND", 404, "shared", "No handler matched, when no more specific code applies."),
        code("METHOD_NOT_ALLOWED", 405, "shared", "The path exists for a different method."),
        code(
            "UNSUPPORTED_MEDIA_TYPE",
            415,
            "shared",
            "The Content-Type is not one the handler reads."),
        code("CONFLICT", 409, "shared", "A conflict with no more specific code."),
        code("BAD_GATEWAY", 502, "shared", "The control plane called a service that failed."),
        code(
            "INTERNAL_ERROR",
            500,
            "shared",
            "An unexpected failure. The body does not include the cause."),
        code("ORDER_NOT_FOUND", 404, "order-service", "The order's event stream is empty."),
        code(
            "ORDER_RULE_VIOLATION",
            422,
            "order-service",
            "The command is well formed and still refused by the aggregate."),
        code(
            "IDEMPOTENCY_KEY_REUSED",
            422,
            "order-service",
            "Idempotency-Key was already stored for a different request body."),
        code(
            "CONCURRENT_UPDATE",
            409,
            "order-service",
            "An optimistic version check lost. Retry the request."),
        code(
            "ORDER_NOT_PROJECTED",
            404,
            "order-query-service",
            "The read model has no row for this order yet."),
        code(
            "READ_MODEL_BEHIND",
            409,
            "order-query-service",
            "A consistency-token read waited and the projection is still behind."),
        code("UNKNOWN_CONSUMER", 404, "platform", "No Kafka listener with that id."),
        code(
            "UNKNOWN_CONSUMER_ACTION",
            400,
            "platform",
            "Consumer actions are pause, resume, stop and start."),
        code(
            "UNKNOWN_GATEWAY_MODE",
            400,
            "payment-service",
            "Gateway mode is healthy, slow, down or declining."),
        code("UNKNOWN_KEYING", 400, "shipping-service", "Carrier keying is tracking, scan or hub."),
        code(
            "NO_SHIPPED_PARCELS", 409, "shipping-service", "The scan lab found no shipped parcel."),
        code(
            "PARTITIONS_CAN_ONLY_GROW",
            400,
            "shipping-service",
            "The requested partition count is not above the current one."),
        code(
            "INVALID_ACTION",
            409,
            "migration-lab",
            "Unknown migration or live-edit action, or the stage refuses it."),
        code(
            "KAFKA_ERROR",
            503,
            "kafka-lab",
            "A broker timed out or refused. The detail is Kafka's message."),
        code("SCENARIO_FAILED", 409, "kafka-lab", "A scenario step did not happen on the cluster."),
        code(
            "NOT_A_PROBE_TOPIC",
            400,
            "kafka-lab",
            "The probe only writes lab.durability or lab.replicated."),
        code("UNKNOWN_ACKS", 400, "kafka-lab", "acks is not a value that scenario accepts."),
        code(
            "NOT_A_LAB_TOPIC",
            400,
            "kafka-lab",
            "min.insync.replicas can only be changed on a lab.* topic."),
        code(
            "SCENARIO_RUNNING",
            409,
            "kafka-lab",
            "Another durability or delivery scenario holds the lock."),
        code(
            "UNKNOWN_PHASE",
            404,
            "kafka-lab",
            "The unclean scenario's phases are break, elect and check."),
        code(
            "UNKNOWN_DELIVERY_MODE",
            400,
            "kafka-lab",
            "Mode is at-most-once, at-least-once or exactly-once."),
        code("PARTITION_OFFLINE", 409, "kafka-lab", "lab.unclean still has no leader."),
        code("NOT_BROKEN_YET", 409, "kafka-lab", "elect or check ran before break."),
        code(
            "WORKER_TIMED_OUT",
            504,
            "kafka-lab",
            "A delivery worker did not finish within 90 seconds."),
        code(
            "TRAFFIC_NOT_RUNNING",
            409,
            "kafka-lab",
            "Stop was asked while traffic was already stopped."),
        code(
            "TRAFFIC_RUNNING",
            409,
            "kafka-lab",
            "Start was asked while traffic was already running."),
        code(
            "UNKNOWN_NODE_ACTION",
            400,
            "kafka-lab",
            "A node can be killed, stopped, started, paused or unpaused."),
        code("UNKNOWN_NODE", 404, "kafka-lab", "No Kafka lab node with that id."),
        code("NODE_ACTION_REFUSED", 409, "kafka-lab", "Docker refused the container action."),
        code(
            "CLUSTER_DEGRADED",
            409,
            "kafka-lab",
            "Fewer than three registered brokers, or no quorum leader."),
        code(
            "CLUSTER_NOT_RECOVERED",
            503,
            "kafka-lab",
            "The nodes did not all come back within 90 seconds."),
        code("CLUSTER_NOT_ANSWERING", 503, "kafka-lab", "An admin call exceeded its timeout."),
        code(
            "KAFKA_LAB_NOT_CONFIGURED",
            503,
            "kafka-lab",
            "The kafka-lab profile is not configured."),
        code("EXPERIMENT_NOT_FOUND", 404, "race-lab", "No race lab experiment with that id."),
        code("RUN_NOT_FOUND", 404, "race-lab", "No race lab run with that id."),
        code(
            "INVALID_RUN_CONFIG",
            400,
            "race-lab",
            "The experiment does not support the mode, or a value is outside its limits."),
        code(
            "RACE_LAB_BUSY",
            409,
            "race-lab",
            "Another run is executing; context.runningRunId names it. One run at a time."),
        code(
            "RUN_ALREADY_STARTED",
            409,
            "race-lab",
            "The run was already started; create a new run to race again."));
  }

  private static CodeDoc code(String code, int status, String scope, String when) {
    return new CodeDoc(code, status, scope, when);
  }
}
