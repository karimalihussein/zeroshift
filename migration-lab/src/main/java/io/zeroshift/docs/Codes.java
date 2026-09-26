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
        code(
            "RATE_LIMITED",
            429,
            "order-service",
            "POST /orders over the edge rate limit of this replica. Retry-After says when to retry."),
        code(
            "LOAD_SHED",
            503,
            "order-service",
            "POST /orders refused because too many orders are unfinished. Retry-After says when to retry."),
        code(
            "BULKHEAD_FULL",
            503,
            "order-service",
            "POST /orders refused because the replica is already running its maximum concurrent placements."),
        code("INVALID_GUARD_SETTINGS", 400, "order-service", "An edge guard limit is below 1."),
        code(
            "UNKNOWN_RETRY_MODE",
            400,
            "payment-service, migration-lab",
            "A retry mode that is not one of the listed ones."),
        code(
            "INVALID_GATEWAY_POLICY",
            400,
            "payment-service",
            "Gateway timeout or attempts outside the allowed range."),
        code("UNKNOWN_SKU", 404, "inventory-service", "Restock of a SKU that is not a product."),
        code(
            "INVALID_LOAD_PROFILE",
            400,
            "migration-lab",
            "A load generator setting outside the lab limits."),
        code(
            "UNKNOWN_FAULT",
            400,
            "migration-lab",
            "A chaos fault that is not latency, bandwidth, reset, partition or down."),
        code(
            "UNKNOWN_LINK",
            404,
            "migration-lab",
            "A chaos link that is not one of the five proxied connections."),
        code(
            "CHAOS_NOT_RUNNING",
            409,
            "migration-lab",
            "Network chaos needs the Toxiproxy overlay, docker-compose.chaos.yml."),
        code("UNKNOWN_EXPERIMENT", 404, "migration-lab", "No resilience experiment with this id."),
        code(
            "EXPERIMENT_RUNNING",
            409,
            "migration-lab",
            "Another resilience experiment is running."),
        code(
            "NOT_WAITING",
            409,
            "migration-lab",
            "No resilience experiment is waiting for its next step."),
        code("NO_EXPERIMENT", 409, "migration-lab", "No resilience experiment has run yet."),
        code("UNKNOWN_LAB", 404, "migration-lab", "No events-over-time lab with this id."),
        code("OUT_OF_ORDER", 409, "migration-lab", "A lab step run before the steps before it."),
        code(
            "STEP_FAILED",
            502,
            "migration-lab",
            "A lab step failed against the running system; it can be run again."),
        code("UNKNOWN_FAILURE_LAB", 404, "migration-lab", "No failure lab with this id."),
        code(
            "STAGE_OUT_OF_ORDER",
            409,
            "migration-lab",
            "A failure-lab stage run before the stages before it."),
        code(
            "STAGE_FAILED",
            502,
            "migration-lab",
            "A failure-lab stage failed, or one of its claims did not hold; it can be run again."),
        code(
            "FAILURE_LAB_BUSY",
            409,
            "migration-lab",
            "A stage, reset or recovery action of this failure lab is still running."),
        code(
            "INVALID_FAILURE_ACTION",
            400,
            "migration-lab",
            "A manual failure-lab action named something the lab does not own or support."),
        code(
            "FAILURE_LAB_UNAVAILABLE",
            503,
            "migration-lab",
            "The failure lab's own infrastructure (profile failure-lab) is not reachable."),
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
            "UNKNOWN_CUSTOMER",
            422,
            "order-service",
            "POST /orders for a customerId that is not a customer of order-service."),
        code(
            "VOUCHER_NOT_APPLICABLE",
            422,
            "order-service",
            "The voucher code does not exist, is inactive, is outside its validity, or the subtotal is below its minimum."),
        code(
            "VOUCHER_EXHAUSTED",
            409,
            "order-service",
            "The voucher's usage limit is used up, possibly by a concurrent order. The same request will not succeed later."),
        code(
            "CATALOG_UNAVAILABLE",
            503,
            "order-service",
            "POST /orders could not price its items because inventory-service did not answer. Retry-After says when to retry."),
        code("CUSTOMER_NOT_FOUND", 404, "order-service", "GET /customers/{id} for an unknown id."),
        code(
            "PRODUCT_NOT_FOUND", 404, "inventory-service", "GET /products/{id} for an unknown id."),
        code("PAYMENT_NOT_FOUND", 404, "payment-service", "GET /payments/{id} for an unknown id."),
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
