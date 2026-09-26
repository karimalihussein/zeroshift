package io.zeroshift.docs;

import static io.zeroshift.docs.Op.field;
import static io.zeroshift.docs.Op.schema;

import io.zeroshift.docs.Model.Operation;
import io.zeroshift.docs.Model.Schema;
import java.util.ArrayList;
import java.util.List;

/** Every HTTP mapping in the repository, keyed so a test can notice a new one. */
final class Surface {
  private Surface() {}

  static List<Operation> all() {
    var ops = new ArrayList<Operation>();
    commerce(ops);
    platform(ops);
    migration(ops);
    LabSurface.add(ops);
    return List.copyOf(ops);
  }

  private static void commerce(List<Operation> ops) {
    var order = "order-service/src/main/java/io/zeroshift/order/web/OrderController.java";
    ops.add(
        Op.api("order-service", "order-service", "POST", "/orders")
            .group("Orders")
            .title("Create order")
            .summary(
                "Places an order. The order and its OrderPlaced event commit in one database transaction; Debezium publishes the outbox row. The response version is the read-your-writes token for the query service.")
            .header(
                "Idempotency-Key",
                false,
                "Request header, optional, 1–200 characters. The same key and the same body return the first response. The same key with a different body is IDEMPOTENCY_KEY_REUSED.")
            .header(
                "Idempotent-Replayed",
                false,
                "Response header. true when this answer was stored for an earlier Idempotency-Key.")
            .body(
                schema(
                    "PlaceOrderRequest",
                    "order-service/src/main/java/io/zeroshift/order/web/PlaceOrderRequest.java",
                    """
                    {
                      "customerId": "ada",
                      "items": [
                        { "sku": "SKU-CABLE", "quantity": 1 }
                      ]
                    }
                    """,
                    field("customerId", "string", true, "1–100 characters."),
                    field(
                        "items",
                        "Line[]",
                        true,
                        "1–20 lines. Line is sku (1–40 characters) and quantity (1–99). SKUs must exist in the order service catalog.")))
            .response(
                202,
                "Accepted. The body is the OrderAccepted record, not an envelope.",
                schema(
                    "OrderAccepted",
                    "order-service/src/main/java/io/zeroshift/order/web/OrderViews.java",
                    """
                    {
                      "orderId": "3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44",
                      "correlationId": "8a2e1c44-1b6f-4d0a-9e77-2c5b8f0d11aa",
                      "eventId": "11111111-1111-4111-8111-111111111111",
                      "total": 9.99,
                      "version": 1,
                      "replayed": false,
                      "traceId": "5a3057b3f1f7a1e0c0ffee0000000001"
                    }
                    """,
                    field("orderId", "uuid", true, "The new order."),
                    field(
                        "correlationId",
                        "uuid",
                        true,
                        "Shared by every message this request causes."),
                    field("eventId", "uuid", true, "The OrderPlaced event id."),
                    field(
                        "total",
                        "decimal",
                        true,
                        "Sum of line subtotals, priced from the catalog."),
                    field(
                        "version",
                        "long",
                        true,
                        "Event-stream version. Pass it as minVersion to the query service."),
                    field(
                        "replayed",
                        "boolean",
                        true,
                        "True when an Idempotency-Key reused the first answer."),
                    field(
                        "traceId",
                        "string",
                        true,
                        "OpenTelemetry trace id, when the agent is attached.")))
            .error(400, "VALIDATION_FAILED", "customerId or a line fails its constraint.")
            .error(400, "MALFORMED_REQUEST", "The body is not JSON the record can bind.")
            .error(
                422, "ORDER_RULE_VIOLATION", "Unknown SKU, or an order rule rejected the command.")
            .error(
                422,
                "IDEMPOTENCY_KEY_REUSED",
                "This Idempotency-Key was already used for a different body.")
            .idempotency(
                "Optional Idempotency-Key. A retry with the same key and body returns the stored 202 and Idempotent-Replayed: true. Without a key, a retry places a second order.")
            .events("OrderPlaced")
            .source(order)
            .done("order-post-orders"));
    ops.add(
        Op.api("order-service", "order-service", "POST", "/orders/dual-write")
            .group("Orders")
            .title("Dual-write demo")
            .summary(
                "The anti-pattern the outbox replaces. The database and Kafka are written separately, and the call then crashes or rolls back. It starts no saga.")
            .query("mode", "enum", true, null, "COMMIT_THEN_CRASH or PUBLISH_THEN_ROLLBACK.")
            .body(placeOrder())
            .response(
                200,
                "DualWriteDemo.Result. The process may die after the response when the mode crashes.",
                schema(
                    "Result",
                    "order-service/src/main/java/io/zeroshift/order/infrastructure/DualWriteDemo.java",
                    """
                    { "orderId": "3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44", "eventId": "11111111-1111-4111-8111-111111111111", "outcome": "committed" }
                    """,
                    field("orderId", "uuid", true, ""),
                    field("eventId", "uuid", true, ""),
                    field(
                        "outcome",
                        "string",
                        true,
                        "What the demo managed to do before crashing or rolling back.")))
            .error(400, "INVALID_PARAMETER", "mode is missing or not a DualWriteDemo.Mode.")
            .source(order)
            .done("order-post-dual-write"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/orders")
            .group("Orders")
            .title("List orders")
            .summary(
                "Newest sagas, each with the order folded from its event stream. A customer filter searches the newest 100 sagas, then keeps that customer.")
            .query(
                "limit",
                "integer",
                false,
                "20",
                "1–100. The service reads one extra row and sets meta.hasMore.")
            .query("customerId", "string", false, null, "Optional, at most 100 characters.")
            .response(
                200,
                "ApiResponse of OrderSummary. data is the page; meta carries count, limit and hasMore.",
                listMeta())
            .error(
                400,
                "VALIDATION_FAILED",
                "limit is outside 1–100, or customerId is longer than 100.")
            .idempotency("Safe read.")
            .source(order)
            .done("order-get-orders"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/orders/{id}")
            .group("Orders")
            .title("Get order")
            .summary(
                "The write model: the order folded from its snapshot and later events, the saga, and the recorded transitions. This is not the query service's read model.")
            .path("id", "uuid", "Order id.")
            .query(
                "useSnapshot",
                "boolean",
                false,
                "true",
                "false rebuilds the aggregate from the first event.")
            .response(
                200,
                "OrderDetail: order, rebuiltFrom, snapshotVersion, events, saga, transitions.",
                schema(
                    "OrderDetail",
                    "order-service/src/main/java/io/zeroshift/order/web/OrderViews.java",
                    null,
                    field(
                        "order",
                        "Order",
                        true,
                        "id, customerId, lines, total, currency, status, paymentId, reservationId, trackingNumber, cancelReason, version."),
                    field(
                        "rebuiltFrom",
                        "string",
                        true,
                        "How the fold was described, for example a snapshot version plus the events after it."),
                    field("snapshotVersion", "long", false, "Null when no snapshot was used."),
                    field(
                        "events",
                        "EventView[]",
                        true,
                        "position, version, eventId, type, schemaVersion, correlationId, causationId, recordedAt, payload."),
                    field("saga", "Saga", false, "Null when this order has no saga."),
                    field(
                        "transitions",
                        "Transition[]",
                        true,
                        "fromState, toState, triggerType, triggerEventId, detail, at.")))
            .error(404, "ORDER_NOT_FOUND", "No event stream for this id (the folded version is 0).")
            .error(400, "INVALID_PARAMETER", "id is not a UUID.")
            .idempotency("Safe read.")
            .source(order)
            .done("order-get-order"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/orders/{id}/fold")
            .group("Orders")
            .title("Fold the event stream")
            .summary(
                "Every intermediate state: the aggregate rebuilt one event at a time, without a snapshot.")
            .path("id", "uuid", "Order id.")
            .response(
                200,
                "A JSON array of OrderRepository.Step. Not wrapped in ApiResponse.",
                schema(
                    "Step",
                    "order-service/src/main/java/io/zeroshift/order/application/OrderRepository.java",
                    null,
                    field("version", "long", true, ""),
                    field("type", "string", true, "Event type name."),
                    field("stateAfter", "Order", true, "The aggregate after this event.")))
            .idempotency("Safe read.")
            .source(order)
            .done("order-get-fold"));
    ops.add(
        Op.api("order-service", "order-service", "DELETE", "/orders/{id}/snapshot")
            .group("Orders")
            .title("Discard snapshot")
            .summary(
                "Deletes the stored snapshot so the next load replays the whole stream. The events stay.")
            .path("id", "uuid", "Order id.")
            .response(204, "No body.", null)
            .idempotency("Deleting an absent snapshot is the same end state.")
            .source(order)
            .done("order-delete-snapshot"));
    ops.add(
        Op.api("order-service", "order-service", "POST", "/orders/{id}/refund")
            .group("Orders")
            .title("Refund an order")
            .summary("Operator recovery. Records a RefundPayment command through the outbox.")
            .path("id", "uuid", "Order id.")
            .query("reason", "string", false, "operator refund", "1–200 characters.")
            .response(
                202,
                "RefundAccepted.",
                schema(
                    "RefundAccepted",
                    "order-service/src/main/java/io/zeroshift/order/web/OrderViews.java",
                    """
                    { "orderId": "3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44", "command": "RefundPayment", "eventId": "11111111-1111-4111-8111-111111111111" }
                    """,
                    field("orderId", "uuid", true, ""),
                    field("command", "string", true, "The command type name."),
                    field("eventId", "uuid", true, "")))
            .error(
                422, "ORDER_RULE_VIOLATION", "The order cannot be refunded from its current state.")
            .error(404, "ORDER_NOT_FOUND", "Unknown order.")
            .events("RefundPayment")
            .source(order)
            .done("order-post-refund"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/lab/lease")
            .group("Experiments")
            .title("Saga scanner lease")
            .summary(
                "Who currently holds the saga-timeout scanner lease, and which replica answered.")
            .response(
                200,
                "LeaseView.",
                schema(
                    "LeaseView",
                    "order-service/src/main/java/io/zeroshift/order/web/OrderViews.java",
                    null,
                    field("name", "string", true, "Lease name."),
                    field("owner", "string", false, "Holder, when the lease is held."),
                    field("token", "long", false, "Fencing token."),
                    field("acquiredAt", "datetime", false, ""),
                    field("expiresAt", "datetime", false, ""),
                    field("held", "boolean", true, ""),
                    field("instance", "string", true, "The replica that answered.")))
            .idempotency("Safe read.")
            .source(order)
            .done("order-get-lease"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/catalog")
            .group("Orders")
            .title("Product catalog")
            .summary("The prices the order service charges. Stock lives in the inventory service.")
            .response(
                200,
                "ApiResponse of Catalog.Product. A complete list, so meta.hasMore is null.",
                schema(
                    "Product",
                    "order-service/src/main/java/io/zeroshift/order/application/Catalog.java",
                    """
                    { "data": [ { "sku": "SKU-CABLE", "name": "USB-C cable", "price": 9.99 } ], "meta": { "count": 1, "limit": null, "hasMore": null } }
                    """,
                    field("sku", "string", true, ""),
                    field("name", "string", true, ""),
                    field("price", "decimal", true, "")))
            .idempotency("Safe read.")
            .source(order)
            .done("order-get-catalog"));

    var query = "order-query-service/src/main/java/io/zeroshift/query/QueryController.java";
    ops.add(
        Op.api("order-query-service", "order-query-service", "GET", "/orders")
            .group("Queries")
            .title("List projected orders")
            .summary(
                "The CQRS read model, newest placedAt first. This is not the order service's write model.")
            .query(
                "limit",
                "integer",
                false,
                "50",
                "1–500. One extra row is fetched to set meta.hasMore.")
            .response(200, "ApiResponse of OrderView.", listMeta())
            .error(400, "VALIDATION_FAILED", "limit is outside 1–500.")
            .idempotency("Safe read.")
            .consistency("Eventually consistent. A write you just made may be missing.")
            .source(query)
            .done("query-get-orders"));
    ops.add(
        Op.api("order-query-service", "order-query-service", "GET", "/orders/{id}")
            .group("Queries")
            .title("Get projected order")
            .summary(
                "Without minVersion this is an eventually consistent read. With the version returned by POST /orders, the service waits up to waitMs for the projection and, if still behind, returns 409 rather than a stale order.")
            .path("id", "uuid", "Order id.")
            .query(
                "minVersion",
                "integer",
                false,
                null,
                "Consistency token. Minimum eventsApplied required. At least 1 when present.")
            .query("waitMs", "long", false, "0", "0–5000. How long to wait for minVersion.")
            .header(
                "X-Projected-Version",
                false,
                "Response header when a consistency-token read succeeds. The version the projection had applied.")
            .header(
                "X-Waited-Ms",
                false,
                "Response header when a consistency-token read succeeds. How long the answer waited.")
            .response(
                200,
                "OrderView, the read model row.",
                schema(
                    "OrderView",
                    "order-query-service/src/main/java/io/zeroshift/query/ReadModels.java",
                    null,
                    field("orderId", "uuid", true, ""),
                    field("customerId", "string", true, ""),
                    field("status", "string", true, ""),
                    field("total", "decimal", true, ""),
                    field("currency", "string", true, ""),
                    field("itemCount", "integer", true, ""),
                    field("lines", "OrderLine[]", true, "sku, quantity, unitPrice."),
                    field("paymentId", "uuid", false, ""),
                    field("reservationId", "uuid", false, ""),
                    field("trackingNumber", "string", false, ""),
                    field("cancelReason", "string", false, ""),
                    field("compensations", "string[]", true, ""),
                    field("eventsApplied", "integer", true, "Read-side version."),
                    field("lastEventType", "string", false, ""),
                    field("lastEventId", "uuid", false, ""),
                    field("lastOffset", "string", false, "Kafka offset of the last applied event."),
                    field("placedAt", "datetime", false, ""),
                    field("projectedAt", "datetime", false, "")))
            .error(404, "ORDER_NOT_PROJECTED", "No minVersion, and the projection has no row yet.")
            .error(
                409,
                "READ_MODEL_BEHIND",
                "minVersion was set and the projection is still behind after waitMs. context has requiredVersion, projectedVersion and waitedMs.")
            .error(400, "VALIDATION_FAILED", "minVersion < 1 or waitMs is outside 0–5000.")
            .idempotency("Safe read.")
            .consistency(
                "Pass the POST /orders version as minVersion. The service waits up to waitMs. Success sends X-Projected-Version and X-Waited-Ms. Still behind is 409 READ_MODEL_BEHIND.")
            .source(query)
            .done("query-get-order"));
    ops.add(
        Op.api("order-query-service", "order-query-service", "GET", "/customers")
            .group("Queries")
            .title("Customer summary")
            .summary("One row per customer, ordered by shipped value.")
            .response(
                200,
                "ApiResponse of CustomerView. A complete list.",
                schema(
                    "CustomerView",
                    "order-query-service/src/main/java/io/zeroshift/query/ReadModels.java",
                    null,
                    field("customerId", "string", true, ""),
                    field("ordersPlaced", "integer", true, ""),
                    field("ordersShipped", "integer", true, ""),
                    field("ordersCancelled", "integer", true, ""),
                    field("shippedValue", "decimal", true, ""),
                    field("updatedAt", "datetime", true, "")))
            .idempotency("Safe read.")
            .source(query)
            .done("query-get-customers"));
    ops.add(
        Op.api("order-query-service", "order-query-service", "GET", "/lab/projection")
            .group("Experiments")
            .title("Projection stats")
            .summary(
                "How many orders and customers the read model holds, and when it last projected.")
            .response(
                200,
                "ReadModels.Stats.",
                schema(
                    "Stats",
                    "order-query-service/src/main/java/io/zeroshift/query/ReadModels.java",
                    null,
                    field("orders", "integer", true, ""),
                    field("customers", "integer", true, ""),
                    field("lastProjectedAt", "datetime", false, "")))
            .idempotency("Safe read.")
            .source(query)
            .done("query-get-projection"));
    ops.add(
        Op.api("order-query-service", "order-query-service", "POST", "/lab/projection/rebuild")
            .group("Experiments")
            .title("Rebuild the projection")
            .summary(
                "Stops the order-projection consumer, rewinds its committed offsets to the start, truncates the read models and that consumer's idempotency rows, then starts the consumer. Offsets move before any delete.")
            .response(
                200,
                "ProjectionRebuild.Started.",
                schema(
                    "Started",
                    "order-query-service/src/main/java/io/zeroshift/query/ProjectionRebuild.java",
                    null,
                    field("at", "datetime", true, ""),
                    field(
                        "resetTo",
                        "object",
                        true,
                        "Partition to the offset the group was moved to.")))
            .source(query)
            .done("query-post-rebuild"));

    ops.add(
        Op.api("inventory-service", "inventory-service", "GET", "/stock")
            .group("Inventory")
            .title("Stock levels")
            .summary(
                "On-hand, reserved and available units per SKU. available is onHand minus reserved.")
            .response(
                200,
                "ApiResponse of PostgresStock.Level.",
                schema(
                    "Level",
                    "inventory-service/src/main/java/io/zeroshift/inventory/infrastructure/PostgresStock.java",
                    null,
                    field("sku", "string", true, ""),
                    field("name", "string", true, ""),
                    field("onHand", "integer", true, ""),
                    field("reserved", "integer", true, ""),
                    field("available", "integer", true, ""),
                    field("version", "integer", true, "Optimistic version of the stock row.")))
            .idempotency("Safe read.")
            .source(
                "inventory-service/src/main/java/io/zeroshift/inventory/infrastructure/StockController.java")
            .done("inventory-get-stock"));

    var pay =
        "payment-service/src/main/java/io/zeroshift/payment/infrastructure/GatewayControl.java";
    ops.add(
        Op.api("payment-service", "payment-service", "GET", "/lab/gateway")
            .group("Payments")
            .title("Payment gateway state")
            .summary("The simulated gateway's mode and the live Resilience4j circuit breaker.")
            .response(
                200,
                "GatewayControl.State.",
                schema(
                    "State",
                    pay,
                    null,
                    field("mode", "string", true, "healthy, slow, down or declining."),
                    field("breakerState", "string", true, "Resilience4j state name."),
                    field("failureRate", "number", true, ""),
                    field("bufferedCalls", "integer", true, ""),
                    field("failedCalls", "integer", true, ""),
                    field("notPermittedCalls", "long", true, ""),
                    field(
                        "policy",
                        "Settings",
                        true,
                        "timeoutMs, retry, maxAttempts, breaker, pauseOnOpen: the live call policy."),
                    field(
                        "consumerPauses",
                        "long",
                        true,
                        "How often an open breaker paused the payment consumer."),
                    field("recentCalls", "Call[]", true, "The latest 30 gateway calls.")))
            .idempotency("Safe read.")
            .source(pay)
            .done("payment-get-gateway"));
    ops.add(
        Op.api("payment-service", "payment-service", "GET", "/lab/gateway/calls")
            .group("Payments")
            .title("Gateway calls")
            .summary("Recorded calls to the simulated card gateway, newest first.")
            .query("orderId", "uuid", false, null, "Only this order's calls when set.")
            .query("limit", "integer", false, "100", "1–500.")
            .response(
                200,
                "ApiResponse of GatewayCalls.Call. This list uses ApiResponse.list, so hasMore stays null.",
                schema(
                    "Call",
                    "payment-service/src/main/java/io/zeroshift/payment/infrastructure/GatewayCalls.java",
                    null,
                    field("id", "long", true, ""),
                    field("orderId", "uuid", true, ""),
                    field("outcome", "string", true, ""),
                    field("latencyMs", "long", true, ""),
                    field("breakerState", "string", true, ""),
                    field("detail", "string", false, ""),
                    field("at", "datetime", true, "")))
            .idempotency("Safe read.")
            .source(pay)
            .done("payment-get-calls"));
    ops.add(
        Op.api("payment-service", "payment-service", "POST", "/lab/gateway/{mode}")
            .group("Payments")
            .title("Set gateway mode")
            .summary("Points the WireMock gateway at healthy, slow, down or declining.")
            .path("mode", "string", "healthy, slow, down or declining. Case-insensitive.")
            .response(200, "The same State document as GET /lab/gateway.", null)
            .error(400, "UNKNOWN_GATEWAY_MODE", "mode is not one of the four.")
            .source(pay)
            .done("payment-post-mode"));
    ops.add(
        Op.api("payment-service", "payment-service", "POST", "/lab/gateway/breaker/reset")
            .group("Payments")
            .title("Reset the circuit breaker")
            .summary("Closes the payment-gateway breaker immediately.")
            .response(200, "GatewayControl.State after the reset.", null)
            .source(pay)
            .done("payment-post-breaker-reset"));

    var ship = "shipping-service/src/main/java/io/zeroshift/shipping/CarrierLab.java";
    ops.add(
        Op.api("shipping-service", "shipping-service", "POST", "/lab/carrier/scans")
            .group("Shipping")
            .title("Publish carrier scans")
            .summary(
                "Publishes scans fromSeq through toSeq for the newest shipped parcels. fromSeq 1 starts a fresh trip and forgets those parcels' tracking first.")
            .query("keying", "string", false, "tracking", "tracking, scan or hub.")
            .query("parcels", "integer", false, "4", "1–10.")
            .query("fromSeq", "integer", false, "1", "1–4.")
            .query("toSeq", "integer", false, "4", "1–4.")
            .response(
                200,
                "Carrier.ScanRun.",
                schema(
                    "ScanRun",
                    "shipping-service/src/main/java/io/zeroshift/shipping/Carrier.java",
                    null,
                    field("keying", "enum", true, "TRACKING, SCAN or HUB."),
                    field(
                        "produced",
                        "Produced[]",
                        true,
                        "trackingNumber, seq, status, key, partition, offset."),
                    field("partitions", "integer", true, "")))
            .error(400, "UNKNOWN_KEYING", "keying is not tracking, scan or hub.")
            .error(409, "NO_SHIPPED_PARCELS", "There is no shipped parcel to scan.")
            .events("ParcelScanned")
            .source(ship)
            .done("shipping-post-scans"));
    ops.add(
        Op.api("shipping-service", "shipping-service", "POST", "/lab/carrier/partitions")
            .group("Shipping")
            .title("Add scan partitions")
            .summary(
                "Kafka can only add partitions. Existing keys may then hash to a different partition.")
            .query("count", "integer", true, null, "1–24. The new total.")
            .response(
                200,
                "Carrier.Partitions.",
                schema(
                    "Partitions",
                    "shipping-service/src/main/java/io/zeroshift/shipping/Carrier.java",
                    null,
                    field("partitions", "integer", true, ""),
                    field("was", "integer", false, "The previous count.")))
            .error(
                400,
                "PARTITIONS_CAN_ONLY_GROW",
                "count is not greater than the current partition count.")
            .source(ship)
            .done("shipping-post-partitions"));
    ops.add(
        Op.api("shipping-service", "shipping-service", "POST", "/lab/carrier/reset")
            .group("Shipping")
            .title("Recreate the scan topic")
            .summary(
                "Recovery from repartitioning. Recreates shipping.carrier-scans with 3 partitions.")
            .response(200, "Carrier.Partitions after the recreate.", null)
            .source(ship)
            .done("shipping-post-reset"));
    ops.add(
        Op.api("shipping-service", "shipping-service", "POST", "/lab/tracking/replay")
            .group("Shipping")
            .title("Replay tracking")
            .summary("Replays every scan still on the topic into an empty tracking projection.")
            .response(
                200,
                "Replayed.",
                schema("Replayed", ship, null, field("replayedPartitions", "integer", true, "")))
            .source(ship)
            .done("shipping-post-replay"));
    ops.add(
        Op.api("shipping-service", "shipping-service", "PUT", "/lab/tracking/guard")
            .group("Shipping")
            .title("Sequence guard")
            .summary(
                "Turns the tracking sequence guard on or off. The guard drops a scan older than the one already applied.")
            .query("on", "boolean", true, null, "true enables the guard.")
            .response(
                200, "Guard.", schema("Guard", ship, null, field("guard", "boolean", true, "")))
            .source(ship)
            .done("shipping-put-guard"));
    ops.add(
        Op.api("shipping-service", "shipping-service", "GET", "/lab/tracking")
            .group("Shipping")
            .title("Tracking projection")
            .summary(
                "The guard flag, the latest parcels and scans, the topic's partition count, and any slow-shard fault.")
            .response(
                200,
                "TrackingView.",
                schema(
                    "TrackingView",
                    ship,
                    null,
                    field("guard", "boolean", true, ""),
                    field(
                        "parcels",
                        "Parcel[]",
                        true,
                        "Up to 12. trackingNumber, orderId, status, lastSeq, scansApplied, regressions, staleSkipped, updatedAt."),
                    field(
                        "scans",
                        "Scan[]",
                        true,
                        "Up to 80. id, trackingNumber, seq, status, recordKey, partition, offset, outcome, at."),
                    field("partitions", "integer", true, ""),
                    field("slow", "string", false, "The carrier-slow fault, when armed.")))
            .idempotency("Safe read.")
            .source(ship)
            .done("shipping-get-tracking"));
  }

  private static void platform(List<Operation> ops) {
    var src = "platform/src/main/java/io/zeroshift/platform/LabAdminController.java";
    var mounted =
        "Mounted on every service that imports PlatformConfiguration: order, payment, inventory, shipping and order-query. Try it chooses which one to call.";
    ops.add(
        Op.api("platform", "platform", "GET", "/lab/state")
            .group("Experiments")
            .title("Service lab state")
            .summary("Consumers, armed faults and outbox slot status for one service. " + mounted)
            .response(
                200,
                "LabAdminController.State.",
                schema(
                    "State",
                    src,
                    null,
                    field("service", "string", true, "spring.application.name."),
                    field(
                        "consumers",
                        "Consumer[]",
                        true,
                        "id, groupId, topics, running, pauseRequested, paused, assignedPartitions, configOverrides."),
                    field("faults", "Fault[]", true, "name, mode, remaining, armedAt."),
                    field(
                        "outbox",
                        "Status",
                        true,
                        "slot, slotActive, lagBytes, rows, lastInsertAt. lagBytes is WAL the connector has not confirmed.")))
            .idempotency("Safe read.")
            .source(src)
            .done("platform-get-state"));
    ops.add(
        Op.api("platform", "platform", "GET", "/lab/decisions")
            .group("Experiments")
            .title("Consumer decisions")
            .summary("The decision log, newest first. " + mounted)
            .query("orderId", "string", false, null, "Only this order when set.")
            .query("limit", "integer", false, "100", "1–500. One extra row sets meta.hasMore.")
            .response(200, "ApiResponse of DecisionLog.Recorded.", listMeta())
            .idempotency("Safe read.")
            .source(src)
            .done("platform-get-decisions"));
    ops.add(
        Op.api("platform", "platform", "GET", "/lab/outbox")
            .group("Experiments")
            .title("Outbox rows")
            .summary("Newest outbox rows. payload is the envelope JSON. " + mounted)
            .query("orderId", "string", false, null, "Filters on aggregate id.")
            .query("limit", "integer", false, "100", "1–500.")
            .response(200, "ApiResponse of Outbox.Row.", listMeta())
            .idempotency("Safe read.")
            .source(src)
            .done("platform-get-outbox"));
    ops.add(
        Op.api("platform", "platform", "POST", "/lab/consumers/{id}/{action}")
            .group("Experiments")
            .title("Pause or stop a consumer")
            .summary(
                "pause and resume keep the member in the group. stop leaves the group; start joins it again. "
                    + mounted)
            .path("id", "string", "Listener id, which is also the consumer group id.")
            .path("action", "string", "pause, resume, stop or start.")
            .response(200, "LabAdminController.State after the action.", null)
            .error(404, "UNKNOWN_CONSUMER", "No listener with this id.")
            .error(400, "UNKNOWN_CONSUMER_ACTION", "action is not pause, resume, stop or start.")
            .source(src)
            .done("platform-post-consumer"));
    ops.add(
        Op.api("platform", "platform", "PUT", "/lab/faults/{name}")
            .group("Experiments")
            .title("Arm a fault")
            .summary("Without times the fault stays armed until it is cleared. " + mounted)
            .path("name", "string", "At most 60 characters.")
            .query("mode", "string", true, null, "1–100 characters. The fault's own mode string.")
            .query("times", "integer", false, null, "1–1000. Omit it to keep the fault armed.")
            .response(200, "LabAdminController.State.", null)
            .error(400, "VALIDATION_FAILED", "name, mode or times is out of bounds.")
            .source(src)
            .done("platform-put-fault"));
    ops.add(
        Op.api("platform", "platform", "DELETE", "/lab/faults/{name}")
            .group("Experiments")
            .title("Clear a fault")
            .summary("Disarms one fault. " + mounted)
            .path("name", "string", "Fault name.")
            .response(200, "LabAdminController.State.", null)
            .source(src)
            .done("platform-delete-fault"));
    ops.add(
        Op.api("platform", "platform", "POST", "/lab/crash")
            .group("Experiments")
            .title("Crash the process")
            .summary(
                "Returns 202, then the JVM exits. The caller sees the request accepted and then the process vanish. "
                    + mounted)
            .response(202, "Empty body. The process exits about 200 ms later.", null)
            .source(src)
            .done("platform-post-crash"));
  }

  private static void migration(List<Operation> ops) {
    ops.add(
        Op.view("migration-lab", "GET", "/")
            .group("Labs")
            .title("Migration dashboard")
            .summary("Thymeleaf page for the SQL Server to PostgreSQL migration.")
            .source("migration-lab/src/main/java/io/zeroshift/web/DashboardController.java")
            .done("lab-get-home"));
    ops.add(
        Op.view("migration-lab", "GET", "/events")
            .group("Labs")
            .title("Event lab")
            .summary("Thymeleaf page for the event-driven control plane.")
            .source("migration-lab/src/main/java/io/zeroshift/eventlab/EventLabController.java")
            .done("events-get-page"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/status")
            .group("Migration")
            .title("Migration status")
            .summary(
                "The dashboard's document: migration state, counts, traffic, completion, rollback and logs. Every number is read from the databases.")
            .response(
                200,
                "Dashboard.",
                schema(
                    "Dashboard",
                    "migration-lab/src/main/java/io/zeroshift/web/DashboardController.java",
                    null,
                    field(
                        "migration",
                        "MigrationState",
                        true,
                        "stage, status, primary, table, checkpoints, counters, validation, timestamps."),
                    field("progress", "number", true, "Stage-weighted, not an ETA."),
                    field("source", "Counts", true, "customers and orders on the source."),
                    field("target", "Counts", true, "customers and orders on PostgreSQL."),
                    field("pending", "long", false, "Change-tracking rows not yet applied."),
                    field("logs", "string[]", true, ""),
                    field("traffic", "TrafficMetrics", true, ""),
                    field("captureError", "string", true, ""),
                    field("seedRows", "integer", true, ""),
                    field(
                        "migrationStartAllowed",
                        "boolean",
                        true,
                        "True only while idle with a non-empty source."),
                    field("completion", "Completion", true, ""),
                    field("rollback", "Rollback", true, ""),
                    field("events", "LogEvent[]", true, "")))
            .idempotency("Safe read.")
            .source("migration-lab/src/main/java/io/zeroshift/web/DashboardController.java")
            .done("lab-get-status"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/actions/{action}")
            .group("Migration")
            .title("Migration action")
            .summary(
                "seed, traffic-start, traffic-stop, start, pause, resume, crash, validate, cutover, rollback, rollback-abort, reset. seed reads the optional rows query parameter.")
            .path("action", "string", "One of the action names above.")
            .query(
                "rows",
                "integer",
                false,
                null,
                "Used by seed. The dashboard's rows field is 1–10,000,000.")
            .response(
                200,
                "ActionResult.",
                schema(
                    "ActionResult",
                    "migration-lab/src/main/java/io/zeroshift/web/DashboardController.java",
                    """
                    { "message": "Action accepted: pause" }
                    """,
                    field("message", "string", true, "")))
            .error(
                409,
                "INVALID_ACTION",
                "Unknown action, or the migration's current stage refuses it.")
            .source("migration-lab/src/main/java/io/zeroshift/web/DashboardController.java")
            .done("lab-post-action"));

    var live = "migration-lab/src/main/java/io/zeroshift/web/LiveChangesController.java";
    var edit =
        schema(
            "OrderEdit",
            "migration-lab/src/main/java/io/zeroshift/domain/OrderEdit.java",
            """
            { "customerName": "Ada Lovelace", "amount": 12.5000, "status": "PAID" }
            """,
            field("customerName", "string", true, "1–180 characters."),
            field(
                "amount",
                "decimal",
                true,
                "Non-negative, at most 15 integer digits and 4 decimal digits."),
            field(
                "status",
                "string",
                true,
                "1–24 letters, numbers, spaces, underscores or hyphens."));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/live/orders/{id}")
            .group("Migration")
            .title("Inspect a live order")
            .summary(
                "Compares one order on SQL Server and PostgreSQL, including pending change-tracking rows.")
            .path("id", "long", "Source order id.")
            .response(200, "LiveChangesService.Inspection.", inspection())
            .idempotency("Safe read.")
            .source(live)
            .done("live-get-order"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/live/copied-order")
            .group("Migration")
            .title("Pick a copied order")
            .summary("Selects an order that has already been copied, for the record inspector.")
            .response(200, "LiveChangesService.Inspection.", inspection())
            .idempotency("Safe read.")
            .source(live)
            .done("live-get-copied"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/live/orders")
            .group("Migration")
            .title("Insert a source order")
            .summary(
                "Inserts a real order into SQL Server while it is still primary, so change tracking can capture it.")
            .body(edit)
            .response(200, "LiveExperiment.", experiment())
            .error(
                409,
                "INVALID_ACTION",
                "The edit breaks OrderEdit's rules, or the source is not writable.")
            .source(live)
            .done("live-post-order"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "PUT", "/api/live/orders/{id}")
            .group("Migration")
            .title("Update a source order")
            .path("id", "long", "Source order id.")
            .body(edit)
            .response(200, "LiveExperiment.", experiment())
            .error(409, "INVALID_ACTION", "The edit is invalid or the source is not writable.")
            .source(live)
            .done("live-put-order"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "DELETE", "/api/live/orders/{id}")
            .group("Migration")
            .title("Delete a source order")
            .path("id", "long", "Source order id.")
            .response(200, "LiveExperiment.", experiment())
            .error(409, "INVALID_ACTION", "The source is not writable.")
            .source(live)
            .done("live-delete-order"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/live/cdc/pause")
            .group("Migration")
            .title("Pause CDC replay")
            .summary(
                "Stops applying captured changes. The snapshot and source writes continue, so the databases drift apart on purpose.")
            .response(
                200,
                "ActionResult.",
                actionMessage("CDC replay paused; snapshot and source writes continue"))
            .source(live)
            .done("live-post-pause"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/live/cdc/resume")
            .group("Migration")
            .title("Resume CDC replay")
            .response(200, "ActionResult.", actionMessage("CDC replay resumed"))
            .source(live)
            .done("live-post-resume"));

    var events = "migration-lab/src/main/java/io/zeroshift/eventlab/EventLabController.java";
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/events/state")
            .group("Event lab")
            .title("Event lab overview")
            .summary(
                "Aggregates topics, groups, connectors, the lease and recent orders from the live services and Kafka. A down service is reported, not invented.")
            .response(200, "A JSON object assembled by LabOverview.", null)
            .idempotency("Safe read.")
            .source(events)
            .done("events-get-state"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/events/labs")
            .group("Event lab")
            .title("Event lab experiments")
            .summary("Experiment state plus the tracking projection, as the /events page polls it.")
            .response(200, "A JSON object.", null)
            .idempotency("Safe read.")
            .source(events)
            .done("events-get-labs"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/events/orders/{id}")
            .group("Event lab")
            .title("Order journey")
            .summary(
                "One order from the write model through the outbox, the tap and each service's decisions.")
            .path("id", "uuid", "Order id.")
            .response(200, "Journey JSON.", null)
            .idempotency("Safe read.")
            .source(events)
            .done("events-get-journey"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/events/orders/{id}/fold")
            .group("Event lab")
            .title("Proxy the event fold")
            .summary("Forwards to order-service GET /orders/{id}/fold.")
            .path("id", "uuid", "Order id.")
            .response(200, "The order service's Step array.", null)
            .error(502, "BAD_GATEWAY", "order-service did not answer.")
            .idempotency("Safe read.")
            .source(events)
            .done("events-get-fold"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/events/actions/{action}")
            .group("Event lab")
            .title("Event lab action")
            .summary(
                "Proxies a control-plane action. Names: place-order, dual-write, consumer-pause, consumer-resume, consumer-seek, crash, exp-idempotency, exp-ryw, refund, carrier-scans, carrier-partitions, carrier-reset, tracking-replay, tracking-guard, fault-arm, fault-clear, gateway, breaker-reset, connector-pause, connector-resume, rebuild-projection, discard-snapshot, duplicate, redrive, poison, retention-demo.")
            .path("action", "string", "One name from LabActions.")
            .body(
                schema(
                    null,
                    null,
                    "{ }",
                    field(
                        "body",
                        "object",
                        false,
                        "Action-specific JSON. place-order reads an order object.")))
            .response(200, "Whatever the underlying service returned, passed through.", null)
            .error(409, "INVALID_ACTION", "Unknown action.")
            .error(502, "BAD_GATEWAY", "The target service refused or was down.")
            .source(events)
            .done("events-post-action"));

    var kafka = "migration-lab/src/main/java/io/zeroshift/kafkalab/KafkaLabController.java";
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/kafka-lab/state")
            .group("Kafka lab")
            .title("Kafka lab state")
            .summary(
                "Cluster snapshot, traffic and the recorded runs. Requires the kafka-lab Compose profile; otherwise the lab reports how to start it.")
            .response(
                200,
                "KafkaLabController.State: cluster, traffic, runs.",
                schema(
                    "State",
                    kafka,
                    null,
                    field(
                        "cluster",
                        "Snapshot",
                        true,
                        "Brokers, topics and groups as the admin client sees them."),
                    field("traffic", "View", true, ""),
                    field("runs", "Runs", true, "failover, acks, retries, unclean, delivery.")))
            .idempotency("Safe read.")
            .error(503, "KAFKA_ERROR", "The lab cluster did not answer.")
            .source(kafka)
            .done("kafka-get-state"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/kafka-lab/nodes/{id}/{action}")
            .group("Kafka lab")
            .title("Fail or start a broker")
            .path("id", "integer", "Node 1, 2 or 3.")
            .path("action", "string", "kill, stop, start, pause or unpause.")
            .response(200, "The cluster snapshot after the action.", null)
            .error(404, "UNKNOWN_NODE", "No node with this id.")
            .error(400, "UNKNOWN_NODE_ACTION", "The action name is not one LabNodes understands.")
            .error(409, "NODE_ACTION_REFUSED", "The node refused the action in its current state.")
            .source(kafka)
            .done("kafka-post-node"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/kafka-lab/nodes/{id}/truncations")
            .group("Kafka lab")
            .title("Log truncations")
            .summary(
                "Lines from a broker's log that show a truncation, for the unclean-election lesson.")
            .path("id", "integer", "Node id.")
            .query(
                "partition",
                "string",
                true,
                null,
                "Matches lab.<name>-<n>, the log file the scenario names.")
            .query(
                "since",
                "datetime",
                false,
                null,
                "Only lines after this instant. Defaults to 30 minutes ago.")
            .response(200, "LogLines: node, partition, lines.", null)
            .idempotency("Safe read.")
            .source(kafka)
            .done("kafka-get-truncations"));
    kafkaPost(
        ops, kafka, "/api/kafka-lab/setup", "kafka-post-setup", "Create the standing lab topics.");
    kafkaPost(
        ops, kafka, "/api/kafka-lab/recover", "kafka-post-recover", "Bring the lab cluster back.");
    kafkaPost(
        ops,
        kafka,
        "/api/kafka-lab/reset",
        "kafka-post-reset",
        "Reset the lab cluster's topics and runs.");
    kafkaPost(
        ops,
        kafka,
        "/api/kafka-lab/elections/preferred",
        "kafka-post-election",
        "Ask for preferred leader election.");
    kafkaPost(
        ops,
        kafka,
        "/api/kafka-lab/traffic/start",
        "kafka-post-traffic-start",
        "Start continuous traffic on lab.replicated. 409 TRAFFIC_RUNNING when it is already running.");
    kafkaPost(
        ops,
        kafka,
        "/api/kafka-lab/traffic/stop",
        "kafka-post-traffic-stop",
        "Stop that traffic. 409 TRAFFIC_NOT_RUNNING when it is already stopped.");
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/kafka-lab/probe")
            .group("Kafka lab")
            .title("Durability probe")
            .summary("One write that probes acks against lab.durability or lab.replicated.")
            .query("topic", "string", false, "lab.durability", "lab.durability or lab.replicated.")
            .query("acks", "string", false, "all", "0, 1 or all.")
            .response(200, "DurabilityLab.Probe: the offset, or the broker's refusal.", null)
            .error(400, "NOT_A_PROBE_TOPIC", "The topic is not lab.durability or lab.replicated.")
            .error(400, "UNKNOWN_ACKS", "acks is not a value the probe accepts.")
            .source(kafka)
            .done("kafka-post-probe"));
    ops.add(
        Op.api(
                "migration-lab",
                "migration-lab",
                "PUT",
                "/api/kafka-lab/topics/{topic}/min-insync-replicas")
            .group("Kafka lab")
            .title("Set min.insync.replicas")
            .path("topic", "string", "A lab.* topic.")
            .query("value", "integer", true, null, "1–3.")
            .response(200, "The topic config after the change.", null)
            .error(400, "NOT_A_LAB_TOPIC", "Only lab.* topics can be changed here.")
            .source(kafka)
            .done("kafka-put-min-isr"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/kafka-lab/scenarios/acks")
            .group("Kafka lab")
            .title("Acks scenario")
            .summary(
                "acks=1 versus acks=all against a crashing leader. Not executable from Try it.")
            .query("acks", "string", true, null, "1 or all.")
            .response(200, "KafkaLabRuns.Run.", null)
            .error(400, "UNKNOWN_ACKS", "acks is not 1 or all.")
            .error(503, "KAFKA_ERROR", "A broker timed out or refused.")
            .error(409, "SCENARIO_FAILED", "A step did not happen on the cluster.")
            .source(kafka)
            .done("kafka-post-acks"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/kafka-lab/scenarios/retries")
            .group("Kafka lab")
            .title("Retries scenario")
            .summary("Producer retries with or without idempotence. Not executable from Try it.")
            .query("idempotent", "boolean", true, null, "Whether the producer is idempotent.")
            .response(200, "KafkaLabRuns.Run.", null)
            .error(503, "KAFKA_ERROR", "A broker timed out or refused.")
            .error(409, "SCENARIO_FAILED", "A step did not happen on the cluster.")
            .source(kafka)
            .done("kafka-post-retries"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/kafka-lab/scenarios/unclean/{phase}")
            .group("Kafka lab")
            .title("Unclean election phase")
            .path("phase", "string", "break, elect or check.")
            .response(200, "The phase result.", null)
            .error(404, "UNKNOWN_PHASE", "phase is not break, elect or check.")
            .error(
                409,
                "PARTITION_OFFLINE",
                "The scenario is waiting on a partition that is still offline.")
            .error(409, "NOT_BROKEN_YET", "elect or check ran before break.")
            .source(kafka)
            .done("kafka-post-unclean"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/kafka-lab/delivery")
            .group("Kafka lab")
            .title("Delivery semantics run")
            .summary(
                "Starts the consume-transform-produce workers on lab.delivery.in and lab.delivery.out.")
            .query("mode", "string", true, null, "at-most-once, at-least-once or exactly-once.")
            .query("crash", "boolean", false, "false", "Crash a worker mid-run when true.")
            .response(200, "KafkaLabRuns.Run.", null)
            .error(400, "UNKNOWN_DELIVERY_MODE", "The requested mode is not one DeliveryLab knows.")
            .error(409, "SCENARIO_RUNNING", "A delivery run is already in progress.")
            .error(504, "WORKER_TIMED_OUT", "A worker did not finish within 90 seconds.")
            .source(kafka)
            .done("kafka-post-delivery"));
  }

  private static void kafkaPost(
      List<Operation> ops, String source, String path, String id, String summary) {
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", path)
            .group("Kafka lab")
            .title(summary)
            .summary(summary + " Acts on the real lab cluster. Not executable from Try it.")
            .response(200, "The lab's result for this lever. Shape depends on the action.", null)
            .error(503, "KAFKA_ERROR", "A broker timed out or refused.")
            .error(409, "SCENARIO_FAILED", "A step did not happen on the cluster.")
            .source(source)
            .done(id));
  }

  private static Schema placeOrder() {
    return schema(
        "PlaceOrderRequest",
        "order-service/src/main/java/io/zeroshift/order/web/PlaceOrderRequest.java",
        """
        { "customerId": "ada", "items": [ { "sku": "SKU-CABLE", "quantity": 1 } ] }
        """,
        field("customerId", "string", true, "1–100 characters."),
        field("items", "Line[]", true, "sku and quantity."));
  }

  private static Schema listMeta() {
    return schema(
        "ApiResponse",
        "platform-web/src/main/java/io/zeroshift/platform/web/ApiResponse.java",
        """
        { "data": [], "meta": { "count": 0, "limit": 20, "hasMore": false } }
        """,
        field("data", "array", true, "The items, already trimmed to limit."),
        field("meta.count", "integer", true, "How many items are in data."),
        field("meta.limit", "integer", false, "The limit asked for. Null on a complete list."),
        field(
            "meta.hasMore",
            "boolean",
            false,
            "True when a further row existed. Null on a complete list."));
  }

  private static Schema inspection() {
    return schema(
        "Inspection",
        "migration-lab/src/main/java/io/zeroshift/application/LiveChangesService.java",
        null,
        field("orderId", "long", true, ""),
        field("source", "OrderRecord", false, "id, customerId, customerName, amount, status."),
        field("target", "OrderRecord", false, "The PostgreSQL copy, when present."),
        field("experiment", "LiveExperiment", false, ""),
        field("changes", "CaptureInfo[]", true, "table, recordId, version, operation, pending."),
        field("inSync", "boolean", true, ""),
        field("cdcPaused", "boolean", true, ""),
        field("ordersCopiedThrough", "long", true, ""),
        field("message", "string", true, ""));
  }

  private static Schema experiment() {
    return schema(
        "LiveExperiment",
        "migration-lab/src/main/java/io/zeroshift/domain/LiveExperiment.java",
        null,
        field("id", "long", true, ""),
        field("orderId", "long", true, ""),
        field("operation", "enum", true, "The traffic operation that was applied."),
        field("before", "OrderRecord", false, ""),
        field("after", "OrderRecord", false, ""));
  }

  private static Schema actionMessage(String example) {
    return schema(
        "ActionResult",
        "migration-lab/src/main/java/io/zeroshift/web/DashboardController.java",
        "{ \"message\": \"" + example + "\" }",
        field("message", "string", true, ""));
  }
}
