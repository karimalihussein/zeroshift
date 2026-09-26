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
                "Places an order for a known customer. The service prices each item from the inventory catalog (GET /products?sku=…, a synchronous call), applies and redeems the voucher, and issues the invoice. The order, its invoice, the voucher's use and its OrderPlaced event commit in one database transaction; Debezium publishes the outbox row. The response version is the read-your-writes token for the query service.")
            .header(
                "Idempotency-Key",
                false,
                "Request header, optional, 1–200 characters. The same key and the same body return the first response. The same key with a different body is IDEMPOTENCY_KEY_REUSED.")
            .header(
                "Idempotent-Replayed",
                false,
                "Response header. true when this answer was stored for an earlier Idempotency-Key.")
            .header(
                "Retry-After",
                false,
                "Response header on 503 CATALOG_UNAVAILABLE (and the edge guards' 429 and 503). Seconds to wait before retrying.")
            .body(placeOrder())
            .response(
                202,
                "Accepted. The body is the OrderAccepted record, not an envelope. Every amount is in currency with 2 decimals; total = subtotal − discount + tax.",
                schema(
                    "OrderAccepted",
                    "order-service/src/main/java/io/zeroshift/order/web/OrderViews.java",
                    """
                    {
                      "orderId": "3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44",
                      "correlationId": "8a2e1c44-1b6f-4d0a-9e77-2c5b8f0d11aa",
                      "eventId": "11111111-1111-4111-8111-111111111111",
                      "invoiceNumber": "INV-2026-000042",
                      "currency": "USD",
                      "subtotal": 25.98,
                      "discount": 2.60,
                      "tax": 1.87,
                      "total": 25.25,
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
                        "invoiceNumber",
                        "string",
                        true,
                        "The invoice issued with the order: INV-{year}-{6 digits}."),
                    field(
                        "currency",
                        "string",
                        true,
                        "ISO 4217 code, commerce.currency (default USD)."),
                    field(
                        "subtotal",
                        "decimal",
                        true,
                        "Sum of item subtotals (catalog unit price × quantity), scale 2."),
                    field(
                        "discount",
                        "decimal",
                        true,
                        "The voucher's discount, 0.00 without one. Never above subtotal."),
                    field(
                        "tax",
                        "decimal",
                        true,
                        "(subtotal − discount) × the tax rate, rounded HALF_EVEN to 2 decimals."),
                    field("total", "decimal", true, "subtotal − discount + tax."),
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
            .error(
                400,
                "VALIDATION_FAILED",
                "customerId is missing, items is empty or has more than 20 lines, a line fails its constraint, or voucherCode is longer than 40.")
            .error(400, "MALFORMED_REQUEST", "The body is not JSON the record can bind.")
            .error(
                422,
                "ORDER_RULE_VIOLATION",
                "A SKU the catalog does not sell, an inactive product, a product priced in another currency, or another order rule.")
            .error(422, "UNKNOWN_CUSTOMER", "No customer with this customerId.")
            .error(
                422,
                "VOUCHER_NOT_APPLICABLE",
                "No voucher with this code, or it is inactive, outside its validity or below its minimum amount.")
            .error(
                409,
                "VOUCHER_EXHAUSTED",
                "The voucher's usage limit is used up, possibly by a concurrent order.")
            .error(
                503,
                "CATALOG_UNAVAILABLE",
                "inventory-service did not answer the price lookup. Retry-After says when to retry.")
            .error(
                422,
                "IDEMPOTENCY_KEY_REUSED",
                "This Idempotency-Key was already used for a different body.")
            .error(
                429,
                "RATE_LIMITED",
                "Edge rate limit of this replica, when switched on. Retry-After says when to retry.")
            .error(
                503,
                "LOAD_SHED",
                "Too many unfinished orders, when load shedding is on. Retry-After says when to retry.")
            .error(
                503,
                "BULKHEAD_FULL",
                "Too many concurrent placements, when the bulkhead is on. Retry-After says when to retry.")
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
                "The anti-pattern the outbox replaces. The order is priced like POST /orders, then the database and Kafka are written separately, and the call crashes or rolls back. It starts no saga.")
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
                "Newest sagas, each with the order folded from its event stream. A customer filter searches the newest 100 sagas, then keeps that customer. OrderSummary is saga plus order; order has the fields listed under Get order.")
            .query(
                "limit",
                "integer",
                false,
                "20",
                "1–100. The service reads one extra row and sets meta.hasMore.")
            .query("customerId", "uuid", false, null, "Optional. Only this customer's orders.")
            .response(
                200,
                "ApiResponse of OrderSummary. data is the page; meta carries count, limit and hasMore.",
                listMeta())
            .error(400, "VALIDATION_FAILED", "limit is outside 1–100.")
            .error(400, "INVALID_PARAMETER", "customerId is not a UUID.")
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
                "OrderDetail: order, invoice, rebuiltFrom, snapshotVersion, events, saga, transitions.",
                schema(
                    "OrderDetail",
                    "order-service/src/main/java/io/zeroshift/order/web/OrderViews.java",
                    null,
                    field(
                        "order",
                        "Order",
                        true,
                        "id, customerId, customerName, lines, currency, subtotal, discount, taxRate, tax, total, voucherCode, invoiceNumber, status, paymentId, reservationId, trackingNumber, cancelReason, version. Each line is productId, sku, name, quantity, unitPrice, subtotal, discount, total, snapshotted when the order was placed."),
                    field(
                        "invoice",
                        "Invoice",
                        false,
                        "id, number, status (ISSUED, PAID or VOIDED), currency, subtotal, discount, tax, total, issuedAt, paidAt, voidedAt. Paid on PaymentAuthorized, voided on cancellation. Null for an order written around the normal path."),
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
    var commerce = "order-service/src/main/java/io/zeroshift/order/web/CommerceController.java";
    ops.add(
        Op.api("order-service", "order-service", "GET", "/customers")
            .group("Customers")
            .title("List customers")
            .summary(
                "The customers orders can be placed for, by name. Seeded by order-service when commerce.demo-data is on.")
            .query(
                "limit",
                "integer",
                false,
                "50",
                "1–500. The service reads one extra row and sets meta.hasMore.")
            .response(200, "ApiResponse of Customer.", customer())
            .error(400, "VALIDATION_FAILED", "limit is outside 1–500.")
            .idempotency("Safe read.")
            .source(commerce)
            .done("order-get-customers"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/customers/{id}")
            .group("Customers")
            .title("Get customer")
            .summary(
                "One customer. An order keeps the name it was placed under; a later edit here never rewrites it.")
            .path("id", "uuid", "Customer id.")
            .response(200, "Customer.", customer())
            .error(404, "CUSTOMER_NOT_FOUND", "No customer with this id.")
            .error(400, "INVALID_PARAMETER", "id is not a UUID.")
            .idempotency("Safe read.")
            .source(commerce)
            .done("order-get-customer"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/vouchers")
            .group("Customers")
            .title("List vouchers")
            .summary(
                "Every voucher, usable or not. usageCount against usageLimit shows what is left; a cancelled order gives its use back.")
            .response(
                200,
                "ApiResponse of Voucher. A complete list, so meta.hasMore is null.",
                schema(
                    "Voucher",
                    "order-service/src/main/java/io/zeroshift/order/domain/Voucher.java",
                    """
                    { "data": [ { "id": "0c6f2a1e-8b3d-4f5a-9e7c-2d1b0a9f8e7d", "code": "WELCOME10", "discountType": "PERCENTAGE", "value": 10.00, "minimumAmount": 0.00, "maximumDiscount": null, "usageLimit": null, "usageCount": 3, "validFrom": "2026-08-27T00:00:00Z", "validUntil": null, "active": true } ], "meta": { "count": 1, "limit": null, "hasMore": null } }
                    """,
                    field("id", "uuid", true, ""),
                    field("code", "string", true, "Stored upper case; accepted in any case."),
                    field("discountType", "enum", true, "FIXED or PERCENTAGE."),
                    field(
                        "value",
                        "decimal",
                        true,
                        "An amount for FIXED, a percentage (at most 100) for PERCENTAGE."),
                    field(
                        "minimumAmount",
                        "decimal",
                        true,
                        "The order subtotal it needs. Below it the voucher is VOUCHER_NOT_APPLICABLE."),
                    field("maximumDiscount", "decimal", false, "A cap on the discount, when set."),
                    field("usageLimit", "integer", false, "Null means unlimited."),
                    field(
                        "usageCount",
                        "integer",
                        true,
                        "Placed orders holding a use. At the limit, placing is VOUCHER_EXHAUSTED."),
                    field("validFrom", "datetime", true, ""),
                    field("validUntil", "datetime", false, "Exclusive. Null means no end."),
                    field("active", "boolean", true, "An inactive voucher is not applicable.")))
            .idempotency("Safe read.")
            .source(commerce)
            .done("order-get-vouchers"));

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
                    field("customerId", "string", true, "The customer's UUID, as text."),
                    field(
                        "customerName",
                        "string",
                        true,
                        "The name the order was placed under. Orders written before the commerce model carry the customer id."),
                    field("status", "string", true, ""),
                    field("currency", "string", true, ""),
                    field("subtotal", "decimal", true, "Scale 2, in currency."),
                    field("discount", "decimal", true, "0.00 without a voucher."),
                    field("taxRate", "decimal", true, "The rate snapshotted on the order."),
                    field("tax", "decimal", true, ""),
                    field("total", "decimal", true, "subtotal − discount + tax."),
                    field("voucherCode", "string", false, ""),
                    field("invoiceNumber", "string", false, ""),
                    field(
                        "invoiceStatus",
                        "string",
                        false,
                        "ISSUED, PAID or VOIDED, as the projection derives it from the events."),
                    field("itemCount", "integer", true, "Sum of quantities."),
                    field(
                        "items",
                        "OrderLine[]",
                        true,
                        "productId, sku, name, quantity, unitPrice, subtotal, discount, total."),
                    field("paymentId", "uuid", false, ""),
                    field("reservationId", "uuid", false, ""),
                    field("trackingNumber", "string", false, ""),
                    field("carrier", "string", false, ""),
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
                    field("customerId", "string", true, "The customer's UUID, as text."),
                    field("customerName", "string", true, "From the customer's latest order."),
                    field("currency", "string", true, "The currency of shippedValue."),
                    field("ordersPlaced", "integer", true, ""),
                    field("ordersShipped", "integer", true, ""),
                    field("ordersCancelled", "integer", true, ""),
                    field("shippedValue", "decimal", true, "Sum of shipped orders' totals."),
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

    var catalog =
        "inventory-service/src/main/java/io/zeroshift/inventory/infrastructure/ProductCatalog.java";
    var products =
        "inventory-service/src/main/java/io/zeroshift/inventory/infrastructure/ProductController.java";
    ops.add(
        Op.api("inventory-service", "inventory-service", "GET", "/products")
            .group("Inventory")
            .title("List products")
            .summary(
                "The catalog: what is sold, at what price, and how many are left. inventory-service owns products; order-service prices every order from here.")
            .query(
                "sku",
                "string[]",
                false,
                null,
                "Repeatable, at most 100. Exactly those products, unknown SKUs left out; the answer is a complete list and limit is ignored. This is how order-service prices an order.")
            .query(
                "active",
                "boolean",
                false,
                null,
                "Only active (true) or inactive (false) products. Omitted means all.")
            .query(
                "limit",
                "integer",
                false,
                "100",
                "1–500, without sku. The service reads one extra row and sets meta.hasMore.")
            .response(
                200,
                "ApiResponse of ProductCatalog.ProductView, by SKU.",
                schema(
                    "ProductView",
                    catalog,
                    """
                    { "data": [ { "id": "7d9f3b2a-4c1e-5a6b-8d7e-9f0a1b2c3d4e", "sku": "SKU-CABLE", "name": "USB-C cable, 2 m", "description": "Braided USB 3.2 cable rated for 100 W charging and 10 Gbps data.", "price": 12.99, "currency": "USD", "stock": 512, "version": 1, "active": true, "createdAt": "2026-09-26T00:00:00Z", "updatedAt": "2026-09-26T00:00:00Z" } ], "meta": { "count": 1, "limit": 100, "hasMore": false } }
                    """,
                    field("id", "uuid", true, "Product id, carried on order lines as productId."),
                    field(
                        "sku",
                        "string",
                        true,
                        "Unique. Upper-case letters, digits and hyphens, 3–40 characters."),
                    field("name", "string", true, ""),
                    field("description", "string", true, ""),
                    field("price", "decimal", true, "Unit price, scale 2."),
                    field(
                        "currency",
                        "string",
                        true,
                        "commerce.currency; the lab sells in one currency."),
                    field("stock", "integer", true, "Units that can still be sold."),
                    field(
                        "version",
                        "long",
                        true,
                        "Optimistic version, incremented by every update."),
                    field("active", "boolean", true, "An inactive product cannot be ordered."),
                    field("createdAt", "datetime", true, ""),
                    field("updatedAt", "datetime", true, "")))
            .error(400, "VALIDATION_FAILED", "limit is outside 1–500, or more than 100 sku values.")
            .idempotency("Safe read.")
            .source(products)
            .done("inventory-get-products"));
    ops.add(
        Op.api("inventory-service", "inventory-service", "GET", "/products/{id}")
            .group("Inventory")
            .title("Get product")
            .summary("One product with its price and remaining stock.")
            .path("id", "uuid", "Product id.")
            .response(200, "ProductCatalog.ProductView, the same fields as List products.", null)
            .error(404, "PRODUCT_NOT_FOUND", "No product with this id.")
            .error(400, "INVALID_PARAMETER", "id is not a UUID.")
            .idempotency("Safe read.")
            .source(products)
            .done("inventory-get-product"));
    ops.add(
        Op.api("inventory-service", "inventory-service", "GET", "/stock")
            .group("Inventory")
            .title("Stock levels")
            .summary(
                "Available and reserved units per product. available can still be sold; reserved is held by open reservations.")
            .response(
                200,
                "ApiResponse of ProductCatalog.StockLevel. A complete list, so meta.hasMore is null.",
                stockLevel())
            .idempotency("Safe read.")
            .source(
                "inventory-service/src/main/java/io/zeroshift/inventory/infrastructure/StockController.java")
            .done("inventory-get-stock"));

    var payments =
        "payment-service/src/main/java/io/zeroshift/payment/infrastructure/PaymentController.java";
    ops.add(
        Op.api("payment-service", "payment-service", "GET", "/payments")
            .group("Payments")
            .title("Payments of an order")
            .summary(
                "Every payment row of the order, oldest first. More than one only when the commands carried different idempotency keys.")
            .query("orderId", "uuid", true, null, "The order.")
            .response(
                200,
                "ApiResponse of PostgresPayments.PaymentView. A complete list, so meta.hasMore is null.",
                schema(
                    "PaymentView",
                    "payment-service/src/main/java/io/zeroshift/payment/infrastructure/PostgresPayments.java",
                    """
                    { "data": [ { "id": "9e8d7c6b-5a4f-4e3d-8c2b-1a0f9e8d7c6b", "orderId": "3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44", "idempotencyKey": "order:3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44:authorize", "status": "AUTHORIZED", "method": "CARD", "amount": 25.25, "currency": "USD", "provider": "zeroshift-gateway", "providerReference": "ch_4Fq9Zt2LmX8Rb1", "failureReason": null, "createdAt": "2026-09-26T00:00:00Z", "authorizedAt": "2026-09-26T00:00:01Z", "declinedAt": null, "refundedAt": null, "voidedAt": null } ], "meta": { "count": 1, "limit": null, "hasMore": null } }
                    """,
                    field("id", "uuid", true, ""),
                    field("orderId", "uuid", true, ""),
                    field(
                        "idempotencyKey",
                        "string",
                        true,
                        "Unique. From AuthorizePayment.idempotencyKey (the saga sends order:{orderId}:authorize) and sent to the gateway as its Idempotency-Key."),
                    field(
                        "status",
                        "enum",
                        true,
                        "PENDING, AUTHORIZED, DECLINED, REFUNDED or VOIDED. PENDING: the key is claimed and the gateway's answer is not recorded yet; a redelivery retries the gateway with the same key."),
                    field("method", "enum", true, "CARD."),
                    field(
                        "amount",
                        "decimal",
                        false,
                        "Scale 2. Null, with currency, only for a VOIDED row: refunded before anything was charged."),
                    field("currency", "string", false, ""),
                    field(
                        "provider", "string", true, "payment.provider, default zeroshift-gateway."),
                    field("providerReference", "string", false, "The gateway's charge reference."),
                    field("failureReason", "string", false, "Why it was declined."),
                    field("createdAt", "datetime", true, ""),
                    field("authorizedAt", "datetime", false, ""),
                    field("declinedAt", "datetime", false, ""),
                    field("refundedAt", "datetime", false, ""),
                    field("voidedAt", "datetime", false, "")))
            .error(400, "INVALID_PARAMETER", "orderId is missing or not a UUID.")
            .idempotency("Safe read.")
            .source(payments)
            .done("payment-get-payments"));
    ops.add(
        Op.api("payment-service", "payment-service", "GET", "/payments/{id}")
            .group("Payments")
            .title("Get payment")
            .path("id", "uuid", "Payment id.")
            .response(
                200, "PostgresPayments.PaymentView, the same fields as Payments of an order.", null)
            .error(404, "PAYMENT_NOT_FOUND", "No payment with this id.")
            .error(400, "INVALID_PARAMETER", "id is not a UUID.")
            .idempotency("Safe read.")
            .source(payments)
            .done("payment-get-payment"));

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

  static Schema placeOrder() {
    return schema(
        "PlaceOrderRequest",
        "order-service/src/main/java/io/zeroshift/order/web/PlaceOrderRequest.java",
        """
        {
          "customerId": "5b0e6f4c-2d1a-4c3e-9f7b-1a2b3c4d5e6f",
          "items": [
            { "sku": "SKU-CABLE", "quantity": 2 }
          ],
          "voucherCode": "WELCOME10"
        }
        """,
        field(
            "customerId",
            "uuid",
            true,
            "A customer of order-service. Take one from GET /customers: the seeded ids are random, so the example's id is a placeholder."),
        field(
            "items",
            "Line[]",
            true,
            "1–20 lines. Line is sku (1–40 characters) and quantity (1–99). Each SKU must be an active product of the inventory catalog (GET /products)."),
        field(
            "voucherCode",
            "string",
            false,
            "Optional, at most 40 characters, any case. One of GET /vouchers."));
  }

  static Schema stockLevel() {
    return schema(
        "StockLevel",
        "inventory-service/src/main/java/io/zeroshift/inventory/infrastructure/ProductCatalog.java",
        """
        { "data": [ { "productId": "7d9f3b2a-4c1e-5a6b-8d7e-9f0a1b2c3d4e", "sku": "SKU-CABLE", "name": "USB-C cable, 2 m", "available": 512, "reserved": 3, "version": 4 } ], "meta": { "count": 1, "limit": null, "hasMore": null } }
        """,
        field("productId", "uuid", true, ""),
        field("sku", "string", true, ""),
        field("name", "string", true, ""),
        field("available", "integer", true, "Units that can still be sold: the product's stock."),
        field("reserved", "integer", true, "Units held by open reservations."),
        field("version", "long", true, "Optimistic version of the product row."));
  }

  private static Schema customer() {
    return schema(
        "Customer",
        "order-service/src/main/java/io/zeroshift/order/domain/Customer.java",
        """
        { "id": "5b0e6f4c-2d1a-4c3e-9f7b-1a2b3c4d5e6f", "name": "Amara Okafor", "email": "amara.okafor@example.com", "phone": "+1 (415) 555-0142", "createdAt": "2026-09-26T00:00:00Z" }
        """,
        field("id", "uuid", true, "Pass it as customerId to POST /orders."),
        field("name", "string", true, "Copied onto each order as customerName when it is placed."),
        field("email", "string", true, "Unique, case-insensitive."),
        field("phone", "string", false, ""),
        field("createdAt", "datetime", true, ""));
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
