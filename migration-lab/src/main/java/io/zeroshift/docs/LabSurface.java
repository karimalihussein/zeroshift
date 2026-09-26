package io.zeroshift.docs;

import io.zeroshift.docs.Model.Operation;
import java.util.List;

/** The resilience lab's and the events-over-time lab's operations (Phases 3 and 4). */
final class LabSurface {
  private LabSurface() {}

  static void add(List<Operation> ops) {
    ops.add(
        Op.api("order-service", "order-service", "GET", "/lab/edge")
            .group("Resilience")
            .title("Edge guard settings")
            .summary(
                "Rate limiter, load shedder and bulkhead settings of this replica, its counters, requests in flight and unfinished sagas.")
            .response(200, "The resulting state.", null)
            .source("order-service/src/main/java/io/zeroshift/order/web/EdgeController.java")
            .done("order-get-edge"));
    ops.add(
        Op.api("order-service", "order-service", "PUT", "/lab/edge")
            .group("Resilience")
            .title("Configure edge guards")
            .summary(
                "Replaces this replica's guard settings for POST /orders: rate limit (429), load shedding above N unfinished sagas (503 LOAD_SHED), bulkhead of N concurrent placements (503 BULKHEAD_FULL).")
            .response(200, "The resulting state.", null)
            .error(400, "INVALID_GUARD_SETTINGS", "A limit is below 1.")
            .source("order-service/src/main/java/io/zeroshift/order/web/EdgeController.java")
            .done("order-put-edge"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/lab/throughput")
            .group("Resilience")
            .title("Saga throughput and latency")
            .summary(
                "Sagas finished in the last seconds with end-to-end p50/p95/p99 from the saga table, and how many are unfinished.")
            .query("seconds", "integer", false, "10", "Window, 1–300 s.")
            .response(200, "The resulting state.", null)
            .source("order-service/src/main/java/io/zeroshift/order/web/EdgeController.java")
            .done("order-get-throughput"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/orders/{id}/history")
            .group("Events over time")
            .title("Order history")
            .summary(
                "Every stored event of the order as stored and as read today (upcast), with the state after each. No snapshot is used.")
            .path("id", "uuid", "Order id.")
            .response(200, "The resulting state.", null)
            .error(404, "ORDER_NOT_FOUND", "No events for this order.")
            .source("order-service/src/main/java/io/zeroshift/order/web/HistoryController.java")
            .done("order-get-history"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/orders/{id}/rebuild")
            .group("Events over time")
            .title("Rebuild an order at a version or instant")
            .summary(
                "Folds the event store up to version, or up to what was recorded by at. Returns the state, the events applied and the ones after.")
            .path("id", "uuid", "Order id.")
            .query("version", "integer", false, null, "Last version to apply.")
            .query("at", "instant", false, null, "Apply events recorded at or before this instant.")
            .response(200, "The resulting state.", null)
            .error(404, "ORDER_NOT_FOUND", "No events for this order.")
            .source("order-service/src/main/java/io/zeroshift/order/web/HistoryController.java")
            .done("order-get-rebuild"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/lab/history/shipped-sales")
            .group("Events over time")
            .title("Shipped sales per SKU")
            .summary(
                "Units, revenue and orders per SKU of every shipped order, read from the event store. The history lab compares projections against it.")
            .response(200, "The resulting state.", null)
            .source("order-service/src/main/java/io/zeroshift/order/web/HistoryController.java")
            .done("order-get-shipped-sales"));
    ops.add(
        Op.api("order-service", "order-service", "GET", "/lab/history/versions")
            .group("Events over time")
            .title("Stored schema versions")
            .summary("Stored events per type and schema version.")
            .response(200, "The resulting state.", null)
            .source("order-service/src/main/java/io/zeroshift/order/web/HistoryController.java")
            .done("order-get-versions"));
    ops.add(
        Op.api("order-service", "order-service", "POST", "/lab/history/legacy-order")
            .group("Events over time")
            .title("Place an order as schema v1")
            .summary(
                "Places an order through the normal path while writing OrderPlaced at schema v1 (no currency), as the previous release did.")
            .response(202, "202 with the placed order, as POST /orders.", null)
            .source("order-service/src/main/java/io/zeroshift/order/web/HistoryController.java")
            .done("order-post-legacy-order"));
    ops.add(
        Op.api("payment-service", "payment-service", "PUT", "/lab/gateway/policy")
            .group("Resilience")
            .title("Set the gateway call policy")
            .summary(
                "Timeout, retry schedule (none, immediate, exponential, jitter), attempts, circuit breaker on/off and pausing the payment consumer while the breaker is open.")
            .response(200, "The resulting state.", null)
            .error(
                400, "UNKNOWN_RETRY_MODE", "retry is not none, immediate, exponential or jitter.")
            .error(
                400,
                "INVALID_GATEWAY_POLICY",
                "Timeout outside 50–60000 ms or attempts outside 1–10.")
            .source(
                "payment-service/src/main/java/io/zeroshift/payment/infrastructure/GatewayControl.java")
            .done("payment-put-policy"));
    ops.add(
        Op.api("inventory-service", "inventory-service", "POST", "/lab/stock/{sku}/restock")
            .group("Resilience")
            .title("Restock a SKU")
            .summary(
                "Raises free stock to at least available units, like a delivery. Never lowers stock.")
            .path("sku", "string", "SKU.")
            .query(
                "available",
                "integer",
                true,
                null,
                "Units free to reserve afterwards, 1–10,000,000.")
            .response(200, "The resulting state.", null)
            .error(404, "UNKNOWN_SKU", "No such SKU.")
            .source(
                "inventory-service/src/main/java/io/zeroshift/inventory/infrastructure/StockController.java")
            .done("inventory-post-restock"));
    ops.add(
        Op.api("platform", "platform", "GET", "/lab/pressure")
            .group("Resilience")
            .title("Resource pressure")
            .summary(
                "CPU, heap, threads, Hikari pool, rebalances, consumer decisions and every zeroshift.* meter of this JVM, read from Micrometer now.")
            .response(200, "The resulting state.", null)
            .source("platform/src/main/java/io/zeroshift/platform/LabAdminController.java")
            .done("platform-get-pressure"));
    ops.add(
        Op.api("platform", "platform", "PUT", "/lab/consumers/{id}/config")
            .group("Resilience")
            .title("Override poll settings")
            .summary(
                "Sets max.poll.records and max.poll.interval.ms for one consumer and restarts it in the background; without parameters the defaults apply again.")
            .path("id", "string", "Listener id.")
            .query("maxPollRecords", "integer", false, null, "1–10000.")
            .query("maxPollIntervalMs", "integer", false, null, "1000–600000.")
            .response(200, "The resulting state.", null)
            .error(404, "UNKNOWN_CONSUMER", "No listener with this id.")
            .source("platform/src/main/java/io/zeroshift/platform/LabAdminController.java")
            .done("platform-put-consumer-config"));
    ops.add(
        Op.view("migration-lab", "GET", "/resilience")
            .group("Labs")
            .title("Resilience lab page")
            .summary("The resilience lab.")
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("resilience-get-page"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/resilience/state")
            .group("Resilience lab")
            .title("Resilience lab state")
            .summary(
                "Samples and markers since a time, the load generator, the chaos links and the running experiment.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-get-resilience-state"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/resilience/controls")
            .group("Resilience lab")
            .title("Lever settings")
            .summary(
                "Edge guards of both replicas, gateway policy and payment consumer, read from the services.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-get-resilience-controls"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/resilience/experiments")
            .group("Resilience lab")
            .title("Resilience experiments")
            .summary("The seven experiments with their steps and claims.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-get-resilience-experiments"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/resilience/runs")
            .group("Resilience lab")
            .title("Recorded resilience runs")
            .summary("Finished experiments from lab_run.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-get-resilience-runs"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/load/start")
            .group("Resilience lab")
            .title("Start the load generator")
            .summary(
                "Restocks the SKUs it orders and starts open-loop arrivals with the given profile.")
            .response(200, "The resulting state.", null)
            .error(400, "INVALID_LOAD_PROFILE", "A value is outside the lab limits.")
            .error(400, "UNKNOWN_RETRY_MODE", "Unknown client retry mode.")
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-load-start"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "PUT", "/api/resilience/load")
            .group("Resilience lab")
            .title("Change the load profile")
            .summary("Rate, concurrency, reads, retries and timeout, without stopping.")
            .response(200, "The resulting state.", null)
            .error(400, "INVALID_LOAD_PROFILE", "A value is outside the lab limits.")
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-put-load"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/load/stop")
            .group("Resilience lab")
            .title("Stop the load generator")
            .summary("Stops arrivals; waiting clients finish on their own.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-load-stop"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/load/reset")
            .group("Resilience lab")
            .title("Reset load counters")
            .summary("Stops the load and zeroes its counters.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-load-reset"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/chaos/{link}/{fault}")
            .group("Resilience lab")
            .title("Break or heal a network link")
            .summary("latency, bandwidth, reset, partition, down or heal on one Toxiproxy link.")
            .response(200, "The resulting state.", null)
            .error(400, "UNKNOWN_FAULT", "Unknown fault.")
            .error(404, "UNKNOWN_LINK", "No such link.")
            .error(409, "CHAOS_NOT_RUNNING", "The chaos overlay is not running.")
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-chaos"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/chaos/heal")
            .group("Resilience lab")
            .title("Heal every link")
            .summary("Removes every toxic and re-enables every proxy.")
            .response(200, "The resulting state.", null)
            .error(409, "CHAOS_NOT_RUNNING", "The chaos overlay is not running.")
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-chaos-heal"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "PUT", "/api/resilience/edge")
            .group("Resilience lab")
            .title("Configure edge guards on both replicas")
            .summary("Applies the same guard settings to both order-service replicas.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-put-edge"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "PUT", "/api/resilience/policy")
            .group("Resilience lab")
            .title("Set the gateway policy")
            .summary("Forwards the policy to payment-service.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-put-policy"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/gateway/{mode}")
            .group("Resilience lab")
            .title("Set the gateway mode")
            .summary("healthy, slow, down or declining.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-gateway-mode"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/breaker/reset")
            .group("Resilience lab")
            .title("Reset the circuit breaker")
            .summary("Closes the payment gateway breaker.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-breaker-reset"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/slow")
            .group("Resilience lab")
            .title("Slow a consumer down")
            .summary("Delays every delivery of a service by ms; 0 restores normal speed.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-slow"));
    ops.add(
        Op.api(
                "migration-lab",
                "migration-lab",
                "POST",
                "/api/resilience/consumers/payment/{action}")
            .group("Resilience lab")
            .title("Pause or resume the payment consumer")
            .summary("pause keeps it in its group.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-payment-consumer"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "PUT", "/api/resilience/consumers/payment/config")
            .group("Resilience lab")
            .title("Payment consumer poll settings")
            .summary("max.poll.records and max.poll.interval.ms; none restores defaults.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-put-payment-consumer-config"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/experiments/{id}/start")
            .group("Resilience lab")
            .title("Start an experiment")
            .summary(
                "Resets the lab, starts the experiment load and runs the Hypothesis step (all steps with auto=true).")
            .response(200, "The resulting state.", null)
            .error(404, "UNKNOWN_EXPERIMENT", "No such experiment.")
            .error(409, "EXPERIMENT_RUNNING", "Another experiment is running.")
            .error(409, "CHAOS_NOT_RUNNING", "The experiment needs the chaos overlay.")
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-experiment-start"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/experiments/next")
            .group("Resilience lab")
            .title("Run the next step")
            .summary("Runs the next step of the waiting experiment (the rest with auto=true).")
            .response(200, "The resulting state.", null)
            .error(409, "NOT_WAITING", "No experiment is waiting.")
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-experiment-next"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/experiments/abort")
            .group("Resilience lab")
            .title("Abort the experiment")
            .summary("Stops it and resets every lever.")
            .response(200, "The resulting state.", null)
            .error(409, "NO_EXPERIMENT", "No experiment has run.")
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-experiment-abort"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/resilience/reset")
            .group("Resilience lab")
            .title("Reset the lab")
            .summary(
                "Heals links, turns guards off, restores the gateway and consumers, stops the load.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/resilience/ResilienceController.java")
            .done("lab-post-resilience-reset"));
    ops.add(
        Op.view("migration-lab", "GET", "/history")
            .group("Labs")
            .title("Events over time page")
            .summary("The events-over-time lab.")
            .source("migration-lab/src/main/java/io/zeroshift/history/HistoryController.java")
            .done("history-get-page"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/history/orders")
            .group("Events over time")
            .title("Recent orders")
            .summary("Recent orders from order-service, optionally for one customer.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/history/HistoryController.java")
            .done("lab-get-history-orders"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/history/orders/{id}")
            .group("Events over time")
            .title("Order history")
            .summary(
                "The order's stored events as stored and as read today, with the state after each.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/history/HistoryController.java")
            .done("lab-get-history-order"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/history/orders/{id}/rebuild")
            .group("Events over time")
            .title("Rebuild at a version or instant")
            .summary("The order folded by order-service up to version or as of at.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/history/HistoryController.java")
            .done("lab-get-history-rebuild"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/history/orders/{id}/kafka")
            .group("Events over time")
            .title("Replay order.events from an instant")
            .summary(
                "Per-partition offsets from Kafka's time index and the order's records from there.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/history/HistoryController.java")
            .done("lab-get-history-kafka"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/history/labs")
            .group("Events over time")
            .title("Events-over-time labs")
            .summary("The four labs, their six steps and the current run.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/history/HistoryController.java")
            .done("lab-get-history-labs"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "POST", "/api/history/labs/{id}/steps/{n}")
            .group("Events over time")
            .title("Run a lab step")
            .summary("Runs step n (0 starts over); steps run in order.")
            .response(200, "The resulting state.", null)
            .error(404, "UNKNOWN_LAB", "No such lab.")
            .error(409, "OUT_OF_ORDER", "Not the next step.")
            .error(502, "STEP_FAILED", "The step failed; it can be run again.")
            .source("migration-lab/src/main/java/io/zeroshift/history/HistoryController.java")
            .done("lab-post-history-step"));
    ops.add(
        Op.api("migration-lab", "migration-lab", "GET", "/api/history/runs")
            .group("Events over time")
            .title("Recorded history runs")
            .summary("Completed labs from lab_run.")
            .response(200, "The resulting state.", null)
            .source("migration-lab/src/main/java/io/zeroshift/history/HistoryController.java")
            .done("lab-get-history-runs"));
  }
}
