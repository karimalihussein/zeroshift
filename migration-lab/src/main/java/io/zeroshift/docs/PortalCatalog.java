package io.zeroshift.docs;

import io.zeroshift.docs.Model.EventDoc;
import io.zeroshift.docs.Model.NavGroup;
import io.zeroshift.docs.Model.NavItem;
import io.zeroshift.docs.Model.Operation;
import io.zeroshift.docs.Model.Portal;
import io.zeroshift.docs.Model.SearchHit;
import io.zeroshift.docs.Model.ServiceDoc;
import io.zeroshift.docs.Model.TopicDoc;
import io.zeroshift.kafkalab.LabTopics;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Assembles the portal from contracts, the topic script, the connector file and the HTTP surface.
 */
public final class PortalCatalog {
  private static final Pattern DATABASES = Pattern.compile("OUTBOX_DATABASES:-([a-z ]+)}");

  private PortalCatalog() {}

  public static Portal build() {
    var script = resource("topics.sh");
    var connector = resource("outbox-connector.json");
    var register = resource("register.sh");
    var headers = Events.headersFromConnector(connector);
    var events = Events.all(headers);
    var topics = topics(script, events);
    var operations = Surface.all();
    var services = services();
    var pages = Guides.all();
    var errors = Codes.all();
    var portal =
        new Portal(
            "ZeroShift",
            "1.0.0",
            "Developer documentation",
            navigation(pages, operations, events),
            pages,
            operations,
            events,
            topics,
            services,
            errors,
            List.of());
    return new Portal(
        portal.name(),
        portal.version(),
        portal.tagline(),
        portal.navigation(),
        portal.pages(),
        portal.operations(),
        portal.events(),
        portal.topics(),
        portal.services(),
        portal.errors(),
        search(portal));
  }

  static String resource(String name) {
    try (InputStream in = PortalCatalog.class.getResourceAsStream("/docs/" + name)) {
      if (in == null) throw new IllegalStateException("Missing classpath resource docs/" + name);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Could not read docs/" + name, e);
    }
  }

  private static List<TopicDoc> topics(String script, List<EventDoc> events) {
    int replication = TopicScript.replicationFactor(script);
    var byTopic = new LinkedHashMap<String, List<String>>();
    for (var event : events)
      byTopic.computeIfAbsent(event.topic(), k -> new ArrayList<>()).add(event.type());
    var docs = new ArrayList<TopicDoc>();
    for (var declared : TopicScript.parse(script)) {
      var types = byTopic.getOrDefault(declared.name(), List.of());
      var dlt = declared.name().endsWith(".dlt");
      docs.add(
          new TopicDoc(
              declared.name(),
              declared.partitions(),
              String.valueOf(replication),
              retention(declared.configs().get("retention.ms")),
              "delete (topics.sh does not set cleanup.policy)",
              configs(declared.configs()),
              dlt
                  ? List.of("DeadLetterPublishingRecoverer")
                  : producersFor(declared.name(), events),
              dlt ? List.of() : Events.consumers(declared.name()),
              declared.name().equals("shipping.carrier-scans")
                  ? "tracking number, event id, or hub-AMS"
                  : declared.name().equals("lab.retention-demo")
                      ? "lab key"
                      : declared.name().equals("lab.dr.payments") ? "account id" : "orderId",
              dlt ? null : declared.name() + ".dlt",
              types,
              dlt
                  ? "One partition, kept without a retention limit, so dead letters stay in arrival order."
                  : note(declared.name())));
    }
    for (var lab : LabTopics.declared()) {
      docs.add(
          new TopicDoc(
              lab.name(),
              lab.partitions(),
              lab.replicationFactor() == null ? null : lab.replicationFactor().toString(),
              lab.retentionMs() == null
                  ? "broker default (pinned topics do not set retention.ms)"
                  : retention(lab.retentionMs()),
              "delete (not overridden)",
              List.of(
                  "min.insync.replicas=" + lab.minInSyncReplicas(),
                  lab.retentionMs() == null
                      ? "retention.ms unset"
                      : "retention.ms=" + lab.retentionMs()),
              List.of("kafka-lab"),
              lab.name().equals(LabTopics.REPLICATED)
                  ? List.of("lab.replicated.reader")
                  : List.of(),
              lab.name().startsWith("lab.delivery") ? "order-{seq}" : "scenario key",
              null,
              List.of(),
              lab.notes()));
    }
    return List.copyOf(docs);
  }

  private static List<String> producersFor(String topic, List<EventDoc> events) {
    return events.stream()
        .filter(event -> event.topic().equals(topic))
        .flatMap(event -> event.producers().stream())
        .distinct()
        .toList();
  }

  private static String note(String topic) {
    if (topic.equals("order.events"))
      return "The replayable history the read model is rebuilt from, so retention is unlimited.";
    if (topic.equals("shipping.carrier-scans"))
      return "Separate from shipping commands so the ordering lab can add partitions and recreate it.";
    if (topic.equals("lab.retention-demo"))
      return "Short retention and a small segment.ms so closed segments disappear while you watch.";
    if (topic.equals("lab.dr.payments"))
      return "The failure lab's disaster-recovery log: kept forever, because a restored database replays from it.";
    if (topic.equals("lab.trust.payments"))
      return "The failure lab's unauthenticated payment events, where a forged event gets in. Its protected twin lives on kafka-secure.";
    return "Commands and participant events are kept for 7 days.";
  }

  private static List<String> configs(Map<String, String> configs) {
    return configs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList();
  }

  static String retention(String millis) {
    if (millis == null) return "broker default";
    return switch (millis) {
      case "-1" -> "forever (retention.ms=-1)";
      case "604800000" -> "7 days";
      case "86400000" -> "1 day";
      case "3600000" -> "1 hour";
      case "60000" -> "60 seconds";
      default -> millis + " ms";
    };
  }

  static List<String> outboxDatabases(String registerScript) {
    var matcher = DATABASES.matcher(registerScript);
    if (!matcher.find())
      throw new IllegalArgumentException("register.sh has no OUTBOX_DATABASES default");
    return List.of(matcher.group(1).trim().split("\\s+"));
  }

  private static List<ServiceDoc> services() {
    return List.of(
        service(
            "migration-lab",
            "Migration lab",
            "SQL Server to PostgreSQL migration, the event-lab control plane, and this documentation portal.",
            8080,
            null,
            "PostgreSQL zeroshift on 55433, SQL Server source on 1434",
            "http://localhost:8080/actuator/health",
            "migration-lab",
            "DashboardController, LiveChangesController, EventLabController, KafkaLabController",
            List.of(),
            List.of("control-plane-tap"),
            List.of("SQL Server", "PostgreSQL", "Kafka", "Kafka Connect"),
            "Micrometer prometheus, request ids, OpenTelemetry agent when Compose sets JAVA_TOOL_OPTIONS."),
        service(
            "order-service",
            "Order service",
            "Event-sourced orders, their invoices, customers and vouchers, and the saga orchestrator. Prices every order from the inventory catalog. Two replicas share the database and the order-saga group.",
            18081,
            "Replica B on 18088",
            "jdbc:postgresql://localhost:55434/orders",
            "http://localhost:18081/actuator/health",
            "order-service",
            "OrderController, CommerceController, HistoryController, EdgeController and LabAdminController",
            List.of("order.events", "payment.commands", "inventory.commands", "shipping.commands"),
            List.of("order-saga"),
            List.of("PostgreSQL", "Debezium", "Kafka", "inventory-service GET /products"),
            "One trace continues across the outbox via the traceparent header. Decision log per replica."),
        service(
            "payment-service",
            "Payment service",
            "Authorizes and refunds through a WireMock gateway, with a retry and a circuit breaker. One payment row per idempotency key, so a redelivered command never charges twice.",
            18082,
            null,
            "jdbc:postgresql://localhost:55434/payments",
            "http://localhost:18082/actuator/health",
            "payment-service",
            "PaymentController, GatewayControl and LabAdminController",
            List.of("payment.events"),
            List.of("payment-service"),
            List.of("PostgreSQL", "Kafka", "WireMock :18090"),
            "Breaker metrics are bound to Micrometer. Gateway calls are stored in the service database."),
        service(
            "inventory-service",
            "Inventory service",
            "Owns the product catalog: prices and stock. Reserves with one conditional update per product, so the last item is sold once, and releases what a cancelled order held.",
            18084,
            null,
            "jdbc:postgresql://localhost:55434/inventory",
            "http://localhost:18084/actuator/health",
            "inventory-service",
            "ProductController, StockController and LabAdminController",
            List.of("inventory.events"),
            List.of("inventory-service"),
            List.of("PostgreSQL", "Kafka"),
            "Consumer decisions and the outbox lag are on /lab."),
        service(
            "shipping-service",
            "Shipping service",
            "Schedules shipments and runs the carrier-scan ordering lab.",
            18085,
            null,
            "jdbc:postgresql://localhost:55434/shipping",
            "http://localhost:18085/actuator/health",
            "shipping-service",
            "CarrierLab and LabAdminController",
            List.of("shipping.events", "shipping.carrier-scans"),
            List.of("shipping-service", "carrier-tracking"),
            List.of("PostgreSQL", "Kafka"),
            "Scan outcomes record the partition and offset they landed on."),
        service(
            "order-query-service",
            "Query service",
            "CQRS read models. It consumes order.events and does not publish an outbox.",
            18086,
            null,
            "jdbc:postgresql://localhost:55434/order_query",
            "http://localhost:18086/actuator/health",
            "order-query-service",
            "QueryController and LabAdminController",
            List.of(),
            List.of("order-projection"),
            List.of("PostgreSQL", "Kafka"),
            "zeroshift.outbox-slot is false. Projection lag is the rebuild signal."));
  }

  private static ServiceDoc service(
      String id,
      String name,
      String purpose,
      int port,
      String replica,
      String database,
      String health,
      String module,
      String rest,
      List<String> produces,
      List<String> consumes,
      List<String> dependsOn,
      String observability) {
    return new ServiceDoc(
        id,
        name,
        purpose,
        port,
        replica,
        database,
        health,
        module,
        List.of(rest),
        produces,
        consumes,
        dependsOn,
        observability);
  }

  private static List<NavGroup> navigation(
      List<io.zeroshift.docs.Model.Page> pages, List<Operation> operations, List<EventDoc> events) {
    var groups = new ArrayList<NavGroup>();
    groups.add(group("Getting started", pages, "Getting started"));
    groups.add(group("Core concepts", pages, "Core concepts"));
    groups.add(
        new NavGroup(
            "Services",
            List.of(
                item("Catalog", "/docs/services"),
                item("Migration lab", "/docs/services/migration-lab"),
                item("Order service", "/docs/services/order-service"),
                item("Payment service", "/docs/services/payment-service"),
                item("Inventory service", "/docs/services/inventory-service"),
                item("Shipping service", "/docs/services/shipping-service"),
                item("Query service", "/docs/services/order-query-service"))));
    var api = new LinkedHashMap<String, List<NavItem>>();
    for (var operation : operations) {
      if (!operation.api()) continue;
      api.computeIfAbsent(operation.group(), key -> new ArrayList<>())
          .add(new NavItem(operation.title(), "/docs/api/" + operation.id(), operation.method()));
    }
    for (var entry : api.entrySet())
      groups.add(new NavGroup(entry.getKey(), List.copyOf(entry.getValue())));
    var eventItems = new ArrayList<NavItem>();
    eventItems.add(item("Envelopes", "/docs/events"));
    for (var event : events)
      eventItems.add(item(event.type(), "/docs/events/" + slug(event.type()), null));
    groups.add(new NavGroup("Events", List.copyOf(eventItems)));
    groups.add(
        new NavGroup(
            "Kafka",
            List.of(
                item("Overview", "/docs/kafka"),
                item("Topics", "/docs/kafka/topics"),
                item("Consumer groups", "/docs/kafka/groups"),
                item("Outbox", "/docs/kafka/outbox"),
                item("Debezium", "/docs/cdc"),
                item("Dead letters", "/docs/kafka/dead-letters"))));
    groups.add(group("gRPC", pages, "gRPC"));
    groups.add(group("Reliability", pages, "Reliability"));
    groups.add(group("Labs", pages, "Labs"));
    return List.copyOf(groups);
  }

  private static NavGroup group(
      String label, List<io.zeroshift.docs.Model.Page> pages, String group) {
    return new NavGroup(
        label,
        pages.stream()
            .filter(page -> page.group().equals(group))
            .map(page -> item(page.title(), "/docs/" + page.slug()))
            .toList());
  }

  private static NavItem item(String label, String href) {
    return item(label, href, null);
  }

  private static NavItem item(String label, String href, String method) {
    return new NavItem(label, href, method);
  }

  static String slug(String value) {
    return value.toLowerCase(Locale.ROOT).replace('.', '-');
  }

  private static List<SearchHit> search(Portal portal) {
    var hits = new ArrayList<SearchHit>();
    for (var page : portal.pages())
      hits.add(new SearchHit("Guide", page.title(), "/docs/" + page.slug(), page.summary()));
    hits.add(
        new SearchHit(
            "Catalog",
            "Service catalog",
            "/docs/services",
            "Ports, databases, topics and health."));
    for (var service : portal.services())
      hits.add(
          new SearchHit(
              "Service", service.name(), "/docs/services/" + service.id(), service.purpose()));
    for (var operation : portal.operations()) {
      if (!operation.api()) continue;
      hits.add(
          new SearchHit(
              "API",
              operation.method() + " " + operation.path(),
              "/docs/api/" + operation.id(),
              operation.title() + " · " + operation.service()));
    }
    for (var event : portal.events())
      hits.add(
          new SearchHit(
              "Event",
              event.type(),
              "/docs/events/" + slug(event.type()),
              event.topic() + " · v" + event.version()));
    for (var topic : portal.topics())
      hits.add(
          new SearchHit(
              "Topic",
              topic.name(),
              "/docs/kafka/topics#" + slug(topic.name()),
              "partitions " + topic.partitions() + " · " + topic.retention()));
    for (var error : portal.errors())
      hits.add(
          new SearchHit(
              "Error",
              error.code(),
              "/docs/errors#" + error.code().toLowerCase(Locale.ROOT),
              error.when()));
    return List.copyOf(hits);
  }
}
